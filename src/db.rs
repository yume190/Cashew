//! SQLite metadata: CAS index, Action Cache storage, access tracking, GC queries.
//!
//! Doc §9: the filesystem CAS is the source of truth and this database is a
//! rebuildable index — every write here happens *after* the blob is published.
//! Doc §14: payloads never live in SQLite (only serialized `ActionResult`s,
//! which are small), and the read path never issues `UPDATE last_access_at`;
//! touches accumulate in memory and are flushed in batches.
//!
//! All SQL runs on `spawn_blocking` threads over an `r2d2` connection pool, so
//! the async runtime is never blocked by disk I/O or by SQLite's write lock.

use std::collections::HashMap;
use std::hash::Hash;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::config::Durability;
use crate::digest::{Digest, HASH_LEN};

/// Bumped only on incompatible schema changes; stored in `counters`.
const SCHEMA_VERSION: i64 = 1;

const KEY_SCHEMA_VERSION: &str = "schema_version";
const KEY_CAS_SIZE: &str = "cas_size_bytes";
const KEY_CAS_COUNT: &str = "cas_entry_count";

/// Upper bound on pending (unflushed) access records. Beyond this the oldest
/// touches are dropped: losing an LRU hint is harmless, unbounded growth is not.
const PENDING_CAP: usize = 50_000;

/// Hashes are stored as raw `BLOB(32)`; timestamps are unix microseconds.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS instances (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS cas_blobs (
    hash           BLOB NOT NULL PRIMARY KEY,
    size_bytes     INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    last_access_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS cas_lru  ON cas_blobs(last_access_at);
CREATE INDEX IF NOT EXISTS cas_fifo ON cas_blobs(created_at);
CREATE TABLE IF NOT EXISTS action_cache (
    instance_id    INTEGER NOT NULL REFERENCES instances(id),
    hash           BLOB NOT NULL,
    size_bytes     INTEGER NOT NULL,
    result         BLOB NOT NULL,
    created_at     INTEGER NOT NULL,
    last_access_at INTEGER NOT NULL,
    PRIMARY KEY (instance_id, hash, size_bytes)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ac_lru ON action_cache(last_access_at);
CREATE TABLE IF NOT EXISTS counters (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"#;

type SqlitePool = Pool<SqliteConnectionManager>;

/// Pending Action Cache touch key: `(instance_name, hash, size_bytes)`.
type AcKey = (String, [u8; HASH_LEN], i64);

/// Which column drives victim selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VictimOrder {
    /// `last_access_at ASC` — LRU / TTL-on-idle.
    LeastRecentlyUsed,
    /// `created_at ASC` — FIFO / TTL-on-age.
    Oldest,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub cas_size_bytes: i64,
    pub cas_entry_count: i64,
    pub ac_entry_count: i64,
    pub ac_size_bytes: i64,
}

/// Handle to the metadata database. Cheap to clone via `Arc`.
pub struct Metadata {
    pool: SqlitePool,
    /// `instance_name -> instances.id`, resolved once per name.
    instances: Mutex<HashMap<String, i64>>,
    /// Pending CAS touches: `hash -> access time (micros)`.
    pending_cas: Mutex<HashMap<[u8; HASH_LEN], i64>>,
    /// Pending AC touches.
    pending_ac: Mutex<HashMap<AcKey, i64>>,
}

impl Metadata {
    pub async fn open(path: &Path, durability: Durability) -> Result<Arc<Metadata>> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || Metadata::open_blocking(&path, durability))
            .await
            .context("metadata open task panicked")?
    }

    fn open_blocking(path: &Path, durability: Durability) -> Result<Arc<Metadata>> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating metadata directory {}", parent.display()))?;
        }

        // `cache` durability trades a window of metadata loss for throughput;
        // the CAS can always rebuild the index (Doc §9).
        let sync = match durability {
            Durability::Cache => "NORMAL",
            Durability::Strict => "FULL",
        };
        // Establish the journal mode on ONE connection first. Switching to WAL
        // takes a brief exclusive lock, and r2d2 opens its connections in
        // parallel, so doing it inside the pool initializer makes the losing
        // connections fail with SQLITE_BUSY while the database is being created.
        // Journal mode is persisted in the file header, so pooled connections
        // only re-assert it as a no-op.
        {
            let boot = rusqlite::Connection::open(path)
                .with_context(|| format!("opening metadata database {}", path.display()))?;
            boot.execute_batch(
                "PRAGMA busy_timeout=5000;\
                 PRAGMA journal_mode=WAL;",
            )
            .context("enabling write-ahead logging")?;
            boot.execute_batch(SCHEMA).context("creating metadata schema")?;
        }

        let manager = SqliteConnectionManager::file(path).with_init(move |conn| {
            conn.execute_batch(&format!(
                "PRAGMA busy_timeout=5000;\
                 PRAGMA journal_mode=WAL;\
                 PRAGMA foreign_keys=ON;\
                 PRAGMA synchronous={sync};"
            ))
        });

        let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        let pool = Pool::builder()
            .max_size(threads.clamp(2, 8) as u32)
            .build(manager)
            .with_context(|| format!("opening metadata database {}", path.display()))?;

        let conn = pool.get().context("acquiring metadata connection")?;

        let found: Option<i64> = conn
            .query_row("SELECT value FROM counters WHERE key = ?1", params![KEY_SCHEMA_VERSION], |r| {
                r.get(0)
            })
            .optional()?;
        match found {
            Some(v) if v != SCHEMA_VERSION => {
                bail!("metadata schema version {v} is not supported (expected {SCHEMA_VERSION})")
            }
            Some(_) => {}
            None => {
                conn.execute(
                    "INSERT INTO counters(key, value) VALUES (?1, ?2)",
                    params![KEY_SCHEMA_VERSION, SCHEMA_VERSION],
                )?;
            }
        }
        for key in [KEY_CAS_SIZE, KEY_CAS_COUNT] {
            conn.execute(
                "INSERT INTO counters(key, value) VALUES (?1, 0) ON CONFLICT(key) DO NOTHING",
                params![key],
            )?;
        }

        // Fold a possibly huge WAL left by an unclean shutdown back into the
        // main database once, at startup only.
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .context("checkpointing write-ahead log")?;
        drop(conn);

        Ok(Arc::new(Metadata {
            pool,
            instances: Mutex::new(HashMap::new()),
            pending_cas: Mutex::new(HashMap::new()),
            pending_ac: Mutex::new(HashMap::new()),
        }))
    }

    /// Index blobs that are already published in the CAS. Idempotent: a blob
    /// that is already indexed only has its access time refreshed, and the
    /// counters stay consistent because only true inserts are counted.
    pub async fn record_blobs(&self, blobs: &[(Digest, u64)]) -> Result<()> {
        if blobs.is_empty() {
            return Ok(());
        }
        let blobs = blobs.to_vec();
        let pool = self.pool.clone();
        spawn_db(move || {
            let mut conn = pool.get()?;
            let tx = conn.transaction()?;
            let now = now_micros();
            let (mut size_delta, mut count_delta) = (0i64, 0i64);
            {
                let mut insert = tx.prepare_cached(
                    "INSERT INTO cas_blobs(hash, size_bytes, created_at, last_access_at)
                     VALUES (?1, ?2, ?3, ?3)
                     ON CONFLICT(hash) DO NOTHING",
                )?;
                let mut touch = tx.prepare_cached(
                    "UPDATE cas_blobs SET last_access_at = ?2 WHERE hash = ?1 AND last_access_at < ?2",
                )?;
                for (digest, size) in &blobs {
                    let size = *size as i64;
                    if insert.execute(params![&digest.hash[..], size, now])? == 1 {
                        size_delta += size;
                        count_delta += 1;
                    } else {
                        touch.execute(params![&digest.hash[..], now])?;
                    }
                }
            }
            bump_counters(&tx, size_delta, count_delta)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Drop index rows for evicted blobs; returns the number of bytes removed.
    pub async fn remove_blobs(&self, digests: &[Digest]) -> Result<u64> {
        if digests.is_empty() {
            return Ok(0);
        }
        let digests = digests.to_vec();
        let pool = self.pool.clone();
        spawn_db(move || {
            let mut conn = pool.get()?;
            let tx = conn.transaction()?;
            let (mut freed, mut count) = (0i64, 0i64);
            {
                let mut del = tx
                    .prepare_cached("DELETE FROM cas_blobs WHERE hash = ?1 RETURNING size_bytes")?;
                for digest in &digests {
                    let size: Option<i64> =
                        del.query_row(params![&digest.hash[..]], |r| r.get(0)).optional()?;
                    if let Some(size) = size {
                        freed += size;
                        count += 1;
                    }
                }
            }
            bump_counters(&tx, -freed, -count)?;
            tx.commit()?;
            Ok(freed as u64)
        })
        .await
    }

    /// Record CAS reads. Non-blocking and lock-light: nothing touches SQLite
    /// until `flush_access` runs (Doc §14).
    pub fn touch_blobs(&self, digests: &[Digest]) {
        if digests.is_empty() {
            return;
        }
        let now = now_micros();
        let mut pending = self.pending_cas.lock().unwrap_or_else(|e| e.into_inner());
        for digest in digests {
            insert_pending(&mut pending, digest.hash, now);
        }
    }

    /// Record an Action Cache hit. Same batching contract as `touch_blobs`.
    pub fn touch_action(&self, instance: &str, d: &Digest) {
        let now = now_micros();
        let mut pending = self.pending_ac.lock().unwrap_or_else(|e| e.into_inner());
        insert_pending(&mut pending, (instance.to_string(), d.hash, d.size_bytes), now);
    }

    /// Apply all pending touches in a single transaction; returns rows updated.
    pub async fn flush_access(&self) -> Result<usize> {
        let cas: Vec<([u8; HASH_LEN], i64)> = {
            let mut pending = self.pending_cas.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *pending).into_iter().collect()
        };
        let ac: Vec<(AcKey, i64)> = {
            let mut pending = self.pending_ac.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *pending).into_iter().collect()
        };
        if cas.is_empty() && ac.is_empty() {
            return Ok(0);
        }

        let pool = self.pool.clone();
        let instances = self.instance_snapshot();
        let resolved = spawn_db(move || {
            let mut conn = pool.get()?;
            let tx = conn.transaction()?;
            let mut cache = instances;
            let mut updated = 0usize;
            {
                let mut cas_update = tx.prepare_cached(
                    "UPDATE cas_blobs SET last_access_at = ?2
                     WHERE hash = ?1 AND last_access_at < ?2",
                )?;
                for (hash, at) in &cas {
                    updated += cas_update.execute(params![&hash[..], at])?;
                }
                let mut ac_update = tx.prepare_cached(
                    "UPDATE action_cache SET last_access_at = ?4
                     WHERE instance_id = ?1 AND hash = ?2 AND size_bytes = ?3
                       AND last_access_at < ?4",
                )?;
                for ((instance, hash, size), at) in &ac {
                    // Touch never creates an instance row: an unknown name simply
                    // has nothing to touch.
                    let Some(id) = lookup_instance(&tx, &mut cache, instance)? else {
                        continue;
                    };
                    updated += ac_update.execute(params![id, &hash[..], size, at])?;
                }
            }
            tx.commit()?;
            Ok((updated, cache))
        })
        .await?;

        let (updated, cache) = resolved;
        self.merge_instances(cache);
        Ok(updated)
    }

    /// Serialized `ActionResult` for `(instance_name, action_digest)`, if cached.
    pub async fn get_action_result(&self, instance: &str, d: &Digest) -> Result<Option<Vec<u8>>> {
        let pool = self.pool.clone();
        let instance = instance.to_string();
        let digest = *d;
        let instances = self.instance_snapshot();
        let (result, cache) = spawn_db(move || {
            let conn = pool.get()?;
            let mut cache = instances;
            let Some(id) = lookup_instance(&conn, &mut cache, &instance)? else {
                return Ok((None, cache));
            };
            let result: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT result FROM action_cache
                     WHERE instance_id = ?1 AND hash = ?2 AND size_bytes = ?3",
                    params![id, &digest.hash[..], digest.size_bytes],
                    |r| r.get(0),
                )
                .optional()?;
            Ok((result, cache))
        })
        .await?;
        self.merge_instances(cache);
        Ok(result)
    }

    /// Insert or replace an Action Cache entry. The caller is responsible for
    /// having verified that the referenced CAS blobs exist (Doc §10).
    pub async fn put_action_result(&self, instance: &str, d: &Digest, bytes: Vec<u8>) -> Result<()> {
        let pool = self.pool.clone();
        let instance = instance.to_string();
        let digest = *d;
        let instances = self.instance_snapshot();
        let cache = spawn_db(move || {
            let conn = pool.get()?;
            let mut cache = instances;
            let id = ensure_instance(&conn, &mut cache, &instance)?;
            let now = now_micros();
            conn.execute(
                "INSERT INTO action_cache(instance_id, hash, size_bytes, result, created_at, last_access_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                 ON CONFLICT(instance_id, hash, size_bytes) DO UPDATE SET
                     result = excluded.result,
                     last_access_at = excluded.last_access_at",
                params![id, &digest.hash[..], digest.size_bytes, bytes, now],
            )?;
            Ok(cache)
        })
        .await?;
        self.merge_instances(cache);
        Ok(())
    }

    /// Delete one Action Cache entry (stale-AC cleanup / eviction).
    pub async fn remove_action_result(&self, instance: &str, d: &Digest) -> Result<bool> {
        let pool = self.pool.clone();
        let instance = instance.to_string();
        let digest = *d;
        let instances = self.instance_snapshot();
        let (removed, cache) = spawn_db(move || {
            let conn = pool.get()?;
            let mut cache = instances;
            let Some(id) = lookup_instance(&conn, &mut cache, &instance)? else {
                return Ok((false, cache));
            };
            let n = conn.execute(
                "DELETE FROM action_cache WHERE instance_id = ?1 AND hash = ?2 AND size_bytes = ?3",
                params![id, &digest.hash[..], digest.size_bytes],
            )?;
            Ok((n > 0, cache))
        })
        .await?;
        self.merge_instances(cache);
        Ok(removed)
    }

    /// Cheap cache statistics: CAS numbers come from maintained counters (never
    /// a `SUM()` over millions of rows); the AC table is small enough to scan.
    pub async fn stats(&self) -> Result<Stats> {
        let pool = self.pool.clone();
        spawn_db(move || {
            let conn = pool.get()?;
            let cas_size_bytes = read_counter(&conn, KEY_CAS_SIZE)?;
            let cas_entry_count = read_counter(&conn, KEY_CAS_COUNT)?;
            let (ac_entry_count, ac_size_bytes) = conn.query_row(
                "SELECT COUNT(*), COALESCE(SUM(length(result)), 0) FROM action_cache",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )?;
            Ok(Stats { cas_size_bytes, cas_entry_count, ac_entry_count, ac_size_bytes })
        })
        .await
    }

    /// Eviction candidates from the CAS index, ordered by policy.
    /// `older_than_micros` applies the TTL cutoff to the ordering column.
    pub async fn select_cas_victims(
        &self,
        order: VictimOrder,
        older_than_micros: Option<i64>,
        limit: usize,
    ) -> Result<Vec<(Digest, u64)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let pool = self.pool.clone();
        spawn_db(move || {
            let column = order_column(order);
            let sql = format!(
                "SELECT hash, size_bytes FROM cas_blobs{} ORDER BY {column} ASC LIMIT ?2",
                match older_than_micros {
                    Some(_) => format!(" WHERE {column} < ?1"),
                    None => String::new(),
                }
            );
            let conn = pool.get()?;
            let mut stmt = conn.prepare_cached(&sql)?;
            let rows = stmt.query_map(
                params![older_than_micros.unwrap_or(0), limit as i64],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)),
            )?;
            let mut out = Vec::new();
            for row in rows {
                let (hash, size) = row?;
                out.push((Digest::new(hash_from_blob(hash)?, size)?, size as u64));
            }
            Ok(out)
        })
        .await
    }

    /// Eviction candidates from the Action Cache, as `(instance_name, digest)`.
    pub async fn select_ac_victims(
        &self,
        order: VictimOrder,
        older_than_micros: Option<i64>,
        limit: usize,
    ) -> Result<Vec<(String, Digest)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let pool = self.pool.clone();
        spawn_db(move || {
            let column = order_column(order);
            let sql = format!(
                "SELECT i.name, a.hash, a.size_bytes
                 FROM action_cache a JOIN instances i ON i.id = a.instance_id{}
                 ORDER BY a.{column} ASC LIMIT ?2",
                match older_than_micros {
                    Some(_) => format!(" WHERE a.{column} < ?1"),
                    None => String::new(),
                }
            );
            let conn = pool.get()?;
            let mut stmt = conn.prepare_cached(&sql)?;
            let rows = stmt.query_map(
                params![older_than_micros.unwrap_or(0), limit as i64],
                |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, i64>(2)?))
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                let (name, hash, size) = row?;
                out.push((name, Digest::new(hash_from_blob(hash)?, size)?));
            }
            Ok(out)
        })
        .await
    }

    /// Rebuild the CAS index from a filesystem scan (Doc §9): the index is
    /// derived state, so a full replace in one transaction is always safe.
    pub async fn reindex(&self, blobs: Vec<(Digest, u64, i64)>) -> Result<Stats> {
        let pool = self.pool.clone();
        spawn_db(move || {
            let mut conn = pool.get()?;
            let tx = conn.transaction()?;
            tx.execute("DELETE FROM cas_blobs", [])?;
            let (mut total_size, mut total_count) = (0i64, 0i64);
            {
                // One prepared statement reused for every row: memory stays flat
                // regardless of how many blobs were scanned.
                let mut insert = tx.prepare(
                    "INSERT INTO cas_blobs(hash, size_bytes, created_at, last_access_at)
                     VALUES (?1, ?2, ?3, ?3)
                     ON CONFLICT(hash) DO NOTHING",
                )?;
                for (digest, size, created_at) in &blobs {
                    let size = *size as i64;
                    if insert.execute(params![&digest.hash[..], size, created_at])? == 1 {
                        total_size += size;
                        total_count += 1;
                    }
                }
            }
            set_counter(&tx, KEY_CAS_SIZE, total_size)?;
            set_counter(&tx, KEY_CAS_COUNT, total_count)?;
            let (ac_entry_count, ac_size_bytes) = tx.query_row(
                "SELECT COUNT(*), COALESCE(SUM(length(result)), 0) FROM action_cache",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )?;
            tx.commit()?;
            Ok(Stats {
                cas_size_bytes: total_size,
                cas_entry_count: total_count,
                ac_entry_count,
                ac_size_bytes,
            })
        })
        .await
    }

    /// Readiness probe: the schema is reachable and the database still accepts
    /// writes. A rolled-back write exercises the WAL write lock without
    /// mutating state; a full `PRAGMA quick_check` is deliberately avoided
    /// because it scans the whole file on every poll.
    pub async fn writable(&self) -> Result<()> {
        let pool = self.pool.clone();
        spawn_db(move || {
            let mut conn = pool.get()?;
            let version = read_counter(&conn, KEY_SCHEMA_VERSION)?;
            if version != SCHEMA_VERSION {
                bail!("metadata schema version {version} != {SCHEMA_VERSION}");
            }
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO counters(key, value) VALUES ('__write_probe__', ?1)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![now_micros()],
            )?;
            tx.rollback()?;
            Ok(())
        })
        .await
    }

    fn instance_snapshot(&self) -> HashMap<String, i64> {
        self.instances.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn merge_instances(&self, resolved: HashMap<String, i64>) {
        let mut cache = self.instances.lock().unwrap_or_else(|e| e.into_inner());
        for (name, id) in resolved {
            cache.insert(name, id);
        }
    }
}

/// Run a blocking SQLite closure on the blocking pool.
async fn spawn_db<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f).await.context("sqlite task panicked")?
}

fn order_column(order: VictimOrder) -> &'static str {
    match order {
        VictimOrder::LeastRecentlyUsed => "last_access_at",
        VictimOrder::Oldest => "created_at",
    }
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn hash_from_blob(raw: Vec<u8>) -> Result<[u8; HASH_LEN]> {
    let len = raw.len();
    raw.try_into().map_err(|_| anyhow!("stored hash has {len} bytes, expected {HASH_LEN}"))
}

fn read_counter(conn: &Connection, key: &str) -> Result<i64> {
    Ok(conn
        .query_row("SELECT value FROM counters WHERE key = ?1", params![key], |r| r.get(0))
        .optional()?
        .unwrap_or(0))
}

fn set_counter(tx: &Transaction<'_>, key: &str, value: i64) -> Result<()> {
    tx.execute(
        "INSERT INTO counters(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

/// Apply counter deltas inside the caller's transaction so `stats()` can read
/// them without aggregating `cas_blobs`. Clamped at zero so a torn historical state
/// can never produce negative sizes.
fn bump_counters(tx: &Transaction<'_>, size_delta: i64, count_delta: i64) -> Result<()> {
    if size_delta == 0 && count_delta == 0 {
        return Ok(());
    }
    // `excluded.value` would observe the clamped insert value, so the update
    // branch applies the raw delta and clamps the result instead.
    let mut stmt = tx.prepare_cached(
        "INSERT INTO counters(key, value) VALUES (?1, MAX(0, ?2))
         ON CONFLICT(key) DO UPDATE SET value = MAX(0, value + ?2)",
    )?;
    stmt.execute(params![KEY_CAS_SIZE, size_delta])?;
    stmt.execute(params![KEY_CAS_COUNT, count_delta])?;
    Ok(())
}

fn lookup_instance(
    conn: &Connection,
    cache: &mut HashMap<String, i64>,
    name: &str,
) -> Result<Option<i64>> {
    if let Some(id) = cache.get(name) {
        return Ok(Some(*id));
    }
    let id: Option<i64> = conn
        .query_row("SELECT id FROM instances WHERE name = ?1", params![name], |r| r.get(0))
        .optional()?;
    if let Some(id) = id {
        cache.insert(name.to_string(), id);
    }
    Ok(id)
}

fn ensure_instance(conn: &Connection, cache: &mut HashMap<String, i64>, name: &str) -> Result<i64> {
    if let Some(id) = cache.get(name) {
        return Ok(*id);
    }
    conn.execute(
        "INSERT INTO instances(name) VALUES (?1) ON CONFLICT(name) DO NOTHING",
        params![name],
    )?;
    let id: i64 =
        conn.query_row("SELECT id FROM instances WHERE name = ?1", params![name], |r| r.get(0))?;
    cache.insert(name.to_string(), id);
    Ok(id)
}

/// Insert a pending touch, evicting the oldest records when the cap is hit.
fn insert_pending<K: Eq + Hash + Clone>(map: &mut HashMap<K, i64>, key: K, now: i64) {
    if map.len() >= PENDING_CAP && !map.contains_key(&key) {
        prune_oldest(map, PENDING_CAP - PENDING_CAP / 8);
    }
    map.insert(key, now);
}

fn prune_oldest<K: Eq + Hash + Clone>(map: &mut HashMap<K, i64>, keep: usize) {
    if map.len() <= keep {
        return;
    }
    let drop_count = map.len() - keep;
    let mut times: Vec<i64> = map.values().copied().collect();
    times.select_nth_unstable(drop_count);
    let threshold = times[drop_count];
    map.retain(|_, at| *at >= threshold);
    // Everything may share one timestamp; drop arbitrary extras to stay bounded.
    if map.len() > keep {
        let excess: Vec<K> = map.keys().take(map.len() - keep).cloned().collect();
        for key in excess {
            map.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        dir: PathBuf,
    }

    impl TempDb {
        fn new() -> TempDb {
            let nanos =
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let dir = std::env::temp_dir().join(format!(
                "bazel-cache-db-{}-{nanos}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            TempDb { dir }
        }

        fn path(&self) -> PathBuf {
            self.dir.join("metadata.sqlite")
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn open_db(tmp: &TempDb) -> Arc<Metadata> {
        Metadata::open(&tmp.path(), Durability::Cache).await.unwrap()
    }

    fn digest(byte: u8, size: i64) -> Digest {
        Digest::new([byte; HASH_LEN], size).unwrap()
    }

    /// Force deterministic timestamps; touching private state is fine in-module.
    fn set_cas_times(md: &Metadata, d: &Digest, created: i64, accessed: i64) {
        let conn = md.pool.get().unwrap();
        let n = conn
            .execute(
                "UPDATE cas_blobs SET created_at = ?2, last_access_at = ?3 WHERE hash = ?1",
                params![&d.hash[..], created, accessed],
            )
            .unwrap();
        assert_eq!(n, 1, "digest not indexed");
    }

    fn cas_times(md: &Metadata, d: &Digest) -> (i64, i64) {
        let conn = md.pool.get().unwrap();
        conn.query_row(
            "SELECT created_at, last_access_at FROM cas_blobs WHERE hash = ?1",
            params![&d.hash[..]],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn counters_survive_insert_duplicate_and_remove() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;

        md.record_blobs(&[(digest(1, 100), 100), (digest(2, 250), 250)]).await.unwrap();
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 2);
        assert_eq!(stats.cas_size_bytes, 350);

        // Re-recording an existing blob must not double count.
        md.record_blobs(&[(digest(1, 100), 100), (digest(1, 100), 100)]).await.unwrap();
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 2);
        assert_eq!(stats.cas_size_bytes, 350);

        // Duplicates inside a single batch count once.
        md.record_blobs(&[(digest(3, 7), 7), (digest(3, 7), 7)]).await.unwrap();
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 3);
        assert_eq!(stats.cas_size_bytes, 357);

        let freed = md.remove_blobs(&[digest(2, 250), digest(9, 1)]).await.unwrap();
        assert_eq!(freed, 250, "only existing rows contribute");
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 2);
        assert_eq!(stats.cas_size_bytes, 107);

        // Removing twice is a no-op, counters stay put.
        assert_eq!(md.remove_blobs(&[digest(2, 250)]).await.unwrap(), 0);
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 2);
        assert_eq!(stats.cas_size_bytes, 107);
    }

    #[tokio::test]
    async fn counters_persist_across_reopen() {
        let tmp = TempDb::new();
        {
            let md = open_db(&tmp).await;
            md.record_blobs(&[(digest(4, 42), 42)]).await.unwrap();
        }
        let md = open_db(&tmp).await;
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 1);
        assert_eq!(stats.cas_size_bytes, 42);
    }

    #[tokio::test]
    async fn flush_access_applies_touches_and_drains_pending() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;

        let a = digest(1, 10);
        let b = digest(2, 20);
        md.record_blobs(&[(a, 10), (b, 20)]).await.unwrap();
        set_cas_times(&md, &a, 1_000, 1_000);
        set_cas_times(&md, &b, 1_000, 1_000);

        md.put_action_result("repo-a", &a, b"result".to_vec()).await.unwrap();
        {
            let conn = md.pool.get().unwrap();
            conn.execute("UPDATE action_cache SET last_access_at = 1000", []).unwrap();
        }

        // Reads only accumulate in memory: nothing hits SQLite yet.
        md.touch_blobs(&[a, a, b]);
        md.touch_action("repo-a", &a);
        assert_eq!(md.pending_cas.lock().unwrap().len(), 2, "duplicates are deduplicated");
        assert_eq!(cas_times(&md, &a).1, 1_000);

        let updated = md.flush_access().await.unwrap();
        assert_eq!(updated, 3, "2 CAS rows + 1 AC row");
        assert!(md.pending_cas.lock().unwrap().is_empty());
        assert!(md.pending_ac.lock().unwrap().is_empty());

        let (created, accessed) = cas_times(&md, &a);
        assert_eq!(created, 1_000, "created_at is immutable");
        assert!(accessed > 1_000, "last_access_at advanced");

        let ac_access: i64 = {
            let conn = md.pool.get().unwrap();
            conn.query_row("SELECT last_access_at FROM action_cache", [], |r| r.get(0)).unwrap()
        };
        assert!(ac_access > 1_000);

        // Nothing pending → no work, no error.
        assert_eq!(md.flush_access().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn flush_access_ignores_unknown_rows() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;
        md.touch_blobs(&[digest(7, 1)]);
        md.touch_action("never-seen", &digest(7, 1));
        assert_eq!(md.flush_access().await.unwrap(), 0);
        let conn = md.pool.get().unwrap();
        let instances: i64 =
            conn.query_row("SELECT COUNT(*) FROM instances", [], |r| r.get(0)).unwrap();
        assert_eq!(instances, 0, "touch must not create instance rows");
    }

    #[tokio::test]
    async fn victim_selection_honours_order_and_ttl_cutoff() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;

        let old_new = digest(1, 1); // created first, used recently
        let new_old = digest(2, 2); // created last, used long ago
        let middle = digest(3, 3);
        md.record_blobs(&[(old_new, 1), (new_old, 2), (middle, 3)]).await.unwrap();
        set_cas_times(&md, &old_new, 100, 900);
        set_cas_times(&md, &middle, 200, 500);
        set_cas_times(&md, &new_old, 300, 100);

        let lru = md.select_cas_victims(VictimOrder::LeastRecentlyUsed, None, 10).await.unwrap();
        assert_eq!(
            lru.iter().map(|(d, _)| d.hash[0]).collect::<Vec<_>>(),
            vec![2, 3, 1],
            "LRU orders by last_access_at"
        );
        assert_eq!(lru[0].1, 2, "size is returned alongside the digest");

        let fifo = md.select_cas_victims(VictimOrder::Oldest, None, 10).await.unwrap();
        assert_eq!(
            fifo.iter().map(|(d, _)| d.hash[0]).collect::<Vec<_>>(),
            vec![1, 3, 2],
            "FIFO orders by created_at"
        );

        let limited = md.select_cas_victims(VictimOrder::Oldest, None, 2).await.unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(md.select_cas_victims(VictimOrder::Oldest, None, 0).await.unwrap().len(), 0);

        // TTL cutoff applies to the ordering column only.
        let idle = md
            .select_cas_victims(VictimOrder::LeastRecentlyUsed, Some(600), 10)
            .await
            .unwrap();
        assert_eq!(idle.iter().map(|(d, _)| d.hash[0]).collect::<Vec<_>>(), vec![2, 3]);
        let aged = md.select_cas_victims(VictimOrder::Oldest, Some(250), 10).await.unwrap();
        assert_eq!(aged.iter().map(|(d, _)| d.hash[0]).collect::<Vec<_>>(), vec![1, 3]);
    }

    #[tokio::test]
    async fn ac_victims_carry_instance_name_and_ordering() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;
        let a = digest(1, 10);
        let b = digest(2, 20);
        md.put_action_result("repo-a", &a, b"a".to_vec()).await.unwrap();
        md.put_action_result("repo-b", &b, b"b".to_vec()).await.unwrap();
        {
            let conn = md.pool.get().unwrap();
            conn.execute(
                "UPDATE action_cache SET created_at = 100, last_access_at = 900 WHERE hash = ?1",
                params![&a.hash[..]],
            )
            .unwrap();
            conn.execute(
                "UPDATE action_cache SET created_at = 300, last_access_at = 100 WHERE hash = ?1",
                params![&b.hash[..]],
            )
            .unwrap();
        }

        let lru = md.select_ac_victims(VictimOrder::LeastRecentlyUsed, None, 10).await.unwrap();
        assert_eq!(
            lru.iter().map(|(n, d)| (n.as_str(), d.hash[0], d.size_bytes)).collect::<Vec<_>>(),
            vec![("repo-b", 2, 20), ("repo-a", 1, 10)]
        );
        let fifo = md.select_ac_victims(VictimOrder::Oldest, None, 10).await.unwrap();
        assert_eq!(fifo[0].0, "repo-a");
        let ttl = md.select_ac_victims(VictimOrder::LeastRecentlyUsed, Some(500), 10).await.unwrap();
        assert_eq!(ttl.len(), 1);
        assert_eq!(ttl[0].0, "repo-b");
    }

    #[tokio::test]
    async fn action_cache_is_namespaced_by_instance() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;
        let action = digest(9, 64);

        md.put_action_result("repo-a", &action, b"from-a".to_vec()).await.unwrap();
        md.put_action_result("repo-b", &action, b"from-b".to_vec()).await.unwrap();

        assert_eq!(
            md.get_action_result("repo-a", &action).await.unwrap().as_deref(),
            Some(&b"from-a"[..])
        );
        assert_eq!(
            md.get_action_result("repo-b", &action).await.unwrap().as_deref(),
            Some(&b"from-b"[..])
        );
        assert!(md.get_action_result("repo-c", &action).await.unwrap().is_none());

        // size_bytes is part of the AC key.
        let other_size = Digest::new(action.hash, 65).unwrap();
        assert!(md.get_action_result("repo-a", &other_size).await.unwrap().is_none());

        let stats = md.stats().await.unwrap();
        assert_eq!(stats.ac_entry_count, 2);
        assert_eq!(stats.ac_size_bytes, 12, "two 6-byte results");

        // Overwrite replaces in place.
        md.put_action_result("repo-a", &action, b"updated".to_vec()).await.unwrap();
        assert_eq!(
            md.get_action_result("repo-a", &action).await.unwrap().as_deref(),
            Some(&b"updated"[..])
        );
        assert_eq!(md.stats().await.unwrap().ac_entry_count, 2);

        // Deleting one namespace leaves the other intact.
        assert!(md.remove_action_result("repo-a", &action).await.unwrap());
        assert!(!md.remove_action_result("repo-a", &action).await.unwrap());
        assert!(!md.remove_action_result("nope", &action).await.unwrap());
        assert!(md.get_action_result("repo-a", &action).await.unwrap().is_none());
        assert!(md.get_action_result("repo-b", &action).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn reindex_rebuilds_counters_from_scratch() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;

        md.record_blobs(&[(digest(1, 100), 100), (digest(2, 100), 100)]).await.unwrap();
        md.put_action_result("repo-a", &digest(5, 1), b"keep".to_vec()).await.unwrap();

        // Scan result disagrees with the stale index: filesystem wins.
        let scanned = vec![
            (digest(2, 100), 100u64, 1_000i64),
            (digest(3, 7), 7, 2_000),
            (digest(3, 7), 7, 2_000), // duplicate must not be double counted
        ];
        let stats = md.reindex(scanned).await.unwrap();
        assert_eq!(stats.cas_entry_count, 2);
        assert_eq!(stats.cas_size_bytes, 107);
        assert_eq!(stats.ac_entry_count, 1, "reindex leaves the Action Cache alone");
        assert_eq!(stats.ac_size_bytes, 4);
        assert_eq!(md.stats().await.unwrap(), stats);

        // Scanned timestamps are preserved and seed last_access_at.
        assert_eq!(cas_times(&md, &digest(3, 7)), (2_000, 2_000));
        assert!(md.get_action_result("repo-a", &digest(5, 1)).await.unwrap().is_some());

        // An empty scan zeroes the CAS counters.
        let stats = md.reindex(Vec::new()).await.unwrap();
        assert_eq!(stats.cas_entry_count, 0);
        assert_eq!(stats.cas_size_bytes, 0);
    }

    #[tokio::test]
    async fn pending_touches_are_capped_and_keep_newest() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;
        {
            let mut pending = md.pending_cas.lock().unwrap();
            for i in 0..PENDING_CAP as u64 {
                let mut hash = [0u8; HASH_LEN];
                hash[..8].copy_from_slice(&i.to_le_bytes());
                insert_pending(&mut pending, hash, i as i64);
            }
            assert_eq!(pending.len(), PENDING_CAP);

            let mut fresh = [0xffu8; HASH_LEN];
            fresh[0] = 1;
            insert_pending(&mut pending, fresh, i64::MAX);
            assert!(pending.len() <= PENDING_CAP, "cap enforced: {}", pending.len());
            assert!(pending.contains_key(&fresh), "newest touch survives");

            let mut oldest = [0u8; HASH_LEN];
            oldest[..8].copy_from_slice(&0u64.to_le_bytes());
            assert!(!pending.contains_key(&oldest), "oldest touch was dropped");
        }
        // The capped map still flushes cleanly (no indexed rows → zero updates).
        assert_eq!(md.flush_access().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn writable_probe_leaves_no_trace() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;
        md.writable().await.unwrap();
        md.writable().await.unwrap();
        let conn = md.pool.get().unwrap();
        let probes: i64 = conn
            .query_row("SELECT COUNT(*) FROM counters WHERE key = '__write_probe__'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(probes, 0, "probe transaction is rolled back");
    }

    #[tokio::test]
    async fn concurrent_record_and_remove_keep_counters_exact() {
        let tmp = TempDb::new();
        let md = open_db(&tmp).await;

        // Many tasks racing to index the same blobs: counters must count each
        // distinct blob exactly once.
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let md = Arc::clone(&md);
            tasks.push(tokio::spawn(async move {
                for i in 0..20u8 {
                    md.record_blobs(&[(digest(i, 10), 10)]).await.unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 20);
        assert_eq!(stats.cas_size_bytes, 200);

        let all: Vec<Digest> = (0..20u8).map(|i| digest(i, 10)).collect();
        assert_eq!(md.remove_blobs(&all).await.unwrap(), 200);
        let stats = md.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 0);
        assert_eq!(stats.cas_size_bytes, 0);
    }

    #[tokio::test]
    async fn rejects_future_schema_version() {
        let tmp = TempDb::new();
        {
            let md = open_db(&tmp).await;
            let conn = md.pool.get().unwrap();
            conn.execute("UPDATE counters SET value = 99 WHERE key = ?1", params![KEY_SCHEMA_VERSION])
                .unwrap();
        }
        let err = Metadata::open(&tmp.path(), Durability::Strict).await.err().unwrap();
        assert!(err.to_string().contains("schema version 99"), "{err}");
    }
}
