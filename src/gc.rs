//! Eviction / garbage collection (Doc §13).
//!
//! The policy is chosen at startup and never hard-coded into the sweep: `Gc`
//! owns the mechanism (select → unlink → drop index row → metrics) while an
//! [`EvictionPolicy`] owns the question "which entries go next?".
//!
//! Two invariants drive the implementation:
//!
//! * **Disk first, index second.** A blob is unlinked before its `cas_blobs`
//!   row is deleted. The reverse order would leave a file nobody knows about
//!   (invisible orphan storage); this order can only leave an index row whose
//!   file is gone, which `reindex` — and the next sweep — heal by themselves.
//! * **Never delete under a reader.** `CasStore::remove` refuses to unlink a
//!   leased digest (Doc §11), so GC leaves those rows alone and retries on a
//!   later sweep.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::config::{Config, EvictionConfig, PolicyKind};
use crate::db::{Metadata, Stats, VictimOrder};
use crate::digest::{Digest, HASH_LEN};
use crate::observability::Metrics;
use crate::storage::CasStore;

/// Upper bound on selection rounds per sweep. With the default batch size this
/// considers a million entries; anything beyond that waits for the next tick
/// instead of holding the sweep (and the SQLite write lock) indefinitely.
const MAX_ROUNDS: usize = 1024;

/// Action Cache rows are tiny but unbounded. Past this count the oldest entries
/// are dropped regardless of policy so SQLite cannot grow without limit.
const AC_ROW_CAP: i64 = 1_000_000;

/// Pluggable eviction strategy (Doc §13).
///
/// `on_read` / `on_write` are the cheap per-request hooks; only access-ordered
/// policies implement them, which is exactly why they exist: the request path
/// stays policy agnostic and pays nothing for FIFO/TTL/none.
#[async_trait]
pub trait EvictionPolicy: Send + Sync {
    fn name(&self) -> &'static str;

    /// Does this policy evict even when the cache is below the high watermark?
    /// Only age-based policies (TTL) do.
    fn evicts_without_pressure(&self) -> bool {
        false
    }

    /// `false` for `none`: the sweep skips straight to refreshing gauges.
    fn enabled(&self) -> bool {
        true
    }

    /// Next victims, most evictable first, capped at `batch` rows and (for
    /// pressure-driven policies) at roughly `bytes_to_free` bytes.
    ///
    /// `bytes_to_free == 0` means "no size pressure": policies that only run
    /// under pressure return nothing.
    async fn select_victims(
        &self,
        db: &Metadata,
        bytes_to_free: u64,
        batch: usize,
    ) -> Result<Vec<(Digest, u64)>>;

    /// Age cutoff (unix micros) for age-based policies, `None` otherwise.
    fn ac_cutoff_micros(&self) -> Option<i64> {
        None
    }

    /// Column that orders Action Cache eviction for this policy.
    fn ac_order(&self) -> VictimOrder {
        VictimOrder::Oldest
    }
}

/// Build the configured policy.
pub fn policy_for(cfg: &EvictionConfig) -> Arc<dyn EvictionPolicy> {
    match cfg.policy {
        PolicyKind::Lru => Arc::new(LruPolicy),
        PolicyKind::Fifo => Arc::new(FifoPolicy),
        PolicyKind::Ttl => Arc::new(TtlPolicy { ttl: cfg.ttl }),
        PolicyKind::None => Arc::new(NonePolicy),
    }
}

/// Least recently used: victims ordered by `last_access_at ASC`.
struct LruPolicy;

#[async_trait]
impl EvictionPolicy for LruPolicy {
    fn name(&self) -> &'static str {
        "lru"
    }

    async fn select_victims(
        &self,
        db: &Metadata,
        bytes_to_free: u64,
        batch: usize,
    ) -> Result<Vec<(Digest, u64)>> {
        if bytes_to_free == 0 {
            return Ok(Vec::new());
        }
        let rows = db.select_cas_victims(VictimOrder::LeastRecentlyUsed, None, batch).await?;
        Ok(cap_to_budget(rows, bytes_to_free))
    }

    fn ac_order(&self) -> VictimOrder {
        VictimOrder::LeastRecentlyUsed
    }
}

/// First in, first out: victims ordered by `created_at ASC`.
struct FifoPolicy;

#[async_trait]
impl EvictionPolicy for FifoPolicy {
    fn name(&self) -> &'static str {
        "fifo"
    }

    async fn select_victims(
        &self,
        db: &Metadata,
        bytes_to_free: u64,
        batch: usize,
    ) -> Result<Vec<(Digest, u64)>> {
        if bytes_to_free == 0 {
            return Ok(Vec::new());
        }
        let rows = db.select_cas_victims(VictimOrder::Oldest, None, batch).await?;
        Ok(cap_to_budget(rows, bytes_to_free))
    }
}

/// Age based: everything older than `ttl` goes on every sweep, pressure or not.
/// If the cache is *still* over budget afterwards, eviction continues
/// oldest-first so the size limit is honoured too.
struct TtlPolicy {
    /// `None` only if validation was bypassed; the policy then degrades to
    /// pure FIFO-under-pressure rather than deleting everything.
    ttl: Option<Duration>,
}

impl TtlPolicy {
    fn cutoff(&self) -> Option<i64> {
        let ttl = self.ttl?;
        let age = i64::try_from(ttl.as_micros()).unwrap_or(i64::MAX);
        Some(now_micros().saturating_sub(age))
    }
}

#[async_trait]
impl EvictionPolicy for TtlPolicy {
    fn name(&self) -> &'static str {
        "ttl"
    }

    fn evicts_without_pressure(&self) -> bool {
        self.ttl.is_some()
    }

    async fn select_victims(
        &self,
        db: &Metadata,
        bytes_to_free: u64,
        batch: usize,
    ) -> Result<Vec<(Digest, u64)>> {
        let mut victims = match self.cutoff() {
            Some(cutoff) => db.select_cas_victims(VictimOrder::Oldest, Some(cutoff), batch).await?,
            None => Vec::new(),
        };
        let aged_bytes: u64 = victims.iter().map(|(_, size)| *size).sum();

        // The TTL pass alone already covers the budget (or filled the batch):
        // nothing more to select this round.
        if aged_bytes >= bytes_to_free || victims.len() >= batch {
            return Ok(victims);
        }

        // Still over the watermark after expiring aged entries: keep going
        // oldest-first. The unfiltered query repeats the aged rows, so dedupe.
        let seen: HashSet<[u8; HASH_LEN]> = victims.iter().map(|(d, _)| d.hash).collect();
        let extra = db.select_cas_victims(VictimOrder::Oldest, None, batch).await?;
        let mut total = aged_bytes;
        for (digest, size) in extra {
            if total >= bytes_to_free || victims.len() >= batch {
                break;
            }
            if seen.contains(&digest.hash) {
                continue;
            }
            total = total.saturating_add(size);
            victims.push((digest, size));
        }
        Ok(victims)
    }

    fn ac_cutoff_micros(&self) -> Option<i64> {
        self.cutoff()
    }
}

/// Never evicts; the cache is managed out of band.
struct NonePolicy;

#[async_trait]
impl EvictionPolicy for NonePolicy {
    fn name(&self) -> &'static str {
        "none"
    }

    fn enabled(&self) -> bool {
        false
    }

    async fn select_victims(
        &self,
        _db: &Metadata,
        _bytes_to_free: u64,
        _batch: usize,
    ) -> Result<Vec<(Digest, u64)>> {
        Ok(Vec::new())
    }
}

/// Trim a victim list once its cumulative size covers the byte budget, so a
/// sweep lands near the low watermark instead of emptying the cache.
fn cap_to_budget(rows: Vec<(Digest, u64)>, budget: u64) -> Vec<(Digest, u64)> {
    let mut total = 0u64;
    let mut out = Vec::with_capacity(rows.len());
    for (digest, size) in rows {
        if total >= budget {
            break;
        }
        total = total.saturating_add(size);
        out.push((digest, size));
    }
    out
}

/// Outcome of one sweep.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Blobs actually unlinked from the CAS.
    pub evicted_entries: u64,
    /// Bytes those blobs occupied.
    pub evicted_bytes: u64,
    /// Action Cache rows deleted.
    pub ac_evicted: u64,
    /// Distinct victims left in place because a reader held a lease.
    pub skipped_leased: u64,
    pub duration: Duration,
}

impl GcReport {
    /// Did this sweep change anything?
    pub fn is_empty(&self) -> bool {
        self.evicted_entries == 0 && self.ac_evicted == 0
    }
}

impl fmt::Display for GcReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "gc: evicted {} blobs ({} bytes), {} action cache entries, {} leased blobs skipped, {:.1?}",
            self.evicted_entries, self.evicted_bytes, self.ac_evicted, self.skipped_leased, self.duration
        )
    }
}

/// What one batch of victims achieved.
#[derive(Default)]
struct RoundOutcome {
    /// Blobs unlinked from disk.
    removed: u64,
    /// Bytes unlinked from disk.
    removed_bytes: u64,
    /// Index rows dropped for blobs whose file had already vanished.
    purged: u64,
    /// Bytes the index released (unlinked + purged); keeps our running size
    /// estimate consistent with `cas_size_bytes`.
    index_freed: u64,
}

impl RoundOutcome {
    fn made_progress(&self) -> bool {
        self.removed > 0 || self.purged > 0
    }
}

/// Background eviction sweeper.
pub struct Gc {
    cfg: Arc<Config>,
    storage: Arc<CasStore>,
    db: Arc<Metadata>,
    metrics: Arc<Metrics>,
    policy: Arc<dyn EvictionPolicy>,
}

impl Gc {
    pub fn new(
        cfg: Arc<Config>,
        storage: Arc<CasStore>,
        db: Arc<Metadata>,
        metrics: Arc<Metrics>,
    ) -> Arc<Gc> {
        let policy = policy_for(&cfg.eviction);
        Arc::new(Gc { cfg, storage, db, metrics, policy })
    }

    /// One full sweep: CAS eviction down to the low watermark (plus any
    /// age-expired entries), then Action Cache maintenance, then metrics.
    pub async fn run_once(&self) -> Result<GcReport> {
        let started = Instant::now();

        if !self.policy.enabled() {
            // `none`: evict nothing, but keep the gauges honest.
            let stats = self.db.stats().await?;
            self.metrics.set_cache_stats(&stats);
            let report = GcReport { duration: started.elapsed(), ..GcReport::default() };
            debug!(policy = self.policy.name(), "gc disabled; nothing evicted");
            return Ok(report);
        }

        // LRU must see the touches buffered since the last flush, otherwise a
        // hot blob can look cold and get evicted.
        if let Err(err) = self.db.flush_access().await {
            warn!(%err, "flushing access timestamps before gc failed; using stale LRU order");
        }

        let stats = self.db.stats().await?;
        let mut report = self.sweep_cas(&stats).await?;
        report.ac_evicted = self.sweep_ac(&stats).await?;
        report.duration = started.elapsed();

        self.metrics.evicted_entries_total.inc_by(report.evicted_entries);
        self.metrics.evicted_bytes_total.inc_by(report.evicted_bytes);
        self.metrics.gc_duration.observe(report.duration.as_secs_f64());
        let after = self.db.stats().await?;
        self.metrics.set_cache_stats(&after);

        if report.is_empty() {
            debug!(
                policy = self.policy.name(),
                cas_bytes = after.cas_size_bytes,
                skipped_leased = report.skipped_leased,
                "gc sweep evicted nothing"
            );
        } else {
            info!(
                policy = self.policy.name(),
                evicted_entries = report.evicted_entries,
                evicted_bytes = report.evicted_bytes,
                ac_evicted = report.ac_evicted,
                skipped_leased = report.skipped_leased,
                cas_bytes = after.cas_size_bytes,
                duration = ?report.duration,
                "gc sweep complete"
            );
        }
        Ok(report)
    }

    /// Evict CAS blobs until the low watermark is reached (and, for age-based
    /// policies, until nothing is expired).
    async fn sweep_cas(&self, stats: &Stats) -> Result<GcReport> {
        let high = self.cfg.high_watermark_bytes();
        let low = self.cfg.low_watermark_bytes();
        let batch = self.cfg.eviction.batch_size.max(1);

        let mut size = stats.cas_size_bytes.max(0) as u64;
        // Pressure is sticky: a sweep that starts above the high watermark runs
        // all the way down to the low one, which is what stops GC thrashing.
        let mut pressure = size > high;
        let mut report = GcReport::default();
        let mut leased: HashSet<[u8; HASH_LEN]> = HashSet::new();

        for round in 0..MAX_ROUNDS {
            let bytes_to_free = if pressure { size.saturating_sub(low) } else { 0 };
            if bytes_to_free == 0 && !self.policy.evicts_without_pressure() {
                break;
            }

            let victims = self.policy.select_victims(&self.db, bytes_to_free, batch).await?;
            if victims.is_empty() {
                break;
            }
            // Skip blobs already found leased in this sweep: retrying them
            // would burn syscalls and hide real progress.
            let victims: Vec<(Digest, u64)> =
                victims.into_iter().filter(|(d, _)| !leased.contains(&d.hash)).collect();
            if victims.is_empty() {
                break;
            }

            let outcome = self.drop_victims(&victims, &mut leased).await?;
            report.evicted_entries += outcome.removed;
            report.evicted_bytes += outcome.removed_bytes;
            size = size.saturating_sub(outcome.index_freed);
            if pressure && size <= low {
                pressure = false;
            }
            if !outcome.made_progress() {
                // Everything in this batch was leased or unreadable; a tighter
                // loop would spin, so leave the rest for the next sweep.
                break;
            }
            if round + 1 == MAX_ROUNDS {
                warn!(
                    rounds = MAX_ROUNDS,
                    cas_bytes = size,
                    "gc hit its per-sweep round limit; continuing next tick"
                );
            }
        }

        report.skipped_leased = leased.len() as u64;
        Ok(report)
    }

    /// Unlink one batch, then drop the matching index rows. Disk first: see the
    /// module header.
    async fn drop_victims(
        &self,
        victims: &[(Digest, u64)],
        leased: &mut HashSet<[u8; HASH_LEN]>,
    ) -> Result<RoundOutcome> {
        let mut outcome = RoundOutcome::default();
        let mut deletable = Vec::with_capacity(victims.len());

        for (digest, size) in victims {
            if digest.is_empty() {
                // The empty blob is virtual (never on disk); its index row is
                // pure overhead, so drop it without touching the filesystem.
                deletable.push(*digest);
                outcome.purged += 1;
                continue;
            }
            match self.storage.remove(digest).await {
                Ok(true) => {
                    deletable.push(*digest);
                    outcome.removed += 1;
                    outcome.removed_bytes += *size;
                }
                Ok(false) => match self.storage.stat(digest).await {
                    // Still on disk => an active reader holds a lease (Doc §11).
                    // Keep the index row so a later sweep can retry.
                    Ok(Some(_)) => {
                        leased.insert(digest.hash);
                    }
                    // Gone behind our back (manual delete, crash): the index is
                    // stale, so purge the row.
                    Ok(None) => {
                        deletable.push(*digest);
                        outcome.purged += 1;
                    }
                    Err(err) => {
                        warn!(digest = %digest, %err, "stat during eviction failed; keeping index row");
                    }
                },
                Err(err) => {
                    warn!(digest = %digest, %err, "unlinking blob failed; keeping index row");
                }
            }
        }

        outcome.index_freed = self.db.remove_blobs(&deletable).await?;
        Ok(outcome)
    }

    /// Action Cache maintenance: expire aged rows under an age-based policy,
    /// and enforce a hard row cap for every other policy so the table cannot
    /// grow without bound.
    async fn sweep_ac(&self, stats: &Stats) -> Result<u64> {
        let batch = self.cfg.eviction.batch_size.max(1);
        let cutoff = self.policy.ac_cutoff_micros();
        let mut over_cap = (stats.ac_entry_count - AC_ROW_CAP).max(0) as u64;
        if cutoff.is_none() && over_cap == 0 {
            return Ok(0);
        }

        let (order, limit_cap) = match cutoff {
            Some(_) => (self.policy.ac_order(), None),
            // Cap enforcement is age based by definition: oldest rows first.
            None => (VictimOrder::Oldest, Some(over_cap)),
        };

        let mut evicted = 0u64;
        for _ in 0..MAX_ROUNDS {
            let limit = match limit_cap {
                Some(_) => batch.min(over_cap.max(1) as usize),
                None => batch,
            };
            let victims = self.db.select_ac_victims(order, cutoff, limit).await?;
            if victims.is_empty() {
                break;
            }
            let mut round = 0u64;
            for (instance, digest) in victims {
                if self.db.remove_action_result(&instance, &digest).await? {
                    round += 1;
                }
            }
            if round == 0 {
                break;
            }
            evicted += round;
            if limit_cap.is_some() {
                over_cap = over_cap.saturating_sub(round);
                if over_cap == 0 {
                    break;
                }
            }
        }
        Ok(evicted)
    }

    /// Sweep on a fixed interval until shutdown.
    pub async fn run_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let interval = self.cfg.eviction.interval;
        let mut ticker = tokio::time::interval(interval);
        // Delay (not Burst) so a long sweep cannot queue a backlog of ticks.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick fires immediately; skip it so startup is not stalled
        // by a full sweep.
        ticker.tick().await;

        info!(
            policy = self.policy.name(),
            interval = ?interval,
            high_watermark = self.cfg.high_watermark_bytes(),
            low_watermark = self.cfg.low_watermark_bytes(),
            "gc loop started"
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match self.run_once().await {
                        Ok(report) if !report.is_empty() => debug!(%report, "gc tick"),
                        Ok(_) => {}
                        Err(err) => warn!(%err, "gc sweep failed; retrying next tick"),
                    }
                }
                // The `Ref` guard is dropped inside the block so the loop
                // future stays `Send` and can be spawned.
                _ = async { let _ = shutdown.wait_for(|stop| *stop).await; } => break,
            }
        }
        debug!("gc loop stopped");
    }
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::config::Durability;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Harness {
        dir: PathBuf,
        storage: Arc<CasStore>,
        db: Arc<Metadata>,
        metrics: Arc<Metrics>,
        gc: Arc<Gc>,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Real `CasStore` + `Metadata` over a throwaway directory.
    async fn harness(tag: &str, tune: impl FnOnce(&mut Config)) -> Harness {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "bazel-cache-gc-{tag}-{}-{nanos}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut cfg = Config::default();
        cfg.storage.root = dir.clone();
        cfg.storage.durability = Durability::Cache;
        cfg.eviction.high_watermark = 0.9;
        cfg.eviction.low_watermark = 0.5;
        cfg.eviction.batch_size = 100;
        tune(&mut cfg);
        cfg.validate().expect("test config must be valid");
        let cfg = Arc::new(cfg);

        let storage = CasStore::open(&cfg.cas_dir(), &cfg.tmp_dir(), cfg.storage.durability)
            .await
            .unwrap();
        let db = Metadata::open(&cfg.db_path(), cfg.storage.durability).await.unwrap();
        let metrics = Metrics::new().unwrap();
        let gc = Gc::new(cfg.clone(), storage.clone(), db.clone(), metrics.clone());
        Harness { dir, storage, db, metrics, gc }
    }

    /// Publish a blob and index it. The sleep keeps `created_at` strictly
    /// increasing so ordering assertions are deterministic.
    async fn put(h: &Harness, byte: u8, len: usize) -> Digest {
        let data = vec![byte; len];
        let digest = Digest::of(&data);
        h.storage.put_bytes(&digest, &data).await.unwrap();
        h.db.record_blobs(&[(digest, len as u64)]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        digest
    }

    async fn indexed(h: &Harness, d: &Digest) -> bool {
        h.db.select_cas_victims(VictimOrder::Oldest, None, 1000)
            .await
            .unwrap()
            .iter()
            .any(|(x, _)| x.hash == d.hash)
    }

    async fn on_disk(h: &Harness, d: &Digest) -> bool {
        h.storage.contains(d).await.unwrap()
    }

    #[tokio::test]
    async fn lru_evicts_the_cold_blob_and_keeps_the_hot_one() {
        // 200 bytes stored, budget 200 => high 180 (over), low 100: free 100.
        let h = harness("lru", |c| {
            c.eviction.policy = PolicyKind::Lru;
            c.storage.max_size = 200;
        })
        .await;

        let cold = put(&h, 1, 100).await;
        let hot = put(&h, 2, 100).await;
        // `cold` was written first, so without a read it would survive FIFO but
        // not LRU; the service read path records the touch, which makes it newest.
        h.db.touch_blobs(&[cold]);
        h.db.flush_access().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;

        let report = h.gc.run_once().await.unwrap();

        assert_eq!(report.evicted_entries, 1, "exactly one victim: {report}");
        assert_eq!(report.evicted_bytes, 100);
        assert_eq!(report.skipped_leased, 0);
        assert!(on_disk(&h, &cold).await, "recently read blob must survive");
        assert!(indexed(&h, &cold).await);
        assert!(!on_disk(&h, &hot).await, "untouched blob must be evicted");
        assert!(!indexed(&h, &hot).await);

        // Metrics mirror the report, and the gauges were refreshed.
        assert_eq!(h.metrics.evicted_entries_total.get(), 1);
        assert_eq!(h.metrics.evicted_bytes_total.get(), 100);
        assert_eq!(h.metrics.cas_entry_count.get(), 1);
        assert_eq!(h.metrics.cas_size_bytes.get(), 100);
    }

    #[tokio::test]
    async fn fifo_evicts_by_insertion_order_ignoring_reads() {
        let h = harness("fifo", |c| {
            c.eviction.policy = PolicyKind::Fifo;
            c.storage.max_size = 200;
        })
        .await;

        let first = put(&h, 1, 100).await;
        let second = put(&h, 2, 100).await;
        // A read must not save the oldest entry under FIFO.
        h.db.touch_blobs(&[first]);

        let report = h.gc.run_once().await.unwrap();

        assert_eq!(report.evicted_entries, 1, "{report}");
        assert!(!on_disk(&h, &first).await, "oldest blob must go first");
        assert!(!indexed(&h, &first).await);
        assert!(on_disk(&h, &second).await);
        assert!(indexed(&h, &second).await);
    }

    #[tokio::test]
    async fn ttl_expires_aged_entries_without_size_pressure() {
        let h = harness("ttl", |c| {
            c.eviction.policy = PolicyKind::Ttl;
            c.eviction.ttl = Some(Duration::from_millis(100));
            // Far below any watermark: this sweep is purely age driven.
            c.storage.max_size = 1 << 30;
        })
        .await;

        let aged = put(&h, 1, 100).await;
        h.db.put_action_result("repo-a", &aged, b"stale".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        let fresh = put(&h, 2, 100).await;
        h.db.put_action_result("repo-a", &fresh, b"fresh".to_vec()).await.unwrap();

        let report = h.gc.run_once().await.unwrap();

        assert_eq!(report.evicted_entries, 1, "only the aged blob expires: {report}");
        assert_eq!(report.evicted_bytes, 100);
        assert!(!on_disk(&h, &aged).await);
        assert!(!indexed(&h, &aged).await);
        assert!(on_disk(&h, &fresh).await);
        assert!(indexed(&h, &fresh).await);

        // The aged Action Cache row goes with it; the fresh one stays.
        assert_eq!(report.ac_evicted, 1);
        assert!(h.db.get_action_result("repo-a", &aged).await.unwrap().is_none());
        assert!(h.db.get_action_result("repo-a", &fresh).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn none_policy_evicts_nothing_even_far_over_budget() {
        let h = harness("none", |c| {
            c.eviction.policy = PolicyKind::None;
            c.storage.max_size = 100; // 10x over budget below.
        })
        .await;

        let mut blobs = Vec::new();
        for byte in 0..10u8 {
            blobs.push(put(&h, byte, 100).await);
        }

        let report = h.gc.run_once().await.unwrap();

        assert_eq!(report.evicted_entries, 0, "{report}");
        assert_eq!(report.evicted_bytes, 0);
        assert_eq!(report.ac_evicted, 0);
        for d in &blobs {
            assert!(on_disk(&h, d).await);
            assert!(indexed(&h, d).await);
        }
        assert_eq!(h.metrics.evicted_entries_total.get(), 0);
        // Gauges are still refreshed so operators can see the overflow.
        assert_eq!(h.metrics.cas_entry_count.get(), 10);
        assert_eq!(h.metrics.cas_size_bytes.get(), 1000);
    }

    #[tokio::test]
    async fn leased_blob_is_skipped_but_other_victims_are_evicted() {
        // 300 bytes stored, budget 300 => high 270 (over), low 150: free 150,
        // which spans the two oldest blobs.
        let h = harness("lease", |c| {
            c.eviction.policy = PolicyKind::Fifo;
            c.storage.max_size = 300;
        })
        .await;

        let pinned = put(&h, 1, 100).await;
        let victim = put(&h, 2, 100).await;
        let newest = put(&h, 3, 100).await;
        let lease = h.storage.lease(&pinned);

        let report = h.gc.run_once().await.unwrap();

        assert_eq!(report.skipped_leased, 1, "{report}");
        assert_eq!(report.evicted_entries, 1);
        assert_eq!(report.evicted_bytes, 100);
        // Doc §11: a blob under an active read survives, index row included.
        assert!(on_disk(&h, &pinned).await);
        assert!(indexed(&h, &pinned).await, "leased blob must stay in sqlite");
        assert!(!on_disk(&h, &victim).await);
        assert!(!indexed(&h, &victim).await);
        assert!(on_disk(&h, &newest).await);

        // The sweep left the cache below the high watermark, so nothing more
        // happens until new uploads push it back over.
        let idle = h.gc.run_once().await.unwrap();
        assert!(idle.is_empty(), "{idle}");

        // Once the reader is gone the blob is evictable like any other.
        drop(lease);
        put(&h, 4, 100).await;
        let report = h.gc.run_once().await.unwrap();
        assert_eq!(report.skipped_leased, 0, "{report}");
        assert!(report.evicted_entries >= 1, "{report}");
        assert!(!on_disk(&h, &pinned).await, "unleased blob is the oldest victim");
        assert!(!indexed(&h, &pinned).await);
    }

    #[tokio::test]
    async fn eviction_stops_at_the_low_watermark() {
        // 1000 bytes stored, budget 1000 => high 900 (over), low 500.
        let h = harness("watermark", |c| {
            c.eviction.policy = PolicyKind::Lru;
            c.storage.max_size = 1000;
        })
        .await;

        for byte in 0..10u8 {
            put(&h, byte, 100).await;
        }

        let report = h.gc.run_once().await.unwrap();

        assert_eq!(report.evicted_entries, 5, "must stop at the low watermark: {report}");
        assert_eq!(report.evicted_bytes, 500);
        let stats = h.db.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 5);
        assert_eq!(stats.cas_size_bytes, 500);

        // A second sweep is a no-op: 500 bytes is below the high watermark.
        let again = h.gc.run_once().await.unwrap();
        assert!(again.is_empty(), "{again}");
        assert_eq!(h.db.stats().await.unwrap().cas_entry_count, 5);
    }

    #[tokio::test]
    async fn blob_deleted_behind_gc_is_purged_from_the_index() {
        let h = harness("phantom", |c| {
            c.eviction.policy = PolicyKind::Lru;
            c.storage.max_size = 100; // 100 stored => high 90 (over), low 50.
        })
        .await;

        let ghost = put(&h, 7, 100).await;
        std::fs::remove_file(h.storage.path_for(&ghost)).unwrap();

        let report = h.gc.run_once().await.unwrap();

        // Nothing was unlinked by GC, but the stale row is gone.
        assert_eq!(report.evicted_entries, 0, "{report}");
        assert_eq!(report.evicted_bytes, 0);
        assert_eq!(report.skipped_leased, 0);
        assert!(!indexed(&h, &ghost).await);
        let stats = h.db.stats().await.unwrap();
        assert_eq!(stats.cas_entry_count, 0);
        assert_eq!(stats.cas_size_bytes, 0);
    }

    #[tokio::test]
    async fn run_loop_sweeps_then_stops_on_shutdown() {
        let h = harness("loop", |c| {
            c.eviction.policy = PolicyKind::Fifo;
            c.storage.max_size = 200;
            c.eviction.interval = Duration::from_millis(20);
        })
        .await;

        let oldest = put(&h, 1, 100).await;
        put(&h, 2, 100).await;

        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(h.gc.clone().run_loop(rx));
        // Let at least one tick fire.
        for _ in 0..100 {
            if !indexed(&h, &oldest).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!indexed(&h, &oldest).await, "loop must run a sweep");

        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task).await.expect("loop must stop").unwrap();
    }
}
