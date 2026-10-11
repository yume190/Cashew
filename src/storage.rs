//! Filesystem CAS: the source of truth for blob payloads (Doc §5, §7, §8, §9, §11).
//!
//! Layout:
//!
//! ```text
//! <root>/cas/<first-hex-byte>/<64-hex-hash>   published, immutable blobs
//! <root>/tmp/<random>.part                    in-flight uploads
//! ```
//!
//! Invariants enforced here:
//!
//! * Writers only ever touch `tmp/`; a blob appears in `cas/` by `rename(2)`,
//!   so a reader never observes a partial payload.
//! * Publication is *publish-if-absent*: CAS is content addressed and
//!   immutable, so a duplicate upload drops its temp file and reports success.
//! * Nothing is published before its size and SHA-256 have been verified.
//! * Garbage collection does not unlink a blob while a `ReadLease` is
//!   outstanding for it (runtime-only registry, Doc §11).

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWriteExt, BufWriter, ReadBuf};
use tracing::warn;

use crate::config::Durability;
use crate::digest::{Digest, HASH_LEN, HEX_LEN};

/// SHA-256 of the empty input. Bazel asks for this digest constantly; it is
/// never materialised on disk.
const EMPTY_SHA256: [u8; HASH_LEN] = [
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
    0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
];

/// Number of per-digest publish locks. Keyed by the first hash byte, i.e. the
/// same sharding the directory layout uses. The critical section is two
/// syscalls, so this is "per-digest coordination", not a global upload lock.
const PUBLISH_SHARDS: usize = 256;

/// Temp-file write buffer. Large enough that gRPC-sized chunks go straight to
/// the kernel, small enough that RAM does not scale with blob size.
const WRITE_BUF_BYTES: usize = 128 * 1024;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("digest mismatch: expected {expected}, computed {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("size mismatch: expected {expected} bytes, received {actual}")]
    SizeMismatch { expected: i64, actual: u64 },
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Result of publishing a verified blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// This call moved the blob into the CAS.
    Published { size_bytes: u64 },
    /// An identical blob was already present; the upload was discarded.
    AlreadyPresent { size_bytes: u64 },
}

impl WriteOutcome {
    pub fn size_bytes(&self) -> u64 {
        match self {
            WriteOutcome::Published { size_bytes } | WriteOutcome::AlreadyPresent { size_bytes } => {
                *size_bytes
            }
        }
    }

    pub fn is_published(&self) -> bool {
        matches!(self, WriteOutcome::Published { .. })
    }
}

/// One entry produced by [`CasStore::scan_blobs`].
#[derive(Debug, Clone)]
pub struct ScannedBlob {
    pub digest: Digest,
    pub size_bytes: u64,
    pub created_at_micros: i64,
}

/// Shared state. Held behind an `Arc` so writers and leases can outlive the
/// borrow of the store without a self-referential handle.
struct Inner {
    cas_dir: PathBuf,
    tmp_dir: PathBuf,
    durability: Durability,
    /// Digests with at least one in-flight reader. Runtime-only (Doc §11).
    leases: Mutex<HashMap<[u8; HASH_LEN], usize>>,
    publish_locks: Vec<tokio::sync::Mutex<()>>,
}

/// Content addressed blob store backed by the filesystem.
pub struct CasStore {
    inner: Arc<Inner>,
}

fn is_empty_digest(d: &Digest) -> bool {
    d.size_bytes == 0 && d.hash == EMPTY_SHA256
}

fn random_token() -> String {
    let hi: u64 = rand::random();
    let lo: u64 = rand::random();
    format!("{hi:016x}{lo:016x}")
}

/// `ctime` is the publication time of a CAS entry: `rename(2)` into the CAS
/// updates it, and it cannot be back-dated by a client-supplied mtime.
fn created_at_micros(meta: &std::fs::Metadata) -> i64 {
    let secs = meta.ctime();
    let nanos = meta.ctime_nsec();
    if secs > 0 {
        return secs.saturating_mul(1_000_000).saturating_add(nanos / 1_000);
    }
    meta.mtime()
        .saturating_mul(1_000_000)
        .saturating_add(meta.mtime_nsec() / 1_000)
}

async fn fsync_dir(path: &Path) -> io::Result<()> {
    let dir = fs::File::open(path).await?;
    dir.sync_all().await
}

impl Inner {
    fn path_for(&self, d: &Digest) -> PathBuf {
        self.cas_dir.join(d.shard()).join(d.hex())
    }

    /// Move a fully verified temp file into the CAS.
    ///
    /// The per-shard lock makes the "does it already exist" check and the
    /// rename atomic with respect to other writers in this process, so a
    /// duplicate upload reports `AlreadyPresent` instead of re-publishing.
    async fn publish(
        &self,
        tmp_path: &Path,
        expected: &Digest,
        written: u64,
    ) -> io::Result<WriteOutcome> {
        let final_path = self.path_for(expected);
        let _guard = self.publish_locks[expected.hash[0] as usize].lock().await;

        match fs::metadata(&final_path).await {
            Ok(_) => {
                let _ = fs::remove_file(tmp_path).await;
                return Ok(WriteOutcome::AlreadyPresent {
                    size_bytes: written,
                });
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let shard_dir = final_path
            .parent()
            .expect("CAS path always has a shard directory")
            .to_path_buf();
        fs::create_dir_all(&shard_dir).await?;
        fs::rename(tmp_path, &final_path).await?;

        if self.durability == Durability::Strict {
            fsync_dir(&shard_dir).await?;
        }
        Ok(WriteOutcome::Published {
            size_bytes: written,
        })
    }

    /// Create `<tmp>/<random>.part`, retrying on the (astronomically unlikely)
    /// name collision. `create_new` keeps this race-free.
    async fn create_temp(&self) -> io::Result<(PathBuf, fs::File)> {
        for _ in 0..16 {
            let path = self.tmp_dir.join(format!("{}.part", random_token()));
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
            {
                Ok(f) => return Ok((path, f)),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(io::Error::other("could not allocate a unique temp file name"))
    }
}

impl CasStore {
    /// Open (creating if needed) the CAS and temp directories.
    ///
    /// Doc §7 forbids splitting them across volumes: publication relies on
    /// `rename(2)` being atomic, which only holds within one filesystem.
    pub async fn open(
        cas_dir: &Path,
        tmp_dir: &Path,
        durability: Durability,
    ) -> io::Result<Arc<CasStore>> {
        fs::create_dir_all(cas_dir).await?;
        fs::create_dir_all(tmp_dir).await?;

        let cas_meta = fs::metadata(cas_dir).await?;
        let tmp_meta = fs::metadata(tmp_dir).await?;
        if cas_meta.dev() != tmp_meta.dev() {
            return Err(io::Error::other(format!(
                "cas dir {} (dev {}) and tmp dir {} (dev {}) are on different filesystems; \
                 atomic publish requires one volume",
                cas_dir.display(),
                cas_meta.dev(),
                tmp_dir.display(),
                tmp_meta.dev()
            )));
        }

        let mut publish_locks = Vec::with_capacity(PUBLISH_SHARDS);
        publish_locks.resize_with(PUBLISH_SHARDS, Default::default);

        Ok(Arc::new(CasStore {
            inner: Arc::new(Inner {
                cas_dir: cas_dir.to_path_buf(),
                tmp_dir: tmp_dir.to_path_buf(),
                durability,
                leases: Mutex::new(HashMap::new()),
                publish_locks,
            }),
        }))
    }

    /// `<cas_dir>/<shard>/<hex>`.
    pub fn path_for(&self, d: &Digest) -> PathBuf {
        self.inner.path_for(d)
    }

    /// Size of a published blob, or `None` if absent.
    pub async fn stat(&self, d: &Digest) -> io::Result<Option<u64>> {
        if is_empty_digest(d) {
            return Ok(Some(0));
        }
        match fs::metadata(self.path_for(d)).await {
            Ok(m) => Ok(Some(m.len())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Presence predicate. Handlers call [`CasStore::stat`] directly because they
    /// also need the size; this stays for assertions.
    #[cfg(test)]
    pub async fn contains(&self, d: &Digest) -> io::Result<bool> {
        Ok(self.stat(d).await?.is_some())
    }

    /// Whole-blob read. Bounded paths only (BatchReadBlobs, ActionResult
    /// inlining); streaming consumers use [`CasStore::open_read`].
    pub async fn read_all(&self, d: &Digest) -> io::Result<Option<Vec<u8>>> {
        if is_empty_digest(d) {
            return Ok(Some(Vec::new()));
        }
        let _lease = self.lease(d);
        match fs::read(self.path_for(d)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Open a blob for streaming at `offset`, reading at most `limit` bytes.
    ///
    /// The returned reader holds a [`ReadLease`], so GC will not unlink the
    /// blob underneath it.
    pub async fn open_read(
        &self,
        d: &Digest,
        offset: u64,
        limit: Option<u64>,
    ) -> io::Result<Option<BlobReader>> {
        // Take the lease before touching the filesystem so that a concurrent
        // GC pass cannot decide to delete between our stat and our open.
        let lease = self.lease(d);

        if is_empty_digest(d) {
            if offset > 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("read offset {offset} beyond end of empty blob"),
                ));
            }
            return Ok(Some(BlobReader {
                file: None,
                remaining: 0,
                _lease: lease,
            }));
        }

        let path = self.path_for(d);
        let mut file = match fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let len = file.metadata().await?.len();
        if offset > len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("read offset {offset} beyond blob length {len}"),
            ));
        }
        if offset > 0 {
            file.seek(io::SeekFrom::Start(offset)).await?;
        }
        let mut remaining = len - offset;
        if let Some(limit) = limit {
            remaining = remaining.min(limit);
        }
        Ok(Some(BlobReader {
            file: Some(file),
            remaining,
            _lease: lease,
        }))
    }

    /// Verify and publish an in-memory blob (BatchUpdateBlobs). Still goes
    /// through tmp + rename so readers never see a partial file.
    pub async fn put_bytes(
        &self,
        expected: &Digest,
        data: &[u8],
    ) -> Result<WriteOutcome, StorageError> {
        let actual = Digest::of(data);
        if actual.size_bytes != expected.size_bytes {
            return Err(StorageError::SizeMismatch {
                expected: expected.size_bytes,
                actual: data.len() as u64,
            });
        }
        if actual.hash != expected.hash {
            return Err(StorageError::DigestMismatch {
                expected: expected.hex(),
                actual: actual.hex(),
            });
        }
        if is_empty_digest(expected) {
            return Ok(WriteOutcome::AlreadyPresent { size_bytes: 0 });
        }

        let (tmp_path, mut file) = self.inner.create_temp().await?;
        let written = data.len() as u64;
        // Hash is already verified above; write straight through.
        let staged = async {
            file.write_all(data).await?;
            file.flush().await?;
            if self.inner.durability == Durability::Strict {
                file.sync_all().await?;
            }
            Ok::<(), io::Error>(())
        }
        .await;
        drop(file);
        if let Err(e) = staged {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(StorageError::Io(e));
        }

        match self.inner.publish(&tmp_path, expected, written).await {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                let _ = fs::remove_file(&tmp_path).await;
                Err(StorageError::Io(e))
            }
        }
    }

    /// Start a streaming upload into `tmp/`.
    pub async fn begin_write(&self) -> io::Result<BlobWriter> {
        let (tmp_path, file) = self.inner.create_temp().await?;
        Ok(BlobWriter {
            inner: Arc::clone(&self.inner),
            tmp_path: Some(tmp_path),
            file: Some(BufWriter::with_capacity(WRITE_BUF_BYTES, file)),
            hasher: Sha256::new(),
            written: 0,
        })
    }

    /// Unlink a published blob. Returns `false` if it was absent or if a
    /// reader currently holds a lease on it (GC must not pull the rug out from
    /// under an active read, Doc §11).
    pub async fn remove(&self, d: &Digest) -> io::Result<bool> {
        if is_empty_digest(d) {
            return Ok(false);
        }
        let path = self.path_for(d);
        // The lease table is held across the unlink so that a reader cannot
        // acquire a lease for a blob we are already deleting. Both the lock
        // hold and the syscall are short; no `.await` happens in between.
        let guard = self.inner.leases.lock().expect("lease registry poisoned");
        if guard.contains_key(&d.hash) {
            return Ok(false);
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                drop(guard);
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                drop(guard);
                Ok(false)
            }
            Err(e) => {
                drop(guard);
                Err(e)
            }
        }
    }

    /// Delete leftover `*.part` files from uploads interrupted by a crash
    /// (Doc §9). Returns the number removed.
    pub async fn cleanup_tmp(&self) -> io::Result<usize> {
        let mut removed = 0usize;
        let mut dir = match fs::read_dir(&self.inner.tmp_dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.ends_with(".part") {
                continue;
            }
            match fs::remove_file(entry.path()).await {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(path = %entry.path().display(), error = %e, "could not remove temp file");
                }
            }
        }
        Ok(removed)
    }

    /// Walk every published blob, streaming metadata only — payloads are never
    /// read. Used to rebuild the SQLite index (Doc §9, §21.D).
    pub async fn scan_blobs<F: FnMut(ScannedBlob)>(&self, mut f: F) -> io::Result<()> {
        let mut shards = match fs::read_dir(&self.inner.cas_dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        while let Some(shard) = shards.next_entry().await? {
            if !shard.file_type().await?.is_dir() {
                warn!(path = %shard.path().display(), "unexpected file in cas root, skipping");
                continue;
            }
            let shard_name = shard.file_name();
            let Some(shard_name) = shard_name.to_str().map(str::to_owned) else {
                warn!(path = %shard.path().display(), "non-utf8 shard directory, skipping");
                continue;
            };
            let mut entries = fs::read_dir(shard.path()).await?;
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    warn!(path = %entry.path().display(), "non-utf8 cas entry, skipping");
                    continue;
                };
                if name.len() != HEX_LEN {
                    warn!(path = %entry.path().display(), "cas entry name is not a sha-256, skipping");
                    continue;
                }
                if !name.starts_with(&shard_name) {
                    warn!(path = %entry.path().display(), "cas entry stored in the wrong shard, skipping");
                    continue;
                }
                let meta = match entry.metadata().await {
                    Ok(m) => m,
                    // Raced with eviction; nothing to report.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                if !meta.is_file() {
                    warn!(path = %entry.path().display(), "cas entry is not a regular file, skipping");
                    continue;
                }
                let size = meta.len();
                let digest = match Digest::from_hex(name, size as i64) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(path = %entry.path().display(), error = %e, "cas entry name is not a sha-256, skipping");
                        continue;
                    }
                };
                f(ScannedBlob {
                    digest,
                    size_bytes: size,
                    created_at_micros: created_at_micros(&meta),
                });
            }
        }
        Ok(())
    }

    /// Pin a digest against eviction for as long as the guard lives.
    pub fn lease(&self, d: &Digest) -> ReadLease {
        {
            let mut guard = self.inner.leases.lock().expect("lease registry poisoned");
            *guard.entry(d.hash).or_insert(0) += 1;
        }
        ReadLease {
            inner: Arc::clone(&self.inner),
            hash: d.hash,
        }
    }

    /// Readiness probe: prove the data volume still accepts writes.
    pub async fn writable(&self) -> io::Result<()> {
        let path = self
            .inner
            .tmp_dir
            .join(format!(".probe-{}", random_token()));
        let result = async {
            fs::write(&path, b"ok").await?;
            Ok::<(), io::Error>(())
        }
        .await;
        let _ = fs::remove_file(&path).await;
        result
    }

    #[cfg(test)]
    fn lease_count(&self, d: &Digest) -> usize {
        self.inner
            .leases
            .lock()
            .expect("lease registry poisoned")
            .get(&d.hash)
            .copied()
            .unwrap_or(0)
    }
}

/// RAII guard keeping a digest alive against GC.
pub struct ReadLease {
    inner: Arc<Inner>,
    hash: [u8; HASH_LEN],
}

impl std::fmt::Debug for ReadLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadLease").field("digest", &hex::encode(self.hash)).finish()
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        let mut guard = match self.inner.leases.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Entry::Occupied(mut e) = guard.entry(self.hash) {
            let n = e.get_mut();
            *n -= 1;
            if *n == 0 {
                e.remove();
            }
        }
    }
}

/// Streaming reader over a published blob, bounded by an optional limit.
pub struct BlobReader {
    file: Option<fs::File>,
    remaining: u64,
    _lease: ReadLease,
}

impl std::fmt::Debug for BlobReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReader")
            .field("remaining", &self.remaining)
            .field("lease", &self._lease)
            .finish()
    }
}

impl BlobReader {
    /// Bytes still available from this reader.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl AsyncRead for BlobReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if me.remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        let Some(file) = me.file.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let want = (buf.remaining() as u64).min(me.remaining) as usize;
        if want == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut limited = buf.take(want);
        match Pin::new(file).poll_read(cx, &mut limited) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {
                let filled = limited.filled().len();
                if filled == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "cas blob is shorter than its recorded length",
                    )));
                }
                // SAFETY: `limited` borrowed `buf`'s unfilled region and the
                // inner reader initialised and filled `filled` bytes of it.
                unsafe { buf.assume_init(filled) };
                buf.advance(filled);
                me.remaining -= filled as u64;
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// Streaming upload: writes to `tmp/`, hashes as it goes, publishes on commit.
pub struct BlobWriter {
    inner: Arc<Inner>,
    tmp_path: Option<PathBuf>,
    file: Option<BufWriter<fs::File>>,
    hasher: Sha256,
    written: u64,
}

impl BlobWriter {
    pub async fn write_chunk(&mut self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("blob writer already finished"))?;
        file.write_all(buf).await?;
        self.hasher.update(buf);
        self.written += buf.len() as u64;
        Ok(())
    }

    /// Bytes accepted so far. The ByteStream handler tracks its own offset, so
    /// this is only read by assertions.
    #[cfg(test)]
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Verify against `expected`, then publish. On any mismatch the temp file
    /// is unlinked and nothing enters the CAS.
    pub async fn commit(mut self, expected: &Digest) -> Result<WriteOutcome, StorageError> {
        let tmp_path = match self.tmp_path.take() {
            Some(p) => p,
            None => return Err(StorageError::Io(io::Error::other("blob writer already finished"))),
        };
        let written = self.written;
        let actual: [u8; HASH_LEN] = std::mem::replace(&mut self.hasher, Sha256::new())
            .finalize()
            .into();

        // Flush before verifying so a late write error surfaces, but report
        // verification failures first: they are the client's fault.
        let flushed = match self.file.take() {
            Some(mut file) => {
                async {
                    file.flush().await?;
                    let file = file.into_inner();
                    if self.inner.durability == Durability::Strict {
                        file.sync_all().await?;
                    }
                    Ok(())
                }
                .await
            }
            None => Err(io::Error::other("blob writer already finished")),
        };

        if written != expected.size_bytes as u64 {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(StorageError::SizeMismatch {
                expected: expected.size_bytes,
                actual: written,
            });
        }
        if actual != expected.hash {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(StorageError::DigestMismatch {
                expected: expected.hex(),
                actual: hex::encode(actual),
            });
        }
        if let Err(e) = flushed {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(StorageError::Io(e));
        }
        // A verified empty payload is never materialised on disk.
        if is_empty_digest(expected) {
            let _ = fs::remove_file(&tmp_path).await;
            return Ok(WriteOutcome::AlreadyPresent { size_bytes: 0 });
        }

        match self.inner.publish(&tmp_path, expected, written).await {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                let _ = fs::remove_file(&tmp_path).await;
                Err(StorageError::Io(e))
            }
        }
    }

    /// Discard the upload; the temp file never reaches the CAS.
    pub async fn abort(mut self) {
        self.file.take();
        if let Some(path) = self.tmp_path.take() {
            let _ = fs::remove_file(&path).await;
        }
    }
}

impl Drop for BlobWriter {
    fn drop(&mut self) {
        // Dropped without commit/abort (cancelled RPC, panic): make sure the
        // partial upload does not linger until the next startup sweep.
        if let Some(path) = self.tmp_path.take() {
            self.file.take();
            let _ = std::fs::remove_file(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::AsyncReadExt;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct TempRoot {
        path: PathBuf,
    }

    impl TempRoot {
        fn new(tag: &str) -> TempRoot {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "bazelcache-storage-{tag}-{}-{nanos}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            TempRoot { path }
        }

        fn cas(&self) -> PathBuf {
            self.path.join("cas")
        }

        fn tmp(&self) -> PathBuf {
            self.path.join("tmp")
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    async fn store(root: &TempRoot) -> Arc<CasStore> {
        CasStore::open(&root.cas(), &root.tmp(), Durability::Cache)
            .await
            .unwrap()
    }

    fn tmp_entries(root: &TempRoot) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(root.tmp())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    fn cas_files(root: &TempRoot) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(shards) = std::fs::read_dir(root.cas()) else {
            return out;
        };
        for shard in shards {
            let shard = shard.unwrap();
            if !shard.file_type().unwrap().is_dir() {
                continue;
            }
            for e in std::fs::read_dir(shard.path()).unwrap() {
                out.push(e.unwrap().path());
            }
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn empty_digest_constant_matches_sha256_of_nothing() {
        assert_eq!(Digest::of(b"").hash, EMPTY_SHA256);
    }

    #[tokio::test]
    async fn digest_mismatch_rejected_and_temp_removed() {
        let root = TempRoot::new("digest-mismatch");
        let s = store(&root).await;

        // Claim the digest of "hello" but upload "world!" (same length).
        let lie = Digest::of(b"hello");
        assert_eq!(lie.size_bytes, 5);
        let mut w = s.begin_write().await.unwrap();
        w.write_chunk(b"world").await.unwrap();
        let err = w.commit(&lie).await.unwrap_err();
        match err {
            StorageError::DigestMismatch { expected, actual } => {
                assert_eq!(expected, lie.hex());
                assert_eq!(actual, Digest::of(b"world").hex());
            }
            other => panic!("expected DigestMismatch, got {other:?}"),
        }

        assert!(tmp_entries(&root).is_empty(), "temp file leaked");
        assert!(cas_files(&root).is_empty(), "unverified blob entered the CAS");
        assert!(!s.contains(&lie).await.unwrap());

        // put_bytes must behave identically and never touch the disk.
        let err = s.put_bytes(&lie, b"world").await.unwrap_err();
        assert!(matches!(err, StorageError::DigestMismatch { .. }));
        assert!(tmp_entries(&root).is_empty());
        assert!(cas_files(&root).is_empty());
    }

    #[tokio::test]
    async fn size_mismatch_rejected() {
        let root = TempRoot::new("size-mismatch");
        let s = store(&root).await;

        let payload = b"the quick brown fox";
        let mut claimed = Digest::of(payload);
        claimed.size_bytes += 7;

        let mut w = s.begin_write().await.unwrap();
        w.write_chunk(payload).await.unwrap();
        assert_eq!(w.written(), payload.len() as u64);
        let err = w.commit(&claimed).await.unwrap_err();
        match err {
            StorageError::SizeMismatch { expected, actual } => {
                assert_eq!(expected, payload.len() as i64 + 7);
                assert_eq!(actual, payload.len() as u64);
            }
            other => panic!("expected SizeMismatch, got {other:?}"),
        }
        assert!(tmp_entries(&root).is_empty());
        assert!(cas_files(&root).is_empty());

        let err = s.put_bytes(&claimed, payload).await.unwrap_err();
        assert!(matches!(err, StorageError::SizeMismatch { .. }));
    }

    #[tokio::test]
    async fn duplicate_upload_publishes_once() {
        let root = TempRoot::new("duplicate");
        let s = store(&root).await;
        let payload = vec![7u8; 64 * 1024];
        let d = Digest::of(&payload);

        // Two writers stream the same blob concurrently, then both commit.
        let mut a = s.begin_write().await.unwrap();
        let mut b = s.begin_write().await.unwrap();
        for chunk in payload.chunks(4096) {
            let (ra, rb) = tokio::join!(a.write_chunk(chunk), b.write_chunk(chunk));
            ra.unwrap();
            rb.unwrap();
        }
        assert_eq!(tmp_entries(&root).len(), 2);

        let (ra, rb) = tokio::join!(a.commit(&d), b.commit(&d));
        let (ra, rb) = (ra.unwrap(), rb.unwrap());

        let published = [ra, rb].iter().filter(|o| o.is_published()).count();
        assert_eq!(published, 1, "exactly one writer publishes: {ra:?} {rb:?}");
        assert_eq!(ra.size_bytes(), payload.len() as u64);
        assert_eq!(rb.size_bytes(), payload.len() as u64);

        assert_eq!(cas_files(&root).len(), 1);
        assert!(tmp_entries(&root).is_empty(), "loser leaked its temp file");
        assert_eq!(s.read_all(&d).await.unwrap().unwrap(), payload);

        // A third, sequential upload of the same content is also a no-op.
        let out = s.put_bytes(&d, &payload).await.unwrap();
        assert_eq!(out, WriteOutcome::AlreadyPresent { size_bytes: payload.len() as u64 });
        assert_eq!(cas_files(&root).len(), 1);
    }

    #[tokio::test]
    async fn reader_never_observes_partial_blob() {
        let root = TempRoot::new("partial");
        let s = store(&root).await;
        let payload = vec![3u8; 40_000];
        let d = Digest::of(&payload);

        let mut w = s.begin_write().await.unwrap();
        w.write_chunk(&payload[..20_000]).await.unwrap();

        // Half the bytes are on disk, but only in tmp/.
        assert_eq!(s.stat(&d).await.unwrap(), None);
        assert!(!s.contains(&d).await.unwrap());
        assert!(s.open_read(&d, 0, None).await.unwrap().is_none());
        assert!(s.read_all(&d).await.unwrap().is_none());
        assert!(cas_files(&root).is_empty());

        w.write_chunk(&payload[20_000..]).await.unwrap();
        assert!(w.commit(&d).await.unwrap().is_published());

        assert_eq!(s.stat(&d).await.unwrap(), Some(40_000));
        let mut r = s.open_read(&d, 0, None).await.unwrap().unwrap();
        assert_eq!(r.remaining(), 40_000);
        let mut got = Vec::new();
        r.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, payload);
        assert_eq!(r.remaining(), 0);
    }

    #[tokio::test]
    async fn dropped_writer_does_not_leak_temp_file() {
        let root = TempRoot::new("dropped");
        let s = store(&root).await;

        let mut w = s.begin_write().await.unwrap();
        w.write_chunk(b"interrupted").await.unwrap();
        drop(w);
        assert!(tmp_entries(&root).is_empty());

        let mut w = s.begin_write().await.unwrap();
        w.write_chunk(b"aborted").await.unwrap();
        w.abort().await;
        assert!(tmp_entries(&root).is_empty());
        assert!(cas_files(&root).is_empty());
    }

    #[tokio::test]
    async fn open_read_honours_offset_and_limit() {
        let root = TempRoot::new("offset");
        let s = store(&root).await;
        let payload: Vec<u8> = (0..=255u8).cycle().take(5000).collect();
        let d = Digest::of(&payload);
        s.put_bytes(&d, &payload).await.unwrap();

        let mut r = s.open_read(&d, 1000, Some(500)).await.unwrap().unwrap();
        assert_eq!(r.remaining(), 500);
        let mut got = Vec::new();
        r.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, &payload[1000..1500]);

        // limit longer than the tail clamps to the tail.
        let mut r = s.open_read(&d, 4900, Some(9999)).await.unwrap().unwrap();
        assert_eq!(r.remaining(), 100);
        let mut got = Vec::new();
        r.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, &payload[4900..]);

        // offset == len is an empty read, offset > len is invalid.
        let r = s.open_read(&d, 5000, None).await.unwrap().unwrap();
        assert_eq!(r.remaining(), 0);
        let err = s.open_read(&d, 5001, None).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn remove_is_blocked_by_outstanding_lease() {
        let root = TempRoot::new("lease");
        let s = store(&root).await;
        let payload = b"evict me later".to_vec();
        let d = Digest::of(&payload);
        s.put_bytes(&d, &payload).await.unwrap();

        let reader = s.open_read(&d, 0, None).await.unwrap().unwrap();
        assert_eq!(s.lease_count(&d), 1);
        assert!(!s.remove(&d).await.unwrap(), "GC deleted a leased blob");
        assert!(s.contains(&d).await.unwrap());

        // Nested leases refcount rather than clobber each other.
        let extra = s.lease(&d);
        assert_eq!(s.lease_count(&d), 2);
        drop(extra);
        assert_eq!(s.lease_count(&d), 1);
        assert!(!s.remove(&d).await.unwrap());

        drop(reader);
        assert_eq!(s.lease_count(&d), 0);
        assert!(s.remove(&d).await.unwrap());
        assert!(!s.contains(&d).await.unwrap());
        assert!(!s.remove(&d).await.unwrap(), "second remove reports absent");
    }

    #[tokio::test]
    async fn cleanup_tmp_removes_crash_leftovers() {
        let root = TempRoot::new("cleanup");
        let s = store(&root).await;

        std::fs::write(root.tmp().join("aaaa.part"), b"stale").unwrap();
        std::fs::write(root.tmp().join("bbbb.part"), b"stale").unwrap();
        std::fs::write(root.tmp().join("keep.txt"), b"not an upload").unwrap();

        assert_eq!(s.cleanup_tmp().await.unwrap(), 2);
        assert_eq!(tmp_entries(&root), vec!["keep.txt".to_string()]);
        assert_eq!(s.cleanup_tmp().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn scan_blobs_round_trips_published_blobs() {
        let root = TempRoot::new("scan");
        let s = store(&root).await;

        let mut expected = Vec::new();
        for i in 0..8u8 {
            let payload = vec![i; 100 + i as usize];
            let d = Digest::of(&payload);
            s.put_bytes(&d, &payload).await.unwrap();
            expected.push((d, payload.len() as u64));
        }
        expected.sort_by_key(|(d, _)| d.hash);

        // Garbage that must be skipped rather than indexed.
        std::fs::create_dir_all(root.cas().join("zz")).unwrap();
        std::fs::write(root.cas().join("zz").join("not-a-hash"), b"junk").unwrap();
        let shard = expected[0].0.shard();
        std::fs::write(root.cas().join(&shard).join("short"), b"junk").unwrap();

        let mut seen = Vec::new();
        s.scan_blobs(|b| seen.push(b)).await.unwrap();
        seen.sort_by_key(|b| b.digest.hash);

        assert_eq!(seen.len(), expected.len());
        for (got, (d, size)) in seen.iter().zip(expected.iter()) {
            assert_eq!(got.digest, *d);
            assert_eq!(got.size_bytes, *size);
            assert_eq!(got.digest.size_bytes, *size as i64);
            assert!(got.created_at_micros > 0, "ctime must be populated");
        }
    }

    #[tokio::test]
    async fn empty_blob_is_virtual() {
        let root = TempRoot::new("empty");
        let s = store(&root).await;
        let empty = Digest::of(b"");

        assert_eq!(s.stat(&empty).await.unwrap(), Some(0));
        assert!(s.contains(&empty).await.unwrap());
        assert_eq!(s.read_all(&empty).await.unwrap(), Some(Vec::new()));

        let mut r = s.open_read(&empty, 0, None).await.unwrap().unwrap();
        assert_eq!(r.remaining(), 0);
        let mut got = Vec::new();
        r.read_to_end(&mut got).await.unwrap();
        assert!(got.is_empty());
        assert_eq!(
            s.open_read(&empty, 1, None).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        assert_eq!(
            s.put_bytes(&empty, b"").await.unwrap(),
            WriteOutcome::AlreadyPresent { size_bytes: 0 }
        );
        let w = s.begin_write().await.unwrap();
        assert_eq!(
            w.commit(&empty).await.unwrap(),
            WriteOutcome::AlreadyPresent { size_bytes: 0 }
        );

        // Nothing was written, nothing can be evicted.
        assert!(cas_files(&root).is_empty());
        assert!(tmp_entries(&root).is_empty());
        assert!(!s.remove(&empty).await.unwrap());

        let mut seen = 0;
        s.scan_blobs(|_| seen += 1).await.unwrap();
        assert_eq!(seen, 0);
    }

    #[tokio::test]
    async fn strict_durability_publishes() {
        let root = TempRoot::new("strict");
        let s = CasStore::open(&root.cas(), &root.tmp(), Durability::Strict)
            .await
            .unwrap();
        let payload = vec![9u8; 8192];
        let d = Digest::of(&payload);

        let mut w = s.begin_write().await.unwrap();
        w.write_chunk(&payload).await.unwrap();
        assert!(w.commit(&d).await.unwrap().is_published());
        assert_eq!(s.read_all(&d).await.unwrap().unwrap(), payload);
        assert!(tmp_entries(&root).is_empty());
    }

    #[tokio::test]
    async fn open_creates_dirs_and_probe_round_trips() {
        let root = TempRoot::new("open");
        let s = store(&root).await;
        assert!(root.cas().is_dir());
        assert!(root.tmp().is_dir());
        s.writable().await.unwrap();
        assert!(tmp_entries(&root).is_empty(), "probe file leaked");
    }

    #[tokio::test]
    async fn path_for_shards_by_first_byte() {
        let root = TempRoot::new("path");
        let s = store(&root).await;
        let d = Digest::of(b"shard me");
        assert_eq!(s.path_for(&d), root.cas().join(d.shard()).join(d.hex()));
        assert_eq!(d.shard().len(), 2);
    }
}
