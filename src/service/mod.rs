//! The REAPI v2 cache surface (Doc §3, §4, §6, §8, §10).
//!
//! Four gRPC services share one [`ServiceContext`]:
//!
//! ```text
//! Capabilities                 what this server actually implements
//! ActionCache                  (instance_name, action digest) -> ActionResult
//! ContentAddressableStorage    batch blob transfer + presence checks
//! ByteStream                   streaming blob transfer
//! ```
//!
//! Two invariants shape the whole module:
//!
//! * The CAS is global and content addressed; `instance_name` namespaces only
//!   the Action Cache.
//! * The filesystem is the source of truth for blob presence. Every existence
//!   check is a `stat(2)` through [`CasStore`], never a SQLite lookup, so a
//!   stale index can never resurrect a deleted blob (Doc §9, §10).

mod action_cache;
mod bytestream;
mod capabilities;
mod cas;

pub use action_cache::ActionCacheSvc;
pub use bytestream::ByteStreamSvc;
pub use capabilities::CapabilitiesSvc;
pub use cas::CasSvc;

use std::sync::Arc;

use futures::stream::{StreamExt, TryStreamExt};
use tonic::{Code, Status};
use tracing::error;

use crate::config::Config;
use crate::db::Metadata;
use crate::digest::Digest;
use crate::observability::Metrics;
use crate::proto::RpcStatus;
use crate::proto::reapi::{Digest as ProtoDigest, digest_function};
use crate::storage::CasStore;

/// Upper bound on concurrent `stat(2)` calls issued by a single request. Keeps
/// a 10k-digest `FindMissingBlobs` from saturating the blocking IO pool.
const STAT_CONCURRENCY: usize = 64;

/// Shared dependencies of every gRPC handler.
pub struct ServiceContext {
    pub cfg: Arc<Config>,
    pub storage: Arc<CasStore>,
    pub db: Arc<Metadata>,
    pub metrics: Arc<Metrics>,
}

impl ServiceContext {
    /// Configured batch ceiling, as the `i64` the protocol uses.
    fn max_batch_bytes(&self) -> i64 {
        i64::try_from(self.cfg.server.max_batch_total_size_bytes).unwrap_or(i64::MAX)
    }
}

pub(crate) fn to_proto_digest(d: &Digest) -> ProtoDigest {
    ProtoDigest { hash: d.hex(), size_bytes: d.size_bytes }
}

/// Convert a wire digest, rejecting anything that is not a well formed SHA-256.
pub(crate) fn parse_digest(d: Option<&ProtoDigest>, field: &str) -> Result<Digest, Status> {
    let d = d.ok_or_else(|| Status::invalid_argument(format!("{field} is required")))?;
    Digest::from_hex(&d.hash, d.size_bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid {field}: {e}")))
}

/// This server implements SHA-256 only (Doc §4). An unset field means "the
/// default", which REAPI defines as SHA-256.
pub(crate) fn check_digest_function(value: i32) -> Result<(), Status> {
    match digest_function::Value::try_from(value) {
        Ok(digest_function::Value::Sha256 | digest_function::Value::Unknown) => Ok(()),
        Ok(other) => Err(Status::invalid_argument(format!(
            "digest function {} is not supported; this server implements SHA256 only",
            other.as_str_name()
        ))),
        Err(_) => Err(Status::invalid_argument(format!(
            "unknown digest function value {value}"
        ))),
    }
}

/// `stat(2)` a batch of digests with bounded concurrency. Results keep the
/// input order so callers can zip them back onto their requests.
pub(crate) async fn stat_many(
    storage: &CasStore,
    digests: &[Digest],
) -> Result<Vec<Option<u64>>, Status> {
    // The futures are materialised up front (no closure crosses the stream
    // combinator) so the resulting future stays `Send` for the gRPC handlers.
    let mut pending = Vec::with_capacity(digests.len());
    for digest in digests {
        pending.push(storage.stat(digest));
    }
    futures::stream::iter(pending)
        .buffered(STAT_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| internal("stat", e))
}

/// Log the cause, return an opaque `INTERNAL`: filesystem paths and SQL never
/// reach the client.
pub(crate) fn internal(operation: &str, err: impl std::fmt::Display) -> Status {
    error!(operation, error = %err, "request failed");
    Status::internal(format!("internal error during {operation}"))
}

/// Identity attached by the authentication interceptor (Doc §12), rendered as
/// `mode:id`. Requests that bypassed the interceptor (unit tests) report
/// `none:unauthenticated`.
pub(crate) fn principal_label<T>(req: &tonic::Request<T>) -> String {
    req.extensions()
        .get::<crate::auth::Principal>()
        .map_or_else(|| "none:unauthenticated".to_string(), |p| format!("{}:{}", p.mode, p.id))
}

pub(crate) fn rpc_ok() -> RpcStatus {
    RpcStatus { code: i32::from(Code::Ok), message: String::new(), details: Vec::new() }
}

pub(crate) fn rpc_status(code: Code, message: impl Into<String>) -> RpcStatus {
    RpcStatus { code: i32::from(code), message: message.into(), details: Vec::new() }
}

/// Canonical gRPC code name, as shown in the desktop traffic list.
pub(crate) fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// Request-level status name of a handler result.
pub(crate) fn result_status<T>(result: &Result<T, Status>) -> &'static str {
    result.as_ref().map_or_else(|status| code_name(status.code()), |_| "OK")
}

/// Summary of a batch's per-entry codes: `OK` when all succeeded, `PARTIAL`
/// when some did, otherwise the first failure's code. Also `(ok, total)`.
pub(crate) fn batch_summary(codes: impl Iterator<Item = i32>) -> (&'static str, usize, usize) {
    let (mut ok, mut total, mut first_error) = (0, 0, None);
    for code in codes {
        total += 1;
        if code == i32::from(Code::Ok) {
            ok += 1;
        } else if first_error.is_none() {
            first_error = Some(code_name(Code::from(code)));
        }
    }
    let status = match first_error {
        None => "OK",
        Some(_) if ok > 0 => "PARTIAL",
        Some(name) => name,
    };
    (status, ok, total)
}

pub(crate) fn digest_label(d: Option<&ProtoDigest>) -> String {
    d.map_or_else(String::new, |d| format!("{}/{}", d.hash, d.size_bytes))
}

/// Batch detail: the digest for a single entry, otherwise a count summary.
pub(crate) fn batch_detail(first: &str, ok: usize, total: usize) -> String {
    if total == 1 {
        first.to_string()
    } else {
        format!("{ok}/{total} blobs ok")
    }
}

impl ServiceContext {
    /// Report one finished request when desktop capture is on. `detail` is only
    /// built in that case, so the CLI path pays one `OnceLock` read. The request
    /// metadata keeps its original session and start time through completion.
    pub(crate) fn trace(
        &self,
        request: crate::traffic::RequestTrace,
        kind: crate::traffic::TrafficKind,
        method: &'static str,
        status: &'static str,
        bytes: u64,
        detail: impl FnOnce() -> String,
    ) {
        if let Some(log) = self.metrics.traffic() {
            log.record(request, kind, method, status, bytes, detail());
        }
    }

    pub(crate) fn tracing_traffic(&self) -> bool {
        self.metrics.traffic().is_some()
    }

    /// The captured TCP session a request arrived on, `None` when capture is
    /// off or the request has no mapped live connection.
    pub(crate) fn session_of<T>(&self, request: &tonic::Request<T>) -> Option<u64> {
        let log = self.metrics.traffic()?;
        log.session_for_peer(request.remote_addr()?)
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::ServiceContext;
    use crate::config::{Config, Durability};
    use crate::db::Metadata;
    use crate::observability::Metrics;
    use crate::storage::CasStore;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Self-cleaning scratch directory; no `tempfile` dependency needed.
    pub(crate) struct TempRoot {
        pub(crate) path: PathBuf,
    }

    impl TempRoot {
        pub(crate) fn new(tag: &str) -> TempRoot {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "bazelcache-service-{tag}-{}-{nanos}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            TempRoot { path }
        }

        pub(crate) fn cas(&self) -> PathBuf {
            self.path.join("cas")
        }

        pub(crate) fn tmp(&self) -> PathBuf {
            self.path.join("tmp")
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// A context backed by a real CAS directory and a real SQLite database.
    pub(crate) async fn context(root: &TempRoot) -> Arc<ServiceContext> {
        context_with(root, |_| {}).await
    }

    pub(crate) async fn context_with(
        root: &TempRoot,
        tweak: impl FnOnce(&mut Config),
    ) -> Arc<ServiceContext> {
        let mut cfg = Config::default();
        cfg.storage.root = root.path.clone();
        cfg.storage.durability = Durability::Cache;
        tweak(&mut cfg);

        let storage = CasStore::open(&root.cas(), &root.tmp(), cfg.storage.durability)
            .await
            .unwrap();
        let db = Metadata::open(&root.path.join("metadata.sqlite"), cfg.storage.durability)
            .await
            .unwrap();

        Arc::new(ServiceContext {
            cfg: Arc::new(cfg),
            storage,
            db,
            metrics: Metrics::new().unwrap(),
        })
    }

    /// Number of files currently sitting in `tmp/`.
    pub(crate) fn tmp_file_count(root: &TempRoot) -> usize {
        std::fs::read_dir(root.tmp()).map(|d| d.count()).unwrap_or(0)
    }

    /// Number of published CAS blobs.
    pub(crate) fn cas_file_count(root: &TempRoot) -> usize {
        let mut n = 0;
        let Ok(shards) = std::fs::read_dir(root.cas()) else {
            return 0;
        };
        for shard in shards.flatten() {
            if let Ok(entries) = std::fs::read_dir(shard.path()) {
                n += entries.count();
            }
        }
        n
    }
}

#[cfg(test)]
mod traffic_tests {
    use super::*;

    #[test]
    fn batch_status_distinguishes_ok_partial_and_failed() {
        let ok = i32::from(Code::Ok);
        let missing = i32::from(Code::NotFound);
        assert_eq!(batch_summary([ok, ok].into_iter()), ("OK", 2, 2));
        assert_eq!(batch_summary([ok, missing].into_iter()), ("PARTIAL", 1, 2));
        assert_eq!(batch_summary([missing, missing].into_iter()), ("NOT_FOUND", 0, 2));
    }
}
