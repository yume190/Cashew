//! Observability: structured logging, Prometheus metrics, and the HTTP
//! side-channel (`/metrics`, `/health`, `/ready`).
//!
//! The gRPC surface is the product; this HTTP listener is operational only and
//! is never authenticated — it is expected to be bound to a private address
//! (see `[metrics] listen` in the configuration).

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntGauge, Opts, Registry,
    TextEncoder, exponential_buckets,
};
use tracing_subscriber::filter::{Directive, EnvFilter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::{LogFormat, LoggingConfig};
use crate::db::{Metadata, Stats};
use crate::storage::CasStore;

/// Prefix applied to every exported metric name.
const NS: &str = "bazel_cache";

/// How long a successful readiness probe is trusted before re-probing.
const READY_CACHE_TTL_MS: u64 = 2_000;

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// Install the global `tracing` subscriber.
///
/// The filter is `RUST_LOG` when set, otherwise `cfg.level` acts as the default
/// directive. Calling this twice (or after another subscriber was installed)
/// returns an error instead of panicking, which keeps it safe to call from
/// tests and from alternate entry points.
pub fn init_logging(cfg: &LoggingConfig) -> Result<()> {
    let default: Directive = cfg
        .level
        .parse()
        .with_context(|| format!("invalid logging.level directive: {:?}", cfg.level))?;

    let filter = EnvFilter::builder()
        .with_default_directive(default)
        .with_env_var("RUST_LOG")
        .from_env()
        .context("invalid RUST_LOG filter")?;

    let ansi = std::io::stderr().is_terminal();
    let registry = tracing_subscriber::registry().with(filter);

    let installed = match cfg.format {
        LogFormat::Json => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .flatten_event(true)
                    .with_current_span(true)
                    .with_span_list(false)
                    .with_writer(std::io::stderr),
            )
            .try_init(),
        LogFormat::Pretty => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_ansi(ansi)
                    .with_writer(std::io::stderr),
            )
            .try_init(),
    };

    installed.map_err(|e| anyhow::anyhow!("a global tracing subscriber is already installed: {e}"))
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Every metric the server exports, plus the registry they are bound to.
///
/// Fields are public: services increment them directly on the hot path, which
/// avoids a wrapper call per request.
pub struct Metrics {
    registry: Registry,

    // --- CAS / AC outcomes -------------------------------------------------
    pub cas_hit_total: IntCounter,
    pub cas_miss_total: IntCounter,
    pub ac_hit_total: IntCounter,
    pub ac_miss_total: IntCounter,

    // --- traffic -----------------------------------------------------------
    pub upload_bytes_total: IntCounter,
    pub download_bytes_total: IntCounter,
    pub upload_count: IntCounter,
    pub download_count: IntCounter,

    // --- eviction ----------------------------------------------------------
    pub evicted_bytes_total: IntCounter,
    pub evicted_entries_total: IntCounter,

    /// Bytes clients did *not* have to upload because `FindMissingBlobs`
    /// reported the blob as already present: the shared-cache dividend.
    pub avoided_bytes_total: IntCounter,

    // --- size / occupancy --------------------------------------------------
    pub cache_size_bytes: IntGauge,
    pub cache_entry_count: IntGauge,
    pub cas_size_bytes: IntGauge,
    pub cas_entry_count: IntGauge,
    pub ac_entry_count: IntGauge,

    // --- latency -----------------------------------------------------------
    pub request_duration: HistogramVec,
    pub read_duration: Histogram,
    pub write_duration: Histogram,
    pub sqlite_duration: Histogram,
    pub gc_duration: Histogram,
}

impl Metrics {
    pub fn new() -> Result<Arc<Metrics>> {
        let registry = Registry::new();

        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                format!("{NS}_request_duration_seconds"),
                "End-to-end handler latency per gRPC method, in seconds.",
            )
            // 1ms .. ~4s
            .buckets(exponential_buckets(0.001, 2.0, 13)?),
            &["method"],
        )?;
        registry.register(Box::new(request_duration.clone()))?;

        let m = Metrics {
            cas_hit_total: counter(
                &registry,
                "cas_hit_total",
                "CAS blob lookups served from the cache.",
            )?,
            cas_miss_total: counter(
                &registry,
                "cas_miss_total",
                "CAS blob lookups that found no stored blob.",
            )?,
            ac_hit_total: counter(
                &registry,
                "ac_hit_total",
                "GetActionResult calls answered with a complete, verified entry.",
            )?,
            ac_miss_total: counter(
                &registry,
                "ac_miss_total",
                "GetActionResult calls with no entry, or with an entry whose blobs were missing.",
            )?,
            upload_bytes_total: counter(
                &registry,
                "upload_bytes_total",
                "Total blob bytes accepted from clients and published into the CAS.",
            )?,
            download_bytes_total: counter(
                &registry,
                "download_bytes_total",
                "Total blob bytes served to clients.",
            )?,
            upload_count: counter(
                &registry,
                "upload_count",
                "Number of blobs accepted from clients.",
            )?,
            download_count: counter(
                &registry,
                "download_count",
                "Number of blobs served to clients.",
            )?,
            evicted_bytes_total: counter(
                &registry,
                "evicted_bytes_total",
                "Total bytes reclaimed by the garbage collector.",
            )?,
            evicted_entries_total: counter(
                &registry,
                "evicted_entries_total",
                "Total cache entries removed by the garbage collector.",
            )?,
            avoided_bytes_total: counter(
                &registry,
                "avoided_bytes_total",
                "Bytes clients skipped uploading because the blob was already present.",
            )?,

            cache_size_bytes: gauge(
                &registry,
                "cache_size_bytes",
                "Total bytes accounted to the cache (CAS plus ActionCache metadata).",
            )?,
            cache_entry_count: gauge(
                &registry,
                "cache_entry_count",
                "Total cache entries (CAS blobs plus ActionCache entries).",
            )?,
            cas_size_bytes: gauge(
                &registry,
                "cas_size_bytes",
                "Bytes stored in the content-addressable store.",
            )?,
            cas_entry_count: gauge(
                &registry,
                "cas_entry_count",
                "Number of blobs in the content-addressable store.",
            )?,
            ac_entry_count: gauge(
                &registry,
                "ac_entry_count",
                "Number of ActionCache entries across all instances.",
            )?,

            request_duration,
            read_duration: histogram(
                &registry,
                "read_duration_seconds",
                "Time spent reading a blob from disk, in seconds.",
                // 1ms .. ~32s
                exponential_buckets(0.001, 2.0, 16)?,
            )?,
            write_duration: histogram(
                &registry,
                "write_duration_seconds",
                "Time spent writing and publishing a blob, in seconds.",
                exponential_buckets(0.001, 2.0, 16)?,
            )?,
            sqlite_duration: histogram(
                &registry,
                "sqlite_duration_seconds",
                "Time spent inside a SQLite transaction, in seconds.",
                // 100us .. ~0.8s
                exponential_buckets(0.0001, 2.0, 14)?,
            )?,
            gc_duration: histogram(
                &registry,
                "gc_duration_seconds",
                "Duration of a garbage-collection sweep, in seconds.",
                // 10ms .. ~327s
                exponential_buckets(0.01, 2.0, 16)?,
            )?,
            registry,
        };

        Ok(Arc::new(m))
    }

    /// Record the latency of one RPC. `started` is the instant the handler
    /// was entered.
    pub fn observe_request(&self, method: &str, started: Instant) {
        self.request_duration
            .with_label_values(&[method])
            .observe(started.elapsed().as_secs_f64());
    }

    /// Publish occupancy from a metadata snapshot, keeping the aggregate
    /// `cache_*` gauges consistent with their CAS/AC components.
    pub fn set_cache_stats(&self, stats: &Stats) {
        self.cas_size_bytes.set(stats.cas_size_bytes);
        self.cas_entry_count.set(stats.cas_entry_count);
        self.ac_entry_count.set(stats.ac_entry_count);
        self.cache_size_bytes
            .set(stats.cas_size_bytes + stats.ac_size_bytes);
        self.cache_entry_count
            .set(stats.cas_entry_count + stats.ac_entry_count);
    }

    /// Prometheus text exposition format (`version=0.0.4`).
    pub fn render(&self) -> String {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        let mut out = Vec::with_capacity(8 * 1024);
        if let Err(e) = encoder.encode(&families, &mut out) {
            tracing::error!(error = %e, "failed to encode metrics");
            return String::new();
        }
        String::from_utf8(out).unwrap_or_default()
    }
}

fn counter(registry: &Registry, name: &str, help: &str) -> Result<IntCounter> {
    let c = IntCounter::with_opts(Opts::new(format!("{NS}_{name}"), help))?;
    registry.register(Box::new(c.clone()))?;
    Ok(c)
}

fn gauge(registry: &Registry, name: &str, help: &str) -> Result<IntGauge> {
    let g = IntGauge::with_opts(Opts::new(format!("{NS}_{name}"), help))?;
    registry.register(Box::new(g.clone()))?;
    Ok(g)
}

fn histogram(registry: &Registry, name: &str, help: &str, buckets: Vec<f64>) -> Result<Histogram> {
    let h = Histogram::with_opts(HistogramOpts::new(format!("{NS}_{name}"), help).buckets(buckets))?;
    registry.register(Box::new(h.clone()))?;
    Ok(h)
}

// ---------------------------------------------------------------------------
// Health / readiness
// ---------------------------------------------------------------------------

/// Readiness tracker.
///
/// Liveness (`/health`) is unconditional: the process answering at all proves
/// the runtime is alive. Readiness additionally requires that startup finished
/// (`mark_ready`) and that both the blob store and the metadata database are
/// writable, since a read-only cache is worse than an absent one — Bazel would
/// keep uploading into a black hole.
pub struct HealthState {
    storage: Arc<CasStore>,
    db: Arc<Metadata>,
    serving: AtomicBool,
    /// Monotonic base for `valid_until_ms`.
    started: Instant,
    /// Milliseconds (since `started`, biased by 1) until which the last
    /// successful probe stays valid; `0` means "must probe". Keeps `/ready`
    /// cheap when a load balancer polls it every few hundred milliseconds.
    valid_until_ms: AtomicU64,
}

impl HealthState {
    pub fn new(storage: Arc<CasStore>, db: Arc<Metadata>) -> Arc<HealthState> {
        Arc::new(HealthState {
            storage,
            db,
            serving: AtomicBool::new(false),
            started: Instant::now(),
            valid_until_ms: AtomicU64::new(0),
        })
    }

    /// Flip to serving. Called once the gRPC listener is accepting.
    pub fn mark_ready(&self) {
        self.serving.store(true, Ordering::Release);
    }

    fn now_stamp(&self) -> u64 {
        self.started.elapsed().as_millis() as u64 + 1
    }

    /// `Ok(())` when the server should receive traffic, otherwise a
    /// human-readable reason that is returned verbatim on `/ready`.
    pub async fn ready(&self) -> Result<(), String> {
        if !self.serving.load(Ordering::Acquire) {
            return Err("startup not complete".to_string());
        }

        let valid_until = self.valid_until_ms.load(Ordering::Acquire);
        if valid_until != 0 && self.now_stamp() < valid_until {
            return Ok(());
        }

        // Only successful probes are cached; a failure is re-checked on every
        // hit so that recovery is visible immediately.
        self.storage
            .writable()
            .await
            .map_err(|e| format!("storage not writable: {e}"))?;
        self.db
            .writable()
            .await
            .map_err(|e| format!("metadata database not writable: {e}"))?;

        self.valid_until_ms
            .store(self.now_stamp() + READY_CACHE_TTL_MS, Ordering::Release);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HTTP side-channel
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct HttpState {
    metrics: Arc<Metrics>,
    health: Arc<HealthState>,
}

/// Serve `/metrics`, `/health` and `/ready` until `shutdown` resolves.
pub async fn serve_http(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    health: Arc<HealthState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind metrics listener on {addr}"))?;
    serve_http_listener(listener, metrics, health, shutdown).await
}

pub(crate) async fn serve_http_listener(
    listener: tokio::net::TcpListener,
    metrics: Arc<Metrics>,
    health: Arc<HealthState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let app = Router::new()
        .route("/", get(root))
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .with_state(HttpState { metrics, health });

    let local = listener.local_addr()?;
    tracing::info!(address = %local, "metrics/health HTTP listener bound");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .context("metrics HTTP server failed")?;

    tracing::info!(address = %local, "metrics/health HTTP listener stopped");
    Ok(())
}

async fn root() -> &'static str {
    "bazel-cache-server: REAPI remote cache. Endpoints: /metrics /health /ready\n"
}

async fn metrics_handler(State(st): State<HttpState>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        st.metrics.render(),
    )
        .into_response()
}

/// Liveness: the process is up and the runtime is scheduling tasks.
async fn health_handler() -> &'static str {
    "ok\n"
}

async fn ready_handler(State(st): State<HttpState>) -> Response {
    match st.health.ready().await {
        Ok(()) => (StatusCode::OK, "ready\n".to_string()).into_response(),
        Err(reason) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {reason}\n"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Durability;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::sync::atomic::AtomicU64;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch_dir(tag: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "bazel-cache-obs-{tag}-{}-{n}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(dir.join("cas")).unwrap();
        std::fs::create_dir_all(dir.join("tmp")).unwrap();
        dir
    }

    async fn health_for(tag: &str) -> (Arc<HealthState>, PathBuf) {
        let dir = scratch_dir(tag);
        let storage = CasStore::open(&dir.join("cas"), &dir.join("tmp"), Durability::Cache)
            .await
            .expect("open CAS");
        let db = Metadata::open(&dir.join("cache.db"), Durability::Cache)
            .await
            .expect("open metadata");
        (HealthState::new(storage, db), dir)
    }

    /// Minimal HTTP/1.1 client: avoids pulling a TLS-capable client into the
    /// test for a plaintext loopback request. Returns the status code, the
    /// lowercased header block, and the body.
    async fn http_get(addr: SocketAddr, path: &str) -> (u16, String, String) {
        let mut sock = TcpStream::connect(addr).await.expect("connect");
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        sock.write_all(req.as_bytes()).await.expect("write request");
        let mut raw = Vec::new();
        sock.read_to_end(&mut raw).await.expect("read response");
        let text = String::from_utf8_lossy(&raw).into_owned();
        let status: u16 = text
            .split_whitespace()
            .nth(1)
            .expect("status line")
            .parse()
            .expect("status code");
        let (head, body) = text
            .split_once("\r\n\r\n")
            .map(|(h, b)| (h.to_lowercase(), b.to_string()))
            .unwrap_or_default();
        (status, head, body)
    }

    struct Server {
        addr: SocketAddr,
        stop: oneshot::Sender<()>,
        handle: tokio::task::JoinHandle<Result<()>>,
    }

    impl Server {
        async fn start(metrics: Arc<Metrics>, health: Arc<HealthState>) -> Server {
            // Ask the OS for a free port, release it, then let the server take
            // it. Retry in case something else grabs it in between.
            for _ in 0..8 {
                let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = probe.local_addr().unwrap();
                drop(probe);

                let (stop, rx) = oneshot::channel::<()>();
                let handle = tokio::spawn(serve_http(
                    addr,
                    Arc::clone(&metrics),
                    Arc::clone(&health),
                    async move {
                        let _ = rx.await;
                    },
                ));

                for _ in 0..200 {
                    if handle.is_finished() {
                        break;
                    }
                    if TcpStream::connect(addr).await.is_ok() {
                        return Server { addr, stop, handle };
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let _ = stop.send(());
                let _ = handle.await;
            }
            panic!("could not start metrics HTTP server");
        }

        async fn shutdown(self) {
            self.stop.send(()).expect("server still listening");
            let joined = tokio::time::timeout(Duration::from_secs(5), self.handle)
                .await
                .expect("server task did not stop after shutdown signal")
                .expect("server task panicked");
            joined.expect("server returned an error");
        }
    }

    #[test]
    fn init_logging_rejects_a_bad_level() {
        let cfg = LoggingConfig {
            level: "bazel_cache=super_loud".to_string(),
            format: LogFormat::Pretty,
        };
        assert!(init_logging(&cfg).is_err());
    }

    #[test]
    fn init_logging_is_single_shot() {
        let cfg = LoggingConfig {
            level: "info".to_string(),
            format: LogFormat::Json,
        };
        // The first call may lose the race against another test in this
        // process; the second must always fail rather than panic.
        let _ = init_logging(&cfg);
        assert!(init_logging(&cfg).is_err());
    }

    #[test]
    fn render_exposes_registered_metrics_with_help() {
        let m = Metrics::new().unwrap();
        m.cas_hit_total.inc_by(3);
        m.observe_request("ByteStream.Read", Instant::now());
        m.set_cache_stats(&Stats {
            cas_size_bytes: 4096,
            cas_entry_count: 2,
            ac_entry_count: 1,
            ac_size_bytes: 512,
        });

        let text = m.render();
        assert!(
            text.contains("bazel_cache_cas_hit_total 3"),
            "counter value missing:\n{text}"
        );
        assert!(text.contains("# HELP bazel_cache_cas_hit_total"));
        assert!(text.contains("bazel_cache_cache_entry_count 3"));
        assert!(text.contains("bazel_cache_cache_size_bytes 4608"));
        assert!(text.contains("bazel_cache_cas_size_bytes 4096"));
        assert!(
            text.contains("bazel_cache_request_duration_seconds_count{method=\"ByteStream.Read\"} 1"),
            "labeled histogram missing:\n{text}"
        );
        // Every declared metric family must be registered exactly once.
        for name in [
            "bazel_cache_cas_miss_total",
            "bazel_cache_ac_hit_total",
            "bazel_cache_ac_miss_total",
            "bazel_cache_upload_bytes_total",
            "bazel_cache_download_bytes_total",
            "bazel_cache_upload_count",
            "bazel_cache_download_count",
            "bazel_cache_evicted_bytes_total",
            "bazel_cache_evicted_entries_total",
            "bazel_cache_avoided_bytes_total",
            "bazel_cache_cache_size_bytes",
            "bazel_cache_cas_entry_count",
            "bazel_cache_ac_entry_count",
            "bazel_cache_read_duration_seconds_count",
            "bazel_cache_write_duration_seconds_count",
            "bazel_cache_sqlite_duration_seconds_count",
            "bazel_cache_gc_duration_seconds_count",
        ] {
            assert_eq!(
                text.matches(&format!("\n{name} ")).count()
                    + usize::from(text.starts_with(&format!("{name} "))),
                1,
                "expected exactly one sample for {name}:\n{text}"
            );
        }
    }

    #[test]
    fn two_metrics_registries_are_independent() {
        // Registration must not use the global default registry, otherwise a
        // second server instance in-process would fail with AlreadyReg.
        let a = Metrics::new().unwrap();
        let b = Metrics::new().unwrap();
        a.cas_hit_total.inc();
        assert!(a.render().contains("bazel_cache_cas_hit_total 1"));
        assert!(b.render().contains("bazel_cache_cas_hit_total 0"));
    }

    #[tokio::test]
    async fn http_surface_reports_metrics_liveness_and_readiness() {
        let metrics = Metrics::new().unwrap();
        let (health, dir) = health_for("http").await;
        let server = Server::start(Arc::clone(&metrics), Arc::clone(&health)).await;

        // Readiness is false until startup completes.
        let (code, _, body) = http_get(server.addr, "/ready").await;
        assert_eq!(code, 503, "body: {body}");
        assert!(body.contains("startup not complete"), "body: {body}");

        // Liveness is independent of readiness.
        let (code, _, body) = http_get(server.addr, "/health").await;
        assert_eq!(code, 200);
        assert_eq!(body, "ok\n");

        health.mark_ready();
        let (code, _, body) = http_get(server.addr, "/ready").await;
        assert_eq!(code, 200, "body: {body}");
        assert_eq!(body, "ready\n");

        // Counter mutations are visible through the HTTP exposition, served in
        // the exposition format Prometheus scrapers expect.
        metrics.upload_bytes_total.inc_by(1234);
        let (code, head, body) = http_get(server.addr, "/metrics").await;
        assert_eq!(code, 200);
        assert!(
            head.contains("content-type: text/plain; version=0.0.4"),
            "metrics headers: {head}"
        );
        assert!(
            body.contains("bazel_cache_upload_bytes_total 1234"),
            "metrics body: {body}"
        );

        let (code, _, body) = http_get(server.addr, "/").await;
        assert_eq!(code, 200);
        assert!(body.contains("/metrics"));

        let (code, _, _) = http_get(server.addr, "/nope").await;
        assert_eq!(code, 404);

        let addr = server.addr;
        server.shutdown().await;

        // The listener is really gone.
        assert!(TcpStream::connect(addr).await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ready_caches_success_and_then_surfaces_backend_failure() {
        let (health, dir) = health_for("probe").await;

        assert!(health.ready().await.is_err(), "not ready before mark_ready");
        health.mark_ready();
        health.ready().await.expect("healthy backends");

        // Remove the upload directory to simulate an unavailable data volume.
        // Keep SQLite open: Windows prevents deleting its live database files.
        // Within the cache window the successful probe result is reused.
        std::fs::remove_dir_all(dir.join("tmp")).unwrap();
        health.ready().await.expect("cached success is reused");

        // Once the cached result expires the failure must surface.
        health.valid_until_ms.store(0, Ordering::Release);
        health.ready().await.expect_err("probe should fail");

        // A recovered backend must be probed immediately after a failure.
        std::fs::create_dir(dir.join("tmp")).unwrap();
        health.ready().await.expect("recovered storage is ready");
        drop(health);
        let _ = std::fs::remove_dir_all(dir);
    }
}
