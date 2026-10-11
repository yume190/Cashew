//! Bazel LAN remote cache server: REAPI v2 cache-only over gRPC.

mod auth;
mod config;
mod db;
mod digest;
mod gc;
mod observability;
mod proto;
mod service;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::{Args, Parser, Subcommand};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tonic::service::InterceptorLayer;
use tracing::{error, info, warn};

use crate::auth::Authenticator;
use crate::config::{AuthMode, Config, Durability, LogFormat, Overrides, PolicyKind};
use crate::db::Metadata;
use crate::gc::Gc;
use crate::observability::{HealthState, Metrics};
use crate::proto::bytestream::byte_stream_server::ByteStreamServer;
use crate::proto::reapi::action_cache_server::ActionCacheServer;
use crate::proto::reapi::capabilities_server::CapabilitiesServer;
use crate::proto::reapi::content_addressable_storage_server::ContentAddressableStorageServer;
use crate::service::{ActionCacheSvc, ByteStreamSvc, CapabilitiesSvc, CasSvc, ServiceContext};
use crate::storage::CasStore;

/// How often pending `last_access_at` updates are flushed to SQLite (Doc §14).
const ACCESS_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(name = "bazel-cache-server", version, about = "Bazel remote cache (REAPI v2, cache only)")]
struct Cli {
    /// TOML configuration file. Without it, defaults + environment are used.
    #[arg(short, long, value_name = "FILE", global = true)]
    config: Option<PathBuf>,

    #[command(flatten)]
    overrides: OverrideArgs,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Args)]
struct OverrideArgs {
    /// gRPC listen address, e.g. 0.0.0.0:50051
    #[arg(long, global = true)]
    listen: Option<SocketAddr>,
    /// Storage root containing cas/, tmp/ and metadata.sqlite
    #[arg(long, global = true)]
    root: Option<PathBuf>,
    /// Cache byte budget, e.g. 800GB
    #[arg(long, global = true, value_parser = config::parse_size)]
    max_size: Option<u64>,
    /// Durability mode
    #[arg(long, global = true, value_parser = parse_durability)]
    durability: Option<Durability>,
    /// Eviction policy: lru, ttl, fifo, none
    #[arg(long, global = true, value_parser = parse_policy)]
    eviction_policy: Option<PolicyKind>,
    /// Authentication mode: none, token, mtls, oidc
    #[arg(long, global = true, value_parser = parse_auth_mode)]
    auth_mode: Option<AuthMode>,
    /// Metrics/health HTTP listen address
    #[arg(long, global = true)]
    metrics_listen: Option<SocketAddr>,
    /// Disable the metrics/health HTTP listener
    #[arg(long, global = true)]
    no_metrics: bool,
    /// Log level or filter directive
    #[arg(long, global = true)]
    log_level: Option<String>,
    /// Log format: pretty or json
    #[arg(long, global = true, value_parser = parse_log_format)]
    log_format: Option<LogFormat>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the cache server (default).
    Serve,
    /// Rebuild SQLite metadata from the filesystem CAS (CAS is the source of truth).
    Reindex,
    /// Run one eviction sweep and exit.
    Gc,
    /// Print cache statistics.
    Stats,
}

fn parse_durability(s: &str) -> Result<Durability, String> {
    match s {
        "cache" => Ok(Durability::Cache),
        "strict" => Ok(Durability::Strict),
        other => Err(format!("expected cache or strict, got {other:?}")),
    }
}

fn parse_policy(s: &str) -> Result<PolicyKind, String> {
    match s {
        "lru" => Ok(PolicyKind::Lru),
        "ttl" => Ok(PolicyKind::Ttl),
        "fifo" => Ok(PolicyKind::Fifo),
        "none" => Ok(PolicyKind::None),
        other => Err(format!("expected lru, ttl, fifo or none, got {other:?}")),
    }
}

fn parse_auth_mode(s: &str) -> Result<AuthMode, String> {
    match s {
        "none" => Ok(AuthMode::None),
        "token" => Ok(AuthMode::Token),
        "mtls" => Ok(AuthMode::Mtls),
        "oidc" => Ok(AuthMode::Oidc),
        other => Err(format!("expected none, token, mtls or oidc, got {other:?}")),
    }
}

fn parse_log_format(s: &str) -> Result<LogFormat, String> {
    match s {
        "pretty" => Ok(LogFormat::Pretty),
        "json" => Ok(LogFormat::Json),
        other => Err(format!("expected pretty or json, got {other:?}")),
    }
}

impl OverrideArgs {
    fn to_overrides(&self) -> Overrides {
        Overrides {
            listen: self.listen,
            root: self.root.clone(),
            max_size: self.max_size,
            durability: self.durability,
            eviction_policy: self.eviction_policy,
            auth_mode: self.auth_mode,
            metrics_listen: self.metrics_listen,
            metrics_enabled: if self.no_metrics { Some(false) } else { None },
            log_level: self.log_level.clone(),
            log_format: self.log_format,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = Arc::new(Config::load(cli.config.as_deref(), &cli.overrides.to_overrides())?);
    observability::init_logging(&cfg.logging)?;

    // Single process-wide rustls provider; tonic uses `tls-ring` and reqwest is
    // built provider-less, so both resolve to ring.
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        warn!("rustls crypto provider was already installed");
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    runtime.block_on(async move {
        match cli.command.unwrap_or(Command::Serve) {
            Command::Serve => serve(cfg).await,
            Command::Reindex => reindex(cfg).await,
            Command::Gc => gc_once(cfg).await,
            Command::Stats => stats(cfg).await,
        }
    })
}

/// Open storage + metadata, which every subcommand needs.
async fn open_core(cfg: &Config) -> Result<(Arc<CasStore>, Arc<Metadata>)> {
    std::fs::create_dir_all(&cfg.storage.root)
        .with_context(|| format!("creating storage root {}", cfg.storage.root.display()))?;
    let storage = CasStore::open(&cfg.cas_dir(), &cfg.tmp_dir(), cfg.storage.durability)
        .await
        .context("opening CAS store")?;
    let db = Metadata::open(&cfg.db_path(), cfg.storage.durability)
        .await
        .context("opening metadata database")?;
    Ok((storage, db))
}

async fn serve(cfg: Arc<Config>) -> Result<()> {
    let (storage, db) = open_core(&cfg).await?;

    let removed = storage.cleanup_tmp().await.context("cleaning tmp uploads")?;
    if removed > 0 {
        info!(removed, "removed incomplete uploads left by a previous run");
    }

    let metrics = Metrics::new()?;
    let stats = db.stats().await?;
    metrics.set_cache_stats(&stats);

    let authenticator = Authenticator::new(&cfg.auth).await.context("initializing authentication")?;
    let tls = auth::server_tls_config(&cfg.auth).context("configuring TLS")?;
    anyhow::ensure!(
        !authenticator.requires_tls() || tls.is_some(),
        "auth.mode = \"mtls\" requires an [auth.tls] section with a server certificate"
    );
    let tls_enabled = tls.is_some();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let health = HealthState::new(storage.clone(), db.clone());

    // Metrics / health HTTP side-channel.
    let http_task = if cfg.metrics.enabled {
        let addr = cfg.metrics.listen;
        let metrics = metrics.clone();
        let health = health.clone();
        let mut rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            let shutdown = async move {
                let _ = rx.wait_for(|stop| *stop).await;
            };
            if let Err(err) = observability::serve_http(addr, metrics, health, shutdown).await {
                error!(%err, "metrics server failed");
            }
        }))
    } else {
        None
    };

    // Batched last_access_at writer (Doc §14), which also keeps the occupancy
    // gauges fresh: GC sweeps are far too coarse to track a running build.
    let flush_task = {
        let db = db.clone();
        let metrics = metrics.clone();
        let mut rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(ACCESS_FLUSH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let started = std::time::Instant::now();
                        match db.flush_access().await {
                            Ok(n) if n > 0 => {
                                metrics.sqlite_duration.observe(started.elapsed().as_secs_f64());
                                tracing::debug!(rows = n, "flushed access timestamps");
                            }
                            Ok(_) => {}
                            Err(err) => warn!(%err, "flushing access timestamps failed"),
                        }
                        match db.stats().await {
                            Ok(stats) => metrics.set_cache_stats(&stats),
                            Err(err) => warn!(%err, "refreshing cache gauges failed"),
                        }
                    }
                    _ = async { let _ = rx.wait_for(|stop| *stop).await; } => break,
                }
            }
            if let Err(err) = db.flush_access().await {
                warn!(%err, "final access flush failed");
            }
        })
    };

    // Eviction sweeper.
    let gc = Gc::new(cfg.clone(), storage.clone(), db.clone(), metrics.clone());
    let gc_task = {
        let gc = gc.clone();
        let rx = shutdown_rx.clone();
        tokio::spawn(async move { gc.run_loop(rx).await })
    };

    let ctx = Arc::new(ServiceContext {
        cfg: cfg.clone(),
        storage: storage.clone(),
        db: db.clone(),
        metrics: metrics.clone(),
    });

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<ContentAddressableStorageServer<CasSvc>>()
        .await;
    health_reporter.set_serving::<ActionCacheServer<ActionCacheSvc>>().await;
    health_reporter.set_serving::<ByteStreamServer<ByteStreamSvc>>().await;
    health_reporter.set_serving::<CapabilitiesServer<CapabilitiesSvc>>().await;

    let auth_layer = InterceptorLayer::new(authenticator.clone());
    let mut builder = tonic::transport::Server::builder().layer(auth_layer);
    if let Some(tls) = tls {
        builder = builder.tls_config(tls).context("applying TLS configuration")?;
    }
    if let Some(max) = cfg.server.max_concurrent_streams {
        builder = builder.max_concurrent_streams(max);
    }

    let router = builder
        .add_service(health_service)
        .add_service(CapabilitiesServer::new(CapabilitiesSvc::new(ctx.clone())))
        .add_service(ActionCacheServer::new(ActionCacheSvc::new(ctx.clone())))
        .add_service(ContentAddressableStorageServer::new(CasSvc::new(ctx.clone())))
        .add_service(ByteStreamServer::new(ByteStreamSvc::new(ctx.clone())));

    health.mark_ready();
    info!(
        listen = %cfg.server.listen,
        root = %cfg.storage.root.display(),
        max_size = cfg.storage.max_size,
        durability = ?cfg.storage.durability,
        eviction = ?cfg.eviction.policy,
        auth = ?authenticator.mode(),
        tls = tls_enabled,
        cas_entries = stats.cas_entry_count,
        cas_bytes = stats.cas_size_bytes,
        ac_entries = stats.ac_entry_count,
        "bazel cache server ready"
    );

    let signal_task = {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            wait_for_shutdown().await;
            let _ = shutdown_tx.send(true);
        })
    };

    let mut rx = shutdown_rx.clone();
    let serve_result = router
        .serve_with_shutdown(cfg.server.listen, async move {
            let _ = rx.wait_for(|stop| *stop).await;
        })
        .await;

    if let Err(err) = serve_result {
        error!(%err, "gRPC server stopped with an error");
    }
    // Stop background tasks even if the server exited on its own error.
    signal_task.abort();
    let _ = shutdown_tx.send(true);

    let _ = flush_task.await;
    let _ = gc_task.await;
    if let Some(task) = http_task {
        let _ = task.await;
    }
    info!("shutdown complete");
    Ok(())
}

async fn wait_for_shutdown() {
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = sigint.recv() => info!("received SIGINT"),
        _ = sigterm.recv() => info!("received SIGTERM"),
    }
}

async fn reindex(cfg: Arc<Config>) -> Result<()> {
    let (storage, db) = open_core(&cfg).await?;
    let removed = storage.cleanup_tmp().await?;
    let started = std::time::Instant::now();

    let mut blobs = Vec::new();
    storage
        .scan_blobs(|blob| blobs.push((blob.digest, blob.size_bytes, blob.created_at_micros)))
        .await
        .context("scanning CAS")?;
    let scanned = blobs.len();
    let stats = db.reindex(blobs).await.context("rebuilding metadata")?;

    println!(
        "reindex: {scanned} blobs, {} bytes, {} action cache entries retained, {removed} temp files removed, {:.1?}",
        stats.cas_size_bytes, stats.ac_entry_count, started.elapsed()
    );
    Ok(())
}

async fn gc_once(cfg: Arc<Config>) -> Result<()> {
    let (storage, db) = open_core(&cfg).await?;
    let metrics = Metrics::new()?;
    let gc = Gc::new(cfg, storage, db, metrics);
    let report = gc.run_once().await?;
    println!("{report}");
    Ok(())
}

async fn stats(cfg: Arc<Config>) -> Result<()> {
    let (_storage, db) = open_core(&cfg).await?;
    let stats = db.stats().await?;
    let used = stats.cas_size_bytes as f64 / cfg.storage.max_size.max(1) as f64;
    println!("cas_entry_count   {}", stats.cas_entry_count);
    println!("cas_size_bytes    {}", stats.cas_size_bytes);
    println!("ac_entry_count    {}", stats.ac_entry_count);
    println!("ac_size_bytes     {}", stats.ac_size_bytes);
    println!("max_size          {}", cfg.storage.max_size);
    println!("used_fraction     {used:.4}");
    println!("high_watermark    {}", cfg.high_watermark_bytes());
    println!("low_watermark     {}", cfg.low_watermark_bytes());
    Ok(())
}
