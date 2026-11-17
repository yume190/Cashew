use crate::auth::{self, Authenticator};
use crate::config::Config;
use crate::db::Metadata;
use crate::gc::Gc;
use crate::observability::{self, HealthState, Metrics};
use crate::proto::bytestream::byte_stream_server::ByteStreamServer;
use crate::proto::reapi::action_cache_server::ActionCacheServer;
use crate::proto::reapi::capabilities_server::CapabilitiesServer;
use crate::proto::reapi::content_addressable_storage_server::ContentAddressableStorageServer;
use crate::service::{ActionCacheSvc, ByteStreamSvc, CapabilitiesSvc, CasSvc, ServiceContext};
use crate::storage::CasStore;
use anyhow::{Context as _, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::InterceptorLayer;
use tracing::{info, warn};

const ACCESS_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
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

/// Run until the owner requests shutdown or drops its shutdown sender.
pub async fn serve(cfg: Arc<Config>, shutdown: watch::Receiver<bool>) -> Result<()> {
    run(cfg, shutdown, None).await
}

pub(crate) struct Ready {
    pub metrics: Arc<Metrics>,
    pub endpoint: String,
}

pub(crate) async fn run(
    cfg: Arc<Config>,
    mut shutdown: watch::Receiver<bool>,
    ready: Option<oneshot::Sender<Ready>>,
) -> Result<()> {
    let (storage, db) = open_core(&cfg).await?;

    let removed = storage
        .cleanup_tmp()
        .await
        .context("cleaning tmp uploads")?;
    if removed > 0 {
        info!(removed, "removed incomplete uploads left by a previous run");
    }

    let metrics = Metrics::new()?;
    let stats = db.stats().await?;
    metrics.set_cache_stats(&stats);

    let authenticator = Authenticator::new(&cfg.auth)
        .await
        .context("initializing authentication")?;
    let tls = auth::server_tls_config(&cfg.auth).context("configuring TLS")?;
    anyhow::ensure!(
        !authenticator.requires_tls() || tls.is_some(),
        "auth.mode = \"mtls\" requires an [auth.tls] section with a server certificate"
    );
    let tls_enabled = tls.is_some();
    let listener = tokio::net::TcpListener::bind(cfg.server.listen)
        .await
        .context("binding gRPC listener")?;
    let http_listener = if cfg.metrics.enabled {
        Some(
            tokio::net::TcpListener::bind(cfg.metrics.listen)
                .await
                .context("binding metrics listener")?,
        )
    } else {
        None
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
    health_reporter
        .set_serving::<ActionCacheServer<ActionCacheSvc>>()
        .await;
    health_reporter
        .set_serving::<ByteStreamServer<ByteStreamSvc>>()
        .await;
    health_reporter
        .set_serving::<CapabilitiesServer<CapabilitiesSvc>>()
        .await;

    let auth_layer = InterceptorLayer::new(authenticator.clone());
    let mut builder = tonic::transport::Server::builder().layer(auth_layer);
    if let Some(tls) = tls {
        builder = builder
            .tls_config(tls)
            .context("applying TLS configuration")?;
    }
    if let Some(max) = cfg.server.max_concurrent_streams {
        builder = builder.max_concurrent_streams(max);
    }

    let router = builder
        .add_service(health_service)
        .add_service(CapabilitiesServer::new(CapabilitiesSvc::new(ctx.clone())))
        .add_service(ActionCacheServer::new(ActionCacheSvc::new(ctx.clone())))
        .add_service(ContentAddressableStorageServer::new(CasSvc::new(
            ctx.clone(),
        )))
        .add_service(ByteStreamServer::new(ByteStreamSvc::new(ctx.clone())));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let health = HealthState::new(storage.clone(), db.clone());

    // Metrics / health HTTP side-channel.
    let http_task = if let Some(listener) = http_listener {
        let metrics = metrics.clone();
        let health = health.clone();
        let mut rx = shutdown_rx.clone();
        let shutdown_on_error = shutdown_tx.clone();
        Some(tokio::spawn(async move {
            let shutdown = async move {
                let _ = rx.wait_for(|stop| *stop).await;
            };
            let result =
                observability::serve_http_listener(listener, metrics, health, shutdown).await;
            if result.is_err() {
                let _ = shutdown_on_error.send(true);
            }
            result
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
    if let Some(ready) = ready {
        let address = listener.local_addr()?;
        let address = if address.ip().is_unspecified() {
            std::net::SocketAddr::new(
                if address.is_ipv4() {
                    std::net::Ipv4Addr::LOCALHOST.into()
                } else {
                    std::net::Ipv6Addr::LOCALHOST.into()
                },
                address.port(),
            )
        } else {
            address
        };
        let _ = ready.send(Ready {
            metrics: metrics.clone(),
            endpoint: format!("{}://{address}", if tls_enabled { "grpcs" } else { "grpc" }),
        });
    }
    let forward_shutdown = shutdown_tx.clone();
    let owner_shutdown = tokio::spawn(async move {
        let _ = shutdown.wait_for(|stop| *stop).await;
        let _ = forward_shutdown.send(true);
    });

    let mut rx = shutdown_rx.clone();
    let serve_result = router
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let _ = rx.wait_for(|stop| *stop).await;
        })
        .await;

    let _ = shutdown_tx.send(true);
    owner_shutdown.abort();

    let _ = flush_task.await;
    let _ = gc_task.await;
    if let Some(task) = http_task {
        task.await.context("joining HTTP server")??;
    }
    info!("shutdown complete");
    serve_result.context("gRPC server failed")
}
pub async fn reindex(cfg: Arc<Config>) -> Result<()> {
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
        stats.cas_size_bytes,
        stats.ac_entry_count,
        started.elapsed()
    );
    Ok(())
}

pub async fn gc_once(cfg: Arc<Config>) -> Result<()> {
    let (storage, db) = open_core(&cfg).await?;
    let metrics = Metrics::new()?;
    let gc = Gc::new(cfg, storage, db, metrics);
    let report = gc.run_once().await?;
    println!("{report}");
    Ok(())
}

pub async fn stats(cfg: Arc<Config>) -> Result<()> {
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
