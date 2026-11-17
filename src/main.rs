//! Bazel LAN remote cache server: REAPI v2 cache-only over gRPC.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use clap::{Args, Parser, Subcommand};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

use bazel_cache_server::config::{
    self, AuthMode, Config, Durability, LogFormat, Overrides, PolicyKind,
};
use bazel_cache_server::{observability, server};

use tokio::sync::watch;
#[cfg(unix)]
use tracing::info;
use tracing::warn;

#[derive(Debug, Parser)]
#[command(
    name = "bazel-cache-server",
    version,
    about = "Bazel remote cache (REAPI v2, cache only)"
)]
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
    let cfg = Arc::new(Config::load(
        cli.config.as_deref(),
        &cli.overrides.to_overrides(),
    )?);
    observability::init_logging(&cfg.logging)?;

    // Single process-wide rustls provider; tonic uses `tls-ring` and reqwest is
    // built provider-less, so both resolve to ring.
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        warn!("rustls crypto provider was already installed");
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    runtime.block_on(async move {
        match cli.command.unwrap_or(Command::Serve) {
            Command::Serve => {
                let (shutdown, rx) = watch::channel(false);
                tokio::spawn(async move {
                    wait_for_shutdown().await;
                    let _ = shutdown.send(true);
                });
                server::serve(cfg, rx).await
            }
            Command::Reindex => server::reindex(cfg).await,
            Command::Gc => server::gc_once(cfg).await,
            Command::Stats => server::stats(cfg).await,
        }
    })
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
    }
    #[cfg(windows)]
    {
        let mut close = tokio::signal::windows::ctrl_close().expect("install close handler");
        let mut shutdown =
            tokio::signal::windows::ctrl_shutdown().expect("install shutdown handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = close.recv() => {},
            _ = shutdown.recv() => {},
        }
    }
}
