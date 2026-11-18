//! Desktop-owned service. Network and disk work stay on a dedicated Tokio thread.
use std::collections::VecDeque;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use tracing_subscriber::fmt::MakeWriter;

use crate::config::{Config, Overrides, parse_size};
use crate::observability::Metrics;
use crate::server;
pub use crate::traffic::{TrafficEvent, TrafficKind, TrafficSession};
use crate::traffic::TrafficLog;

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DesktopSettings {
    pub root: String,
    pub listen: String,
    pub metrics_listen: String,
    pub max_size: String,
    pub config_file: String,
}

impl Default for DesktopSettings {
    fn default() -> Self {
        let dirs = directories::ProjectDirs::from("dev", "yume", "Cashew")
            .expect("user profile must be available");
        let root = dirs.cache_dir();
        Self {
            root: root.to_string_lossy().into_owned(),
            listen: "127.0.0.1:50051".into(),
            metrics_listen: "127.0.0.1:9090".into(),
            max_size: "100GB".into(),
            config_file: String::new(),
        }
    }
}

impl DesktopSettings {
    fn config(&self) -> Result<Config> {
        anyhow::ensure!(
            !self.root.trim().is_empty(),
            "Cache directory cannot be empty"
        );
        let path = (!self.config_file.trim().is_empty()).then(|| Path::new(&self.config_file));
        Config::load(
            path,
            &Overrides {
                root: Some(self.root.clone().into()),
                listen: Some(self.listen.trim().parse().context("Invalid gRPC address")?),
                metrics_listen: Some(
                    self.metrics_listen
                        .trim()
                        .parse()
                        .context("Invalid metrics address")?,
                ),
                max_size: Some(parse_size(self.max_size.trim()).context("Invalid cache capacity")?),
                ..Default::default()
            },
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ServicePhase {
    #[default]
    Stopped,
    Starting,
    Running,
    Stopping,
    Failed,
}

#[derive(Clone, Default)]
pub struct DesktopSnapshot {
    pub phase: ServicePhase,
    pub error: Option<String>,
    pub cas_entries: u64,
    pub cas_bytes: u64,
    pub ac_entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub endpoint: Option<String>,
    pub uptime_secs: u64,
    pub logs: Vec<String>,
    pub upload_requests: u64,
    pub query_requests: u64,
    pub download_requests: u64,
    /// Real socket bytes (HTTP/2 and TLS included, TCP/IP headers excluded).
    pub network_uploaded_bytes: u64,
    pub network_downloaded_bytes: u64,
    pub network_upload_bytes_per_sec: f64,
    pub network_download_bytes_per_sec: f64,
    /// Newest first; every live session plus the latest 200 closed.
    pub sessions: Vec<TrafficSession>,
    pub selected_session_id: Option<u64>,
    /// Selected session's operations only, newest first, at most 200.
    pub events: Vec<TrafficEvent>,
}

enum Command {
    Start(DesktopSettings),
    Stop,
    Quit,
}

type Logs = Arc<Mutex<VecDeque<String>>>;

pub struct DesktopController {
    commands: mpsc::UnboundedSender<Command>,
    state: Arc<Mutex<DesktopSnapshot>>,
    logs: Logs,
    /// Requested session id; 0 means newest.
    selection: Arc<AtomicU64>,
    thread: Option<JoinHandle<()>>,
}

impl DesktopController {
    pub fn new() -> Result<Self> {
        let (commands, rx) = mpsc::unbounded_channel();
        let state = Arc::new(Mutex::new(DesktopSnapshot::default()));
        let logs = Arc::new(Mutex::new(VecDeque::new()));
        let selection = Arc::new(AtomicU64::new(0));
        let worker_state = state.clone();
        let worker_selection = selection.clone();
        let writer = LogSink(logs.clone());
        let thread = std::thread::Builder::new()
            .name("cashew-service".into())
            .spawn(move || {
                let subscriber = tracing_subscriber::fmt()
                    .with_ansi(false)
                    .with_writer(writer)
                    .with_max_level(tracing::Level::INFO)
                    .finish();
                tracing::subscriber::with_default(subscriber, || {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime.block_on(worker(rx, worker_state, worker_selection)),
                        Err(error) => {
                            fail(&worker_state, format!("Creating service runtime: {error}"))
                        }
                    }
                });
            })
            .context("Starting service thread")?;
        Ok(Self {
            commands,
            state,
            logs,
            selection,
            thread: Some(thread),
        })
    }

    pub fn start(&mut self, settings: DesktopSettings) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if !matches!(state.phase, ServicePhase::Stopped | ServicePhase::Failed) {
            bail!("Service is already active");
        }
        self.selection.store(0, Ordering::Relaxed);
        *state = DesktopSnapshot {
            phase: ServicePhase::Starting,
            ..Default::default()
        };
        if self.commands.send(Command::Start(settings)).is_err() {
            state.phase = ServicePhase::Failed;
            state.error = Some("Service thread has stopped".into());
            bail!("Service thread has stopped");
        }
        Ok(())
    }

    pub fn stop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if matches!(state.phase, ServicePhase::Starting | ServicePhase::Running) {
            state.phase = ServicePhase::Stopping;
            let _ = self.commands.send(Command::Stop);
        }
    }

    /// Select a captured session for the event list; `None` selects the newest.
    /// The snapshot reflects it on the next 500 ms sample.
    pub fn select_session(&self, id: Option<u64>) {
        self.selection.store(id.unwrap_or(0), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DesktopSnapshot {
        let mut snapshot = self.state.lock().unwrap().clone();
        snapshot.logs = self.logs.lock().unwrap().iter().cloned().collect();
        snapshot
    }
}

impl Drop for DesktopController {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Quit);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn fail(state: &Mutex<DesktopSnapshot>, error: String) {
    tracing::error!(%error, "cache service failed");
    let mut state = state.lock().unwrap();
    state.phase = ServicePhase::Failed;
    state.error = Some(error);
}

/// Copy the capture log into the snapshot: totals, every retained session, and
/// only the selected session's events. A requested session that has been evicted
/// falls back to the newest one and the request is cleared.
fn publish_traffic(state: &mut DesktopSnapshot, traffic: &TrafficLog, selection: &AtomicU64) {
    let totals = traffic.totals();
    state.upload_requests = totals.upload;
    state.query_requests = totals.query;
    state.download_requests = totals.download;
    state.network_uploaded_bytes = totals.network_uploaded;
    state.network_downloaded_bytes = totals.network_downloaded;
    let requested = match selection.load(Ordering::Relaxed) {
        0 => None,
        id => Some(id),
    };
    let snapshot = traffic.snapshot(requested);
    if let Some(id) = requested.filter(|id| snapshot.selected_session_id != Some(*id)) {
        let _ = selection.compare_exchange(id, 0, Ordering::Relaxed, Ordering::Relaxed);
    }
    state.sessions = snapshot.sessions;
    state.selected_session_id = snapshot.selected_session_id;
    state.events = snapshot.events;
}

async fn worker(
    mut commands: mpsc::UnboundedReceiver<Command>,
    state: Arc<Mutex<DesktopSnapshot>>,
    selection: Arc<AtomicU64>,
) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut job: Option<tokio::task::JoinHandle<Result<()>>> = None;
    let mut stop: Option<watch::Sender<bool>> = None;
    let mut ready: Option<oneshot::Receiver<server::Ready>> = None;
    let mut metrics: Option<Arc<Metrics>> = None;
    let mut started = Instant::now();
    let mut last_sample = started;
    let mut last_network = (0u64, 0u64);
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Start(settings)) if job.is_none() => {
                    metrics = None;
                    match settings.config() {
                        Ok(cfg) => {
                            let (tx, rx) = watch::channel(false);
                            let (ready_tx, ready_rx) = oneshot::channel();
                            stop = Some(tx);
                            ready = Some(ready_rx);
                            job = Some(tokio::spawn(server::run(Arc::new(cfg), rx, Some(ready_tx))));
                        }
                        Err(error) => fail(&state, format!("{error:#}")),
                    }
                }
                Some(Command::Stop) => {
                    if let Some(stop) = &stop { let _ = stop.send(true); }
                }
                Some(Command::Quit) | None => {
                    if let Some(stop) = stop.take() { let _ = stop.send(true); }
                    if let Some(job) = job.take() { let _ = job.await; }
                    return;
                }
                Some(Command::Start(_)) => {}
            },
            result = async {
                match &mut ready {
                    Some(ready) => ready.await,
                    None => std::future::pending().await,
                }
            } => {
                ready = None;
                if let Ok(result) = result {
                    metrics = Some(result.metrics);
                    started = Instant::now();
                    last_sample = started;
                    last_network = (0, 0);
                    let mut state = state.lock().unwrap();
                    state.endpoint = result.endpoint;
                    if state.phase == ServicePhase::Starting { state.phase = ServicePhase::Running; }
                }
            }
            result = async {
                match &mut job {
                    Some(job) => job.await,
                    None => std::future::pending().await,
                }
            } => {
                job = None;
                ready = None;
                stop = None;
                // The server has returned, so every socket is closed: publish the
                // final session states and stop all rates.
                if let Some(metrics) = &metrics
                    && let Some(traffic) = metrics.traffic() {
                    let mut state = state.lock().unwrap();
                    publish_traffic(&mut state, traffic, &selection);
                }
                {
                    let mut state = state.lock().unwrap();
                    state.network_upload_bytes_per_sec = 0.0;
                    state.network_download_bytes_per_sec = 0.0;
                    for session in &mut state.sessions {
                        session.upload_bytes_per_sec = 0.0;
                        session.download_bytes_per_sec = 0.0;
                    }
                }
                match result {
                    Ok(Ok(())) => state.lock().unwrap().phase = ServicePhase::Stopped,
                    Ok(Err(error)) => fail(&state, format!("{error:#}")),
                    Err(error) => fail(&state, format!("Service task failed: {error}")),
                }
            }
            _ = tick.tick() => {
                if let Some(metrics) = &metrics {
                    let mut state = state.lock().unwrap();
                    state.cas_entries = metrics.cas_entry_count.get().max(0) as u64;
                    state.cas_bytes = metrics.cas_size_bytes.get().max(0) as u64;
                    state.ac_entries = metrics.ac_entry_count.get().max(0) as u64;
                    state.hits = metrics.cas_hit_total.get() + metrics.ac_hit_total.get();
                    state.misses = metrics.cas_miss_total.get() + metrics.ac_miss_total.get();
                    state.uploaded_bytes = metrics.upload_bytes_total.get();
                    state.downloaded_bytes = metrics.download_bytes_total.get();
                    if job.is_some() {
                        state.uptime_secs = started.elapsed().as_secs();
                    }
                    // Real elapsed time between samples; tick jitter cannot skew the rate.
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_sample).as_secs_f64();
                    last_sample = now;
                    if let Some(traffic) = metrics.traffic() {
                        traffic.sample_rates(elapsed);
                        publish_traffic(&mut state, traffic, &selection);
                        if elapsed > 0.0 && job.is_some() {
                            state.network_upload_bytes_per_sec = state
                                .network_uploaded_bytes
                                .saturating_sub(last_network.0) as f64 / elapsed;
                            state.network_download_bytes_per_sec = state
                                .network_downloaded_bytes
                                .saturating_sub(last_network.1) as f64 / elapsed;
                        } else {
                            state.network_upload_bytes_per_sec = 0.0;
                            state.network_download_bytes_per_sec = 0.0;
                        }
                        last_network = (state.network_uploaded_bytes, state.network_downloaded_bytes);
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct LogSink(Logs);
struct LogWriter {
    logs: Logs,
    bytes: Vec<u8>,
}
impl<'a> MakeWriter<'a> for LogSink {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter {
            logs: self.0.clone(),
            bytes: Vec::new(),
        }
    }
}
impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = 8192_usize.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&bytes[..remaining.min(bytes.len())]);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Drop for LogWriter {
    fn drop(&mut self) {
        let mut logs = self.logs.lock().unwrap();
        for line in String::from_utf8_lossy(&self.bytes).lines() {
            if logs.len() == 200 {
                logs.pop_front();
            }
            logs.push_back(line.to_string());
        }
    }
}
