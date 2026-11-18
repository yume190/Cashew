//! Opt-in capture of real traffic for the desktop shell, grouped by TCP session.
//!
//! Every accepted TCP connection becomes one [`TrafficSession`]. The accepted
//! socket is wrapped in [`ObservedIo`], which counts the bytes that really cross
//! the socket (HTTP/2 and TLS framing included, TCP/IP headers excluded) from the
//! moment of accept, and closes the session when the socket is dropped.
//!
//! Handlers report one [`TrafficEvent`] per request (streaming reads report when
//! the stream ends, fails, or is dropped by the client). A handler finds its
//! session through the request's remote address, which is unique among live
//! connections. The log is created only when
//! [`Metrics::enable_traffic`](crate::observability::Metrics::enable_traffic)
//! is called; the CLI server wraps sockets with no observer, so it never
//! allocates session state, takes a lock, or records a byte.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::{Connected, TcpConnectInfo};

/// Newest events retained per session.
pub(crate) const MAX_EVENTS: usize = 200;
/// Newest closed sessions retained. Live sessions are always retained.
pub(crate) const MAX_CLOSED_SESSIONS: usize = 200;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrafficKind {
    Upload,
    Query,
    Download,
}

/// Identity and start time captured for one RPC, including streaming completion.
pub(crate) struct RequestTrace {
    pub session: Option<u64>,
    pub started: Instant,
}

#[derive(Clone, Debug)]
pub struct TrafficEvent {
    pub id: u64,
    /// Session (one TCP connection) that issued the request.
    pub session_id: u64,
    /// Unix epoch milliseconds when the request completed.
    pub timestamp_ms: u64,
    pub kind: TrafficKind,
    pub method: &'static str,
    /// Canonical gRPC code name (`OK`, `NOT_FOUND`, ...) or `PARTIAL` for a
    /// batch where some entries succeeded and some failed.
    pub status: &'static str,
    /// Payload bytes actually moved (blob data, or encoded ActionResult).
    pub bytes: u64,
    pub detail: String,
    pub duration_ms: f64,
}

/// One accepted TCP connection. Byte counts are real socket bytes (HTTP/2 and
/// TLS overhead included, TCP/IP headers excluded); request counts come from
/// completed operations.
#[derive(Clone, Debug)]
pub struct TrafficSession {
    pub id: u64,
    pub peer_addr: SocketAddr,
    /// Unix epoch milliseconds.
    pub started_at_ms: u64,
    /// `None` while the socket is still connected.
    pub ended_at_ms: Option<u64>,
    pub last_activity_ms: u64,
    /// Bytes received from the client.
    pub uploaded_bytes: u64,
    /// Bytes sent to the client.
    pub downloaded_bytes: u64,
    pub upload_bytes_per_sec: f64,
    pub download_bytes_per_sec: f64,
    pub upload_requests: u64,
    pub query_requests: u64,
    pub download_requests: u64,
}

/// Lock-free per-socket counters, shared between the IO wrapper and the log.
struct SocketCounters {
    uploaded: AtomicU64,
    downloaded: AtomicU64,
    last_activity_ms: AtomicU64,
}

struct Session {
    info: TrafficSession,
    counters: Arc<SocketCounters>,
    /// Byte counts at the previous rate sample.
    sampled: (u64, u64),
    /// Newest last.
    events: VecDeque<TrafficEvent>,
}

#[derive(Default)]
struct State {
    /// Ordered by id, so the newest session is the last entry.
    sessions: BTreeMap<u64, Session>,
    /// Live connections only; a peer address is unique among them.
    peers: HashMap<SocketAddr, u64>,
    /// Closed session ids, oldest first.
    closed: VecDeque<u64>,
}

/// Request totals, wire totals, and the session registry.
#[derive(Default)]
pub(crate) struct TrafficLog {
    state: Mutex<State>,
    next_session_id: AtomicU64,
    next_event_id: AtomicU64,
    upload: AtomicU64,
    query: AtomicU64,
    download: AtomicU64,
    network_uploaded: AtomicU64,
    network_downloaded: AtomicU64,
}

pub(crate) struct TrafficTotals {
    pub upload: u64,
    pub query: u64,
    pub download: u64,
    pub network_uploaded: u64,
    pub network_downloaded: u64,
}

/// Sessions newest first, the effective selection, and that session's events
/// newest first.
pub(crate) struct TrafficSnapshot {
    pub sessions: Vec<TrafficSession>,
    pub selected_session_id: Option<u64>,
    pub events: Vec<TrafficEvent>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

impl TrafficLog {
    /// Register an accepted connection. Dropping the returned observer closes
    /// the session.
    pub(crate) fn open(self: &Arc<Self>, peer_addr: SocketAddr) -> IoObserver {
        let now = now_ms();
        let counters = Arc::new(SocketCounters {
            uploaded: AtomicU64::new(0),
            downloaded: AtomicU64::new(0),
            last_activity_ms: AtomicU64::new(now),
        });
        let mut state = self.state.lock().unwrap();
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed) + 1;
        state.peers.insert(peer_addr, id);
        state.sessions.insert(
            id,
            Session {
                info: TrafficSession {
                    id,
                    peer_addr,
                    started_at_ms: now,
                    ended_at_ms: None,
                    last_activity_ms: now,
                    uploaded_bytes: 0,
                    downloaded_bytes: 0,
                    upload_bytes_per_sec: 0.0,
                    download_bytes_per_sec: 0.0,
                    upload_requests: 0,
                    query_requests: 0,
                    download_requests: 0,
                },
                counters: counters.clone(),
                sampled: (0, 0),
                events: VecDeque::new(),
            },
        );
        drop(state);
        IoObserver {
            log: self.clone(),
            session_id: id,
            peer_addr,
            counters,
        }
    }

    fn close(&self, id: u64, peer_addr: SocketAddr) {
        let now = now_ms();
        let mut state = self.state.lock().unwrap();
        if state.peers.get(&peer_addr) == Some(&id) {
            state.peers.remove(&peer_addr);
        }
        let Some(session) = state.sessions.get_mut(&id) else {
            return;
        };
        session.info.ended_at_ms = Some(now);
        state.closed.push_back(id);
        while state.closed.len() > MAX_CLOSED_SESSIONS {
            if let Some(old) = state.closed.pop_front() {
                state.sessions.remove(&old);
            }
        }
    }

    /// Session of the live connection with this remote address.
    pub(crate) fn session_for_peer(&self, peer_addr: SocketAddr) -> Option<u64> {
        self.state.lock().unwrap().peers.get(&peer_addr).copied()
    }

    /// Count one finished request. Global totals always advance; the event and
    /// the per-session counter are kept only while the session is retained.
    /// `session` is `None` when the request carried no mapped connection, in
    /// which case no event is invented.
    pub(crate) fn record(
        &self,
        request: RequestTrace,
        kind: TrafficKind,
        method: &'static str,
        status: &'static str,
        bytes: u64,
        detail: String,
    ) {
        let counter = match kind {
            TrafficKind::Upload => &self.upload,
            TrafficKind::Query => &self.query,
            TrafficKind::Download => &self.download,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let Some(session_id) = request.session else {
            return;
        };
        let duration_ms = request.started.elapsed().as_secs_f64() * 1000.0;
        let timestamp_ms = now_ms();
        let mut state = self.state.lock().unwrap();
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return;
        };
        let requests = match kind {
            TrafficKind::Upload => &mut session.info.upload_requests,
            TrafficKind::Query => &mut session.info.query_requests,
            TrafficKind::Download => &mut session.info.download_requests,
        };
        *requests += 1;
        // Allocate the id under the lock so ring order equals id order.
        let id = self.next_event_id.fetch_add(1, Ordering::Relaxed) + 1;
        if session.events.len() == MAX_EVENTS {
            session.events.pop_front();
        }
        session.events.push_back(TrafficEvent {
            id,
            session_id,
            timestamp_ms,
            kind,
            method,
            status,
            bytes,
            detail,
            duration_ms,
        });
    }

    pub(crate) fn totals(&self) -> TrafficTotals {
        TrafficTotals {
            upload: self.upload.load(Ordering::Relaxed),
            query: self.query.load(Ordering::Relaxed),
            download: self.download.load(Ordering::Relaxed),
            network_uploaded: self.network_uploaded.load(Ordering::Relaxed),
            network_downloaded: self.network_downloaded.load(Ordering::Relaxed),
        }
    }

    /// Recompute per-session rates over the real `elapsed_secs` since the last
    /// call. Closed sessions report no rate.
    pub(crate) fn sample_rates(&self, elapsed_secs: f64) {
        if elapsed_secs <= 0.0 {
            return;
        }
        let mut state = self.state.lock().unwrap();
        for session in state.sessions.values_mut() {
            let up = session.counters.uploaded.load(Ordering::Relaxed);
            let down = session.counters.downloaded.load(Ordering::Relaxed);
            let live = session.info.ended_at_ms.is_none();
            session.info.upload_bytes_per_sec = if live {
                up.saturating_sub(session.sampled.0) as f64 / elapsed_secs
            } else {
                0.0
            };
            session.info.download_bytes_per_sec = if live {
                down.saturating_sub(session.sampled.1) as f64 / elapsed_secs
            } else {
                0.0
            };
            session.sampled = (up, down);
        }
    }

    /// Resolve `requested` (falling back to the newest session when it is
    /// `None` or no longer retained) and copy only that session's events.
    pub(crate) fn snapshot(&self, requested: Option<u64>) -> TrafficSnapshot {
        let state = self.state.lock().unwrap();
        let selected = requested
            .filter(|id| state.sessions.contains_key(id))
            .or_else(|| state.sessions.keys().next_back().copied());
        let sessions = state
            .sessions
            .values()
            .rev()
            .map(|session| {
                let mut info = session.info.clone();
                info.uploaded_bytes = session.counters.uploaded.load(Ordering::Relaxed);
                info.downloaded_bytes = session.counters.downloaded.load(Ordering::Relaxed);
                info.last_activity_ms = session.counters.last_activity_ms.load(Ordering::Relaxed);
                info
            })
            .collect();
        let events = selected
            .and_then(|id| state.sessions.get(&id))
            .map(|session| session.events.iter().rev().cloned().collect())
            .unwrap_or_default();
        TrafficSnapshot {
            sessions,
            selected_session_id: selected,
            events,
        }
    }
}

/// Owned by the wrapped socket: counts its bytes and closes its session on drop.
pub(crate) struct IoObserver {
    log: Arc<TrafficLog>,
    session_id: u64,
    peer_addr: SocketAddr,
    counters: Arc<SocketCounters>,
}

impl IoObserver {
    fn received(&self, n: usize) {
        if n == 0 {
            return;
        }
        let n = n as u64;
        self.counters.uploaded.fetch_add(n, Ordering::Relaxed);
        self.log.network_uploaded.fetch_add(n, Ordering::Relaxed);
        self.counters
            .last_activity_ms
            .store(now_ms(), Ordering::Relaxed);
    }

    fn sent(&self, n: usize) {
        if n == 0 {
            return;
        }
        let n = n as u64;
        self.counters.downloaded.fetch_add(n, Ordering::Relaxed);
        self.log.network_downloaded.fetch_add(n, Ordering::Relaxed);
        self.counters
            .last_activity_ms
            .store(now_ms(), Ordering::Relaxed);
    }
}

impl Drop for IoObserver {
    fn drop(&mut self) {
        self.log.close(self.session_id, self.peer_addr);
    }
}

/// A socket that optionally reports the bytes it moves. With no observer it
/// only delegates.
pub(crate) struct ObservedIo<S> {
    inner: S,
    observer: Option<IoObserver>,
}

impl<S> ObservedIo<S> {
    pub(crate) fn new(inner: S, observer: Option<IoObserver>) -> Self {
        Self { inner, observer }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for ObservedIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(observer) = &this.observer else {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        };
        let before = buf.filled().len();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            observer.received(buf.filled().len().saturating_sub(before));
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ObservedIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, data);
        if let (Poll::Ready(Ok(n)), Some(observer)) = (&poll, &this.observer) {
            observer.sent(*n);
        }
        poll
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        if let (Poll::Ready(Ok(n)), Some(observer)) = (&poll, &this.observer) {
            observer.sent(*n);
        }
        poll
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Same connect info as the bare socket, so TLS/mTLS wrapping and
/// `Request::remote_addr` behave exactly as without the wrapper.
impl<S: Connected<ConnectInfo = TcpConnectInfo>> Connected for ObservedIo<S> {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn peer(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn record(log: &TrafficLog, session: Option<u64>, kind: TrafficKind, bytes: u64) {
        log.record(
            crate::traffic::RequestTrace {
                session,
                started: Instant::now(),
            },
            kind,
            "m",
            "OK",
            bytes,
            String::new(),
        );
    }

    #[test]
    fn event_ring_is_bounded_per_session_newest_first_and_counts_every_request() {
        let log = Arc::new(TrafficLog::default());
        let observer = log.open(peer(1000));
        let id = observer.session_id;
        for i in 0..(MAX_EVENTS + 5) {
            record(&log, Some(id), TrafficKind::Query, i as u64);
        }
        record(&log, Some(id), TrafficKind::Upload, 1);
        let snapshot = log.snapshot(None);
        assert_eq!(snapshot.events.len(), MAX_EVENTS);
        assert_eq!(snapshot.events[0].kind, TrafficKind::Upload);
        assert!(snapshot.events.windows(2).all(|w| w[0].id > w[1].id));
        assert!(snapshot.events.iter().all(|e| e.session_id == id));
        let totals = log.totals();
        assert_eq!((totals.query, totals.upload, totals.download), (205, 1, 0));
        // Per-session counts survive event eviction.
        let session = &snapshot.sessions[0];
        assert_eq!((session.query_requests, session.upload_requests), (205, 1));
    }

    #[test]
    fn each_accept_is_its_own_session_and_reconnect_gets_a_new_id() {
        let log = Arc::new(TrafficLog::default());
        let a = log.open(peer(1000));
        let b = log.open(peer(1001));
        assert_ne!(a.session_id, b.session_id);
        assert_eq!(log.session_for_peer(peer(1000)), Some(a.session_id));
        assert_eq!(log.session_for_peer(peer(1001)), Some(b.session_id));

        let first = a.session_id;
        drop(a);
        assert_eq!(log.session_for_peer(peer(1000)), None);
        // Same address reconnecting is a different session.
        let again = log.open(peer(1000));
        assert_ne!(again.session_id, first);
        assert_eq!(log.session_for_peer(peer(1000)), Some(again.session_id));

        let snapshot = log.snapshot(None);
        assert_eq!(snapshot.sessions.len(), 3);
        // Newest first; the old one is closed, the others live.
        assert_eq!(snapshot.sessions[0].id, again.session_id);
        let old = snapshot.sessions.iter().find(|s| s.id == first).unwrap();
        assert!(old.ended_at_ms.is_some());
        assert!(
            snapshot
                .sessions
                .iter()
                .filter(|s| s.ended_at_ms.is_none())
                .count()
                == 2
        );
    }

    #[test]
    fn events_stay_with_their_original_session_after_close_and_reconnect() {
        let log = Arc::new(TrafficLog::default());
        let a = log.open(peer(1000));
        let first = a.session_id;
        drop(a);
        // A stream guard that outlives the socket still reports to its session.
        record(&log, Some(first), TrafficKind::Download, 7);
        let b = log.open(peer(1000));
        record(&log, Some(b.session_id), TrafficKind::Query, 0);

        let old = log.snapshot(Some(first));
        assert_eq!(old.selected_session_id, Some(first));
        assert_eq!(old.events.len(), 1);
        assert_eq!(old.events[0].kind, TrafficKind::Download);
        let new = log.snapshot(None);
        assert_eq!(new.selected_session_id, Some(b.session_id));
        assert_eq!(new.events.len(), 1);
        assert_eq!(new.events[0].kind, TrafficKind::Query);
    }

    #[test]
    fn closed_sessions_are_bounded_but_live_sessions_and_totals_are_not() {
        let log = Arc::new(TrafficLog::default());
        let live = log.open(peer(1));
        let mut first_closed = None;
        for port in 0..(MAX_CLOSED_SESSIONS + 3) {
            let observer = log.open(peer(2000 + port as u16));
            first_closed.get_or_insert(observer.session_id);
            record(&log, Some(observer.session_id), TrafficKind::Query, 0);
        }
        let snapshot = log.snapshot(first_closed);
        // The oldest closed session was evicted: selection falls back to newest.
        assert!(snapshot.sessions.iter().all(|s| Some(s.id) != first_closed));
        assert_eq!(
            snapshot
                .sessions
                .iter()
                .filter(|s| s.ended_at_ms.is_some())
                .count(),
            MAX_CLOSED_SESSIONS
        );
        assert!(snapshot.sessions.iter().any(|s| s.id == live.session_id));
        assert_eq!(snapshot.selected_session_id, Some(snapshot.sessions[0].id));
        assert_eq!(log.totals().query, (MAX_CLOSED_SESSIONS + 3) as u64);
        // Late events for an evicted session still count globally but add no row.
        record(&log, first_closed, TrafficKind::Upload, 1);
        assert_eq!(log.totals().upload, 1);
    }

    #[test]
    fn unmapped_requests_count_but_never_invent_a_session() {
        let log = Arc::new(TrafficLog::default());
        record(&log, None, TrafficKind::Query, 0);
        let snapshot = log.snapshot(None);
        assert!(snapshot.sessions.is_empty());
        assert_eq!(snapshot.selected_session_id, None);
        assert!(snapshot.events.is_empty());
        assert_eq!(log.totals().query, 1);
    }

    #[test]
    fn selection_follows_newest_unless_pinned() {
        let log = Arc::new(TrafficLog::default());
        let a = log.open(peer(1));
        assert_eq!(log.snapshot(None).selected_session_id, Some(a.session_id));
        let b = log.open(peer(2));
        assert_eq!(log.snapshot(None).selected_session_id, Some(b.session_id));
        assert_eq!(
            log.snapshot(Some(a.session_id)).selected_session_id,
            Some(a.session_id)
        );
    }

    #[test]
    fn rates_are_sampled_per_live_session() {
        let log = Arc::new(TrafficLog::default());
        let a = log.open(peer(1));
        a.received(1000);
        a.sent(500);
        log.sample_rates(0.5);
        let snapshot = log.snapshot(None);
        let session = &snapshot.sessions[0];
        assert_eq!(session.upload_bytes_per_sec, 2000.0);
        assert_eq!(session.download_bytes_per_sec, 1000.0);
        log.sample_rates(0.5);
        assert_eq!(log.snapshot(None).sessions[0].upload_bytes_per_sec, 0.0);
        let id = a.session_id;
        drop(a);
        log.sample_rates(0.5);
        let closed = &log.snapshot(None).sessions[0];
        assert_eq!(closed.id, id);
        assert_eq!(
            (closed.uploaded_bytes, closed.downloaded_bytes),
            (1000, 500)
        );
        let totals = log.totals();
        assert_eq!(
            (totals.network_uploaded, totals.network_downloaded),
            (1000, 500)
        );
    }

    #[tokio::test]
    async fn wrapper_counts_exact_socket_bytes_in_both_directions() {
        let log = Arc::new(TrafficLog::default());
        let (client, server) = tokio::io::duplex(64);
        let observer = log.open(peer(1));
        let id = observer.session_id;
        let mut server = ObservedIo::new(server, Some(observer));
        let mut client = client;

        client.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 16];
        let n = server.read(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        server.write_all(b"worlds!").await.unwrap();
        server.flush().await.unwrap();
        let mut reply = [0u8; 7];
        client.read_exact(&mut reply).await.unwrap();

        let snapshot = log.snapshot(Some(id));
        assert_eq!(snapshot.sessions[0].uploaded_bytes, 5);
        assert_eq!(snapshot.sessions[0].downloaded_bytes, 7);
        assert!(snapshot.sessions[0].ended_at_ms.is_none());
        drop(server);
        assert!(log.snapshot(Some(id)).sessions[0].ended_at_ms.is_some());
    }

    #[tokio::test]
    async fn wrapper_without_observer_only_delegates() {
        let (client, server) = tokio::io::duplex(64);
        let mut server = ObservedIo::new(server, None);
        let mut client = client;
        client.write_all(b"abc").await.unwrap();
        let mut buf = [0u8; 3];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"abc");
    }
}
