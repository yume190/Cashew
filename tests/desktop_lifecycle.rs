use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bazel_cache_server::desktop::{DesktopController, DesktopSettings, ServicePhase};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);
struct TempRoot(PathBuf);
impl TempRoot {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
                "cashew-lifecycle-{}-{}-{}",
                std::process::id(),
                NEXT_ROOT.fetch_add(1, Ordering::Relaxed),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
    }
    fn settings(&self) -> DesktopSettings {
        DesktopSettings {
            root: self.0.to_string_lossy().into_owned(),
            listen: free_address(),
            metrics_listen: free_address(),
            max_size: "32MiB".into(),
            config_file: String::new(),
        }
    }
}
impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn free_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}
async fn phase(controller: &DesktopController, expected: ServicePhase) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let snapshot = controller.snapshot();
            if snapshot.phase == expected {
                return;
            }
            assert!(
                snapshot.phase != ServicePhase::Failed || expected == ServicePhase::Failed,
                "unexpected service failure: {:?}",
                snapshot.error
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "expected {expected:?}, got {:?}: {:?}",
            controller.snapshot().phase,
            controller.snapshot().error
        )
    });
}

#[tokio::test]
async fn failed_bind_can_retry_and_stop_releases_both_listeners() {
    let root = TempRoot::new();
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut settings = root.settings();
    settings.listen = occupied.local_addr().unwrap().to_string();
    let mut controller = DesktopController::new().unwrap();
    controller.start(settings.clone()).unwrap();
    phase(&controller, ServicePhase::Failed).await;
    assert!(controller.snapshot().endpoint.is_none());
    drop(occupied);

    for _ in 0..2 {
        controller.start(settings.clone()).unwrap();
        phase(&controller, ServicePhase::Running).await;
        let ready = reqwest::get(format!("http://{}/ready", settings.metrics_listen))
            .await
            .unwrap();
        assert!(ready.status().is_success());
        controller.stop();
        phase(&controller, ServicePhase::Stopped).await;
        let _grpc = TcpListener::bind(&settings.listen).unwrap();
        let _metrics = TcpListener::bind(&settings.metrics_listen).unwrap();
    }
}

#[tokio::test]
async fn occupied_metrics_port_does_not_leave_grpc_running() {
    let root = TempRoot::new();
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut settings = root.settings();
    settings.metrics_listen = occupied.local_addr().unwrap().to_string();
    let mut controller = DesktopController::new().unwrap();
    controller.start(settings.clone()).unwrap();
    phase(&controller, ServicePhase::Failed).await;
    assert!(controller.snapshot().endpoint.is_none());
    let _grpc = TcpListener::bind(&settings.listen).unwrap();
    drop((_grpc, occupied));
    controller.start(settings).unwrap();
    phase(&controller, ServicePhase::Running).await;
}

#[tokio::test]
async fn stop_during_startup_and_drop_running_controller_release_resources() {
    let root = TempRoot::new();
    let settings = root.settings();
    let mut controller = DesktopController::new().unwrap();
    controller.start(settings.clone()).unwrap();
    controller.stop();
    phase(&controller, ServicePhase::Stopped).await;
    controller.start(settings.clone()).unwrap();
    phase(&controller, ServicePhase::Running).await;
    drop(controller);
    let _grpc = TcpListener::bind(&settings.listen).unwrap();
    let _metrics = TcpListener::bind(&settings.metrics_listen).unwrap();
    drop((_grpc, _metrics));
    // A later process must be able to open the same SQLite database and store.
    let mut replacement = DesktopController::new().unwrap();
    replacement.start(settings).unwrap();
    phase(&replacement, ServicePhase::Running).await;
}

#[allow(unused_imports)]
#[path = "../src/proto.rs"]
mod proto;

use bazel_cache_server::desktop::DesktopSnapshot;
use proto::reapi::{Digest, FindMissingBlobsRequest, FindMissingBlobsResponse, GetCapabilitiesRequest, ServerCapabilities};

async fn snapshot_where(
    controller: &DesktopController,
    what: &str,
    mut ready: impl FnMut(&DesktopSnapshot) -> bool,
) -> DesktopSnapshot {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let snapshot = controller.snapshot();
            if ready(&snapshot) {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}: {:?}", controller.snapshot().sessions))
}

async fn unary<Req, Resp>(grpc: &mut tonic::client::Grpc<tonic::transport::Channel>, path: &'static str, request: Req) -> Resp
where
    Req: prost::Message + Send + Sync + 'static,
    Resp: prost::Message + Default + Send + Sync + 'static,
{
    grpc.ready().await.unwrap();
    grpc.unary(
        tonic::Request::new(request),
        tonic::codegen::http::uri::PathAndQuery::from_static(path),
        tonic_prost::ProstCodec::<Req, Resp>::default(),
    )
    .await
    .unwrap()
    .into_inner()
}

async fn connect(endpoint: &str) -> tonic::client::Grpc<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{endpoint}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    tonic::client::Grpc::new(channel)
}

const CAPABILITIES: &str = "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities";
const FIND_MISSING: &str = "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs";

async fn get_capabilities(grpc: &mut tonic::client::Grpc<tonic::transport::Channel>) {
    unary::<_, ServerCapabilities>(grpc, CAPABILITIES, GetCapabilitiesRequest::default()).await;
}

#[tokio::test]
async fn stopped_service_keeps_every_closed_session_selectable_until_restart() {
    let root = TempRoot::new();
    let settings = root.settings();
    let mut controller = DesktopController::new().unwrap();
    controller.start(settings.clone()).unwrap();
    phase(&controller, ServicePhase::Running).await;

    // Connection A issues two requests, connection B one, so the sessions are
    // told apart by their real request counts.
    let mut first = connect(&settings.listen).await;
    get_capabilities(&mut first).await;
    unary::<_, FindMissingBlobsResponse>(
        &mut first,
        FIND_MISSING,
        FindMissingBlobsRequest {
            blob_digests: vec![Digest { hash: "ab".repeat(32), size_bytes: 7 }],
            ..Default::default()
        },
    )
    .await;
    let mut second = connect(&settings.listen).await;
    get_capabilities(&mut second).await;
    drop((first, second));

    let live = snapshot_where(&controller, "two closed sessions with 2 and 1 requests", |s| {
        let mut counts: Vec<u64> = s.sessions.iter().map(|x| x.query_requests).collect();
        counts.sort_unstable();
        s.sessions.len() == 2 && counts == [1, 2] && s.sessions.iter().all(|x| x.ended_at_ms.is_some())
    })
    .await;
    let id_of = |snapshot: &DesktopSnapshot, queries: u64| {
        snapshot.sessions.iter().find(|x| x.query_requests == queries).unwrap().id
    };
    let (first_id, second_id) = (id_of(&live, 2), id_of(&live, 1));
    assert_ne!(first_id, second_id);

    controller.stop();
    phase(&controller, ServicePhase::Stopped).await;

    for (id, methods) in [
        (first_id, vec!["CAS.FindMissingBlobs", "Capabilities.GetCapabilities"]),
        (second_id, vec!["Capabilities.GetCapabilities"]),
        (first_id, vec!["CAS.FindMissingBlobs", "Capabilities.GetCapabilities"]),
    ] {
        controller.select_session(Some(id));
        let shown = snapshot_where(&controller, "stopped history of the selected session", |s| {
            s.phase == ServicePhase::Stopped
                && s.selected_session_id == Some(id)
                && s.events.len() == methods.len()
                && s.events.iter().all(|e| e.session_id == id)
        })
        .await;
        let mut seen: Vec<&str> = shown.events.iter().map(|e| e.method).collect();
        seen.sort_unstable();
        assert_eq!(seen, methods);
        assert_eq!(shown.sessions.len(), 2, "closed sessions must stay listed");
        assert_eq!(shown.network_upload_bytes_per_sec, 0.0);
        assert_eq!(shown.network_download_bytes_per_sec, 0.0);
        for session in &shown.sessions {
            assert!(session.ended_at_ms.is_some());
            assert_eq!(session.upload_bytes_per_sec, 0.0);
            assert_eq!(session.download_bytes_per_sec, 0.0);
        }
    }

    controller.start(settings).unwrap();
    phase(&controller, ServicePhase::Running).await;
    let fresh = snapshot_where(&controller, "old session history cleared by restart", |s| {
        s.phase == ServicePhase::Running && s.sessions.is_empty() && s.events.is_empty()
    })
    .await;
    assert_eq!((fresh.query_requests, fresh.upload_requests, fresh.download_requests), (0, 0, 0));
    assert_eq!((fresh.network_uploaded_bytes, fresh.network_downloaded_bytes), (0, 0));
}
