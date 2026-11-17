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
