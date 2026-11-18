//! `Capabilities.GetCapabilities`.
//!
//! Doc §4 requires this to describe what the server *actually* does: SHA-256
//! only, no compression, no remote execution, with the batch ceiling the CAS
//! service really enforces.

use std::sync::Arc;
use std::time::Instant;

use tonic::{Request, Response, Status};

use super::ServiceContext;
use crate::traffic::TrafficKind;
use crate::proto::SemVer;
use crate::proto::reapi::capabilities_server::Capabilities;
use crate::proto::reapi::{
    ActionCacheUpdateCapabilities, CacheCapabilities, ExecutionCapabilities, GetCapabilitiesRequest,
    ServerCapabilities, compressor, digest_function, symlink_absolute_path_strategy,
};

/// Lowest REAPI version this server speaks.
const LOW_API_VERSION: (i32, i32) = (2, 0);
/// Highest REAPI version this server speaks.
const HIGH_API_VERSION: (i32, i32) = (2, 3);

pub struct CapabilitiesSvc {
    ctx: Arc<ServiceContext>,
}

impl CapabilitiesSvc {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }
}

fn version(v: (i32, i32)) -> SemVer {
    SemVer { major: v.0, minor: v.1, patch: 0, prerelease: String::new() }
}

#[tonic::async_trait]
impl Capabilities for CapabilitiesSvc {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        let started = Instant::now();
        let session = self.ctx.session_of(&request);
        let instance = if self.ctx.tracing_traffic() {
            request.into_inner().instance_name
        } else {
            String::new()
        };

        let caps = ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_functions: vec![digest_function::Value::Sha256 as i32],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                    update_enabled: true,
                }),
                cache_priority_capabilities: None,
                max_batch_total_size_bytes: self.ctx.max_batch_bytes(),
                symlink_absolute_path_strategy: symlink_absolute_path_strategy::Value::Disallowed
                    as i32,
                // V1 stores and transfers raw bytes only (Doc §17).
                supported_compressors: vec![compressor::Value::Identity as i32],
                supported_batch_update_compressors: vec![compressor::Value::Identity as i32],
                // 0 = no server-imposed per-blob ceiling; capacity is the disk budget.
                max_cas_blob_size_bytes: 0,
                split_blob_support: false,
                splice_blob_support: false,
                fast_cdc_2020_params: None,
                rep_max_cdc_params: None,
            }),
            execution_capabilities: Some(ExecutionCapabilities {
                digest_function: digest_function::Value::Sha256 as i32,
                // Cache only: this server never executes actions (Doc §1).
                exec_enabled: false,
                execution_priority_capabilities: None,
                supported_node_properties: Vec::new(),
                digest_functions: vec![digest_function::Value::Sha256 as i32],
            }),
            deprecated_api_version: None,
            low_api_version: Some(version(LOW_API_VERSION)),
            high_api_version: Some(version(HIGH_API_VERSION)),
        };

        self.ctx.metrics.observe_request("Capabilities.GetCapabilities", started);
        self.ctx.trace(crate::traffic::RequestTrace { session, started }, TrafficKind::Query, "Capabilities.GetCapabilities", "OK", 0, || {
            if instance.is_empty() { "default instance".to_string() } else { instance }
        });
        Ok(Response::new(caps))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::testutil::{TempRoot, context_with};

    #[tokio::test]
    async fn advertises_only_what_is_implemented() {
        let root = TempRoot::new("caps");
        let ctx = context_with(&root, |cfg| {
            cfg.server.max_batch_total_size_bytes = 1234;
        })
        .await;
        let svc = CapabilitiesSvc::new(ctx);

        let caps = svc
            .get_capabilities(Request::new(GetCapabilitiesRequest {
                instance_name: "repo-a".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        let cache = caps.cache_capabilities.expect("cache capabilities");
        assert_eq!(cache.digest_functions, vec![digest_function::Value::Sha256 as i32]);
        assert!(cache.action_cache_update_capabilities.unwrap().update_enabled);
        assert_eq!(cache.max_batch_total_size_bytes, 1234);
        assert_eq!(
            cache.symlink_absolute_path_strategy,
            symlink_absolute_path_strategy::Value::Disallowed as i32
        );
        assert_eq!(cache.supported_compressors, vec![compressor::Value::Identity as i32]);
        assert_eq!(
            cache.supported_batch_update_compressors,
            vec![compressor::Value::Identity as i32]
        );
        assert!(!cache.split_blob_support);
        assert!(!cache.splice_blob_support);

        let exec = caps.execution_capabilities.expect("execution capabilities");
        assert!(!exec.exec_enabled);
        assert_eq!(exec.digest_function, digest_function::Value::Sha256 as i32);
        assert_eq!(exec.digest_functions, vec![digest_function::Value::Sha256 as i32]);

        let low = caps.low_api_version.unwrap();
        let high = caps.high_api_version.unwrap();
        assert_eq!((low.major, low.minor), LOW_API_VERSION);
        assert_eq!((high.major, high.minor), HIGH_API_VERSION);
    }
}
