//! `ActionCache.GetActionResult` / `UpdateActionResult` (Doc §6, §10).
//!
//! Entries live in SQLite, namespaced by `instance_name`; the blobs they point
//! at live in the global CAS. Consistency is *lazy* (Doc §10):
//!
//! ```text
//! write:  every referenced blob must already exist  -> else FAILED_PRECONDITION
//! read:   re-validate referenced blobs with stat(2) -> missing: drop entry, NOT_FOUND
//! ```
//!
//! No refcounts and no AC -> CAS dependency graph: GC may evict a blob an entry
//! points at, and the next read repairs the index.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use prost::Message as _;
use tonic::{Request, Response, Status};
use tracing::warn;

use super::{ServiceContext, check_digest_function, internal, parse_digest, stat_many};
use crate::digest::Digest;
use crate::proto::reapi::action_cache_server::ActionCache;
use crate::proto::reapi::{ActionResult, GetActionResultRequest, UpdateActionResultRequest};

pub struct ActionCacheSvc {
    ctx: Arc<ServiceContext>,
}

impl ActionCacheSvc {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }
}

/// Every CAS blob an `ActionResult` references directly.
///
/// `Tree` payloads are *not* walked: a tree's own blob proves the directory
/// structure was uploaded, and walking it would mean parsing arbitrarily large
/// protos on every cache hit.
fn referenced_digests(result: &ActionResult) -> Result<Vec<Digest>, Status> {
    let mut out = Vec::with_capacity(result.output_files.len() + result.output_directories.len() + 2);
    for file in &result.output_files {
        if let Some(d) = file.digest.as_ref() {
            out.push(parse_digest(Some(d), &format!("output_files[{}].digest", file.path))?);
        }
    }
    for dir in &result.output_directories {
        if let Some(d) = dir.tree_digest.as_ref() {
            out.push(parse_digest(
                Some(d),
                &format!("output_directories[{}].tree_digest", dir.path),
            )?);
        }
    }
    if let Some(d) = result.stdout_digest.as_ref() {
        out.push(parse_digest(Some(d), "stdout_digest")?);
    }
    if let Some(d) = result.stderr_digest.as_ref() {
        out.push(parse_digest(Some(d), "stderr_digest")?);
    }
    // One blob can be referenced by several outputs; stat it (and bill it) once.
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// Human readable list of missing blobs, in the `blobs/{hash}/{size}` form the
/// `google.rpc.PreconditionFailure` convention uses for its violation subjects.
fn missing_subjects(missing: &[Digest]) -> String {
    missing
        .iter()
        .map(|d| format!("blobs/{}/{}", d.hex(), d.size_bytes))
        .collect::<Vec<_>>()
        .join(", ")
}

impl ActionCacheSvc {
    /// Fill `stdout_raw` / `stderr_raw` / `OutputFile.contents` for the blobs
    /// the client asked to have inlined, as long as they fit in one message.
    async fn inline_blobs(
        &self,
        result: &mut ActionResult,
        req: &GetActionResultRequest,
    ) -> Result<(), Status> {
        let limit = self.ctx.cfg.server.max_batch_total_size_bytes;
        let storage = &self.ctx.storage;

        if req.inline_stdout
            && result.stdout_raw.is_empty()
            && let Some(d) = result.stdout_digest.as_ref()
        {
            let digest = parse_digest(Some(d), "stdout_digest")?;
            if digest.size_bytes as u64 <= limit
                && let Some(bytes) =
                    storage.read_all(&digest).await.map_err(|e| internal("read stdout", e))?
            {
                result.stdout_raw = bytes;
            }
        }
        if req.inline_stderr
            && result.stderr_raw.is_empty()
            && let Some(d) = result.stderr_digest.as_ref()
        {
            let digest = parse_digest(Some(d), "stderr_digest")?;
            if digest.size_bytes as u64 <= limit
                && let Some(bytes) =
                    storage.read_all(&digest).await.map_err(|e| internal("read stderr", e))?
            {
                result.stderr_raw = bytes;
            }
        }
        if !req.inline_output_files.is_empty() {
            let wanted: HashSet<&str> =
                req.inline_output_files.iter().map(String::as_str).collect();
            for file in result.output_files.iter_mut() {
                if !wanted.contains(file.path.as_str()) || !file.contents.is_empty() {
                    continue;
                }
                let Some(d) = file.digest.as_ref() else { continue };
                let digest = parse_digest(Some(d), "output_files[].digest")?;
                if digest.size_bytes as u64 > limit {
                    continue;
                }
                if let Some(bytes) = storage
                    .read_all(&digest)
                    .await
                    .map_err(|e| internal("read output file", e))?
                {
                    file.contents = bytes;
                }
            }
        }
        Ok(())
    }

    async fn get_inner(&self, req: GetActionResultRequest) -> Result<ActionResult, Status> {
        check_digest_function(req.digest_function)?;
        let action = parse_digest(req.action_digest.as_ref(), "action_digest")?;
        let instance = req.instance_name.as_str();
        let metrics = &self.ctx.metrics;

        let Some(stored) = self
            .ctx
            .db
            .get_action_result(instance, &action)
            .await
            .map_err(|e| internal("action cache lookup", e))?
        else {
            metrics.ac_miss_total.inc();
            return Err(Status::not_found(format!("no action result for {action}")));
        };

        let mut result = match ActionResult::decode(stored.as_slice()) {
            Ok(r) => r,
            Err(e) => {
                warn!(%action, error = %e, "dropping undecodable action cache entry");
                self.drop_entry(instance, &action).await;
                metrics.ac_miss_total.inc();
                return Err(Status::not_found(format!("no action result for {action}")));
            }
        };

        // Lazy validation: the entry is only a cache hit while every blob it
        // points at is still on disk (Doc §10).
        let referenced = match referenced_digests(&result) {
            Ok(d) => d,
            Err(status) => {
                warn!(%action, error = %status.message(), "dropping malformed action cache entry");
                self.drop_entry(instance, &action).await;
                metrics.ac_miss_total.inc();
                return Err(Status::not_found(format!("no action result for {action}")));
            }
        };
        let sizes = stat_many(&self.ctx.storage, &referenced).await?;
        let missing: Vec<Digest> = referenced
            .iter()
            .zip(&sizes)
            .filter(|(_, size)| size.is_none())
            .map(|(d, _)| *d)
            .collect();
        if !missing.is_empty() {
            warn!(
                %action,
                missing = missing.len(),
                "action result references evicted blobs; dropping entry"
            );
            self.drop_entry(instance, &action).await;
            metrics.ac_miss_total.inc();
            return Err(Status::not_found(format!("no action result for {action}")));
        }

        self.inline_blobs(&mut result, &req).await?;

        self.ctx.db.touch_action(instance, &action);
        self.ctx.db.touch_blobs(&referenced);
        metrics.ac_hit_total.inc();
        // The outputs the client just avoided rebuilding.
        let avoided: u64 = sizes.iter().map(|s| s.unwrap_or(0)).sum();
        metrics.avoided_bytes_total.inc_by(avoided);

        Ok(result)
    }

    /// Best-effort removal of a stale entry; a failure here only costs one more
    /// miss on the next read.
    async fn drop_entry(&self, instance: &str, action: &Digest) {
        if let Err(e) = self.ctx.db.remove_action_result(instance, action).await {
            warn!(%action, error = %e, "could not remove stale action cache entry");
        }
    }

    async fn update_inner(&self, req: UpdateActionResultRequest) -> Result<ActionResult, Status> {
        check_digest_function(req.digest_function)?;
        let action = parse_digest(req.action_digest.as_ref(), "action_digest")?;
        let result = req
            .action_result
            .ok_or_else(|| Status::invalid_argument("action_result is required"))?;

        let referenced = referenced_digests(&result)?;
        let sizes = stat_many(&self.ctx.storage, &referenced).await?;
        let missing: Vec<Digest> = referenced
            .iter()
            .zip(&sizes)
            .filter(|(_, size)| size.is_none())
            .map(|(d, _)| *d)
            .collect();
        if !missing.is_empty() {
            // PreconditionFailure semantics: violation type MISSING, one subject
            // per blob the client must upload before this result can be cached.
            return Err(Status::failed_precondition(format!(
                "MISSING: action result references {} blob(s) that are not in the CAS: {}",
                missing.len(),
                missing_subjects(&missing)
            )));
        }

        self.ctx
            .db
            .put_action_result(&req.instance_name, &action, result.encode_to_vec())
            .await
            .map_err(|e| internal("action cache write", e))?;
        self.ctx.db.touch_blobs(&referenced);

        Ok(result)
    }
}

#[tonic::async_trait]
impl ActionCache for ActionCacheSvc {
    async fn get_action_result(
        &self,
        request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let started = Instant::now();
        let out = self.get_inner(request.into_inner()).await;
        self.ctx.metrics.observe_request("ActionCache.GetActionResult", started);
        out.map(Response::new)
    }

    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let started = Instant::now();
        let principal = crate::service::principal_label(&request);
        let out = self.update_inner(request.into_inner()).await;
        tracing::debug!(%principal, ok = out.is_ok(), "UpdateActionResult");
        self.ctx.metrics.observe_request("ActionCache.UpdateActionResult", started);
        out.map(Response::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::reapi::{OutputDirectory, OutputFile};
    use crate::service::testutil::{TempRoot, context};
    use crate::service::to_proto_digest;
    use tonic::Code;

    /// Publish a blob and return its digest.
    async fn put(ctx: &ServiceContext, data: &[u8]) -> Digest {
        let d = Digest::of(data);
        ctx.storage.put_bytes(&d, data).await.unwrap();
        d
    }

    #[tokio::test]
    async fn round_trip_and_stale_entry_removal() {
        let root = TempRoot::new("ac-roundtrip");
        let ctx = context(&root).await;
        let svc = ActionCacheSvc::new(ctx.clone());

        let out = put(&ctx, b"the output file").await;
        let tree = put(&ctx, b"a serialized tree").await;
        let stdout = put(&ctx, b"compiling...\n").await;
        let action = Digest::of(b"the action");

        let result = ActionResult {
            output_files: vec![OutputFile {
                path: "bazel-out/foo.o".into(),
                digest: Some(to_proto_digest(&out)),
                is_executable: false,
                contents: Vec::new(),
                node_properties: None,
            }],
            output_directories: vec![OutputDirectory {
                path: "bazel-out/dir".into(),
                tree_digest: Some(to_proto_digest(&tree)),
                is_topologically_sorted: false,
                root_directory_digest: None,
            }],
            exit_code: 0,
            stdout_digest: Some(to_proto_digest(&stdout)),
            ..Default::default()
        };

        let stored = svc
            .update_action_result(Request::new(UpdateActionResultRequest {
                instance_name: "repo-a".into(),
                action_digest: Some(to_proto_digest(&action)),
                action_result: Some(result.clone()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stored, result);

        let got = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: "repo-a".into(),
                action_digest: Some(to_proto_digest(&action)),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got, result);
        assert_eq!(ctx.metrics.ac_hit_total.get(), 1);

        // A different instance is a different namespace (Doc §4).
        let other = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: "repo-b".into(),
                action_digest: Some(to_proto_digest(&action)),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(other.code(), Code::NotFound);

        // Evict a referenced blob: the entry must disappear, not just fail.
        assert!(ctx.storage.remove(&tree).await.unwrap());
        let err = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: "repo-a".into(),
                action_digest: Some(to_proto_digest(&action)),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        assert!(
            ctx.db.get_action_result("repo-a", &action).await.unwrap().is_none(),
            "stale action cache row must be deleted"
        );
        assert_eq!(ctx.metrics.ac_miss_total.get(), 2);
    }

    #[tokio::test]
    async fn update_rejects_missing_referenced_blob() {
        let root = TempRoot::new("ac-missing");
        let ctx = context(&root).await;
        let svc = ActionCacheSvc::new(ctx.clone());

        let present = put(&ctx, b"present").await;
        let absent = Digest::of(b"never uploaded");
        let action = Digest::of(b"action");

        let err = svc
            .update_action_result(Request::new(UpdateActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(to_proto_digest(&action)),
                action_result: Some(ActionResult {
                    output_files: vec![
                        OutputFile {
                            path: "a".into(),
                            digest: Some(to_proto_digest(&present)),
                            ..Default::default()
                        },
                        OutputFile {
                            path: "b".into(),
                            digest: Some(to_proto_digest(&absent)),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains(&absent.hex()), "message names the missing blob");
        assert!(!err.message().contains(&present.hex()), "present blob is not listed");
        assert!(ctx.db.get_action_result("", &action).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn inlines_requested_blobs_within_the_batch_limit() {
        let root = TempRoot::new("ac-inline");
        let ctx = context(&root).await;
        let svc = ActionCacheSvc::new(ctx.clone());

        let stdout = put(&ctx, b"hello stdout").await;
        let small = put(&ctx, b"small file").await;
        let big = put(&ctx, &vec![7u8; (ctx.cfg.server.max_batch_total_size_bytes + 1) as usize])
            .await;
        let action = Digest::of(b"inline action");

        let result = ActionResult {
            output_files: vec![
                OutputFile {
                    path: "small".into(),
                    digest: Some(to_proto_digest(&small)),
                    ..Default::default()
                },
                OutputFile {
                    path: "big".into(),
                    digest: Some(to_proto_digest(&big)),
                    ..Default::default()
                },
            ],
            stdout_digest: Some(to_proto_digest(&stdout)),
            ..Default::default()
        };
        svc.update_action_result(Request::new(UpdateActionResultRequest {
            action_digest: Some(to_proto_digest(&action)),
            action_result: Some(result),
            ..Default::default()
        }))
        .await
        .unwrap();

        let got = svc
            .get_action_result(Request::new(GetActionResultRequest {
                action_digest: Some(to_proto_digest(&action)),
                inline_stdout: true,
                inline_output_files: vec!["small".into(), "big".into()],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(got.stdout_raw, b"hello stdout");
        assert_eq!(got.output_files[0].contents, b"small file");
        assert!(
            got.output_files[1].contents.is_empty(),
            "a blob larger than the batch limit stays in digest form"
        );
    }

    #[tokio::test]
    async fn rejects_foreign_digest_function() {
        let root = TempRoot::new("ac-digestfn");
        let ctx = context(&root).await;
        let svc = ActionCacheSvc::new(ctx);

        let err = svc
            .get_action_result(Request::new(GetActionResultRequest {
                action_digest: Some(to_proto_digest(&Digest::of(b"x"))),
                digest_function: crate::proto::reapi::digest_function::Value::Blake3 as i32,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        let err = svc
            .get_action_result(Request::new(GetActionResultRequest {
                action_digest: Some(crate::proto::reapi::Digest {
                    hash: "not-hex".into(),
                    size_bytes: 1,
                }),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
