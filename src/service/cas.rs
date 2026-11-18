//! `ContentAddressableStorage`: presence checks and small-blob batch transfer.
//!
//! The CAS is global and content addressed, so `instance_name` is accepted but
//! never used as a key (Doc §4 namespaces only the Action Cache). Presence is
//! always decided by `stat(2)` on the filesystem, which is the source of truth.
//!
//! Batch RPCs are bounded by `server.max_batch_total_size_bytes`, the same value
//! `GetCapabilities` advertises; anything larger belongs on ByteStream.

use std::sync::Arc;
use std::time::Instant;

use tonic::{Code, Request, Response, Status};
use tracing::error;

use super::{
    ServiceContext, check_digest_function, internal, parse_digest, rpc_ok, rpc_status, stat_many,
    to_proto_digest,
};
use super::{batch_detail, batch_summary, code_name, digest_label, result_status};
use crate::traffic::TrafficKind;
use crate::digest::Digest;
use crate::proto::reapi::content_addressable_storage_server::ContentAddressableStorage;
use crate::proto::reapi::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse,
    GetChunkMappingRequest, GetChunkMappingResponse, GetTreeRequest, GetTreeResponse,
    RegisterChunkMappingRequest, RegisterChunkMappingResponse, SpliceBlobRequest,
    SpliceBlobResponse, SplitBlobRequest, SplitBlobResponse, batch_read_blobs_response,
    batch_update_blobs_response, compressor,
};
use crate::storage::StorageError;

/// Streams this service never produces; every streaming RPC it declares is
/// outside the cache subset and answers `UNIMPLEMENTED`.
type EmptyStream<T> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

pub struct CasSvc {
    ctx: Arc<ServiceContext>,
}

impl CasSvc {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }
}

/// Only identity transfer is implemented (Doc §17).
fn check_compressor(value: i32) -> Result<(), String> {
    match compressor::Value::try_from(value) {
        Ok(compressor::Value::Identity) => Ok(()),
        Ok(other) => Err(format!(
            "compressor {} is not supported; this server transfers identity only",
            other.as_str_name()
        )),
        Err(_) => Err(format!("unknown compressor value {value}")),
    }
}

impl CasSvc {
    async fn find_missing_inner(
        &self,
        req: FindMissingBlobsRequest,
    ) -> Result<FindMissingBlobsResponse, Status> {
        check_digest_function(req.digest_function)?;

        let mut digests = Vec::with_capacity(req.blob_digests.len());
        for d in &req.blob_digests {
            digests.push(parse_digest(Some(d), "blob_digests[]")?);
        }
        let sizes = stat_many(&self.ctx.storage, &digests).await?;

        let metrics = &self.ctx.metrics;
        let mut missing = Vec::new();
        let mut present = Vec::new();
        let mut avoided = 0u64;
        for (digest, size) in digests.iter().zip(&sizes) {
            match size {
                Some(size) => {
                    metrics.cas_hit_total.inc();
                    // Bytes the client does not have to upload: the dividend of
                    // sharing one cache between worktrees (Doc §16).
                    avoided += *size;
                    present.push(*digest);
                }
                None => {
                    metrics.cas_miss_total.inc();
                    missing.push(to_proto_digest(digest));
                }
            }
        }
        metrics.avoided_bytes_total.inc_by(avoided);
        self.ctx.db.touch_blobs(&present);

        Ok(FindMissingBlobsResponse { missing_blob_digests: missing })
    }

    async fn batch_update_inner(
        &self,
        req: BatchUpdateBlobsRequest,
    ) -> Result<(BatchUpdateBlobsResponse, u64), Status> {
        check_digest_function(req.digest_function)?;

        let limit = self.ctx.cfg.server.max_batch_total_size_bytes;
        let total: u64 = req.requests.iter().map(|r| r.data.len() as u64).sum();
        if total > limit {
            return Err(Status::invalid_argument(format!(
                "batch payload of {total} bytes exceeds the server limit of {limit} bytes; \
                 split the request or use the ByteStream API"
            )));
        }

        let started = Instant::now();
        let mut responses = Vec::with_capacity(req.requests.len());
        let mut published = Vec::new();
        let mut touched = Vec::new();
        let mut uploaded_bytes = 0u64;
        let mut uploaded_count = 0u64;

        for entry in req.requests {
            let digest_proto = entry.digest.clone();
            let status = match self.store_one(&entry).await {
                Ok((digest, newly_published)) => {
                    uploaded_bytes += entry.data.len() as u64;
                    uploaded_count += 1;
                    match newly_published {
                        Some(size) => published.push((digest, size)),
                        // Duplicate upload: nothing to index, but it still
                        // counts as an access for eviction ordering.
                        None => touched.push(digest),
                    }
                    rpc_ok()
                }
                Err(status) => status,
            };
            responses
                .push(batch_update_blobs_response::Response { digest: digest_proto, status: Some(status) });
        }

        // One transaction for the whole batch; the blobs are already on disk, so
        // this only updates the rebuildable index (Doc §9).
        if !published.is_empty() {
            self.ctx
                .db
                .record_blobs(&published)
                .await
                .map_err(|e| internal("index uploaded blobs", e))?;
        }
        self.ctx.db.touch_blobs(&touched);

        let metrics = &self.ctx.metrics;
        metrics.write_duration.observe(started.elapsed().as_secs_f64());
        metrics.upload_count.inc_by(uploaded_count);
        metrics.upload_bytes_total.inc_by(uploaded_bytes);

        Ok((BatchUpdateBlobsResponse { responses }, uploaded_bytes))
    }

    /// Store one batch entry. The second tuple element is `Some(size)` when
    /// this call published the blob and the index must learn about it, `None`
    /// when an identical blob was already present (Doc §7 publish-if-absent).
    /// A rejected entry never fails the whole batch.
    async fn store_one(
        &self,
        entry: &crate::proto::reapi::batch_update_blobs_request::Request,
    ) -> Result<(Digest, Option<u64>), crate::proto::RpcStatus> {
        if let Err(message) = check_compressor(entry.compressor) {
            return Err(rpc_status(Code::InvalidArgument, message));
        }
        let digest = parse_digest(entry.digest.as_ref(), "digest")
            .map_err(|s| rpc_status(Code::InvalidArgument, s.message().to_string()))?;

        match self.ctx.storage.put_bytes(&digest, &entry.data).await {
            Ok(outcome) if outcome.is_published() => Ok((digest, Some(outcome.size_bytes()))),
            Ok(_) => Ok((digest, None)),
            Err(e @ (StorageError::DigestMismatch { .. } | StorageError::SizeMismatch { .. })) => {
                Err(rpc_status(Code::InvalidArgument, e.to_string()))
            }
            Err(StorageError::Io(e)) => {
                error!(%digest, error = %e, "storing batch blob failed");
                Err(rpc_status(Code::Internal, "internal error storing blob"))
            }
        }
    }

    async fn batch_read_inner(
        &self,
        req: BatchReadBlobsRequest,
    ) -> Result<BatchReadBlobsResponse, Status> {
        check_digest_function(req.digest_function)?;

        // An empty list means "identity is fine"; otherwise identity must be
        // among the compressors the client accepts.
        let identity_ok = req.acceptable_compressors.is_empty()
            || req
                .acceptable_compressors
                .contains(&(compressor::Value::Identity as i32));
        let refusal = (!identity_ok).then(|| {
            rpc_status(
                Code::InvalidArgument,
                "this server only serves IDENTITY encoded blobs",
            )
        });

        let started = Instant::now();
        let limit = self.ctx.cfg.server.max_batch_total_size_bytes;
        let mut accumulated = 0u64;
        let mut exhausted = false;
        let mut responses = Vec::with_capacity(req.digests.len());
        let mut hits = Vec::new();
        let mut downloaded = 0u64;

        for proto in &req.digests {
            if let Some(status) = refusal.clone() {
                responses.push(batch_read_blobs_response::Response {
                    digest: Some(proto.clone()),
                    data: Vec::new(),
                    compressor: compressor::Value::Identity as i32,
                    status: Some(status),
                });
                continue;
            }

            let digest = parse_digest(Some(proto), "digests[]")?;
            // Budget on the declared size: a content addressed blob is exactly
            // that long, so no read is needed to know it does not fit.
            let want = digest.size_bytes as u64;
            if !exhausted && accumulated.saturating_add(want) > limit {
                exhausted = true;
            }
            if exhausted {
                responses.push(batch_read_blobs_response::Response {
                    digest: Some(proto.clone()),
                    data: Vec::new(),
                    compressor: compressor::Value::Identity as i32,
                    status: Some(rpc_status(
                        Code::ResourceExhausted,
                        format!(
                            "blob does not fit in the remaining batch budget of {} bytes; \
                             request it separately or use the ByteStream API",
                            limit.saturating_sub(accumulated)
                        ),
                    )),
                });
                continue;
            }

            let read = self
                .ctx
                .storage
                .read_all(&digest)
                .await
                .map_err(|e| internal("read batch blob", e))?;
            let (data, status) = match read {
                Some(bytes) => {
                    self.ctx.metrics.cas_hit_total.inc();
                    accumulated += bytes.len() as u64;
                    downloaded += bytes.len() as u64;
                    hits.push(digest);
                    (bytes, rpc_ok())
                }
                None => {
                    self.ctx.metrics.cas_miss_total.inc();
                    (
                        Vec::new(),
                        rpc_status(Code::NotFound, format!("blob {digest} is not in the CAS")),
                    )
                }
            };
            responses.push(batch_read_blobs_response::Response {
                digest: Some(proto.clone()),
                data,
                compressor: compressor::Value::Identity as i32,
                status: Some(status),
            });
        }

        self.ctx.db.touch_blobs(&hits);
        let metrics = &self.ctx.metrics;
        metrics.read_duration.observe(started.elapsed().as_secs_f64());
        metrics.download_count.inc_by(hits.len() as u64);
        metrics.download_bytes_total.inc_by(downloaded);

        Ok(BatchReadBlobsResponse { responses })
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CasSvc {
    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let started = Instant::now();
        let session = self.ctx.session_of(&request);
        let request = request.into_inner();
        let total = request.blob_digests.len();
        let out = self.find_missing_inner(request).await;
        self.ctx.metrics.observe_request("CAS.FindMissingBlobs", started);
        self.ctx.trace(crate::traffic::RequestTrace { session, started }, TrafficKind::Query, "CAS.FindMissingBlobs", result_status(&out), 0, || {
            let missing = out.as_ref().map_or(0, |r| r.missing_blob_digests.len());
            format!("{total} digests, {missing} missing")
        });
        out.map(Response::new)
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let started = Instant::now();
        let session = self.ctx.session_of(&request);
        let request = request.into_inner();
        let first = if self.ctx.tracing_traffic() {
            digest_label(request.requests.first().and_then(|r| r.digest.as_ref()))
        } else {
            String::new()
        };
        let out = self.batch_update_inner(request).await;
        self.ctx.metrics.observe_request("CAS.BatchUpdateBlobs", started);
        let (status, bytes) = match &out {
            Ok((response, bytes)) => (
                batch_summary(response.responses.iter().map(|r| r.status.as_ref().map_or(0, |s| s.code))).0,
                *bytes,
            ),
            Err(status) => (code_name(status.code()), 0),
        };
        self.ctx.trace(crate::traffic::RequestTrace { session, started }, TrafficKind::Upload, "CAS.BatchUpdateBlobs", status, bytes, || match &out {
            Ok((response, _)) => {
                let (_, ok, total) = batch_summary(
                    response.responses.iter().map(|r| r.status.as_ref().map_or(0, |s| s.code)),
                );
                batch_detail(&first, ok, total)
            }
            Err(_) => first.clone(),
        });
        out.map(|(response, _)| Response::new(response))
    }

    async fn batch_read_blobs(
        &self,
        request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let started = Instant::now();
        let session = self.ctx.session_of(&request);
        let request = request.into_inner();
        let first = if self.ctx.tracing_traffic() {
            digest_label(request.digests.first())
        } else {
            String::new()
        };
        let out = self.batch_read_inner(request).await;
        self.ctx.metrics.observe_request("CAS.BatchReadBlobs", started);
        self.ctx.trace(crate::traffic::RequestTrace { session, started }, TrafficKind::Download, "CAS.BatchReadBlobs", match &out {
            Ok(response) => {
                batch_summary(response.responses.iter().map(|r| r.status.as_ref().map_or(0, |s| s.code))).0
            }
            Err(status) => code_name(status.code()),
        }, out.as_ref().map_or(0, |r| {
            r.responses.iter().map(|e| e.data.len() as u64).sum()
        }), || match &out {
            Ok(response) => {
                let (_, ok, total) = batch_summary(
                    response.responses.iter().map(|r| r.status.as_ref().map_or(0, |s| s.code)),
                );
                batch_detail(&first, ok, total)
            }
            Err(_) => first.clone(),
        });
        out.map(Response::new)
    }

    type GetTreeStream = EmptyStream<GetTreeResponse>;

    /// Not implemented: this server stores opaque blobs and never parses
    /// `Directory` protos, so it cannot enumerate a tree. Bazel does not need
    /// `GetTree` for caching; it fetches the `Tree` blob from the CAS instead.
    async fn get_tree(
        &self,
        _request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::unimplemented(
            "GetTree is not implemented; fetch the Tree blob with BatchReadBlobs or ByteStream.Read",
        ))
    }

    /// Blob splitting is not advertised in `GetCapabilities`.
    async fn split_blob(
        &self,
        _request: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        Err(Status::unimplemented("blob splitting is not supported"))
    }

    type GetChunkMappingStream = EmptyStream<GetChunkMappingResponse>;

    async fn get_chunk_mapping(
        &self,
        _request: Request<GetChunkMappingRequest>,
    ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
        Err(Status::unimplemented("chunk mappings are not supported"))
    }

    async fn splice_blob(
        &self,
        _request: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        Err(Status::unimplemented("blob splicing is not supported"))
    }

    async fn register_chunk_mapping(
        &self,
        _request: Request<tonic::Streaming<RegisterChunkMappingRequest>>,
    ) -> Result<Response<RegisterChunkMappingResponse>, Status> {
        Err(Status::unimplemented("chunk mappings are not supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::proto::reapi::batch_update_blobs_request;
    use crate::service::testutil::{TempRoot, context, context_with};

    /// Hashes carried by a digest list, for order-independent comparison.
    fn hex_set(digests: &[crate::proto::reapi::Digest]) -> HashSet<String> {
        digests.iter().map(|d| d.hash.clone()).collect()
    }

    fn update_entry(digest: &Digest, data: &[u8]) -> batch_update_blobs_request::Request {
        batch_update_blobs_request::Request {
            digest: Some(to_proto_digest(digest)),
            data: data.to_vec(),
            compressor: compressor::Value::Identity as i32,
        }
    }

    #[tokio::test]
    async fn find_missing_reports_exactly_the_absent_blobs() {
        let root = TempRoot::new("cas-find");
        let ctx = context(&root).await;
        let svc = CasSvc::new(ctx.clone());

        let present = Digest::of(b"present blob");
        ctx.storage.put_bytes(&present, b"present blob").await.unwrap();
        let absent_a = Digest::of(b"absent a");
        let absent_b = Digest::of(b"absent b");
        let empty = Digest::of(b"");

        let resp = svc
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: "repo-a".into(),
                blob_digests: vec![
                    to_proto_digest(&present),
                    to_proto_digest(&absent_a),
                    to_proto_digest(&empty),
                    to_proto_digest(&absent_b),
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            hex_set(&resp.missing_blob_digests),
            HashSet::from([absent_a.hex(), absent_b.hex()]),
            "the empty blob is always present and must never be requested"
        );
        assert_eq!(ctx.metrics.cas_hit_total.get(), 2);
        assert_eq!(ctx.metrics.cas_miss_total.get(), 2);
        assert_eq!(ctx.metrics.avoided_bytes_total.get(), b"present blob".len() as u64);
    }

    #[tokio::test]
    async fn batch_update_rejects_only_the_corrupted_entry() {
        let root = TempRoot::new("cas-update");
        let ctx = context(&root).await;
        let svc = CasSvc::new(ctx.clone());

        let good_a = Digest::of(b"good a");
        let good_b = Digest::of(b"good b");
        let liar = Digest::of(b"claimed content");

        let resp = svc
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: String::new(),
                requests: vec![
                    update_entry(&good_a, b"good a"),
                    // Payload does not hash to the digest it claims.
                    update_entry(&liar, b"different content"),
                    update_entry(&good_b, b"good b"),
                    batch_update_blobs_request::Request {
                        digest: Some(to_proto_digest(&Digest::of(b"zstd"))),
                        data: b"zstd".to_vec(),
                        compressor: compressor::Value::Zstd as i32,
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        let codes: Vec<i32> = resp.responses.iter().map(|r| r.status.as_ref().unwrap().code).collect();
        assert_eq!(
            codes,
            vec![
                i32::from(Code::Ok),
                i32::from(Code::InvalidArgument),
                i32::from(Code::Ok),
                i32::from(Code::InvalidArgument),
            ]
        );
        assert!(ctx.storage.contains(&good_a).await.unwrap());
        assert!(ctx.storage.contains(&good_b).await.unwrap());
        assert!(!ctx.storage.contains(&liar).await.unwrap());
        assert_eq!(ctx.db.stats().await.unwrap().cas_entry_count, 2, "only valid blobs indexed");
    }

    #[tokio::test]
    async fn batch_update_rejects_an_oversized_request() {
        let root = TempRoot::new("cas-update-big");
        let ctx = context_with(&root, |cfg| cfg.server.max_batch_total_size_bytes = 8).await;
        let svc = CasSvc::new(ctx.clone());

        let blob = Digest::of(b"0123456789");
        let err = svc
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                requests: vec![update_entry(&blob, b"0123456789")],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(!ctx.storage.contains(&blob).await.unwrap());
    }

    #[tokio::test]
    async fn batch_read_reports_exhaustion_and_misses() {
        let root = TempRoot::new("cas-read");
        // Room for exactly one of the two blobs below.
        let ctx = context_with(&root, |cfg| cfg.server.max_batch_total_size_bytes = 12).await;
        let svc = CasSvc::new(ctx.clone());

        let first = Digest::of(b"aaaaaaaa");
        ctx.storage.put_bytes(&first, b"aaaaaaaa").await.unwrap();
        let second = Digest::of(b"bbbbbbbb");
        ctx.storage.put_bytes(&second, b"bbbbbbbb").await.unwrap();
        let absent = Digest::of(b"nowhere");

        let resp = svc
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                digests: vec![
                    to_proto_digest(&first),
                    to_proto_digest(&second),
                    to_proto_digest(&absent),
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.responses.len(), 3);
        assert_eq!(resp.responses[0].status.as_ref().unwrap().code, i32::from(Code::Ok));
        assert_eq!(resp.responses[0].data, b"aaaaaaaa");
        assert_eq!(
            resp.responses[1].status.as_ref().unwrap().code,
            i32::from(Code::ResourceExhausted),
            "a blob past the batch budget is reported, not silently dropped"
        );
        assert!(resp.responses[1].data.is_empty());
        assert_eq!(
            resp.responses[2].status.as_ref().unwrap().code,
            i32::from(Code::ResourceExhausted),
            "entries after the budget is spent stay exhausted"
        );
        assert_eq!(ctx.metrics.download_bytes_total.get(), 8);
    }

    #[tokio::test]
    async fn batch_read_reports_missing_blobs() {
        let root = TempRoot::new("cas-read-missing");
        let ctx = context(&root).await;
        let svc = CasSvc::new(ctx.clone());

        let present = Digest::of(b"here");
        ctx.storage.put_bytes(&present, b"here").await.unwrap();
        let absent = Digest::of(b"gone");

        let resp = svc
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                digests: vec![to_proto_digest(&present), to_proto_digest(&absent)],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.responses[0].data, b"here");
        assert_eq!(
            resp.responses[1].status.as_ref().unwrap().code,
            i32::from(Code::NotFound)
        );
        assert_eq!(ctx.metrics.cas_miss_total.get(), 1);
    }

    #[tokio::test]
    async fn batch_read_rejects_compressed_only_clients() {
        let root = TempRoot::new("cas-read-zstd");
        let ctx = context(&root).await;
        let svc = CasSvc::new(ctx.clone());

        let blob = Digest::of(b"payload");
        ctx.storage.put_bytes(&blob, b"payload").await.unwrap();

        let resp = svc
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                digests: vec![to_proto_digest(&blob)],
                acceptable_compressors: vec![compressor::Value::Zstd as i32],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            resp.responses[0].status.as_ref().unwrap().code,
            i32::from(Code::InvalidArgument)
        );
        assert!(resp.responses[0].data.is_empty());
    }

    #[tokio::test]
    async fn get_tree_is_unimplemented() {
        let root = TempRoot::new("cas-tree");
        let ctx = context(&root).await;
        let svc = CasSvc::new(ctx);

        let err = svc
            .get_tree(Request::new(GetTreeRequest::default()))
            .await
            .err()
            .expect("GetTree must be refused");
        assert_eq!(err.code(), Code::Unimplemented);
        assert!(err.message().contains("ByteStream.Read") || err.message().contains("BatchReadBlobs"));
    }
}
