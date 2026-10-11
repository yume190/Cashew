//! `ByteStream.Read` / `Write` / `QueryWriteStatus` — the streaming blob path
//! (Doc §3, §7, §8).
//!
//! Resource names (REAPI's ByteStream binding). `instance_name` may itself
//! contain `/`, so names are parsed from their known tokens, never by counting
//! slashes from the left:
//!
//! ```text
//! read   {instance_name}/blobs/{hash}/{size}
//!        {instance_name}/blobs/{digest_function}/{hash}/{size}
//! write  {instance_name}/uploads/{uuid}/blobs/{hash}/{size}{/metadata...}
//! ```
//!
//! The `compressed-blobs/{compressor}/...` variants parse but answer
//! `UNIMPLEMENTED`: V1 advertises identity only (Doc §17).
//!
//! Neither direction buffers a whole blob: reads stream from the CAS file under
//! a `ReadLease` that outlives the response stream, writes stream straight into
//! `tmp/` and are published by `rename(2)` only after verification (Doc §8, §9).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};

use super::{ServiceContext, internal};
use crate::digest::{Digest, HEX_LEN};
use crate::observability::Metrics;
use crate::proto::bytestream::byte_stream_server::ByteStream;
use crate::proto::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use crate::storage::{BlobReader, StorageError};

const BLOBS: &str = "blobs";
const COMPRESSED_BLOBS: &str = "compressed-blobs";
const UPLOADS: &str = "uploads";
/// The only digest function that may appear in a resource name (Doc §4).
const SHA256: &str = "sha256";

pub struct ByteStreamSvc {
    ctx: Arc<ServiceContext>,
}

impl ByteStreamSvc {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }
}

/// A resource name after parsing: which Action Cache namespace it mentions and
/// which blob it addresses. The CAS itself is global, so `instance` is only
/// carried for logging and symmetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resource {
    pub(crate) instance: String,
    pub(crate) digest: Digest,
}

fn looks_like_hash(s: &str) -> bool {
    s.len() == HEX_LEN && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn malformed(name: &str, expected: &str) -> Status {
    Status::invalid_argument(format!("malformed resource name {name:?}; expected {expected}"))
}

fn compressed_unsupported(compressor: &str) -> Status {
    Status::unimplemented(format!(
        "compressed-blobs/{compressor} is not supported; this server transfers identity encoded \
         blobs only"
    ))
}

fn digest_from(hash: &str, size: &str, name: &str) -> Result<Digest, Status> {
    let size: i64 = size
        .parse()
        .map_err(|_| malformed(name, "a non-negative integer blob size"))?;
    Digest::from_hex(hash, size)
        .map_err(|e| Status::invalid_argument(format!("invalid digest in {name:?}: {e}")))
}

fn check_digest_function_token(token: &str, name: &str) -> Result<(), Status> {
    if token.eq_ignore_ascii_case(SHA256) {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "digest function {token:?} in {name:?} is not supported; this server implements \
             SHA256 only"
        )))
    }
}

/// `{instance_name}/blobs/{digest_function/}{hash}/{size}`.
pub(crate) fn parse_read_resource(name: &str) -> Result<Resource, Status> {
    const SHAPE: &str = "{instance_name}/blobs/{hash}/{size}";
    let segments: Vec<&str> = name.split('/').collect();
    if segments.len() < 3 {
        return Err(malformed(name, SHAPE));
    }
    let (head, tail) = segments.split_at(segments.len() - 2);

    // Anchor on the keyword next to the digest: everything to its left is the
    // instance name, which may contain slashes (and even the word "blobs").
    let instance_end = match head {
        [.., kind] if *kind == BLOBS => head.len() - 1,
        [.., kind, function] if *kind == BLOBS => {
            check_digest_function_token(function, name)?;
            head.len() - 2
        }
        [.., kind, compressor] if *kind == COMPRESSED_BLOBS => {
            return Err(compressed_unsupported(compressor));
        }
        [.., kind, compressor, _function] if *kind == COMPRESSED_BLOBS => {
            return Err(compressed_unsupported(compressor));
        }
        _ => return Err(malformed(name, SHAPE)),
    };

    Ok(Resource {
        instance: head[..instance_end].join("/"),
        digest: digest_from(tail[0], tail[1], name)?,
    })
}

/// `{instance_name}/uploads/{uuid}/blobs/{digest_function/}{hash}/{size}{/metadata}`.
pub(crate) fn parse_write_resource(name: &str) -> Result<Resource, Status> {
    const SHAPE: &str = "{instance_name}/uploads/{uuid}/blobs/{hash}/{size}";
    let segments: Vec<&str> = name.split('/').collect();

    // An instance name may contain "uploads"; try every candidate anchor and
    // keep the first one whose tail actually parses as a blob reference.
    for (i, segment) in segments.iter().enumerate() {
        if *segment != UPLOADS {
            continue;
        }
        if let Some(resource) = parse_write_at(&segments, i, name)? {
            return Ok(resource);
        }
    }
    Err(malformed(name, SHAPE))
}

/// Try to read a write resource whose `uploads` token sits at `i`.
/// `Ok(None)` means "not an upload anchor", so the caller may try the next one.
fn parse_write_at(segments: &[&str], i: usize, name: &str) -> Result<Option<Resource>, Status> {
    let Some(uuid) = segments.get(i + 1) else {
        return Ok(None);
    };
    if uuid.is_empty() {
        return Ok(None);
    }
    let Some(kind) = segments.get(i + 2) else {
        return Ok(None);
    };

    if *kind == COMPRESSED_BLOBS {
        let compressor = segments.get(i + 3).copied().unwrap_or_default();
        return Err(compressed_unsupported(compressor));
    }
    if *kind != BLOBS {
        return Ok(None);
    }

    // Either `blobs/{hash}/{size}` or `blobs/{digest_function}/{hash}/{size}`;
    // anything after the size is optional client metadata and is ignored.
    let instance = segments[..i].join("/");
    match (segments.get(i + 3), segments.get(i + 4)) {
        (Some(hash), Some(size)) if looks_like_hash(hash) => Ok(Some(Resource {
            instance,
            digest: digest_from(hash, size, name)?,
        })),
        (Some(function), Some(hash)) if looks_like_hash(hash) => {
            check_digest_function_token(function, name)?;
            let Some(size) = segments.get(i + 5) else {
                return Ok(None);
            };
            Ok(Some(Resource { instance, digest: digest_from(hash, size, name)? }))
        }
        _ => Ok(None),
    }
}

/// Everything one `Read` response stream needs to keep alive while it runs —
/// most importantly the `ReadLease` inside `BlobReader`, which keeps GC off the
/// blob until the last chunk has been produced (Doc §11).
struct ReadState {
    reader: BlobReader,
    chunk: usize,
    metrics: Arc<Metrics>,
    started: Instant,
    done: bool,
}

impl ByteStreamSvc {
    /// The upload state machine, generic over the request stream so it can be
    /// driven directly in tests.
    pub(crate) async fn write_stream<S>(&self, stream: S) -> Result<WriteResponse, Status>
    where
        S: futures::Stream<Item = Result<WriteRequest, Status>> + Send,
    {
        futures::pin_mut!(stream);

        let first = match stream.next().await {
            Some(Ok(message)) => message,
            Some(Err(status)) => return Err(status),
            None => {
                return Err(Status::invalid_argument(
                    "write stream closed before sending a resource name",
                ));
            }
        };
        let resource_name = first.resource_name.clone();
        let digest = parse_write_resource(&resource_name)?.digest;
        let size = digest.size_bytes as u64;

        // Publish-if-absent (Doc §7): the CAS is immutable, so an upload of a
        // blob we already hold is a no-op that still reports full progress.
        if self
            .ctx
            .storage
            .stat(&digest)
            .await
            .map_err(|e| internal("stat blob", e))?
            .is_some()
        {
            while let Some(item) = stream.next().await {
                if item.is_err() {
                    break;
                }
            }
            self.ctx.db.touch_blobs(&[digest]);
            return Ok(WriteResponse { committed_size: digest.size_bytes });
        }

        let started = Instant::now();
        let mut writer = self
            .ctx
            .storage
            .begin_write()
            .await
            .map_err(|e| internal("begin upload", e))?;

        let mut offset = 0u64;
        let mut finished = false;
        let mut pending = Some(first);
        let result = loop {
            let message = match pending.take() {
                Some(message) => message,
                None => match stream.next().await {
                    Some(Ok(message)) => message,
                    Some(Err(status)) => break Err(status),
                    None => break Ok(()),
                },
            };

            if !message.resource_name.is_empty() && message.resource_name != resource_name {
                break Err(Status::invalid_argument(format!(
                    "resource name changed mid-stream: {:?} then {:?}",
                    resource_name, message.resource_name
                )));
            }
            if message.write_offset < 0 {
                break Err(Status::invalid_argument(format!(
                    "write_offset must not be negative, got {}",
                    message.write_offset
                )));
            }
            if message.write_offset as u64 != offset {
                break Err(Status::invalid_argument(format!(
                    "writes must be contiguous: expected write_offset {offset}, got {}",
                    message.write_offset
                )));
            }
            let len = message.data.len() as u64;
            if offset + len > size {
                break Err(Status::invalid_argument(format!(
                    "upload is longer than the digest declares: {} bytes written past {size}",
                    offset + len
                )));
            }
            if let Err(e) = writer.write_chunk(&message.data).await {
                break Err(internal("write upload chunk", e));
            }
            offset += len;
            if message.finish_write {
                finished = true;
                break Ok(());
            }
        };

        if let Err(status) = result {
            writer.abort().await;
            return Err(status);
        }
        if !finished {
            writer.abort().await;
            return Err(Status::invalid_argument(
                "write stream ended without finish_write; nothing was committed",
            ));
        }

        let outcome = match writer.commit(&digest).await {
            Ok(outcome) => outcome,
            Err(e @ (StorageError::DigestMismatch { .. } | StorageError::SizeMismatch { .. })) => {
                return Err(Status::invalid_argument(e.to_string()));
            }
            Err(StorageError::Io(e)) => return Err(internal("publish blob", e)),
        };

        if outcome.is_published() {
            // Index only after the blob is in the CAS (Doc §9).
            self.ctx
                .db
                .record_blobs(&[(digest, outcome.size_bytes())])
                .await
                .map_err(|e| internal("index uploaded blob", e))?;
        } else {
            self.ctx.db.touch_blobs(&[digest]);
        }

        let metrics = &self.ctx.metrics;
        metrics.write_duration.observe(started.elapsed().as_secs_f64());
        metrics.upload_count.inc();
        metrics.upload_bytes_total.inc_by(size);

        Ok(WriteResponse { committed_size: digest.size_bytes })
    }

    async fn read_inner(
        &self,
        req: ReadRequest,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<ReadResponse, Status>> + Send>>, Status>
    {
        let digest = parse_read_resource(&req.resource_name)?.digest;

        if req.read_offset < 0 {
            return Err(Status::invalid_argument(format!(
                "read_offset must not be negative, got {}",
                req.read_offset
            )));
        }
        if req.read_limit < 0 {
            return Err(Status::invalid_argument(format!(
                "read_limit must not be negative, got {}",
                req.read_limit
            )));
        }
        let offset = req.read_offset as u64;
        if offset > digest.size_bytes as u64 {
            return Err(Status::out_of_range(format!(
                "read_offset {offset} is past the end of a {} byte blob",
                digest.size_bytes
            )));
        }
        // read_limit == 0 means "until the end of the resource".
        let limit = (req.read_limit > 0).then_some(req.read_limit as u64);

        let started = Instant::now();
        let reader = self
            .ctx
            .storage
            .open_read(&digest, offset, limit)
            .await
            .map_err(|e| internal("open blob", e))?;
        let Some(reader) = reader else {
            self.ctx.metrics.cas_miss_total.inc();
            return Err(Status::not_found(format!("blob {digest} is not in the CAS")));
        };

        self.ctx.db.touch_blobs(&[digest]);
        self.ctx.metrics.cas_hit_total.inc();
        self.ctx.metrics.download_count.inc();

        let state = ReadState {
            reader,
            chunk: self.ctx.cfg.server.stream_chunk_size_bytes.max(1) as usize,
            metrics: self.ctx.metrics.clone(),
            started,
            done: false,
        };

        let stream = futures::stream::unfold(state, |mut st| async move {
            if st.done {
                return None;
            }
            let want = st.reader.remaining().min(st.chunk as u64) as usize;
            if want == 0 {
                st.metrics.read_duration.observe(st.started.elapsed().as_secs_f64());
                return None;
            }

            // Exactly one allocation per chunk, sized to what is left.
            let mut buf = Vec::with_capacity(want);
            while buf.len() < want {
                match st.reader.read_buf(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) => {
                        st.done = true;
                        return Some((Err(internal("stream blob", e)), st));
                    }
                }
            }
            if buf.is_empty() {
                st.metrics.read_duration.observe(st.started.elapsed().as_secs_f64());
                return None;
            }
            st.metrics.download_bytes_total.inc_by(buf.len() as u64);
            Some((Ok(ReadResponse { data: buf }), st))
        });

        Ok(Box::pin(stream))
    }

    async fn query_inner(
        &self,
        req: QueryWriteStatusRequest,
    ) -> Result<QueryWriteStatusResponse, Status> {
        let digest = parse_write_resource(&req.resource_name)?.digest;
        // No resumable upload state is kept: an interrupted upload simply has
        // its temp file dropped, and the client restarts it from zero.
        match self.ctx.storage.stat(&digest).await.map_err(|e| internal("stat blob", e))? {
            Some(_) => Ok(QueryWriteStatusResponse {
                committed_size: digest.size_bytes,
                complete: true,
            }),
            None => Err(Status::not_found(format!(
                "no completed upload for blob {digest}"
            ))),
        }
    }
}

#[tonic::async_trait]
impl ByteStream for ByteStreamSvc {
    type ReadStream = Pin<Box<dyn futures::Stream<Item = Result<ReadResponse, Status>> + Send>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let started = Instant::now();
        let out = self.read_inner(request.into_inner()).await;
        self.ctx.metrics.observe_request("ByteStream.Read", started);
        out.map(Response::new)
    }

    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let started = Instant::now();
        let out = self.write_stream(request.into_inner()).await;
        self.ctx.metrics.observe_request("ByteStream.Write", started);
        out.map(Response::new)
    }

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        let started = Instant::now();
        let out = self.query_inner(request.into_inner()).await;
        self.ctx.metrics.observe_request("ByteStream.QueryWriteStatus", started);
        out.map(Response::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::testutil::{TempRoot, cas_file_count, context, context_with, tmp_file_count};
    use tonic::Code;

    fn write_name(instance: &str, d: &Digest) -> String {
        let prefix = if instance.is_empty() { String::new() } else { format!("{instance}/") };
        format!("{prefix}uploads/0e4b5a1d-1111-2222-3333-444455556666/blobs/{}/{}", d.hex(), d.size_bytes)
    }

    fn read_name(instance: &str, d: &Digest) -> String {
        let prefix = if instance.is_empty() { String::new() } else { format!("{instance}/") };
        format!("{prefix}blobs/{}/{}", d.hex(), d.size_bytes)
    }

    fn chunks(name: &str, data: &[u8], chunk: usize) -> Vec<Result<WriteRequest, Status>> {
        let mut out = Vec::new();
        let mut offset = 0usize;
        while offset < data.len() {
            let end = (offset + chunk).min(data.len());
            out.push(Ok(WriteRequest {
                resource_name: if offset == 0 { name.to_string() } else { String::new() },
                write_offset: offset as i64,
                finish_write: end == data.len(),
                data: data[offset..end].to_vec(),
            }));
            offset = end;
        }
        if out.is_empty() {
            out.push(Ok(WriteRequest {
                resource_name: name.to_string(),
                write_offset: 0,
                finish_write: true,
                data: Vec::new(),
            }));
        }
        out
    }

    async fn collect_read(svc: &ByteStreamSvc, req: ReadRequest) -> Result<Vec<u8>, Status> {
        let stream = svc.read(Request::new(req)).await?.into_inner();
        let responses: Vec<Result<ReadResponse, Status>> = stream.collect().await;
        let mut out = Vec::new();
        for response in responses {
            out.extend_from_slice(&response?.data);
        }
        Ok(out)
    }

    #[test]
    fn parses_resource_names_with_slashes_in_the_instance() {
        let d = Digest::of(b"payload");

        let r = parse_read_resource(&read_name("team/repo-a", &d)).unwrap();
        assert_eq!(r.instance, "team/repo-a");
        assert_eq!(r.digest, d);

        let r = parse_read_resource(&read_name("", &d)).unwrap();
        assert_eq!(r.instance, "");

        // An instance name may legitimately end in "blobs" or contain "uploads".
        let r = parse_read_resource(&read_name("weird/blobs", &d)).unwrap();
        assert_eq!(r.instance, "weird/blobs");

        let r = parse_read_resource(&format!("inst/blobs/sha256/{}/{}", d.hex(), d.size_bytes))
            .unwrap();
        assert_eq!(r.instance, "inst");
        assert_eq!(r.digest, d);

        let r = parse_write_resource(&write_name("team/uploads/repo", &d)).unwrap();
        assert_eq!(r.instance, "team/uploads/repo");
        assert_eq!(r.digest, d);

        // Trailing metadata after the size is allowed and ignored.
        let r = parse_write_resource(&format!("{}/some/metadata", write_name("inst", &d))).unwrap();
        assert_eq!(r.instance, "inst");
        assert_eq!(r.digest, d);

        let r =
            parse_write_resource(&format!("inst/uploads/uuid/blobs/sha256/{}/{}", d.hex(), d.size_bytes))
                .unwrap();
        assert_eq!(r.digest, d);
    }

    #[test]
    fn rejects_compressed_and_malformed_resource_names() {
        let d = Digest::of(b"payload");

        for name in [
            format!("inst/compressed-blobs/zstd/{}/{}", d.hex(), d.size_bytes),
            format!("inst/compressed-blobs/zstd/sha256/{}/{}", d.hex(), d.size_bytes),
        ] {
            assert_eq!(parse_read_resource(&name).unwrap_err().code(), Code::Unimplemented, "{name}");
        }
        assert_eq!(
            parse_write_resource(&format!(
                "inst/uploads/uuid/compressed-blobs/zstd/{}/{}",
                d.hex(),
                d.size_bytes
            ))
            .unwrap_err()
            .code(),
            Code::Unimplemented
        );

        for name in [
            String::new(),
            "blobs".to_string(),
            format!("inst/bl0bs/{}/{}", d.hex(), d.size_bytes),
            format!("inst/blobs/{}/not-a-number", d.hex()),
            format!("inst/blobs/sha1/{}/{}", d.hex(), d.size_bytes),
            format!("inst/blobs/xyz/{}", d.size_bytes),
        ] {
            assert_eq!(
                parse_read_resource(&name).unwrap_err().code(),
                Code::InvalidArgument,
                "{name}"
            );
        }
        for name in [
            format!("inst/blobs/{}/{}", d.hex(), d.size_bytes),
            format!("inst/uploads/{}/{}", d.hex(), d.size_bytes),
        ] {
            assert_eq!(
                parse_write_resource(&name).unwrap_err().code(),
                Code::InvalidArgument,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn multi_chunk_write_then_read_back() {
        let root = TempRoot::new("bs-roundtrip");
        // Force several response chunks for a blob of this size.
        let ctx = context_with(&root, |cfg| cfg.server.stream_chunk_size_bytes = 1024).await;
        let svc = ByteStreamSvc::new(ctx.clone());

        let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
        let digest = Digest::of(&payload);
        let name = write_name("repo-a", &digest);

        let response = svc
            .write_stream(futures::stream::iter(chunks(&name, &payload, 7_000)))
            .await
            .unwrap();
        assert_eq!(response.committed_size, digest.size_bytes);
        assert_eq!(tmp_file_count(&root), 0, "no temp file may survive a commit");
        assert_eq!(cas_file_count(&root), 1);
        assert_eq!(ctx.db.stats().await.unwrap().cas_entry_count, 1);
        assert_eq!(ctx.metrics.upload_bytes_total.get(), payload.len() as u64);

        let read_back = collect_read(
            &svc,
            ReadRequest { resource_name: read_name("repo-a", &digest), ..Default::default() },
        )
        .await
        .unwrap();
        assert_eq!(read_back, payload, "blob must round-trip byte for byte");

        // Offset and limit.
        let tail = collect_read(
            &svc,
            ReadRequest {
                resource_name: read_name("", &digest),
                read_offset: 39_000,
                read_limit: 0,
            },
        )
        .await
        .unwrap();
        assert_eq!(tail, payload[39_000..]);

        let window = collect_read(
            &svc,
            ReadRequest {
                resource_name: read_name("", &digest),
                read_offset: 10,
                read_limit: 2_500,
            },
        )
        .await
        .unwrap();
        assert_eq!(window, payload[10..2_510]);

        // Reading exactly at the end is an empty stream, not an error.
        let empty = collect_read(
            &svc,
            ReadRequest {
                resource_name: read_name("", &digest),
                read_offset: digest.size_bytes,
                read_limit: 0,
            },
        )
        .await
        .unwrap();
        assert!(empty.is_empty());

        // A second upload of the same blob takes the publish-if-absent path.
        let again = svc
            .write_stream(futures::stream::iter(chunks(&name, &payload, 7_000)))
            .await
            .unwrap();
        assert_eq!(again.committed_size, digest.size_bytes);
        assert_eq!(cas_file_count(&root), 1);
    }

    #[tokio::test]
    async fn read_rejects_bad_ranges_and_missing_blobs() {
        let root = TempRoot::new("bs-read-errors");
        let ctx = context(&root).await;
        let svc = ByteStreamSvc::new(ctx.clone());

        let payload = b"0123456789".to_vec();
        let digest = Digest::of(&payload);
        ctx.storage.put_bytes(&digest, &payload).await.unwrap();

        let err = collect_read(
            &svc,
            ReadRequest {
                resource_name: read_name("", &digest),
                read_offset: 11,
                read_limit: 0,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), Code::OutOfRange);

        for (offset, limit) in [(-1, 0), (0, -5)] {
            let err = collect_read(
                &svc,
                ReadRequest {
                    resource_name: read_name("", &digest),
                    read_offset: offset,
                    read_limit: limit,
                },
            )
            .await
            .unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument);
        }

        let absent = Digest::of(b"not uploaded");
        let err = collect_read(
            &svc,
            ReadRequest { resource_name: read_name("", &absent), ..Default::default() },
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn rejects_wrong_digest_and_leaves_nothing_behind() {
        let root = TempRoot::new("bs-bad-digest");
        let ctx = context(&root).await;
        let svc = ByteStreamSvc::new(ctx.clone());

        let claimed = Digest::of(b"what the client promised");
        let name = write_name("", &claimed);
        let lie = b"something else entirely!".to_vec();
        assert_eq!(lie.len() as i64, claimed.size_bytes, "same length, different bytes");

        let err = svc
            .write_stream(futures::stream::iter(chunks(&name, &lie, 8)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains(&claimed.hex()), "message names the expected digest");
        assert_eq!(tmp_file_count(&root), 0, "temp file must be removed");
        assert_eq!(cas_file_count(&root), 0, "nothing may reach the CAS");
        assert_eq!(ctx.db.stats().await.unwrap().cas_entry_count, 0);
    }

    #[tokio::test]
    async fn rejects_streams_that_never_finish_or_overrun() {
        let root = TempRoot::new("bs-protocol");
        let ctx = context(&root).await;
        let svc = ByteStreamSvc::new(ctx.clone());

        let payload = b"abcdefghij".to_vec();
        let digest = Digest::of(&payload);
        let name = write_name("", &digest);

        // No finish_write.
        let err = svc
            .write_stream(futures::stream::iter(vec![Ok(WriteRequest {
                resource_name: name.clone(),
                write_offset: 0,
                finish_write: false,
                data: payload.clone(),
            })]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(tmp_file_count(&root), 0);
        assert_eq!(cas_file_count(&root), 0);

        // Non-contiguous offsets.
        let err = svc
            .write_stream(futures::stream::iter(vec![
                Ok(WriteRequest {
                    resource_name: name.clone(),
                    write_offset: 0,
                    finish_write: false,
                    data: payload[..4].to_vec(),
                }),
                Ok(WriteRequest {
                    resource_name: String::new(),
                    write_offset: 6,
                    finish_write: true,
                    data: payload[6..].to_vec(),
                }),
            ]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // More bytes than the digest declares.
        let err = svc
            .write_stream(futures::stream::iter(vec![Ok(WriteRequest {
                resource_name: name.clone(),
                write_offset: 0,
                finish_write: true,
                data: b"much longer than ten bytes".to_vec(),
            })]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // A contradicting resource name mid-stream.
        let err = svc
            .write_stream(futures::stream::iter(vec![
                Ok(WriteRequest {
                    resource_name: name.clone(),
                    write_offset: 0,
                    finish_write: false,
                    data: payload[..4].to_vec(),
                }),
                Ok(WriteRequest {
                    resource_name: write_name("other", &digest),
                    write_offset: 4,
                    finish_write: true,
                    data: payload[4..].to_vec(),
                }),
            ]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        assert_eq!(tmp_file_count(&root), 0, "every rejected upload cleans up its temp file");
        assert_eq!(cas_file_count(&root), 0);
        assert!(!ctx.storage.contains(&digest).await.unwrap());
    }

    #[tokio::test]
    async fn query_write_status_reports_completed_uploads() {
        let root = TempRoot::new("bs-query");
        let ctx = context(&root).await;
        let svc = ByteStreamSvc::new(ctx.clone());

        let payload = b"queried blob".to_vec();
        let digest = Digest::of(&payload);
        let name = write_name("repo-a", &digest);

        let err = svc
            .query_write_status(Request::new(QueryWriteStatusRequest {
                resource_name: name.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);

        svc.write_stream(futures::stream::iter(chunks(&name, &payload, 5)))
            .await
            .unwrap();

        let status = svc
            .query_write_status(Request::new(QueryWriteStatusRequest { resource_name: name }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.committed_size, digest.size_bytes);
        assert!(status.complete);
    }

    #[tokio::test]
    async fn empty_blob_is_always_available() {
        let root = TempRoot::new("bs-empty");
        let ctx = context(&root).await;
        let svc = ByteStreamSvc::new(ctx.clone());

        let empty = Digest::of(b"");
        let data = collect_read(
            &svc,
            ReadRequest { resource_name: read_name("repo-a", &empty), ..Default::default() },
        )
        .await
        .unwrap();
        assert!(data.is_empty());

        // Uploading it is a no-op that reports success and stores nothing.
        let response = svc
            .write_stream(futures::stream::iter(chunks(&write_name("", &empty), &[], 8)))
            .await
            .unwrap();
        assert_eq!(response.committed_size, 0);
        assert_eq!(cas_file_count(&root), 0);
        assert_eq!(ctx.db.stats().await.unwrap().cas_entry_count, 0);
    }
}
