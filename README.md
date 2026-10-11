# Bazel LAN Remote Cache Server

A single-node Bazel **remote cache** (Remote Execution API v2, cache subset only) in Rust.
Design spec: [`Doc.md`](Doc.md). Not a remote *execution* server.

```text
Bazel clients ── gRPC REAPI ──► cache server ──► /data (CAS files + metadata.sqlite)
```

## Implemented surface

| Service | RPCs |
| --- | --- |
| `Capabilities` | `GetCapabilities` |
| `ActionCache` | `GetActionResult`, `UpdateActionResult` |
| `ContentAddressableStorage` | `FindMissingBlobs`, `BatchReadBlobs`, `BatchUpdateBlobs` (`GetTree` → `UNIMPLEMENTED`) |
| `ByteStream` | `Read`, `Write`, `QueryWriteStatus` |
| `grpc.health.v1.Health` | `Check`, `Watch` |

SHA-256 only, identity encoding only (no zstd), no remote execution.

## Run

```bash
cargo build --release
./target/release/bazel-cache-server --root /data --max-size 800GB serve
```

```bash
bazel build --remote_cache=grpc://cache-host:50051 //...
```

Container:

```bash
docker build -t bazel-cache .
docker run -d -p 50051:50051 -p 9090:9090 -v /srv/bazel-cache:/data bazel-cache
```

## CLI

| Command | Purpose |
| --- | --- |
| `serve` (default) | Run the gRPC cache, metrics/health HTTP listener, and the GC sweeper. |
| `reindex` | Rebuild `metadata.sqlite` by scanning the CAS. The filesystem is the source of truth; Action Cache entries live only in SQLite and are *not* recoverable this way. |
| `gc` | Run one eviction sweep and exit. |
| `stats` | Print entry counts, bytes and watermarks. |

Every setting can be supplied three ways, highest precedence first: CLI flag,
`BAZEL_CACHE_*` environment variable, TOML file (`--config`). See
[`config.example.toml`](config.example.toml). Invalid configuration fails
startup — there is no silent fallback.

## Storage layout

```text
/data
├── cas/<first-hex-byte>/<sha256-hex>   immutable blob payloads
├── tmp/<random>.part                   in-flight uploads, cleaned at startup
└── metadata.sqlite                     index, Action Cache, GC metadata (WAL)
```

Uploads stream to `tmp/`, are verified against the client-declared digest and
size, then atomically renamed into `cas/` (publish-if-absent) *before* SQLite is
touched. A crash can therefore leave a blob that SQLite does not know about,
which `reindex` repairs; it can never leave a corrupt or partially visible blob.

`durability = "cache"` (default) skips per-blob fsync — Bazel can always rebuild.
`durability = "strict"` fsyncs the file and its parent directory and runs SQLite
with `synchronous=FULL`.

### Namespacing

The CAS is **global and content addressed**: the same SHA-256 is the same bytes,
so blobs are shared across all clients — that sharing is the point of the server.
`instance_name` namespaces the **Action Cache** only, so `repo-a` and `repo-b`
keep independent action results while still sharing blob storage.

### SQLite schema

```sql
instances(id, name UNIQUE)
cas_blobs(hash BLOB(32) PRIMARY KEY, size_bytes, created_at, last_access_at)   -- + LRU / FIFO indexes
action_cache(instance_id, hash BLOB(32), size_bytes, result BLOB, created_at, last_access_at)
counters(key, value)   -- cas_size_bytes / cas_entry_count, maintained transactionally
```

Reads never write to SQLite: `last_access_at` updates are accumulated in memory
and flushed in one transaction every 5 s.

## Eviction

```toml
[storage]
max_size = "800GB"
[eviction]
policy = "lru"          # lru | ttl | fifo | none
high_watermark = 0.90   # sweep starts above this
low_watermark  = 0.75   # sweep runs until below this
interval = "60s"
```

`ttl` additionally expires entries older than `eviction.ttl` on every sweep,
regardless of pressure. GC never deletes a blob with an active reader (an
in-memory lease registry); those are skipped and retried on the next sweep.

## Authentication

`auth.mode`: `none`, `token` (constant-time `Authorization: Bearer` check from
value/env/file), `mtls` (client certificate verified against `auth.tls.client_ca`,
principal from the certificate CN/SAN), `oidc` (JWT validated against a cached
JWKS refreshed in the background; `iss`/`aud`/`exp` enforced, RS256/384/512 and
ES256).

```bash
bazel build --remote_cache=grpc://host:50051 --remote_header=authorization="Bearer $TOKEN" //...
```

Authorization is flat: an authenticated principal gets full read/write access.

## Observability

`:9090/metrics` (Prometheus), `:9090/health` (liveness), `:9090/ready`
(readiness: storage and SQLite writable), plus the gRPC health service.
Metrics are prefixed `bazel_cache_`: hit/miss counters for CAS and AC, upload and
download bytes/counts, occupancy gauges, eviction totals, `avoided_bytes_total`
(bytes clients did not transfer because the cache already had the blob), and
latency histograms for requests, reads, writes, SQLite and GC.

Logging is `tracing`, `pretty` or `json`.

## Tests

```bash
cargo test        # 92 unit tests: storage, SQLite, auth, GC, REAPI handlers, HTTP surface
cargo clippy --all-targets
```

End-to-end verification used Bazel 9.3 against a live server: cold build uploads
9 blobs / 3 action results, `bazel clean` + rebuild reports `3 remote cache hit`,
and the 8 MiB artifact comes back byte-identical.
