# Cashew

A single-node Bazel **remote cache** (Remote Execution API v2, cache subset only) in Rust,
with a native GPUI desktop manager for macOS and Windows.
Design spec: [`Doc.md`](Doc.md). Builds execute on the Bazel client.

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

## Desktop app

```bash
cargo run -p cashew-desktop --bin cashew
```

The native interface uses `gpui-kit 0.9`, matching SwiftCache's GPUI stack.
總覽 shows CAS usage, blob/action counts, CAS + Action Cache hit rate, transfer
bytes, uptime, and a copyable Bazel cache argument. 服務設定 configures the cache
directory, capacity, gRPC listener, metrics listener, and an optional advanced
TOML file. 日誌 keeps the latest 200 service log lines.

Start/stop runs the same cache engine inside the desktop process. Closing the
window quits the app, drains the service, flushes access timestamps, and releases
its listeners. No separate server executable is needed. Settings are locked
while the service starts, runs, or stops; the service does not auto-start.

Desktop defaults: `127.0.0.1:50051` for gRPC, `127.0.0.1:9090` for metrics,
100 GB capacity, and a cache directory under the user's platform cache directory.
Settings persist in `desktop.json` in the platform configuration directory
(`%APPDATA%\yume\Cashew\config` on Windows). Set `CASHEW_DATA_DIR` to use a separate
settings directory; a new profile then also defaults its cache beneath that directory.
Unreadable settings are preserved and must be repaired before saving or starting.

The desktop fields override the advanced TOML file and `BAZEL_CACHE_*`
environment variables. Authentication, TLS, durability, GC and other options use
the existing TOML schema in [`config.example.toml`](config.example.toml).
The copied cache URL reflects whether the active service uses TLS; configure the
certificate hostname and Bazel trust/authentication flags for TLS deployments.
For other machines to connect, bind gRPC to `0.0.0.0:50051`, allow that port through
the host firewall, and use the server's LAN hostname or address in Bazel.

### Windows

Build on Windows with the Rust **MSVC** toolchain, Visual Studio Build Tools
(Desktop development with C++, including the Windows SDK), and `protoc` on PATH:

```powershell
cargo build --locked --release -p cashew-desktop --bin cashew
.\target\release\cashew.exe
```

The desktop executable opens a native GPUI window without a console. The CLI
remains available separately:

```powershell
cargo build --locked --release -p bazel-cache-server --bin bazel-cache-server
.\target\release\bazel-cache-server.exe --root D:\Cashew --max-size 100GB serve
```

The Windows CLI defaults its storage root to the user's platform cache directory;
Unix CLI/container defaults remain `/data`. CLI shutdown handles Ctrl+C and Windows
console close/shutdown events. The Windows GitHub Actions workflow tests the cache
engine and desktop lifecycle, builds both executables, exercises CLI `stats`, opens
and closes the native desktop window, and uploads the executables together as
`cashew-windows-x86_64`.

[Verified Windows build and executable download](https://github.com/yume190/Cashew/actions/runs/44065718140/artifacts/14321443088):
92 engine tests + 3 desktop lifecycle tests passed on the native Windows runner;
both release executables built, and the Cashew window opened and exited normally.

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
`durability = "strict"` fsyncs file contents and runs SQLite with
`synchronous=FULL`. Unix additionally fsyncs the parent directory; Windows
publishes on the same volume using `MoveFileExW(MOVEFILE_WRITE_THROUGH)` without
cross-volume copying or replacing an existing blob. Reindex timestamps use Unix
change time or Windows creation time. Power-loss guarantees depend on the host
filesystem and storage hardware.

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
cargo test -p bazel-cache-server  # engine + desktop lifecycle regressions
cargo clippy --workspace --all-targets
```

End-to-end verification used Bazel 9.3 against a live server: cold build uploads
9 blobs / 3 action results, `bazel clean` + rebuild reports `3 remote cache hit`,
and the 8 MiB artifact comes back byte-identical.
