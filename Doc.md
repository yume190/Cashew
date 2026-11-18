# Bazel LAN Remote Cache Server

## 1. Goal

實作一個輕量、單機、以 Rust 撰寫的 **Bazel Remote Cache Server**。

主要使用情境是多個 Coding Agent / Git worktree / CI 在區域網路內共享 Bazel build cache，減少重複 build/test 成本。

典型環境：

```text
                    LAN
                     │
        ┌────────────┼────────────┐
        │            │            │
      Mac A        Mac B          CI
        │            │            │
   Worktree A   Worktree C        │
   Worktree B   Worktree D        │
        │            │            │
      Agents       Agents         │
        │            │            │
        └─────── Bazel clients ───┘
                     │
                  gRPC REAPI
                     │
             ┌───────▼────────┐
             │ Rust Cache     │
             │ Server         │
             └───────┬────────┘
                     │
                Local Storage
```

這不是 Remote Execution Server。

不提供：

```text
Remote Execution
Scheduler
Workers
Distributed Execution
Cloud Storage Backend
```

---

# 2. Core Requirements

目前已確定：

```text
Language:             Rust
Client:               Bazel only
Protocol:             gRPC REAPI
REAPI scope:          Cache only
Network:              Local LAN
Expected clients:     約 2–5
Storage:              Local disk
Cache capacity:       約 0.5–1 TB
Metadata database:    SQLite
Remote Execution:     No
Container:            Linux OCI container
```

主要 deployment：

```text
Linux host
    │
Docker / Podman
    │
Rust Cache Server
    │
/data
    │
local SSD / NVMe
```

macOS / Windows host 可以透過 Docker / Podman 執行。

Server implementation 本身不需要處理 Windows filesystem semantics。

---

# 3. REAPI

使用：

```text
gRPC
+
Remote Execution API v2
```

但只實作 Remote Cache 所需 subset。

預計支援：

```text
Capabilities
ActionCache
ContentAddressableStorage
ByteStream
```

具體 RPC：

```text
GetCapabilities

GetActionResult
UpdateActionResult

FindMissingBlobs
BatchReadBlobs
BatchUpdateBlobs

ByteStream.Read
ByteStream.Write
```

不實作：

```text
Execution
Scheduler
Worker
Operations for remote execution
```

---

# 4. Bazel Compatibility

目前目標：

```text
Bazel 8+
REAPI v2
SHA-256
```

支援：

```text
ActionCache
CAS
ByteStream

FindMissingBlobs
BatchReadBlobs
BatchUpdateBlobs

GetCapabilities

instance_name
```

`instance_name` 可以作為 namespace：

```text
(instance_name, digest)
```

例如：

```text
repo-a / abc123
repo-b / abc123
```

視為不同 cache namespace。

V1 暫不支援：

```text
Remote Execution
alternative digest algorithms
zstd compressed blobs
```

Server 必須透過 Capabilities 正確宣告自己實際支援的能力。

---

# 5. Storage Architecture

採用：

```text
/data
├── cas/
├── tmp/
└── metadata.sqlite
```

## CAS

CAS payload 存 filesystem。

例如 digest：

```text
abcdef123456...
```

可以存成：

```text
/data/cas/ab/abcdef123456...
```

使用 digest prefix 做 directory sharding，避免所有 blob 集中在單一 directory。

CAS 儲存：

```text
raw bytes
```

V1 不做 server-side storage compression。

CAS 是 immutable。

相同 digest 永遠代表相同 content。

---

# 6. Action Cache

採用：

```text
AC -> SQLite
```

不為每個 ActionResult 建立 filesystem file。

SQLite 儲存：

```text
action digest
serialized ActionResult protobuf
timestamps / metadata
```

因此：

```text
CAS payload    -> filesystem
Action Cache   -> SQLite
Metadata       -> SQLite
```

---

# 7. Temporary Upload

Upload 不可以直接寫入 CAS final path。

流程：

```text
ByteStream.Write
       │
       ▼
/data/tmp/<random>.part
       │
       ▼
stream write
       │
       ├── calculate SHA-256
       └── count size
       │
       ▼
verify expected digest + size
       │
       ▼
close
       │
       ▼
publish CAS
```

只有完整驗證成功的 blob 才能進入：

```text
/data/cas/...
```

CAS 與 tmp 必須位於同一個 filesystem：

```text
/data/cas
/data/tmp
```

不允許把 tmp 設定到另外一個 volume。

如果 final digest 已經存在：

```text
delete temp
return success
```

不 overwrite existing CAS。

也就是：

```text
publish-if-absent
```

---

# 8. Streaming

Large object 必須 streaming。

禁止核心 storage API 要求：

```rust
put(Vec<u8>)
```

這種 artifact size = RAM usage 的設計。

概念：

```text
Network
  │
  ▼
chunk
  │
  ├── hash.update()
  │
  ▼
File write
```

Download：

```text
Disk
 │
 ▼
chunks
 │
 ▼
ByteStream.Read
```

Server RAM usage 不應該隨單一 artifact size 線性成長。

具體 chunk size / thresholds 尚未決定。

---

# 9. Crash Consistency

核心原則：

> Filesystem CAS 是 source of truth，SQLite 是 rebuildable metadata/index。

CAS commit 順序：

```text
1. write temp
2. verify size + SHA-256
3. close/flush
4. atomic publish/rename into CAS
5. update SQLite metadata
```

不能先寫 SQLite 再 publish CAS。

如果 crash 發生：

```text
CAS 已存在
SQLite metadata 尚未寫入
```

這是允許的，因為 metadata 可以重建。

如果 crash 發生在 upload：

```text
/data/tmp/*.part
```

留下 incomplete temp file，啟動時可以清理。

## Durability

預計提供：

```text
cache
strict
```

例如：

```toml
[storage]
durability = "cache"
```

`cache`：

```text
不強制每個 blob 做完整 fsync durability
```

因為資料可以由 Bazel rebuild。

`strict`：

```text
fsync file
atomic rename
fsync parent directory
SQLite commit
```

作為較高 durability 選項。

---

# 10. AC / CAS Consistency

ActionResult 會引用 CAS blobs：

```text
ActionResult
├── output A -> CAS A
├── output B -> CAS B
└── stdout   -> CAS C
```

採用 **lazy validation**。

## AC Write

原則：

```text
CAS blobs exist
      ↓
UpdateActionResult
      ↓
AC write
```

寫入 AC 前確認必要 CAS 已存在。

## AC Read

```text
GetActionResult
       │
       ▼
read ActionResult
       │
       ▼
validate referenced CAS
       │
    ┌──┴──┐
   yes    missing
    │       │
 return    delete stale AC
 result    return NOT_FOUND
```

不做 CAS refcount。

不要求 GC 維護完整 AC → CAS dependency graph。

可以未來加入 background stale AC cleanup，但不是 correctness requirement。

---

# 11. Concurrency Model

核心原則：

```text
No global lock
Per-digest coordination only
CAS immutable
```

## Duplicate Upload

如果兩個 client 同時 upload 相同 digest：

```text
Client A -> tmp/A
Client B -> tmp/B
```

兩邊可以同時寫。

第一個 publish 成功。

第二個發現 final CAS 已存在：

```text
delete tmp
return success
```

不需要 global upload lock。

## Concurrent Read

相同 blob：

```text
Reader A
Reader B
Reader C
```

允許 concurrent read。

## Read vs Write

Reader 只看 final CAS。

Writer 只寫 tmp。

所以 reader 永遠不會看到 partial blob。

## GC vs Reader

GC 不刪除 active reader 正在使用的 digest。

可以有 lightweight in-memory active-entry registry。

這些 runtime state 不需要 durable。

## SQLite

AC operation 使用短 transaction。

---

# 12. Authentication

Authentication 在 startup/config 選擇。

預計提供四種模式：

```text
none
token
mtls
oidc
```

架構上 authentication 與 cache/storage core 分離。

概念：

```text
gRPC Request
     │
     ▼
Authenticator
     │
     ▼
Principal
     │
     ▼
REAPI Service
```

模式：

### none

LAN trusted environment，不驗證 client。

### token

Static token / API key。

透過 gRPC metadata，例如：

```text
Authorization: Bearer <token>
```

Secret 應從：

```text
environment
或
secret file
```

取得。

### mTLS

Server/client certificate authentication。

設定包含：

```text
CA
server certificate
server private key
```

### OIDC

預留 enterprise identity integration：

```text
issuer
audience
```

Authentication 與 authorization 分開。

第一階段 authorization 可以單純：

```text
authenticated -> read/write
```

未來才考慮：

```text
read
write
admin
```

---

# 13. Cache / Eviction Policy

Cache policy 不寫死。

Startup 時選擇：

```toml
[eviction]
policy = "lru"
```

預計架構：

```rust
trait EvictionPolicy {
    fn on_read(...);
    fn on_write(...);
    fn select_victims(...);
}
```

可能的 policy：

```text
LRU
TTL
FIFO
None
```

具體 V1 要實作哪些尚未決定。

Storage capacity 與 eviction policy 分開。

例如：

```toml
[storage]
max_size = "800GB"

[eviction]
policy = "lru"
high_watermark = 0.90
low_watermark = 0.75
```

實體 disk 不建議全部配置給 cache，需要預留：

```text
SQLite
tmp upload
filesystem overhead
GC headroom
```

---

# 14. Metadata Database

選擇：

```text
SQLite
```

而不是 PostgreSQL。

理由：

```text
single server
local metadata
2–5 clients
不需要 distributed metadata
不需要 HA DB
```

預計：

```text
SQLite WAL
```

大 payload 不放 SQLite。

SQLite 只存：

```text
CAS metadata
Action Cache
access metadata
GC metadata
```

不建議每次 CAS GET 都立即：

```sql
UPDATE last_access_at
```

預計使用：

```text
GET
 ↓
in-memory access tracking
 ↓
batch
 ↓
SQLite transaction
```

避免 read-heavy workload 變成大量 DB write。

SQLite schema 尚未正式決定。

---

# 15. Configuration

使用：

```text
TOML
+
environment variables
+
CLI overrides
```

優先順序：

```text
CLI
 >
Environment
 >
TOML
 >
defaults
```

例如：

```toml
[server]
listen = "0.0.0.0:50051"

[storage]
root = "/data"
max_size = "800GB"
durability = "cache"

[eviction]
policy = "lru"
high_watermark = 0.90
low_watermark = 0.75

[auth]
mode = "none"

[metrics]
enabled = true
listen = "0.0.0.0:9090"

[logging]
level = "info"
format = "json"
```

Startup 必須 validation。

錯誤 config：

```text
fail startup
```

不能 silent fallback。

V1 不需要 hot reload。

修改 config 後：

```text
restart container
```

即可。

---

# 16. Observability

V1 就提供 observability。

## Metrics

Prometheus-compatible endpoint，例如：

```text
:9090/metrics
```

主要 metrics：

```text
cas_hit_total
cas_miss_total

ac_hit_total
ac_miss_total

upload_bytes_total
download_bytes_total

upload_count
download_count

cache_size_bytes
cache_entry_count

cas_size_bytes
cas_entry_count
ac_entry_count

evicted_bytes_total
evicted_entries_total

request_duration
read_duration
write_duration
sqlite_duration
gc_duration
```

可以額外估算：

```text
avoided_bytes
```

用來觀察 shared cache 的實際效益。

## Logging

使用 structured logging。

Rust 可以考慮：

```text
tracing
tracing-subscriber
```

支援：

```text
pretty
json
```

---

# 17. Compression

V1：

```text
Disk representation:
raw bytes
```

不做 server-side transparent compression。

不做：

```text
CAS -> zstd -> disk
```

避免增加：

```text
CPU
metadata complexity
read decompression
write compression
hash complexity
```

REAPI compression capability 可以預留，但 V1 可以明確宣告不支援 compressed blobs。

未來根據實際 cache object compression ratio 再決定是否加入：

```text
storage.compression = "zstd"
```

---

# 18. Runtime / Filesystem / Hardware

Server implementation：

```text
Linux only
```

建議以 OCI container distribution：

```text
Docker
Podman
```

Persistence：

```text
/data
```

建議 filesystem：

```text
ext4
XFS
```

Storage：

```text
SSD
preferably NVMe
```

不建議把 `/data` 放：

```text
NFS
SMB
```

因為目前 architecture 假設 local filesystem semantics：

```text
atomic rename
SQLite WAL
low-latency metadata
```

建議起始硬體：

```text
4 CPU cores
8 GB RAM
1 TB SSD/NVMe
1 GbE minimum
2.5 GbE+ preferred
```

---

# 19. Deployment Contract

理想上整個 server 對外只需要：

```text
OCI image
+
/data persistent volume
+
gRPC port
+
optional metrics port
```

例如：

```text
container
├── :50051  REAPI gRPC
├── :9090   Metrics
└── /data
    ├── cas/
    ├── tmp/
    └── metadata.sqlite
```

因此可以部署到：

```text
Docker
Podman
future Kubernetes
```

但目前不需要 Kubernetes-specific functionality。

---

# 20. Explicit Non-Goals

目前不要實作：

```text
Remote Execution
Scheduler
Workers

Distributed CAS
Replication
HA

S3
GCS
Azure Blob

NFS/SMB storage backend

PostgreSQL

Windows-native server

server-side storage compression

multi-tenancy

complex authorization/RBAC

hot config reload
```

除非後續需求改變。

---

# 21. 設計決議（V1 已實作）

原本列為「尚未決定」的項目，已在實作前確認並完成，結果如下。
原始問題清單保留在本節末尾作為歷史紀錄。

## A. SQLite Schema（已決議）

```sql
instances(id INTEGER PK, name TEXT UNIQUE)              -- instance_name 正規化
cas_blobs(hash BLOB(32) PK, size_bytes, created_at, last_access_at) WITHOUT ROWID
  INDEX cas_lru(last_access_at), cas_fifo(created_at)
action_cache(instance_id, hash BLOB(32), size_bytes, result BLOB,
             created_at, last_access_at) PK(instance_id, hash, size_bytes) WITHOUT ROWID
  INDEX ac_lru(last_access_at)
counters(key TEXT PK, value INTEGER)                    -- cas_size_bytes / cas_entry_count / schema_version
```

```text
CAS/AC 分表（不共用）
digest 用 BLOB(32) raw，不用 hex TEXT
timestamp 一律 unix microseconds (INTEGER)
CAS 全域共用（content addressed），instance_name 只對 AC 分命名空間
access tracking：in-memory map，每 5s flush 成單一 transaction
counters 在 insert/delete 的同一 transaction 內維護，stats 不做 SUM 掃描
WAL：開檔時 checkpoint(TRUNCATE) 一次，之後交給 SQLite autocheckpoint
synchronous：cache=NORMAL / strict=FULL
```

## B. GC / Eviction（已決議）

```text
V1 提供 lru / ttl / fifo / none 四種
high/low watermark 預設 0.90 / 0.75
GC interval 預設 60s，batch size 預設 1000
ttl policy 每次 sweep 都清過期項目，不受 watermark 限制
active reader：in-memory lease registry，GC 跳過有 reader 的 blob
AC 清理：ttl policy 依年齡清除；其他 policy 僅在超過 1,000,000 筆時清最舊的
刪除順序：先刪 filesystem，再刪 SQLite（metadata 可重建，orphan blob 不可）
```

## C. Batch / ByteStream Threshold（已決議）

```text
max_batch_total_size_bytes 預設 4MiB（同時透過 Capabilities 宣告並強制執行）
stream_chunk_size_bytes 預設 1MiB
兩者皆可設定；客戶端（Bazel）自行決定 batch 或 stream
BatchReadBlobs 超出上限的項目回 RESOURCE_EXHAUSTED，不靜默截斷
```

## D. Recovery / Reindex（已決議）

```text
CLI：serve / reindex / gc / stats
reindex：掃描 cas/ 重建 cas_blobs 與 counters（AC 僅存在 SQLite，無法重建）
tmp 清理：serve 與 reindex 啟動時刪除所有 *.part
```

## E. Health / Readiness（已決議）

```text
gRPC health checking（grpc.health.v1）
HTTP /health  liveness
HTTP /ready   readiness：storage 可寫 + SQLite 可寫（成功結果快取 2s）
```

## F. Authentication（已決議）

```text
V1 四種全部實作：none / token / mtls / oidc
token：constant-time 比對，secret 來自 value > env > file
mtls：client CA 驗證，principal 取自憑證 CN / SAN / serial
oidc：JWKS 背景更新，驗證 iss / aud / exp，支援 RS256/384/512 與 ES256
```

## G. Authorization（已決議）

```text
authenticated -> full cache access
Principal 會附在 request extension 上供 logging 使用
```

## H. Compression（維持原決定）

```text
V1 只支援 identity；compressed-blobs resource 回 UNIMPLEMENTED
Capabilities 誠實宣告 supported_compressors = [IDENTITY]
```

## I. 驗證（已執行）

```text
92 個 unit test（storage / sqlite / auth / gc / REAPI handler / HTTP surface）
Bazel 9.3 端到端：cold build 上傳 9 blobs + 3 action results，
bazel clean 後重建回報 3 remote cache hit，8MiB artifact 位元相同
token 模式：無 header 被拒（UNAUTHENTICATED），帶 header 正常
strict durability、gc、reindex、stats CLI 均實測通過
正式 benchmark（與 NativeLink / bazel-remote 比較）尚未進行
```

## J. Cashew GPUI desktop / Windows

新增 `desktop-gpui` workspace member，使用與 SwiftCache 相同的 `gpui-kit = 0.9.0`。
桌面與既有 CLI 共用 `src/server.rs` 的 REAPI cache engine；桌面由專用 Tokio
執行緒管理服務，不需要 sidecar executable。介面提供總覽、服務設定、最近 200 行日誌，
設定保存在平台設定目錄下的 `desktop.json`；可用 `CASHEW_DATA_DIR` 隔離設定與預設快取。

桌面預設 100GB、gRPC `127.0.0.1:50051`、metrics `127.0.0.1:9090`，不自動啟動。
進階認證、TLS、GC、durability 由選填 TOML 載入，介面欄位優先於環境與 TOML。
服務必須成功 bind 所有啟用的 listener 才回報 Running；啟動錯誤會顯示在介面。
停止與關閉視窗會等待服務結束、flush access timestamps 並釋放連接埠。

Windows 使用 MSVC / Windows SDK，桌面執行檔為 `cashew.exe`；CLI 保留
`bazel-cache-server.exe`。檔案 volume ID 透過 Windows handle 查詢，CAS/tmp 必須同卷，
同卷發布使用 `MoveFileExW`，strict 模式先 flush file contents，再使用
`MOVEFILE_WRITE_THROUGH`；Unix 保留 rename + directory fsync。Windows reindex 使用
檔案 creation time；CLI 支援 Windows Ctrl+C、console close/shutdown。
`.github/workflows/windows.yml` 負責 Windows 測試、release build 與 exe artifacts。
Windows 原生驗證已通過：[GitHub Actions run 44065718140](https://github.com/yume190/Cashew/actions/runs/44065718140)。
92 個 engine tests、3 個 desktop lifecycle tests 全數通過，CLI / GPUI release
執行檔均建置成功；CLI `stats` 實跑成功，`cashew.exe` 建立標題為 Cashew 的原生視窗，
送出關閉事件後以 exit code 0 結束。兩個 exe 已上傳為 `cashew-windows-x86_64` artifact。

macOS 原生 GPUI 視窗實測：由介面啟動、停止與重新啟動服務；停止後 gRPC/metrics
連接埠均關閉，日誌含正常 shutdown。Bazel 9.3.2 冷建置上傳 8MiB 產物，`bazel clean`
後重建回報 `1 remote cache hit`，兩次產物 SHA-256 相同；介面顯示 3 CAS blobs、
1 Action Cache result 與 8MiB 上傳/下載。另新增三個生命週期 regression tests，
涵蓋 gRPC/metrics bind 失敗後的復原、啟動中停止、重啟及 controller drop 後釋放資源。

## K. 桌面設定與流量事件

服務設定將監聽範圍與 Port 分開：僅本機 `127.0.0.1`、區網 `0.0.0.0`、自訂 IP；
儲存位置可輸入或選擇資料夾。有效內容停止輸入 400ms 後自動保存，離開時補寫待儲存內容，
並明示保存狀態。錯誤內容不覆寫上一份設定。JSON 以同目錄暫存檔完整寫入、同步，再 atomic replace。
外觀在「設定」頁選擇跟隨系統、淺色、深色，立即套用並保存於獨立的 `appearance.json`。

區網模式仍 bind `0.0.0.0`，但總覽與複製的 client URL 使用偵測到的本機區網 IP。
找不到可用位址時顯示原因，不提供 wildcard／loopback 的假替代連線位址。

「流量／事件」以每條 TCP/gRPC 連線為 session，接受連線時建立獨立 ID，保留遠端 IP:port，
同一 IP 的重連與同時連線不會合併。主清單顯示連線中／已關閉、雙向即時速率與累計資料量、
Upload / Query / Download 已完成 RPC 次數；只有 Download 操作的 session 明示「僅下載」。
點選 session 查看開始、結束、最後活動、時長，以及各次 RPC 的結果、payload、耗時與 digest／批次明細。
保留所有使用中的 session 與最近 200 條已關閉 session，每條各保留最新 200 筆操作；
操作累計不受明細保留上限影響。停止後仍能切換查看所有保留的 session，下一次啟動才清除。

網路統計直接量測 socket 讀寫，含 HTTP/2 / TLS 通訊開銷，不含 TCP/IP 標頭；
client → cache 是上傳，cache → client 是下載。控制訊框不算 Upload RPC，
所以僅下載 session 仍可能有少量上行網路位元組。總覽維持 CAS payload 統計，操作明細另列 payload。
傳輸尚未完成時就更新網路速率。ByteStream download 在真正結束、錯誤或取消時記錄事件。
事件與 session 蒐集只在桌面啟用；CLI 不建立 session registry 或操作 ring。

參考 [GlassWire](https://www.glasswire.com/userguide/)、
[NetLimiter](https://netlimiter.com/docs/user-gui-client/child-windows/connection-log)、
[Little Snitch](https://help.obdev.at/littlesnitch7/monitor-connection-list)：
優先呈現連線歸屬、雙向速率／累計、連線生命週期與可展開的操作明細。

本次實測一般 gRPC 與 mTLS：一條連線完成 Query ×2、Upload ×1、Download ×1，
另一條只有 Download ×1；兩次 8MiB 下載 SHA-256 一致，串流上傳尚未完成即觀察到速率。
個別關閉連線、明暗模式、920px／1180px 視窗、篩選及停止後切換歷史均走過原生 GPUI。
macOS 當時鎖定且螢幕休眠，未能擷取畫面；未宣稱完成本次截圖視覺檢查。

Windows 圖示由 `desktop-gpui/build.rs` 將多尺寸 `assets/cashew.ico` 嵌入 numeric resource 1，
符合 GPUI Windows backend 的載入方式；原生 CI 同時檢查 PE resource 與視窗 class icon。



---

# 21-X. 原始待討論清單（歷史紀錄）

以下為實作前的原始問題清單，保留以供追溯。


## A. SQLite Schema

尚未決定：

```text
CAS metadata table
Action Cache table
access metadata
GC metadata
indexes
timestamps
instance_name representation
```

以及：

```text
CAS/AC 是否共用 table
digest 用 BLOB 還是 TEXT
SQLite batching strategy
WAL checkpoint policy
```

這是下一個值得討論的項目。

---

## B. GC / Eviction Implementation

只確定：

```text
policy startup selectable
```

尚未決定 V1 實際提供：

```text
LRU?
TTL?
FIFO?
None?
size-aware?
```

以及：

```text
high/low watermark defaults
GC batch size
GC interval
active-reader handling
GC priority
```

---

## C. Large Blob / Batch Threshold

已決定支援：

```text
BatchReadBlobs
BatchUpdateBlobs
ByteStream
```

但尚未決定：

```text
多大的 blob 使用 ByteStream
多小的 blob 使用 Batch API
batch maximum size
stream chunk size
maximum concurrent streams
```

需要 benchmark 後決定。

---

## D. Recovery / Reindex Tooling

已確定 filesystem CAS 是 source of truth。

但尚未決定 CLI，例如：

```text
server --reindex
server verify
server gc
server stats
server repair
```

以及：

```text
SQLite corruption recovery
orphan CAS detection
stale metadata detection
tmp cleanup age
```

---

## E. Health / Readiness

尚未決定：

```text
gRPC health checking
HTTP /health
HTTP /ready
```

以及 readiness 是否檢查：

```text
disk writable
SQLite writable
free disk space
GC state
```

---

## F. Authentication Implementation Order

四種模式已決定：

```text
none
token
mTLS
OIDC
```

但尚未決定：

```text
是否 V1 四種全部完成
或
V1 none + token
V2 mTLS + OIDC
```

OIDC provider compatibility 也尚未定義。

---

## G. Authorization

Authentication 與 authorization 已決定分開。

但尚未決定是否需要：

```text
read-only
read-write
admin
```

目前可以暫時視為：

```text
authenticated -> full cache access
```

---

## H. Compression Transport Support

V1 disk 不壓縮已決定。

但 REAPI compressed transport：

```text
zstd
```

何時支援尚未決定。

---

## I. Benchmark / Acceptance Criteria

尚未定義。

至少應該 benchmark：

```text
cold cache
warm cache

small blobs
medium blobs
large blobs

1 client
2 clients
5 clients

simultaneous upload/download

GC under load
```

並測量：

```text
throughput
latency
CPU
RAM
disk IOPS
network throughput
SQLite contention
cache hit rate
```

還應該與至少一個成熟 implementation 比較，例如：

```text
NativeLink
或
bazel-remote
```

---

# 22. Recommended Next Design Order

後續 discussion 建議依序處理：

```text
1. SQLite schema
2. GC / eviction implementation
3. Batch vs ByteStream thresholds
4. Recovery / reindex
5. Health / readiness
6. Authentication implementation phases
7. Authorization
8. Benchmark / acceptance criteria
9. Rust crate / module architecture
10. Implementation plan
```

在這些項目確認以前，不要過早固定 internal architecture。

尤其：

```text
SQLite schema
GC model
streaming/batching
```

會直接影響 storage layer API，建議先完成這三項設計再開始大量實作。