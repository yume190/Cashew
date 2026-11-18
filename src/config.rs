//! Configuration: TOML file, then environment, then CLI overrides.
//!
//! Precedence (highest first): CLI > environment > TOML > defaults.
//! Invalid configuration fails startup; there is never a silent fallback.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const ENV_PREFIX: &str = "BAZEL_CACHE";

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub eviction: EvictionConfig,
    pub auth: AuthConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    /// Advertised + enforced cap for BatchRead/BatchUpdate payloads.
    #[serde(deserialize_with = "de_size")]
    pub max_batch_total_size_bytes: u64,
    /// Chunk size used for ByteStream.Read responses and disk IO.
    #[serde(deserialize_with = "de_size")]
    pub stream_chunk_size_bytes: u64,
    /// HTTP/2 concurrent stream cap per connection; `None` = tonic default.
    pub max_concurrent_streams: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    pub root: PathBuf,
    /// Byte budget for CAS payload; eviction watermarks are fractions of this.
    #[serde(deserialize_with = "de_size")]
    pub max_size: u64,
    pub durability: Durability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    /// No per-blob fsync; data is rebuildable by Bazel.
    Cache,
    /// Fsync file, durable publication (Unix directory sync / Windows write-through), SQLite FULL.
    Strict,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EvictionConfig {
    pub policy: PolicyKind,
    pub high_watermark: f64,
    pub low_watermark: f64,
    /// Interval between GC sweeps.
    #[serde(deserialize_with = "de_duration")]
    pub interval: Duration,
    /// Rows considered per victim-selection query.
    pub batch_size: usize,
    /// Required when `policy = "ttl"`: maximum entry age.
    #[serde(default, deserialize_with = "de_opt_duration")]
    pub ttl: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyKind {
    Lru,
    Ttl,
    Fifo,
    None,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub token: Option<TokenConfig>,
    pub tls: Option<TlsConfig>,
    pub oidc: Option<OidcConfig>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    #[default]
    None,
    Token,
    Mtls,
    Oidc,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TokenConfig {
    /// Inline token. Discouraged; prefer `file` or `env`.
    pub value: Option<String>,
    /// File containing the token (trailing whitespace trimmed).
    pub file: Option<PathBuf>,
    /// Environment variable holding the token.
    pub env: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    /// Client CA bundle; required for `mode = "mtls"`.
    #[serde(default)]
    pub client_ca: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    pub issuer: String,
    pub audience: String,
    /// Defaults to `<issuer>/.well-known/openid-configuration` discovery.
    #[serde(default)]
    pub jwks_url: Option<String>,
    #[serde(default = "default_jwks_refresh", deserialize_with = "de_duration")]
    pub refresh_interval: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub listen: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:50051".parse().expect("valid default listen addr"),
            // REAPI's conventional batch ceiling; keeps gRPC messages under 4 MiB.
            max_batch_total_size_bytes: 4 * 1024 * 1024,
            stream_chunk_size_bytes: 1024 * 1024,
            max_concurrent_streams: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            root: default_storage_root(),
            max_size: 800 * 1000 * 1000 * 1000,
            durability: Durability::Cache,
        }
    }
}

fn default_storage_root() -> PathBuf {
    #[cfg(windows)]
    {
        directories::ProjectDirs::from("dev", "yume", "Cashew")
            .expect("Windows user profile must be available")
            .cache_dir().to_path_buf()
    }
    #[cfg(not(windows))]
    { PathBuf::from("/data") }
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            policy: PolicyKind::Lru,
            high_watermark: 0.90,
            low_watermark: 0.75,
            interval: Duration::from_secs(60),
            batch_size: 1000,
            ttl: None,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: "0.0.0.0:9090".parse().expect("valid default metrics addr"),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Pretty,
        }
    }
}

fn default_jwks_refresh() -> Duration {
    Duration::from_secs(300)
}

/// Overrides supplied on the command line; `None` means "keep lower-precedence value".
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub listen: Option<SocketAddr>,
    pub root: Option<PathBuf>,
    pub max_size: Option<u64>,
    pub durability: Option<Durability>,
    pub eviction_policy: Option<PolicyKind>,
    pub auth_mode: Option<AuthMode>,
    pub metrics_listen: Option<SocketAddr>,
    pub metrics_enabled: Option<bool>,
    pub log_level: Option<String>,
    pub log_format: Option<LogFormat>,
}

impl Config {
    /// Load TOML (if given), apply environment, apply CLI overrides, validate.
    pub fn load(path: Option<&Path>, overrides: &Overrides) -> Result<Self> {
        let mut cfg = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p)
                    .with_context(|| format!("reading config file {}", p.display()))?;
                toml::from_str(&text)
                    .with_context(|| format!("parsing config file {}", p.display()))?
            }
            None => Config::default(),
        };
        cfg.apply_env()?;
        cfg.apply_overrides(overrides);
        cfg.validate()?;
        Ok(cfg)
    }

    fn apply_env(&mut self) -> Result<()> {
        if let Some(v) = env_var("SERVER_LISTEN") {
            self.server.listen = v.parse().context("BAZEL_CACHE_SERVER_LISTEN")?;
        }
        if let Some(v) = env_var("SERVER_MAX_BATCH_TOTAL_SIZE_BYTES") {
            self.server.max_batch_total_size_bytes =
                parse_size(&v).context("BAZEL_CACHE_SERVER_MAX_BATCH_TOTAL_SIZE_BYTES")?;
        }
        if let Some(v) = env_var("SERVER_STREAM_CHUNK_SIZE_BYTES") {
            self.server.stream_chunk_size_bytes =
                parse_size(&v).context("BAZEL_CACHE_SERVER_STREAM_CHUNK_SIZE_BYTES")?;
        }
        if let Some(v) = env_var("STORAGE_ROOT") {
            self.storage.root = PathBuf::from(v);
        }
        if let Some(v) = env_var("STORAGE_MAX_SIZE") {
            self.storage.max_size = parse_size(&v).context("BAZEL_CACHE_STORAGE_MAX_SIZE")?;
        }
        if let Some(v) = env_var("STORAGE_DURABILITY") {
            self.storage.durability = match v.as_str() {
                "cache" => Durability::Cache,
                "strict" => Durability::Strict,
                other => bail!("BAZEL_CACHE_STORAGE_DURABILITY: unknown value {other:?}"),
            };
        }
        if let Some(v) = env_var("EVICTION_POLICY") {
            self.eviction.policy = parse_policy(&v)?;
        }
        if let Some(v) = env_var("EVICTION_HIGH_WATERMARK") {
            self.eviction.high_watermark = v.parse().context("BAZEL_CACHE_EVICTION_HIGH_WATERMARK")?;
        }
        if let Some(v) = env_var("EVICTION_LOW_WATERMARK") {
            self.eviction.low_watermark = v.parse().context("BAZEL_CACHE_EVICTION_LOW_WATERMARK")?;
        }
        if let Some(v) = env_var("EVICTION_TTL") {
            self.eviction.ttl = Some(parse_duration(&v).context("BAZEL_CACHE_EVICTION_TTL")?);
        }
        if let Some(v) = env_var("AUTH_MODE") {
            self.auth.mode = match v.as_str() {
                "none" => AuthMode::None,
                "token" => AuthMode::Token,
                "mtls" => AuthMode::Mtls,
                "oidc" => AuthMode::Oidc,
                other => bail!("BAZEL_CACHE_AUTH_MODE: unknown value {other:?}"),
            };
        }
        if let Some(v) = env_var("AUTH_TOKEN") {
            let token = self.auth.token.get_or_insert_with(TokenConfig::default);
            token.value = Some(v);
        }
        if let Some(v) = env_var("METRICS_ENABLED") {
            self.metrics.enabled = parse_bool(&v).context("BAZEL_CACHE_METRICS_ENABLED")?;
        }
        if let Some(v) = env_var("METRICS_LISTEN") {
            self.metrics.listen = v.parse().context("BAZEL_CACHE_METRICS_LISTEN")?;
        }
        if let Some(v) = env_var("LOGGING_LEVEL") {
            self.logging.level = v;
        }
        if let Some(v) = env_var("LOGGING_FORMAT") {
            self.logging.format = match v.as_str() {
                "pretty" => LogFormat::Pretty,
                "json" => LogFormat::Json,
                other => bail!("BAZEL_CACHE_LOGGING_FORMAT: unknown value {other:?}"),
            };
        }
        Ok(())
    }

    fn apply_overrides(&mut self, o: &Overrides) {
        if let Some(v) = o.listen {
            self.server.listen = v;
        }
        if let Some(v) = &o.root {
            self.storage.root = v.clone();
        }
        if let Some(v) = o.max_size {
            self.storage.max_size = v;
        }
        if let Some(v) = o.durability {
            self.storage.durability = v;
        }
        if let Some(v) = o.eviction_policy {
            self.eviction.policy = v;
        }
        if let Some(v) = o.auth_mode {
            self.auth.mode = v;
        }
        if let Some(v) = o.metrics_listen {
            self.metrics.listen = v;
        }
        if let Some(v) = o.metrics_enabled {
            self.metrics.enabled = v;
        }
        if let Some(v) = &o.log_level {
            self.logging.level = v.clone();
        }
        if let Some(v) = o.log_format {
            self.logging.format = v;
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.storage.max_size == 0 {
            bail!("storage.max_size must be > 0");
        }
        if self.server.max_batch_total_size_bytes == 0 {
            bail!("server.max_batch_total_size_bytes must be > 0");
        }
        if self.server.stream_chunk_size_bytes == 0 {
            bail!("server.stream_chunk_size_bytes must be > 0");
        }
        let (hi, lo) = (self.eviction.high_watermark, self.eviction.low_watermark);
        if !(0.0..=1.0).contains(&hi) || !(0.0..=1.0).contains(&lo) {
            bail!("eviction watermarks must be within 0.0..=1.0 (high={hi}, low={lo})");
        }
        if lo >= hi {
            bail!("eviction.low_watermark ({lo}) must be below high_watermark ({hi})");
        }
        if self.eviction.interval.is_zero() {
            bail!("eviction.interval must be > 0");
        }
        if self.eviction.batch_size == 0 {
            bail!("eviction.batch_size must be > 0");
        }
        if self.eviction.policy == PolicyKind::Ttl && self.eviction.ttl.is_none() {
            bail!("eviction.ttl is required when eviction.policy = \"ttl\"");
        }
        match self.auth.mode {
            AuthMode::None => {}
            AuthMode::Token => {
                let t = self
                    .auth
                    .token
                    .as_ref()
                    .context("auth.token section is required when auth.mode = \"token\"")?;
                if t.value.is_none() && t.file.is_none() && t.env.is_none() {
                    bail!("auth.token requires one of: value, file, env");
                }
            }
            AuthMode::Mtls => {
                let tls = self
                    .auth
                    .tls
                    .as_ref()
                    .context("auth.tls section is required when auth.mode = \"mtls\"")?;
                if tls.client_ca.is_none() {
                    bail!("auth.tls.client_ca is required when auth.mode = \"mtls\"");
                }
            }
            AuthMode::Oidc => {
                let o = self
                    .auth
                    .oidc
                    .as_ref()
                    .context("auth.oidc section is required when auth.mode = \"oidc\"")?;
                if o.issuer.is_empty() {
                    bail!("auth.oidc.issuer must not be empty");
                }
                if o.audience.is_empty() {
                    bail!("auth.oidc.audience must not be empty");
                }
                if o.refresh_interval.is_zero() {
                    bail!("auth.oidc.refresh_interval must be > 0");
                }
            }
        }
        if self.logging.level.parse::<tracing::level_filters::LevelFilter>().is_err()
            && !self.logging.level.contains('=')
        {
            bail!("logging.level {:?} is not a valid filter", self.logging.level);
        }
        Ok(())
    }

    pub fn cas_dir(&self) -> PathBuf {
        self.storage.root.join("cas")
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.storage.root.join("tmp")
    }

    pub fn db_path(&self) -> PathBuf {
        self.storage.root.join("metadata.sqlite")
    }

    /// Byte level at which a GC sweep starts.
    pub fn high_watermark_bytes(&self) -> u64 {
        (self.storage.max_size as f64 * self.eviction.high_watermark) as u64
    }

    /// Byte level a GC sweep must reach before stopping.
    pub fn low_watermark_bytes(&self) -> u64 {
        (self.storage.max_size as f64 * self.eviction.low_watermark) as u64
    }
}

fn env_var(suffix: &str) -> Option<String> {
    std::env::var(format!("{ENV_PREFIX}_{suffix}")).ok().filter(|v| !v.is_empty())
}

fn parse_policy(v: &str) -> Result<PolicyKind> {
    Ok(match v {
        "lru" => PolicyKind::Lru,
        "ttl" => PolicyKind::Ttl,
        "fifo" => PolicyKind::Fifo,
        "none" => PolicyKind::None,
        other => bail!("unknown eviction policy {other:?} (expected lru, ttl, fifo, none)"),
    })
}

fn parse_bool(v: &str) -> Result<bool> {
    match v {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("expected a boolean, got {other:?}"),
    }
}

/// Parse `"800GB"`, `"1.5TiB"`, `"4194304"`. SI units are powers of 1000,
/// IEC units powers of 1024.
pub fn parse_size(input: &str) -> Result<u64> {
    let s = input.trim();
    if s.is_empty() {
        bail!("empty size value");
    }
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '_'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num
        .replace('_', "")
        .parse()
        .with_context(|| format!("invalid size number in {input:?}"))?;
    if num < 0.0 {
        bail!("size must not be negative: {input:?}");
    }
    let mult: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" => 1e3,
        "m" | "mb" => 1e6,
        "g" | "gb" => 1e9,
        "t" | "tb" => 1e12,
        "kib" => 1024.0,
        "mib" => 1024f64.powi(2),
        "gib" => 1024f64.powi(3),
        "tib" => 1024f64.powi(4),
        other => bail!("unknown size unit {other:?} in {input:?}"),
    };
    Ok((num * mult) as u64)
}

/// Parse `"60s"`, `"10m"`, `"24h"`, `"7d"`, or bare seconds.
pub fn parse_duration(input: &str) -> Result<Duration> {
    let s = input.trim();
    if s.is_empty() {
        bail!("empty duration value");
    }
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '_'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num
        .replace('_', "")
        .parse()
        .with_context(|| format!("invalid duration number in {input:?}"))?;
    if num < 0.0 {
        bail!("duration must not be negative: {input:?}");
    }
    let secs = match unit.trim() {
        "" | "s" | "sec" | "secs" => num,
        "ms" => num / 1000.0,
        "m" | "min" | "mins" => num * 60.0,
        "h" | "hr" | "hrs" => num * 3600.0,
        "d" | "day" | "days" => num * 86400.0,
        other => bail!("unknown duration unit {other:?} in {input:?}"),
    };
    Ok(Duration::from_secs_f64(secs))
}

fn de_size<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    use serde::de::Error as _;
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SizeRepr {
        Int(u64),
        Str(String),
    }
    match SizeRepr::deserialize(d)? {
        SizeRepr::Int(v) => Ok(v),
        SizeRepr::Str(s) => parse_size(&s).map_err(D::Error::custom),
    }
}

fn de_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    use serde::de::Error as _;
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DurRepr {
        Secs(u64),
        Str(String),
    }
    match DurRepr::deserialize(d)? {
        DurRepr::Secs(v) => Ok(Duration::from_secs(v)),
        DurRepr::Str(s) => parse_duration(&s).map_err(D::Error::custom),
    }
}

fn de_opt_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    #[derive(Deserialize)]
    struct Wrap(#[serde(deserialize_with = "de_duration")] Duration);
    Ok(Option::<Wrap>::deserialize(d)?.map(|w| w.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sizes_and_durations() {
        assert_eq!(parse_size("800GB").unwrap(), 800_000_000_000);
        assert_eq!(parse_size("1GiB").unwrap(), 1_073_741_824);
        assert_eq!(parse_size("4194304").unwrap(), 4_194_304);
        assert_eq!(parse_size("1.5 TiB").unwrap(), 1_649_267_441_664);
        assert!(parse_size("5PB").is_err());
        assert_eq!(parse_duration("10m").unwrap(), Duration::from_secs(600));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert!(parse_duration("3 fortnights").is_err());
    }

    #[test]
    fn toml_accepts_documented_example() {
        let cfg: Config = toml::from_str(
            r#"
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
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.storage.max_size, 800_000_000_000);
        assert_eq!(cfg.eviction.policy, PolicyKind::Lru);
        assert_eq!(cfg.logging.format, LogFormat::Json);
        assert_eq!(cfg.cas_dir(), PathBuf::from("/data/cas"));
        assert_eq!(cfg.db_path(), PathBuf::from("/data/metadata.sqlite"));
    }

    #[test]
    fn rejects_invalid_combinations() {
        let mut cfg = Config::default();
        cfg.eviction.policy = PolicyKind::Ttl;
        assert!(cfg.validate().is_err(), "ttl policy without ttl must fail");
        cfg.eviction.ttl = Some(Duration::from_secs(3600));
        cfg.validate().unwrap();

        cfg.eviction.low_watermark = 0.95;
        assert!(cfg.validate().is_err(), "low >= high must fail");
        cfg.eviction.low_watermark = 0.75;

        cfg.auth.mode = AuthMode::Token;
        assert!(cfg.validate().is_err(), "token mode without secret must fail");
        cfg.auth.token = Some(TokenConfig {
            value: Some("s3cret".into()),
            ..Default::default()
        });
        cfg.validate().unwrap();

        cfg.auth.mode = AuthMode::Mtls;
        assert!(cfg.validate().is_err(), "mtls without tls section must fail");
    }

    #[test]
    fn unknown_keys_fail_startup() {
        let err = toml::from_str::<Config>("[storage]\nroot = \"/data\"\ntypo = 1\n").unwrap_err();
        assert!(err.to_string().contains("typo"), "{err}");
    }

    #[test]
    fn watermark_bytes_track_max_size() {
        let mut cfg = Config::default();
        cfg.storage.max_size = 1000;
        assert_eq!(cfg.high_watermark_bytes(), 900);
        assert_eq!(cfg.low_watermark_bytes(), 750);
    }
}
