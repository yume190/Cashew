//! Authentication (Doc.md §12).
//!
//! Four modes, selected at startup from `[auth]`:
//!
//! * `none`  — LAN-trusted; every caller is `anonymous`.
//! * `token` — static shared secret in `Authorization: Bearer <token>`.
//! * `mtls`  — TLS client certificates; identity comes from the peer leaf cert.
//! * `oidc`  — JWT bearer tokens validated against a cached JWKS.
//!
//! Authentication is deliberately separate from the cache core: the result of
//! every mode is a [`Principal`] stashed in the request extensions. Authorization
//! (phase one) is simply "authenticated ⇒ read/write".
//!
//! The request path never performs IO: the token secret and the JWKS key set are
//! resolved at startup and refreshed in the background.

use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Status};
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

use crate::config::{AuthConfig, AuthMode, OidcConfig, TokenConfig};

/// The authenticated caller. Inserted into every request's extensions.
#[derive(Clone, Debug)]
pub struct Principal {
    pub id: String,
    pub mode: &'static str,
}

impl Principal {
    fn anonymous() -> Self {
        Principal { id: "anonymous".to_string(), mode: "none" }
    }
}

/// Shared, cheap-to-clone authenticator. One instance is cloned into every
/// intercepted service.
#[derive(Clone)]
pub struct Authenticator {
    mode: AuthMode,
    inner: Arc<Inner>,
}

enum Inner {
    None,
    /// Pre-resolved shared secret.
    Token(Vec<u8>),
    Mtls,
    Oidc(Arc<OidcVerifier>),
}

impl Authenticator {
    pub async fn new(cfg: &AuthConfig) -> anyhow::Result<Authenticator> {
        let inner = match cfg.mode {
            AuthMode::None => Inner::None,
            AuthMode::Token => {
                let secret = resolve_token(cfg.token.as_ref())?;
                Inner::Token(secret.into_bytes())
            }
            AuthMode::Mtls => {
                let tls = cfg
                    .tls
                    .as_ref()
                    .ok_or_else(|| anyhow!("auth.mode = \"mtls\" requires an [auth.tls] section"))?;
                if tls.client_ca.is_none() {
                    bail!("auth.mode = \"mtls\" requires auth.tls.client_ca");
                }
                Inner::Mtls
            }
            AuthMode::Oidc => {
                let oidc = cfg
                    .oidc
                    .as_ref()
                    .ok_or_else(|| anyhow!("auth.mode = \"oidc\" requires an [auth.oidc] section"))?;
                Inner::Oidc(OidcVerifier::start(oidc).await?)
            }
        };
        Ok(Authenticator { mode: cfg.mode, inner: Arc::new(inner) })
    }

    /// Configured mode, for startup logging.
    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// True when the server cannot run without TLS (client certs are mandatory).
    pub fn requires_tls(&self) -> bool {
        self.mode == AuthMode::Mtls
    }

    pub fn authenticate(&self, req: &Request<()>) -> Result<Principal, Status> {
        match &*self.inner {
            Inner::None => Ok(Principal::anonymous()),
            Inner::Token(secret) => {
                let presented = bearer_token(req)?;
                if constant_time_eq(presented.as_bytes(), secret) {
                    Ok(Principal { id: "token".to_string(), mode: "token" })
                } else {
                    Err(Status::unauthenticated("invalid bearer token"))
                }
            }
            Inner::Mtls => {
                let certs = req
                    .peer_certs()
                    .ok_or_else(|| Status::unauthenticated("client certificate required"))?;
                let leaf = certs
                    .first()
                    .ok_or_else(|| Status::unauthenticated("client certificate required"))?;
                let id = principal_id_from_cert(leaf.as_ref())?;
                Ok(Principal { id, mode: "mtls" })
            }
            Inner::Oidc(verifier) => {
                let token = bearer_token(req)?;
                let id = verifier.verify(token)?;
                Ok(Principal { id, mode: "oidc" })
            }
        }
    }
}

impl Interceptor for Authenticator {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let principal = self.authenticate(&req)?;
        req.extensions_mut().insert(principal);
        Ok(req)
    }
}

// ---------------------------------------------------------------------------
// Bearer header
// ---------------------------------------------------------------------------

/// Extract the bearer credential from `authorization`. The scheme is matched
/// case-insensitively (`Bearer`, `bearer`, `BEARER`) per RFC 7235 §2.1.
fn bearer_token(req: &Request<()>) -> Result<&str, Status> {
    let value = req
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
    let value = value
        .to_str()
        .map_err(|_| Status::unauthenticated("malformed authorization header"))?;
    parse_bearer(value).ok_or_else(|| Status::unauthenticated("malformed authorization header"))
}

fn parse_bearer(value: &str) -> Option<&str> {
    let (scheme, rest) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim_start_matches(' ');
    if token.is_empty() { None } else { Some(token) }
}

// ---------------------------------------------------------------------------
// Static token
// ---------------------------------------------------------------------------

/// Precedence: inline `value`, then `env`, then `file`. An empty secret is a
/// startup error — silently accepting it would disable authentication.
fn resolve_token(cfg: Option<&TokenConfig>) -> anyhow::Result<String> {
    let cfg = cfg.ok_or_else(|| anyhow!("auth.mode = \"token\" requires an [auth.token] section"))?;

    if let Some(value) = &cfg.value {
        let value = value.trim_end_matches(['\r', '\n', ' ', '\t']);
        if value.is_empty() {
            bail!("auth.token.value is empty");
        }
        return Ok(value.to_string());
    }
    if let Some(var) = &cfg.env {
        let value = std::env::var(var)
            .with_context(|| format!("auth.token.env: environment variable {var} is not set"))?;
        let value = value.trim_end_matches(['\r', '\n', ' ', '\t']);
        if value.is_empty() {
            bail!("auth.token.env: environment variable {var} is empty");
        }
        return Ok(value.to_string());
    }
    if let Some(path) = &cfg.file {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading auth.token.file {}", path.display()))?;
        let value = raw.trim_end_matches(['\r', '\n', ' ', '\t']);
        if value.is_empty() {
            bail!("auth.token.file {} is empty", path.display());
        }
        return Ok(value.to_string());
    }
    bail!("auth.token requires one of: value, env, file")
}

/// Compare two secrets without leaking their contents through timing.
///
/// The loop always runs `max(len)` iterations — a length mismatch folds into the
/// accumulator instead of short-circuiting, so neither the length nor the
/// position of the first differing byte is observable.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    ct_scan(a, b).0 == 0
}

/// Returns `(difference accumulator, bytes examined)`. The iteration count is
/// returned so tests can prove the scan never exits early.
fn ct_scan(a: &[u8], b: &[u8]) -> (u32, usize) {
    let mut diff = (a.len() ^ b.len()) as u32;
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = if i < a.len() { a[i] } else { 0 };
        let y = if i < b.len() { b[i] } else { 0 };
        diff |= (x ^ y) as u32;
    }
    (diff, n)
}

// ---------------------------------------------------------------------------
// mTLS
// ---------------------------------------------------------------------------

/// Derive a stable identity from a client leaf certificate: subject CN, else the
/// first subject alternative name, else the serial number in hex.
fn principal_id_from_cert(der: &[u8]) -> Result<String, Status> {
    let (_, cert) = X509Certificate::from_der(der)
        .map_err(|e| Status::unauthenticated(format!("unparsable client certificate: {e}")))?;

    if let Some(cn) = cert.subject().iter_common_name().next()
        && let Ok(cn) = cn.as_str()
        && !cn.is_empty()
    {
        return Ok(cn.to_string());
    }

    if let Ok(Some(san)) = cert.subject_alternative_name()
        && let Some(name) = san.value.general_names.iter().find_map(general_name_to_string)
    {
        return Ok(name);
    }

    Ok(hex::encode(cert.raw_serial()))
}

fn general_name_to_string(name: &GeneralName<'_>) -> Option<String> {
    match name {
        GeneralName::DNSName(s) | GeneralName::RFC822Name(s) | GeneralName::URI(s) => {
            (!s.is_empty()).then(|| s.to_string())
        }
        GeneralName::IPAddress(bytes) => match bytes.len() {
            4 => {
                let o: [u8; 4] = (*bytes).try_into().ok()?;
                Some(Ipv4Addr::from(o).to_string())
            }
            16 => {
                let o: [u8; 16] = (*bytes).try_into().ok()?;
                Some(Ipv6Addr::from(o).to_string())
            }
            _ => None,
        },
        GeneralName::DirectoryName(n) => {
            n.iter_common_name().next().and_then(|cn| cn.as_str().ok()).map(str::to_string)
        }
        _ => None,
    }
}

/// Build the server-side TLS configuration implied by `[auth]`.
///
/// * `mode = "mtls"` → server identity plus a mandatory client CA.
/// * any other mode with `[auth.tls]` → plain server TLS, no client certs.
/// * no `[auth.tls]` → no TLS at all.
pub fn server_tls_config(cfg: &AuthConfig) -> anyhow::Result<Option<ServerTlsConfig>> {
    let Some(tls) = cfg.tls.as_ref() else {
        if cfg.mode == AuthMode::Mtls {
            bail!("auth.mode = \"mtls\" requires an [auth.tls] section");
        }
        return Ok(None);
    };

    let cert = std::fs::read(&tls.cert)
        .with_context(|| format!("reading auth.tls.cert {}", tls.cert.display()))?;
    let key = std::fs::read(&tls.key)
        .with_context(|| format!("reading auth.tls.key {}", tls.key.display()))?;
    let mut config = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));

    if cfg.mode == AuthMode::Mtls {
        let ca_path = tls
            .client_ca
            .as_ref()
            .ok_or_else(|| anyhow!("auth.mode = \"mtls\" requires auth.tls.client_ca"))?;
        let ca = std::fs::read(ca_path)
            .with_context(|| format!("reading auth.tls.client_ca {}", ca_path.display()))?;
        config = config.client_ca_root(Certificate::from_pem(ca)).client_auth_optional(false);
    }

    Ok(Some(config))
}

// ---------------------------------------------------------------------------
// OIDC
// ---------------------------------------------------------------------------

/// A JWKS entry: the decoding key plus the validation profile derived from the
/// key's algorithm. Both are precomputed so the request path only runs crypto.
struct JwtKey {
    key: DecodingKey,
    validation: Validation,
}

struct OidcVerifier {
    jwks_url: String,
    http: reqwest::Client,
    keys: RwLock<Arc<HashMap<String, JwtKey>>>,
}

#[derive(Deserialize)]
struct OidcClaims {
    sub: Option<String>,
}

#[derive(Deserialize)]
struct Discovery {
    jwks_uri: String,
}

impl OidcVerifier {
    /// Resolve the JWKS URL, load it once (startup fails if unreachable), then
    /// keep it fresh in the background.
    async fn start(cfg: &OidcConfig) -> anyhow::Result<Arc<OidcVerifier>> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent(concat!("bazel-cache-server/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("building the OIDC HTTP client")?;

        let jwks_url = match &cfg.jwks_url {
            Some(url) => url.clone(),
            None => discover_jwks_url(&http, &cfg.issuer).await?,
        };

        let set = fetch_jwks(&http, &jwks_url).await?;
        let keys = keys_from_jwks(&set, &cfg.issuer, &cfg.audience);
        if keys.is_empty() {
            bail!("JWKS at {jwks_url} contains no usable RS256/RS384/RS512/ES256 keys");
        }
        tracing::info!(url = %jwks_url, keys = keys.len(), "loaded OIDC JWKS");

        let verifier = Arc::new(OidcVerifier {
            jwks_url,
            http,
            keys: RwLock::new(Arc::new(keys)),
        });

        let interval = cfg.refresh_interval.max(Duration::from_secs(10));
        let issuer = cfg.issuer.clone();
        let audience = cfg.audience.clone();
        let weak = Arc::downgrade(&verifier);
        tokio::spawn(refresh_loop(weak, issuer, audience, interval));

        Ok(verifier)
    }

    fn verify(&self, token: &str) -> Result<String, Status> {
        let header = decode_header(token)
            .map_err(|_| Status::unauthenticated("malformed bearer token"))?;

        let keys = {
            let guard = self.keys.read().unwrap_or_else(|e| e.into_inner());
            Arc::clone(&guard)
        };

        // Providers that publish a single key may omit `kid` from their tokens.
        let entry = match &header.kid {
            Some(kid) => keys.get(kid),
            None if keys.len() == 1 => keys.values().next(),
            None => None,
        }
        .ok_or_else(|| Status::unauthenticated("unknown token signing key"))?;

        let data = decode::<OidcClaims>(token, &entry.key, &entry.validation)
            .map_err(|e| Status::unauthenticated(format!("invalid bearer token: {e}")))?;

        match data.claims.sub {
            Some(sub) if !sub.is_empty() => Ok(sub),
            _ => Err(Status::unauthenticated("bearer token has no sub claim")),
        }
    }
}

/// Periodic JWKS refresh. A failed refresh keeps the previously loaded keys:
/// an unreachable IdP must not lock every builder out of the cache.
async fn refresh_loop(
    verifier: Weak<OidcVerifier>,
    issuer: String,
    audience: String,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick completes immediately

    loop {
        ticker.tick().await;
        let Some(verifier) = verifier.upgrade() else { return };

        match fetch_jwks(&verifier.http, &verifier.jwks_url).await {
            Ok(set) => {
                let keys = keys_from_jwks(&set, &issuer, &audience);
                if keys.is_empty() {
                    tracing::warn!(
                        url = %verifier.jwks_url,
                        "JWKS refresh returned no usable keys; keeping previous key set"
                    );
                    continue;
                }
                let count = keys.len();
                *verifier.keys.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(keys);
                tracing::debug!(url = %verifier.jwks_url, keys = count, "refreshed OIDC JWKS");
            }
            Err(e) => tracing::warn!(
                url = %verifier.jwks_url,
                error = %e,
                "JWKS refresh failed; keeping previous key set"
            ),
        }
    }
}

async fn discover_jwks_url(http: &reqwest::Client, issuer: &str) -> anyhow::Result<String> {
    let url = format!("{}/.well-known/openid-configuration", issuer.trim_end_matches('/'));
    let doc: Discovery = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetching OIDC discovery document {url}"))?
        .error_for_status()
        .with_context(|| format!("fetching OIDC discovery document {url}"))?
        .json()
        .await
        .with_context(|| format!("parsing OIDC discovery document {url}"))?;
    Ok(doc.jwks_uri)
}

async fn fetch_jwks(http: &reqwest::Client, url: &str) -> anyhow::Result<JwkSet> {
    http.get(url)
        .send()
        .await
        .with_context(|| format!("fetching JWKS {url}"))?
        .error_for_status()
        .with_context(|| format!("fetching JWKS {url}"))?
        .json()
        .await
        .with_context(|| format!("parsing JWKS {url}"))
}

/// Convert a JWKS into usable decoding keys, skipping anything we do not accept.
fn keys_from_jwks(set: &JwkSet, issuer: &str, audience: &str) -> HashMap<String, JwtKey> {
    let mut out = HashMap::with_capacity(set.keys.len());
    for jwk in &set.keys {
        let Some(alg) = jwk_algorithm(jwk) else {
            tracing::debug!(kid = ?jwk.common.key_id, "skipping JWK with unsupported algorithm");
            continue;
        };
        let key = match DecodingKey::from_jwk(jwk) {
            Ok(key) => key,
            Err(e) => {
                tracing::warn!(kid = ?jwk.common.key_id, error = %e, "skipping unusable JWK");
                continue;
            }
        };
        let kid = jwk.common.key_id.clone().unwrap_or_default();
        out.insert(kid, JwtKey { key, validation: validation_for(alg, issuer, audience) });
    }
    out
}

/// Accepted signature algorithms. `alg` is advisory in a JWK, so fall back to
/// the key type when it is absent.
fn jwk_algorithm(jwk: &Jwk) -> Option<Algorithm> {
    match jwk.common.key_algorithm {
        Some(KeyAlgorithm::RS256) => return Some(Algorithm::RS256),
        Some(KeyAlgorithm::RS384) => return Some(Algorithm::RS384),
        Some(KeyAlgorithm::RS512) => return Some(Algorithm::RS512),
        Some(KeyAlgorithm::ES256) => return Some(Algorithm::ES256),
        Some(_) => return None,
        None => {}
    }
    match &jwk.algorithm {
        AlgorithmParameters::RSA(_) => Some(Algorithm::RS256),
        AlgorithmParameters::EllipticCurve(ec) if ec.curve == EllipticCurve::P256 => {
            Some(Algorithm::ES256)
        }
        _ => None,
    }
}

fn validation_for(alg: Algorithm, issuer: &str, audience: &str) -> Validation {
    let mut validation = Validation::new(alg);
    validation.leeway = 60;
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.validate_aud = true;
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation.required_spec_claims =
        ["exp", "iss", "aud"].into_iter().map(str::to_string).collect::<HashSet<String>>();
    validation
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TlsConfig, TokenConfig};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn temp_path(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("bazel-cache-auth-{}-{}-{}", std::process::id(), tag, n))
    }

    fn req_with_auth(value: &str) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut().insert("authorization", value.parse().unwrap());
        req
    }

    fn token_cfg(cfg: TokenConfig) -> AuthConfig {
        AuthConfig { mode: AuthMode::Token, token: Some(cfg), tls: None, oidc: None }
    }

    // --- none -------------------------------------------------------------

    #[tokio::test]
    async fn none_mode_accepts_every_request() {
        let auth = Authenticator::new(&AuthConfig::default()).await.unwrap();
        assert_eq!(auth.mode(), AuthMode::None);
        assert!(!auth.requires_tls());

        let p = auth.authenticate(&Request::new(())).unwrap();
        assert_eq!(p.id, "anonymous");
        assert_eq!(p.mode, "none");

        // Garbage credentials are ignored rather than rejected.
        let p = auth.authenticate(&req_with_auth("Bearer nonsense")).unwrap();
        assert_eq!(p.id, "anonymous");
    }

    #[tokio::test]
    async fn interceptor_inserts_principal_into_extensions() {
        let mut auth = Authenticator::new(&AuthConfig::default()).await.unwrap();
        let req = Interceptor::call(&mut auth, Request::new(())).unwrap();
        let p = req.extensions().get::<Principal>().expect("principal in extensions");
        assert_eq!(p.id, "anonymous");
    }

    // --- token ------------------------------------------------------------

    #[tokio::test]
    async fn token_mode_accepts_only_the_exact_secret() {
        let cfg = token_cfg(TokenConfig {
            value: Some("s3cret-token".into()),
            file: None,
            env: None,
        });
        let auth = Authenticator::new(&cfg).await.unwrap();

        let p = auth.authenticate(&req_with_auth("Bearer s3cret-token")).unwrap();
        assert_eq!(p.id, "token");
        assert_eq!(p.mode, "token");

        // Scheme comparison is case-insensitive.
        assert!(auth.authenticate(&req_with_auth("bearer s3cret-token")).is_ok());
        assert!(auth.authenticate(&req_with_auth("BEARER s3cret-token")).is_ok());

        for bad in [
            "Bearer wrong-token",
            "Bearer s3cret-toke",   // prefix
            "Bearer s3cret-tokenX", // extension
            "Bearer S3CRET-TOKEN",  // token itself is case sensitive
        ] {
            let err = auth.authenticate(&req_with_auth(bad)).unwrap_err();
            assert_eq!(err.code(), tonic::Code::Unauthenticated, "{bad}");
        }
    }

    #[tokio::test]
    async fn token_mode_rejects_missing_and_malformed_headers() {
        let cfg =
            token_cfg(TokenConfig { value: Some("abc".into()), file: None, env: None });
        let auth = Authenticator::new(&cfg).await.unwrap();

        let err = auth.authenticate(&Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("missing"), "{}", err.message());

        for bad in ["abc", "Basic abc", "Bearer", "Bearer ", "Bearerabc", "  Bearer abc"] {
            let err = auth.authenticate(&req_with_auth(bad)).unwrap_err();
            assert_eq!(err.code(), tonic::Code::Unauthenticated, "{bad}");
            assert!(err.message().contains("malformed"), "{bad}: {}", err.message());
        }
    }

    #[test]
    fn bearer_parsing_tolerates_extra_scheme_spacing() {
        assert_eq!(parse_bearer("Bearer   abc"), Some("abc"));
        assert_eq!(parse_bearer("Bearer a b"), Some("a b"));
        assert_eq!(parse_bearer("bEaReR x"), Some("x"));
        assert_eq!(parse_bearer("Bearer"), None);
        assert_eq!(parse_bearer(""), None);
    }

    #[tokio::test]
    async fn token_precedence_is_value_then_env_then_file() {
        let path = temp_path("token");
        std::fs::write(&path, "from-file\n").unwrap();
        // SAFETY: single-threaded setup inside this test; the variable name is
        // unique to this test so no sibling test observes it.
        unsafe { std::env::set_var("BAZEL_CACHE_TEST_TOKEN_PRECEDENCE", "from-env") };

        let all = TokenConfig {
            value: Some("from-value".into()),
            file: Some(path.clone()),
            env: Some("BAZEL_CACHE_TEST_TOKEN_PRECEDENCE".into()),
        };
        assert_eq!(resolve_token(Some(&all)).unwrap(), "from-value");

        let env_and_file = TokenConfig { value: None, ..all.clone() };
        assert_eq!(resolve_token(Some(&env_and_file)).unwrap(), "from-env");

        let file_only = TokenConfig { value: None, env: None, file: Some(path.clone()) };
        // Trailing newline is trimmed.
        assert_eq!(resolve_token(Some(&file_only)).unwrap(), "from-file");

        unsafe { std::env::remove_var("BAZEL_CACHE_TEST_TOKEN_PRECEDENCE") };
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_or_absent_token_sources_fail_startup() {
        assert!(resolve_token(None).is_err());
        assert!(resolve_token(Some(&TokenConfig::default())).is_err());
        assert!(
            resolve_token(Some(&TokenConfig {
                value: Some("   \n".into()),
                file: None,
                env: None
            }))
            .is_err()
        );

        let path = temp_path("empty-token");
        std::fs::write(&path, "\n\n").unwrap();
        assert!(
            resolve_token(Some(&TokenConfig {
                value: None,
                env: None,
                file: Some(path.clone())
            }))
            .is_err()
        );
        std::fs::remove_file(&path).ok();

        assert!(
            resolve_token(Some(&TokenConfig {
                value: None,
                file: None,
                env: Some("BAZEL_CACHE_TEST_DEFINITELY_UNSET".into())
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn token_mode_without_section_fails_startup() {
        let cfg = AuthConfig { mode: AuthMode::Token, token: None, tls: None, oidc: None };
        assert!(Authenticator::new(&cfg).await.is_err());
    }

    // --- constant time ----------------------------------------------------

    #[test]
    fn constant_time_eq_matches_equality() {
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"ab", b"abc"));
        assert!(!constant_time_eq(b"", b"a"));
        // A zero byte must not be confused with "past the end" padding.
        assert!(!constant_time_eq(b"a", b"a\0"));
        assert!(!constant_time_eq(b"a\0", b"a"));
    }

    #[test]
    fn constant_time_eq_never_exits_early() {
        // Mismatch on the very first byte still scans the whole buffer.
        let (diff, examined) = ct_scan(b"Xbcdefgh", b"abcdefgh");
        assert_ne!(diff, 0);
        assert_eq!(examined, 8);

        // A length mismatch scans max(len), not min(len), and still reports a
        // difference even when the shorter input is a proper prefix.
        let (diff, examined) = ct_scan(b"abc", b"abcdefghij");
        assert_ne!(diff, 0);
        assert_eq!(examined, 10);

        let (diff, examined) = ct_scan(b"abcdefghij", b"abc");
        assert_ne!(diff, 0);
        assert_eq!(examined, 10);

        // Equal inputs: full scan, zero accumulator.
        let (diff, examined) = ct_scan(b"abcdefghij", b"abcdefghij");
        assert_eq!(diff, 0);
        assert_eq!(examined, 10);
    }

    // --- mTLS -------------------------------------------------------------

    /// Self-signed P-256 certificate, `CN=builder-01, O=Yume`,
    /// SAN `DNS:builder-01.lan, DNS:alt.example`. Generated with
    /// `openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1`.
    const CERT_CN: &str = concat!(
        "308201c53082016ba00302010202147eede7deb9334be86948152654b1ec366726e802300a06082a8648ce3d04030",
        "2",
        "30243113301106035504030c0a6275696c6465722d3031310d300b060355040a0c0459756d65301e170d323631303",
        "03631",
        "30333635355a170d3336313030333130333635355a30243113301106035504030c0a6275696c6465722d3031310d3",
        "00b",
        "060355040a0c0459756d653059301306072a8648ce3d020106082a8648ce3d030107034200048e459f7c33e73651b",
        "213",
        "856250aaee1c1ae932189de4aef3bea3c281671ff5544d95b8a7acdebbe54868dbe278b3f288a1d54e3620d495bcd",
        "8c2",
        "d30c7d88b7bea37b3079301d0603551d0e04160414698960c43f9ff66223c6317758f65aad25e958d9301f0603551",
        "d23",
        "041830168014698960c43f9ff66223c6317758f65aad25e958d9300f0603551d130101ff040530030101ff3026060",
        "355",
        "1d11041f301d820e6275696c6465722d30312e6c616e820b616c742e6578616d706c65300a06082a8648ce3d04030",
        "203",
        "480030450220488f89c95e13d34577cab4710b04a519884db17f029e11281ad428d1db74029e022100d914a12728a",
        "f14",
        "5f8b0ebaa337e3bb3d12bd5ceac5258d01bfa1bd2e9414f059",
    );

    /// Same recipe, subject `O=Yume, OU=Builders` (no CN),
    /// SAN `DNS:sanonly.example.com`.
    const CERT_SAN_ONLY: &str = concat!(
        "308201b93082015fa0030201020214289512c5d4edd50d8584b7f214f88a52121f88a5300a06082a8648ce3d04030",
        "2",
        "3022310d300b060355040a0c0459756d653111300f060355040b0c084275696c64657273301e170d3236313030363",
        "13033",
        "3635355a170d3336313030333130333635355a3022310d300b060355040a0c0459756d653111300f060355040b0c0",
        "842",
        "75696c646572733059301306072a8648ce3d020106082a8648ce3d030107034200045fbb59481cee581e59901399e",
        "b41",
        "276d21c5efedac90b47a398371e72c5bbc2ecd15f445a1ba1d6aa9555d18ab278edd37b9e1447b4d2d00870213344",
        "27b",
        "d568a3733071301d0603551d0e04160414937cd87708084c3778bddcd963ad2c3df0f9c0cf301f0603551d2304183",
        "016",
        "8014937cd87708084c3778bddcd963ad2c3df0f9c0cf300f0603551d130101ff040530030101ff301e0603551d110",
        "417",
        "3015821373616e6f6e6c792e6578616d706c652e636f6d300a06082a8648ce3d0403020348003045022100b7f6e4b",
        "f8d",
        "dc212dcdca63de5dd27e06cdc7a95376e8cfa041fbffce439ba836022047b3eb51adb889126d9ebac6e84a3ef21b6",
        "fcf",
        "23d1ff4f7dffed29538dbac24a",
    );

    /// Same recipe, subject `O=Yume` only, no SAN extension.
    /// Serial `7e0b26d7c84844af6664ffe84d0f04551b661749`.
    const CERT_SERIAL_ONLY: &str = concat!(
        "3082017430820119a00302010202147e0b26d7c84844af6664ffe84d0f04551b661749300a06082a8648ce3d04030",
        "2",
        "300f310d300b060355040a0c0459756d65301e170d3236313030363130333635365a170d33363130303331303336",
        "35365a",
        "300f310d300b060355040a0c0459756d653059301306072a8648ce3d020106082a8648ce3d03010703420004b5750",
        "0d7",
        "3b0abadd684b5c8040590798b6d98d8877d292a597539390cad64cdce1e2c19796e5a847b4dc9e8c17809d5269853",
        "f28",
        "1dd8d7745fb14c4e43573b03a3533051301d0603551d0e041604147a25f9dfd7a4c635fe27403162eb8cf6e19b667",
        "830",
        "1f0603551d230418301680147a25f9dfd7a4c635fe27403162eb8cf6e19b6678300f0603551d130101ff040530030",
        "101",
        "ff300a06082a8648ce3d040302034900304602210098f496deebc5228c58a2425e8ac2ec311677e39974faaed8567",
        "c48",
        "388c9bb287022100aa16650966a88b2eb27b6faf443782b3b984ce9b7fb64f19b8549d84c6515364",
    );

    #[test]
    fn mtls_principal_prefers_cn_then_san_then_serial() {
        let der = hex::decode(CERT_CN).unwrap();
        assert_eq!(principal_id_from_cert(&der).unwrap(), "builder-01");

        let der = hex::decode(CERT_SAN_ONLY).unwrap();
        assert_eq!(principal_id_from_cert(&der).unwrap(), "sanonly.example.com");

        let der = hex::decode(CERT_SERIAL_ONLY).unwrap();
        assert_eq!(
            principal_id_from_cert(&der).unwrap(),
            "7e0b26d7c84844af6664ffe84d0f04551b661749"
        );
    }

    #[test]
    fn mtls_principal_rejects_garbage_certificates() {
        let err = principal_id_from_cert(b"not a certificate").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn mtls_requires_a_peer_certificate() {
        let path = temp_path("mtls-pem");
        std::fs::write(&path, "placeholder").unwrap();
        let cfg = AuthConfig {
            mode: AuthMode::Mtls,
            token: None,
            tls: Some(TlsConfig {
                cert: path.clone(),
                key: path.clone(),
                client_ca: Some(path.clone()),
            }),
            oidc: None,
        };
        let auth = Authenticator::new(&cfg).await.unwrap();
        assert!(auth.requires_tls());

        // A plain request carries no TLS connect info.
        let err = auth.authenticate(&Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert_eq!(err.message(), "client certificate required");

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn mtls_without_client_ca_fails_startup() {
        let path = temp_path("mtls-noca");
        std::fs::write(&path, "placeholder").unwrap();
        let cfg = AuthConfig {
            mode: AuthMode::Mtls,
            token: None,
            tls: Some(TlsConfig { cert: path.clone(), key: path.clone(), client_ca: None }),
            oidc: None,
        };
        assert!(Authenticator::new(&cfg).await.is_err());
        assert!(server_tls_config(&cfg).is_err());
        std::fs::remove_file(&path).ok();
    }

    // --- server_tls_config -----------------------------------------------

    #[test]
    fn tls_config_is_absent_without_a_tls_section() {
        assert!(server_tls_config(&AuthConfig::default()).unwrap().is_none());

        let token_only =
            token_cfg(TokenConfig { value: Some("x".into()), file: None, env: None });
        assert!(server_tls_config(&token_only).unwrap().is_none());

        let mtls_no_tls = AuthConfig { mode: AuthMode::Mtls, token: None, tls: None, oidc: None };
        assert!(server_tls_config(&mtls_no_tls).is_err());
    }

    #[test]
    fn tls_config_is_built_for_tls_and_mtls_modes() {
        let path = temp_path("tls-pem");
        std::fs::write(&path, "placeholder-pem").unwrap();
        let tls = TlsConfig {
            cert: path.clone(),
            key: path.clone(),
            client_ca: Some(path.clone()),
        };

        // Token mode + TLS: server TLS only, the client CA is ignored.
        let cfg = AuthConfig {
            mode: AuthMode::Token,
            token: Some(TokenConfig { value: Some("x".into()), file: None, env: None }),
            tls: Some(tls.clone()),
            oidc: None,
        };
        assert!(server_tls_config(&cfg).unwrap().is_some());

        // mTLS mode: identity plus mandatory client CA.
        let cfg = AuthConfig { mode: AuthMode::Mtls, token: None, tls: Some(tls), oidc: None };
        assert!(server_tls_config(&cfg).unwrap().is_some());

        std::fs::remove_file(&path).ok();

        // A missing PEM file is a startup error, not a silent plaintext server.
        let cfg = AuthConfig {
            mode: AuthMode::Mtls,
            token: None,
            tls: Some(TlsConfig {
                cert: temp_path("missing"),
                key: temp_path("missing"),
                client_ca: None,
            }),
            oidc: None,
        };
        assert!(server_tls_config(&cfg).is_err());
    }

    // --- OIDC -------------------------------------------------------------

    /// P-256 signing key for the OIDC tests (generated with openssl, test-only).
    const JWT_PRIVATE_PEM: &str = concat!(
        "-----BEGIN PRIVATE KEY-----\n",
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgzWrNiq/UyU5S9FTl\n",
        "nyaYpk4N2cFvCOZyvg8YL9xWACehRANCAARkcFRmaxtiyfM+bUQE2OO2RfzESzfm\n",
        "xnfJCtkauAX5T4kyg+8zDL+BR7N7YgVacAvNNq6eKbMjFOKOhfcKd+4k\n",
        "-----END PRIVATE KEY-----\n",
    );
    const JWT_JWK_X: &str = "ZHBUZmsbYsnzPm1EBNjjtkX8xEs35sZ3yQrZGrgF-U8";
    const JWT_JWK_Y: &str = "iTKD7zMMv4FHs3tiBVpwC802rp4psyMU4o6F9wp37iQ";

    const ISSUER: &str = "https://idp.example.com";
    const AUDIENCE: &str = "bazel-cache";

    #[derive(Serialize)]
    struct TestClaims {
        iss: String,
        aud: String,
        sub: String,
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<u64>,
    }

    fn jwks_json(kid: &str, alg: &str) -> String {
        format!(
            r#"{{"keys":[{{"kty":"EC","crv":"P-256","alg":"{alg}","use":"sig","kid":"{kid}","x":"{JWT_JWK_X}","y":"{JWT_JWK_Y}"}}]}}"#
        )
    }

    /// The real server installs the rustls provider in `main`; tests must do it
    /// themselves before any `reqwest::Client` is built.
    fn test_http_client() -> reqwest::Client {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        reqwest::Client::builder().build().unwrap()
    }

    fn verifier_with(kid: &str, alg: &str) -> OidcVerifier {
        let set: JwkSet = serde_json::from_str(&jwks_json(kid, alg)).unwrap();
        let keys = keys_from_jwks(&set, ISSUER, AUDIENCE);
        assert_eq!(keys.len(), 1, "JWKS should yield exactly one usable key");
        OidcVerifier {
            jwks_url: "https://idp.example.com/jwks".into(),
            http: test_http_client(),
            keys: RwLock::new(Arc::new(keys)),
        }
    }

    fn now() -> u64 {
        jsonwebtoken::get_current_timestamp()
    }

    fn sign(kid: Option<&str>, claims: &TestClaims) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = kid.map(str::to_string);
        let key = EncodingKey::from_ec_pem(JWT_PRIVATE_PEM.as_bytes()).unwrap();
        encode(&header, claims, &key).unwrap()
    }

    fn valid_claims() -> TestClaims {
        TestClaims {
            iss: ISSUER.into(),
            aud: AUDIENCE.into(),
            sub: "builder@example.com".into(),
            exp: now() + 600,
            nbf: None,
        }
    }

    #[test]
    fn oidc_accepts_a_well_formed_token() {
        let v = verifier_with("k1", "ES256");
        let token = sign(Some("k1"), &valid_claims());
        assert_eq!(v.verify(&token).unwrap(), "builder@example.com");
    }

    #[test]
    fn oidc_rejects_bad_issuer_audience_and_expiry() {
        let v = verifier_with("k1", "ES256");

        let wrong_iss = TestClaims { iss: "https://evil.example".into(), ..valid_claims() };
        assert!(v.verify(&sign(Some("k1"), &wrong_iss)).is_err());

        let wrong_aud = TestClaims { aud: "some-other-service".into(), ..valid_claims() };
        assert!(v.verify(&sign(Some("k1"), &wrong_aud)).is_err());

        // Expired well beyond the 60s leeway.
        let expired = TestClaims { exp: now() - 3600, ..valid_claims() };
        assert!(v.verify(&sign(Some("k1"), &expired)).is_err());

        // Inside the 60s leeway: still accepted.
        let just_expired = TestClaims { exp: now() - 10, ..valid_claims() };
        assert!(v.verify(&sign(Some("k1"), &just_expired)).is_ok());

        // Not yet valid.
        let not_yet = TestClaims { nbf: Some(now() + 3600), ..valid_claims() };
        assert!(v.verify(&sign(Some("k1"), &not_yet)).is_err());
    }

    #[test]
    fn oidc_rejects_unknown_kid_and_tampered_signatures() {
        let v = verifier_with("k1", "ES256");

        let err = v.verify(&sign(Some("k2"), &valid_claims())).unwrap_err();
        assert_eq!(err.message(), "unknown token signing key");

        // Flipping a payload byte invalidates the signature.
        let token = sign(Some("k1"), &valid_claims());
        let mut parts: Vec<&str> = token.split('.').collect();
        let payload = parts[1].to_string();
        let mut bytes = payload.into_bytes();
        bytes[0] = if bytes[0] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(bytes).unwrap();
        parts[1] = &tampered;
        assert!(v.verify(&parts.join(".")).is_err());

        assert!(v.verify("not-a-jwt").is_err());
    }

    #[test]
    fn oidc_rejects_tokens_without_a_subject() {
        let v = verifier_with("k1", "ES256");
        // `sub` is not part of jsonwebtoken's required claims here, so an empty
        // subject must be caught by our own check.
        let claims = TestClaims { sub: String::new(), ..valid_claims() };
        let err = v.verify(&sign(Some("k1"), &claims)).unwrap_err();
        assert_eq!(err.message(), "bearer token has no sub claim");
    }

    #[test]
    fn oidc_accepts_a_kid_less_token_only_with_a_single_key() {
        let v = verifier_with("k1", "ES256");
        assert_eq!(v.verify(&sign(None, &valid_claims())).unwrap(), "builder@example.com");

        // Two keys: an unlabelled token is ambiguous and must be rejected.
        let two = format!(
            r#"{{"keys":[
                {{"kty":"EC","crv":"P-256","alg":"ES256","kid":"k1","x":"{JWT_JWK_X}","y":"{JWT_JWK_Y}"}},
                {{"kty":"EC","crv":"P-256","alg":"ES256","kid":"k2","x":"{JWT_JWK_X}","y":"{JWT_JWK_Y}"}}
            ]}}"#
        );
        let set: JwkSet = serde_json::from_str(&two).unwrap();
        let keys = keys_from_jwks(&set, ISSUER, AUDIENCE);
        assert_eq!(keys.len(), 2);
        let v = OidcVerifier {
            jwks_url: String::new(),
            http: test_http_client(),
            keys: RwLock::new(Arc::new(keys)),
        };
        assert!(v.verify(&sign(None, &valid_claims())).is_err());
    }

    #[test]
    fn jwks_conversion_skips_unsupported_algorithms_and_infers_missing_alg() {
        // HS256 keys must never be accepted: they would let any holder of the
        // public key mint tokens.
        let hs = r#"{"keys":[{"kty":"oct","alg":"HS256","kid":"h","k":"c2VjcmV0"}]}"#;
        let set: JwkSet = serde_json::from_str(hs).unwrap();
        assert!(keys_from_jwks(&set, ISSUER, AUDIENCE).is_empty());

        // P-521 is not in the accepted set.
        let p521 = r#"{"keys":[{"kty":"EC","crv":"P-521","kid":"e","x":"AA","y":"AA"}]}"#;
        let set: JwkSet = serde_json::from_str(p521).unwrap();
        assert!(keys_from_jwks(&set, ISSUER, AUDIENCE).is_empty());

        // No `alg`: inferred from the key type, and the token still verifies.
        let set: JwkSet = serde_json::from_str(&format!(
            r#"{{"keys":[{{"kty":"EC","crv":"P-256","kid":"k1","x":"{JWT_JWK_X}","y":"{JWT_JWK_Y}"}}]}}"#
        ))
        .unwrap();
        let keys = keys_from_jwks(&set, ISSUER, AUDIENCE);
        assert_eq!(keys.len(), 1);
        let v = OidcVerifier {
            jwks_url: String::new(),
            http: test_http_client(),
            keys: RwLock::new(Arc::new(keys)),
        };
        assert_eq!(v.verify(&sign(Some("k1"), &valid_claims())).unwrap(), "builder@example.com");
    }

    #[test]
    fn validation_profile_requires_registered_claims() {
        let v = validation_for(Algorithm::RS256, ISSUER, AUDIENCE);
        assert_eq!(v.leeway, 60);
        assert!(v.validate_exp && v.validate_aud && v.validate_nbf);
        assert!(v.required_spec_claims.contains("exp"));
        assert!(v.required_spec_claims.contains("iss"));
        assert!(v.required_spec_claims.contains("aud"));
        assert_eq!(v.algorithms, vec![Algorithm::RS256]);
    }

    #[tokio::test]
    async fn oidc_without_section_fails_startup() {
        let cfg = AuthConfig { mode: AuthMode::Oidc, token: None, tls: None, oidc: None };
        assert!(Authenticator::new(&cfg).await.is_err());
    }
}
