//! SHA-256 content digest, the shared key type for CAS and Action Cache.

use std::fmt;

use sha2::{Digest as _, Sha256};
use thiserror::Error;

pub const HASH_LEN: usize = 32;
pub const HEX_LEN: usize = HASH_LEN * 2;

/// A REAPI digest: raw SHA-256 bytes plus the payload size.
///
/// `size_bytes` is part of the REAPI key for Action Cache entries; CAS blobs are
/// keyed by `hash` alone (content addressed) and `size_bytes` is verified.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest {
    pub hash: [u8; HASH_LEN],
    pub size_bytes: i64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DigestError {
    #[error("digest hash must be {HEX_LEN} lowercase hex characters, got {0} characters")]
    BadLength(usize),
    #[error("digest hash is not valid hex")]
    BadHex,
    #[error("digest size_bytes must be >= 0, got {0}")]
    NegativeSize(i64),
}

impl Digest {
    pub fn new(hash: [u8; HASH_LEN], size_bytes: i64) -> Result<Self, DigestError> {
        if size_bytes < 0 {
            return Err(DigestError::NegativeSize(size_bytes));
        }
        Ok(Self { hash, size_bytes })
    }

    /// Parse a hex hash string plus size, as carried in REAPI messages and
    /// ByteStream resource names.
    pub fn from_hex(hash: &str, size_bytes: i64) -> Result<Self, DigestError> {
        if hash.len() != HEX_LEN {
            return Err(DigestError::BadLength(hash.len()));
        }
        let mut bytes = [0u8; HASH_LEN];
        hex::decode_to_slice(hash.as_bytes(), &mut bytes).map_err(|_| DigestError::BadHex)?;
        Self::new(bytes, size_bytes)
    }

    /// Digest of an in-memory buffer.
    pub fn of(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        Self {
            hash: hasher.finalize().into(),
            size_bytes: data.len() as i64,
        }
    }

    pub fn hex(&self) -> String {
        hex::encode(self.hash)
    }

    /// `ab` — the CAS directory shard for this digest.
    pub fn shard(&self) -> String {
        hex::encode(&self.hash[..1])
    }

    pub fn is_empty(&self) -> bool {
        self.size_bytes == 0
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.hex(), self.size_bytes)
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_malformed_hashes() {
        assert_eq!(Digest::from_hex("abc", 1), Err(DigestError::BadLength(3)));
        assert_eq!(Digest::from_hex(&"z".repeat(64), 1), Err(DigestError::BadHex));
        assert_eq!(
            Digest::from_hex(&"a".repeat(64), -1),
            Err(DigestError::NegativeSize(-1))
        );
    }

    #[test]
    fn hashes_match_known_sha256() {
        let d = Digest::of(b"hello");
        assert_eq!(
            d.hex(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(d.size_bytes, 5);
        assert_eq!(d.shard(), "2c");
        assert_eq!(Digest::from_hex(&d.hex(), 5), Ok(d));
    }

    #[test]
    fn empty_blob_digest() {
        let d = Digest::of(b"");
        assert!(d.is_empty());
        assert_eq!(
            d.hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
