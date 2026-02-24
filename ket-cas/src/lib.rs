//! Content-Addressable Store using BLAKE3 hashing.
//!
//! Flat file store: `.ket/cas/<blake3_hex>` = raw bytes.
//! Atomic writes (write to `.tmp`, rename).
//! Dedup on put (skip if CID already exists).

use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum CasError {
    #[error("CAS directory not initialized: {0}")]
    NotInitialized(PathBuf),
    #[error("Content not found: {0}")]
    NotFound(String),
    #[error("Integrity check failed for {cid}: expected content hash doesn't match")]
    IntegrityError { cid: String },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// A content identifier — BLAKE3-256 hash as 64-char hex string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Cid(pub String);

impl Cid {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Cid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Cid {
    fn from(s: String) -> Self {
        Cid(s)
    }
}

impl From<&str> for Cid {
    fn from(s: &str) -> Self {
        Cid(s.to_string())
    }
}

/// Hash raw bytes, return CID.
pub fn hash_bytes(data: &[u8]) -> Cid {
    Cid(blake3::hash(data).to_hex().to_string())
}

/// Hash a file by streaming, return CID.
pub fn hash_file(path: &Path) -> Result<Cid, CasError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(Cid(hasher.finalize().to_hex().to_string()))
}

/// The CAS store backed by flat files.
pub struct Store {
    root: PathBuf,
}

impl Store {
    /// Open a CAS store at the given directory (e.g., `.ket/cas/`).
    pub fn open(root: PathBuf) -> Result<Self, CasError> {
        if !root.exists() {
            return Err(CasError::NotInitialized(root));
        }
        Ok(Store { root })
    }

    /// Create a new CAS store directory.
    pub fn init(root: &Path) -> Result<Self, CasError> {
        fs::create_dir_all(root)?;
        Ok(Store {
            root: root.to_path_buf(),
        })
    }

    /// Store content, return its CID. Deduplicates: skips write if CID exists.
    pub fn put(&self, data: &[u8]) -> Result<Cid, CasError> {
        let cid = hash_bytes(data);
        let target = self.blob_path(&cid);

        if target.exists() {
            return Ok(cid); // dedup
        }

        // Atomic write: write to tmp, then rename
        let tmp = self.root.join(format!(".tmp.{}", &cid.0[..16]));
        fs::write(&tmp, data)?;
        fs::rename(&tmp, &target)?;

        Ok(cid)
    }

    /// Store a file by path, return its CID.
    pub fn put_file(&self, path: &Path) -> Result<Cid, CasError> {
        let data = fs::read(path)?;
        self.put(&data)
    }

    /// Retrieve content by CID.
    pub fn get(&self, cid: &Cid) -> Result<Vec<u8>, CasError> {
        let path = self.blob_path(cid);
        if !path.exists() {
            return Err(CasError::NotFound(cid.0.clone()));
        }
        Ok(fs::read(&path)?)
    }

    /// Verify integrity: re-hash stored content, compare to CID.
    pub fn verify(&self, cid: &Cid) -> Result<bool, CasError> {
        let data = self.get(cid)?;
        let actual = hash_bytes(&data);
        Ok(actual == *cid)
    }

    /// Check if a CID exists in the store.
    pub fn exists(&self, cid: &Cid) -> bool {
        self.blob_path(cid).exists()
    }

    /// List all CIDs in the store.
    pub fn list(&self) -> Result<Vec<Cid>, CasError> {
        let mut cids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.len() == 64 && !name.starts_with('.') {
                cids.push(Cid(name));
            }
        }
        cids.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(cids)
    }

    /// Get the byte size of a blob.
    pub fn blob_size(&self, cid: &Cid) -> Result<u64, CasError> {
        let path = self.blob_path(cid);
        if !path.exists() {
            return Err(CasError::NotFound(cid.0.clone()));
        }
        Ok(fs::metadata(&path)?.len())
    }

    /// Delete a blob by CID. Returns true if it existed.
    pub fn delete(&self, cid: &Cid) -> Result<bool, CasError> {
        let path = self.blob_path(cid);
        if path.exists() {
            fs::remove_file(&path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Total size of the CAS store in bytes.
    pub fn total_size(&self) -> Result<u64, CasError> {
        let mut total = 0u64;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.len() == 64 && !name.starts_with('.') {
                total += entry.metadata()?.len();
            }
        }
        Ok(total)
    }

    /// Get the root path of the store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn blob_path(&self, cid: &Cid) -> PathBuf {
        self.root.join(&cid.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn put_get_roundtrip() {
        let dir = std::env::temp_dir().join("ket-cas-test-roundtrip");
        let _ = fs::remove_dir_all(&dir);
        let store = Store::init(&dir).unwrap();

        let data = b"hello world";
        let cid = store.put(data).unwrap();
        assert_eq!(cid.0.len(), 64);

        let retrieved = store.get(&cid).unwrap();
        assert_eq!(retrieved, data);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dedup() {
        let dir = std::env::temp_dir().join("ket-cas-test-dedup");
        let _ = fs::remove_dir_all(&dir);
        let store = Store::init(&dir).unwrap();

        let cid1 = store.put(b"same content").unwrap();
        let cid2 = store.put(b"same content").unwrap();
        assert_eq!(cid1, cid2);

        let entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
            .collect();
        assert_eq!(entries.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn verify_integrity() {
        let dir = std::env::temp_dir().join("ket-cas-test-verify");
        let _ = fs::remove_dir_all(&dir);
        let store = Store::init(&dir).unwrap();

        let cid = store.put(b"verify me").unwrap();
        assert!(store.verify(&cid).unwrap());

        // Corrupt the file
        let path = dir.join(&cid.0);
        fs::write(&path, b"corrupted").unwrap();
        assert!(!store.verify(&cid).unwrap());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn hash_deterministic() {
        let a = hash_bytes(b"test");
        let b = hash_bytes(b"test");
        assert_eq!(a, b);

        let c = hash_bytes(b"different");
        assert_ne!(a, c);
    }

    #[test]
    fn not_found() {
        let dir = std::env::temp_dir().join("ket-cas-test-notfound");
        let _ = fs::remove_dir_all(&dir);
        let store = Store::init(&dir).unwrap();

        let result = store.get(&Cid("0".repeat(64)));
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&dir);
    }
}
