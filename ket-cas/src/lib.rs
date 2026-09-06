//! Content-Addressable Store using BLAKE3 hashing.
//!
//! Flat file store: `.ket/cas/<blake3_hex>` = raw bytes.
//! Atomic writes (write to `.tmp`, rename).
//! Dedup on put (skip if CID already exists).

pub mod log;

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

    /// 64 lowercase hex chars — the only shape a BLAKE3-256 CID can have.
    pub fn is_well_formed(&self) -> bool {
        self.0.len() == 64
            && self
                .0
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
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
    ///
    /// Safe under concurrent writers — processes or threads — putting the
    /// same bytes at once: each writer uses its own temp file, and the rename
    /// is atomic, so the last one to land simply replaces identical bytes.
    /// (Before this, the temp name was derived from the CID alone, so two
    /// concurrent puts of the same content raced on one file and one of
    /// them failed with ENOENT — found by the parallel-review demo.)
    pub fn put(&self, data: &[u8]) -> Result<Cid, CasError> {
        let cid = hash_bytes(data);
        let target = self.blob_path(&cid);

        if target.exists() {
            return Ok(cid); // dedup
        }

        // Atomic write: write to a per-writer tmp, then rename onto the CID.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = self.root.join(format!(
            ".tmp.{}.{}.{}",
            &cid.0[..16],
            std::process::id(),
            seq
        ));
        // Write, fsync, then rename: the log is fsynced on every append, so
        // a blob the log names must be at least as durable as the log line.
        // Any failure before the rename removes the temp file — a partial
        // `.tmp.*` must never outlive the call.
        if let Err(e) = write_synced(&tmp, data) {
            let _ = fs::remove_file(&tmp);
            return Err(e.into());
        }
        if let Err(e) = fs::rename(&tmp, &target) {
            let _ = fs::remove_file(&tmp);
            // A concurrent writer may have landed the same content first;
            // that is a successful dedup, not an error.
            if target.exists() {
                return Ok(cid);
            }
            return Err(e.into());
        }

        Ok(cid)
    }

    /// Store a file by path, return its CID.
    pub fn put_file(&self, path: &Path) -> Result<Cid, CasError> {
        let data = fs::read(path)?;
        self.put(&data)
    }

    /// Retrieve content by CID.
    ///
    /// A malformed CID (empty, non-hex, wrong length) is `NotFound`, never a
    /// filesystem probe: `Cid::from("")` must not read the store directory,
    /// and `Cid::from("../x")` must not escape it.
    pub fn get(&self, cid: &Cid) -> Result<Vec<u8>, CasError> {
        if !cid.is_well_formed() {
            return Err(CasError::NotFound(cid.0.clone()));
        }
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
        cid.is_well_formed() && self.blob_path(cid).is_file()
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

    /// Get the byte size of a blob. Malformed CIDs are `NotFound`, as in `get`.
    pub fn blob_size(&self, cid: &Cid) -> Result<u64, CasError> {
        if !cid.is_well_formed() {
            return Err(CasError::NotFound(cid.0.clone()));
        }
        let path = self.blob_path(cid);
        if !path.exists() {
            return Err(CasError::NotFound(cid.0.clone()));
        }
        Ok(fs::metadata(&path)?.len())
    }

    /// Delete a blob by CID. Returns true if it existed. A malformed CID
    /// never names a file to remove (`Cid::from("../x")` must not escape).
    pub fn delete(&self, cid: &Cid) -> Result<bool, CasError> {
        if !cid.is_well_formed() {
            return Ok(false);
        }
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

/// Write `data` to `path` and flush it to disk before returning.
fn write_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = fs::File::create(path)?;
    f.write_all(data)?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn concurrent_puts_of_same_content_all_succeed() {
        let dir = std::env::temp_dir().join("ket-cas-test-concurrent");
        let _ = fs::remove_dir_all(&dir);
        let store = std::sync::Arc::new(Store::init(&dir).unwrap());
        let data = b"the same finding, written by eight reviewers at once".to_vec();

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                let data = data.clone();
                std::thread::spawn(move || store.put(&data))
            })
            .collect();
        let cids: Vec<Cid> = handles
            .into_iter()
            .map(|h| h.join().unwrap().expect("every concurrent put succeeds"))
            .collect();

        assert!(cids.iter().all(|c| *c == cids[0]), "one content, one CID");
        assert!(store.verify(&cids[0]).unwrap());
        let leftovers: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with(".tmp."))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp files left behind: {leftovers:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_cid_never_touches_the_filesystem_on_delete_or_size() {
        let dir = std::env::temp_dir().join("ket-cas-test-traversal");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let outside = dir.join("outside.txt");
        fs::write(&outside, b"keep me").unwrap();
        let store = Store::init(&dir.join("cas")).unwrap();
        let escape = Cid::from("../outside.txt");
        assert!(
            !store.delete(&escape).unwrap(),
            "malformed CID deletes nothing"
        );
        assert!(
            outside.exists(),
            "a traversal CID must not remove files outside the store"
        );
        assert!(matches!(
            store.blob_size(&escape),
            Err(CasError::NotFound(_))
        ));
        assert!(matches!(
            store.blob_size(&Cid::from("")),
            Err(CasError::NotFound(_))
        ));
    }

    #[test]
    #[cfg(unix)]
    fn failed_put_leaves_no_temp_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join("ket-cas-test-failed-put");
        let _ = fs::remove_dir_all(&dir);
        let store = Store::init(&dir.join("cas")).unwrap();
        // Root can't take a new file (0o555); root user bypasses this, so skip there.
        if unsafe { libc_geteuid() } == 0 {
            return;
        }
        std::fs::set_permissions(store.root(), std::fs::Permissions::from_mode(0o555)).unwrap();
        let res = store.put(b"never lands");
        std::fs::set_permissions(store.root(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(res.is_err(), "put into an unwritable root must fail");
        let leftovers: Vec<_> = std::fs::read_dir(store.root())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".tmp."))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no .tmp.* after a failed put: {leftovers:?}"
        );
    }

    #[cfg(unix)]
    unsafe fn libc_geteuid() -> u32 {
        extern "C" {
            fn geteuid() -> u32;
        }
        geteuid()
    }

    #[test]
    fn malformed_cid_is_not_found_not_a_filesystem_probe() {
        let dir = std::env::temp_dir().join("ket-cas-test-malformed");
        let _ = fs::remove_dir_all(&dir);
        let store = Store::init(&dir).unwrap();
        for bad in ["", "abc", "../../etc/passwd", &"Z".repeat(64)] {
            assert!(
                matches!(store.get(&Cid::from(bad)), Err(CasError::NotFound(_))),
                "{bad:?}"
            );
            assert!(!store.exists(&Cid::from(bad)));
        }
        let _ = fs::remove_dir_all(&dir);
    }

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
