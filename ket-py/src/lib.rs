//! PyO3 bindings for Ket — exposes CAS, DAG, and drift detection to Python.
//!
//! Usage from Python:
//! ```python
//! import ket
//!
//! store = ket.Store("/path/to/.ket/cas")
//! cid = store.put(b"hello world")
//! data = store.get(cid)
//! assert store.verify(cid)
//!
//! dag = ket.Dag("/path/to/.ket/cas")
//! node_cid, content_cid = dag.store("content", "memory", [], "human")
//! lineage = dag.lineage(node_cid)
//! ```

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use std::path::PathBuf;

/// Content-Addressable Store.
#[pyclass]
struct Store {
    inner: ket_cas::Store,
}

#[pymethods]
impl Store {
    /// Open an existing CAS store.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let inner =
            ket_cas::Store::open(PathBuf::from(path)).map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(Store { inner })
    }

    /// Create a new CAS store at the given path.
    #[staticmethod]
    fn init(path: &str) -> PyResult<Self> {
        let inner = ket_cas::Store::init(std::path::Path::new(path))
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(Store { inner })
    }

    /// Store content, return its CID (64-char hex string).
    fn put(&self, data: &[u8]) -> PyResult<String> {
        let cid = self
            .inner
            .put(data)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(cid.0)
    }

    /// Store a file by path, return its CID.
    fn put_file(&self, path: &str) -> PyResult<String> {
        let cid = self
            .inner
            .put_file(std::path::Path::new(path))
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(cid.0)
    }

    /// Retrieve content by CID.
    fn get(&self, cid: &str) -> PyResult<Vec<u8>> {
        self.inner
            .get(&ket_cas::Cid::from(cid))
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Verify integrity of a stored blob.
    fn verify(&self, cid: &str) -> PyResult<bool> {
        self.inner
            .verify(&ket_cas::Cid::from(cid))
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Check if a CID exists.
    fn exists(&self, cid: &str) -> bool {
        self.inner.exists(&ket_cas::Cid::from(cid))
    }

    /// List all CIDs in the store.
    fn list(&self) -> PyResult<Vec<String>> {
        let cids = self
            .inner
            .list()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(cids.into_iter().map(|c| c.0).collect())
    }

    /// Total size of the store in bytes.
    fn total_size(&self) -> PyResult<u64> {
        self.inner
            .total_size()
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }
}

/// Hash bytes and return the BLAKE3 CID without storing.
#[pyfunction]
fn hash_bytes(data: &[u8]) -> String {
    ket_cas::hash_bytes(data).0
}

/// Hash a file and return the BLAKE3 CID without storing.
#[pyfunction]
fn hash_file(path: &str) -> PyResult<String> {
    let cid = ket_cas::hash_file(std::path::Path::new(path))
        .map_err(|e| PyIOError::new_err(e.to_string()))?;
    Ok(cid.0)
}

/// Merkle DAG operations.
#[pyclass]
struct Dag {
    cas: ket_cas::Store,
}

#[pymethods]
impl Dag {
    /// Open a DAG backed by a CAS store.
    #[new]
    fn new(cas_path: &str) -> PyResult<Self> {
        let cas = ket_cas::Store::open(PathBuf::from(cas_path))
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(Dag { cas })
    }

    /// Store content and create a DAG node.
    /// Returns (node_cid, content_cid).
    fn store(
        &self,
        content: &[u8],
        kind: &str,
        parents: Vec<String>,
        agent: &str,
    ) -> PyResult<(String, String)> {
        let node_kind = parse_kind(kind)?;
        let parent_cids: Vec<ket_cas::Cid> = parents.into_iter().map(ket_cas::Cid::from).collect();

        let dag = ket_dag::Dag::new(&self.cas);
        let (node_cid, content_cid) = dag
            .store_with_node(content, node_kind, parent_cids, agent)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Ok((node_cid.0, content_cid.0))
    }

    /// Get a DAG node as a Python dict.
    fn get_node(&self, cid: &str) -> PyResult<PyObject> {
        let dag = ket_dag::Dag::new(&self.cas);
        let node = dag
            .get_node(&ket_cas::Cid::from(cid))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("kind", node.kind.to_string())?;
            dict.set_item("agent", &node.agent)?;
            dict.set_item("timestamp", &node.timestamp)?;
            dict.set_item("output_cid", node.output_cid.as_str())?;
            let parents: Vec<&str> = node.parents.iter().map(|p| p.as_str()).collect();
            dict.set_item("parents", parents)?;
            let meta: Vec<(&str, &str)> = node.meta.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            dict.set_item("meta", meta)?;
            Ok(dict.into())
        })
    }

    /// Trace lineage — returns list of (cid, node_dict) tuples.
    fn lineage(&self, cid: &str) -> PyResult<Vec<(String, PyObject)>> {
        let dag = ket_dag::Dag::new(&self.cas);
        let lineage = dag
            .lineage(&ket_cas::Cid::from(cid))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Python::with_gil(|py| {
            let mut result = Vec::new();
            for (node_cid, node) in lineage {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("kind", node.kind.to_string())?;
                dict.set_item("agent", &node.agent)?;
                dict.set_item("timestamp", &node.timestamp)?;
                dict.set_item("output_cid", node.output_cid.as_str())?;
                let parents: Vec<&str> = node.parents.iter().map(|p| p.as_str()).collect();
                dict.set_item("parents", parents)?;
                result.push((node_cid.0, dict.into()));
            }
            Ok(result)
        })
    }

    /// Check if a file has drifted from a CID.
    fn check_drift(&self, path: &str, expected_cid: &str) -> PyResult<bool> {
        let dag = ket_dag::Dag::new(&self.cas);
        dag.check_drift(std::path::Path::new(path), &ket_cas::Cid::from(expected_cid))
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Export a subgraph as JSON string.
    fn export(&self, root_cid: &str) -> PyResult<String> {
        let dag = ket_dag::Dag::new(&self.cas);
        let bundle = dag
            .export(&ket_cas::Cid::from(root_cid))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        serde_json::to_string(&bundle).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Import a subgraph from JSON string. Returns count of imported blobs.
    fn import_bundle(&self, json_str: &str) -> PyResult<usize> {
        let bundle: ket_dag::DagBundle =
            serde_json::from_str(json_str).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let dag = ket_dag::Dag::new(&self.cas);
        dag.import(&bundle)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

fn parse_kind(s: &str) -> PyResult<ket_dag::NodeKind> {
    match s {
        "memory" => Ok(ket_dag::NodeKind::Memory),
        "code" => Ok(ket_dag::NodeKind::Code),
        "reasoning" => Ok(ket_dag::NodeKind::Reasoning),
        "task" => Ok(ket_dag::NodeKind::Task),
        "cdom" => Ok(ket_dag::NodeKind::Cdom),
        "score" => Ok(ket_dag::NodeKind::Score),
        "context" => Ok(ket_dag::NodeKind::Context),
        _ => Err(PyValueError::new_err(format!("Unknown kind: {s}"))),
    }
}

/// The ket Python module.
#[pymodule]
fn ket(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Store>()?;
    m.add_class::<Dag>()?;
    m.add_function(wrap_pyfunction!(hash_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(hash_file, m)?)?;
    Ok(())
}
