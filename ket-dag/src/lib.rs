//! Merkle DAG layer built on top of ket-cas.
//!
//! Each DagNode is serialized to JSON, stored in CAS, and addressable by CID.
//! Parents form the DAG edges. Cycles use soft links (stored separately).

use ket_cas::{Cid, Store as CasStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("CAS error: {0}")]
    Cas(#[from] ket_cas::CasError),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Node not found: {0}")]
    NotFound(String),
}

/// The kind of artifact a DAG node represents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    Memory,
    Code,
    Reasoning,
    Task,
    Cdom,
    Score,
    Context,
}

impl std::fmt::Display for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeKind::Memory => write!(f, "memory"),
            NodeKind::Code => write!(f, "code"),
            NodeKind::Reasoning => write!(f, "reasoning"),
            NodeKind::Task => write!(f, "task"),
            NodeKind::Cdom => write!(f, "cdom"),
            NodeKind::Score => write!(f, "score"),
            NodeKind::Context => write!(f, "context"),
        }
    }
}

/// A node in the Merkle DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNode {
    /// What kind of artifact this represents.
    pub kind: NodeKind,
    /// Parent node CIDs (what this derived from).
    pub parents: Vec<Cid>,
    /// CID of the produced artifact content.
    pub output_cid: Cid,
    /// Which agent produced this (claude, codex, copilot, evermemos, human).
    pub agent: String,
    /// ISO 8601 timestamp.
    pub timestamp: String,
    /// Flat key-value metadata.
    pub meta: Vec<(String, String)>,
}

impl DagNode {
    /// Create a new DagNode with the current timestamp.
    pub fn new(kind: NodeKind, parents: Vec<Cid>, output_cid: Cid, agent: &str) -> Self {
        DagNode {
            kind,
            parents,
            output_cid,
            agent: agent.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            meta: Vec::new(),
        }
    }

    /// Add a metadata key-value pair.
    pub fn with_meta(mut self, key: &str, value: &str) -> Self {
        self.meta.push((key.to_string(), value.to_string()));
        self
    }

    /// Serialize this node to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, DagError> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Deserialize a node from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, DagError> {
        Ok(serde_json::from_slice(data)?)
    }

    /// Get a metadata value by key.
    pub fn get_meta(&self, key: &str) -> Option<&str> {
        self.meta
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// A soft link — represents a relationship that would create a cycle in the DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoftLink {
    pub from_cid: Cid,
    pub to_cid: Cid,
    pub relation: String,
    pub created_at: String,
}

/// DAG operations backed by CAS.
pub struct Dag<'a> {
    cas: &'a CasStore,
}

impl<'a> Dag<'a> {
    pub fn new(cas: &'a CasStore) -> Self {
        Dag { cas }
    }

    /// Store a DagNode in CAS. Returns the CID of the serialized node.
    pub fn put_node(&self, node: &DagNode) -> Result<Cid, DagError> {
        let bytes = node.to_bytes()?;
        Ok(self.cas.put(&bytes)?)
    }

    /// Retrieve a DagNode by its CID.
    pub fn get_node(&self, cid: &Cid) -> Result<DagNode, DagError> {
        let bytes = self.cas.get(cid)?;
        DagNode::from_bytes(&bytes)
    }

    /// Store content and create a DagNode pointing to it.
    /// Returns (node_cid, content_cid).
    pub fn store_with_node(
        &self,
        content: &[u8],
        kind: NodeKind,
        parents: Vec<Cid>,
        agent: &str,
    ) -> Result<(Cid, Cid), DagError> {
        let content_cid = self.cas.put(content)?;
        let node = DagNode::new(kind, parents, content_cid.clone(), agent);
        let node_cid = self.put_node(&node)?;
        Ok((node_cid, content_cid))
    }

    /// Trace the lineage of a node — walk up the parent chain.
    pub fn lineage(&self, cid: &Cid) -> Result<Vec<(Cid, DagNode)>, DagError> {
        let mut result = Vec::new();
        let mut queue = vec![cid.clone()];
        let mut visited = std::collections::HashSet::new();

        while let Some(current) = queue.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            match self.get_node(&current) {
                Ok(node) => {
                    for parent in &node.parents {
                        queue.push(parent.clone());
                    }
                    result.push((current, node));
                }
                Err(DagError::Cas(ket_cas::CasError::NotFound(_))) => {
                    // Parent might be raw content, not a node — skip
                }
                Err(DagError::Serde(_)) => {
                    // Not a valid node (raw content blob) — skip
                }
                Err(e) => return Err(e),
            }
        }

        Ok(result)
    }

    /// Check if content has drifted by comparing a file's current hash to a stored CID.
    pub fn check_drift(&self, path: &std::path::Path, expected_cid: &Cid) -> Result<bool, DagError> {
        let current_cid = ket_cas::hash_file(path)?;
        Ok(current_cid != *expected_cid)
    }

    /// Find all CIDs referenced by DAG nodes (node CIDs + output CIDs).
    pub fn referenced_cids(&self) -> Result<std::collections::HashSet<Cid>, DagError> {
        let mut referenced = std::collections::HashSet::new();
        let all_cids = self.cas.list()?;

        for cid in &all_cids {
            if let Ok(node) = self.get_node(cid) {
                // The node itself is referenced
                referenced.insert(cid.clone());
                // Its output content is referenced
                referenced.insert(node.output_cid.clone());
                // Its parent node CIDs are referenced
                for parent in &node.parents {
                    referenced.insert(parent.clone());
                }
            }
        }

        Ok(referenced)
    }

    /// Export a DAG subgraph as a self-contained bundle.
    /// Walks the node + all ancestors, collecting nodes and their output blobs.
    pub fn export(&self, root_cid: &Cid) -> Result<DagBundle, DagError> {
        let lineage = self.lineage(root_cid)?;
        let mut entries = Vec::new();

        for (cid, node) in &lineage {
            // Get the node's serialized bytes
            let node_bytes = self.cas.get(cid)?;
            // Get the output content
            let output_bytes = self.cas.get(&node.output_cid)?;

            entries.push(BundleEntry {
                node_cid: cid.clone(),
                node_bytes,
                output_cid: node.output_cid.clone(),
                output_bytes,
            });
        }

        Ok(DagBundle {
            root_cid: root_cid.clone(),
            entries,
        })
    }

    /// Import a DAG bundle into this store.
    /// Returns the number of new blobs imported.
    pub fn import(&self, bundle: &DagBundle) -> Result<usize, DagError> {
        let mut imported = 0;

        for entry in &bundle.entries {
            // Import the output content
            if !self.cas.exists(&entry.output_cid) {
                let cid = self.cas.put(&entry.output_bytes)?;
                assert_eq!(cid, entry.output_cid, "Output CID mismatch on import");
                imported += 1;
            }

            // Import the node
            if !self.cas.exists(&entry.node_cid) {
                let cid = self.cas.put(&entry.node_bytes)?;
                assert_eq!(cid, entry.node_cid, "Node CID mismatch on import");
                imported += 1;
            }
        }

        Ok(imported)
    }
}

/// A self-contained bundle of DAG nodes and their content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagBundle {
    pub root_cid: Cid,
    pub entries: Vec<BundleEntry>,
}

/// A single entry in a DAG bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleEntry {
    pub node_cid: Cid,
    #[serde(with = "base64_bytes")]
    pub node_bytes: Vec<u8>,
    pub output_cid: Cid,
    #[serde(with = "base64_bytes")]
    pub output_bytes: Vec<u8>,
}

mod base64_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        STANDARD.decode(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_store(name: &str) -> (CasStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("ket-dag-test-{name}"));
        let _ = fs::remove_dir_all(&dir);
        let store = CasStore::init(&dir).unwrap();
        (store, dir)
    }

    #[test]
    fn store_and_retrieve_node() {
        let (cas, dir) = temp_store("store-retrieve");
        let dag = Dag::new(&cas);

        let content_cid = cas.put(b"hello world").unwrap();
        let node = DagNode::new(NodeKind::Memory, vec![], content_cid.clone(), "human")
            .with_meta("source", "test");

        let node_cid = dag.put_node(&node).unwrap();
        let retrieved = dag.get_node(&node_cid).unwrap();

        assert_eq!(retrieved.kind, NodeKind::Memory);
        assert_eq!(retrieved.output_cid, content_cid);
        assert_eq!(retrieved.agent, "human");
        assert_eq!(retrieved.get_meta("source"), Some("test"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn store_with_node_creates_both() {
        let (cas, dir) = temp_store("store-with-node");
        let dag = Dag::new(&cas);

        let (node_cid, content_cid) =
            dag.store_with_node(b"artifact content", NodeKind::Code, vec![], "claude").unwrap();

        assert!(cas.exists(&node_cid));
        assert!(cas.exists(&content_cid));

        let node = dag.get_node(&node_cid).unwrap();
        assert_eq!(node.output_cid, content_cid);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn lineage_traces_parents() {
        let (cas, dir) = temp_store("lineage");
        let dag = Dag::new(&cas);

        // Create a chain: root -> child -> grandchild
        let (root_cid, _) =
            dag.store_with_node(b"root", NodeKind::Memory, vec![], "human").unwrap();
        let (child_cid, _) =
            dag.store_with_node(b"child", NodeKind::Memory, vec![root_cid.clone()], "human").unwrap();
        let (grandchild_cid, _) =
            dag.store_with_node(b"grandchild", NodeKind::Memory, vec![child_cid.clone()], "human").unwrap();

        let lineage = dag.lineage(&grandchild_cid).unwrap();
        assert_eq!(lineage.len(), 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn drift_detection() {
        let (cas, dir) = temp_store("drift");
        let dag = Dag::new(&cas);

        let test_file = dir.join("test.txt");
        fs::write(&test_file, b"original").unwrap();
        let original_cid = ket_cas::hash_file(&test_file).unwrap();

        // No drift yet
        assert!(!dag.check_drift(&test_file, &original_cid).unwrap());

        // Modify the file
        fs::write(&test_file, b"modified").unwrap();
        assert!(dag.check_drift(&test_file, &original_cid).unwrap());

        let _ = fs::remove_dir_all(&dir);
    }
}
