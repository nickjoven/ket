//! Merkle DAG layer built on top of ket-cas.
//!
//! Each DagNode is serialized to JSON, stored in CAS, and addressable by CID.
//! Parents form the DAG edges. Cycles use soft links (stored separately).
//!
//! Nodes optionally carry a `DecayConfig` and an initial `activation` value.
//! Decay is applied on query (not write) to preserve CID invariants — stored
//! bytes never change, so the CID remains stable.

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

// ---------------------------------------------------------------------------
// Decay infrastructure
// ---------------------------------------------------------------------------

/// Per-node decay configuration.
///
/// Stored in `DagNode.decay_config` when a node should have time-varying
/// activation. Absent means no decay (activation is constant).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecayConfig {
    /// Exponential decay half-life in seconds. Use `f64::INFINITY` for no decay.
    pub half_life_secs: f64,
    /// Minimum activation floor — decay never reduces activation below this.
    pub activation_floor: f64,
}

impl Default for DecayConfig {
    fn default() -> Self {
        DecayConfig {
            half_life_secs: f64::INFINITY,
            activation_floor: 0.0,
        }
    }
}

/// Compute the decayed activation given elapsed seconds since the node was written.
///
/// Formula: `activation × e^(−elapsed × ln(2) / half_life)`, clamped to floor.
///
/// **Decay applies on query, not on write.** The stored `activation` value is
/// never mutated — this function computes an ephemeral value for the caller.
pub fn compute_decayed_activation(activation: f64, elapsed_secs: f64, config: &DecayConfig) -> f64 {
    if config.half_life_secs.is_infinite() || config.half_life_secs <= 0.0 {
        return activation.max(config.activation_floor);
    }
    let decay_factor =
        (-elapsed_secs * std::f64::consts::LN_2 / config.half_life_secs).exp();
    (activation * decay_factor).max(config.activation_floor)
}

// ---------------------------------------------------------------------------
// Node kinds
// ---------------------------------------------------------------------------

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
    /// Optional CID of the schema that `output_cid` conforms to.
    /// The schema blob is user-defined and stored in CAS — ket does not
    /// interpret or validate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_cid: Option<Cid>,
    /// Initial activation value stored at write time.
    ///
    /// The *decayed* activation is computed on query via `decayed_activation()`.
    /// Storing the raw value here preserves content-addressing invariants —
    /// the stored bytes (and thus the CID) never change due to decay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<f64>,
    /// Per-node decay configuration. Absent means no decay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decay_config: Option<DecayConfig>,
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
            schema_cid: None,
            activation: None,
            decay_config: None,
        }
    }

    /// Add a metadata key-value pair.
    pub fn with_meta(mut self, key: &str, value: &str) -> Self {
        self.meta.push((key.to_string(), value.to_string()));
        self
    }

    /// Set the schema CID that this node's output conforms to.
    pub fn with_schema(mut self, schema_cid: Cid) -> Self {
        self.schema_cid = Some(schema_cid);
        self
    }

    /// Declare a saturation level for this node.
    ///
    /// Saturation encodes epistemic confidence on [0.0, 1.0]:
    /// - `0.0` — open query: the content is a question or hypothesis with no
    ///           supporting evidence; the optimizer treats it as maximally
    ///           uncertain and will prioritize exploration.
    /// - `1.0` — settled claim: the content is fully supported; the optimizer
    ///           can skip re-examining it and prune its subtree.
    /// - intermediate — partial belief: scored or partially-confirmed knowledge.
    ///
    /// This replaces the need for a separate "query substrate" layer. Rather
    /// than maintaining two parallel structures (one for claims, one for
    /// open questions), every DAG node carries its own epistemic status. The
    /// optimizer reads `saturation` from metadata and computes
    /// `info_potential = 1.0 - saturation` directly, without touching the
    /// scores table for nodes that have declared their confidence explicitly.
    pub fn with_saturation(mut self, value: f32) -> Self {
        let clamped = value.clamp(0.0, 1.0);
        self.meta.push(("saturation".to_string(), clamped.to_string()));
        self
    }

    /// Read the declared saturation value, if any.
    ///
    /// Returns `None` when no saturation has been set — the optimizer then
    /// falls back to deriving unsaturation from the scores table.
    pub fn saturation(&self) -> Option<f32> {
        self.get_meta("saturation")
            .and_then(|s| s.parse::<f32>().ok())
            .map(|v| v.clamp(0.0, 1.0))
    }

    /// Returns `true` when this node is a settled **claim** (saturation = 1.0).
    ///
    /// A claim carries fully-supported content. The optimizer can treat it as
    /// exhausted and will assign `Tier::Skip` without consulting the scores table.
    pub fn is_claim(&self) -> bool {
        self.saturation().map_or(false, |s| s >= 1.0)
    }

    /// Returns `true` when this node is an open **query** (saturation = 0.0 or unset).
    ///
    /// A query is a node whose content is a question, hypothesis, or placeholder
    /// that has not yet been answered. It is maximally uncertain and will receive
    /// the highest exploration priority from the optimizer.
    pub fn is_query(&self) -> bool {
        self.saturation().map_or(true, |s| s == 0.0)
    }

    /// Set the initial activation value and decay configuration.
    ///
    /// The raw `activation` is stored; the decayed value is computed on query
    /// via `decayed_activation()` to preserve CID invariants.
    pub fn with_decay(mut self, activation: f64, config: DecayConfig) -> Self {
        self.activation = Some(activation);
        self.decay_config = Some(config);
        self
    }

    /// Compute the decay-adjusted activation at `elapsed_secs` after this node
    /// was written.  Returns the stored activation (or `1.0`) if no decay config
    /// is present.
    pub fn decayed_activation(&self, elapsed_secs: f64) -> f64 {
        let base = self.activation.unwrap_or(1.0);
        match &self.decay_config {
            Some(cfg) => compute_decayed_activation(base, elapsed_secs, cfg),
            None => base,
        }
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
        self.lineage_bounded(cid, None)
    }

    /// Trace lineage with an optional depth bound.
    ///
    /// `max_depth = Some(0)` returns only the start node.
    /// `max_depth = None` walks the full ancestor chain (equivalent to `lineage()`).
    pub fn lineage_bounded(
        &self,
        cid: &Cid,
        max_depth: Option<u32>,
    ) -> Result<Vec<(Cid, DagNode)>, DagError> {
        let mut result = Vec::new();
        // Queue entries: (cid, depth_from_start)
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((cid.clone(), 0u32));
        let mut visited = std::collections::HashSet::new();

        while let Some((current, depth)) = queue.pop_front() {
            if !visited.insert(current.clone()) {
                continue;
            }
            match self.get_node(&current) {
                Ok(node) => {
                    let within_bound = max_depth.map_or(true, |d| depth < d);
                    if within_bound {
                        for parent in &node.parents {
                            queue.push_back((parent.clone(), depth + 1));
                        }
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

    /// Compute dedup statistics for a given schema CID.
    ///
    /// Returns (total_nodes, unique_outputs) — if these diverge significantly,
    /// the schema is producing effective deduplication. If they're equal,
    /// every node has unique output and the schema isn't constraining content
    /// enough for CAS dedup to help.
    pub fn schema_stats(&self, schema_cid: &Cid) -> Result<(usize, usize), DagError> {
        let all_cids = self.cas.list()?;
        let mut total = 0usize;
        let mut outputs = std::collections::HashSet::new();

        for cid in &all_cids {
            if let Ok(node) = self.get_node(cid) {
                if node.schema_cid.as_ref() == Some(schema_cid) {
                    total += 1;
                    outputs.insert(node.output_cid.clone());
                }
            }
        }

        Ok((total, outputs.len()))
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
    fn lineage_bounded_respects_depth() {
        let (cas, dir) = temp_store("lineage-bounded");
        let dag = Dag::new(&cas);

        // Create a chain: root -> child -> grandchild -> great_grandchild
        let (root_cid, _) =
            dag.store_with_node(b"root", NodeKind::Memory, vec![], "human").unwrap();
        let (child_cid, _) =
            dag.store_with_node(b"child", NodeKind::Memory, vec![root_cid.clone()], "human")
                .unwrap();
        let (gc_cid, _) =
            dag.store_with_node(b"grandchild", NodeKind::Memory, vec![child_cid.clone()], "human")
                .unwrap();
        let (ggc_cid, _) = dag
            .store_with_node(
                b"great_grandchild",
                NodeKind::Memory,
                vec![gc_cid.clone()],
                "human",
            )
            .unwrap();

        // depth=0: only the start node
        let l0 = dag.lineage_bounded(&ggc_cid, Some(0)).unwrap();
        assert_eq!(l0.len(), 1);

        // depth=1: start + its parents
        let l1 = dag.lineage_bounded(&ggc_cid, Some(1)).unwrap();
        assert_eq!(l1.len(), 2);

        // depth=2: start + parents + grandparents
        let l2 = dag.lineage_bounded(&ggc_cid, Some(2)).unwrap();
        assert_eq!(l2.len(), 3);

        // unbounded: full chain
        let all = dag.lineage(&ggc_cid).unwrap();
        assert_eq!(all.len(), 4);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn saturation_claim_query_classification() {
        let (cas, dir) = temp_store("saturation");
        let cid = cas.put(b"content").unwrap();

        // No saturation → is_query, not is_claim, saturation() = None
        let bare = DagNode::new(NodeKind::Memory, vec![], cid.clone(), "human");
        assert!(bare.is_query(), "undeclared saturation should be a query");
        assert!(!bare.is_claim(), "undeclared saturation should not be a claim");
        assert_eq!(bare.saturation(), None);

        // saturation = 0.0 → explicit query
        let query = DagNode::new(NodeKind::Memory, vec![], cid.clone(), "human")
            .with_saturation(0.0);
        assert!(query.is_query());
        assert!(!query.is_claim());
        assert_eq!(query.saturation(), Some(0.0));

        // saturation = 1.0 → settled claim
        let claim = DagNode::new(NodeKind::Memory, vec![], cid.clone(), "human")
            .with_saturation(1.0);
        assert!(claim.is_claim());
        assert!(!claim.is_query());
        assert_eq!(claim.saturation(), Some(1.0));

        // saturation = 0.75 → partial belief (neither pure query nor pure claim)
        let partial = DagNode::new(NodeKind::Memory, vec![], cid.clone(), "human")
            .with_saturation(0.75);
        assert!(!partial.is_query());
        assert!(!partial.is_claim());
        assert_eq!(partial.saturation(), Some(0.75));

        // Values are clamped to [0.0, 1.0]
        let clamped_hi = DagNode::new(NodeKind::Memory, vec![], cid.clone(), "human")
            .with_saturation(2.0);
        assert_eq!(clamped_hi.saturation(), Some(1.0));

        let clamped_lo = DagNode::new(NodeKind::Memory, vec![], cid.clone(), "human")
            .with_saturation(-0.5);
        assert_eq!(clamped_lo.saturation(), Some(0.0));

        // Round-trip through CAS serialization
        let dag = super::Dag::new(&cas);
        let node_cid = dag.put_node(&claim).unwrap();
        let retrieved = dag.get_node(&node_cid).unwrap();
        assert!(retrieved.is_claim(), "saturation should survive CAS round-trip");
        assert_eq!(retrieved.saturation(), Some(1.0));

        let _ = std::fs::remove_dir_all(&dir);
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
