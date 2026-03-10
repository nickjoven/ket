//! Merkle DAG layer built on top of ket-cas.
//!
//! Each DagNode is serialized to JSON, stored in CAS, and addressable by CID.
//! Parents form the DAG edges. Cycles use soft links (stored separately).
//!
//! Extended with decay and quantum walk primitives per the decay–quantum walk
//! coupling spec. Decay applies on query (not write) to preserve CID invariants.
//! Quantum amplitudes are ephemeral runtime state and are never persisted.

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
// Quantum walk engine
// ---------------------------------------------------------------------------

/// Minimal complex number for quantum walk amplitude tracking.
#[derive(Debug, Clone, Copy)]
pub struct Complex {
    pub re: f64,
    pub im: f64,
}

impl Complex {
    pub fn new(re: f64, im: f64) -> Self {
        Complex { re, im }
    }

    pub fn norm_sq(&self) -> f64 {
        self.re * self.re + self.im * self.im
    }

    pub fn norm(&self) -> f64 {
        self.norm_sq().sqrt()
    }
}

/// Ephemeral quantum walk amplitude for a single node.
///
/// This is a **runtime-only** type — it is never serialized and never stored
/// in CAS.  `#[derive(Serialize, Deserialize)]` is intentionally absent:
/// serializing partial state (session_id without amplitude) would silently
/// produce a value that deserializes with zero amplitude, which is always wrong.
///
/// `session_id` identifies the walk session that produced this amplitude,
/// allowing callers to correlate amplitudes across nodes from the same run.
#[derive(Debug, Clone)]
pub struct QuantumAmplitude {
    /// Session identifier for this amplitude (e.g. a UUID or run label).
    pub session_id: String,
    /// Real component of the complex amplitude.
    pub re: f64,
    /// Imaginary component of the complex amplitude.
    pub im: f64,
}

impl QuantumAmplitude {
    pub fn new(session_id: &str, re: f64, im: f64) -> Self {
        QuantumAmplitude {
            session_id: session_id.to_string(),
            re,
            im,
        }
    }

    pub fn norm_sq(&self) -> f64 {
        self.re * self.re + self.im * self.im
    }

    pub fn norm(&self) -> f64 {
        self.norm_sq().sqrt()
    }
}

/// Continuous-time quantum walk engine.
///
/// Maintains ephemeral amplitude vectors over a DAG topology.  Amplitudes are
/// **never persisted** — they exist only within a session to detect topological
/// orientation and geometric inconsistency (hypothesis H-IC).
///
/// Evolution discretizes the Schrödinger equation via forward Euler:
/// `|ψ(t+dt)⟩ ≈ (I − i·H·dt)|ψ(t)⟩`
/// where H is the decay-weighted adjacency Hamiltonian.  Note that
/// `(I − iH·dt)` is **not unitary** — its operator norm exceeds 1 for H ≠ 0.
/// The state vector is renormalized after each step to keep ‖ψ‖ = 1, which
/// compensates for norm growth but introduces a systematic phase distortion
/// proportional to dt².  Keep dt small (≤ 0.1) and prefer more steps over
/// larger steps to limit this error.
pub struct QuantumWalkEngine {
    /// Number of nodes in the walk.
    pub num_nodes: usize,
    /// Current amplitude state vector (one entry per node).
    amplitudes: Vec<Complex>,
    /// Optional decay configuration per node — influences Hamiltonian weights.
    decay_configs: Vec<Option<DecayConfig>>,
}

impl QuantumWalkEngine {
    /// Create a new engine initialized with a uniform superposition over all nodes.
    pub fn new(num_nodes: usize, decay_configs: Vec<Option<DecayConfig>>) -> Self {
        let init_amp = if num_nodes > 0 {
            1.0 / (num_nodes as f64).sqrt()
        } else {
            0.0
        };
        QuantumWalkEngine {
            num_nodes,
            amplitudes: vec![Complex::new(init_amp, 0.0); num_nodes],
            decay_configs,
        }
    }

    /// Concentrate amplitude at a single source node (classical start).
    pub fn with_source(mut self, source_idx: usize) -> Self {
        self.amplitudes = vec![Complex::new(0.0, 0.0); self.num_nodes];
        if source_idx < self.num_nodes {
            self.amplitudes[source_idx] = Complex::new(1.0, 0.0);
        }
        self
    }

    /// Build the decay-weighted Hamiltonian matrix from an adjacency list.
    ///
    /// `adjacency[i]` is a list of `(neighbor_index, edge_weight)` pairs.
    /// **The adjacency list must already encode both directions** of every
    /// undirected edge (i.e. if (j, w) appears in `adjacency[i]`, then
    /// (i, w) must appear in `adjacency[j]`).  `build_hamiltonian` does NOT
    /// symmetrize — adding h[j][i] here would double-count edges already
    /// encoded in both directions by the caller.
    ///
    /// Each edge weight is scaled by the decay factors of both endpoints, so
    /// edges between fast-decaying nodes contribute less to the Hamiltonian.
    pub fn build_hamiltonian(
        &self,
        adjacency: &[Vec<(usize, f64)>],
        elapsed_secs: f64,
    ) -> Vec<Vec<f64>> {
        let n = self.num_nodes;
        let mut h = vec![vec![0.0; n]; n];

        // Per-node decay factor derived from half-life at query time.
        let decay_factors: Vec<f64> = (0..n)
            .map(|i| match self.decay_configs.get(i) {
                Some(Some(cfg)) if cfg.half_life_secs.is_finite() && cfg.half_life_secs > 0.0 => {
                    (-elapsed_secs * std::f64::consts::LN_2 / cfg.half_life_secs).exp()
                }
                _ => 1.0,
            })
            .collect();

        for (i, neighbors) in adjacency.iter().enumerate() {
            for &(j, weight) in neighbors {
                if i < n && j < n {
                    // Each directed entry contributes exactly once.
                    // Symmetry comes from the caller encoding both directions.
                    h[i][j] += weight * decay_factors[i] * decay_factors[j];
                }
            }
        }

        h
    }

    /// Evolve amplitudes by one forward-Euler step: `|ψ(t+dt)⟩ ≈ (I − i·H·dt)|ψ(t)⟩`.
    ///
    /// The state vector is renormalized after each step to maintain ‖ψ‖ = 1.
    /// This corrects norm growth but introduces O(dt²) phase error per step.
    /// Use small dt (≤ 0.1) to keep the approximation accurate.
    pub fn step(&mut self, hamiltonian: &[Vec<f64>], dt: f64) {
        let n = self.num_nodes;
        let mut new_amps = vec![Complex::new(0.0, 0.0); n];

        for i in 0..n {
            // Identity term
            new_amps[i].re += self.amplitudes[i].re;
            new_amps[i].im += self.amplitudes[i].im;

            // −i·H·dt term: (−i)·h·(a+bi) = h·b − h·a·i
            for j in 0..n {
                let h_dt = hamiltonian[i][j] * dt;
                let amp_j = self.amplitudes[j];
                new_amps[i].re += h_dt * amp_j.im;
                new_amps[i].im -= h_dt * amp_j.re;
            }
        }

        // Re-normalize to maintain unit norm
        let norm_sq: f64 = new_amps.iter().map(|a| a.norm_sq()).sum();
        if norm_sq > 1e-12 {
            let norm = norm_sq.sqrt();
            for a in &mut new_amps {
                a.re /= norm;
                a.im /= norm;
            }
        }

        self.amplitudes = new_amps;
    }

    /// Evolve for `steps` time steps.
    pub fn evolve(&mut self, hamiltonian: &[Vec<f64>], dt: f64, steps: usize) {
        for _ in 0..steps {
            self.step(hamiltonian, dt);
        }
    }

    /// Get amplitude at a node index. Returns `None` if index is out of range.
    pub fn amplitude(&self, idx: usize) -> Option<Complex> {
        self.amplitudes.get(idx).copied()
    }

    /// Compute a walk coherence score in `[0.0, 1.0]`.
    ///
    /// This measures **amplitude localization** via inverse Shannon entropy of
    /// the probability distribution `p_i = |ψ_i|² / Σ|ψ_j|²`:
    ///
    /// ```text
    /// coherence = 1 − H(p) / ln(n)
    /// ```
    ///
    /// where `H(p) = −Σ p_i ln p_i` is the Shannon entropy of the walk.
    ///
    /// **Interpretation note**: this is a localization measure, not a direct
    /// measurement of destructive interference in the quantum-mechanical sense.
    /// True destructive interference (amplitude cancellation at a node due to
    /// phase opposition of incoming paths) is not directly observable from the
    /// probability distribution alone — it requires tracking the signed
    /// amplitudes relative to the graph structure.
    ///
    /// What this metric *can* indicate:
    /// - High score (close to 1): walk is localized — amplitude concentrated on
    ///   few nodes, as happens early in a walk from a source or in disconnected
    ///   subgraphs.
    /// - Low score (close to 0): walk is delocalized — amplitude spread nearly
    ///   uniformly, as expected for well-connected, topologically consistent
    ///   subgraphs.
    ///
    /// The H-IC hypothesis relates structural inconsistency to interference
    /// patterns; use `amplitude()` on individual nodes to inspect whether
    /// specific nodes have anomalously suppressed amplitude relative to their
    /// high-amplitude neighbors.
    pub fn coherence(&self) -> f64 {
        let total: f64 = self.amplitudes.iter().map(|a| a.norm_sq()).sum();
        if total < 1e-12 {
            return 0.0;
        }
        let n = self.num_nodes as f64;
        let mut entropy = 0.0;
        for a in &self.amplitudes {
            let p = a.norm_sq() / total;
            if p > 1e-12 {
                entropy -= p * p.ln();
            }
        }
        let max_entropy = if n > 1.0 { n.ln() } else { 1.0 };
        1.0 - (entropy / max_entropy).clamp(0.0, 1.0)
    }

    /// Export current amplitudes as `QuantumAmplitude` structs tagged with a session id.
    pub fn amplitudes_as_structs(&self, session_id: &str) -> Vec<QuantumAmplitude> {
        self.amplitudes
            .iter()
            .map(|a| QuantumAmplitude::new(session_id, a.re, a.im))
            .collect()
    }
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
