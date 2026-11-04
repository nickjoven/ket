//! Invalidation propagation: which claims lose their support when a node falls.
//!
//! Given a seed CID treated as refuted, walk *downstream* (children) and give
//! every node reached a status from its support parents. Only `grounds` and
//! `derives` count as support; `proposes`, `confirms`, `refutes` and
//! `supersedes` are never followed and never count.
//!
//! The downstream lookup is a `ChildIndex` — a projection or a CAS scan — and
//! is only ever a *candidate* source. Every candidate is re-read from CAS and
//! its typed parents there decide the edge, so a stale or forged index can add
//! work but never change a verdict. Because a node's CID covers its parent
//! list, the CAS support graph is acyclic; the closure is therefore evaluated
//! once in topological order, which is the exact (least) fixed point and
//! handles diamonds without revisits. A cycle can only come from a forged
//! index and its nodes are reported `unknown`, never invalidated.
//!
//! The result is the shared `ket.invalidated/v1` report used by the CLI, MCP
//! and the retraction-POC report.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use ket_cas::{Cid, Store as CasStore};
use serde::Serialize;

use crate::{Dag, DagError, DagNode, EdgeKind};

pub const REPORT_SCHEMA: &str = "ket.invalidated/v1";

#[derive(Debug, thiserror::Error)]
pub enum PropagateError {
    #[error("{0}")]
    Dag(#[from] DagError),
    #[error("Seed not found in CAS (or not a DAG node): {0}")]
    SeedNotFound(String),
    #[error("Child index error: {0}")]
    Index(String),
}

/// Candidate downstream lookup. Never trusted for edge kinds or existence.
pub trait ChildIndex {
    /// Nodes that may have `parent` as a parent, through any edge kind.
    fn children(&self, parent: &Cid) -> Result<Vec<Cid>, String>;
    /// Names the index in the report (`cas-scan`, `projection`, ...).
    fn label(&self) -> String;
}

/// Child index built from one scan of every DAG node in a CAS store.
pub struct CasChildIndex {
    children: HashMap<Cid, Vec<Cid>>,
}

impl CasChildIndex {
    pub fn build(cas: &CasStore) -> Result<Self, DagError> {
        let mut children: HashMap<Cid, BTreeSet<String>> = HashMap::new();
        for cid in cas.list()? {
            let Ok(bytes) = cas.get(&cid) else { continue };
            let Ok(node) = DagNode::from_bytes(&bytes) else {
                continue;
            };
            for parent in &node.parents {
                children
                    .entry(parent.clone())
                    .or_default()
                    .insert(cid.as_str().to_string());
            }
        }
        Ok(CasChildIndex {
            children: children
                .into_iter()
                .map(|(p, cs)| (p, cs.into_iter().map(Cid).collect()))
                .collect(),
        })
    }
}

impl ChildIndex for CasChildIndex {
    fn children(&self, parent: &Cid) -> Result<Vec<Cid>, String> {
        Ok(self.children.get(parent).cloned().unwrap_or_default())
    }
    fn label(&self) -> String {
        "cas-scan".into()
    }
}

/// Which nodes start out refuted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedScope {
    /// Exactly the seed: "what if this fell".
    #[default]
    SeedOnly,
    /// The seed, plus any node inside the seed's downstream closure that has a
    /// recorded `refutes` child. Never a global walk.
    WithRecordedRefutations,
}

#[derive(Debug, Clone, Default)]
pub struct PropagateOptions {
    pub scope: SeedScope,
    /// Stop expanding after this many closure nodes; the rest is `frontier`.
    pub max_nodes: Option<usize>,
    /// Keep `intact` nodes in the report (the seed is always kept).
    pub include_intact: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// The seed, or (with recorded refutations) a node with a `refutes` child.
    Refuted,
    /// Nonempty support, every support parent refuted or invalidated.
    Invalidated,
    /// Some support lost, some not. Does not propagate.
    Weakened,
    /// A support parent is missing, unresolved (truncated) or itself unknown.
    /// Takes precedence over weakened/intact. Does not propagate.
    Unknown,
    Intact,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeReport {
    pub cid: String,
    pub status: Status,
    /// Distinct `grounds`/`derives` parents.
    pub support_total: usize,
    pub support_lost: Vec<String>,
    /// Support parents absent from CAS (or not DAG nodes).
    pub support_missing: Vec<String>,
    /// Support parents whose status is undetermined: unexpanded frontier
    /// nodes, `unknown` parents, or nodes left on a forged cycle.
    pub support_unresolved: Vec<String>,
    /// Shortest support-path length from the seed (seed = 0).
    pub depth: usize,
    /// One shortest support path seed → this node. A witness, not proof:
    /// invalidation is proven by `support_lost.len() == support_total`.
    pub witness_path: Vec<String>,
    /// Nodes linking here with `supersedes`. Non-empty halts expansion here.
    pub superseded_by: Vec<String>,
    /// Nodes linking here with `refutes` (recorded evidence).
    pub refuted_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplacementBoundary {
    pub superseded: String,
    pub by: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProjectionMismatch {
    pub parent: String,
    pub child: String,
    /// `child_missing` or `no_such_parent_link`.
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InvalidationReport {
    pub schema: &'static str,
    pub seed: String,
    pub scope: SeedScope,
    pub index: String,
    pub refuted: Vec<String>,
    /// False when the closure was truncated or any support was missing.
    pub complete: bool,
    pub frontier: Vec<String>,
    pub missing: Vec<String>,
    pub nodes: Vec<NodeReport>,
    pub replacement_boundaries: Vec<ReplacementBoundary>,
    pub projection_mismatch: Vec<ProjectionMismatch>,
}

impl InvalidationReport {
    pub fn status_of(&self, cid: &Cid) -> Option<Status> {
        self.nodes
            .iter()
            .find(|n| n.cid == cid.as_str())
            .map(|n| n.status)
    }
}

fn is_support(kind: EdgeKind) -> bool {
    matches!(kind, EdgeKind::Grounds | EdgeKind::Derives)
}

/// Per closure node, what discovery learned from CAS.
struct Found {
    node: DagNode,
    depth: usize,
    pred: Option<Cid>,
    superseded_by: BTreeSet<String>,
    refuted_by: BTreeSet<String>,
}

/// Propagate the loss of `seed` downstream. See the module docs.
pub fn invalidated(
    dag: &Dag,
    index: &dyn ChildIndex,
    seed: &Cid,
    opts: &PropagateOptions,
) -> Result<InvalidationReport, PropagateError> {
    let seed_node = match dag.get_node(seed) {
        Ok(n) => n,
        Err(DagError::Cas(ket_cas::CasError::NotFound(_))) | Err(DagError::Serde(_)) => {
            return Err(PropagateError::SeedNotFound(seed.as_str().to_string()));
        }
        Err(e) => return Err(e.into()),
    };

    // --- Discovery: BFS over CAS-verified support edges --------------------
    let mut found: BTreeMap<String, Found> = BTreeMap::new();
    let mut mismatches: BTreeSet<(String, String, &'static str)> = BTreeSet::new();
    let mut boundaries: BTreeSet<(String, String)> = BTreeSet::new();
    let mut frontier: BTreeSet<String> = BTreeSet::new();
    let mut queued: BTreeSet<String> = BTreeSet::new();
    let mut queue: VecDeque<(Cid, DagNode, usize, Option<Cid>)> = VecDeque::new();
    queue.push_back((seed.clone(), seed_node, 0, None));
    queued.insert(seed.as_str().to_string());
    let mut child_cache: HashMap<String, Option<DagNode>> = HashMap::new();

    while let Some((cid, node, depth, pred)) = queue.pop_front() {
        if opts.max_nodes.is_some_and(|m| found.len() >= m) {
            frontier.insert(cid.as_str().to_string());
            for (c, ..) in queue.drain(..) {
                frontier.insert(c.as_str().to_string());
            }
            break;
        }
        let mut candidates = index.children(&cid).map_err(PropagateError::Index)?;
        candidates.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        candidates.dedup();

        let mut support_children = Vec::new();
        let mut superseded_by = BTreeSet::new();
        let mut refuted_by = BTreeSet::new();
        for child in candidates {
            let entry = child_cache
                .entry(child.as_str().to_string())
                .or_insert_with(|| dag.get_node(&child).ok());
            let Some(child_node) = entry.clone() else {
                mismatches.insert((
                    cid.as_str().to_string(),
                    child.as_str().to_string(),
                    "child_missing",
                ));
                continue;
            };
            let kinds: Vec<EdgeKind> = child_node
                .parent_links()
                .filter(|(p, _)| *p == &cid)
                .map(|(_, k)| k)
                .collect();
            if kinds.is_empty() {
                mismatches.insert((
                    cid.as_str().to_string(),
                    child.as_str().to_string(),
                    "no_such_parent_link",
                ));
                continue;
            }
            if kinds.contains(&EdgeKind::Supersedes) {
                superseded_by.insert(child.as_str().to_string());
            }
            if kinds.contains(&EdgeKind::Refutes) {
                refuted_by.insert(child.as_str().to_string());
            }
            if kinds.iter().copied().any(is_support) {
                support_children.push((child, child_node));
            }
        }

        // A superseded node halts expansion: its replacement is never entered
        // and its dependents are reached only through other support paths.
        if superseded_by.is_empty() {
            for (child, child_node) in support_children {
                if queued.insert(child.as_str().to_string()) {
                    queue.push_back((child, child_node, depth + 1, Some(cid.clone())));
                }
            }
        } else {
            for by in &superseded_by {
                boundaries.insert((cid.as_str().to_string(), by.clone()));
            }
        }

        found.insert(
            cid.as_str().to_string(),
            Found {
                node,
                depth,
                pred,
                superseded_by,
                refuted_by,
            },
        );
    }

    // --- Refuted set ------------------------------------------------------
    let mut refuted: BTreeSet<String> = BTreeSet::new();
    refuted.insert(seed.as_str().to_string());
    if opts.scope == SeedScope::WithRecordedRefutations {
        for (cid, f) in &found {
            if !f.refuted_by.is_empty() {
                refuted.insert(cid.clone());
            }
        }
    }

    // --- Support parents of every closure node, from CAS -----------------
    let mut support: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (cid, f) in &found {
        let parents = f
            .node
            .parent_links()
            .filter(|(_, k)| is_support(*k))
            .map(|(p, _)| p.as_str().to_string())
            .collect();
        support.insert(cid.clone(), parents);
    }

    // --- Topological evaluation over closure-internal support edges ------
    let mut indegree: BTreeMap<&str, usize> = found.keys().map(|k| (k.as_str(), 0)).collect();
    let mut out_edges: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (cid, parents) in &support {
        for p in parents {
            if found.contains_key(p) {
                *indegree.get_mut(cid.as_str()).unwrap() += 1;
                out_edges.entry(p.as_str()).or_default().push(cid.as_str());
            }
        }
    }
    let mut ready: BTreeSet<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(c, _)| *c)
        .collect();
    let mut order: Vec<&str> = Vec::with_capacity(found.len());
    while let Some(c) = ready.pop_first() {
        order.push(c);
        for child in out_edges.get(c).into_iter().flatten() {
            let d = indegree.get_mut(child).unwrap();
            *d -= 1;
            if *d == 0 {
                ready.insert(child);
            }
        }
    }
    // Anything left sits on a cycle only a forged index could produce.
    let cyclic: BTreeSet<&str> = indegree
        .iter()
        .filter(|(_, d)| **d > 0)
        .map(|(c, _)| *c)
        .collect();

    let mut status: BTreeMap<String, Status> = BTreeMap::new();
    let mut missing: BTreeSet<String> = BTreeSet::new();
    let mut reports: BTreeMap<String, NodeReport> = BTreeMap::new();
    let mut presence: HashMap<String, bool> = HashMap::new();

    for c in order.iter().copied().chain(cyclic.iter().copied()) {
        let parents = &support[c];
        let mut lost = Vec::new();
        let mut miss = Vec::new();
        let mut unresolved = Vec::new();
        for p in parents {
            if refuted.contains(p) {
                lost.push(p.clone());
            } else if let Some(s) = status.get(p) {
                match s {
                    Status::Refuted | Status::Invalidated => lost.push(p.clone()),
                    Status::Unknown => unresolved.push(p.clone()),
                    Status::Weakened | Status::Intact => {}
                }
            } else if cyclic.contains(p.as_str()) || frontier.contains(p) {
                unresolved.push(p.clone());
            } else if found.contains_key(p) {
                // In closure but not yet evaluated: only possible on a cycle.
                unresolved.push(p.clone());
            } else {
                // Outside the closure: not downstream of the seed, so its
                // standing is unaffected — provided it exists.
                let present = *presence
                    .entry(p.clone())
                    .or_insert_with(|| dag.get_node(&Cid(p.clone())).is_ok());
                if !present {
                    miss.push(p.clone());
                    missing.insert(p.clone());
                }
            }
        }
        let s = if refuted.contains(c) {
            Status::Refuted
        } else if cyclic.contains(c) || !miss.is_empty() || !unresolved.is_empty() {
            Status::Unknown
        } else if !parents.is_empty() && lost.len() == parents.len() {
            Status::Invalidated
        } else if !lost.is_empty() {
            Status::Weakened
        } else {
            Status::Intact
        };
        status.insert(c.to_string(), s);

        let f = &found[c];
        reports.insert(
            c.to_string(),
            NodeReport {
                cid: c.to_string(),
                status: s,
                support_total: parents.len(),
                support_lost: lost,
                support_missing: miss,
                support_unresolved: unresolved,
                depth: f.depth,
                witness_path: witness(&found, c),
                superseded_by: f.superseded_by.iter().cloned().collect(),
                refuted_by: f.refuted_by.iter().cloned().collect(),
            },
        );
    }

    let complete = frontier.is_empty() && missing.is_empty() && cyclic.is_empty();
    let nodes = reports
        .into_values()
        .filter(|n| opts.include_intact || n.status != Status::Intact || n.cid == seed.as_str())
        .collect();

    Ok(InvalidationReport {
        schema: REPORT_SCHEMA,
        seed: seed.as_str().to_string(),
        scope: opts.scope,
        index: index.label(),
        refuted: refuted.into_iter().collect(),
        complete,
        frontier: frontier.into_iter().collect(),
        missing: missing.into_iter().collect(),
        nodes,
        replacement_boundaries: boundaries
            .into_iter()
            .map(|(superseded, by)| ReplacementBoundary { superseded, by })
            .collect(),
        projection_mismatch: mismatches
            .into_iter()
            .map(|(parent, child, reason)| ProjectionMismatch {
                parent,
                child,
                reason: reason.to_string(),
            })
            .collect(),
    })
}

fn witness(found: &BTreeMap<String, Found>, cid: &str) -> Vec<String> {
    let mut path = vec![cid.to_string()];
    let mut cur = found[cid].pred.clone();
    while let Some(p) = cur {
        path.push(p.as_str().to_string());
        cur = found.get(p.as_str()).and_then(|f| f.pred.clone());
    }
    path.reverse();
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeKind;
    use std::fs;

    struct Fx {
        cas: CasStore,
        dir: std::path::PathBuf,
    }

    impl Fx {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("ket-propagate-test-{name}"));
            let _ = fs::remove_dir_all(&dir);
            let cas = CasStore::init(&dir).unwrap();
            Fx { cas, dir }
        }
        fn dag(&self) -> Dag<'_> {
            Dag::new(&self.cas)
        }
        /// A node with `label` content and typed parents.
        fn node(&self, label: &str, parents: &[(&Cid, EdgeKind)]) -> Cid {
            let out = self.cas.put(label.as_bytes()).unwrap();
            let node = DagNode::new_typed(
                NodeKind::Reasoning,
                parents.iter().map(|(c, k)| ((*c).clone(), *k)).collect(),
                out,
                "test",
            );
            self.dag().put_node(&node).unwrap()
        }
        fn run(&self, seed: &Cid, opts: PropagateOptions) -> InvalidationReport {
            let index = CasChildIndex::build(&self.cas).unwrap();
            invalidated(&self.dag(), &index, seed, &opts).unwrap()
        }
    }

    impl Drop for Fx {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    use EdgeKind::*;

    fn all() -> PropagateOptions {
        PropagateOptions {
            include_intact: true,
            ..Default::default()
        }
    }

    #[test]
    fn full_invalidation_down_a_chain() {
        let fx = Fx::new("chain");
        let a = fx.node("a", &[]);
        let b = fx.node("b", &[(&a, Grounds)]);
        let c = fx.node("c", &[(&b, Derives)]);
        let r = fx.run(&a, all());
        assert_eq!(r.status_of(&a), Some(Status::Refuted));
        assert_eq!(r.status_of(&b), Some(Status::Invalidated));
        assert_eq!(r.status_of(&c), Some(Status::Invalidated));
        let nc = r.nodes.iter().find(|n| n.cid == c.as_str()).unwrap();
        assert_eq!(nc.depth, 2);
        assert_eq!(nc.witness_path, vec![a.0.clone(), b.0.clone(), c.0.clone()]);
        assert!(r.complete);
        assert_eq!(r.schema, REPORT_SCHEMA);
    }

    #[test]
    fn partial_loss_weakens_and_does_not_propagate() {
        let fx = Fx::new("partial");
        let a = fx.node("a", &[]);
        let other = fx.node("other", &[]);
        let b = fx.node("b", &[(&a, Grounds), (&other, Grounds)]);
        let c = fx.node("c", &[(&b, Derives)]);
        let r = fx.run(&a, all());
        assert_eq!(r.status_of(&b), Some(Status::Weakened));
        assert_eq!(r.status_of(&c), Some(Status::Intact));
        let nb = r.nodes.iter().find(|n| n.cid == b.as_str()).unwrap();
        assert_eq!(nb.support_total, 2);
        assert_eq!(nb.support_lost, vec![a.0.clone()]);
    }

    #[test]
    fn diamond_both_paths_lost_invalidates() {
        let fx = Fx::new("diamond");
        let a = fx.node("a", &[]);
        let l = fx.node("l", &[(&a, Derives)]);
        let rr = fx.node("r", &[(&a, Derives)]);
        let d = fx.node("d", &[(&l, Derives), (&rr, Derives)]);
        let r = fx.run(&a, all());
        assert_eq!(r.status_of(&d), Some(Status::Invalidated));
    }

    #[test]
    fn diamond_one_path_lost_weakens() {
        let fx = Fx::new("diamond-half");
        let a = fx.node("a", &[]);
        let ext = fx.node("ext", &[]);
        let l = fx.node("l", &[(&a, Derives)]);
        let rr = fx.node("r", &[(&a, Derives), (&ext, Grounds)]); // weakened only
        let d = fx.node("d", &[(&l, Derives), (&rr, Derives)]);
        let r = fx.run(&a, all());
        assert_eq!(r.status_of(&rr), Some(Status::Weakened));
        assert_eq!(r.status_of(&d), Some(Status::Weakened));
    }

    #[test]
    fn long_and_short_paths_converge_regardless_of_order() {
        // d is reached first via the short path (depth 1) but its second
        // parent is only final after the long chain; topological order makes
        // the verdict independent of discovery order.
        let fx = Fx::new("revisit");
        let a = fx.node("a", &[]);
        let x1 = fx.node("x1", &[(&a, Derives)]);
        let x2 = fx.node("x2", &[(&x1, Derives)]);
        let x3 = fx.node("x3", &[(&x2, Derives)]);
        let d = fx.node("d", &[(&a, Derives), (&x3, Derives)]);
        let r = fx.run(&a, all());
        assert_eq!(r.status_of(&d), Some(Status::Invalidated));
        let nd = r.nodes.iter().find(|n| n.cid == d.as_str()).unwrap();
        assert_eq!(nd.depth, 1);
    }

    #[test]
    fn no_support_never_invalidates_vacuously() {
        let fx = Fx::new("nosupport");
        let a = fx.node("a", &[]);
        let hyp = fx.node("hyp", &[(&a, Proposes)]);
        let r = fx.run(&a, all());
        // Reached only through a non-support edge: not in the closure at all.
        assert_eq!(r.status_of(&hyp), None);
    }

    #[test]
    fn non_support_edges_are_not_followed() {
        let fx = Fx::new("nonsupport");
        let a = fx.node("a", &[]);
        for (i, k) in [Proposes, Confirms, Refutes, Supersedes]
            .into_iter()
            .enumerate()
        {
            let n = fx.node(&format!("n{i}"), &[(&a, k)]);
            let below = fx.node(&format!("below{i}"), &[(&n, Derives)]);
            let r = fx.run(&a, all());
            assert_eq!(r.status_of(&n), None, "{k} child must not be entered");
            assert_eq!(r.status_of(&below), None, "{k}: nothing past it");
        }
    }

    #[test]
    fn missing_parent_makes_unknown_and_incomplete() {
        let fx = Fx::new("missing");
        let a = fx.node("a", &[]);
        let ghost = Cid("0".repeat(64));
        let b = fx.node("b", &[(&a, Derives), (&ghost, Grounds)]);
        let c = fx.node("c", &[(&b, Derives)]);
        let r = fx.run(&a, all());
        assert_eq!(r.status_of(&b), Some(Status::Unknown));
        // An unknown parent leaves its child undetermined too.
        assert_eq!(r.status_of(&c), Some(Status::Unknown));
        assert!(!r.complete);
        assert_eq!(r.missing, vec![ghost.0.clone()]);
        let nb = r.nodes.iter().find(|n| n.cid == b.as_str()).unwrap();
        assert_eq!(nb.support_missing, vec![ghost.0]);
    }

    #[test]
    fn missing_seed_is_an_error() {
        let fx = Fx::new("noseed");
        let index = CasChildIndex::build(&fx.cas).unwrap();
        let err = invalidated(&fx.dag(), &index, &Cid("f".repeat(64)), &all()).unwrap_err();
        assert!(matches!(err, PropagateError::SeedNotFound(_)));
    }

    #[test]
    fn supersedes_halts_expansion_and_reports_boundary() {
        let fx = Fx::new("supersedes");
        let a = fx.node("a", &[]);
        let x = fx.node("x", &[(&a, Derives)]);
        let rep = fx.node("x-v2", &[(&x, Supersedes)]);
        let dep = fx.node("dep", &[(&x, Derives)]);
        let r = fx.run(&a, all());
        let nx = r.nodes.iter().find(|n| n.cid == x.as_str()).unwrap();
        assert_eq!(nx.status, Status::Invalidated);
        assert_eq!(nx.superseded_by, vec![rep.0.clone()]);
        assert_eq!(r.status_of(&rep), None, "replacement never entered");
        assert_eq!(r.status_of(&dep), None, "no expansion through superseded x");
        assert_eq!(
            r.replacement_boundaries,
            vec![ReplacementBoundary {
                superseded: x.0.clone(),
                by: rep.0.clone()
            }]
        );
    }

    #[test]
    fn supersedes_alternate_path_still_traversed() {
        let fx = Fx::new("supersedes-alt");
        let a = fx.node("a", &[]);
        let x = fx.node("x", &[(&a, Derives)]);
        let _rep = fx.node("x-v2", &[(&x, Supersedes)]);
        let y = fx.node("y", &[(&a, Derives)]);
        let dep = fx.node("dep", &[(&x, Derives), (&y, Derives)]);
        let r = fx.run(&a, all());
        // Reached via y; x's own (invalidated) status still counts as a parent.
        assert_eq!(r.status_of(&dep), Some(Status::Invalidated));
        let nd = r.nodes.iter().find(|n| n.cid == dep.as_str()).unwrap();
        assert_eq!(
            nd.witness_path,
            vec![a.0.clone(), y.0.clone(), dep.0.clone()]
        );
    }

    #[test]
    fn recorded_refutations_only_inside_the_closure() {
        let fx = Fx::new("recorded");
        let a = fx.node("a", &[]);
        let ext = fx.node("ext", &[]);
        let b = fx.node("b", &[(&a, Derives), (&ext, Derives)]);
        let _rb = fx.node("refute-b", &[(&b, Refutes)]);
        let c = fx.node("c", &[(&b, Derives)]);
        // Unrelated refutation elsewhere in the store.
        let z = fx.node("z", &[]);
        let _rz = fx.node("refute-z", &[(&z, Refutes)]);
        let zc = fx.node("zc", &[(&z, Derives)]);

        let seed_only = fx.run(&a, all());
        assert_eq!(seed_only.status_of(&b), Some(Status::Weakened));
        assert_eq!(seed_only.status_of(&c), Some(Status::Intact));
        assert_eq!(seed_only.refuted, vec![a.0.clone()]);

        let rec = fx.run(
            &a,
            PropagateOptions {
                scope: SeedScope::WithRecordedRefutations,
                include_intact: true,
                ..Default::default()
            },
        );
        assert_eq!(rec.status_of(&b), Some(Status::Refuted));
        assert_eq!(rec.status_of(&c), Some(Status::Invalidated));
        assert_eq!(rec.status_of(&zc), None, "no global walk");
        assert!(!rec.refuted.contains(&z.0));
        let nb = rec.nodes.iter().find(|n| n.cid == b.as_str()).unwrap();
        assert_eq!(nb.refuted_by.len(), 1);
    }

    #[test]
    fn max_nodes_truncates_to_frontier_and_unresolved() {
        let fx = Fx::new("truncate");
        let a = fx.node("a", &[]);
        let b = fx.node("b", &[(&a, Derives)]);
        let c = fx.node("c", &[(&b, Derives)]);
        let r = fx.run(
            &a,
            PropagateOptions {
                max_nodes: Some(2),
                include_intact: true,
                ..Default::default()
            },
        );
        assert!(!r.complete);
        assert_eq!(r.frontier, vec![c.0.clone()]);
        assert_eq!(r.status_of(&c), None);
        assert_eq!(r.status_of(&b), Some(Status::Invalidated));
    }

    #[test]
    fn include_intact_false_drops_intact_but_keeps_seed() {
        let fx = Fx::new("intact");
        let a = fx.node("a", &[]);
        let ext = fx.node("ext", &[]);
        let b = fx.node("b", &[(&a, Derives), (&ext, Derives)]);
        let c = fx.node("c", &[(&b, Derives)]);
        let r = fx.run(&a, PropagateOptions::default());
        assert_eq!(r.status_of(&a), Some(Status::Refuted));
        assert_eq!(r.status_of(&b), Some(Status::Weakened));
        assert_eq!(r.status_of(&c), None);
    }

    /// An index that lies: extra children, reversed order, a forged cycle.
    struct Forged {
        inner: CasChildIndex,
        extra: HashMap<Cid, Vec<Cid>>,
    }
    impl ChildIndex for Forged {
        fn children(&self, p: &Cid) -> Result<Vec<Cid>, String> {
            let mut v = self.inner.children(p)?;
            v.extend(self.extra.get(p).cloned().unwrap_or_default());
            v.reverse();
            Ok(v)
        }
        fn label(&self) -> String {
            "forged".into()
        }
    }

    #[test]
    fn forged_index_cannot_change_verdicts_and_terminates() {
        let fx = Fx::new("forged");
        let a = fx.node("a", &[]);
        let ext = fx.node("ext", &[]);
        let b = fx.node("b", &[(&a, Derives), (&ext, Derives)]);
        let c = fx.node("c", &[(&b, Derives)]);
        let unrelated = fx.node("unrelated", &[]);
        let ghost = Cid("e".repeat(64));
        let mut extra = HashMap::new();
        // c "is a parent of" b (cycle claim), a "has" unrelated and ghost children.
        extra.insert(c.clone(), vec![b.clone()]);
        extra.insert(a.clone(), vec![unrelated.clone(), ghost.clone()]);
        let index = Forged {
            inner: CasChildIndex::build(&fx.cas).unwrap(),
            extra,
        };
        let honest = fx.run(&a, all());
        let r = invalidated(&fx.dag(), &index, &a, &all()).unwrap();
        for n in &honest.nodes {
            assert_eq!(r.status_of(&Cid(n.cid.clone())), Some(n.status));
        }
        assert_eq!(r.status_of(&unrelated), None);
        let reasons: BTreeSet<_> = r
            .projection_mismatch
            .iter()
            .map(|m| m.reason.as_str())
            .collect();
        assert!(reasons.contains("child_missing"));
        assert!(reasons.contains("no_such_parent_link"));
        assert_eq!(r.index, "forged");
    }

    #[test]
    fn output_is_stable_under_index_order() {
        let fx = Fx::new("stable");
        let a = fx.node("a", &[]);
        for i in 0..6 {
            let m = fx.node(&format!("m{i}"), &[(&a, Derives)]);
            fx.node(&format!("leaf{i}"), &[(&m, Derives)]);
        }
        let honest = fx.run(&a, all());
        let shuffled = Forged {
            inner: CasChildIndex::build(&fx.cas).unwrap(),
            extra: HashMap::new(),
        };
        let r = invalidated(&fx.dag(), &shuffled, &a, &all()).unwrap();
        let strip = |r: &InvalidationReport| {
            let mut v = serde_json::to_value(r).unwrap();
            v["index"] = serde_json::Value::Null;
            v
        };
        assert_eq!(strip(&honest), strip(&r));
    }
}
