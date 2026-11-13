//! Labelling: which nodes stand, which fall, and whether two views agree.
//!
//! `propagate` answers "what if this node fell" from one seed. This module
//! answers the whole-graph question: given a set of nodes (a *world*), give
//! every node a status from the support and attack edges inside that world.
//!
//! * **Support** is a `grounds` or `derives` parent. A node's support parents
//!   are *linked*: it needs all of them.
//! * **Attack** is a `refutes` link from another node in the world.
//! * `proposes`, `confirms` and `supersedes` are neither, and are ignored.
//!
//! The rules are the grounded semantics of argumentation theory, extended
//! with support:
//!
//! * `out` — some attacker is `in`, or some support parent is `out`.
//! * `in` — every support parent is `in` and every attacker is `out`.
//! * `unknown` — not settled by the two rules above, and the reason is an
//!   evidence gap: a support parent absent from CAS (or not a DAG node), or a
//!   dependency that is itself `unknown`.
//! * `contested` — not settled, with no evidence gap: the node sits on a
//!   standoff (`r` refutes `n` on the strength of `p`, while `r2` refutes `p`
//!   on the strength of `n`).
//!
//! `in`/`out` are the least fixed point of the two rules, so the result is
//! unique and does not depend on evaluation order. A refuted refuter no
//! longer defeats its target (*reinstatement*). A definite defeat outranks an
//! evidence gap: a node with a missing ground and an `in` attacker is `out`.
//!
//! Differences from `ket.invalidated/v1`, which is unchanged: v1 marks a node
//! invalidated only when *all* its support is lost (here, any), has no
//! reinstatement, and consults recorded refutations only inside the seed's
//! downstream closure.
//!
//! A `refutes` link is read as an attack on that node (that derivation). Two
//! nodes with the same `output_cid` are separate derivations of one claim;
//! `convergent_claims` reports the claim-level status, which is `in` while any
//! derivation of it is.
//!
//! A world is closed under parents, read from verified CAS bytes; no index or
//! projection is consulted. Storage failures and corrupt bytes are errors.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ket_cas::Cid;
use serde::Serialize;

use crate::propagate::read_node;
use crate::{Dag, DagError, DagNode, EdgeKind};

pub const LABEL_SCHEMA: &str = "ket.labelling/v1";
pub const CONSISTENCY_SCHEMA: &str = "ket.consistency/v1";

#[derive(Debug, thiserror::Error)]
pub enum LabelError {
    #[error("{0}")]
    Dag(#[from] DagError),
    #[error("Root not found in CAS (or not a DAG node): {0}")]
    RootNotFound(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Label {
    In,
    Out,
    Contested,
    Unknown,
}

fn is_support(kind: EdgeKind) -> bool {
    matches!(kind, EdgeKind::Grounds | EdgeKind::Derives)
}

/// A set of DAG nodes closed under parents.
#[derive(Debug, Clone, Default)]
pub struct World {
    nodes: BTreeMap<String, DagNode>,
}

impl World {
    /// The roots and all their ancestors through every edge kind. A refuting
    /// root therefore brings in the node it refutes.
    pub fn closure(dag: &Dag, roots: &[Cid]) -> Result<World, LabelError> {
        let mut nodes = BTreeMap::new();
        let mut absent: BTreeSet<String> = BTreeSet::new();
        let mut stack = Vec::new();
        for root in roots {
            let node = read_node(dag, root)?
                .ok_or_else(|| LabelError::RootNotFound(root.as_str().to_string()))?;
            if nodes.insert(root.as_str().to_string(), node).is_none() {
                stack.push(root.as_str().to_string());
            }
        }
        while let Some(cid) = stack.pop() {
            let parents: Vec<Cid> = nodes[&cid].parents.clone();
            for parent in parents {
                let key = parent.as_str();
                if nodes.contains_key(key) || absent.contains(key) {
                    continue;
                }
                match read_node(dag, &parent)? {
                    Some(node) => {
                        nodes.insert(key.to_string(), node);
                        stack.push(key.to_string());
                    }
                    None => {
                        absent.insert(key.to_string());
                    }
                }
            }
        }
        Ok(World { nodes })
    }

    /// Every DAG node in the store.
    pub fn all(dag: &Dag) -> Result<World, LabelError> {
        let mut nodes = BTreeMap::new();
        for cid in dag.cas.list().map_err(DagError::from)? {
            if let Some(node) = read_node(dag, &cid)? {
                nodes.insert(cid.as_str().to_string(), node);
            }
        }
        Ok(World { nodes })
    }

    /// Both worlds together. Closed under parents because each side is.
    pub fn union(&self, other: &World) -> World {
        let mut nodes = self.nodes.clone();
        for (cid, node) in &other.nodes {
            nodes.entry(cid.clone()).or_insert_with(|| node.clone());
        }
        World { nodes }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn contains(&self, cid: &str) -> bool {
        self.nodes.contains_key(cid)
    }

    pub fn cids(&self) -> impl Iterator<Item = &str> {
        self.nodes.keys().map(String::as_str)
    }
}

/// What one node's status depends on, inside its world.
#[derive(Debug, Clone, Default)]
struct Deps {
    /// Support parents present in the world.
    support: BTreeSet<String>,
    /// Support parents absent from CAS, or present but not DAG nodes.
    support_missing: BTreeSet<String>,
    /// Nodes in the world with a `refutes` link here.
    attackers: BTreeSet<String>,
}

/// The status of every node in a world.
#[derive(Debug, Clone)]
pub struct Labelling {
    labels: BTreeMap<String, Label>,
    deps: BTreeMap<String, Deps>,
}

impl Labelling {
    pub fn of(world: &World) -> Labelling {
        let mut deps: BTreeMap<String, Deps> = world
            .nodes
            .keys()
            .map(|cid| (cid.clone(), Deps::default()))
            .collect();
        // Reverse edges: who each node supports, and who it attacks.
        let mut supports: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        let mut attacks: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (cid, node) in &world.nodes {
            for (parent, kind) in node.parent_links() {
                let parent = parent.as_str();
                if is_support(kind) {
                    if let Some((key, _)) = world.nodes.get_key_value(parent) {
                        if deps.get_mut(cid).unwrap().support.insert(key.clone()) {
                            supports.entry(key.as_str()).or_default().push(cid);
                        }
                    } else {
                        deps.get_mut(cid)
                            .unwrap()
                            .support_missing
                            .insert(parent.to_string());
                    }
                } else if kind == EdgeKind::Refutes {
                    if let Some((key, _)) = world.nodes.get_key_value(parent) {
                        if deps.get_mut(key).unwrap().attackers.insert(cid.clone()) {
                            attacks.entry(cid.as_str()).or_default().push(key);
                        }
                    }
                }
            }
        }

        // Least fixed point of the in/out rules. `need` counts what still
        // stands between a node and `in`: support not yet in, attackers not
        // yet out. A missing support parent is never satisfied.
        let mut need: BTreeMap<&str, usize> = deps
            .iter()
            .map(|(cid, d)| {
                (
                    cid.as_str(),
                    d.support.len() + d.support_missing.len() + d.attackers.len(),
                )
            })
            .collect();
        let mut labels: BTreeMap<String, Label> = BTreeMap::new();
        let mut queue: VecDeque<&str> = VecDeque::new();
        for (cid, n) in &need {
            if *n == 0 {
                labels.insert(cid.to_string(), Label::In);
                queue.push_back(cid);
            }
        }
        while let Some(cid) = queue.pop_front() {
            let (defeated, satisfied) = match labels[cid] {
                // An in node defeats what it attacks and satisfies what it supports.
                Label::In => (attacks.get(cid), supports.get(cid)),
                // An out node takes down what it supports and frees what it attacks.
                Label::Out => (supports.get(cid), attacks.get(cid)),
                Label::Contested | Label::Unknown => unreachable!("only in/out are queued"),
            };
            for target in defeated.into_iter().flatten() {
                if !labels.contains_key(*target) {
                    labels.insert(target.to_string(), Label::Out);
                    queue.push_back(target);
                }
            }
            for target in satisfied.into_iter().flatten() {
                let n = need.get_mut(target).unwrap();
                *n -= 1;
                if *n == 0 && !labels.contains_key(*target) {
                    labels.insert(target.to_string(), Label::In);
                    queue.push_back(target);
                }
            }
        }

        // Among the unsettled, an evidence gap spreads to whatever depends on it.
        let mut gap: VecDeque<&str> = deps
            .iter()
            .filter(|(cid, d)| !labels.contains_key(*cid) && !d.support_missing.is_empty())
            .map(|(cid, _)| cid.as_str())
            .collect();
        for cid in &gap {
            labels.insert(cid.to_string(), Label::Unknown);
        }
        while let Some(cid) = gap.pop_front() {
            let dependents = supports
                .get(cid)
                .into_iter()
                .flatten()
                .chain(attacks.get(cid).into_iter().flatten());
            for target in dependents {
                if !labels.contains_key(*target) {
                    labels.insert(target.to_string(), Label::Unknown);
                    gap.push_back(target);
                }
            }
        }
        for cid in deps.keys() {
            labels.entry(cid.clone()).or_insert(Label::Contested);
        }

        Labelling { labels, deps }
    }

    pub fn get(&self, cid: &Cid) -> Option<Label> {
        self.labels.get(cid.as_str()).copied()
    }

    fn label(&self, cid: &str) -> Option<Label> {
        self.labels.get(cid).copied()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeLabel {
    pub cid: String,
    pub label: Label,
    pub output_cid: String,
    /// Distinct `grounds`/`derives` parents, present or not.
    pub support_total: usize,
    /// Support parents that are `out`.
    pub support_out: Vec<String>,
    /// Support parents absent from CAS, or not DAG nodes.
    pub support_missing: Vec<String>,
    /// Every node in the world with a `refutes` link here.
    pub attackers: Vec<String>,
    /// The attackers that are `in`: the ones that defeat this node.
    pub attackers_in: Vec<String>,
    /// Dependencies that are `contested` or `unknown`.
    pub unsettled: Vec<String>,
}

/// One claim (content CID) with more than one derivation in the world.
#[derive(Debug, Clone, Serialize)]
pub struct ClaimLabel {
    pub output_cid: String,
    /// `in` while any derivation is; `out` only when all are.
    pub label: Label,
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct LabelCounts {
    #[serde(rename = "in")]
    pub in_: usize,
    pub out: usize,
    pub contested: usize,
    pub unknown: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelReport {
    pub schema: &'static str,
    /// `closure` (the roots and their ancestors) or `store` (every node).
    pub scope: &'static str,
    pub roots: Vec<String>,
    /// False when any support parent is missing: some statuses are `unknown`.
    pub complete: bool,
    pub counts: LabelCounts,
    pub missing: Vec<String>,
    pub nodes: Vec<NodeLabel>,
    pub convergent_claims: Vec<ClaimLabel>,
}

impl LabelReport {
    pub fn label_of(&self, cid: &Cid) -> Option<Label> {
        self.nodes
            .iter()
            .find(|n| n.cid == cid.as_str())
            .map(|n| n.label)
    }
}

fn claim_label(labels: impl Iterator<Item = Label>) -> Label {
    let all: BTreeSet<Label> = labels.collect();
    if all.contains(&Label::In) {
        Label::In
    } else if all.iter().all(|l| *l == Label::Out) {
        Label::Out
    } else if all.contains(&Label::Unknown) {
        Label::Unknown
    } else {
        Label::Contested
    }
}

fn build_report(world: &World, scope: &'static str, roots: &[Cid]) -> LabelReport {
    let labelling = Labelling::of(world);
    let mut counts = LabelCounts::default();
    let mut missing: BTreeSet<String> = BTreeSet::new();
    let mut by_output: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut nodes = Vec::with_capacity(world.len());
    for (cid, node) in &world.nodes {
        let label = labelling.labels[cid];
        match label {
            Label::In => counts.in_ += 1,
            Label::Out => counts.out += 1,
            Label::Contested => counts.contested += 1,
            Label::Unknown => counts.unknown += 1,
        }
        let deps = &labelling.deps[cid];
        missing.extend(deps.support_missing.iter().cloned());
        by_output
            .entry(node.output_cid.as_str())
            .or_default()
            .push(cid);
        let with = |set: &BTreeSet<String>, want: &[Label]| -> Vec<String> {
            set.iter()
                .filter(|c| want.contains(&labelling.labels[*c]))
                .cloned()
                .collect()
        };
        let mut unsettled = with(&deps.support, &[Label::Contested, Label::Unknown]);
        unsettled.extend(with(&deps.attackers, &[Label::Contested, Label::Unknown]));
        unsettled.sort();
        nodes.push(NodeLabel {
            cid: cid.clone(),
            label,
            output_cid: node.output_cid.as_str().to_string(),
            support_total: deps.support.len() + deps.support_missing.len(),
            support_out: with(&deps.support, &[Label::Out]),
            support_missing: deps.support_missing.iter().cloned().collect(),
            attackers: deps.attackers.iter().cloned().collect(),
            attackers_in: with(&deps.attackers, &[Label::In]),
            unsettled,
        });
    }
    let convergent_claims = by_output
        .into_iter()
        .filter(|(_, cids)| cids.len() > 1)
        .map(|(output, cids)| ClaimLabel {
            output_cid: output.to_string(),
            label: claim_label(cids.iter().map(|c| labelling.labels[*c])),
            nodes: cids.iter().map(|c| c.to_string()).collect(),
        })
        .collect();
    let mut roots: Vec<String> = roots.iter().map(|c| c.as_str().to_string()).collect();
    roots.sort();
    roots.dedup();
    LabelReport {
        schema: LABEL_SCHEMA,
        scope,
        roots,
        complete: missing.is_empty(),
        counts,
        missing: missing.into_iter().collect(),
        nodes,
        convergent_claims,
    }
}

/// Label the roots and everything they rest on.
pub fn label(dag: &Dag, roots: &[Cid]) -> Result<LabelReport, LabelError> {
    Ok(build_report(&World::closure(dag, roots)?, "closure", roots))
}

/// Label every DAG node in the store.
pub fn label_store(dag: &Dag) -> Result<LabelReport, LabelError> {
    Ok(build_report(&World::all(dag)?, "store", &[]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    A,
    B,
}

/// A node whose status in one view differs from its status in both together.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Flip {
    pub cid: String,
    /// The view whose status changed.
    pub side: Side,
    pub before: Label,
    pub after: Label,
    /// From this node, through dependencies whose status also differs from
    /// that view, to the first one that does not differ or that the view
    /// lacked. A witness for where the disagreement enters, not the only one.
    pub because: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsistencyReport {
    pub schema: &'static str,
    pub a_roots: Vec<String>,
    pub b_roots: Vec<String>,
    /// True when neither view changes any status in the other.
    pub consistent: bool,
    /// False when the combined view has a missing support parent.
    pub complete: bool,
    pub a_nodes: usize,
    pub b_nodes: usize,
    pub shared_nodes: usize,
    pub union_nodes: usize,
    pub flips: Vec<Flip>,
}

/// Follow the dependencies that explain `start`'s status in the union and
/// that the side saw differently (or did not have).
fn because(side: &Labelling, union: &Labelling, start: &str) -> Vec<String> {
    let changed = |cid: &str| side.label(cid) != union.label(cid);
    let mut path = vec![start.to_string()];
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut cur = start;
    seen.insert(cur);
    loop {
        let deps = &union.deps[cur];
        let label = union.labels[cur];
        let is = |cid: &String, want: Label| union.labels[cid] == want;
        // Dependencies that account for the label, attackers first.
        let explains: Vec<&String> = match label {
            Label::Out => deps
                .attackers
                .iter()
                .filter(|c| is(c, Label::In))
                .chain(deps.support.iter().filter(|c| is(c, Label::Out)))
                .collect(),
            Label::In => deps.attackers.iter().chain(deps.support.iter()).collect(),
            Label::Contested | Label::Unknown => deps
                .attackers
                .iter()
                .chain(deps.support.iter())
                .filter(|c| is(c, label))
                .collect(),
        };
        let Some(next) = explains
            .into_iter()
            .find(|c| changed(c) && !seen.contains(c.as_str()))
        else {
            return path;
        };
        path.push(next.clone());
        seen.insert(next);
        if !side.labels.contains_key(next) {
            // The view never had this node: this is where the other enters.
            return path;
        }
        cur = next;
    }
}

/// Whether two views agree: label each alone and both together, and report
/// every node whose status the combination changes.
pub fn consistency(dag: &Dag, a: &[Cid], b: &[Cid]) -> Result<ConsistencyReport, LabelError> {
    let world_a = World::closure(dag, a)?;
    let world_b = World::closure(dag, b)?;
    let world_u = world_a.union(&world_b);
    let label_a = Labelling::of(&world_a);
    let label_b = Labelling::of(&world_b);
    let label_u = Labelling::of(&world_u);

    let mut flips = Vec::new();
    for (side, labelling) in [(Side::A, &label_a), (Side::B, &label_b)] {
        for (cid, before) in &labelling.labels {
            let after = label_u.labels[cid];
            if *before != after {
                flips.push(Flip {
                    cid: cid.clone(),
                    side,
                    before: *before,
                    after,
                    because: because(labelling, &label_u, cid),
                });
            }
        }
    }
    flips.sort_by(|x, y| (&x.cid, x.side).cmp(&(&y.cid, y.side)));

    let roots = |cids: &[Cid]| {
        let mut v: Vec<String> = cids.iter().map(|c| c.as_str().to_string()).collect();
        v.sort();
        v.dedup();
        v
    };
    Ok(ConsistencyReport {
        schema: CONSISTENCY_SCHEMA,
        a_roots: roots(a),
        b_roots: roots(b),
        consistent: flips.is_empty(),
        complete: label_u.deps.values().all(|d| d.support_missing.is_empty()),
        a_nodes: world_a.len(),
        b_nodes: world_b.len(),
        shared_nodes: world_a.cids().filter(|c| world_b.contains(c)).count(),
        union_nodes: world_u.len(),
        flips,
    })
}

#[cfg(test)]
mod tests;
