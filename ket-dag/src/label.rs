//! Labelling: which nodes stand, which fall, and whether two views agree.
//!
//! `propagate` answers "what if this node fell" from one seed. This module
//! answers the whole-graph question: given a set of nodes (a *world*), give
//! every node a status from the support and attack edges inside that world.
//!
//! * **Support** is a `grounds` or `derives` parent.
//! * **Attack** is a `refutes` link from another node in the world.
//! * `proposes`, `confirms` and `supersedes` are neither, and are ignored.
//!
//! How much support a node needs, and whose attacks count, are not sealed
//! into nodes: they are a [`Policy`], an input to the labelling that the
//! report records with its content address. The same graph can be read under
//! different policies and the reports say which one each used.
//!
//! * `support: all` (the default) — support parents are *linked*: the node
//!   needs every one of them. Right when the edges are real premises.
//! * `support: any` — support parents are *alternatives*: one standing
//!   parent is enough, and the node falls only when all have fallen. The
//!   conservative reading for citation graphs.
//! * `ignore_attacks_by` — `refutes` links from nodes by these agents are not
//!   attacks (listed in the report as `attackers_ignored`).
//!
//! The rules are the grounded semantics of argumentation theory, extended
//! with support:
//!
//! * `out` — some attacker is `in`, or the node's support has fallen: under
//!   `all`, some support parent is `out`; under `any`, it has support and
//!   every support parent is `out`.
//! * `in` — every attacker is `out`, and the node's support stands: under
//!   `all`, every support parent is `in`; under `any`, it has no support or
//!   some support parent is `in`.
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
//! Relation to `ket.invalidated/v1`, which is unchanged: v1 marks a node
//! invalidated only when all its support is lost, which is `support: any`
//! here. Where a single refutation is the only attack, the two agree on what
//! falls (tested on generated graphs). v1 has no reinstatement and consults
//! recorded refutations only inside the seed's downstream closure.
//!
//! The `all` rule over-claims on citation graphs: every paper citing one
//! retracted paper among many comes out `out`. Use `support: any` there.
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
use serde::{Deserialize, Serialize};

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

pub const POLICY_SCHEMA: &str = "ket.label-policy/v1";

/// How much support a node needs to stand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SupportRule {
    /// Every support parent: they are linked premises.
    #[default]
    All,
    /// At least one support parent: they are alternatives.
    Any,
}

impl SupportRule {
    pub fn parse(s: &str) -> Option<SupportRule> {
        match s {
            "all" => Some(SupportRule::All),
            "any" => Some(SupportRule::Any),
            _ => None,
        }
    }
}

/// The reading rules a labelling is computed under. Not part of any node:
/// recorded in each report with the content address of its canonical bytes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Policy {
    pub support: SupportRule,
    /// Agents whose `refutes` links are not counted as attacks.
    pub ignore_attacks_by: BTreeSet<String>,
}

#[derive(Serialize)]
struct PolicyBytes<'a> {
    schema: &'static str,
    support: SupportRule,
    ignore_attacks_by: &'a BTreeSet<String>,
}

impl Policy {
    /// Canonical bytes: fixed field order, sorted agent set, no whitespace.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&PolicyBytes {
            schema: POLICY_SCHEMA,
            support: self.support,
            ignore_attacks_by: &self.ignore_attacks_by,
        })
        .expect("a policy always serializes")
    }

    /// The CID the policy's canonical bytes would have in CAS.
    pub fn cid(&self) -> Cid {
        ket_cas::hash_bytes(&self.to_bytes())
    }
}

/// A policy as it appears in a report.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyRecord {
    pub cid: String,
    pub schema: &'static str,
    pub support: SupportRule,
    pub ignore_attacks_by: Vec<String>,
}

impl From<&Policy> for PolicyRecord {
    fn from(p: &Policy) -> Self {
        PolicyRecord {
            cid: p.cid().as_str().to_string(),
            schema: POLICY_SCHEMA,
            support: p.support,
            ignore_attacks_by: p.ignore_attacks_by.iter().cloned().collect(),
        }
    }
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
    /// Nodes in the world with a `refutes` link here that count as attacks.
    attackers: BTreeSet<String>,
    /// Nodes with a `refutes` link here whose agent the policy ignores.
    attackers_ignored: BTreeSet<String>,
}

/// The status of every node in a world.
#[derive(Debug, Clone)]
pub struct Labelling {
    labels: BTreeMap<String, Label>,
    deps: BTreeMap<String, Deps>,
}

impl Labelling {
    pub fn of(world: &World, policy: &Policy) -> Labelling {
        let mut deps: BTreeMap<String, Deps> = world
            .nodes
            .keys()
            .map(|cid| (cid.clone(), Deps::default()))
            .collect();
        // Reverse edges: who each node supports, and who it attacks.
        let mut supports: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        let mut attacks: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (cid, node) in &world.nodes {
            let ignored = policy.ignore_attacks_by.contains(&node.agent);
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
                        let target = deps.get_mut(key).unwrap();
                        if ignored {
                            target.attackers_ignored.insert(cid.clone());
                        } else if target.attackers.insert(cid.clone()) {
                            attacks.entry(cid.as_str()).or_default().push(key);
                        }
                    }
                }
            }
        }

        // Least fixed point of the in/out rules, by counting. A label, once
        // given, is final: the rules are monotone and their conditions for in
        // and for out cannot both hold. A missing support parent is never in
        // and never out.
        #[derive(Default, Clone, Copy)]
        struct Tally {
            support_in: usize,
            support_out: usize,
            attackers_out: usize,
            attacker_in: bool,
        }
        let decide = |d: &Deps, t: &Tally| -> Option<Label> {
            let total = d.support.len() + d.support_missing.len();
            let fallen = match policy.support {
                SupportRule::All => t.support_out > 0,
                SupportRule::Any => total > 0 && t.support_out == total,
            };
            let stands = match policy.support {
                SupportRule::All => t.support_in == total,
                SupportRule::Any => total == 0 || t.support_in > 0,
            };
            if t.attacker_in || fallen {
                Some(Label::Out)
            } else if stands && t.attackers_out == d.attackers.len() {
                Some(Label::In)
            } else {
                None
            }
        };
        let mut tally: BTreeMap<&str, Tally> = deps
            .keys()
            .map(|c| (c.as_str(), Tally::default()))
            .collect();
        let mut labels: BTreeMap<String, Label> = BTreeMap::new();
        let mut queue: VecDeque<&str> = VecDeque::new();
        for (cid, d) in &deps {
            if let Some(label) = decide(d, &tally[cid.as_str()]) {
                labels.insert(cid.clone(), label);
                queue.push_back(cid);
            }
        }
        while let Some(cid) = queue.pop_front() {
            let label = labels[cid];
            for target in supports.get(cid).into_iter().flatten() {
                let t = tally.get_mut(target).unwrap();
                match label {
                    Label::In => t.support_in += 1,
                    Label::Out => t.support_out += 1,
                    Label::Contested | Label::Unknown => unreachable!("only in/out are queued"),
                }
            }
            for target in attacks.get(cid).into_iter().flatten() {
                let t = tally.get_mut(target).unwrap();
                match label {
                    Label::In => t.attacker_in = true,
                    Label::Out => t.attackers_out += 1,
                    Label::Contested | Label::Unknown => unreachable!("only in/out are queued"),
                }
            }
            let touched = supports
                .get(cid)
                .into_iter()
                .flatten()
                .chain(attacks.get(cid).into_iter().flatten());
            for target in touched {
                if labels.contains_key(*target) {
                    continue;
                }
                if let Some(next) = decide(&deps[*target], &tally[target]) {
                    labels.insert(target.to_string(), next);
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
    /// Nodes with a `refutes` link here that the policy does not count.
    pub attackers_ignored: Vec<String>,
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
    pub policy: PolicyRecord,
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

fn build_report(world: &World, scope: &'static str, roots: &[Cid], policy: &Policy) -> LabelReport {
    let labelling = Labelling::of(world, policy);
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
            attackers_ignored: deps.attackers_ignored.iter().cloned().collect(),
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
        policy: policy.into(),
        complete: missing.is_empty(),
        counts,
        missing: missing.into_iter().collect(),
        nodes,
        convergent_claims,
    }
}

/// Label the roots and everything they rest on.
pub fn label(dag: &Dag, roots: &[Cid], policy: &Policy) -> Result<LabelReport, LabelError> {
    Ok(build_report(
        &World::closure(dag, roots)?,
        "closure",
        roots,
        policy,
    ))
}

/// Label every DAG node in the store.
pub fn label_store(dag: &Dag, policy: &Policy) -> Result<LabelReport, LabelError> {
    Ok(build_report(&World::all(dag)?, "store", &[], policy))
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
    /// that view, to a node that view did not have: where the other view
    /// enters. A shortest such chain, not the only one.
    pub because: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsistencyReport {
    pub schema: &'static str,
    pub a_roots: Vec<String>,
    pub b_roots: Vec<String>,
    pub policy: PolicyRecord,
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

/// A shortest chain of dependencies from `start` to a node the side did not
/// have, through nodes whose status the side saw differently.
///
/// Breadth-first, so a view that was already in a standoff cannot trap the
/// walk on its old cycle: the search goes round it and out to the new node.
/// Dependencies that account for the current status (an `in` attacker or an
/// `out` support parent for `out`; a same-status dependency for `contested`
/// and `unknown`) are tried alone first; if they do not lead out, any
/// dependency with a changed status may be used.
fn because(side: &Labelling, union: &Labelling, start: &str) -> Vec<String> {
    let changed = |cid: &str| side.label(cid) != union.label(cid);
    let search = |explaining_only: bool| -> Option<Vec<String>> {
        let mut from: BTreeMap<&str, &str> = BTreeMap::new();
        let mut queue: VecDeque<&str> = VecDeque::from([start]);
        while let Some(cur) = queue.pop_front() {
            if !side.labels.contains_key(cur) {
                // The view never had this node: this is where the other enters.
                let mut path = vec![cur.to_string()];
                let mut at = cur;
                while let Some(prev) = from.get(at) {
                    path.push(prev.to_string());
                    at = prev;
                }
                path.reverse();
                return Some(path);
            }
            let deps = &union.deps[cur];
            let label = union.labels[cur];
            for (dep, attacker) in deps
                .attackers
                .iter()
                .map(|c| (c, true))
                .chain(deps.support.iter().map(|c| (c, false)))
            {
                let dep_label = union.labels[dep];
                let explains = match label {
                    Label::Out => dep_label == if attacker { Label::In } else { Label::Out },
                    Label::In => true,
                    Label::Contested | Label::Unknown => dep_label == label,
                };
                if (explains || !explaining_only)
                    && changed(dep)
                    && dep != start
                    && !from.contains_key(dep.as_str())
                {
                    from.insert(dep, cur);
                    queue.push_back(dep);
                }
            }
        }
        None
    };
    search(true)
        .or_else(|| search(false))
        .unwrap_or_else(|| vec![start.to_string()])
}

/// Whether two views agree: label each alone and both together, and report
/// every node whose status the combination changes.
pub fn consistency(
    dag: &Dag,
    a: &[Cid],
    b: &[Cid],
    policy: &Policy,
) -> Result<ConsistencyReport, LabelError> {
    let world_a = World::closure(dag, a)?;
    let world_b = World::closure(dag, b)?;
    let world_u = world_a.union(&world_b);
    let label_a = Labelling::of(&world_a, policy);
    let label_b = Labelling::of(&world_b, policy);
    let label_u = Labelling::of(&world_u, policy);

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
        policy: policy.into(),
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
