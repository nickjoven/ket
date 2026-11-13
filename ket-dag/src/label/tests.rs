//! Canonical cases with labels written down by hand, then generated graphs
//! checked against a deliberately naive reference implementation.

use super::*;
use crate::NodeKind;
use ket_cas::Store as CasStore;
use std::fs;
use EdgeKind::*;
use Label::{Contested, In, Out, Unknown};

struct Fx {
    cas: CasStore,
    dir: std::path::PathBuf,
}

impl Fx {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ket-label-test-{name}"));
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
    fn label(&self, roots: &[&Cid]) -> LabelReport {
        let roots: Vec<Cid> = roots.iter().map(|c| (*c).clone()).collect();
        label(&self.dag(), &roots).unwrap()
    }
    fn consistency(&self, a: &[&Cid], b: &[&Cid]) -> ConsistencyReport {
        let a: Vec<Cid> = a.iter().map(|c| (*c).clone()).collect();
        let b: Vec<Cid> = b.iter().map(|c| (*c).clone()).collect();
        consistency(&self.dag(), &a, &b).unwrap()
    }
}

impl Drop for Fx {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// A CID that is in no store.
fn absent(tag: &str) -> Cid {
    ket_cas::hash_bytes(format!("never stored: {tag}").as_bytes())
}

fn assert_labels(report: &LabelReport, want: &[(&Cid, Label)]) {
    for (cid, label) in want {
        assert_eq!(
            report.label_of(cid),
            Some(*label),
            "label of {}",
            cid.as_str()
        );
    }
}

// --- Canonical cases -------------------------------------------------------

#[test]
fn an_unattacked_chain_is_in() {
    let fx = Fx::new("chain");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Grounds)]);
    let c = fx.node("c", &[(&b, Derives)]);
    let r = fx.label(&[&c]);
    assert_labels(&r, &[(&a, In), (&b, In), (&c, In)]);
    assert!(r.complete);
    assert_eq!(r.schema, LABEL_SCHEMA);
    assert_eq!(
        r.counts,
        LabelCounts {
            in_: 3,
            ..Default::default()
        }
    );
}

#[test]
fn a_refutation_defeats_its_target_and_what_rests_on_it() {
    let fx = Fx::new("refute");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Derives)]);
    let c = fx.node("c", &[(&b, Derives)]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let r = fx.label(&[&c, &r1]);
    assert_labels(&r, &[(&a, Out), (&b, Out), (&c, Out), (&r1, In)]);
    let na = r.nodes.iter().find(|n| n.cid == a.as_str()).unwrap();
    assert_eq!(na.attackers_in, vec![r1.0.clone()]);
    let nb = r.nodes.iter().find(|n| n.cid == b.as_str()).unwrap();
    assert_eq!(nb.support_out, vec![a.0.clone()]);
}

#[test]
fn a_refutation_outside_the_world_does_not_count() {
    let fx = Fx::new("outside");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Derives)]);
    let _r1 = fx.node("r1", &[(&a, Refutes)]);
    // The world of `b` alone does not contain the refutation.
    assert_labels(&fx.label(&[&b]), &[(&a, In), (&b, In)]);
    // The whole store does.
    let all = label_store(&fx.dag()).unwrap();
    assert_labels(&all, &[(&a, Out), (&b, Out)]);
    assert_eq!(all.scope, "store");
}

#[test]
fn a_refuted_refuter_reinstates_its_target() {
    let fx = Fx::new("reinstate");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Derives)]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let r2 = fx.node("r2", &[(&r1, Refutes)]);
    let r = fx.label(&[&b, &r2]);
    assert_labels(&r, &[(&a, In), (&b, In), (&r1, Out), (&r2, In)]);
}

#[test]
fn attack_chains_alternate() {
    let fx = Fx::new("alternate");
    let a = fx.node("a", &[]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let r2 = fx.node("r2", &[(&r1, Refutes)]);
    let r3 = fx.node("r3", &[(&r2, Refutes)]);
    let r = fx.label(&[&r3]);
    assert_labels(&r, &[(&a, Out), (&r1, In), (&r2, Out), (&r3, In)]);
}

#[test]
fn a_refuter_whose_own_ground_fell_defeats_nothing() {
    let fx = Fx::new("unsupported-refuter");
    let a = fx.node("a", &[]);
    let basis = fx.node("basis", &[]);
    let r1 = fx.node("r1", &[(&a, Refutes), (&basis, Grounds)]);
    let r2 = fx.node("r2", &[(&basis, Refutes)]);
    let r = fx.label(&[&r1, &r2]);
    assert_labels(&r, &[(&basis, Out), (&r1, Out), (&a, In), (&r2, In)]);
}

#[test]
fn premises_are_linked_losing_one_is_enough() {
    let fx = Fx::new("linked");
    let a = fx.node("a", &[]);
    let other = fx.node("other", &[]);
    let b = fx.node("b", &[(&a, Grounds), (&other, Grounds)]);
    let c = fx.node("c", &[(&b, Derives)]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let r = fx.label(&[&c, &r1]);
    assert_labels(&r, &[(&a, Out), (&other, In), (&b, Out), (&c, Out)]);

    // The same graph under ket.invalidated/v1: b is only weakened, c intact.
    use crate::propagate::{invalidated, CasChildIndex, PropagateOptions, Status};
    let index = CasChildIndex::build(&fx.cas).unwrap();
    let opts = PropagateOptions {
        include_intact: true,
        ..Default::default()
    };
    let v1 = invalidated(&fx.dag(), &index, &a, &opts).unwrap();
    assert_eq!(v1.status_of(&b), Some(Status::Weakened));
    assert_eq!(v1.status_of(&c), Some(Status::Intact));
}

#[test]
fn a_diamond_falls_through_either_arm() {
    let fx = Fx::new("diamond");
    let a = fx.node("a", &[]);
    let l = fx.node("l", &[(&a, Derives)]);
    let rr = fx.node("r", &[(&a, Derives)]);
    let side = fx.node("side", &[]);
    let d = fx.node("d", &[(&l, Derives), (&rr, Derives)]);
    let e = fx.node("e", &[(&rr, Derives), (&side, Grounds)]);
    let hit = fx.node("hit", &[(&l, Refutes)]);
    let r = fx.label(&[&d, &e, &hit]);
    assert_labels(&r, &[(&a, In), (&l, Out), (&rr, In), (&d, Out), (&e, In)]);
}

#[test]
fn a_claim_survives_while_another_derivation_of_it_stands() {
    let fx = Fx::new("convergent");
    let g1 = fx.node("ground one", &[]);
    let g2 = fx.node("ground two", &[]);
    // Same content, different grounds: two derivations of one claim.
    let p1 = fx.node("the claim", &[(&g1, Grounds)]);
    let p2 = fx.node("the claim", &[(&g2, Grounds)]);
    assert_ne!(p1, p2);
    let r1 = fx.node("r1", &[(&g1, Refutes)]);
    let r = fx.label(&[&p1, &p2, &r1]);
    assert_labels(&r, &[(&p1, Out), (&p2, In)]);
    assert_eq!(r.convergent_claims.len(), 1);
    let claim = &r.convergent_claims[0];
    assert_eq!(claim.label, In);
    let mut both = vec![p1.0.clone(), p2.0.clone()];
    both.sort();
    assert_eq!(claim.nodes, both);

    // Refute the other ground too and the claim itself is out.
    let r2 = fx.node("r2", &[(&g2, Refutes)]);
    let r = fx.label(&[&p1, &p2, &r1, &r2]);
    assert_eq!(r.convergent_claims[0].label, Out);
}

#[test]
fn an_indirect_standoff_is_contested_not_decided() {
    let fx = Fx::new("standoff");
    let n = fx.node("n", &[]);
    let p = fx.node("p", &[]);
    // "n fails, because p" against "p fails, because n".
    let r = fx.node("r", &[(&n, Refutes), (&p, Grounds)]);
    let r2 = fx.node("r2", &[(&p, Refutes), (&n, Grounds)]);
    let child = fx.node("child", &[(&n, Derives)]);
    let bystander = fx.node("bystander", &[]);
    let report = fx.label(&[&r, &r2, &child, &bystander]);
    assert_labels(
        &report,
        &[
            (&n, Contested),
            (&p, Contested),
            (&r, Contested),
            (&r2, Contested),
            (&child, Contested),
            (&bystander, In),
        ],
    );
    // No evidence is missing: the report is complete, and says who is unsettled.
    assert!(report.complete);
    let nn = report.nodes.iter().find(|x| x.cid == n.as_str()).unwrap();
    assert_eq!(nn.unsettled, vec![r.0.clone()]);
}

#[test]
fn a_third_party_breaks_a_standoff() {
    let fx = Fx::new("standoff-broken");
    let n = fx.node("n", &[]);
    let p = fx.node("p", &[]);
    let r = fx.node("r", &[(&n, Refutes), (&p, Grounds)]);
    let r2 = fx.node("r2", &[(&p, Refutes), (&n, Grounds)]);
    let x = fx.node("x", &[(&r2, Refutes)]);
    let report = fx.label(&[&r, &r2, &x]);
    assert_labels(
        &report,
        &[(&x, In), (&r2, Out), (&p, In), (&r, In), (&n, Out)],
    );
}

#[test]
fn a_missing_ground_is_unknown_and_spreads() {
    let fx = Fx::new("missing");
    let gone = absent("ground");
    let a = fx.node("a", &[(&gone, Grounds)]);
    let b = fx.node("b", &[(&a, Derives)]);
    let fine = fx.node("fine", &[]);
    let r = fx.label(&[&b, &fine]);
    assert_labels(&r, &[(&a, Unknown), (&b, Unknown), (&fine, In)]);
    assert!(!r.complete);
    assert_eq!(r.missing, vec![gone.0.clone()]);
    let na = r.nodes.iter().find(|n| n.cid == a.as_str()).unwrap();
    assert_eq!(na.support_missing, vec![gone.0.clone()]);
    assert_eq!(na.support_total, 1);
}

#[test]
fn a_raw_blob_is_not_a_node_so_support_through_it_is_unknown() {
    let fx = Fx::new("raw-parent");
    let raw = fx.cas.put(b"just bytes, not a node").unwrap();
    let a = fx.node("a", &[(&raw, Grounds)]);
    let r = fx.label(&[&a]);
    assert_labels(&r, &[(&a, Unknown)]);
    assert_eq!(r.missing, vec![raw.0.clone()]);
}

#[test]
fn a_definite_defeat_outranks_an_evidence_gap() {
    let fx = Fx::new("gap-and-defeat");
    let gone = absent("ground");
    let a = fx.node("a", &[(&gone, Grounds)]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let r = fx.label(&[&r1]);
    // Whatever the missing ground would have been, a is defeated.
    assert_labels(&r, &[(&a, Out), (&r1, In)]);
    assert!(!r.complete);
}

#[test]
fn an_attacker_of_unknown_standing_leaves_its_target_unknown() {
    let fx = Fx::new("unknown-attacker");
    let gone = absent("ground");
    let a = fx.node("a", &[]);
    let r1 = fx.node("r1", &[(&a, Refutes), (&gone, Grounds)]);
    let r = fx.label(&[&r1]);
    assert_labels(&r, &[(&r1, Unknown), (&a, Unknown)]);
}

#[test]
fn proposes_confirms_and_supersedes_are_neither_support_nor_attack() {
    let fx = Fx::new("other-kinds");
    let a = fx.node("a", &[]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let hypo = fx.node("hypothesis", &[(&a, Proposes)]);
    let conf = fx.node("confirmation", &[(&a, Confirms)]);
    let newer = fx.node("replacement", &[(&a, Supersedes)]);
    let gone = absent("proposed from");
    let loose = fx.node("loose", &[(&gone, Proposes)]);
    let r = fx.label(&[&r1, &hypo, &conf, &newer, &loose]);
    assert_labels(
        &r,
        &[
            (&a, Out),
            (&hypo, In),
            (&conf, In),
            (&newer, In),
            (&loose, In),
        ],
    );
    // A missing non-support parent is not an evidence gap.
    assert!(r.complete);
}

#[test]
fn a_world_is_the_roots_and_all_their_ancestors() {
    let fx = Fx::new("world");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Derives)]);
    let r1 = fx.node("r1", &[(&b, Refutes)]);
    let _unrelated = fx.node("unrelated", &[]);
    let world = World::closure(&fx.dag(), std::slice::from_ref(&r1)).unwrap();
    // The refuting root brings in what it refutes, and that node's ancestors.
    let mut want = vec![a.0.clone(), b.0.clone(), r1.0.clone()];
    want.sort();
    assert_eq!(world.cids().collect::<Vec<_>>(), want);
    assert_eq!(World::all(&fx.dag()).unwrap().len(), 4);
}

#[test]
fn a_root_that_is_not_a_node_is_an_error() {
    let fx = Fx::new("bad-root");
    let raw = fx.cas.put(b"raw").unwrap();
    for bad in [absent("root"), raw] {
        let err = label(&fx.dag(), std::slice::from_ref(&bad)).unwrap_err();
        assert!(matches!(err, LabelError::RootNotFound(_)), "{err}");
    }
}

#[test]
fn tampered_bytes_are_an_error_not_a_gap() {
    let fx = Fx::new("tampered");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Derives)]);
    // Overwrite a's blob in place with different bytes.
    let mut hit = false;
    let mut stack = vec![fx.dir.clone()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .to_string_lossy()
                .replace('/', "")
                .ends_with(a.as_str())
            {
                let mut perms = fs::metadata(&path).unwrap().permissions();
                #[allow(clippy::permissions_set_readonly_false)]
                perms.set_readonly(false);
                fs::set_permissions(&path, perms).unwrap();
                fs::write(&path, b"{\"not\":\"what was hashed\"}").unwrap();
                hit = true;
            }
        }
    }
    assert!(hit, "did not find a's blob on disk");
    let err = label(&fx.dag(), std::slice::from_ref(&b)).unwrap_err();
    assert!(
        matches!(
            err,
            LabelError::Dag(DagError::Cas(ket_cas::CasError::IntegrityError { .. }))
        ),
        "{err}"
    );
}

// --- Consistency -----------------------------------------------------------

#[test]
fn views_that_only_add_to_each_other_are_consistent() {
    let fx = Fx::new("consistent");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Derives)]);
    let c = fx.node("c", &[(&a, Derives)]);
    let r = fx.consistency(&[&b], &[&c]);
    assert!(r.consistent);
    assert!(r.flips.is_empty());
    assert_eq!((r.a_nodes, r.b_nodes), (2, 2));
    assert_eq!((r.shared_nodes, r.union_nodes), (1, 3));
    assert_eq!(r.schema, CONSISTENCY_SCHEMA);
}

#[test]
fn a_refutation_held_by_one_view_flips_the_other() {
    let fx = Fx::new("inconsistent");
    let a = fx.node("a", &[]);
    let b = fx.node("b", &[(&a, Derives)]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let r = fx.consistency(&[&b], &[&r1]);
    assert!(!r.consistent);
    let mut want = vec![
        Flip {
            cid: a.0.clone(),
            side: Side::A,
            before: In,
            after: Out,
            because: vec![a.0.clone(), r1.0.clone()],
        },
        Flip {
            cid: b.0.clone(),
            side: Side::A,
            before: In,
            after: Out,
            because: vec![b.0.clone(), a.0.clone(), r1.0.clone()],
        },
    ];
    want.sort_by(|x, y| x.cid.cmp(&y.cid));
    assert_eq!(r.flips, want);
}

#[test]
fn reinstatement_across_views_is_a_disagreement_too() {
    let fx = Fx::new("reinstate-across");
    let a = fx.node("a", &[]);
    let r1 = fx.node("r1", &[(&a, Refutes)]);
    let r2 = fx.node("r2", &[(&r1, Refutes)]);
    // A holds the refutation; B holds the refutation of the refutation.
    let r = fx.consistency(&[&r1], &[&r2]);
    assert!(!r.consistent);
    let flip_a = r
        .flips
        .iter()
        .find(|f| f.cid == a.as_str() && f.side == Side::A)
        .unwrap();
    assert_eq!((flip_a.before, flip_a.after), (Out, In));
    assert_eq!(
        flip_a.because,
        vec![a.0.clone(), r1.0.clone(), r2.0.clone()]
    );
    let flip_r1 = r
        .flips
        .iter()
        .find(|f| f.cid == r1.as_str() && f.side == Side::A)
        .unwrap();
    assert_eq!((flip_r1.before, flip_r1.after), (In, Out));
    // B already had r1 out and a in: nothing in B changes.
    assert!(r.flips.iter().all(|f| f.side == Side::A));
}

#[test]
fn two_views_that_each_refute_the_others_ground_end_contested() {
    let fx = Fx::new("contested-across");
    let n = fx.node("n", &[]);
    let p = fx.node("p", &[]);
    let r = fx.node("r", &[(&n, Refutes), (&p, Grounds)]);
    let r2 = fx.node("r2", &[(&p, Refutes), (&n, Grounds)]);
    let report = fx.consistency(&[&r], &[&r2]);
    assert!(!report.consistent);
    assert!(report.complete);
    for f in &report.flips {
        assert_eq!(f.after, Contested, "{f:?}");
    }
    // Alone, each view is decided: its refuter in, its target out.
    let alone = fx.label(&[&r]);
    assert_labels(&alone, &[(&r, In), (&n, Out), (&p, In)]);
}

// --- Generated graphs against a naive oracle -------------------------------

/// splitmix64: a fixed, dependency-free stream per seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A random typed DAG in `fx`: every node's CID, in creation order.
fn generate(fx: &Fx, seed: u64) -> Vec<Cid> {
    let mut rng = Rng(seed);
    let count = 4 + rng.below(11) as usize;
    let mut cids: Vec<Cid> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    for i in 0..count {
        let mut picked: Vec<usize> = Vec::new();
        if !cids.is_empty() {
            for _ in 0..rng.below(4) {
                let j = rng.below(cids.len() as u64) as usize;
                if !picked.contains(&j) {
                    picked.push(j);
                }
            }
        }
        let mut parents: Vec<(Cid, EdgeKind)> = picked
            .iter()
            .map(|j| {
                let kind = match rng.below(100) {
                    0..=34 => Derives,
                    35..=54 => Grounds,
                    55..=84 => Refutes,
                    85..=89 => Proposes,
                    90..=94 => Confirms,
                    _ => Supersedes,
                };
                (cids[*j].clone(), kind)
            })
            .collect();
        if rng.below(100) < 6 {
            parents.push((absent(&format!("{seed}-{i}")), Grounds));
        }
        // Now and then reuse earlier content: a second derivation of a claim.
        let label = if !labels.is_empty() && rng.below(100) < 10 {
            labels[rng.below(labels.len() as u64) as usize].clone()
        } else {
            format!("g{seed}-n{i}")
        };
        let refs: Vec<(&Cid, EdgeKind)> = parents.iter().map(|(c, k)| (c, *k)).collect();
        let cid = fx.node(&label, &refs);
        if !cids.contains(&cid) {
            cids.push(cid);
            labels.push(label);
        }
    }
    cids
}

/// The rules as written, by repeated full sweeps. Reads edges straight from
/// the nodes; shares nothing with `Labelling::of` beyond the `World`.
fn oracle(world: &World) -> BTreeMap<String, Label> {
    let support = |cid: &str| -> Vec<String> {
        world.nodes[cid]
            .parent_links()
            .filter(|(_, k)| matches!(k, Grounds | Derives))
            .map(|(p, _)| p.as_str().to_string())
            .collect()
    };
    let attackers = |cid: &str| -> Vec<String> {
        world
            .nodes
            .iter()
            .filter(|(_, node)| {
                node.parent_links()
                    .any(|(p, k)| k == Refutes && p.as_str() == cid)
            })
            .map(|(c, _)| c.clone())
            .collect()
    };
    let mut labels: BTreeMap<String, Label> = BTreeMap::new();
    loop {
        let mut changed = false;
        for cid in world.cids() {
            if labels.contains_key(cid) {
                continue;
            }
            let (sup, att) = (support(cid), attackers(cid));
            let got = |c: &String| labels.get(c).copied();
            let new = if att.iter().any(|c| got(c) == Some(In))
                || sup.iter().any(|c| got(c) == Some(Out))
            {
                Some(Out)
            } else if sup.iter().all(|c| got(c) == Some(In))
                && att.iter().all(|c| got(c) == Some(Out))
            {
                Some(In)
            } else {
                None
            };
            if let Some(l) = new {
                labels.insert(cid.to_string(), l);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    loop {
        let mut changed = false;
        for cid in world.cids() {
            if labels.contains_key(cid) {
                continue;
            }
            let gap = support(cid)
                .iter()
                .any(|c| !world.contains(c) || labels.get(c) == Some(&Unknown))
                || attackers(cid)
                    .iter()
                    .any(|c| labels.get(c) == Some(&Unknown));
            if gap {
                labels.insert(cid.to_string(), Unknown);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    for cid in world.cids() {
        labels.entry(cid.to_string()).or_insert(Contested);
    }
    labels
}

const SEEDS: std::ops::Range<u64> = 0..400;

#[test]
fn generated_graphs_match_the_oracle_and_cover_every_label() {
    let mut seen: BTreeMap<Label, usize> = BTreeMap::new();
    let mut reinstated = 0;
    for seed in SEEDS {
        let fx = Fx::new(&format!("gen-{seed}"));
        let cids = generate(&fx, seed);
        let world = World::all(&fx.dag()).unwrap();
        assert_eq!(world.len(), cids.len(), "seed {seed}");
        let labelling = Labelling::of(&world);
        assert_eq!(labelling.labels, oracle(&world), "seed {seed}");

        for (cid, label) in &labelling.labels {
            *seen.entry(*label).or_default() += 1;
            let deps = &labelling.deps[cid];
            let dep_labels = || {
                deps.support
                    .iter()
                    .chain(deps.attackers.iter())
                    .map(|c| labelling.labels[c])
            };
            match label {
                // Contested only on a standoff: some dependency is contested too,
                // so following them never ends, which a finite DAG of nodes can
                // only do by going round a cycle of support and attack.
                Contested => {
                    assert!(deps.support_missing.is_empty(), "seed {seed}");
                    assert!(dep_labels().any(|l| l == Contested), "seed {seed}");
                    assert!(dep_labels().all(|l| l != Unknown), "seed {seed}");
                }
                // Unknown always traces to an evidence gap.
                Unknown => assert!(
                    !deps.support_missing.is_empty() || dep_labels().any(|l| l == Unknown),
                    "seed {seed}"
                ),
                In => {
                    if deps.attackers.iter().any(|c| labelling.labels[c] == Out) {
                        reinstated += 1;
                    }
                }
                Out => {}
            }
        }

        // The whole store, the closure of every node, and the roots in another
        // order are the same world and give the same report.
        let report = label_store(&fx.dag()).unwrap();
        let mut shuffled = cids.clone();
        shuffled.reverse();
        let by_roots = label(&fx.dag(), &shuffled).unwrap();
        assert_eq!(
            serde_json::to_value(&report.nodes).unwrap(),
            serde_json::to_value(&by_roots.nodes).unwrap(),
            "seed {seed}"
        );
    }
    // The generator has to reach every rule, or agreement above means little.
    for label in [In, Out, Contested, Unknown] {
        assert!(
            seen.get(&label).copied().unwrap_or(0) >= 20,
            "too few {label:?}: {seen:?}"
        );
    }
    assert!(reinstated >= 20, "too few reinstatements: {reinstated}");
}

#[test]
fn without_attacks_or_gaps_everything_is_in() {
    for seed in SEEDS.step_by(7) {
        let fx = Fx::new(&format!("plain-{seed}"));
        generate(&fx, seed);
        let world = World::all(&fx.dag()).unwrap();
        let labelling = Labelling::of(&world);
        let plain = labelling
            .deps
            .values()
            .all(|d| d.attackers.is_empty() && d.support_missing.is_empty());
        if plain {
            assert!(labelling.labels.values().all(|l| *l == In), "seed {seed}");
        }
    }
}

#[test]
fn consistency_is_symmetric_and_matches_its_definition() {
    let mut inconsistent = 0;
    let mut consistent = 0;
    for seed in SEEDS {
        let fx = Fx::new(&format!("sym-{seed}"));
        let cids = generate(&fx, seed);
        let mut rng = Rng(seed ^ 0xA5A5);
        let (mut a, mut b): (Vec<Cid>, Vec<Cid>) = (Vec::new(), Vec::new());
        for cid in &cids {
            match rng.below(3) {
                0 => a.push(cid.clone()),
                1 => b.push(cid.clone()),
                _ => {}
            }
        }
        if a.is_empty() || b.is_empty() {
            continue;
        }
        let ab = consistency(&fx.dag(), &a, &b).unwrap();
        let ba = consistency(&fx.dag(), &b, &a).unwrap();
        assert_eq!(ab.consistent, ba.consistent, "seed {seed}");
        let key = |f: &Flip, swap: bool| {
            let side = match (f.side, swap) {
                (Side::A, false) | (Side::B, true) => Side::A,
                _ => Side::B,
            };
            (f.cid.clone(), side, f.before, f.after)
        };
        let mut left: Vec<_> = ab.flips.iter().map(|f| key(f, false)).collect();
        let mut right: Vec<_> = ba.flips.iter().map(|f| key(f, true)).collect();
        left.sort();
        right.sort();
        assert_eq!(left, right, "seed {seed}");

        // The definition, recomputed with the oracle.
        let wa = World::closure(&fx.dag(), &a).unwrap();
        let wb = World::closure(&fx.dag(), &b).unwrap();
        let (oa, ob, ou) = (oracle(&wa), oracle(&wb), oracle(&wa.union(&wb)));
        let mut want: Vec<_> = oa
            .iter()
            .map(|(c, l)| (c, Side::A, l))
            .chain(ob.iter().map(|(c, l)| (c, Side::B, l)))
            .filter(|(c, _, l)| ou[*c] != **l)
            .map(|(c, side, l)| (c.clone(), side, *l, ou[c]))
            .collect();
        want.sort();
        assert_eq!(left, want, "seed {seed}");

        // Every witness starts at the flipped node, never repeats, and walks
        // only through nodes of the combined view.
        for f in &ab.flips {
            assert_eq!(f.because[0], f.cid, "seed {seed}");
            let distinct: BTreeSet<&String> = f.because.iter().collect();
            assert_eq!(distinct.len(), f.because.len(), "seed {seed}");
            assert!(f.because.iter().all(|c| ou.contains_key(c)), "seed {seed}");
        }
        if ab.consistent {
            consistent += 1;
        } else {
            inconsistent += 1;
        }
    }
    assert!(
        consistent >= 20 && inconsistent >= 20,
        "{consistent} consistent, {inconsistent} inconsistent"
    );
}
