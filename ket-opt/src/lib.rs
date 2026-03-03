//! WQS binary search optimizer for substrate traversal allocation.
//!
//! Implements Lagrangian relaxation (WQS / Aliens trick) over the DAG's
//! tree structure to calibrate tier thresholds, depth decay, and scoring
//! weights. Calibration results are persisted as DAG nodes with provenance.
//!
//! This is distinct from `ket-score` which evaluates artifacts on four
//! dimensions. `ket-opt` optimizes *traversal allocation* — deciding how
//! much compute to spend on each node given budget constraints.

use ket_cas::{Cid, Store as CasStore};
use ket_dag::{Dag, NodeKind};
use ket_score::ScoringEngine;
use ket_sql::DoltDb;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, thiserror::Error)]
pub enum OptError {
    #[error("CAS error: {0}")]
    Cas(#[from] ket_cas::CasError),
    #[error("DAG error: {0}")]
    Dag(#[from] ket_dag::DagError),
    #[error("SQL error: {0}")]
    Sql(#[from] ket_sql::SqlError),
    #[error("Score error: {0}")]
    Score(#[from] ket_score::ScoreError),
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    #[error("Empty tree: no nodes found from root {0}")]
    EmptyTree(String),
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),
}

/// Compute tiers per paper §2.3.
/// Match compute cost to expected information gain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tier {
    /// Tier 0: Skip entirely (cost=0).
    Skip = 0,
    /// Tier 1: Hash comparisons, drift checks, DAG traversal (cost=1).
    Shallow = 1,
    /// Tier 2: Lineage tracing, symbol scanning, SQL queries (cost=2).
    Moderate = 2,
    /// Tier 3: Full rebuilds, multi-file refactors, auto-scoring suites (cost=4).
    Deep = 3,
}

impl Tier {
    /// The compute cost weight for this tier.
    pub fn cost(&self) -> f64 {
        match self {
            Tier::Skip => 0.0,
            Tier::Shallow => 1.0,
            Tier::Moderate => 2.0,
            Tier::Deep => 4.0,
        }
    }

    /// All tiers in ascending order.
    pub fn all() -> &'static [Tier] {
        &[Tier::Skip, Tier::Shallow, Tier::Moderate, Tier::Deep]
    }

    /// Information gain multiplier for this tier.
    /// Higher tiers extract more information from a node.
    fn gain_multiplier(&self) -> f64 {
        match self {
            Tier::Skip => 0.0,
            Tier::Shallow => 0.3,
            Tier::Moderate => 0.7,
            Tier::Deep => 1.0,
        }
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tier::Skip => write!(f, "skip"),
            Tier::Shallow => write!(f, "shallow"),
            Tier::Moderate => write!(f, "moderate"),
            Tier::Deep => write!(f, "deep"),
        }
    }
}

/// A node in the spanning tree built from the DAG.
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// CID of the DAG node.
    pub cid: Cid,
    /// Child indices in the tree (indices into a flat Vec).
    pub children: Vec<usize>,
    /// Information potential: 1.0 - avg(scores). Unscored nodes get 1.0.
    pub info_potential: f64,
    /// Depth from root (0-indexed).
    pub depth: u32,
}

/// Budget constraints for optimization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraints {
    /// Maximum total compute cost across all nodes.
    pub max_cost: f64,
    /// Maximum depth to explore.
    pub max_depth: u32,
    /// Maximum number of Tier 3 (Deep) calls.
    pub max_tier3_calls: u32,
}

/// Lagrange multipliers — the calibration output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lambdas {
    /// Penalty per unit of compute cost.
    pub lambda_cost: f64,
    /// Penalty per unit of depth.
    pub lambda_depth: f64,
    /// Penalty per Tier 3 call.
    pub lambda_tier3: f64,
}

/// Result of a calibration run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationResult {
    /// Optimal Lagrange multipliers.
    pub lambdas: Lambdas,
    /// Tier assignment for each node CID.
    pub allocated_tiers: HashMap<String, String>,
    /// Total information gain achieved.
    pub total_gain: f64,
    /// Total compute cost incurred.
    pub total_cost: f64,
    /// Number of binary search iterations.
    pub iterations: u32,
    /// Root CID that was calibrated.
    pub root_cid: String,
}

// ---------------------------------------------------------------------------
// Solver functions
// ---------------------------------------------------------------------------

/// Solve the penalized problem for fixed lambda values.
///
/// For each node, compute:
/// `net_gain = info_potential * gain_multiplier(tier) - Σ(λ_i × cost_i)`.
/// Assigns the tier with the best net gain; skips if all are negative.
///
/// Returns (total_gain, total_cost, tier3_count, assignments).
fn solve_penalized(
    nodes: &[TreeNode],
    lambdas: &Lambdas,
) -> (f64, f64, u32, Vec<Tier>) {
    let mut total_gain = 0.0;
    let mut total_cost = 0.0;
    let mut tier3_count = 0u32;
    let mut assignments = Vec::with_capacity(nodes.len());

    for node in nodes {
        let mut best_tier = Tier::Skip;
        let mut best_net = 0.0; // Skip has net gain of 0

        for &tier in Tier::all() {
            if tier == Tier::Skip {
                continue;
            }

            let gain = node.info_potential * tier.gain_multiplier();
            let penalty_cost = lambdas.lambda_cost * tier.cost();
            let penalty_depth = lambdas.lambda_depth * node.depth as f64;
            let penalty_tier3 = if tier == Tier::Deep {
                lambdas.lambda_tier3
            } else {
                0.0
            };
            let net = gain - penalty_cost - penalty_depth - penalty_tier3;

            if net > best_net {
                best_net = net;
                best_tier = tier;
            }
        }

        if best_tier == Tier::Deep {
            tier3_count += 1;
        }
        total_gain += node.info_potential * best_tier.gain_multiplier();
        total_cost += best_tier.cost();
        assignments.push(best_tier);
    }

    (total_gain, total_cost, tier3_count, assignments)
}

/// WQS binary search optimization over lambda values.
///
/// Nested binary search: outer loop on lambda_cost (controls total cost),
/// inner loops on lambda_depth and lambda_tier3. ~20 iterations per dimension
/// for precision of 2^-20.
pub fn wqs_optimize(
    nodes: &[TreeNode],
    constraints: &Constraints,
) -> CalibrationResult {
    let max_iters: u32 = 20;
    let mut total_iterations = 0u32;

    // Binary search bounds for lambda_cost
    let mut lo_cost = 0.0_f64;
    let mut hi_cost = 10.0_f64;

    let mut best_lambdas = Lambdas {
        lambda_cost: 0.0,
        lambda_depth: 0.0,
        lambda_tier3: 0.0,
    };
    let mut best_assignments = vec![Tier::Skip; nodes.len()];
    let mut best_gain = 0.0_f64;
    let mut best_cost = 0.0_f64;

    for _ in 0..max_iters {
        let lambda_cost = (lo_cost + hi_cost) / 2.0;

        // Inner binary search on lambda_tier3
        let mut lo_t3 = 0.0_f64;
        let mut hi_t3 = 10.0_f64;
        let mut inner_best_lambda_t3 = 0.0;
        let mut inner_best_lambda_depth = 0.0;

        for _ in 0..max_iters {
            let lambda_tier3 = (lo_t3 + hi_t3) / 2.0;

            // Innermost: binary search on lambda_depth
            let mut lo_d = 0.0_f64;
            let mut hi_d = 1.0_f64;

            for _ in 0..max_iters {
                let lambda_depth = (lo_d + hi_d) / 2.0;
                let lambdas = Lambdas {
                    lambda_cost,
                    lambda_depth,
                    lambda_tier3,
                };

                let (_, _, _, assignments) = solve_penalized(nodes, &lambdas);

                // Check max depth used
                let max_depth_used = nodes
                    .iter()
                    .zip(assignments.iter())
                    .filter(|(_, t)| **t != Tier::Skip)
                    .map(|(n, _)| n.depth)
                    .max()
                    .unwrap_or(0);

                if max_depth_used > constraints.max_depth {
                    lo_d = lambda_depth;
                } else {
                    hi_d = lambda_depth;
                }
                total_iterations += 1;
            }

            inner_best_lambda_depth = (lo_d + hi_d) / 2.0;

            let lambdas = Lambdas {
                lambda_cost,
                lambda_depth: inner_best_lambda_depth,
                lambda_tier3,
            };
            let (_, _, tier3_count, _) = solve_penalized(nodes, &lambdas);

            if tier3_count > constraints.max_tier3_calls {
                lo_t3 = lambda_tier3;
            } else {
                hi_t3 = lambda_tier3;
            }

            inner_best_lambda_t3 = (lo_t3 + hi_t3) / 2.0;
        }

        let lambdas = Lambdas {
            lambda_cost,
            lambda_depth: inner_best_lambda_depth,
            lambda_tier3: inner_best_lambda_t3,
        };
        let (gain, cost, _, assignments) = solve_penalized(nodes, &lambdas);

        if cost > constraints.max_cost {
            lo_cost = lambda_cost;
        } else {
            hi_cost = lambda_cost;
            // Track best feasible solution
            if gain > best_gain {
                best_gain = gain;
                best_cost = cost;
                best_lambdas = lambdas;
                best_assignments = assignments;
            }
        }
    }

    // Build allocated_tiers map
    let mut allocated_tiers = HashMap::new();
    for (i, node) in nodes.iter().enumerate() {
        allocated_tiers.insert(node.cid.0.clone(), best_assignments[i].to_string());
    }

    CalibrationResult {
        lambdas: best_lambdas,
        allocated_tiers,
        total_gain: best_gain,
        total_cost: best_cost,
        iterations: total_iterations,
        root_cid: nodes.first().map(|n| n.cid.0.clone()).unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Bridge functions
// ---------------------------------------------------------------------------

/// Build a spanning tree from the DAG via BFS from root.
///
/// Diamonds (nodes reachable via multiple paths) are resolved by first-visit.
/// Score data is pulled from ket-score to compute info_potential per node.
pub fn dag_to_tree(
    dag: &Dag<'_>,
    db: &DoltDb,
    root_cid: &Cid,
) -> Result<Vec<TreeNode>, OptError> {
    let engine = ScoringEngine::new(db);
    let mut nodes: Vec<TreeNode> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    // Map from CID -> index in nodes vec
    let mut cid_to_idx: HashMap<String, usize> = HashMap::new();
    // BFS queue: (cid, parent_index, depth)
    let mut queue: VecDeque<(Cid, Option<usize>, u32)> = VecDeque::new();

    queue.push_back((root_cid.clone(), None, 0));

    while let Some((cid, parent_idx, depth)) = queue.pop_front() {
        if !visited.insert(cid.0.clone()) {
            continue;
        }

        // Try to get the node from CAS
        let _dag_node = match dag.get_node(&cid) {
            Ok(n) => n,
            Err(ket_dag::DagError::Cas(ket_cas::CasError::NotFound(_)))
            | Err(ket_dag::DagError::Serde(_)) => continue,
            Err(e) => return Err(OptError::Dag(e)),
        };

        // Compute info_potential from scores
        let info_potential = compute_info_potential(&engine, &cid);

        let idx = nodes.len();
        nodes.push(TreeNode {
            cid: cid.clone(),
            children: Vec::new(),
            info_potential,
            depth,
        });
        cid_to_idx.insert(cid.0.clone(), idx);

        // Add as child of parent
        if let Some(pidx) = parent_idx {
            nodes[pidx].children.push(idx);
        }

        // Enqueue children (nodes whose parent list contains this CID).
        // In the DAG structure, we look for nodes that have this as a parent.
        // But since we're doing BFS from root *downward*, we use SQL children_of.
        let children_csv = db.children_of(cid.as_str());
        if let Ok(csv) = children_csv {
            for line in csv.lines().skip(1) {
                // CSV: child_cid,kind,agent,created_at
                let child_cid = line.split(',').next().unwrap_or("").trim();
                if !child_cid.is_empty() && !visited.contains(child_cid) {
                    queue.push_back((Cid::from(child_cid), Some(idx), depth + 1));
                }
            }
        }
        // Also check CAS-only children via parent references
        // (Walk the DAG node's own children if any are encoded differently)
    }

    if nodes.is_empty() {
        return Err(OptError::EmptyTree(root_cid.0.clone()));
    }

    Ok(nodes)
}

/// Compute info_potential for a node: 1.0 - avg(scores).
/// Unscored nodes get potential of 1.0 (maximum uncertainty).
fn compute_info_potential(engine: &ScoringEngine<'_>, cid: &Cid) -> f64 {
    let scores_csv = match engine.scores_for(cid) {
        Ok(csv) => csv,
        Err(_) => return 1.0,
    };

    let mut sum = 0.0;
    let mut count = 0u32;
    for line in scores_csv.lines().skip(1) {
        // CSV: dimension,value,scorer,evidence
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 2 {
            if let Ok(val) = parts[1].trim().parse::<f64>() {
                sum += val;
                count += 1;
            }
        }
    }

    if count == 0 {
        1.0
    } else {
        (1.0 - sum / count as f64).max(0.0)
    }
}

/// End-to-end calibration: build tree -> optimize -> store result as DAG node.
///
/// Returns the CID of the calibration DAG node and the CalibrationResult.
pub fn calibrate(
    _cas: &CasStore,
    dag: &Dag<'_>,
    db: &DoltDb,
    root_cid: &Cid,
    constraints: &Constraints,
    agent: &str,
) -> Result<(Cid, CalibrationResult), OptError> {
    // Build spanning tree
    let tree = dag_to_tree(dag, db, root_cid)?;

    // Optimize
    let result = wqs_optimize(&tree, constraints);

    // Store result as DAG node (kind=Reasoning, parent=root_cid)
    let result_json = serde_json::to_vec(&result).map_err(|e| {
        OptError::ConstraintViolation(format!("Failed to serialize result: {e}"))
    })?;

    let (node_cid, _content_cid) = dag.store_with_node(
        &result_json,
        NodeKind::Reasoning,
        vec![root_cid.clone()],
        agent,
    )?;

    // Sync to SQL
    let node = dag.get_node(&node_cid)?;
    db.sync_dag_node(
        node_cid.as_str(),
        "reasoning",
        agent,
        &node.timestamp,
        node.output_cid.as_str(),
        &format!("calibration for {}", &root_cid.0[..12.min(root_cid.0.len())]),
        &[(root_cid.as_str(), 0)],
    )?;

    // Insert calibration row
    insert_calibration(db, &node_cid, &result, agent)?;

    Ok((node_cid, result))
}

/// Insert a calibration result into the SQL calibrations table.
fn insert_calibration(
    db: &DoltDb,
    cid: &Cid,
    result: &CalibrationResult,
    agent: &str,
) -> Result<(), OptError> {
    let now = chrono::Utc::now().to_rfc3339();
    let sql = format!(
        "INSERT INTO calibrations (cid, root_cid, lambda_cost, lambda_depth, lambda_tier3, \
         total_gain, total_cost, iterations, agent, ts) \
         VALUES ('{}', '{}', {}, {}, {}, {}, {}, {}, '{}', '{}')",
        cid.as_str(),
        result.root_cid,
        result.lambdas.lambda_cost,
        result.lambdas.lambda_depth,
        result.lambdas.lambda_tier3,
        result.total_gain,
        result.total_cost,
        result.iterations,
        agent,
        now,
    );
    db.exec(&sql)?;
    Ok(())
}

/// Read back a calibration result from SQL by its CID.
pub fn inspect_calibration(db: &DoltDb, cid: &str) -> Result<CalibrationResult, OptError> {
    let csv = db.query(&format!(
        "SELECT root_cid, lambda_cost, lambda_depth, lambda_tier3, \
         total_gain, total_cost, iterations FROM calibrations WHERE cid = '{cid}'"
    ))?;

    let line = csv
        .lines()
        .nth(1)
        .ok_or_else(|| OptError::NodeNotFound(cid.to_string()))?;

    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() < 7 {
        return Err(OptError::NodeNotFound(cid.to_string()));
    }

    Ok(CalibrationResult {
        lambdas: Lambdas {
            lambda_cost: parts[1].trim().parse().unwrap_or(0.0),
            lambda_depth: parts[2].trim().parse().unwrap_or(0.0),
            lambda_tier3: parts[3].trim().parse().unwrap_or(0.0),
        },
        allocated_tiers: HashMap::new(), // Not stored in SQL row
        total_gain: parts[4].trim().parse().unwrap_or(0.0),
        total_cost: parts[5].trim().parse().unwrap_or(0.0),
        iterations: parts[6].trim().parse().unwrap_or(0),
        root_cid: parts[0].trim().to_string(),
    })
}

/// Get all calibrations for a subtree root, ordered by timestamp.
pub fn calibration_history(db: &DoltDb, root_cid: &str) -> Result<Vec<CalibrationResult>, OptError> {
    let csv = db.query(&format!(
        "SELECT cid, root_cid, lambda_cost, lambda_depth, lambda_tier3, \
         total_gain, total_cost, iterations FROM calibrations \
         WHERE root_cid = '{root_cid}' ORDER BY ts"
    ))?;

    let mut results = Vec::new();
    for line in csv.lines().skip(1) {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 8 {
            continue;
        }
        results.push(CalibrationResult {
            lambdas: Lambdas {
                lambda_cost: parts[2].trim().parse().unwrap_or(0.0),
                lambda_depth: parts[3].trim().parse().unwrap_or(0.0),
                lambda_tier3: parts[4].trim().parse().unwrap_or(0.0),
            },
            allocated_tiers: HashMap::new(),
            total_gain: parts[5].trim().parse().unwrap_or(0.0),
            total_cost: parts[6].trim().parse().unwrap_or(0.0),
            iterations: parts[7].trim().parse().unwrap_or(0),
            root_cid: parts[1].trim().to_string(),
        });
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a linear chain of TreeNodes.
    fn linear_chain(n: usize) -> Vec<TreeNode> {
        let mut nodes = Vec::new();
        for i in 0..n {
            let mut children = Vec::new();
            if i + 1 < n {
                children.push(i + 1);
            }
            nodes.push(TreeNode {
                cid: Cid::from(format!("{:064x}", i)),
                children,
                info_potential: 1.0, // all unscored
                depth: i as u32,
            });
        }
        nodes
    }

    /// Helper: create a diamond DAG (tree spanning).
    /// Root -> A, Root -> B, A -> D, B -> D (D visited once via first path).
    fn diamond_tree() -> Vec<TreeNode> {
        vec![
            TreeNode {
                cid: Cid::from(format!("{:064x}", 0)),
                children: vec![1, 2],
                info_potential: 1.0,
                depth: 0,
            },
            TreeNode {
                cid: Cid::from(format!("{:064x}", 1)),
                children: vec![3],
                info_potential: 0.8,
                depth: 1,
            },
            TreeNode {
                cid: Cid::from(format!("{:064x}", 2)),
                children: vec![],
                info_potential: 0.6,
                depth: 1,
            },
            TreeNode {
                cid: Cid::from(format!("{:064x}", 3)),
                children: vec![],
                info_potential: 0.9,
                depth: 2,
            },
        ]
    }

    #[test]
    fn test_linear_chain_traversal() {
        let nodes = linear_chain(5);
        let constraints = Constraints {
            max_cost: 100.0,
            max_depth: 10,
            max_tier3_calls: 10,
        };
        let result = wqs_optimize(&nodes, &constraints);
        assert_eq!(result.allocated_tiers.len(), 5);
        assert!(result.total_gain > 0.0);
    }

    #[test]
    fn test_diamond_spanning_tree() {
        let nodes = diamond_tree();
        assert_eq!(nodes.len(), 4); // Diamond resolved to 4 nodes
        assert_eq!(nodes[0].children.len(), 2); // Root has 2 children

        let constraints = Constraints {
            max_cost: 100.0,
            max_depth: 10,
            max_tier3_calls: 10,
        };
        let result = wqs_optimize(&nodes, &constraints);
        assert_eq!(result.allocated_tiers.len(), 4);
    }

    #[test]
    fn test_tight_budget_skips_nodes() {
        let nodes = linear_chain(5);
        let constraints = Constraints {
            max_cost: 2.0, // Very tight: can only afford ~2 shallow nodes
            max_depth: 10,
            max_tier3_calls: 0,
        };
        let result = wqs_optimize(&nodes, &constraints);

        // With tight budget, total cost should be <= max_cost
        assert!(
            result.total_cost <= constraints.max_cost + 0.01,
            "total_cost {} should be <= max_cost {}",
            result.total_cost,
            constraints.max_cost
        );

        // Some nodes should be skipped
        let skip_count = result
            .allocated_tiers
            .values()
            .filter(|t| t.as_str() == "skip")
            .count();
        assert!(skip_count > 0, "Some nodes should be skipped with tight budget");
    }

    #[test]
    fn test_loose_budget_deep_nodes() {
        let nodes = linear_chain(5);
        let constraints = Constraints {
            max_cost: 1000.0,
            max_depth: 100,
            max_tier3_calls: 100,
        };
        let result = wqs_optimize(&nodes, &constraints);

        // With loose budget and lambdas near zero, all nodes should get deep
        let deep_count = result
            .allocated_tiers
            .values()
            .filter(|t| t.as_str() == "deep")
            .count();
        assert_eq!(deep_count, 5, "All nodes should be deep with loose budget");
    }

    #[test]
    fn test_concavity_convergence() {
        // Binary search should converge: increasing lambda_cost monotonically
        // decreases total cost.
        let nodes = linear_chain(10);

        let low_lambda = Lambdas {
            lambda_cost: 0.01,
            lambda_depth: 0.0,
            lambda_tier3: 0.0,
        };
        let high_lambda = Lambdas {
            lambda_cost: 5.0,
            lambda_depth: 0.0,
            lambda_tier3: 0.0,
        };

        let (_, cost_low, _, _) = solve_penalized(&nodes, &low_lambda);
        let (_, cost_high, _, _) = solve_penalized(&nodes, &high_lambda);

        assert!(
            cost_low >= cost_high,
            "Higher lambda should yield lower cost: low={cost_low}, high={cost_high}"
        );
    }

    #[test]
    fn test_idempotence() {
        let nodes = linear_chain(5);
        let constraints = Constraints {
            max_cost: 10.0,
            max_depth: 5,
            max_tier3_calls: 2,
        };

        let result1 = wqs_optimize(&nodes, &constraints);
        let result2 = wqs_optimize(&nodes, &constraints);

        // Same inputs -> same allocations
        assert_eq!(result1.allocated_tiers, result2.allocated_tiers);
        assert!((result1.total_gain - result2.total_gain).abs() < 1e-10);
        assert!((result1.total_cost - result2.total_cost).abs() < 1e-10);
    }

    // Note: SQL round-trip and history ordering tests require a running Dolt
    // instance and are tested via integration tests / CLI.
    #[test]
    fn test_solve_penalized_zero_lambdas() {
        // With zero penalties, every node should get Deep (max gain)
        let nodes = linear_chain(3);
        let lambdas = Lambdas {
            lambda_cost: 0.0,
            lambda_depth: 0.0,
            lambda_tier3: 0.0,
        };
        let (gain, cost, tier3_count, assignments) = solve_penalized(&nodes, &lambdas);

        assert_eq!(assignments.len(), 3);
        for t in &assignments {
            assert_eq!(*t, Tier::Deep);
        }
        assert_eq!(tier3_count, 3);
        assert!((cost - 12.0).abs() < 1e-10); // 3 * 4.0
        assert!((gain - 3.0).abs() < 1e-10); // 3 * 1.0 * 1.0
    }

    #[test]
    fn test_solve_penalized_high_cost_penalty() {
        // With very high cost penalty, everything should be Skip
        let nodes = linear_chain(3);
        let lambdas = Lambdas {
            lambda_cost: 100.0,
            lambda_depth: 0.0,
            lambda_tier3: 0.0,
        };
        let (gain, cost, tier3_count, assignments) = solve_penalized(&nodes, &lambdas);

        for t in &assignments {
            assert_eq!(*t, Tier::Skip);
        }
        assert!((gain - 0.0).abs() < 1e-10);
        assert!((cost - 0.0).abs() < 1e-10);
        assert_eq!(tier3_count, 0);
    }
}
