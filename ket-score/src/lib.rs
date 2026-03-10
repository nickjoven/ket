//! Scoring engine for agent outputs.
//!
//! Dimensions: correctness, efficiency, style, completeness.
//! Sources: auto (compile/test), peer (cross-agent review), human.
//! Routing: pick the best agent for a task based on historical scores.

use ket_cas::Cid;
use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Debug, thiserror::Error)]
pub enum ScoreError {
    #[error("SQL error: {0}")]
    Sql(#[from] ket_sql::SqlError),
    #[error("Invalid dimension: {0}")]
    InvalidDimension(String),
    #[error("Score value must be between 0.0 and 1.0, got {0}")]
    OutOfRange(f64),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Scoring dimensions.
///
/// The original four dimensions (Correctness, Efficiency, Style, Completeness)
/// are extended with two decay–quantum walk dimensions:
/// - `QuantumCoherence`: amplitude localization score from a quantum walk
///   (1 − normalized Shannon entropy of |ψ|²). This is an **experimental**
///   graph-dynamics signal — whether it correlates with structural
///   inconsistency (hypothesis H-IC) is unverified.
/// - `DecayAdjustedActivation`: activation after exponential decay; tracks
///   how "live" a node's contribution is given its age and half-life.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    Correctness,
    Efficiency,
    Style,
    Completeness,
    /// Amplitude localization score from a quantum walk
    /// (1 − normalized Shannon entropy of |ψ|²).
    /// 1.0 = amplitude concentrated on few nodes; 0.0 = amplitude spread
    /// uniformly across all nodes.  **Experimental** — the connection to
    /// structural consistency claimed by hypothesis H-IC is not yet verified.
    QuantumCoherence,
    /// Decay-adjusted activation — activation value after exponential decay
    /// is applied at query time.
    DecayAdjustedActivation,
}

impl Dimension {
    pub fn as_str(&self) -> &'static str {
        match self {
            Dimension::Correctness => "correctness",
            Dimension::Efficiency => "efficiency",
            Dimension::Style => "style",
            Dimension::Completeness => "completeness",
            Dimension::QuantumCoherence => "quantum_coherence",
            Dimension::DecayAdjustedActivation => "decay_adjusted_activation",
        }
    }

    pub fn parse(s: &str) -> Result<Self, ScoreError> {
        match s.to_lowercase().as_str() {
            "correctness" => Ok(Dimension::Correctness),
            "efficiency" => Ok(Dimension::Efficiency),
            "style" => Ok(Dimension::Style),
            "completeness" => Ok(Dimension::Completeness),
            "quantum_coherence" => Ok(Dimension::QuantumCoherence),
            "decay_adjusted_activation" => Ok(Dimension::DecayAdjustedActivation),
            _ => Err(ScoreError::InvalidDimension(s.to_string())),
        }
    }

    pub fn all() -> &'static [Dimension] {
        &[
            Dimension::Correctness,
            Dimension::Efficiency,
            Dimension::Style,
            Dimension::Completeness,
            Dimension::QuantumCoherence,
            Dimension::DecayAdjustedActivation,
        ]
    }
}

/// A score entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Score {
    pub node_cid: Cid,
    pub agent: String,
    pub scorer: String,
    pub dimension: Dimension,
    pub value: f64,
    pub evidence: String,
}

impl Score {
    pub fn new(
        node_cid: Cid,
        agent: &str,
        scorer: &str,
        dimension: Dimension,
        value: f64,
        evidence: &str,
    ) -> Result<Self, ScoreError> {
        if !(0.0..=1.0).contains(&value) {
            return Err(ScoreError::OutOfRange(value));
        }
        Ok(Score {
            node_cid,
            agent: agent.to_string(),
            scorer: scorer.to_string(),
            dimension,
            value,
            evidence: evidence.to_string(),
        })
    }
}

/// An agent's profile — aggregated scores across dimensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProfile {
    pub agent: String,
    pub correctness: Option<f64>,
    pub efficiency: Option<f64>,
    pub style: Option<f64>,
    pub completeness: Option<f64>,
    /// Average quantum coherence score across scored nodes.
    pub quantum_coherence: Option<f64>,
    /// Average decay-adjusted activation across scored nodes.
    pub decay_adjusted_activation: Option<f64>,
    pub total_scores: u64,
}

/// Auto-score result from running a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScoreResult {
    pub dimension: Dimension,
    pub value: f64,
    pub evidence: String,
}

/// Scoring engine that persists to Dolt.
pub struct ScoringEngine<'a> {
    db: &'a ket_sql::DoltDb,
}

impl<'a> ScoringEngine<'a> {
    pub fn new(db: &'a ket_sql::DoltDb) -> Self {
        ScoringEngine { db }
    }

    /// Record a score for a DAG node.
    pub fn record(&self, score: &Score) -> Result<(), ScoreError> {
        let id = uuid::Uuid::now_v7().to_string();
        self.db.insert_score(
            &id,
            score.node_cid.as_str(),
            &score.agent,
            &score.scorer,
            score.dimension.as_str(),
            score.value,
            &score.evidence,
        )?;
        Ok(())
    }

    /// Get all scores for a node.
    pub fn scores_for(&self, node_cid: &Cid) -> Result<String, ScoreError> {
        Ok(self.db.scores_for_node(node_cid.as_str())?)
    }

    /// Get an agent's scoring profile (averages across dimensions).
    pub fn agent_profile(&self, agent: &str) -> Result<String, ScoreError> {
        Ok(self.db.agent_score_profile(agent)?)
    }

    /// Find the best agent for a given dimension.
    pub fn route(&self, dimension: &str) -> Result<String, ScoreError> {
        Ok(self.db.best_agent_for(dimension)?)
    }

    /// Auto-score a code artifact by running compile/test commands.
    /// Returns scores for correctness (did it compile/pass tests).
    pub fn auto_score_code(
        &self,
        node_cid: &Cid,
        agent: &str,
        work_dir: &std::path::Path,
    ) -> Result<Vec<AutoScoreResult>, ScoreError> {
        let mut results = Vec::new();

        // Try cargo build (correctness: does it compile?)
        let compile_output = Command::new("cargo")
            .args(["build", "--quiet"])
            .current_dir(work_dir)
            .output();

        if let Ok(output) = compile_output {
            let compiled = output.status.success();
            let evidence = if compiled {
                "cargo build succeeded".to_string()
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                format!(
                    "cargo build failed: {}",
                    stderr.lines().take(5).collect::<Vec<_>>().join("; ")
                )
            };
            let score_val = if compiled { 1.0 } else { 0.0 };

            let result = AutoScoreResult {
                dimension: Dimension::Correctness,
                value: score_val,
                evidence: evidence.clone(),
            };
            results.push(result);

            // Record it
            let score =
                Score::new(node_cid.clone(), agent, "auto:compile", Dimension::Correctness, score_val, &evidence)?;
            self.record(&score)?;
        }

        // Try cargo test (completeness: do tests pass?)
        let test_output = Command::new("cargo")
            .args(["test", "--quiet"])
            .current_dir(work_dir)
            .output();

        if let Ok(output) = test_output {
            let passed = output.status.success();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let evidence = if passed {
                let test_line = stdout
                    .lines()
                    .find(|l| l.contains("test result"))
                    .unwrap_or("tests passed");
                format!("cargo test: {test_line}")
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                format!(
                    "cargo test failed: {}",
                    stderr.lines().take(5).collect::<Vec<_>>().join("; ")
                )
            };
            let score_val = if passed { 1.0 } else { 0.0 };

            let result = AutoScoreResult {
                dimension: Dimension::Completeness,
                value: score_val,
                evidence: evidence.clone(),
            };
            results.push(result);

            let score = Score::new(
                node_cid.clone(),
                agent,
                "auto:test",
                Dimension::Completeness,
                score_val,
                &evidence,
            )?;
            self.record(&score)?;
        }

        // Try clippy (style: does it pass lints?)
        let clippy_output = Command::new("cargo")
            .args(["clippy", "--quiet", "--", "-D", "warnings"])
            .current_dir(work_dir)
            .output();

        if let Ok(output) = clippy_output {
            let clean = output.status.success();
            let evidence = if clean {
                "cargo clippy: no warnings".to_string()
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let warning_count = stderr.matches("warning").count();
                format!("cargo clippy: {warning_count} warnings")
            };
            // Partial score: 1.0 if clean, 0.5 if some warnings, 0.0 if many
            let score_val = if clean { 1.0 } else { 0.3 };

            let result = AutoScoreResult {
                dimension: Dimension::Style,
                value: score_val,
                evidence: evidence.clone(),
            };
            results.push(result);

            let score =
                Score::new(node_cid.clone(), agent, "auto:clippy", Dimension::Style, score_val, &evidence)?;
            self.record(&score)?;
        }

        Ok(results)
    }
}
