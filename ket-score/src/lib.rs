//! Scoring engine for agent outputs.
//!
//! Dimensions: correctness, efficiency, style, completeness.
//! Sources: auto (compile/test), peer (cross-agent review), human.

use ket_cas::Cid;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ScoreError {
    #[error("SQL error: {0}")]
    Sql(#[from] ket_sql::SqlError),
    #[error("Invalid dimension: {0}")]
    InvalidDimension(String),
    #[error("Score value must be between 0.0 and 1.0, got {0}")]
    OutOfRange(f64),
}

/// Scoring dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dimension {
    Correctness,
    Efficiency,
    Style,
    Completeness,
}

impl Dimension {
    pub fn as_str(&self) -> &'static str {
        match self {
            Dimension::Correctness => "correctness",
            Dimension::Efficiency => "efficiency",
            Dimension::Style => "style",
            Dimension::Completeness => "completeness",
        }
    }

    pub fn parse(s: &str) -> Result<Self, ScoreError> {
        match s.to_lowercase().as_str() {
            "correctness" => Ok(Dimension::Correctness),
            "efficiency" => Ok(Dimension::Efficiency),
            "style" => Ok(Dimension::Style),
            "completeness" => Ok(Dimension::Completeness),
            _ => Err(ScoreError::InvalidDimension(s.to_string())),
        }
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
}
