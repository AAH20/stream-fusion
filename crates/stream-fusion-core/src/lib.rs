use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FusionError {
    #[error("Feature not found for entity: {0}")]
    NotFound(String),
    #[error("Schema validation mismatch: {0}")]
    SchemaMismatch(String),
    #[error("Storage engine failure: {0}")]
    StorageError(String),
    #[error("Serialization failure: {0}")]
    SerializationError(String),
}

pub type Result<T> = std::result::Result<T, FusionError>;

/// Represents a single primitive feature value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum FeatureValue {
    Float(f64),
    Int(i64),
    String(String),
    FloatVector(Vec<f32>),
    Bool(bool),
}

/// A real-time feature row associated with an Entity ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRow {
    pub entity_id: String,
    pub feature_view: String,
    pub values: HashMap<String, FeatureValue>,
    pub timestamp: DateTime<Utc>,
    pub sequence_num: u64,
}

impl FeatureRow {
    pub fn new(entity_id: impl Into<String>, feature_view: impl Into<String>) -> Self {
        Self {
            entity_id: entity_id.into(),
            feature_view: feature_view.into(),
            values: HashMap::new(),
            timestamp: Utc::now(),
            sequence_num: 0,
        }
    }

    pub fn with_value(mut self, key: impl Into<String>, val: FeatureValue) -> Self {
        self.values.insert(key.into(), val);
        self
    }
}

/// Invariant: Measure exact feature staleness (online vs offline skew).
pub struct InvariantEvaluator;

impl InvariantEvaluator {
    pub fn calculate_staleness_ms(row: &FeatureRow) -> i64 {
        let now = Utc::now();
        (now - row.timestamp).num_milliseconds().max(0)
    }

    /// Projects CTR degradation from feature staleness, linearly in
    /// `staleness_per_1000ms_pct`.
    ///
    /// That rate is *not* a built-in "industry benchmark" — an earlier
    /// version hardcoded 0.3%/second here with that label and no citation,
    /// which was a fabricated authority claim: nothing in this crate
    /// measured it, and no source was ever attached. It's a required
    /// parameter now specifically so nobody can call the output of this
    /// function precise without having supplied a real, defensible rate
    /// themselves — from their own model's offline/online skew
    /// measurements, not from this library.
    pub fn evaluate_conversion_delta(baseline_ctr: f64, current_staleness_ms: i64, staleness_per_1000ms_pct: f64) -> f64 {
        let degradation_factor = (current_staleness_ms as f64 / 1000.0) * (staleness_per_1000ms_pct / 100.0);
        (baseline_ctr * (1.0 - degradation_factor)).max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_row_creation() {
        let row = FeatureRow::new("user_10293", "user_click_stream")
            .with_value("clicks_last_5m", FeatureValue::Int(14))
            .with_value("recent_category_embedding", FeatureValue::FloatVector(vec![0.12, 0.45, -0.89]));

        assert_eq!(row.entity_id, "user_10293");
        assert_eq!(row.values.len(), 2);
    }

    #[test]
    fn test_staleness_invariant() {
        let row = FeatureRow::new("user_883", "user_recency");
        let staleness = InvariantEvaluator::calculate_staleness_ms(&row);
        assert!(staleness >= 0);

        let baseline = 0.045; // 4.5% CTR
        let simulated = InvariantEvaluator::evaluate_conversion_delta(baseline, 2000, 0.3);
        assert!(simulated < baseline);
    }
}
