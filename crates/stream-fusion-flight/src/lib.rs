use stream_fusion_core::{FeatureRow, Result};
#[cfg(test)]
use stream_fusion_core::FeatureValue;
use stream_fusion_storage::OnlineFeatureStore;
use std::sync::Arc;
use std::time::Instant;

/// In-process ingestion and batch-retrieval harness over `OnlineFeatureStore`.
///
/// This does not speak the Kafka wire protocol or the Arrow Flight RPC
/// protocol — there is no network client, no consumer group, no gRPC
/// service here. It accepts already-deserialized `FeatureRow`s (however
/// they got here — a real Kafka consumer feeding this store is exactly the
/// integration this crate doesn't yet include, see the workspace README's
/// roadmap) and measures how fast this store ingests and serves them.
/// Earlier versions of this doc called this "Arrow Flight" and "Kafka
/// ingestion," which overstated what's actually implemented; naming it
/// accurately here rather than leaving the impression of an integration
/// that doesn't exist.
pub struct FeatureIngestHarness {
    store: Arc<OnlineFeatureStore>,
}

impl FeatureIngestHarness {
    pub fn new(store: Arc<OnlineFeatureStore>) -> Self {
        Self { store }
    }

    /// Ingest a batch of already-deserialized feature rows.
    pub fn ingest_batch(&self, batch: Vec<FeatureRow>) -> Result<(usize, u128)> {
        let start = Instant::now();
        let count = batch.len();
        for row in batch {
            self.store.put(row)?;
        }
        let elapsed_us = start.elapsed().as_micros();
        Ok((count, elapsed_us))
    }

    /// Batch retrieval for candidate scoring. Returns `Arc<FeatureRow>` —
    /// see `OnlineFeatureStore::get_batch`.
    pub fn serve_batch(&self, entity_ids: &[&str]) -> (Vec<Option<Arc<FeatureRow>>>, u128) {
        let start = Instant::now();
        let rows = self.store.get_batch(entity_ids);
        let elapsed_us = start.elapsed().as_micros();
        (rows, elapsed_us)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_and_serve_flow() {
        let store = Arc::new(OnlineFeatureStore::new());
        let harness = FeatureIngestHarness::new(store);

        let mut batch = Vec::new();
        for i in 0..100 {
            batch.push(
                FeatureRow::new(format!("user_{}", i), "realtime_features")
                    .with_value("recency_score", FeatureValue::Float(0.98))
                    .with_value("category_id", FeatureValue::Int(12)),
            );
        }

        let (ingested, time_us) = harness.ingest_batch(batch).unwrap();
        assert_eq!(ingested, 100);
        println!("Ingested 100 rows in {} us", time_us);

        let (rows, fetch_us) = harness.serve_batch(&["user_10", "user_20", "user_30"]);
        assert_eq!(rows.len(), 3);
        assert!(rows[0].is_some());
        println!("Retrieved 3 rows in {} us", fetch_us);
    }
}
