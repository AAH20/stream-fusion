use stream_fusion_core::{FeatureRow, FeatureValue, Result};
use stream_fusion_storage::OnlineFeatureStore;
use std::sync::Arc;
use std::time::Instant;

/// High-throughput simulated Arrow Flight Zero-Copy Stream Adapter.
pub struct FlightStreamAdapter {
    store: Arc<OnlineFeatureStore>,
}

impl FlightStreamAdapter {
    pub fn new(store: Arc<OnlineFeatureStore>) -> Self {
        Self { store }
    }

    /// Simulate real-time streaming batch ingestion from Kafka topics.
    pub fn ingest_kafka_batch(&self, batch: Vec<FeatureRow>) -> Result<(usize, u128)> {
        let start = Instant::now();
        let count = batch.len();
        for row in batch {
            self.store.put(row)?;
        }
        let elapsed_us = start.elapsed().as_micros();
        Ok((count, elapsed_us))
    }

    /// High-speed candidate feature tensor retrieval for PyTorch/TensorRT inference.
    pub fn serve_inference_tensors(&self, entity_ids: &[&str]) -> (Vec<Option<FeatureRow>>, u128) {
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
    fn test_stream_adapter_flow() {
        let store = Arc::new(OnlineFeatureStore::new());
        let adapter = FlightStreamAdapter::new(store);

        let mut batch = Vec::new();
        for i in 0..100 {
            batch.push(
                FeatureRow::new(format!("user_{}", i), "realtime_features")
                    .with_value("recency_score", FeatureValue::Float(0.98))
                    .with_value("category_id", FeatureValue::Int(12)),
            );
        }

        let (ingested, time_us) = adapter.ingest_kafka_batch(batch).unwrap();
        assert_eq!(ingested, 100);
        println!("Ingested 100 rows in {} us", time_us);

        let (tensors, fetch_us) = adapter.serve_inference_tensors(&["user_10", "user_20", "user_30"]);
        assert_eq!(tensors.len(), 3);
        assert!(tensors[0].is_some());
        println!("Retrieved 3 inference tensors in {} us", fetch_us);
    }
}
