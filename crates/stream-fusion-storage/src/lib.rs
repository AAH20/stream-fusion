use chrono::Utc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stream_fusion_core::{FeatureRow, FeatureValue, FusionError, Result};

/// Lock-free, high-throughput in-memory feature store ring buffer.
#[derive(Clone)]
pub struct OnlineFeatureStore {
    // Sharded concurrent map: entity_id -> FeatureRow
    store: Arc<DashMap<String, FeatureRow>>,
    total_writes: Arc<AtomicU64>,
    total_reads: Arc<AtomicU64>,
}

impl Default for OnlineFeatureStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OnlineFeatureStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
            total_writes: Arc::new(AtomicU64::new(0)),
            total_reads: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Ingest a feature row with atomic write sequence and timestamp watermarking.
    pub fn put(&self, mut row: FeatureRow) -> Result<()> {
        let seq = self.total_writes.fetch_add(1, Ordering::SeqCst);
        row.sequence_num = seq;
        row.timestamp = Utc::now();
        self.store.insert(row.entity_id.clone(), row);
        Ok(())
    }

    /// Retrieve a feature row for real-time model inference.
    pub fn get(&self, entity_id: &str) -> Result<FeatureRow> {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
        self.store
            .get(entity_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| FusionError::NotFound(entity_id.to_string()))
    }

    /// Batch retrieval for Two-Tower / Multi-Task candidate scoring.
    pub fn get_batch(&self, entity_ids: &[&str]) -> Vec<Option<FeatureRow>> {
        entity_ids
            .iter()
            .map(|id| {
                self.total_reads.fetch_add(1, Ordering::Relaxed);
                self.store.get(*id).map(|r| r.value().clone())
            })
            .collect()
    }

    pub fn total_records(&self) -> usize {
        self.store.len()
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.total_writes.load(Ordering::Relaxed),
            self.total_reads.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_put_and_get() {
        let store = OnlineFeatureStore::new();
        let row = FeatureRow::new("user_123", "clicks")
            .with_value("total", FeatureValue::Int(42));

        store.put(row).unwrap();
        let fetched = store.get("user_123").unwrap();
        assert_eq!(fetched.entity_id, "user_123");
        assert_eq!(fetched.values.get("total"), Some(&FeatureValue::Int(42)));
    }

    #[test]
    fn test_batch_retrieval() {
        let store = OnlineFeatureStore::new();
        store.put(FeatureRow::new("u1", "v").with_value("x", FeatureValue::Float(1.0))).unwrap();
        store.put(FeatureRow::new("u2", "v").with_value("x", FeatureValue::Float(2.0))).unwrap();

        let batch = store.get_batch(&["u1", "u2", "u_non_existent"]);
        assert_eq!(batch.len(), 3);
        assert!(batch[0].is_some());
        assert!(batch[1].is_some());
        assert!(batch[2].is_none());
    }
}
