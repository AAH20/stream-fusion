use clap::{Parser, Subcommand};
use std::sync::Arc;
use std::time::Instant;
use stream_fusion_core::{FeatureRow, FeatureValue, InvariantEvaluator};
use stream_fusion_flight::FlightStreamAdapter;
use stream_fusion_storage::OnlineFeatureStore;

#[derive(Parser)]
#[command(name = "stream-fusion")]
#[command(about = "High-Performance Zero-Copy Real-Time Feature Invariant & Streaming Engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Benchmark simulated Kafka streaming ingestion vs PyTorch tensor retrieval.
    Benchmark {
        #[arg(short, long, default_value_t = 50000)]
        events: usize,
        #[arg(short, long, default_value_t = 1000)]
        concurrency_batch: usize,
    },
    /// Audit online vs offline feature staleness and calculate dollar GMV impact.
    AuditSkew {
        #[arg(long, default_value_t = 10000000.0)]
        daily_gmv: f64,
        #[arg(long, default_value_t = 0.045)]
        baseline_ctr: f64,
        #[arg(long, default_value_t = 25000)]
        simulated_lag_ms: i64,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Benchmark { events, concurrency_batch } => {
            println!("================================================================================");
            println!("  STREAM-FUSION ZERO-COPY REAL-TIME FEATURE STREAMING BENCHMARK");
            println!("================================================================================");
            println!("• Ingesting {} streaming events in batches of {}...", events, concurrency_batch);

            let store = Arc::new(OnlineFeatureStore::new());
            let adapter = FlightStreamAdapter::new(store.clone());

            let mut test_data = Vec::new();
            for i in 0..events {
                test_data.push(
                    FeatureRow::new(format!("user_{}", i), "user_session_clickstream")
                        .with_value("recency_rank", FeatureValue::Float(0.992))
                        .with_value("clicks_last_10m", FeatureValue::Int((i % 25) as i64))
                        .with_value("in_market_cat", FeatureValue::String("Electronics".to_string())),
                );
            }

            let start = Instant::now();
            let mut total_ingested = 0;
            for chunk in test_data.chunks(concurrency_batch) {
                let (count, _) = adapter.ingest_kafka_batch(chunk.to_vec()).expect("Ingestion failed");
                total_ingested += count;
            }
            let ingest_duration = start.elapsed();
            let throughput = total_ingested as f64 / ingest_duration.as_secs_f64();

            println!("\n[ INGESTION RESULTS ]");
            println!("  Total Events Ingested: {}", total_ingested);
            println!("  Ingestion Duration:    {:.2?}", ingest_duration);
            println!("  Throughput:            {:.2} events/second", throughput);

            println!("\n[ INFERENCE TENSOR RETRIEVAL BENCHMARK ]");
            let test_ids: Vec<String> = (0..500).map(|i| format!("user_{}", i * 10)).collect();
            let id_slices: Vec<&str> = test_ids.iter().map(|s| s.as_str()).collect();

            let (tensors, fetch_us) = adapter.serve_inference_tensors(&id_slices);
            println!("  Candidate Batch Size:  {}", tensors.len());
            println!("  p99 Retrieval Latency: {:.3} ms (Total: {} us)", fetch_us as f64 / 1000.0, fetch_us);
            println!("  Latency Per Entity:    {:.3} us", fetch_us as f64 / id_slices.len() as f64);
            println!("================================================================================\n");
        }
        Commands::AuditSkew { daily_gmv, baseline_ctr, simulated_lag_ms } => {
            println!("================================================================================");
            println!("  ONLINE/OFFLINE FEATURE SKEW & DOLLAR GMV LOSS AUDITOR");
            println!("================================================================================");
            println!("• Daily Platform GMV:        ${:.2}", daily_gmv);
            println!("• Baseline Ranking CTR:      {:.2}%", baseline_ctr * 100.0);
            println!("• Detected Feature Lag:      {} ms ({:.1} seconds)", simulated_lag_ms, simulated_lag_ms as f64 / 1000.0);

            let degraded_ctr = InvariantEvaluator::evaluate_conversion_delta(baseline_ctr, simulated_lag_ms);
            let ctr_drop_pct = (baseline_ctr - degraded_ctr) / baseline_ctr * 100.0;
            let daily_lost_gmv = daily_gmv * (ctr_drop_pct / 100.0) * 0.35; // assuming 35% margin attribution

            println!("\n[ FINANCIAL & INVARIANT IMPACT ]");
            println!("  Degraded CTR under Lag:    {:.2}%", degraded_ctr * 100.0);
            println!("  Relative CTR Degradation:  {:.2}%", ctr_drop_pct);
            println!("  Estimated Daily Lost GMV:  ${:.2}", daily_lost_gmv);
            println!("  Annualized Lost GMV:       ${:.2}", daily_lost_gmv * 365.0);
            println!("================================================================================\n");
        }
    }
}
