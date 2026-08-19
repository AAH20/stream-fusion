use clap::{Parser, Subcommand};
use std::sync::Arc;
use std::time::Instant;
use stream_fusion_core::{FeatureRow, FeatureValue, InvariantEvaluator};
use stream_fusion_flight::FeatureIngestHarness;
use stream_fusion_storage::OnlineFeatureStore;

#[derive(Parser)]
#[command(name = "stream-fusion")]
#[command(about = "Concurrent in-memory feature store: ingestion/retrieval benchmark and a feature-staleness cost calculator", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Benchmark in-process feature ingestion and batch retrieval against this store.
    /// Measures this store only — no Kafka, no network, no real streaming source.
    Benchmark {
        #[arg(short, long, default_value_t = 50000)]
        events: usize,
        #[arg(short, long, default_value_t = 1000)]
        concurrency_batch: usize,
    },
    /// Calculate the dollar cost of a given feature-staleness assumption.
    /// Both rate parameters are inputs you supply, not built-in constants —
    /// plug in numbers from your own measured online/offline skew, not
    /// defaults from this tool.
    AuditSkew {
        #[arg(long, default_value_t = 10000000.0)]
        daily_gmv: f64,
        #[arg(long, default_value_t = 0.045)]
        baseline_ctr: f64,
        #[arg(long, default_value_t = 25000)]
        simulated_lag_ms: i64,
        /// Your measured CTR degradation per 1000ms of feature staleness, as a percent. No default — you must supply this from your own data.
        #[arg(long)]
        staleness_degradation_pct_per_1000ms: f64,
        /// Your measured gross margin, as a fraction of GMV, used to convert a CTR drop into a dollar loss. No default — you must supply this from your own data.
        #[arg(long)]
        margin_attribution: f64,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Benchmark { events, concurrency_batch } => {
            println!("================================================================================");
            println!("  STREAM-FUSION IN-MEMORY FEATURE STORE BENCHMARK");
            println!("  (in-process only — no Kafka, no network, no real streaming source)");
            println!("================================================================================");
            println!("• Ingesting {} rows in batches of {}...", events, concurrency_batch);

            let store = Arc::new(OnlineFeatureStore::new());
            let harness = FeatureIngestHarness::new(store.clone());

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
                let (count, _) = harness.ingest_batch(chunk.to_vec()).expect("Ingestion failed");
                total_ingested += count;
            }
            let ingest_duration = start.elapsed();
            let throughput = total_ingested as f64 / ingest_duration.as_secs_f64();

            println!("\n[ INGESTION RESULTS ]");
            println!("  Total Rows Ingested:   {}", total_ingested);
            println!("  Ingestion Duration:    {:.2?}", ingest_duration);
            println!("  Throughput:            {:.2} rows/second", throughput);

            println!("\n[ BATCH RETRIEVAL BENCHMARK ]");
            let test_ids: Vec<String> = (0..500).map(|i| format!("user_{}", i * 10)).collect();
            let id_slices: Vec<&str> = test_ids.iter().map(|s| s.as_str()).collect();

            let (rows, fetch_us) = harness.serve_batch(&id_slices);
            println!("  Candidate Batch Size:  {}", rows.len());
            println!("  Total Retrieval Time:  {:.3} ms (Total: {} us)", fetch_us as f64 / 1000.0, fetch_us);
            println!("  Latency Per Entity:    {:.3} us", fetch_us as f64 / id_slices.len() as f64);
            println!("================================================================================\n");
        }
        Commands::AuditSkew {
            daily_gmv,
            baseline_ctr,
            simulated_lag_ms,
            staleness_degradation_pct_per_1000ms,
            margin_attribution,
        } => {
            println!("================================================================================");
            println!("  FEATURE-STALENESS COST CALCULATOR");
            println!("  (both rates below are your inputs, not built-in constants)");
            println!("================================================================================");
            println!("• Daily Platform GMV:            ${:.2}", daily_gmv);
            println!("• Baseline Ranking CTR:          {:.2}%", baseline_ctr * 100.0);
            println!("• Assumed Feature Lag:           {} ms ({:.1} seconds)", simulated_lag_ms, simulated_lag_ms as f64 / 1000.0);
            println!("• Your CTR Degradation Rate:     {:.3}% per 1000ms", staleness_degradation_pct_per_1000ms);
            println!("• Your Margin Attribution:       {:.1}%", margin_attribution * 100.0);

            let degraded_ctr = InvariantEvaluator::evaluate_conversion_delta(
                baseline_ctr,
                simulated_lag_ms,
                staleness_degradation_pct_per_1000ms,
            );
            let ctr_drop_pct = (baseline_ctr - degraded_ctr) / baseline_ctr * 100.0;
            let daily_lost_gmv = daily_gmv * (ctr_drop_pct / 100.0) * margin_attribution;

            println!("\n[ PROJECTED IMPACT, GIVEN YOUR INPUTS ABOVE ]");
            println!("  Degraded CTR under Lag:    {:.2}%", degraded_ctr * 100.0);
            println!("  Relative CTR Degradation:  {:.2}%", ctr_drop_pct);
            println!("  Projected Daily Lost GMV:  ${:.2}", daily_lost_gmv);
            println!("  Projected Annual Lost GMV: ${:.2}", daily_lost_gmv * 365.0);
            println!("================================================================================\n");
        }
    }
}
