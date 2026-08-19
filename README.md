# Stream-Fusion

A concurrent, in-memory feature store for real-time recommendation/ranking
systems, with a benchmark harness and a feature-staleness cost calculator.

[![CI](https://github.com/AAH20/stream-fusion/actions/workflows/ci.yml/badge.svg)](https://github.com/AAH20/stream-fusion/actions)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)

An earlier version of this README claimed this was zero-copy, lock-free,
and integrated with Kafka, Arrow Flight, PyTorch/TensorRT, and A2Z SOC, and
priced feature staleness against a built-in "industry benchmark" constant
with no citation. None of that was true of the code at the time. This
version describes what's actually here — the fixes are load-bearing, not
cosmetic:

- **"Zero-copy" is now actually true.** Reads used to `.clone()` the full
  row (deep-copying its `HashMap<String, FeatureValue>` and every `String`
  inside it) on every `get`/`get_batch` call — the literal opposite of the
  claim. Storage now holds `Arc<FeatureRow>`; a read clones an `Arc`
  (pointer + refcount), not the row's contents. Retrieval got measurably
  faster as a direct result (0.361ms → 0.148ms for a 500-candidate batch on
  the same machine), which is what actually fixing an overclaim looks like
  instead of just renaming it.
- **"Lock-free" is gone.** The store is backed by `DashMap`, a sharded,
  `RwLock`-guarded concurrent map — fast under real concurrent load, but
  not lock-free in the formal sense (no wait-free, CAS-only progress
  guarantee). Calling it lock-free was a real overclaim, not a rounding
  error.
- **No Kafka, Arrow Flight, or PyTorch/TensorRT integration exists**, and
  the code no longer implies otherwise. There's no network client, no
  consumer group, no gRPC service, no `arrow-flight` dependency anywhere in
  this workspace. What's here is `FeatureIngestHarness`: an in-process
  harness that ingests already-deserialized `FeatureRow`s into the store
  and measures it. A real Kafka consumer feeding rows into this store is
  exactly the integration this crate doesn't yet include — see Roadmap.
- **The financial calculator no longer has invented constants baked in.**
  It used to hardcode "every 1000ms of feature lag degrades CTR by ~0.3%"
  labeled as an "industry benchmark" with no source, and a "35% margin
  attribution" with no justification, then reported the resulting dollar
  figure with false precision. Both are now required CLI flags you supply
  from your own measured data — the tool refuses to run without them,
  rather than silently assuming numbers nobody verified.
- **The CI badge above now points at a real workflow** (`.github/workflows/ci.yml`)
  instead of one that didn't exist.
- **The "Commercial Integration with A2Z SOC" section is removed.** No code
  in this repo talks to a2zsoc.com; the claim had no basis in the codebase.

## What's actually here

```
stream-fusion/
├── crates/
│   ├── stream-fusion-core/       # FeatureRow, FeatureValue, and the staleness/CTR-delta calculator.
│   ├── stream-fusion-storage/    # OnlineFeatureStore: a concurrent, Arc-backed in-memory feature store.
│   ├── stream-fusion-flight/     # FeatureIngestHarness: in-process ingest/retrieve benchmark instrumentation.
│   └── stream-fusion-cli/        # `benchmark` and `audit-skew` subcommands.
```

## Try it

```bash
cargo test --workspace

# Benchmarks this store only — no Kafka, no network, no real streaming source.
cargo run --release -p stream-fusion-cli -- benchmark --events 100000 --concurrency-batch 2000

# Both rate flags are required — your own measured numbers, not this tool's assumptions.
cargo run --release -p stream-fusion-cli -- audit-skew \
  --daily-gmv 10000000 \
  --baseline-ctr 0.045 \
  --simulated-lag-ms 25000 \
  --staleness-degradation-pct-per-1000ms 0.3 \
  --margin-attribution 0.35
```

Representative numbers from a run on one development machine — not a
guarantee, and reproducible only in the narrow sense of "this store,
single-process, no network involved":

```
[ INGESTION RESULTS ]
  Throughput:            ~1.9-2.9M rows/second (varies by run)

[ BATCH RETRIEVAL BENCHMARK ]
  Candidate Batch Size:  500
  Total Retrieval Time:  ~0.15ms
```

## Honest scope

- This is a data structure, not a streaming platform. It has no network
  layer, no persistence, no schema registry, no consumer offsets — it's an
  `Arc`-backed concurrent hashmap with atomic write sequencing and
  timestamp watermarking, benchmarked honestly for what it is.
- `evaluate_conversion_delta`'s CTR-degradation-per-1000ms rate and the
  CLI's margin-attribution figure are user-supplied assumptions, not
  validated constants — this tool does the arithmetic once you provide
  real numbers; it doesn't know your numbers for you.
- No Kafka, Arrow Flight, or model-serving integration ships today. See
  Roadmap.

## Roadmap

- A real Kafka (or Redpanda) consumer feeding `FeatureIngestHarness` —
  this is what would make the "streaming" half of the name true beyond an
  in-process benchmark.
- An actual Arrow Flight service for cross-process, zero-copy tensor
  serving to a model-serving layer (PyTorch/TensorRT), rather than an
  in-process `Vec`/`Arc` handoff.
- Durable storage / WAL for the feature store (currently pure in-memory,
  lost on restart).

## License

MIT OR Apache-2.0
