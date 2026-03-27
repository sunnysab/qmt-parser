# Tick Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repository-owned Criterion benchmark suite that compares end-to-end `Vec<TickData>` and `Polars DataFrame` performance on the real tick sample, with correctness checks that keep the comparison honest.

**Architecture:** Keep benchmark-only logic out of production modules. Put shared analysis helpers and correctness utilities in a benchmark support module under `benches/common/`, reuse that module from an integration test, and keep the actual Criterion entrypoint thin. Add no production API unless implementation proves it is necessary.

**Tech Stack:** Rust 2024, Criterion, Polars 0.52, cargo bench, integration tests under `tests/`

---

## File Map

**Modify**
- `Cargo.toml`
- `README.md`

**Create**
- `benches/common/tick_analysis.rs`
- `benches/tick_benchmark.rs`
- `tests/tick_benchmark_equivalence.rs`

**Responsibilities**
- `Cargo.toml`
  - add benchmark-only dependencies
  - register the Criterion benchmark target
  - gate benchmark execution on the `polars` feature
- `benches/common/tick_analysis.rs`
  - sample path helper
  - shared summary type
  - workload enum
  - native `Vec<TickData>` analysis functions
  - Polars analysis functions
  - floating-point equality helper used by tests and benchmark preflight checks
- `benches/tick_benchmark.rs`
  - Criterion groups for `parse_only`, `analyze_only`, `end_to_end`
  - benchmark naming and setup
  - preflight correctness assertions before timing
- `tests/tick_benchmark_equivalence.rs`
  - correctness tests for summary equality and non-empty sample input
  - low-level unit coverage for native workload semantics on a tiny hand-built dataset
- `README.md`
  - short benchmark section with run command and scope caveats

## Ground Rules For Implementation

- Run all new tests and benchmarks with `--features polars`.
- Keep benchmark support code benchmark-local; do not move it into `src/` unless blocked by Rust module visibility.
- Reuse the repository sample at `data/000001-20250529-tick.dat`.
- Treat correctness assertions as mandatory, not advisory.
- Keep the first version focused on time measurements only.

### Task 1: Add benchmark scaffolding and sample-path smoke test

**Files:**
- Modify: `Cargo.toml`
- Create: `benches/common/tick_analysis.rs`
- Test: `tests/tick_benchmark_equivalence.rs`

- [ ] **Step 1: Write the failing smoke test**

Create `tests/tick_benchmark_equivalence.rs` with a feature gate and a first test that expects the shared helper module to provide the sample path plus non-empty parsed inputs.

```rust
#![cfg(feature = "polars")]

#[path = "../benches/common/tick_analysis.rs"]
mod tick_analysis;

use tick_analysis::{load_tick_dataframe, load_tick_structs, sample_tick_path};

#[test]
fn sample_tick_inputs_are_non_empty() {
    let path = sample_tick_path();
    assert!(path.exists(), "missing sample tick file: {}", path.display());

    let rows = load_tick_structs().expect("tick structs should parse");
    assert!(!rows.is_empty(), "tick struct sample should not be empty");

    let df = load_tick_dataframe().expect("tick dataframe should parse");
    assert!(df.height() > 0, "tick dataframe sample should not be empty");
}
```

- [ ] **Step 2: Run the smoke test to verify it fails**

Run: `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence -v`

Expected: FAIL with a module/file error because `benches/common/tick_analysis.rs` does not exist yet.

- [ ] **Step 3: Write the minimal scaffolding**

Modify `Cargo.toml` to add the benchmark dependency and register the benchmark target.

```toml
[dev-dependencies]
criterion = "0.5"

[[bench]]
name = "tick_benchmark"
harness = false
required-features = ["polars"]
```

Create `benches/common/tick_analysis.rs` with just enough shared loading helpers to satisfy the smoke test.

```rust
use std::path::PathBuf;

use polars::prelude::DataFrame;
use qmt_parser::{parse_ticks_to_dataframe, parse_ticks_to_structs, TickData, TickParseError};

pub fn sample_tick_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/000001-20250529-tick.dat")
}

pub fn load_tick_structs() -> Result<Vec<TickData>, TickParseError> {
    parse_ticks_to_structs(sample_tick_path())
}

pub fn load_tick_dataframe() -> Result<DataFrame, TickParseError> {
    parse_ticks_to_dataframe(sample_tick_path())
}
```

- [ ] **Step 4: Run the smoke test again**

Run: `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence -v`

Expected: PASS with `sample_tick_inputs_are_non_empty ... ok`

- [ ] **Step 5: Commit the scaffolding**

```bash
git add Cargo.toml benches/common/tick_analysis.rs tests/tick_benchmark_equivalence.rs
git commit -m "test: add tick benchmark scaffolding"
```

### Task 2: Implement the shared summary model and native Vec analysis

**Files:**
- Modify: `benches/common/tick_analysis.rs`
- Modify: `tests/tick_benchmark_equivalence.rs`

- [ ] **Step 1: Write failing native-analysis tests on a tiny hand-built dataset**

Extend `tests/tick_benchmark_equivalence.rs` with a helper constructor and targeted tests for `basic_scan` and `mixed_orderbook`.

```rust
use qmt_parser::TickData;
use tick_analysis::{analyze_ticks_vec, TickAnalysisSummary, Workload};

fn make_tick(
    last_price: Option<f64>,
    amount: Option<f64>,
    volume: Option<u64>,
    best_ask: Option<f64>,
    best_bid: Option<f64>,
    ask_vols: [Option<u32>; 5],
    bid_vols: [Option<u32>; 5],
) -> TickData {
    TickData {
        market: Some("SZ".to_string()),
        symbol: "000001".to_string(),
        date: "20250529".to_string(),
        raw_qmt_timestamp: 0,
        market_phase_status: 0,
        last_price,
        last_close: 10.0,
        amount,
        volume,
        ask_prices: [best_ask, None, None, None, None],
        ask_vols,
        bid_prices: [best_bid, None, None, None, None],
        bid_vols,
        qmt_status_field_1_raw: 0,
        qmt_status_field_2_raw: 0,
    }
}

#[test]
fn vec_basic_scan_matches_expected_summary() {
    let rows = vec![
        make_tick(Some(10.0), Some(100.0), Some(10), Some(10.1), Some(9.9), [Some(1), Some(2), None, None, None], [Some(3), None, None, None, None]),
        make_tick(Some(12.0), Some(300.0), Some(20), Some(12.1), Some(11.9), [Some(4), Some(5), None, None, None], [Some(6), Some(7), None, None, None]),
        make_tick(None, Some(400.0), Some(30), None, None, [None; 5], [None; 5]),
    ];

    let summary = analyze_ticks_vec(&rows, Workload::BasicScan);
    assert_eq!(summary.row_count, 2);
    assert_eq!(summary.amount_sum, Some(400.0));
    assert_eq!(summary.volume_sum, Some(30));
    assert_eq!(summary.last_price_min, Some(10.0));
    assert_eq!(summary.last_price_max, Some(12.0));
    assert_eq!(summary.last_price_mean, Some(11.0));
}

#[test]
fn vec_mixed_orderbook_matches_expected_summary() {
    // build two valid rows and assert spread/mid/5-level volume means
}
```

- [ ] **Step 2: Run the native-analysis tests to verify they fail**

Run: `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence vec_ -- -q`

Expected: FAIL with unresolved imports or missing items for `TickAnalysisSummary`, `Workload`, and `analyze_ticks_vec`.

- [ ] **Step 3: Implement the shared summary type and native analysis**

Add the benchmark-local API to `benches/common/tick_analysis.rs`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    BasicScan,
    MixedOrderBook,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TickAnalysisSummary {
    pub row_count: u64,
    pub amount_sum: Option<f64>,
    pub volume_sum: Option<u64>,
    pub last_price_min: Option<f64>,
    pub last_price_max: Option<f64>,
    pub last_price_mean: Option<f64>,
    pub spread_mean: Option<f64>,
    pub spread_max: Option<f64>,
    pub mid_price_mean: Option<f64>,
    pub ask_vol_sum_5_mean: Option<f64>,
    pub bid_vol_sum_5_mean: Option<f64>,
}

pub fn analyze_ticks_vec(rows: &[TickData], workload: Workload) -> TickAnalysisSummary {
    match workload {
        Workload::BasicScan => analyze_basic_scan_vec(rows),
        Workload::MixedOrderBook => analyze_mixed_orderbook_vec(rows),
    }
}
```

Implementation notes:
- keep each workload to a single sequential scan
- skip rows that do not satisfy the spec's validity filter
- compute means from explicit `(sum, count)` accumulators
- for the mixed workload, only count rows where both top-of-book prices exist when producing spread and mid-price aggregates
- sum five-level volumes by iterating the `[Option<u32>; 5]` arrays and ignoring `None`

- [ ] **Step 4: Run the native-analysis tests again**

Run: `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence vec_ -- -q`

Expected: PASS for both `vec_basic_scan_matches_expected_summary` and `vec_mixed_orderbook_matches_expected_summary`

- [ ] **Step 5: Commit the native-analysis support**

```bash
git add benches/common/tick_analysis.rs tests/tick_benchmark_equivalence.rs
git commit -m "feat: add native tick benchmark analysis"
```

### Task 3: Implement Polars analysis and correctness equality checks

**Files:**
- Modify: `benches/common/tick_analysis.rs`
- Modify: `tests/tick_benchmark_equivalence.rs`

- [ ] **Step 1: Write failing equivalence tests for the repository sample**

Extend `tests/tick_benchmark_equivalence.rs` with summary comparison tests for both workloads plus a tolerance helper.

```rust
use tick_analysis::{
    analyze_ticks_polars, analyze_ticks_vec, assert_summary_close, load_tick_dataframe,
    load_tick_structs, Workload,
};

#[test]
fn sample_basic_scan_vec_and_polars_match() {
    let rows = load_tick_structs().expect("tick structs");
    let df = load_tick_dataframe().expect("tick dataframe");

    let vec_summary = analyze_ticks_vec(&rows, Workload::BasicScan);
    let polars_summary = analyze_ticks_polars(&df, Workload::BasicScan).expect("polars summary");

    assert_summary_close(&vec_summary, &polars_summary, 1e-9);
}

#[test]
fn sample_mixed_orderbook_vec_and_polars_match() {
    let rows = load_tick_structs().expect("tick structs");
    let df = load_tick_dataframe().expect("tick dataframe");

    let vec_summary = analyze_ticks_vec(&rows, Workload::MixedOrderBook);
    let polars_summary = analyze_ticks_polars(&df, Workload::MixedOrderBook).expect("polars summary");

    assert_summary_close(&vec_summary, &polars_summary, 1e-9);
}
```

- [ ] **Step 2: Run the equivalence tests to verify they fail**

Run: `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence sample_ -- -q`

Expected: FAIL with missing `analyze_ticks_polars` and `assert_summary_close`.

- [ ] **Step 3: Implement Polars workload analysis and tolerance-aware equality**

Extend `benches/common/tick_analysis.rs` with:

```rust
use polars::prelude::*;

pub fn analyze_ticks_polars(
    df: &DataFrame,
    workload: Workload,
) -> Result<TickAnalysisSummary, PolarsError> {
    match workload {
        Workload::BasicScan => analyze_basic_scan_polars(df),
        Workload::MixedOrderBook => analyze_mixed_orderbook_polars(df),
    }
}

pub fn assert_summary_close(
    left: &TickAnalysisSummary,
    right: &TickAnalysisSummary,
    tolerance: f64,
) {
    assert_eq!(left.row_count, right.row_count);
    assert_option_f64_close(left.amount_sum, right.amount_sum, tolerance, "amount_sum");
    // repeat for the other floating-point fields
    assert_eq!(left.volume_sum, right.volume_sum);
}
```

Polars implementation requirements:
- use `LazyFrame` expressions, not Rust-side column extraction
- basic scan should filter on non-null `last_price`, `volume`, and `amount`
- mixed order book should derive:
  - `best_ask` from `col("askPrice").list().get(lit(0), true)`
  - `best_bid` from `col("bidPrice").list().get(lit(0), true)`
  - `spread` and `mid_price` from those derived columns
  - five-level sums from `col("askVol").list().sum()` and `col("bidVol").list().sum()`
- convert the single-row aggregate result into `TickAnalysisSummary` without collecting intermediate Rust vectors

- [ ] **Step 4: Run the full correctness test file**

Run: `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence -v`

Expected: PASS for the smoke test, hand-built native tests, and both repository-sample equivalence tests

- [ ] **Step 5: Commit the correctness layer**

```bash
git add benches/common/tick_analysis.rs tests/tick_benchmark_equivalence.rs
git commit -m "test: verify vec and polars tick summaries match"
```

### Task 4: Wire the Criterion benchmark suite

**Files:**
- Create: `benches/tick_benchmark.rs`
- Modify: `benches/common/tick_analysis.rs`

- [ ] **Step 1: Write the failing benchmark target**

Create `benches/tick_benchmark.rs` that references the shared benchmark helpers and defines the intended benchmark names, even before all functions exist.

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

#[path = "common/tick_analysis.rs"]
mod tick_analysis;

use tick_analysis::{
    analyze_ticks_polars, analyze_ticks_vec, assert_summary_close, load_tick_dataframe,
    load_tick_structs, sample_tick_path, Workload,
};

fn bench_tick(c: &mut Criterion) {
    let mut group = c.benchmark_group("tick");
    // parse_only
    // analyze_only/basic_scan
    // analyze_only/mixed_orderbook
    // end_to_end/basic_scan
    // end_to_end/mixed_orderbook
    group.finish();
}

criterion_group!(benches, bench_tick);
criterion_main!(benches);
```

- [ ] **Step 2: Run benchmark compilation to verify it fails**

Run: `cargo bench -p qmt-parser --features polars --bench tick_benchmark --no-run`

Expected: FAIL until the benchmark file references compile cleanly and preflight helpers are complete.

- [ ] **Step 3: Implement the benchmark groups and preflight assertions**

Fill in `benches/tick_benchmark.rs` with:

```rust
fn bench_tick(c: &mut Criterion) {
    let sample_path = sample_tick_path();

    let preloaded_rows = load_tick_structs().expect("tick structs");
    let preloaded_df = load_tick_dataframe().expect("tick dataframe");

    let vec_basic = analyze_ticks_vec(&preloaded_rows, Workload::BasicScan);
    let polars_basic = analyze_ticks_polars(&preloaded_df, Workload::BasicScan).expect("polars basic");
    assert_summary_close(&vec_basic, &polars_basic, 1e-9);

    let vec_mixed = analyze_ticks_vec(&preloaded_rows, Workload::MixedOrderBook);
    let polars_mixed = analyze_ticks_polars(&preloaded_df, Workload::MixedOrderBook).expect("polars mixed");
    assert_summary_close(&vec_mixed, &polars_mixed, 1e-9);

    let mut group = c.benchmark_group("tick");

    group.bench_function("parse_only/vec", |b| {
        b.iter(|| black_box(qmt_parser::parse_ticks_to_structs(black_box(&sample_path)).unwrap()))
    });

    group.bench_function("parse_only/polars", |b| {
        b.iter(|| black_box(qmt_parser::parse_ticks_to_dataframe(black_box(&sample_path)).unwrap()))
    });

    group.bench_function("analyze_only/basic_scan/vec", |b| {
        b.iter(|| black_box(analyze_ticks_vec(black_box(&preloaded_rows), Workload::BasicScan)))
    });

    // add the remaining analyze_only and end_to_end cases

    group.finish();
}
```

Implementation notes:
- keep benchmark names aligned with the spec
- use `black_box` for both inputs and outputs
- build `analyze_only` inputs once, outside timed closures
- keep `end_to_end` parse plus analysis entirely inside the closure

- [ ] **Step 4: Run benchmark compilation and one real benchmark pass**

Run: `cargo bench -p qmt-parser --features polars --bench tick_benchmark --no-run`

Expected: PASS, producing a compiled benchmark target

Run: `cargo bench -p qmt-parser --features polars --bench tick_benchmark`

Expected: PASS, with Criterion output listing all eight benchmark cases from the spec

- [ ] **Step 5: Commit the benchmark suite**

```bash
git add benches/common/tick_analysis.rs benches/tick_benchmark.rs
git commit -m "feat: add tick vec vs polars benchmarks"
```

### Task 5: Document benchmark usage and run final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Write the failing documentation check**

Confirm the README does not yet document the benchmark command or scope.

Run: `rg -n "cargo bench -p qmt-parser --features polars --bench tick_benchmark|single-symbol, single-day tick benchmark" README.md`

Expected: no matches

- [ ] **Step 2: Update README with a concise benchmark section**

Add a short section near the API/usage material:

```md
## Benchmark

This repository includes a Criterion benchmark for the sample tick file at
`data/000001-20250529-tick.dat`.

Run:

```bash
cargo bench -p qmt-parser --features polars --bench tick_benchmark
```

Scope:
- single-symbol, single-day tick sample
- compares `parse_ticks_to_structs` + native Rust analysis vs `parse_ticks_to_dataframe` + Polars analysis
- intended for repository regression tracking, not broad claims about all analytical workloads
```

- [ ] **Step 3: Run documentation and code verification**

Run: `rg -n "cargo bench -p qmt-parser --features polars --bench tick_benchmark|single-symbol, single-day tick sample" README.md`

Expected: matches the new benchmark section

Run: `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence -v`

Expected: PASS

Run: `cargo bench -p qmt-parser --features polars --bench tick_benchmark --no-run`

Expected: PASS

- [ ] **Step 4: Run one final end-to-end benchmark invocation**

Run: `cargo bench -p qmt-parser --features polars --bench tick_benchmark`

Expected: PASS with Criterion output for:
- `tick/parse_only/vec`
- `tick/parse_only/polars`
- `tick/analyze_only/basic_scan/vec`
- `tick/analyze_only/basic_scan/polars`
- `tick/analyze_only/mixed_orderbook/vec`
- `tick/analyze_only/mixed_orderbook/polars`
- `tick/end_to_end/basic_scan/vec`
- `tick/end_to_end/basic_scan/polars`
- `tick/end_to_end/mixed_orderbook/vec`
- `tick/end_to_end/mixed_orderbook/polars`

- [ ] **Step 5: Commit the documentation and verification pass**

```bash
git add README.md
git commit -m "docs: document tick benchmark usage"
```

## Final Verification Checklist

- [ ] `cargo test -p qmt-parser --features polars --test tick_benchmark_equivalence -v`
- [ ] `cargo bench -p qmt-parser --features polars --bench tick_benchmark --no-run`
- [ ] `cargo bench -p qmt-parser --features polars --bench tick_benchmark`
- [ ] Confirm benchmark names match the design spec
- [ ] Confirm `Vec` and `Polars` summaries are checked for equality before timing

## Notes For The Implementer

- If Polars list-column expressions behave differently than expected in 0.52, keep the benchmark semantics aligned with the spec before optimizing expression style.
- If benchmark compile time becomes painful, resist moving helpers into production code just for convenience; benchmark-local support is the preferred boundary.
- If actual benchmark output names differ slightly from the plan because of Criterion grouping constraints, keep them predictably mappable to the spec and update the README accordingly.
