# Tick Benchmark Design

## Summary

Design a long-lived benchmark suite for `qmt-parser` that compares the end-to-end performance of two equivalent processing paths on real QMT tick data:

- `Vec` path: `parse_ticks_to_structs` followed by Rust-native analysis over `Vec<TickData>`
- `Polars` path: `parse_ticks_to_dataframe` followed by equivalent analysis over `DataFrame`/`LazyFrame`

The benchmark should answer a practical question for this crate: on the repository's real tick sample, what is the performance difference between row-oriented native structs and column-oriented Polars for the typical "parse then analyze" workflow?

## Goals

- Measure end-to-end cost from `.dat` file to final analysis summary.
- Preserve enough decomposition to explain where time is spent.
- Keep the suite stable enough to retain in the repository and use during future PRs.
- Ensure both paths compute the same outputs before comparing timings.

## Non-Goals

- Measuring peak memory usage in the first version.
- Benchmarking multi-symbol, multi-day, join, sort, or group-by heavy workflows.
- Generalizing conclusions beyond the repository's single-symbol, single-day tick sample.
- Expanding the benchmark to minute or daily bars in the first version.

## Input Data

- Fixed source file: `data/000001-20250529-tick.dat`
- File size at design time: about `843 KiB` (`862272` bytes)
- Estimated row count at design time: about `5988` rows, based on `144` bytes per tick record

The benchmark uses the repository sample directly rather than synthetic or expanded data. This keeps the suite aligned with the crate's current parsing behavior and avoids inventing distribution assumptions for order-book fields.

## Benchmark Scope

The suite compares two paths only:

1. `Vec` path
   - Parse with `parse_ticks_to_structs(path)`
   - Analyze with a dedicated Rust function that scans `&[TickData]`

2. `Polars` path
   - Parse with `parse_ticks_to_dataframe(path)`
   - Analyze with a dedicated Polars function that consumes the returned `DataFrame`

The benchmark must not compare against alternative parsers, alternate file-loading strategies, or custom columnar layouts in this first iteration.

## Benchmark Groups

Three benchmark groups are included:

1. `parse_only`
   - Measures the cost of parsing the file into the target representation.
   - Cases:
     - `tick/parse_only/vec`
     - `tick/parse_only/polars`

2. `analyze_only`
   - Measures the cost of analysis once the target representation already exists.
   - The `Vec<TickData>` and `DataFrame` inputs are prepared outside the benchmark loop.
   - Cases:
     - `tick/analyze_only/basic_scan/vec`
     - `tick/analyze_only/basic_scan/polars`
     - `tick/analyze_only/mixed_orderbook/vec`
     - `tick/analyze_only/mixed_orderbook/polars`

3. `end_to_end`
   - Measures parse plus analysis together.
   - This is the primary decision-making benchmark.
   - Cases:
     - `tick/end_to_end/basic_scan/vec`
     - `tick/end_to_end/basic_scan/polars`
     - `tick/end_to_end/mixed_orderbook/vec`
     - `tick/end_to_end/mixed_orderbook/polars`

`parse_only` and `analyze_only` exist to explain the `end_to_end` results. Any future optimization work should still treat `end_to_end` as the main signal.

## Workloads

### Workload A: Basic Scan

This workload represents a common first-pass statistical scan after parsing.

Filter condition:

- `last_price.is_some()`
- `volume.is_some()`
- `amount.is_some()`

Output metrics:

- valid trade row count
- total `amount`
- total `volume`
- minimum `last_price`
- maximum `last_price`
- mean `last_price`

### Workload B: Mixed Order Book

This workload extends the basic scan with lightweight five-level order-book access so the benchmark does not collapse into a pure scalar-column aggregate.

Base filter condition:

- same validity filter as Basic Scan

Derived fields:

- `best_ask = askPrice[0]`
- `best_bid = bidPrice[0]`
- `spread = best_ask - best_bid` when both exist
- `mid_price = (best_ask + best_bid) / 2.0` when both exist
- `ask_vol_sum_5 = sum(askVol[0..5])` over present values
- `bid_vol_sum_5 = sum(bidVol[0..5])` over present values

Output metrics:

- valid order-book row count
- mean `spread`
- maximum `spread`
- mean `mid_price`
- mean `ask_vol_sum_5`
- mean `bid_vol_sum_5`

## Fairness Rules

To keep the comparison interpretable, both paths must follow these rules:

- Both paths must produce the same final summary values within a documented floating-point tolerance before any timing comparisons are considered valid.
- The `Vec` path may use a single sequential scan per workload and local accumulators, but must not build extra indexes, maps, or copied intermediate tables.
- The `Polars` path must operate on the `DataFrame` returned by this crate and must not convert columns back into Rust vectors for analysis.
- The first version excludes sort, join, and group-by workloads because the current sample is a single symbol on a single trading day; adding those would bias the benchmark toward generic engine capability rather than this crate's actual usage path.
- Order-book access is intentionally limited to first-level extraction and five-level volume summation so both implementations stay straightforward and clearly equivalent.

## Result Model

A shared summary type should be defined for workload outputs so correctness can be asserted directly.

Recommended shape:

- `TickAnalysisSummary`
  - `row_count: u64`
  - `amount_sum: Option<f64>`
  - `volume_sum: Option<u64>`
  - `last_price_min: Option<f64>`
  - `last_price_max: Option<f64>`
  - `last_price_mean: Option<f64>`
  - `spread_mean: Option<f64>`
  - `spread_max: Option<f64>`
  - `mid_price_mean: Option<f64>`
  - `ask_vol_sum_5_mean: Option<f64>`
  - `bid_vol_sum_5_mean: Option<f64>`

The summary may use optional fields so one type can represent both workloads without inventing unused placeholder values.

## Harness Design

- Use `criterion` rather than ad hoc `Instant` timing.
- Benchmarks should live under `benches/`.
- The benchmark target should assume the `polars` feature is enabled. The intended run command is:
  - `cargo bench --features polars`
- Benchmark inputs should use a fixed relative path from the crate root and should fail early with a clear message if the sample file is missing.
- `analyze_only` must prepare parsed inputs outside the timed closure and use `black_box` on both inputs and outputs.
- `end_to_end` must perform parse and analysis inside the timed closure.
- Benchmark naming should make data type, phase, workload, and implementation obvious.

## Verification Requirements

Before relying on any timing result, the benchmark code must verify:

- `Vec` and `Polars` implementations return matching summaries for `basic_scan`
- `Vec` and `Polars` implementations return matching summaries for `mixed_orderbook`
- floating-point comparisons use an explicit tolerance
- the sample row count is non-zero so empty-file fast paths do not invalidate measurements

## Reporting Expectations

The benchmark documentation or module-level comments should state:

- the exact input sample used
- its size and approximate row count
- that this is a single-symbol, single-day tick benchmark
- that conclusions should not be generalized to multi-symbol analytical workloads

Primary metric:

- time per iteration

Optional derived metric:

- records per second

Memory metrics are explicitly deferred to a later iteration.

## Risks And Constraints

- The sample is relatively small, so benchmark variance may be more noticeable than on a larger dataset.
- Because the sample is fixed, results primarily show constant-factor differences on this exact data shape.
- Polars expressions over list columns may require careful formulation to keep order-book logic equivalent to the native Rust implementation.

These are acceptable trade-offs for the first repository-owned benchmark because the goal is stable and interpretable comparison, not exhaustive workload coverage.

## Implementation Boundary

This spec covers benchmark design only. It does not prescribe file/module names for the implementation beyond using the standard `benches/` location, and it does not require any production API changes unless implementation reveals a narrow benchmarking need.

## Recommended Next Step

Create an implementation plan that:

- adds Criterion benchmark infrastructure
- defines shared workload summary and equality checks
- implements `Vec` analysis functions
- implements equivalent Polars analysis functions
- wires benchmark groups and names
- documents how to run the suite
