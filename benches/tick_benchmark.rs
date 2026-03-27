use std::time::Duration;

use std::path::PathBuf;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group};
use qmt_parser::{parse_ticks_to_dataframe, parse_ticks_to_structs};

#[path = "common/tick_analysis.rs"]
mod tick_analysis;

use tick_analysis::{
    Workload, analyze_ticks_polars, analyze_ticks_vec, assert_summary_close, load_tick_dataframe,
    load_tick_structs, sample_tick_path,
};

fn bench_tick(c: &mut Criterion) {
    let sample_path = sample_tick_path();
    let preloaded_rows = load_tick_structs().expect("tick structs should parse");
    let preloaded_df = load_tick_dataframe().expect("tick dataframe should parse");
    let row_count = preloaded_rows.len() as u64;

    let vec_basic = analyze_ticks_vec(&preloaded_rows, Workload::BasicScan);
    let polars_basic =
        analyze_ticks_polars(&preloaded_df, Workload::BasicScan).expect("polars basic summary");
    assert_summary_close(&vec_basic, &polars_basic, 1e-9);

    let vec_mixed = analyze_ticks_vec(&preloaded_rows, Workload::MixedOrderBook);
    let polars_mixed = analyze_ticks_polars(&preloaded_df, Workload::MixedOrderBook)
        .expect("polars mixed summary");
    assert_summary_close(&vec_mixed, &polars_mixed, 1e-9);

    let mut group = c.benchmark_group("tick");
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(row_count));

    group.bench_with_input(
        BenchmarkId::new("parse_only", "vec"),
        &sample_path,
        |b, path| {
            b.iter(|| black_box(parse_ticks_to_structs(black_box(path)).expect("parse vec")))
        },
    );
    group.bench_with_input(
        BenchmarkId::new("parse_only", "polars"),
        &sample_path,
        |b, path| {
            b.iter(|| black_box(parse_ticks_to_dataframe(black_box(path)).expect("parse polars")))
        },
    );

    group.bench_function("analyze_only/basic_scan/vec", |b| {
        b.iter(|| black_box(analyze_ticks_vec(black_box(&preloaded_rows), Workload::BasicScan)))
    });
    group.bench_function("analyze_only/basic_scan/polars", |b| {
        b.iter(|| {
            black_box(
                analyze_ticks_polars(black_box(&preloaded_df), Workload::BasicScan)
                    .expect("analyze polars basic"),
            )
        })
    });
    group.bench_function("analyze_only/mixed_orderbook/vec", |b| {
        b.iter(|| {
            black_box(analyze_ticks_vec(
                black_box(&preloaded_rows),
                Workload::MixedOrderBook,
            ))
        })
    });
    group.bench_function("analyze_only/mixed_orderbook/polars", |b| {
        b.iter(|| {
            black_box(
                analyze_ticks_polars(black_box(&preloaded_df), Workload::MixedOrderBook)
                    .expect("analyze polars mixed"),
            )
        })
    });

    group.bench_with_input(
        BenchmarkId::new("end_to_end/basic_scan", "vec"),
        &sample_path,
        |b, path| {
            b.iter(|| {
                let rows = parse_ticks_to_structs(black_box(path)).expect("parse vec");
                black_box(analyze_ticks_vec(black_box(&rows), Workload::BasicScan))
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("end_to_end/basic_scan", "polars"),
        &sample_path,
        |b, path| {
            b.iter(|| {
                let df = parse_ticks_to_dataframe(black_box(path)).expect("parse polars");
                black_box(
                    analyze_ticks_polars(black_box(&df), Workload::BasicScan)
                        .expect("analyze polars basic"),
                )
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("end_to_end/mixed_orderbook", "vec"),
        &sample_path,
        |b, path| {
            b.iter(|| {
                let rows = parse_ticks_to_structs(black_box(path)).expect("parse vec");
                black_box(analyze_ticks_vec(
                    black_box(&rows),
                    Workload::MixedOrderBook,
                ))
            })
        },
    );
    group.bench_with_input(
        BenchmarkId::new("end_to_end/mixed_orderbook", "polars"),
        &sample_path,
        |b, path| {
            b.iter(|| {
                let df = parse_ticks_to_dataframe(black_box(path)).expect("parse polars");
                black_box(
                    analyze_ticks_polars(black_box(&df), Workload::MixedOrderBook)
                        .expect("analyze polars mixed"),
                )
            })
        },
    );

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_tick
}

fn main() {
    configure_criterion_home();
    benches();
    Criterion::default().configure_from_args().final_summary();
}

fn configure_criterion_home() {
    let criterion_home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/criterion");
    unsafe {
        std::env::set_var("CRITERION_HOME", criterion_home);
    }
}
