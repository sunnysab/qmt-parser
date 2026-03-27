#![cfg(feature = "polars")]

#[path = "../benches/common/tick_analysis.rs"]
mod tick_analysis;

use qmt_parser::TickData;
use tick_analysis::{
    Workload, analyze_ticks_polars, analyze_ticks_vec, assert_summary_close, load_tick_dataframe,
    load_tick_structs, sample_tick_path,
};

fn assert_option_f64_eq(left: Option<f64>, right: Option<f64>) {
    match (left, right) {
        (Some(left), Some(right)) => assert!((left - right).abs() < 1e-9, "{left} != {right}"),
        (None, None) => {}
        _ => panic!("mismatched optional floats: left={left:?} right={right:?}"),
    }
}

#[test]
fn sample_tick_inputs_are_non_empty() {
    let path = sample_tick_path();
    assert!(
        path.exists(),
        "missing sample tick file: {}",
        path.display()
    );

    let rows = load_tick_structs().expect("tick structs should parse");
    assert!(!rows.is_empty(), "tick struct sample should not be empty");

    let df = load_tick_dataframe().expect("tick dataframe should parse");
    assert!(df.height() > 0, "tick dataframe sample should not be empty");
}

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
        make_tick(
            Some(10.0),
            Some(100.0),
            Some(10),
            Some(10.1),
            Some(9.9),
            [Some(1), Some(2), None, None, None],
            [Some(3), None, None, None, None],
        ),
        make_tick(
            Some(12.0),
            Some(300.0),
            Some(20),
            Some(12.1),
            Some(11.9),
            [Some(4), Some(5), None, None, None],
            [Some(6), Some(7), None, None, None],
        ),
        make_tick(
            None,
            Some(400.0),
            Some(30),
            None,
            None,
            [None; 5],
            [None; 5],
        ),
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
    let rows = vec![
        make_tick(
            Some(10.0),
            Some(100.0),
            Some(10),
            Some(10.1),
            Some(9.9),
            [Some(1), Some(2), None, None, None],
            [Some(3), None, None, None, None],
        ),
        make_tick(
            Some(12.0),
            Some(300.0),
            Some(20),
            Some(12.1),
            Some(11.9),
            [Some(4), Some(5), None, None, None],
            [Some(6), Some(7), None, None, None],
        ),
        make_tick(
            Some(13.0),
            Some(500.0),
            Some(50),
            None,
            Some(12.9),
            [None; 5],
            [None; 5],
        ),
    ];

    let summary = analyze_ticks_vec(&rows, Workload::MixedOrderBook);

    assert_eq!(summary.row_count, 2);
    assert_option_f64_eq(summary.spread_mean, Some(0.2));
    assert_option_f64_eq(summary.spread_max, Some(0.2));
    assert_option_f64_eq(summary.mid_price_mean, Some(11.0));
    assert_option_f64_eq(summary.ask_vol_sum_5_mean, Some(6.0));
    assert_option_f64_eq(summary.bid_vol_sum_5_mean, Some(8.0));
}

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
    let polars_summary =
        analyze_ticks_polars(&df, Workload::MixedOrderBook).expect("polars summary");

    assert_summary_close(&vec_summary, &polars_summary, 1e-9);
}
