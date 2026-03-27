use std::path::PathBuf;

use polars::prelude::*;
use qmt_parser::{TickData, TickParseError, parse_ticks_to_dataframe, parse_ticks_to_structs};

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

pub fn sample_tick_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/000001-20250529-tick.dat")
}

pub fn load_tick_structs() -> Result<Vec<TickData>, TickParseError> {
    parse_ticks_to_structs(sample_tick_path())
}

pub fn load_tick_dataframe() -> Result<DataFrame, TickParseError> {
    parse_ticks_to_dataframe(sample_tick_path())
}

pub fn analyze_ticks_vec(rows: &[TickData], workload: Workload) -> TickAnalysisSummary {
    match workload {
        Workload::BasicScan => analyze_basic_scan_vec(rows),
        Workload::MixedOrderBook => analyze_mixed_orderbook_vec(rows),
    }
}

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
    assert_eq!(left.row_count, right.row_count, "row_count mismatch");
    assert_eq!(left.volume_sum, right.volume_sum, "volume_sum mismatch");
    assert_option_f64_close(left.amount_sum, right.amount_sum, tolerance, "amount_sum");
    assert_option_f64_close(
        left.last_price_min,
        right.last_price_min,
        tolerance,
        "last_price_min",
    );
    assert_option_f64_close(
        left.last_price_max,
        right.last_price_max,
        tolerance,
        "last_price_max",
    );
    assert_option_f64_close(
        left.last_price_mean,
        right.last_price_mean,
        tolerance,
        "last_price_mean",
    );
    assert_option_f64_close(left.spread_mean, right.spread_mean, tolerance, "spread_mean");
    assert_option_f64_close(left.spread_max, right.spread_max, tolerance, "spread_max");
    assert_option_f64_close(
        left.mid_price_mean,
        right.mid_price_mean,
        tolerance,
        "mid_price_mean",
    );
    assert_option_f64_close(
        left.ask_vol_sum_5_mean,
        right.ask_vol_sum_5_mean,
        tolerance,
        "ask_vol_sum_5_mean",
    );
    assert_option_f64_close(
        left.bid_vol_sum_5_mean,
        right.bid_vol_sum_5_mean,
        tolerance,
        "bid_vol_sum_5_mean",
    );
}

fn analyze_basic_scan_vec(rows: &[TickData]) -> TickAnalysisSummary {
    let mut row_count = 0_u64;
    let mut amount_sum = 0.0_f64;
    let mut volume_sum = 0_u64;
    let mut last_price_sum = 0.0_f64;
    let mut last_price_min: Option<f64> = None;
    let mut last_price_max: Option<f64> = None;

    for row in rows {
        let (Some(last_price), Some(amount), Some(volume)) = (row.last_price, row.amount, row.volume) else {
            continue;
        };

        row_count += 1;
        amount_sum += amount;
        volume_sum += volume;
        last_price_sum += last_price;
        last_price_min = Some(last_price_min.map_or(last_price, |current| current.min(last_price)));
        last_price_max = Some(last_price_max.map_or(last_price, |current| current.max(last_price)));
    }

    TickAnalysisSummary {
        row_count,
        amount_sum: (row_count > 0).then_some(amount_sum),
        volume_sum: (row_count > 0).then_some(volume_sum),
        last_price_min,
        last_price_max,
        last_price_mean: (row_count > 0).then_some(last_price_sum / row_count as f64),
        spread_mean: None,
        spread_max: None,
        mid_price_mean: None,
        ask_vol_sum_5_mean: None,
        bid_vol_sum_5_mean: None,
    }
}

fn analyze_mixed_orderbook_vec(rows: &[TickData]) -> TickAnalysisSummary {
    let mut row_count = 0_u64;
    let mut spread_sum = 0.0_f64;
    let mut spread_max: Option<f64> = None;
    let mut mid_price_sum = 0.0_f64;
    let mut ask_vol_sum = 0.0_f64;
    let mut bid_vol_sum = 0.0_f64;

    for row in rows {
        let (Some(_last_price), Some(_amount), Some(_volume)) = (row.last_price, row.amount, row.volume) else {
            continue;
        };
        let (Some(best_ask), Some(best_bid)) = (row.ask_prices[0], row.bid_prices[0]) else {
            continue;
        };

        let spread = best_ask - best_bid;
        let mid_price = (best_ask + best_bid) / 2.0;
        let ask_sum_5 = sum_levels(&row.ask_vols) as f64;
        let bid_sum_5 = sum_levels(&row.bid_vols) as f64;

        row_count += 1;
        spread_sum += spread;
        spread_max = Some(spread_max.map_or(spread, |current| current.max(spread)));
        mid_price_sum += mid_price;
        ask_vol_sum += ask_sum_5;
        bid_vol_sum += bid_sum_5;
    }

    TickAnalysisSummary {
        row_count,
        amount_sum: None,
        volume_sum: None,
        last_price_min: None,
        last_price_max: None,
        last_price_mean: None,
        spread_mean: (row_count > 0).then_some(spread_sum / row_count as f64),
        spread_max,
        mid_price_mean: (row_count > 0).then_some(mid_price_sum / row_count as f64),
        ask_vol_sum_5_mean: (row_count > 0).then_some(ask_vol_sum / row_count as f64),
        bid_vol_sum_5_mean: (row_count > 0).then_some(bid_vol_sum / row_count as f64),
    }
}

fn sum_levels(levels: &[Option<u32>; 5]) -> u32 {
    levels.iter().flatten().copied().sum()
}

fn analyze_basic_scan_polars(df: &DataFrame) -> Result<TickAnalysisSummary, PolarsError> {
    let valid_trade = valid_trade_filter();
    let summary = df
        .clone()
        .lazy()
        .filter(valid_trade)
        .select([
            len().cast(DataType::UInt64).alias("row_count"),
            col("amount").sum().alias("amount_sum"),
            col("volume").sum().alias("volume_sum"),
            col("last_price").min().alias("last_price_min"),
            col("last_price").max().alias("last_price_max"),
            col("last_price").mean().alias("last_price_mean"),
        ])
        .collect()?;

    Ok(TickAnalysisSummary {
        row_count: get_u64_value(&summary, "row_count")?.unwrap_or(0),
        amount_sum: get_f64_value(&summary, "amount_sum")?,
        volume_sum: get_u64_value(&summary, "volume_sum")?,
        last_price_min: get_f64_value(&summary, "last_price_min")?,
        last_price_max: get_f64_value(&summary, "last_price_max")?,
        last_price_mean: get_f64_value(&summary, "last_price_mean")?,
        spread_mean: None,
        spread_max: None,
        mid_price_mean: None,
        ask_vol_sum_5_mean: None,
        bid_vol_sum_5_mean: None,
    })
}

fn analyze_mixed_orderbook_polars(df: &DataFrame) -> Result<TickAnalysisSummary, PolarsError> {
    let summary = df
        .clone()
        .lazy()
        .filter(valid_trade_filter())
        .with_columns([
            col("askPrice")
                .list()
                .get(lit(0), true)
                .alias("best_ask"),
            col("bidPrice")
                .list()
                .get(lit(0), true)
                .alias("best_bid"),
            col("askVol").list().sum().alias("ask_vol_sum_5"),
            col("bidVol").list().sum().alias("bid_vol_sum_5"),
        ])
        .filter(col("best_ask").is_not_null().and(col("best_bid").is_not_null()))
        .with_columns([
            (col("best_ask") - col("best_bid")).alias("spread"),
            ((col("best_ask") + col("best_bid")) / lit(2.0)).alias("mid_price"),
        ])
        .select([
            len().cast(DataType::UInt64).alias("row_count"),
            col("spread").mean().alias("spread_mean"),
            col("spread").max().alias("spread_max"),
            col("mid_price").mean().alias("mid_price_mean"),
            col("ask_vol_sum_5").mean().alias("ask_vol_sum_5_mean"),
            col("bid_vol_sum_5").mean().alias("bid_vol_sum_5_mean"),
        ])
        .collect()?;

    Ok(TickAnalysisSummary {
        row_count: get_u64_value(&summary, "row_count")?.unwrap_or(0),
        amount_sum: None,
        volume_sum: None,
        last_price_min: None,
        last_price_max: None,
        last_price_mean: None,
        spread_mean: get_f64_value(&summary, "spread_mean")?,
        spread_max: get_f64_value(&summary, "spread_max")?,
        mid_price_mean: get_f64_value(&summary, "mid_price_mean")?,
        ask_vol_sum_5_mean: get_f64_value(&summary, "ask_vol_sum_5_mean")?,
        bid_vol_sum_5_mean: get_f64_value(&summary, "bid_vol_sum_5_mean")?,
    })
}

fn valid_trade_filter() -> Expr {
    col("last_price")
        .is_not_null()
        .and(col("volume").is_not_null())
        .and(col("amount").is_not_null())
}

fn get_f64_value(df: &DataFrame, name: &str) -> Result<Option<f64>, PolarsError> {
    match df.column(name)?.get(0)? {
        AnyValue::Null => Ok(None),
        AnyValue::Float64(value) => Ok(Some(value)),
        AnyValue::Float32(value) => Ok(Some(value as f64)),
        AnyValue::UInt64(value) => Ok(Some(value as f64)),
        AnyValue::UInt32(value) => Ok(Some(value as f64)),
        AnyValue::Int64(value) => Ok(Some(value as f64)),
        AnyValue::Int32(value) => Ok(Some(value as f64)),
        value => Err(PolarsError::ComputeError(
            format!("unexpected value for {name}: {value:?}").into(),
        )),
    }
}

fn get_u64_value(df: &DataFrame, name: &str) -> Result<Option<u64>, PolarsError> {
    match df.column(name)?.get(0)? {
        AnyValue::Null => Ok(None),
        AnyValue::UInt64(value) => Ok(Some(value)),
        AnyValue::UInt32(value) => Ok(Some(value as u64)),
        AnyValue::Int64(value) if value >= 0 => Ok(Some(value as u64)),
        AnyValue::Int32(value) if value >= 0 => Ok(Some(value as u64)),
        value => Err(PolarsError::ComputeError(
            format!("unexpected value for {name}: {value:?}").into(),
        )),
    }
}

fn assert_option_f64_close(left: Option<f64>, right: Option<f64>, tolerance: f64, field: &str) {
    match (left, right) {
        (Some(left), Some(right)) => {
            assert!(
                (left - right).abs() <= tolerance,
                "{field} mismatch: left={left} right={right} tolerance={tolerance}"
            );
        }
        (None, None) => {}
        _ => panic!("{field} mismatch: left={left:?} right={right:?}"),
    }
}
