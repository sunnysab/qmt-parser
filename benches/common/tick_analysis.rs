use std::path::PathBuf;

use polars::prelude::DataFrame;
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
