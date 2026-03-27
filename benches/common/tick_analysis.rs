use std::path::PathBuf;

use polars::prelude::DataFrame;
use qmt_parser::{TickData, TickParseError, parse_ticks_to_dataframe, parse_ticks_to_structs};

pub fn sample_tick_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/000001-20250529-tick.dat")
}

pub fn load_tick_structs() -> Result<Vec<TickData>, TickParseError> {
    parse_ticks_to_structs(sample_tick_path())
}

pub fn load_tick_dataframe() -> Result<DataFrame, TickParseError> {
    parse_ticks_to_dataframe(sample_tick_path())
}
