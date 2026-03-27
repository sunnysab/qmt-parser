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
