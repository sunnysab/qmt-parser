//! src/lib.rs

pub mod tick;

pub mod min;

// 将 tick 模块中的公共项目重新导出，作为库的顶层API
pub use tick::{
    parse_ticks_to_dataframe,
    parse_ticks_to_structs,
    TickData,
    ParseError
};