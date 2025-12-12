//! src/lib.rs

pub mod day;
pub mod min;
pub mod tick;

// 顶层 API 重导出，简化调用
pub use tick::{ParseError as TickParseError, TickData, TickReader, parse_ticks_to_dataframe, parse_ticks_to_structs};

pub use min::{MinKlineData, MinReader, ParseError as MinParseError, parse_min_to_dataframe, parse_min_to_structs};

pub use day::{
    DailyKlineData, DailyReader, ParseError as DailyParseError, parse_daily_to_dataframe, parse_daily_to_structs,
};
