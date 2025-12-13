//! src/lib.rs

pub mod day;
pub mod error;
pub mod min;
pub mod tick;

// 顶层 API 重导出，简化调用
pub use day::{DailyKlineData, DailyReader, parse_daily_to_dataframe, parse_daily_to_structs};
pub use error::{DailyParseError, MinParseError, TickParseError};
pub use min::{MinKlineData, MinReader, parse_min_to_dataframe, parse_min_to_structs};
pub use tick::{TickData, TickReader, parse_ticks_to_dataframe, parse_ticks_to_structs};
