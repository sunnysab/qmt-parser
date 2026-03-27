//! src/lib.rs

pub mod day;
pub mod error;
pub mod finance;
pub mod min;
pub mod tick;
mod dividend;

// 顶层 API 重导出，简化调用
pub use day::daily_dataframe_column_names;
pub use day::{DailyKlineData, DailyReader, parse_daily_to_dataframe, parse_daily_to_structs};
pub use error::{DailyParseError, FinanceParseError, MinParseError, TickParseError};
pub use finance::{FinanceData, FinanceError, FinanceReader, FinanceRecord, FileType, Shareholder};
pub use min::min_dataframe_column_names;
pub use min::{MinKlineData, MinReader, parse_min_to_dataframe, parse_min_to_structs};
pub use tick::{FULL_TICK_API_FIELD_NAMES, TICK_DATAFRAME_COLUMN_NAMES, tick_api_field_names, tick_dataframe_column_names};
pub use tick::{TickData, TickReader, parse_ticks_to_dataframe, parse_ticks_to_structs};
pub use dividend::{DividendDb, DividendRecord};
