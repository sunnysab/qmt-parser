//! src/lib.rs

pub mod day;
pub mod dividend;
pub mod error;
pub mod finance;
pub mod min;
pub mod tick;

// 顶层 API 重导出，简化调用
pub use day::daily_dataframe_column_names;
#[cfg(feature = "polars")]
pub use day::{parse_daily_file_to_dataframe, parse_daily_kline, parse_daily_to_dataframe, parse_daily_to_dataframe_in_range};
pub use day::{parse_daily_file_to_structs, parse_daily_to_structs, parse_daily_to_structs_in_range};
pub use day::{DailyKlineData, DailyReader};
pub use error::{DailyParseError, MinParseError, TickParseError};
#[allow(deprecated)]
pub use error::FinanceParseError;
pub use finance::{FinanceData, FinanceError, FinanceReader, FinanceRecord, FileType, Shareholder};
pub use min::min_dataframe_column_names;
#[cfg(feature = "polars")]
pub use min::{parse_kline_to_dataframe, parse_min_to_dataframe};
pub use min::{MinKlineData, MinReader, parse_min_to_structs};
pub use tick::{FULL_TICK_API_FIELD_NAMES, TICK_DATAFRAME_COLUMN_NAMES, tick_api_field_names, tick_dataframe_column_names};
#[cfg(feature = "polars")]
pub use tick::parse_ticks_to_dataframe;
pub use tick::{TickData, TickReader, parse_ticks_to_structs};
pub use dividend::{DividendDb, DividendError, DividendRecord};
