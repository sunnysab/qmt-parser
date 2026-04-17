#![warn(missing_docs)]
//! `qmt-parser` 提供面向 MiniQMT / QMT 本地数据目录的解析能力。
//!
//! 这个 crate 主要覆盖三类能力：
//!
//! - 历史行情 `.dat` 文件解析：tick、1 分钟线、日线
//! - 可选的 Polars `DataFrame` 输出
//! - QMT 本地财务与分红数据读取
//!
//! 大多数用户可以直接从 crate 根模块调用重导出的顶层函数，例如
//! [`parse_ticks_to_structs`]、[`parse_min_to_structs`]、
//! [`parse_daily_to_structs`] 或 [`FinanceReader::read_file`]。
//!
//! # Feature Flags
//!
//! - `polars`：默认开启，启用 `parse_*_to_dataframe` 系列接口
//!
//! # Quick Start
//!
//! 解析 tick 文件为结构体：
//!
//! ```no_run
//! use qmt_parser::parse_ticks_to_structs;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let ticks = parse_ticks_to_structs("data/000001-20250529-tick.dat")?;
//! println!("rows = {}", ticks.len());
//! # Ok(())
//! # }
//! ```
//!
//! 解析日线并限制日期范围：
//!
//! ```no_run
//! use qmt_parser::parse_daily_to_structs;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let rows = parse_daily_to_structs("data/day/000001.dat", "20230101", "20231231")?;
//! println!("rows = {}", rows.len());
//! # Ok(())
//! # }
//! ```
//!
//! 在启用 `polars` feature 时生成 `DataFrame`：
//!
//! ```no_run
//! # #[cfg(feature = "polars")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use qmt_parser::parse_min_to_dataframe;
//!
//! let df = parse_min_to_dataframe("data/000001-1m.dat")?;
//! println!("{:?}", df.shape());
//! # Ok(())
//! # }
//! #
//! # #[cfg(not(feature = "polars"))]
//! # fn main() {}
//! ```
//!
//! # Modules
//!
//! - [`tick`]：Tick 分笔解析
//! - [`min`]：1 分钟 K 线解析
//! - [`day`]：日线解析与派生列处理
//! - [`finance`]：财务 `.DAT` 解析
//! - [`dividend`]：分红送配 LevelDB 查询
//! - [`metadata`]：xtquant 本地资料/板块文件解析
//! - [`error`]：公共错误类型

/// 基于 datadir 根目录的自动发现入口。
pub mod datadir;
/// 日线解析与 DataFrame 导出。
pub mod day;
/// 分红送配 LevelDB 查询。
pub mod dividend;
/// 公开错误类型定义。
pub mod error;
/// QMT 财务 `.DAT` 解析。
pub mod finance;
/// xtquant 本地资料文件解析。
pub mod metadata;
/// 1 分钟 K 线解析与 DataFrame 导出。
pub mod min;
/// Tick 分笔解析与 DataFrame 导出。
pub mod tick;

pub use datadir::QmtDataDir;
/// 日线 DataFrame 的输出列名。
pub use day::daily_dataframe_column_names;
pub use day::{DailyKlineData, DailyReader};
#[cfg(feature = "polars")]
pub use day::{
    parse_daily_file_to_dataframe, parse_daily_to_dataframe, parse_daily_to_dataframe_in_range,
};
pub use day::{
    parse_daily_file_to_structs, parse_daily_to_structs, parse_daily_to_structs_in_range,
};
pub use dividend::{DividendDb, DividendError, DividendRecord};
pub use error::{DailyParseError, DataDirError, MetadataParseError, MinParseError, TickParseError};
pub use finance::{FileType, FinanceData, FinanceError, FinanceReader, FinanceRecord, Shareholder};
pub use metadata::{
    load_holidays_from_root, load_holidays_from_standard_paths, load_industry_from_root,
    load_industry_from_standard_paths, load_sector_names_from_root,
    load_sector_names_from_standard_paths, load_sector_weight_index_from_root,
    load_sector_weight_index_from_standard_paths, load_sector_weight_members_from_root,
    load_sector_weight_members_from_standard_paths, load_sectorlist_from_root,
    load_sectorlist_from_standard_paths, parse_holiday_file, parse_industry_file,
    parse_sector_name_file, parse_sector_weight_index, parse_sector_weight_members,
    parse_sectorlist_dat,
};
/// 分钟线 DataFrame 的输出列名。
pub use min::min_dataframe_column_names;
#[cfg(feature = "polars")]
pub use min::parse_min_to_dataframe;
pub use min::{MinKlineData, MinReader, parse_min_to_structs};
#[cfg(feature = "polars")]
pub use tick::parse_ticks_to_dataframe;
pub use tick::{
    FULL_TICK_API_FIELD_NAMES, TICK_DATAFRAME_COLUMN_NAMES, tick_api_field_names,
    tick_dataframe_column_names,
};
pub use tick::{TickData, TickReader, parse_ticks_to_structs};
