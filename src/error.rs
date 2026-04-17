//! 通用错误类型。
//!
//! 这些错误主要用于 tick、分钟线和日线三个历史行情解析模块。

use thiserror::Error;

#[cfg(feature = "polars")]
use polars::error::PolarsError;

/// Tick 解析错误。
#[derive(Error, Debug)]
pub enum TickParseError {
    /// 输入路径为空。
    #[error("文件路径不能为空")]
    EmptyPath,
    /// 输入文件扩展名不是 `.dat` 或 `.DAT`。
    #[error("文件必须是.dat或.DAT格式: {0}")]
    InvalidExtension(String),
    /// 无法从文件路径中提取合法的日期元数据。
    #[error("无法从文件名解析日期")]
    InvalidFileName,
    /// 底层文件读取或字节解析失败。
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    /// Polars 侧构建 `DataFrame` 失败。
    #[cfg(feature = "polars")]
    #[error("Polars错误: {0}")]
    Polars(#[from] PolarsError),
}

/// xtquant 本地资料文件解析错误。
#[derive(Debug, Error)]
pub enum MetadataParseError {
    /// I/O 失败。
    #[error("failed to read metadata file: {0}")]
    Io(#[from] std::io::Error),
    /// 文件中没有可用记录。
    #[error("no records parsed from {0}")]
    NoRecords(&'static str),
}

/// 1 分钟线解析错误。
#[derive(Error, Debug)]
pub enum MinParseError {
    /// 输入路径为空。
    #[error("文件路径不能为空")]
    EmptyPath,
    /// 输入文件扩展名不是 `.dat` 或 `.DAT`。
    #[error("文件必须是.dat或.DAT格式: {0}")]
    InvalidExtension(String),
    /// 底层文件读取或字节解析失败。
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    /// Polars 侧构建 `DataFrame` 失败。
    #[cfg(feature = "polars")]
    #[error("Polars错误: {0}")]
    Polars(#[from] PolarsError),
}

/// 日线解析错误。
#[derive(Error, Debug)]
pub enum DailyParseError {
    /// 输入路径为空。
    #[error("文件路径不能为空")]
    EmptyPath,
    /// 输入文件扩展名不是 `.dat` 或 `.DAT`。
    #[error("文件必须是.dat或.DAT格式: {0}")]
    InvalidExtension(String),
    /// 起始日期字符串无法按 `YYYYMMDD` 解析。
    #[error("开始日期格式错误: {0}")]
    InvalidStartDate(String),
    /// 结束日期字符串无法按 `YYYYMMDD` 解析。
    #[error("结束日期格式错误: {0}")]
    InvalidEndDate(String),
    /// 文件中的时间戳值非法或超出预期范围。
    #[error("无效的时间戳")]
    InvalidTimestamp,
    /// 底层文件读取或字节解析失败。
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    /// Polars 侧构建 `DataFrame` 失败。
    #[cfg(feature = "polars")]
    #[error("Polars错误: {0}")]
    Polars(#[from] PolarsError),
}
