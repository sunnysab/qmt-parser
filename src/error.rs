use thiserror::Error;

#[cfg(feature = "polars")]
use polars::error::PolarsError;

/// Tick 解析错误
#[derive(Error, Debug)]
pub enum TickParseError {
    #[error("文件路径不能为空")]
    EmptyPath,
    #[error("文件必须是.dat或.DAT格式: {0}")]
    InvalidExtension(String),
    #[error("无法从文件名解析日期")]
    InvalidFileName,
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "polars")]
    #[error("Polars错误: {0}")]
    Polars(#[from] PolarsError),
}

/// 1分钟线解析错误
#[derive(Error, Debug)]
pub enum MinParseError {
    #[error("文件路径不能为空")]
    EmptyPath,
    #[error("文件必须是.dat或.DAT格式: {0}")]
    InvalidExtension(String),
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "polars")]
    #[error("Polars错误: {0}")]
    Polars(#[from] PolarsError),
}

/// 日线解析错误
#[derive(Error, Debug)]
pub enum DailyParseError {
    #[error("文件路径不能为空")]
    EmptyPath,
    #[error("文件必须是.dat或.DAT格式: {0}")]
    InvalidExtension(String),
    #[error("开始日期格式错误: {0}")]
    InvalidStartDate(String),
    #[error("结束日期格式错误: {0}")]
    InvalidEndDate(String),
    #[error("无效的时间戳")]
    InvalidTimestamp,
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "polars")]
    #[error("Polars错误: {0}")]
    Polars(#[from] PolarsError),
}

#[deprecated(note = "Use FinanceError instead; FinanceParseError is now an alias for compatibility.")]
pub type FinanceParseError = crate::finance::FinanceError;
