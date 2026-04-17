//! 基于 QMT `datadir` 根目录的自动发现入口。
//!
//! 这个模块把“文件路径拼装/发现”和“实际解析”拆开：
//!
//! - [`QmtDataDir`] 负责从 datadir 发现 tick、分钟线、日线、财务、分红和 metadata 文件
//! - 具体二进制解析仍委托给现有模块

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::NaiveDate;

use crate::day::{
    DailyKlineData, parse_daily_file_to_structs, parse_daily_to_structs,
    parse_daily_to_structs_in_range,
};
use crate::dividend::DividendDb;
use crate::error::DataDirError;
use crate::finance::{FileType, FinanceReader, FinanceRecord};
use crate::metadata::{
    load_holidays_from_root, load_industry_from_root, load_sector_names_from_root,
    load_sector_weight_index_from_root, load_sector_weight_members_from_root,
    load_sectorlist_from_root,
};
use crate::min::{MinKlineData, parse_min_to_structs};
use crate::tick::{TickData, parse_ticks_to_structs};

#[cfg(feature = "polars")]
use crate::day::{
    parse_daily_file_to_dataframe, parse_daily_to_dataframe, parse_daily_to_dataframe_in_range,
};
#[cfg(feature = "polars")]
use crate::min::parse_min_to_dataframe;
#[cfg(feature = "polars")]
use crate::tick::parse_ticks_to_dataframe;
#[cfg(feature = "polars")]
use polars::prelude::DataFrame;

/// 交易市场枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Market {
    /// 上海市场。
    Sh,
    /// 深圳市场。
    Sz,
    /// 北京市场。
    Bj,
}

impl Market {
    /// 返回 QMT datadir 使用的市场目录名。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sh => "SH",
            Self::Sz => "SZ",
            Self::Bj => "BJ",
        }
    }
}

impl TryFrom<&str> for Market {
    type Error = DataDirError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let normalized = value.trim().to_ascii_uppercase();
        match normalized.as_str() {
            "SH" => Ok(Self::Sh),
            "SZ" => Ok(Self::Sz),
            "BJ" => Ok(Self::Bj),
            _ => Err(DataDirError::InvalidInput(format!(
                "unknown market: {value}"
            ))),
        }
    }
}

/// 解析证券代码字符串，支持 `SZ000001` 和 `000001.SZ` 格式。
pub fn parse_security_code(value: &str) -> Result<(Market, String), DataDirError> {
    let raw = value.trim();
    validate_non_empty("security_code", raw)?;

    if let Some((symbol, market)) = raw.rsplit_once('.') {
        validate_symbol(symbol)?;
        return Ok((Market::try_from(market)?, symbol.to_string()));
    }

    if raw.len() <= 2 {
        return Err(DataDirError::InvalidInput(format!(
            "unsupported security code: {value}"
        )));
    }

    let (market, symbol) = raw.split_at(2);
    validate_symbol(symbol)?;
    Ok((Market::try_from(market)?, symbol.to_string()))
}

/// QMT datadir 根目录句柄。
#[derive(Debug, Clone)]
pub struct QmtDataDir {
    root: PathBuf,
}

impl QmtDataDir {
    /// 创建 datadir 根目录句柄。
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DataDirError> {
        let root = path.as_ref().to_path_buf();
        if !root.is_dir() {
            return Err(DataDirError::InvalidRoot(root));
        }
        Ok(Self { root })
    }

    /// 返回 datadir 根目录。
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 定位 tick 文件路径。
    pub fn tick_path(
        &self,
        market: Market,
        symbol: &str,
        date: &str,
    ) -> Result<PathBuf, DataDirError> {
        validate_symbol(symbol)?;
        validate_date(date)?;
        first_existing(
            "tick file",
            vec![
                self.root
                    .join(market.as_str())
                    .join("0")
                    .join(symbol)
                    .join(format!("{date}.dat")),
                self.root
                    .join(market.as_str())
                    .join("0")
                    .join(symbol)
                    .join(format!("{date}.DAT")),
            ],
        )
    }

    /// 定位 1 分钟线文件路径。
    pub fn min_path(&self, market: Market, symbol: &str) -> Result<PathBuf, DataDirError> {
        validate_symbol(symbol)?;
        first_existing(
            "minute file",
            vec![
                self.root
                    .join(market.as_str())
                    .join("60")
                    .join(format!("{symbol}.dat")),
                self.root
                    .join(market.as_str())
                    .join("60")
                    .join(format!("{symbol}.DAT")),
            ],
        )
    }

    /// 定位日线文件路径。
    pub fn day_path(&self, market: Market, symbol: &str) -> Result<PathBuf, DataDirError> {
        validate_symbol(symbol)?;
        first_existing(
            "daily file",
            vec![
                self.root
                    .join(market.as_str())
                    .join("86400")
                    .join(format!("{symbol}.DAT")),
                self.root
                    .join(market.as_str())
                    .join("86400")
                    .join(format!("{symbol}.dat")),
            ],
        )
    }

    /// 定位财务文件路径。
    pub fn finance_path(&self, symbol: &str, file_type: FileType) -> Result<PathBuf, DataDirError> {
        validate_symbol(symbol)?;
        let file_id = file_type as u16;
        let filename_upper = format!("{symbol}_{file_id}.DAT");
        let filename_lower = format!("{symbol}_{file_id}.dat");
        first_existing(
            "finance file",
            vec![
                self.root.join("financial").join(&filename_upper),
                self.root.join("financial").join(&filename_lower),
                self.root.join("finance").join(&filename_upper),
                self.root.join("finance").join(&filename_lower),
                self.root.join("Finance").join(&filename_upper),
                self.root.join("Finance").join(&filename_lower),
            ],
        )
    }

    /// 定位分红 LevelDB 目录。
    pub fn dividend_db_path(&self) -> Result<PathBuf, DataDirError> {
        first_existing("dividend db", vec![self.root.join("DividData")])
    }

    /// 从 datadir 发现并解析 tick 文件为结构体。
    pub fn parse_ticks_to_structs(
        &self,
        market: Market,
        symbol: &str,
        date: &str,
    ) -> Result<Vec<TickData>, DataDirError> {
        Ok(parse_ticks_to_structs(
            self.tick_path(market, symbol, date)?,
        )?)
    }

    /// 从 datadir 发现并解析 tick 文件为 `DataFrame`。
    #[cfg(feature = "polars")]
    pub fn parse_ticks_to_dataframe(
        &self,
        market: Market,
        symbol: &str,
        date: &str,
    ) -> Result<DataFrame, DataDirError> {
        Ok(parse_ticks_to_dataframe(
            self.tick_path(market, symbol, date)?,
        )?)
    }

    /// 从 datadir 发现并解析 1 分钟线文件为结构体。
    pub fn parse_min_to_structs(
        &self,
        market: Market,
        symbol: &str,
    ) -> Result<Vec<MinKlineData>, DataDirError> {
        Ok(parse_min_to_structs(self.min_path(market, symbol)?)?)
    }

    /// 从 datadir 发现并解析 1 分钟线文件为 `DataFrame`。
    #[cfg(feature = "polars")]
    pub fn parse_min_to_dataframe(
        &self,
        market: Market,
        symbol: &str,
    ) -> Result<DataFrame, DataDirError> {
        Ok(parse_min_to_dataframe(self.min_path(market, symbol)?)?)
    }

    /// 从 datadir 发现并解析整个日线文件为结构体。
    pub fn parse_daily_file_to_structs(
        &self,
        market: Market,
        symbol: &str,
    ) -> Result<Vec<DailyKlineData>, DataDirError> {
        Ok(parse_daily_file_to_structs(self.day_path(market, symbol)?)?)
    }

    /// 从 datadir 发现并按字符串日期范围解析日线为结构体。
    pub fn parse_daily_to_structs(
        &self,
        market: Market,
        symbol: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<DailyKlineData>, DataDirError> {
        Ok(parse_daily_to_structs(
            self.day_path(market, symbol)?,
            start,
            end,
        )?)
    }

    /// 从 datadir 发现并按 typed 日期范围解析日线为结构体。
    pub fn parse_daily_to_structs_in_range(
        &self,
        market: Market,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<Vec<DailyKlineData>, DataDirError> {
        Ok(parse_daily_to_structs_in_range(
            self.day_path(market, symbol)?,
            start,
            end,
        )?)
    }

    /// 从 datadir 发现并解析整个日线文件为 `DataFrame`。
    #[cfg(feature = "polars")]
    pub fn parse_daily_file_to_dataframe(
        &self,
        market: Market,
        symbol: &str,
    ) -> Result<DataFrame, DataDirError> {
        Ok(parse_daily_file_to_dataframe(
            self.day_path(market, symbol)?,
        )?)
    }

    /// 从 datadir 发现并按字符串日期范围解析日线为 `DataFrame`。
    #[cfg(feature = "polars")]
    pub fn parse_daily_to_dataframe(
        &self,
        market: Market,
        symbol: &str,
        start: &str,
        end: &str,
    ) -> Result<DataFrame, DataDirError> {
        Ok(parse_daily_to_dataframe(
            self.day_path(market, symbol)?,
            start,
            end,
        )?)
    }

    /// 从 datadir 发现并按 typed 日期范围解析日线为 `DataFrame`。
    #[cfg(feature = "polars")]
    pub fn parse_daily_to_dataframe_in_range(
        &self,
        market: Market,
        symbol: &str,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<DataFrame, DataDirError> {
        Ok(parse_daily_to_dataframe_in_range(
            self.day_path(market, symbol)?,
            start,
            end,
        )?)
    }

    /// 从 datadir 发现并读取财务文件。
    pub fn read_finance(
        &self,
        symbol: &str,
        file_type: FileType,
    ) -> Result<Vec<FinanceRecord>, DataDirError> {
        Ok(FinanceReader::read_file(
            self.finance_path(symbol, file_type)?,
        )?)
    }

    /// 从 datadir 发现并打开分红数据库。
    pub fn open_dividend_db(&self) -> Result<DividendDb, DataDirError> {
        Ok(DividendDb::new(self.dividend_db_path()?)?)
    }

    /// 从 datadir 发现并加载节假日列表。
    pub fn load_holidays(&self) -> Result<Vec<i64>, DataDirError> {
        Ok(load_holidays_from_root(&self.root)?)
    }

    /// 从 datadir 发现并加载 sector 名称。
    pub fn load_sector_names(&self) -> Result<Vec<String>, DataDirError> {
        Ok(load_sector_names_from_root(&self.root)?)
    }

    /// 从 datadir 发现并加载 `sectorlist.DAT`。
    pub fn load_sectorlist(&self) -> Result<Vec<String>, DataDirError> {
        Ok(load_sectorlist_from_root(&self.root)?)
    }

    /// 从 datadir 发现并加载全部 sector 成员映射。
    pub fn load_sector_weight_members(
        &self,
    ) -> Result<BTreeMap<String, Vec<String>>, DataDirError> {
        Ok(load_sector_weight_members_from_root(&self.root)?)
    }

    /// 从 datadir 发现并加载指定 sector/index 的权重映射。
    pub fn load_sector_weight_index(
        &self,
        index_code: &str,
    ) -> Result<BTreeMap<String, f64>, DataDirError> {
        validate_non_empty("index_code", index_code)?;
        Ok(load_sector_weight_index_from_root(&self.root, index_code)?)
    }

    /// 从 datadir 发现并加载行业成员映射。
    pub fn load_industry(&self) -> Result<BTreeMap<String, Vec<String>>, DataDirError> {
        Ok(load_industry_from_root(&self.root)?)
    }
}

fn validate_symbol(symbol: &str) -> Result<(), DataDirError> {
    validate_non_empty("symbol", symbol)
}

fn validate_date(date: &str) -> Result<(), DataDirError> {
    validate_non_empty("date", date)?;
    if date.len() != 8 || !date.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(DataDirError::InvalidInput(format!(
            "date must be YYYYMMDD, got {date}"
        )));
    }
    Ok(())
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), DataDirError> {
    if value.trim().is_empty() {
        return Err(DataDirError::InvalidInput(format!(
            "{field} cannot be empty"
        )));
    }
    Ok(())
}

fn first_existing(kind: &'static str, tried: Vec<PathBuf>) -> Result<PathBuf, DataDirError> {
    for path in &tried {
        if path.exists() {
            return Ok(path.clone());
        }
    }
    Err(DataDirError::PathNotFound { kind, tried })
}
