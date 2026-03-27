//! 1 分钟 K 线解析。
//!
//! 这个模块提供两层对外接口：
//!
//! - [`MinReader`]：按记录流式迭代读取
//! - [`parse_min_to_structs`] / [`parse_min_to_dataframe`]：一次性读完整个文件

use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::Path;

use crate::error::MinParseError;
use byteorder::{LittleEndian, ReadBytesExt};
#[cfg(feature = "polars")]
use polars::datatypes::PlSmallStr;
#[cfg(feature = "polars")]
use polars::prelude::*;

const RECORD_SIZE: usize = 64;
const PRICE_SCALE: f64 = 1000.0;

/// 分钟线 `DataFrame` 输出列名。
pub const MIN_DATAFRAME_COLUMN_NAMES: [&str; 11] = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "settlementPrice",
    "openInterest",
    "preClose",
    "suspendFlag",
];

/// 返回当前分钟线 `DataFrame` 输出列名。
pub fn min_dataframe_column_names() -> &'static [&'static str] {
    &MIN_DATAFRAME_COLUMN_NAMES
}

/// 单条 1 分钟 K 线记录。
#[derive(Debug, Clone)]
pub struct MinKlineData {
    /// 北京时间毫秒时间戳。
    pub timestamp_ms: i64,
    /// 开盘价。
    pub open: f64,
    /// 最高价。
    pub high: f64,
    /// 最低价。
    pub low: f64,
    /// 收盘价。
    pub close: f64,
    /// 成交量。
    pub volume: u32,
    /// 成交额。
    pub amount: f64,
    /// 持仓量。
    pub open_interest: u32,
    /// 文件中记录的昨收价。
    pub pre_close: f64,
}

/// 流式读取分钟线文件的迭代器。
pub struct MinReader<R: Read> {
    reader: BufReader<R>,
    buffer: [u8; RECORD_SIZE],
}

impl MinReader<File> {
    /// 从 `.dat` 文件路径创建分钟线读取器。
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, MinParseError> {
        let path = path.as_ref();
        validate_dat_path(path)?;
        let file = File::open(path)?;
        Ok(Self::new(file))
    }
}

impl<R: Read> MinReader<R> {
    /// 从任意 `Read` 实例构造分钟线读取器。
    pub fn new(reader: R) -> Self {
        MinReader {
            reader: BufReader::new(reader),
            buffer: [0u8; RECORD_SIZE],
        }
    }
}

impl<R: Read> Iterator for MinReader<R> {
    type Item = Result<MinKlineData, MinParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = self.reader.read_exact(&mut self.buffer) {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(MinParseError::Io(err)));
        }

        let mut cursor = Cursor::new(&self.buffer[..]);
        Some(parse_record(&mut cursor).map_err(MinParseError::Io))
    }
}

/// 把分钟线文件完整解析为 `Vec<MinKlineData>`。
///
/// 适合应用层直接消费 typed 结构体。
///
/// # Examples
///
/// ```no_run
/// use qmt_parser::parse_min_to_structs;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let rows = parse_min_to_structs("data/000001-1m.dat")?;
/// println!("rows = {}", rows.len());
/// # Ok(())
/// # }
/// ```
pub fn parse_min_to_structs(path: impl AsRef<Path>) -> Result<Vec<MinKlineData>, MinParseError> {
    let path_ref = path.as_ref();
    let mut reader = MinReader::from_path(path_ref)?;
    let estimated_rows = estimate_rows(path_ref)?;
    let mut out = Vec::with_capacity(estimated_rows);
    for item in &mut reader {
        out.push(item?);
    }
    Ok(out)
}

/// 把分钟线文件完整解析为 Polars `DataFrame`。
///
/// 输出 schema 可通过 [`min_dataframe_column_names`] 获取。
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "polars")]
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qmt_parser::parse_min_to_dataframe;
///
/// let df = parse_min_to_dataframe("data/000001-1m.dat")?;
/// println!("{:?}", df.shape());
/// # Ok(())
/// # }
/// #
/// # #[cfg(not(feature = "polars"))]
/// # fn main() {}
/// ```
#[cfg(feature = "polars")]
pub fn parse_min_to_dataframe(path: impl AsRef<Path>) -> Result<DataFrame, MinParseError> {
    let path_ref = path.as_ref();
    let mut reader = MinReader::from_path(path_ref)?;
    let estimated_rows = estimate_rows(path_ref)?;

    let mut timestamps = Vec::with_capacity(estimated_rows);
    let mut opens = Vec::with_capacity(estimated_rows);
    let mut highs = Vec::with_capacity(estimated_rows);
    let mut lows = Vec::with_capacity(estimated_rows);
    let mut closes = Vec::with_capacity(estimated_rows);
    let mut volumes = Vec::with_capacity(estimated_rows);
    let mut amounts = Vec::with_capacity(estimated_rows);
    let mut open_interests = Vec::with_capacity(estimated_rows);
    let mut pre_closes = Vec::with_capacity(estimated_rows);

    for item in &mut reader {
        let record = item?;
        timestamps.push(record.timestamp_ms);
        opens.push(record.open);
        highs.push(record.high);
        lows.push(record.low);
        closes.push(record.close);
        volumes.push(record.volume);
        amounts.push(record.amount);
        open_interests.push(record.open_interest);
        pre_closes.push(record.pre_close);
    }

    if timestamps.is_empty() {
        return Ok(DataFrame::empty());
    }

    let len = timestamps.len();
    let settlement_prices = Series::new("settlementPrice".into(), vec![0.0f64; len]);
    let suspend_flags = Series::new("suspendFlag".into(), vec![0i32; len]);

    let df = df![
        "timestamp_ms" => timestamps,
        "open" => opens,
        "high" => highs,
        "low" => lows,
        "close" => closes,
        "volume" => volumes,
        "amount" => amounts,
        "settlementPrice" => settlement_prices,
        "openInterest" => open_interests,
        "preClose" => pre_closes,
        "suspendFlag" => suspend_flags,
    ]?;

    let raw_tz = TimeZone::opt_try_new(None::<PlSmallStr>)?;
    let china_tz = TimeZone::opt_try_new(Some("Asia/Shanghai"))?;
    let df = df
        .lazy()
        .with_column(
            col("timestamp_ms")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, raw_tz))
                .dt()
                .convert_time_zone(china_tz.unwrap())
                .alias("time"),
        )
        .select([
            col("time"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("amount"),
            col("settlementPrice"),
            col("openInterest"),
            col("preClose"),
            col("suspendFlag"),
        ])
        .collect()?;

    Ok(df)
}

fn validate_dat_path(path: &Path) -> Result<(), MinParseError> {
    if path.as_os_str().is_empty() {
        return Err(MinParseError::EmptyPath);
    }
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if ext != "dat" {
        return Err(MinParseError::InvalidExtension(path.display().to_string()));
    }
    Ok(())
}

fn estimate_rows(path: &Path) -> Result<usize, MinParseError> {
    let file_len = std::fs::metadata(path)?.len();
    Ok((file_len as usize) / RECORD_SIZE + 1)
}

fn parse_record(cursor: &mut Cursor<&[u8]>) -> std::io::Result<MinKlineData> {
    cursor.set_position(8);
    let ts_seconds = cursor.read_u32::<LittleEndian>()?;
    let open = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;
    let high = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;
    let low = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;
    let close = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;

    cursor.set_position(32);
    let volume = cursor.read_u32::<LittleEndian>()?;

    cursor.set_position(40);
    let amount = cursor.read_u64::<LittleEndian>()? as f64;

    let open_interest = cursor.read_u32::<LittleEndian>()?;

    cursor.set_position(60);
    let pre_close = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;

    Ok(MinKlineData {
        timestamp_ms: ts_seconds as i64 * 1000,
        open,
        high,
        low,
        close,
        volume,
        amount,
        open_interest,
        pre_close,
    })
}

#[cfg(all(test, feature = "polars"))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_min_dataframe() -> Result<(), MinParseError> {
        let test_file = PathBuf::from("data/000000-1m.dat");

        let df = parse_min_to_dataframe(&test_file)?;

        println!("--- Tail ---");
        println!("{}", df.tail(Some(5)));
        assert_eq!(
            df.get_column_names_str().as_slice(),
            min_dataframe_column_names()
        );
        Ok(())
    }
}
