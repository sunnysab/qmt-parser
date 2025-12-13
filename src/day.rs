use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::Path;

use crate::error::DailyParseError;
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone};
use polars::datatypes::PlSmallStr;
use polars::prelude::*;

const RECORD_SIZE: usize = 64;
const PRICE_SCALE: f64 = 1000.0;
const AMOUNT_SCALE: f64 = 100.0;

/// Level 1: 日线原始结构
#[derive(Debug, Clone)]
pub struct DailyKlineData {
    pub timestamp_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u32,
    pub amount: f64,
    pub open_interest: u32,
    pub file_pre_close: f64,
}

/// Level 2: 日线 Reader，仅做原始读取与日期过滤
pub struct DailyReader<R: Read> {
    reader: BufReader<R>,
    buffer: [u8; RECORD_SIZE],
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tz_offset: FixedOffset,
}

impl DailyReader<File> {
    pub fn from_path(
        path: impl AsRef<Path>,
        start: Option<NaiveDate>,
        end: Option<NaiveDate>,
    ) -> Result<Self, DailyParseError> {
        let path = path.as_ref();
        validate_dat_path(path)?;
        let file = File::open(path)?;
        Ok(Self::new(file, start, end))
    }
}

impl<R: Read> DailyReader<R> {
    pub fn new(reader: R, start: Option<NaiveDate>, end: Option<NaiveDate>) -> Self {
        DailyReader {
            reader: BufReader::new(reader),
            buffer: [0u8; RECORD_SIZE],
            start,
            end,
            tz_offset: FixedOffset::east_opt(8 * 3600).expect("valid offset"),
        }
    }
}

impl<R: Read> Iterator for DailyReader<R> {
    type Item = Result<DailyKlineData, DailyParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Err(err) = self.reader.read_exact(&mut self.buffer) {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(DailyParseError::Io(err)));
            }

            let mut cursor = Cursor::new(&self.buffer[..]);
            match parse_record(&mut cursor, self.start, self.end, self.tz_offset) {
                Ok(Some(record)) => return Some(Ok(record)),
                Ok(None) => continue, // 过滤掉不在时间范围内的记录
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Level 3 API: Vec<Struct>
pub fn parse_daily_to_structs(
    path: impl AsRef<Path>,
    start_date_str: &str,
    end_date_str: &str,
) -> Result<Vec<DailyKlineData>, DailyParseError> {
    let path_ref = path.as_ref();
    let start = parse_date(start_date_str).map_err(|e| DailyParseError::InvalidStartDate(e))?;
    let end = parse_date(end_date_str).map_err(|e| DailyParseError::InvalidEndDate(e))?;
    let mut reader = DailyReader::from_path(path_ref, Some(start), Some(end))?;
    let mut out = Vec::with_capacity(estimate_rows(path_ref)?);
    for item in &mut reader {
        out.push(item?);
    }
    Ok(out)
}

/// Level 3 API: DataFrame（在 Lazy 端处理业务逻辑）
pub fn parse_daily_to_dataframe(
    path: impl AsRef<Path>,
    start_date_str: &str,
    end_date_str: &str,
) -> Result<DataFrame, DailyParseError> {
    let path_ref = path.as_ref();
    let start = parse_date(start_date_str).map_err(|e| DailyParseError::InvalidStartDate(e))?;
    let end = parse_date(end_date_str).map_err(|e| DailyParseError::InvalidEndDate(e))?;
    let mut reader = DailyReader::from_path(path_ref, Some(start), Some(end))?;

    let estimated_rows = estimate_rows(path_ref)?;
    let mut timestamps = Vec::with_capacity(estimated_rows);
    let mut opens = Vec::with_capacity(estimated_rows);
    let mut highs = Vec::with_capacity(estimated_rows);
    let mut lows = Vec::with_capacity(estimated_rows);
    let mut closes = Vec::with_capacity(estimated_rows);
    let mut volumes = Vec::with_capacity(estimated_rows);
    let mut amounts = Vec::with_capacity(estimated_rows);
    let mut open_interests = Vec::with_capacity(estimated_rows);
    let mut file_pre_closes = Vec::with_capacity(estimated_rows);

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
        file_pre_closes.push(record.file_pre_close);
    }

    if timestamps.is_empty() {
        return Ok(DataFrame::empty());
    }

    let df = df![
        "timestamp_ms" => timestamps,
        "open" => opens,
        "high" => highs,
        "low" => lows,
        "close" => closes,
        "volume" => volumes,
        "amount" => amounts,
        "openInterest" => open_interests,
        "file_preClose" => file_pre_closes,
    ]?;

    let raw_tz = polars::prelude::TimeZone::opt_try_new(None::<PlSmallStr>)?;
    let china_tz = polars::prelude::TimeZone::opt_try_new(Some("Asia/Shanghai"))?;
    let df_final = df
        .lazy()
        .sort(["timestamp_ms"], Default::default())
        .with_column(
            (col("volume").eq(lit(0)).and(col("amount").eq(lit(0.0))))
                .cast(DataType::Int32)
                .alias("suspendFlag"),
        )
        .with_column(col("close").shift(lit(1)).alias("calc_pre_close"))
        .with_column(
            when(col("suspendFlag").eq(lit(1)))
                .then(col("close"))
                .otherwise(col("calc_pre_close").fill_null(col("close")))
                .alias("preClose"),
        )
        .with_columns(vec![
            col("timestamp_ms")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, raw_tz))
                .dt()
                .convert_time_zone(china_tz.unwrap())
                .alias("time"),
            lit(0.0).alias("settlementPrice"),
        ])
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

    Ok(df_final)
}

fn validate_dat_path(path: &Path) -> Result<(), DailyParseError> {
    if path.as_os_str().is_empty() {
        return Err(DailyParseError::EmptyPath);
    }
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if ext != "dat" {
        return Err(DailyParseError::InvalidExtension(path.display().to_string()));
    }
    Ok(())
}

fn estimate_rows(path: &Path) -> Result<usize, DailyParseError> {
    let file_len = std::fs::metadata(path)?.len();
    Ok((file_len as usize) / RECORD_SIZE + 1)
}

fn parse_date(date: &str) -> std::result::Result<NaiveDate, String> {
    NaiveDate::parse_from_str(date, "%Y%m%d").map_err(|e| e.to_string())
}

fn parse_record(
    cursor: &mut Cursor<&[u8]>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    tz_offset: FixedOffset,
) -> Result<Option<DailyKlineData>, DailyParseError> {
    cursor.set_position(8);
    let ts_seconds = cursor.read_u32::<LittleEndian>()?;
    let dt_utc = DateTime::from_timestamp(ts_seconds as i64, 0)
        .ok_or(DailyParseError::InvalidTimestamp)?
        .naive_utc();

    let current_date = tz_offset.from_utc_datetime(&dt_utc).date_naive();
    if let Some(start) = start {
        if current_date < start {
            return Ok(None);
        }
    }
    if let Some(end) = end {
        if current_date > end {
            return Ok(None);
        }
    }

    let open = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;
    let high = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;
    let low = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;
    let close = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;

    cursor.set_position(32);
    let volume = cursor.read_u32::<LittleEndian>()?;

    cursor.set_position(40);
    let raw_amount = cursor.read_u64::<LittleEndian>()?;
    let amount = raw_amount as f64 / AMOUNT_SCALE;

    let open_interest = cursor.read_u32::<LittleEndian>()?;

    cursor.set_position(60);
    let file_pre_close = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;

    Ok(Some(DailyKlineData {
        timestamp_ms: ts_seconds as i64 * 1000,
        open,
        high,
        low,
        close,
        volume,
        amount,
        open_interest,
        file_pre_close,
    }))
}

/// 兼容旧命名
pub fn parse_daily_kline(
    path: impl AsRef<Path>,
    start_date_str: &str,
    end_date_str: &str,
) -> Result<DataFrame, DailyParseError> {
    parse_daily_to_dataframe(path, start_date_str, end_date_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_daily_kline() -> Result<(), DailyParseError> {
        let daily_path = PathBuf::from("/mnt/data/trade/qmtdata/datadir/SZ/86400/000001.DAT");

        if !daily_path.exists() {
            println!("测试文件不存在，跳过测试: {:?}", daily_path);
            return Ok(());
        }

        let start = "19910401";
        let end = "19910425";

        let df = parse_daily_to_dataframe(&daily_path, start, end)?;

        println!("--- Daily DataFrame (Shape: {:?}) ---", df.shape());
        println!("{}", df);

        if df.height() > 0 {
            let cols = df.get_column_names();
            assert!(cols.iter().any(|c| c.as_str() == "suspendFlag"));
            assert!(cols.iter().any(|c| c.as_str() == "preClose"));

            if df.height() >= 2 {
                let s_close = df.column("close")?;
                let s_pre = df.column("preClose")?;
                let s_suspend = df.column("suspendFlag")?;

                let close_0 = s_close.f64()?.get(0).unwrap();
                let pre_1 = s_pre.f64()?.get(1).unwrap();
                let suspend_1 = s_suspend.i32()?.get(1).unwrap();

                if suspend_1 == 0 {
                    assert!((pre_1 - close_0).abs() < 0.001, "PreClose calculation logic error");
                }
            }
        }

        Ok(())
    }
}
