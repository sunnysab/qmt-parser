use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::Path;

use crate::error::TickParseError;
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{FixedOffset, NaiveDate, TimeZone};
use polars::datatypes::PlSmallStr;
use polars::prelude::*;

const RECORD_SIZE: usize = 144;
const PRICE_SCALE: f64 = 1000.0;
const CALL_AUCTION_PHASE_CODE: u32 = 12;
const QMT_TICK_TIME_OFFSET_MS: u32 = 396_300_000;
const BJ_TICK_TIME_OFFSET_MS: u32 = 50_400_000;

pub const FULL_TICK_API_FIELD_NAMES: [&str; 17] = [
    "lastPrice",
    "amount",
    "volume",
    "pvolume",
    "openInt",
    "stockStatus",
    "lastSettlementPrice",
    "open",
    "high",
    "low",
    "settlementPrice",
    "lastClose",
    "askPrice",
    "bidPrice",
    "askVol",
    "bidVol",
    "timetag",
];

pub const TICK_DATAFRAME_COLUMN_NAMES: [&str; 26] = [
    "market",
    "symbol",
    "date",
    "raw_qmt_timestamp",
    "time",
    "last_price",
    "open",
    "high",
    "low",
    "last_close",
    "amount",
    "volume",
    "pvolume",
    "tickvol",
    "market_phase_status",
    "stockStatus",
    "qmt_status_field_1_raw",
    "qmt_status_field_2_raw",
    "lastSettlementPrice",
    "askPrice",
    "bidPrice",
    "askVol",
    "bidVol",
    "settlementPrice",
    "transactionNum",
    "pe",
];

pub fn tick_api_field_names() -> &'static [&'static str] {
    &FULL_TICK_API_FIELD_NAMES
}

pub fn tick_dataframe_column_names() -> &'static [&'static str] {
    &TICK_DATAFRAME_COLUMN_NAMES
}

/// Level 1: 原始 Tick 结构体 (定长数组)
#[derive(Debug, Clone)]
pub struct TickData {
    pub market: Option<String>,
    pub symbol: String,
    pub date: String,
    pub raw_qmt_timestamp: u32,
    pub market_phase_status: u32,
    pub last_price: Option<f64>,
    pub last_close: f64,
    pub amount: Option<f64>,
    pub volume: Option<u64>,
    pub ask_prices: [Option<f64>; 5],
    pub ask_vols: [Option<u32>; 5],
    pub bid_prices: [Option<f64>; 5],
    pub bid_vols: [Option<u32>; 5],
    pub qmt_status_field_1_raw: u32,
    pub qmt_status_field_2_raw: u32,
}

/// Level 2: 迭代器 Reader
pub struct TickReader<R: Read> {
    reader: BufReader<R>,
    market: Option<String>,
    symbol: String,
    date: String,
    buffer: [u8; RECORD_SIZE],
}

impl TickReader<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, TickParseError> {
        let path = path.as_ref();
        validate_dat_path(path)?;
        let (market, symbol, date) = extract_tick_file_metadata(path)?;
        let file = File::open(path)?;
        Ok(Self::new(file, market, symbol, date))
    }
}

impl<R: Read> TickReader<R> {
    pub fn new(
        reader: R,
        market: Option<String>,
        symbol: impl Into<String>,
        date: impl Into<String>,
    ) -> Self {
        TickReader {
            reader: BufReader::new(reader),
            market,
            symbol: symbol.into(),
            date: date.into(),
            buffer: [0u8; RECORD_SIZE],
        }
    }
}

impl<R: Read> Iterator for TickReader<R> {
    type Item = Result<TickData, TickParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = self.reader.read_exact(&mut self.buffer) {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(TickParseError::Io(err)));
        }

        let mut cursor = Cursor::new(&self.buffer[..]);
        Some(
            parse_single_record(&mut cursor, self.market.as_deref(), &self.symbol, &self.date)
                .map_err(TickParseError::Io),
        )
    }
}

/// Level 3 API: 返回 Vec<TickData>
pub fn parse_ticks_to_structs(path: impl AsRef<Path>) -> Result<Vec<TickData>, TickParseError> {
    let path_ref = path.as_ref();
    let estimated_rows = estimate_rows(path_ref)?;
    let mut reader = TickReader::from_path(path_ref)?;
    let mut rows = Vec::with_capacity(estimated_rows);
    for tick in &mut reader {
        rows.push(tick?);
    }
    Ok(rows)
}

/// Level 3 API: 返回 DataFrame
pub fn parse_ticks_to_dataframe(path: impl AsRef<Path>) -> Result<DataFrame, TickParseError> {
    let path_ref = path.as_ref();
    let estimated_rows = estimate_rows(path_ref)?;
    let mut reader = TickReader::from_path(path_ref)?;

    let price_levels = 5;

    let mut dates = Vec::with_capacity(estimated_rows);
    let mut markets = Vec::with_capacity(estimated_rows);
    let mut symbols = Vec::with_capacity(estimated_rows);
    let mut raw_qmt_timestamps = Vec::with_capacity(estimated_rows);
    let mut time_values = Vec::with_capacity(estimated_rows);
    let mut last_prices: Vec<Option<f64>> = Vec::with_capacity(estimated_rows);
    let mut amounts: Vec<Option<f64>> = Vec::with_capacity(estimated_rows);
    let mut volumes: Vec<Option<u64>> = Vec::with_capacity(estimated_rows);
    let mut market_phase_statuses = Vec::with_capacity(estimated_rows);
    let mut last_closes = Vec::with_capacity(estimated_rows);
    let mut qmt_status_1 = Vec::with_capacity(estimated_rows);
    let mut qmt_status_2 = Vec::with_capacity(estimated_rows);

    let mut ask_price_builder = ListPrimitiveChunkedBuilder::<Float64Type>::new(
        "askPrice".into(),
        estimated_rows,
        estimated_rows * price_levels,
        DataType::Float64,
    );
    let mut ask_vol_builder = ListPrimitiveChunkedBuilder::<UInt32Type>::new(
        "askVol".into(),
        estimated_rows,
        estimated_rows * price_levels,
        DataType::UInt32,
    );
    let mut bid_price_builder = ListPrimitiveChunkedBuilder::<Float64Type>::new(
        "bidPrice".into(),
        estimated_rows,
        estimated_rows * price_levels,
        DataType::Float64,
    );
    let mut bid_vol_builder = ListPrimitiveChunkedBuilder::<UInt32Type>::new(
        "bidVol".into(),
        estimated_rows,
        estimated_rows * price_levels,
        DataType::UInt32,
    );

    for result in &mut reader {
        let tick = result?;
        let decoded_time =
            compose_tick_datetime_ms(tick.market.as_deref(), &tick.date, tick.raw_qmt_timestamp);
        markets.push(tick.market);
        symbols.push(tick.symbol);
        dates.push(tick.date);
        raw_qmt_timestamps.push(tick.raw_qmt_timestamp);
        time_values.push(decoded_time);
        market_phase_statuses.push(tick.market_phase_status);
        last_closes.push(tick.last_close);
        last_prices.push(tick.last_price);
        amounts.push(tick.amount);
        volumes.push(tick.volume);
        qmt_status_1.push(tick.qmt_status_field_1_raw);
        qmt_status_2.push(tick.qmt_status_field_2_raw);

        ask_price_builder.append_iter(tick.ask_prices.iter().copied());
        ask_vol_builder.append_iter(tick.ask_vols.iter().copied());
        bid_price_builder.append_iter(tick.bid_prices.iter().copied());
        bid_vol_builder.append_iter(tick.bid_vols.iter().copied());
    }

    if dates.is_empty() {
        return Ok(DataFrame::default());
    }

    let num_rows = dates.len();
    let empty_f64: Series = Series::new(PlSmallStr::from("empty_f64"), vec![None::<f64>; num_rows]);
    let empty_i64: Series = Series::new(PlSmallStr::from("empty_i64"), vec![None::<i64>; num_rows]);

    let df = df![
        "market" => markets,
        "symbol" => symbols,
        "date" => dates,
        "raw_qmt_timestamp" => raw_qmt_timestamps,
        "time" => time_values,
        "last_price" => last_prices,
        "open" => empty_f64.clone(),
        "high" => empty_f64.clone(),
        "low" => empty_f64.clone(),
        "last_close" => last_closes,
        "amount" => amounts,
        "volume" => volumes,
        "pvolume" => empty_i64.clone(),
        "tickvol" => empty_i64.clone(),
        "market_phase_status" => market_phase_statuses,
        "stockStatus" => empty_i64.clone(),
        "qmt_status_field_1_raw" => qmt_status_1,
        "qmt_status_field_2_raw" => qmt_status_2,
        "lastSettlementPrice" => empty_f64.clone(),
        "askPrice" => ask_price_builder.finish(),
        "bidPrice" => bid_price_builder.finish(),
        "askVol" => ask_vol_builder.finish(),
        "bidVol" => bid_vol_builder.finish(),
        "settlementPrice" => empty_f64.clone(),
        "transactionNum" => empty_f64.clone(),
        "pe" => empty_f64,
    ]?;

    let raw_tz = polars::prelude::TimeZone::opt_try_new(None::<PlSmallStr>)?;
    let china_tz = polars::prelude::TimeZone::opt_try_new(Some("Asia/Shanghai"))?;

    let df = df
        .lazy()
        .with_column(
            col("time")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, raw_tz))
                .dt()
                .convert_time_zone(china_tz.unwrap())
                .alias("time"),
        )
        .collect()?;

    Ok(df)
}

fn decode_qmt_timestamp_ms(raw: u32) -> Option<u32> {
    raw.checked_sub(QMT_TICK_TIME_OFFSET_MS).filter(|ms| *ms < 86_400_000)
}

fn decode_qmt_timestamp_ms_for_market(market: Option<&str>, raw: u32) -> Option<u32> {
    match market {
        Some("BJ") => Some((raw % 86_400_000 + 86_400_000 - BJ_TICK_TIME_OFFSET_MS) % 86_400_000),
        _ => decode_qmt_timestamp_ms(raw),
    }
}

fn compose_tick_datetime_ms(market: Option<&str>, date_str: &str, raw: u32) -> Option<i64> {
    let trade_date = extract_trade_date(date_str)?;
    let time_ms = decode_qmt_timestamp_ms_for_market(market, raw)? as i64;
    let bj = FixedOffset::east_opt(8 * 3600)?;
    let day_start = trade_date.and_hms_opt(0, 0, 0)?;
    let local_dt = bj.from_local_datetime(&day_start).single()?;
    Some(local_dt.timestamp_millis() + time_ms)
}

fn extract_trade_date(date_str: &str) -> Option<NaiveDate> {
    if date_str.len() == 8 && date_str.chars().all(|c| c.is_ascii_digit()) {
        return NaiveDate::parse_from_str(date_str, "%Y%m%d").ok();
    }

    date_str
        .split('-')
        .find(|part| part.len() == 8 && part.chars().all(|c| c.is_ascii_digit()))
        .and_then(|part| NaiveDate::parse_from_str(part, "%Y%m%d").ok())
}

fn validate_dat_path(path: &Path) -> Result<(), TickParseError> {
    if path.as_os_str().is_empty() {
        return Err(TickParseError::EmptyPath);
    }
    if path.extension().and_then(|s| s.to_str()) != Some("dat") {
        return Err(TickParseError::InvalidExtension(path.display().to_string()));
    }
    Ok(())
}

fn extract_tick_file_metadata(path: &Path) -> Result<(Option<String>, String, String), TickParseError> {
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or(TickParseError::InvalidFileName)?;
    let stem = filename
        .split('.')
        .next()
        .ok_or(TickParseError::InvalidFileName)?;

    let market = path
        .ancestors()
        .filter_map(|p| p.file_name().and_then(|s| s.to_str()))
        .find(|s| matches!(*s, "SH" | "SZ" | "BJ"))
        .map(|s| s.to_string());

    let (symbol, date) = if stem.len() == 8 && stem.chars().all(|c| c.is_ascii_digit()) {
        let symbol = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .ok_or(TickParseError::InvalidFileName)?;
        (symbol.to_string(), stem.to_string())
    } else {
        let mut parts = stem.split('-');
        let symbol = parts.next().ok_or(TickParseError::InvalidFileName)?;
        let date = parts.next().ok_or(TickParseError::InvalidFileName)?;
        (symbol.to_string(), date.to_string())
    };

    if symbol.is_empty() || date.len() != 8 || !date.chars().all(|c| c.is_ascii_digit()) {
        return Err(TickParseError::InvalidFileName);
    }

    Ok((market, symbol.to_string(), date.to_string()))
}

fn estimate_rows(path: &Path) -> Result<usize, TickParseError> {
    let file_len = std::fs::metadata(path)?.len();
    Ok((file_len as usize) / RECORD_SIZE + 1)
}

/// 从单个144字节的记录中解析出核心数据，返回一个 TickData 实例
fn parse_single_record(
    cursor: &mut Cursor<&[u8]>,
    market: Option<&str>,
    symbol: &str,
    date_str: &str,
) -> std::io::Result<TickData> {
    let raw_qmt_timestamp = cursor.read_u32::<LittleEndian>()?;
    let qmt_status_field_1_raw = cursor.read_u32::<LittleEndian>()?;
    cursor.set_position(8);
    let raw_last_price = cursor.read_u32::<LittleEndian>()?;
    let qmt_status_field_2_raw = cursor.read_u32::<LittleEndian>()?;
    let raw_amount = cursor.read_u32::<LittleEndian>()?;
    cursor.set_position(24);
    let raw_volume = cursor.read_u32::<LittleEndian>()?;
    let market_phase_status = cursor.read_u32::<LittleEndian>()?;
    cursor.set_position(60);
    let last_close = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;

    let mut tick = TickData {
        market: market.map(str::to_string),
        symbol: symbol.to_string(),
        date: date_str.to_string(),
        raw_qmt_timestamp,
        market_phase_status,
        last_close,
        qmt_status_field_1_raw,
        qmt_status_field_2_raw,
        last_price: None,
        amount: None,
        volume: None,
        ask_prices: [None; 5],
        ask_vols: [None; 5],
        bid_prices: [None; 5],
        bid_vols: [None; 5],
    };

    if market_phase_status == CALL_AUCTION_PHASE_CODE {
        tick.last_price = Some(0.0);
        tick.amount = Some(0.0);
        tick.volume = Some(0);
        tick.ask_vols = [Some(0); 5];
        tick.bid_vols = [Some(0); 5];
        cursor.set_position(64);
        let ref_price = cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE;
        tick.ask_prices[0] = Some(ref_price);
        tick.bid_prices[0] = Some(ref_price);
        cursor.set_position(84);
        tick.ask_vols[0] = Some(cursor.read_u32::<LittleEndian>()?);
        tick.ask_vols[1] = Some(cursor.read_u32::<LittleEndian>()?);
        cursor.set_position(124);
        tick.bid_vols[0] = Some(cursor.read_u32::<LittleEndian>()?);
    } else {
        tick.last_price = Some(raw_last_price as f64 / PRICE_SCALE);
        tick.amount = Some(raw_amount as f64);
        tick.volume = Some(raw_volume as u64);
        for i in 0..5 {
            cursor.set_position(64 + (i * 4) as u64);
            tick.ask_prices[i] = Some(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
            cursor.set_position(84 + (i * 4) as u64);
            tick.ask_vols[i] = Some(cursor.read_u32::<LittleEndian>()?);
            cursor.set_position(104 + (i * 4) as u64);
            tick.bid_prices[i] = Some(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
            cursor.set_position(124 + (i * 4) as u64);
            tick.bid_vols[i] = Some(cursor.read_u32::<LittleEndian>()?);
        }
    }

    Ok(tick)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    const DAT_FILE: &str = "data/000001-20250529-tick.dat";

    #[test]
    fn run_struct_demo() -> Result<(), TickParseError> {
        let file_to_parse = PathBuf::from(DAT_FILE);
        let all_ticks = parse_ticks_to_structs(file_to_parse)?;
        println!("成功解析 {} 条 tick 数据。\n", all_ticks.len());
        if let Some(first_tick) = all_ticks.first() {
            println!("--- 第一条 Tick 示例 ---\n{:#?}", first_tick);
        }
        if let Some(last_tick) = all_ticks.last() {
            println!("\n--- 最后一条 Tick 示例 ---\n{:#?}", last_tick);
        }
        Ok(())
    }

    #[test]
    fn run_polars_demo() -> Result<(), TickParseError> {
        let file_to_parse = PathBuf::from(DAT_FILE);
        let df = parse_ticks_to_dataframe(file_to_parse)?;
        println!("成功解析 DataFrame，尺寸: {:?}\n", df.shape());
        println!("--- DataFrame (前5行和后5行) ---\n{}", df);

        if df.height() > 0 {
            let result_df = df
                .clone()
                .lazy()
                .select([col("last_price").mean().alias("mean_price")])
                .collect()?;

            let mean_price: f64 = result_df.column("mean_price")?.get(0)?.try_extract()?;

            println!("\n--- Polars 分析示例 ---");
            println!("所有Tick的平均价格: {:.4}", mean_price);
        }
        Ok(())
    }

    #[test]
    fn test_tick_schema_names() -> Result<(), TickParseError> {
        assert_eq!(tick_api_field_names()[0], "lastPrice");
        assert_eq!(tick_api_field_names()[4], "openInt");
        assert_eq!(tick_api_field_names()[16], "timetag");

        let df = parse_ticks_to_dataframe(PathBuf::from(DAT_FILE))?;
        let names = df.get_column_names_str();
        assert_eq!(names.as_slice(), tick_dataframe_column_names());
        Ok(())
    }

    #[test]
    fn test_extract_tick_file_metadata() -> Result<(), TickParseError> {
        let (market, symbol, date) = extract_tick_file_metadata(Path::new(DAT_FILE))?;
        assert_eq!(market, None);
        assert_eq!(symbol, "000001");
        assert_eq!(date, "20250529");
        Ok(())
    }

    #[test]
    fn test_decode_qmt_timestamp() {
        assert_eq!(decode_qmt_timestamp_ms(429_610_528), Some(33_310_528));
        assert_eq!(decode_qmt_timestamp_ms(450_316_528), Some(54_016_528));
        assert_eq!(
            compose_tick_datetime_ms(None, "20250529", 429_610_528),
            Some(1_748_481_310_528),
        );
        assert_eq!(decode_qmt_timestamp_ms_for_market(Some("BJ"), 2_070_911_528), Some(33_311_528));
    }

    #[test]
    fn test_tick_dataframe_time_column_populated() -> Result<(), TickParseError> {
        let df = parse_ticks_to_dataframe(PathBuf::from(DAT_FILE))?;
        assert_eq!(df.column("market")?.str()?.get(0), None);
        assert_eq!(df.column("symbol")?.str()?.get(0), Some("000001"));
        assert_eq!(df.column("date")?.str()?.get(0), Some("20250529"));
        let time_col = df.column("time")?;
        assert_eq!(time_col.null_count(), 0);
        assert!(matches!(time_col.dtype(), DataType::Datetime(_, _)));
        Ok(())
    }

    #[test]
    fn test_extract_tick_file_metadata_with_market() -> Result<(), TickParseError> {
        let path = Path::new("/mnt/data/trade/qmtdata/datadir/BJ/0/430017/20250617.dat");
        let (market, symbol, date) = extract_tick_file_metadata(path)?;
        assert_eq!(market.as_deref(), Some("BJ"));
        assert_eq!(symbol, "430017");
        assert_eq!(date, "20250617");
        Ok(())
    }
}
