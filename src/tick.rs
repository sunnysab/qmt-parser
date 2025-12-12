use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::Path;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt};
use polars::prelude::*;
use thiserror::Error;

const RECORD_SIZE: usize = 144;
const PRICE_SCALE: f64 = 1000.0;
const CALL_AUCTION_PHASE_CODE: u32 = 12;

/// Level 1: 原始 Tick 结构体
#[derive(Debug, Clone)]
pub struct TickData {
    pub date: String,
    pub raw_qmt_timestamp: u32,
    pub market_phase_status: u32,
    pub last_price: Option<f64>,
    pub last_close: f64,
    pub amount: Option<f64>,
    pub volume: Option<u64>,
    pub ask_prices: Vec<Option<f64>>,
    pub ask_vols: Vec<Option<u32>>,
    pub bid_prices: Vec<Option<f64>>,
    pub bid_vols: Vec<Option<u32>>,
    pub qmt_status_field_1_raw: u32,
    pub qmt_status_field_2_raw: u32,
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("文件路径不能为空")]
    EmptyPath,
    #[error("文件必须是.dat格式: {0}")]
    InvalidExtension(String),
    #[error("无法从文件名解析日期")]
    InvalidFileName,
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
}

/// Level 2: 迭代器 Reader
pub struct TickReader<R: Read> {
    reader: BufReader<R>,
    date: String,
    buffer: [u8; RECORD_SIZE],
}

impl TickReader<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ParseError> {
        let path = path.as_ref();
        validate_dat_path(path)?;
        let date = extract_date_from_path(path)?;
        let file = File::open(path)?;
        Ok(Self::new(file, date))
    }
}

impl<R: Read> TickReader<R> {
    pub fn new(reader: R, date: impl Into<String>) -> Self {
        TickReader {
            reader: BufReader::new(reader),
            date: date.into(),
            buffer: [0u8; RECORD_SIZE],
        }
    }
}

impl<R: Read> Iterator for TickReader<R> {
    type Item = Result<TickData, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = self.reader.read_exact(&mut self.buffer) {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(ParseError::Io(err)));
        }

        let mut cursor = Cursor::new(&self.buffer[..]);
        Some(parse_single_record(&mut cursor, &self.date).map_err(ParseError::Io))
    }
}

/// Level 3 API: 返回 Vec<TickData>
pub fn parse_ticks_to_structs(path: impl AsRef<Path>) -> Result<Vec<TickData>> {
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
pub fn parse_ticks_to_dataframe(path: impl AsRef<Path>) -> Result<DataFrame> {
    let path_ref = path.as_ref();
    let estimated_rows = estimate_rows(path_ref)?;
    let mut reader = TickReader::from_path(path_ref)?;

    let price_levels = 5;

    let mut dates = Vec::with_capacity(estimated_rows);
    let mut raw_qmt_timestamps = Vec::with_capacity(estimated_rows);
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
        dates.push(tick.date);
        raw_qmt_timestamps.push(tick.raw_qmt_timestamp);
        market_phase_statuses.push(tick.market_phase_status);
        last_closes.push(tick.last_close);
        last_prices.push(tick.last_price);
        amounts.push(tick.amount);
        volumes.push(tick.volume);
        qmt_status_1.push(tick.qmt_status_field_1_raw);
        qmt_status_2.push(tick.qmt_status_field_2_raw);

        ask_price_builder.append_iter(tick.ask_prices.into_iter());
        ask_vol_builder.append_iter(tick.ask_vols.into_iter());
        bid_price_builder.append_iter(tick.bid_prices.into_iter());
        bid_vol_builder.append_iter(tick.bid_vols.into_iter());
    }

    if dates.is_empty() {
        return Ok(DataFrame::default());
    }

    let num_rows = dates.len();
    let empty_f64: Series = Series::new(PlSmallStr::from("empty_f64"), vec![None::<f64>; num_rows]);
    let empty_i64: Series = Series::new(PlSmallStr::from("empty_i64"), vec![None::<i64>; num_rows]);

    let df = df![
        "date" => dates,
        "raw_qmt_timestamp" => raw_qmt_timestamps,
        "time" => empty_f64.clone(),
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

    Ok(df)
}

fn validate_dat_path(path: &Path) -> Result<(), ParseError> {
    if path.as_os_str().is_empty() {
        return Err(ParseError::EmptyPath);
    }
    if path.extension().and_then(|s| s.to_str()) != Some("dat") {
        return Err(ParseError::InvalidExtension(path.display().to_string()));
    }
    Ok(())
}

fn extract_date_from_path(path: &Path) -> Result<String, ParseError> {
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or(ParseError::InvalidFileName)?;
    filename
        .split('.')
        .next()
        .map(|s| s.to_string())
        .ok_or(ParseError::InvalidFileName)
}

fn estimate_rows(path: &Path) -> Result<usize> {
    let file_len = std::fs::metadata(path)?.len();
    Ok((file_len as usize) / RECORD_SIZE + 1)
}

/// 从单个144字节的记录中解析出核心数据，返回一个 TickData 实例
fn parse_single_record(cursor: &mut Cursor<&[u8]>, date_str: &str) -> std::io::Result<TickData> {
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
        date: date_str.to_string(),
        raw_qmt_timestamp,
        market_phase_status,
        last_close,
        qmt_status_field_1_raw,
        qmt_status_field_2_raw,
        last_price: None,
        amount: None,
        volume: None,
        ask_prices: vec![None; 5],
        ask_vols: vec![None; 5],
        bid_prices: vec![None; 5],
        bid_vols: vec![None; 5],
    };

    if market_phase_status == CALL_AUCTION_PHASE_CODE {
        tick.last_price = Some(0.0);
        tick.amount = Some(0.0);
        tick.volume = Some(0);
        tick.ask_vols = vec![Some(0); 5];
        tick.bid_vols = vec![Some(0); 5];
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
    fn run_struct_demo() -> Result<()> {
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
    fn run_polars_demo() -> Result<()> {
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
}
