//! src/tick.rs

use std::fs::File;
use std::io::{BufReader, Read, Cursor};
use std::path::Path;
use byteorder::{LittleEndian, ReadBytesExt};
use anyhow::{Context, Result};
use thiserror::Error;
use polars::prelude::*;

// --- 公共常量和错误类型 ---
const RECORD_SIZE: usize = 144;
const PRICE_SCALE: f64 = 1000.0;
const CALL_AUCTION_PHASE_CODE: u32 = 12;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("文件路径不能为空")]
    EmptyPath,
    #[error("文件必须是.dat格式: {0}")]
    InvalidExtension(String),
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
}

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


pub fn parse_ticks_to_structs(path: impl AsRef<Path>) -> Result<Vec<TickData>> {
    let (mut reader, date_str) = setup_reader_and_date(path.as_ref())?;

    let mut ticks_data = Vec::new();
    let mut record_buffer = [0u8; RECORD_SIZE];

    while let Ok(()) = reader.read_exact(&mut record_buffer) {
        let mut cursor = Cursor::new(&record_buffer[..]);
        let (tick, _) = parse_single_record(&mut cursor, &date_str)?;
        ticks_data.push(tick);
    }
    Ok(ticks_data)
}


pub fn parse_ticks_to_dataframe(path: impl AsRef<Path>) -> Result<DataFrame> {
    // --- 修复点 #2: `?` 现在可以正常工作了 ---
    let (mut reader, date_str) = setup_reader_and_date(path.as_ref())?;

    // 预估行数，尽量减少 Vec 和 Builder 的扩容
    let estimated_rows = 5000;
    let price_levels = 5;

    // 为DataFrame的每一列初始化构建器
    let mut dates = Vec::with_capacity(estimated_rows);
    let mut raw_qmt_timestamps = Vec::with_capacity(estimated_rows);
    let mut last_prices: Vec<Option<f64>> = Vec::with_capacity(estimated_rows);
    let mut amounts: Vec<Option<f64>> = Vec::with_capacity(estimated_rows);
    let mut volumes: Vec<Option<u64>> = Vec::with_capacity(estimated_rows);
    let mut market_phase_statuses = Vec::with_capacity(estimated_rows);
    let mut last_closes = Vec::with_capacity(estimated_rows);
    let mut qmt_status_1 = Vec::with_capacity(estimated_rows);
    let mut qmt_status_2 = Vec::with_capacity(estimated_rows);

    // --- 修复点 #1: 正确设置 value_capacity 并保留第四个 DataType 参数 ---
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

    let mut record_buffer = [0u8; RECORD_SIZE];

    while let Ok(()) = reader.read_exact(&mut record_buffer) {
        let mut cursor = Cursor::new(&record_buffer[..]);
        // --- 修复点 #2: `?` 现在可以正常工作了 ---
        let (tick, _) = parse_single_record(&mut cursor, &date_str)?;

        // 将 TickData 的字段填充到各个列的 Vec 和 Builder 中
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

    // 构建 DataFrame
    let num_rows = dates.len();
    let empty_f64: Series = Series::new(PlSmallStr::from("empty_f64"), vec![None::<f64>; num_rows]);
    let empty_i64: Series = Series::new(PlSmallStr::from("empty_i64"), vec![None::<i64>; num_rows]);

    // df! 宏返回 PolarsResult，使用 `?` 可以自动将其错误转换为 anyhow::Error
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
    ]?; // 注意这里的 `?`，它将 PolarsError 转换为 anyhow::Error

    Ok(df)
}

/// 验证路径，打开文件，并返回 BufReader 和 日期字符串
fn setup_reader_and_date(path: &Path) -> Result<(BufReader<File>, String)> {
    if path.as_os_str().is_empty() {
        return Err(ParseError::EmptyPath.into());
    }
    if path.extension().and_then(|s| s.to_str()) != Some("dat") {
        return Err(ParseError::InvalidExtension(path.display().to_string()).into());
    }

    let file = File::open(path)
        .with_context(|| format!("无法打开文件: {}", path.display()))?;

    let filename = path.file_name()
        .and_then(|s| s.to_str())
        .context("无法获取文件名")?;
    let date_str = filename.split('.').next().context("无法从文件名中解析日期")?.to_string();

    Ok((BufReader::new(file), date_str))
}

/// 从单个144字节的记录中解析出核心数据，返回一个 TickData 实例
fn parse_single_record(cursor: &mut Cursor<&[u8]>, date_str: &str) -> Result<(TickData, u32)> {
    // 解析通用字段
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
    Ok((tick, market_phase_status))
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
            // 1. 调用 .lazy() 进入惰性模式
            // 2. 在 LazyFrame 上调用 .select()，它接受表达式
            // 3. 调用 .collect() 触发计算
            let result_df = df.clone().lazy()
                .select([
                    col("last_price").mean().alias("mean_price"),
                ])
                .collect()?;

            // 从结果DataFrame中提取出计算出的标量值
            let mean_price: f64 = result_df
                .column("mean_price")?
                .get(0)?
                .try_extract()?;

            println!("\n--- Polars 分析示例 ---");
            println!("所有Tick的平均价格: {:.4}", mean_price);
        }
        Ok(())
    }
}
