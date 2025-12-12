use std::fs::File;
use std::io::{BufReader, Read, Cursor};
use std::path::Path;
use byteorder::{LittleEndian, ReadBytesExt};
use anyhow::{Context, Result};
use polars::prelude::*;
use polars::datatypes::PlSmallStr;

const RECORD_SIZE: usize = 64;
const PRICE_SCALE: f64 = 1000.0;

/// 解析 1分钟 K线数据 (.dat) 到 Polars DataFrame
pub fn parse_kline_to_dataframe(path: impl AsRef<Path>) -> Result<DataFrame> {
    let path = path.as_ref();

    // 1. 预计算行数
    let file_len = std::fs::metadata(path)
        .with_context(|| format!("无法获取文件元数据: {}", path.display()))?
        .len();

    if file_len == 0 {
        return Ok(DataFrame::empty());
    }

    let estimated_rows = (file_len as usize) / RECORD_SIZE;

    // 2. 初始化列向量
    let mut timestamps = Vec::with_capacity(estimated_rows);
    let mut opens = Vec::with_capacity(estimated_rows);
    let mut highs = Vec::with_capacity(estimated_rows);
    let mut lows = Vec::with_capacity(estimated_rows);
    let mut closes = Vec::with_capacity(estimated_rows);
    let mut volumes = Vec::with_capacity(estimated_rows);
    let mut amounts = Vec::with_capacity(estimated_rows);
    let mut open_interests = Vec::with_capacity(estimated_rows);
    let mut pre_closes = Vec::with_capacity(estimated_rows);

    // 3. 打开文件读取
    let file = File::open(path).with_context(|| format!("无法打开文件: {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buffer = [0u8; RECORD_SIZE];

    // 4. 解析循环
    while let Ok(()) = reader.read_exact(&mut buffer) {
        let mut cursor = Cursor::new(&buffer[..]);

        // Offset 8: Unix时间戳 (u32)
        cursor.set_position(8);
        let ts_seconds = cursor.read_u32::<LittleEndian>()?;
        timestamps.push(ts_seconds as i64 * 1000); // 转毫秒

        // Offset 12..28: OHLC
        opens.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
        highs.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
        lows.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
        closes.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);

        // Offset 32: Volume
        cursor.set_position(32);
        volumes.push(cursor.read_u32::<LittleEndian>()?);

        // Offset 40: Amount
        cursor.set_position(40);
        amounts.push(cursor.read_u64::<LittleEndian>()? as f64);

        // Offset 48: Open Interest
        open_interests.push(cursor.read_u32::<LittleEndian>()?);

        // Offset 60: Pre Close
        cursor.set_position(60);
        pre_closes.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
    }

    if timestamps.is_empty() {
        return Ok(DataFrame::empty());
    }

    // 5. 构建 DataFrame
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

    // 6. 处理时间列
    // 逻辑：
    // a. 原始数据是 Unix Timestamp (UTC)
    // b. cast 声明它是 UTC 时间
    // c. convert_time_zone 转换为 上海时间 (UTC+8)
    let raw_tz = TimeZone::opt_try_new(None::<PlSmallStr>)?;
    let china_tz = TimeZone::opt_try_new(Some("Asia/Shanghai"))?;
    let df = df.lazy()
        .with_column(
            col("timestamp_ms")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, raw_tz))
                .dt().convert_time_zone(
                china_tz.unwrap()
            )
                .alias("time")
        )
        .select([
            col("time"), col("open"), col("high"), col("low"), col("close"),
            col("volume"), col("amount"), col("settlementPrice"),
            col("openInterest"), col("preClose"), col("suspendFlag")
        ])
        .collect()?;

    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_kline_dataframe() -> Result<()> {
        let test_file = PathBuf::from("data/000000-1m.dat");

        let df = parse_kline_to_dataframe(&test_file)?;

        println!("--- Tail ---");
        println!("{}", df.tail(Some(5)));
        Ok(())
    }
}