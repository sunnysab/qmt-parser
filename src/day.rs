use std::fs::File;
use std::io::{BufReader, Read, Cursor};
use std::path::Path;
use byteorder::{LittleEndian, ReadBytesExt};
use anyhow::{Context, Result, anyhow};
use polars::prelude::*;
use chrono::{NaiveDate, DateTime, FixedOffset, TimeZone};

const RECORD_SIZE: usize = 64;
const PRICE_SCALE: f64 = 1000.0;
const AMOUNT_SCALE: f64 = 100.0;

/// 解析 QMT 日线数据 (.dat)
pub fn parse_daily_kline(
    path: impl AsRef<Path>,
    start_date_str: &str,
    end_date_str: &str
) -> Result<DataFrame> {
    let path = path.as_ref();

    // 0. 解析日期参数
    let start_date = NaiveDate::parse_from_str(start_date_str, "%Y%m%d")
        .map_err(|e| anyhow!("开始日期格式错误: {}", e))?;
    let end_date = NaiveDate::parse_from_str(end_date_str, "%Y%m%d")
        .map_err(|e| anyhow!("结束日期格式错误: {}", e))?;

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
    let mut file_pre_closes = Vec::with_capacity(estimated_rows);

    // 3. 打开文件
    let file = File::open(path).with_context(|| format!("无法打开文件: {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buffer = [0u8; RECORD_SIZE];

    // 用于时区转换 (QMT 时间戳是 UTC，需要转为 +8 才能正确判断日期)
    let tz_offset = FixedOffset::east_opt(8 * 3600).unwrap();

    // 4. 解析循环
    while let Ok(()) = reader.read_exact(&mut buffer) {
        let mut cursor = Cursor::new(&buffer[..]);

        // Offset 8: Unix时间戳 (u32)
        cursor.set_position(8);
        let ts_seconds = cursor.read_u32::<LittleEndian>()?;

        // --- 日期过滤逻辑 ---
        // 修复: 使用 DateTime::from_timestamp 替代 deprecated 的 NaiveDateTime::from_timestamp_opt
        let dt_utc = DateTime::from_timestamp(ts_seconds as i64, 0)
            .ok_or_else(|| anyhow!("无效的时间戳"))?
            .naive_utc(); // 转为 NaiveDateTime 用于后续转换

        let current_date = tz_offset.from_utc_datetime(&dt_utc).date_naive();

        if current_date < start_date || current_date > end_date {
            continue;
        }

        timestamps.push(ts_seconds as i64 * 1000); // 存毫秒

        // Offset 12..28: OHLC (除以 1000.0)
        opens.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
        highs.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
        lows.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
        closes.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);

        // Offset 32: Volume
        cursor.set_position(32);
        volumes.push(cursor.read_u32::<LittleEndian>()?);

        // Offset 40: Amount (日线除以 100.0)
        cursor.set_position(40);
        let raw_amount = cursor.read_u64::<LittleEndian>()?;
        amounts.push(raw_amount as f64 / AMOUNT_SCALE);

        // Offset 48: Open Interest
        open_interests.push(cursor.read_u32::<LittleEndian>()?);

        // Offset 60: Pre Close (文件原始值)
        cursor.set_position(60);
        file_pre_closes.push(cursor.read_u32::<LittleEndian>()? as f64 / PRICE_SCALE);
    }

    if timestamps.is_empty() {
        return Ok(DataFrame::empty());
    }

    // 5. 构建基础 DataFrame
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

    // 6. 业务逻辑处理 (LazyFrame)
    let df_final = df.lazy()
        // 修复: sort 必须传数组/切片
        .sort(["timestamp_ms"], Default::default())

        .with_column(
            (col("volume").eq(lit(0)).and(col("amount").eq(lit(0.0))))
                .cast(DataType::Int32)
                .alias("suspendFlag")
        )
        // 修复: shift 现在接收 Expr，需要用 lit(1)
        .with_column(
            col("close").shift(lit(1)).alias("calc_pre_close")
        )
        .with_column(
            when(col("suspendFlag").eq(lit(1)))
                .then(col("close"))
                .otherwise(
                    col("calc_pre_close").fill_null(col("close"))
                )
                .alias("preClose")
        )
        // 修复: 时区转换不再需要 TimeZone trait 对象，直接传字符串
        .with_columns(vec![
            col("timestamp_ms")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None)) // 先转 UTC
                .dt().convert_time_zone(polars::prelude::TimeZone::opt_try_new("Asia/Shanghai".into())?.unwrap())         // 再转上海时间
                .alias("time"),
            lit(0.0).alias("settlementPrice")
        ])
        .select([
            col("time"), col("open"), col("high"), col("low"), col("close"),
            col("volume"), col("amount"), col("settlementPrice"),
            col("openInterest"), col("preClose"), col("suspendFlag")
        ])
        .collect()?;

    Ok(df_final)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_daily_kline() -> Result<()> {
        let daily_path = PathBuf::from("/mnt/data/trade/qmtdata/datadir/SZ/86400/000001.DAT");

        if !daily_path.exists() {
            println!("测试文件不存在，跳过测试: {:?}", daily_path);
            return Ok(());
        }

        let start = "19910401";
        let end = "19910425";

        let df = parse_daily_kline(&daily_path, start, end)?;

        println!("--- Daily DataFrame (Shape: {:?}) ---", df.shape());
        println!("{}", df);

        if df.height() > 0 {
            let cols = df.get_column_names();
            // 修复: PlSmallStr 类型不匹配问题，使用 iter().any() 检查
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