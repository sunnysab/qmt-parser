//! QMT 分红送配 LevelDB 查询。
//!
//! 这个模块读取 `DividData` 目录中的 LevelDB 数据，并把单条 value 解码为
//! [`DividendRecord`]。

use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use rusty_leveldb::{DB, LdbIterator, Options};
use std::path::Path;
use std::str;
use thiserror::Error;

/// 单条除权除息记录。
#[derive(Debug, Clone)]
pub struct DividendRecord {
    /// 除权除息日。
    pub ex_dividend_date: NaiveDate,
    /// 股权登记日。
    pub record_date: Option<NaiveDate>,
    /// 每股现金红利。
    pub interest: f64,
    /// 每股送股。
    pub stock_bonus: f64,
    /// 每股转赠。
    pub stock_gift: f64,
    /// 配股数量。
    pub allot_num: f64,
    /// 配股价格。
    pub allot_price: f64,
    /// 股改相关值，当前仍保留原始含义。
    pub gugai: f64,
    /// 当前版本存在但语义未完全确认的额外槽位。
    pub unknown64_raw: f64,
    /// 复权系数。
    pub adjust_factor: f64,
    /// Value 中记录的原始时间戳。
    pub timestamp_raw: i64,
}

/// 分红数据库读取错误。
#[derive(Debug, Error)]
pub enum DividendError {
    /// LevelDB 打开失败。
    #[error("无法打开 LevelDB: {0}")]
    OpenDb(String),
    /// 无法创建数据库迭代器。
    #[error("无法创建 LevelDB 迭代器")]
    IteratorUnavailable,
    /// Key 结构不符合预期。
    #[error("非法的分红 Key: {0}")]
    InvalidKey(String),
    /// Key 不是合法 UTF-8。
    #[error("分红 Key 不是有效 UTF-8")]
    InvalidUtf8Key,
    /// 时间戳非法。
    #[error("无效的分红时间戳: {0}")]
    InvalidTimestamp(i64),
    /// Value 结构不符合预期。
    #[error("无法解析分红 Value: {0}")]
    InvalidValue(String),
}

/// 分红 LevelDB 查询句柄。
pub struct DividendDb {
    db: DB,
}

impl DividendDb {
    /// 打开 `DividData` 数据库目录。
    ///
    /// 注意：LevelDB 同时只能被一个进程加锁访问；如果 QMT 正在运行，
    /// 打开原目录可能失败，通常建议传入备份目录。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use qmt_parser::dividend::DividendDb;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let _db = DividendDb::new("/path/to/DividData")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DividendError> {
        let options = Options {
            create_if_missing: false,
            ..Options::default()
        };

        match DB::open(path, options) {
            Ok(db) => Ok(Self { db }),
            Err(e) => Err(DividendError::OpenDb(e.to_string())),
        }
    }

    /// 查询指定市场和证券代码的除权除息记录。
    ///
    /// `market` 典型值为 `SH`、`SZ`、`BJ`。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use qmt_parser::dividend::DividendDb;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = DividendDb::new("/path/to/DividData")?;
    /// let records = db.query("SH", "600000")?;
    /// println!("records = {}", records.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(
        &mut self,
        market: &str,
        code: &str,
    ) -> Result<Vec<DividendRecord>, DividendError> {
        let mut results = Vec::new();

        let prefix = format!("{}|{}|", market, code);
        let prefix_bytes = prefix.as_bytes();

        let mut iter = self
            .db
            .new_iter()
            .map_err(|_| DividendError::IteratorUnavailable)?;

        iter.seek(prefix_bytes);

        while let Some((key, value)) = iter.next() {
            if !key.starts_with(prefix_bytes) {
                break;
            }

            let ts_key = match Self::parse_key_timestamp(&key)? {
                Some(ts_key) => ts_key,
                None => continue,
            };

            if ts_key == 0 || ts_key > 3_000_000_000_000 {
                continue;
            }

            if let Some(record) = Self::parse_value(&value)? {
                results.push(record);
            }
        }

        Ok(results)
    }

    /// 解析二进制 Value
    /// 当前观测布局:
    /// [8 bytes unknown] [8 bytes TS]
    /// [interest] [stock_bonus] [stock_gift] [allot_num] [allot_price]
    /// [gugai] [unknown64] [adjust_factor]
    /// [record_date: u32] [padding: u32] [ex_dividend_date: u32] [padding: u32]
    fn parse_value(data: &[u8]) -> Result<Option<DividendRecord>, DividendError> {
        if data.is_empty() {
            return Ok(None);
        }
        if data.len() < 96 {
            return Err(DividendError::InvalidValue(format!(
                "value too short: expected at least 96 bytes, got {}",
                data.len()
            )));
        }

        let ts_val = LittleEndian::read_i64(&data[8..16]);
        if ts_val <= 0 {
            return Err(DividendError::InvalidTimestamp(ts_val));
        }
        let interest = LittleEndian::read_f64(&data[16..24]);
        let stock_bonus = LittleEndian::read_f64(&data[24..32]);
        let stock_gift = LittleEndian::read_f64(&data[32..40]);
        let allot_num = LittleEndian::read_f64(&data[40..48]);
        let allot_price = LittleEndian::read_f64(&data[48..56]);
        let gugai = LittleEndian::read_f64(&data[56..64]);
        let unknown64_raw = LittleEndian::read_f64(&data[64..72]);
        let adjust_factor = LittleEndian::read_f64(&data[72..80]);

        let record_date = Self::parse_yyyymmdd_u32(LittleEndian::read_u32(&data[80..84]));
        let ex_dividend_date = Self::parse_yyyymmdd_u32(LittleEndian::read_u32(&data[88..92]))
            .or_else(|| Self::date_from_timestamp_bj(ts_val))
            .ok_or(DividendError::InvalidTimestamp(ts_val))?;

        Ok(Some(DividendRecord {
            ex_dividend_date,
            record_date,
            interest,
            stock_bonus,
            stock_gift,
            allot_num,
            allot_price,
            gugai,
            unknown64_raw,
            adjust_factor,
            timestamp_raw: ts_val,
        }))
    }

    fn parse_key_timestamp(key: &[u8]) -> Result<Option<i64>, DividendError> {
        let key_str = str::from_utf8(key).map_err(|_| DividendError::InvalidUtf8Key)?;
        let parts: Vec<&str> = key_str.split('|').collect();
        if parts.len() < 4 {
            return Err(DividendError::InvalidKey(key_str.to_string()));
        }

        let ts = parts
            .last()
            .ok_or_else(|| DividendError::InvalidKey(key_str.to_string()))?
            .parse::<i64>()
            .map_err(|_| DividendError::InvalidKey(key_str.to_string()))?;

        if ts == 0 || ts > 3_000_000_000_000 {
            return Ok(None);
        }

        Ok(Some(ts))
    }

    fn parse_yyyymmdd_u32(raw: u32) -> Option<NaiveDate> {
        if raw == 0 {
            return None;
        }

        let year = (raw / 10_000) as i32;
        let month = raw / 100 % 100;
        let day = raw % 100;
        NaiveDate::from_ymd_opt(year, month, day)
    }

    fn date_from_timestamp_bj(ts_val: i64) -> Option<NaiveDate> {
        let seconds = ts_val / 1000;
        let nanoseconds = (ts_val % 1000) * 1_000_000;
        let dt_utc = DateTime::<Utc>::from_timestamp(seconds, nanoseconds as u32)?;
        let bj = FixedOffset::east_opt(8 * 3600)?;
        Some(dt_utc.with_timezone(&bj).date_naive())
    }
}

#[test]
fn test_dividend() {
    // 假设这是你复制出来的临时目录，避免锁冲突
    let db_path = "/mnt/data/trade/qmtdata/datadir/DividData";

    // 初始化
    let mut qmt_db = match DividendDb::new(db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("错误: {}", e);
            return;
        }
    };

    // 查询 21国债16 (SH.185222)
    println!("正在查询 SH.185222 ...");
    let records = qmt_db.query("SH", "185222").expect("query dividend");

    if records.is_empty() {
        eprintln!("未找到记录或解析失败。");
    }

    for record in records {
        println!("--------------------------------");
        println!("除权日   : {}", record.ex_dividend_date);
        println!("登记日   : {:?}", record.record_date);
        println!("每股红利 : {:.4}", record.interest);
        println!("每股送转 : {:.4}", record.stock_bonus);
        println!("每股转赠 : {:.4}", record.stock_gift);
        println!("配股数量 : {:.4}", record.allot_num);
        println!("配股价格 : {:.4}", record.allot_price);
        println!("股改值   : {:.4}", record.gugai);
        println!("复权系数 : {:.6}", record.adjust_factor);
    }
}

#[test]
fn test_parse_dividend_value_cash_dates_and_factor() {
    let raw = decode_hex(
        "2087c6faff7f000000488fa1850100005c8fc2f5285c09400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ce8853b1786f03fdfaf340100000000e0af340100000000",
    )
    .unwrap();

    let record = DividendDb::parse_value(&raw)
        .expect("should parse")
        .expect("record");
    assert_eq!(
        record.ex_dividend_date,
        NaiveDate::from_ymd_opt(2023, 1, 12).unwrap()
    );
    assert_eq!(
        record.record_date,
        Some(NaiveDate::from_ymd_opt(2023, 1, 11).unwrap())
    );
    assert_eq!(record.interest, 3.17);
    assert_eq!(record.stock_bonus, 0.0);
    assert_eq!(record.stock_gift, 0.0);
    assert_eq!(record.allot_num, 0.0);
    assert_eq!(record.allot_price, 0.0);
    assert_eq!(record.gugai, 0.0);
    assert!((record.adjust_factor - 1.032737).abs() < 1e-9);
}

#[test]
fn test_parse_dividend_value_bonus_gift_and_rights_issue() {
    let bonus_raw = decode_hex(
        "2087c6faff7f000000e4f9da630100009a9999999999b93f0000000000000000000000000000e03f0000000000000000000000000000000000000000000000000000000000000000b56b425a6350f83f7fee33010000000080ee330100000000",
    )
    .unwrap();
    let bonus_record = DividendDb::parse_value(&bonus_raw)
        .expect("should parse")
        .expect("record");
    assert_eq!(
        bonus_record.ex_dividend_date,
        NaiveDate::from_ymd_opt(2018, 6, 8).unwrap()
    );
    assert_eq!(
        bonus_record.record_date,
        Some(NaiveDate::from_ymd_opt(2018, 6, 7).unwrap())
    );
    assert_eq!(bonus_record.interest, 0.1);
    assert_eq!(bonus_record.stock_bonus, 0.0);
    assert_eq!(bonus_record.stock_gift, 0.5);
    assert_eq!(bonus_record.allot_num, 0.0);
    assert_eq!(bonus_record.allot_price, 0.0);
    assert_eq!(bonus_record.gugai, 0.0);
    assert!((bonus_record.adjust_factor - 1.519626).abs() < 1e-9);

    let rights_raw = decode_hex(
        "2087c6faff7f00000040675d27010000000000000000000000000000000000000000000000000000a4703d0ad7a3c03f3333333333b3214000000000000000000000000000000000ae9b525e2be1f03fd0b43201000000000000000000000000",
    )
    .unwrap();
    let rights_record = DividendDb::parse_value(&rights_raw)
        .expect("should parse")
        .expect("record");
    assert_eq!(
        rights_record.ex_dividend_date,
        NaiveDate::from_ymd_opt(2010, 3, 15).unwrap()
    );
    assert_eq!(
        rights_record.record_date,
        Some(NaiveDate::from_ymd_opt(2010, 3, 4).unwrap())
    );
    assert_eq!(rights_record.interest, 0.0);
    assert_eq!(rights_record.stock_bonus, 0.0);
    assert_eq!(rights_record.stock_gift, 0.0);
    assert!((rights_record.allot_num - 0.13).abs() < 1e-12);
    assert!((rights_record.allot_price - 8.85).abs() < 1e-12);
    assert_eq!(rights_record.gugai, 0.0);
    assert!((rights_record.adjust_factor - 1.054973).abs() < 1e-9);
}

#[test]
fn test_parse_dividend_value_gugai_slot() {
    let raw = decode_hex(
        "2087c6faff7f000000583e5b940100005c8fc2f5285c0940000000000000000000000000000000000000000000000000000000000000000000000000000059400000000000000000199293895b85f03ffefd34010000000000fe340100000000",
    )
    .unwrap();

    let record = DividendDb::parse_value(&raw)
        .expect("should parse")
        .expect("record");
    assert_eq!(
        record.ex_dividend_date,
        NaiveDate::from_ymd_opt(2025, 1, 12).unwrap()
    );
    assert_eq!(
        record.record_date,
        Some(NaiveDate::from_ymd_opt(2025, 1, 10).unwrap())
    );
    assert_eq!(record.interest, 3.17);
    assert_eq!(record.stock_bonus, 0.0);
    assert_eq!(record.stock_gift, 0.0);
    assert_eq!(record.allot_num, 0.0);
    assert_eq!(record.allot_price, 0.0);
    assert_eq!(record.gugai, 100.0);
    assert!((record.adjust_factor - 1.032558).abs() < 1e-9);
}

#[test]
fn test_dividend_open_missing_db_returns_typed_error() {
    match DividendDb::new("/definitely/missing/dividend-db") {
        Err(DividendError::OpenDb(_)) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected missing db to fail"),
    }
}

#[test]
fn test_parse_dividend_key_timestamp_rejects_invalid_key() {
    let err = DividendDb::parse_key_timestamp(b"SH|185222").unwrap_err();
    assert!(matches!(err, DividendError::InvalidKey(_)));
}

#[cfg(test)]
fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(2) {
        return Err("hex length must be even".to_string());
    }

    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex at {}", i))?;
        let lo = (bytes[i + 1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex at {}", i + 1))?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Ok(out)
}
