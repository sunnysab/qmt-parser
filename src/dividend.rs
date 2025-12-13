use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, NaiveDate, Utc};
use rusty_leveldb::{DB, Options, LdbIterator};
use std::path::Path;
use std::str;

/// 除权/分红数据结构
#[derive(Debug, Clone)]
pub struct DividendRecord {
    pub date: NaiveDate,      // 除权/付息日期
    pub cash: f64,            // 派息/分红金额 (每股/每百元)
    pub share_ratio: f64,     // 送转股比例
    pub allotment_price: f64, // 配股价
    pub timestamp_raw: i64,   // 原始时间戳
}

pub struct DividendDb {
    db: DB,
}

impl DividendDb {
    /// 打开数据库
    /// 注意：LevelDB 同时只能被一个进程加锁访问。如果 QMT 在运行，这里可能会失败。
    /// 建议传入备份目录路径。
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let mut options = Options::default();
        // 设为只读模式稍安全，但 rusty-leveldb 主要是靠文件锁
        options.create_if_missing = false;

        match DB::open(path, options) {
            Ok(db) => Ok(Self { db }),
            Err(e) => Err(format!("无法打开 LevelDB: {}", e)),
        }
    }

    /// 查询指定股票/债券的除权信息
    /// market: "SH", "SZ", "BJ"
    /// code: "185222", "600000" 等
    pub fn query(&mut self, market: &str, code: &str) -> Vec<DividendRecord> {
        let mut results = Vec::new();

        // 构造 Key 前缀，例如 "SH|185222"
        // 注意：实际 Key 可能是 "SH|185222|4000|..."
        let prefix = format!("{}|{}|", market, code);
        let prefix_bytes = prefix.as_bytes();

        // 创建迭代器
        let mut iter = self.db.new_iter().unwrap();

        // Seek 到第一个匹配前缀的位置
        iter.seek(prefix_bytes);

        while let Some((key, value)) = iter.next() {
            // 1. 检查 Key 是否以前缀开头，如果不是则说明已经遍历完该标的的数据
            if !key.starts_with(prefix_bytes) {
                break;
            }

            // 2. 解析 Key，提取时间戳
            // Key 格式: "SH|185222|4000|1736697600000"
            let key_str = match str::from_utf8(&key) {
                Ok(s) => s,
                Err(_) => continue, // 非法 Key 忽略
            };

            let parts: Vec<&str> = key_str.split('|').collect();
            if parts.len() < 4 {
                continue;
            }

            // 解析时间戳 (Key 的最后一部分)
            let ts_key: i64 = parts.last().unwrap().parse().unwrap_or(0);

            // 3. 过滤无效的哨兵数据 (0 或 999999...)
            // 999999999999 对应 2001-09-09，是 LevelDB 常用的 End Sentinel
            if ts_key == 0 || ts_key > 3_000_000_000_000 {
                continue;
            }

            // 4. 解析 Value (C++ Struct 二进制)
            if let Some(record) = Self::parse_value(&value) {
                // 双重校验：通常 Value 里的时间戳应该和 Key 里的接近或一致
                // 这里我们信任 Value 解析出的数据，或者以 Key 为准
                results.push(record);
            }
        }

        results
    }

    /// 解析二进制 Value
    /// 结构: [8 bytes Padding] [8 bytes TS] [8 bytes Cash] [8 bytes Ratio] [8 bytes Price]
    fn parse_value(data: &[u8]) -> Option<DividendRecord> {
        if data.len() < 40 {
            return None;
        }

        // 使用 LittleEndian 读取
        // 0..8 是未知的 Padding (对应你看到的 e0 67 f9...)，跳过
        let ts_val = LittleEndian::read_i64(&data[8..16]);
        let cash = LittleEndian::read_f64(&data[16..24]);
        let ratio = LittleEndian::read_f64(&data[24..32]);
        let price = LittleEndian::read_f64(&data[32..40]);

        // 转换时间戳 (毫秒 -> NaiveDate)
        // 注意：有时数据可能是秒级，但 QMT 这里通常是毫秒
        let seconds = ts_val / 1000;
        let nanoseconds = (ts_val % 1000) * 1_000_000;

        let dt = DateTime::<Utc>::from_timestamp(seconds, nanoseconds as u32);

        match dt {
            Some(d) => Some(DividendRecord {
                date: d.date_naive(),
                cash,
                share_ratio: ratio,
                allotment_price: price,
                timestamp_raw: ts_val,
            }),
            None => None,
        }
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
    let records = qmt_db.query("SH", "185222");

    if records.is_empty() {
        eprintln!("未找到记录或解析失败。");
    }

    for record in records {
        println!("--------------------------------");
        println!("日期: {}", record.date);
        println!("派息/分红: {:.4}", record.cash);
        println!("送转比例 : {:.4}", record.share_ratio);
        println!("配股价   : {:.4}", record.allotment_price);
    }
}