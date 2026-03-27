//! QMT 本地财务 .DAT 解析（逆向工程版本，字段可能不完全）

use std::fs::File;
use std::io;
use std::path::Path;

use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, FixedOffset, TimeZone};
use memmap2::MmapOptions;
use thiserror::Error;

const STRIDE_REPORT: usize = 656;  // 7001, 7002, 7003
const STRIDE_RATIOS: usize = 344;  // 7008
const STRIDE_CAPITAL: usize = 56;  // 7004
const STRIDE_HOLDER: usize = 64;   // 7005
const STRIDE_TOP_HOLDER: usize = 416; // 7006, 7007

// 有效时间戳范围 (1990年 - 2050年, 毫秒)
const MIN_VALID_TS: i64 = 631_152_000_000;
const MAX_VALID_TS: i64 = 2_524_608_000_000;
const QMT_NAN_HEX: u64 = 0x7FEFFFFFFFFFFFFF;

// --- 错误定义 ---
/// 财务解析错误
#[derive(Debug, Error)]
pub enum FinanceError {
    #[error("IO Error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid File Extension: {0}")]
    InvalidExtension(String),
    #[error("Unsupported File Type ID: {0}")]
    UnsupportedType(u16),
    #[error("Parse Error: {0}")]
    Parse(String),
}

/// 北京时间类型 (UTC+8)
pub type BjDateTime = DateTime<FixedOffset>;

/// 一条财务记录，包含报告/公告时间与具体数据枚举
#[derive(Debug, Clone)]
pub struct FinanceRecord {
    pub report_date: BjDateTime,
    pub announce_date: BjDateTime,
    pub data: FinanceData,
}

/// 不同类型的财务数据载荷
#[derive(Debug, Clone)]
pub enum FinanceData {
    /// 7001, 7002, 7003: 财务报表 (80个指标)
    Report { columns: Vec<f64> },
    /// 7004: 股本结构
    Capital {
        total_share: f64,
        flow_share: f64,
        restricted: f64,
        free_float_share: f64,
    },
    /// 7005: 股东人数
    HolderCount {
        total_holders: i64,
        a_holders: i64,
        b_holders: i64,
        h_holders: i64,
        float_holders: i64,
        other_holders: i64,
    },
    /// 7006, 7007: 十大(流通)股东
    TopHolder {
        holders: Vec<Shareholder>,
    },
    /// 7008: 财务比率 (41个指标)
    Ratios { ratios: Vec<f64> },
}

/// 股东信息（变长文件启发式解析）
#[derive(Debug, Clone)]
pub struct Shareholder {
    pub name: String,
    pub holder_type: String,
    pub hold_amount: f64,
    pub change_reason: String,
    pub hold_ratio: f64, // 比例 (如 0.05 代表 5%)
    pub share_type: String, // 股份性质 (e.g. "流通A股")
    pub rank: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// 财务文件类型枚举 (7001-7008)
pub enum FileType {
    BalanceSheet = 7001,
    Income = 7002,
    CashFlow = 7003,
    Capital = 7004,
    HolderCount = 7005,
    TopFlowHolder = 7006,
    TopHolder = 7007,
    Ratios = 7008,
}

impl FileType {
    pub fn from_id(id: u16) -> Option<Self> {
        match id {
            7001 => Some(Self::BalanceSheet),
            7002 => Some(Self::Income),
            7003 => Some(Self::CashFlow),
            7004 => Some(Self::Capital),
            7005 => Some(Self::HolderCount),
            7006 => Some(Self::TopFlowHolder),
            7007 => Some(Self::TopHolder),
            7008 => Some(Self::Ratios),
            _ => None,
        }
    }
}


/// 财务文件读取器
pub struct FinanceReader;

impl FinanceReader {
    /// 读取文件并解析为 Struct 列表
    pub fn read_file(path: impl AsRef<Path>) -> Result<Vec<FinanceRecord>, FinanceError> {
        let path = path.as_ref();
        let file_type = Self::detect_type(path)?;
        let file = File::open(path)?;

        // mmap 零拷贝读取
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let data = &mmap[..];

        match file_type {
            FileType::BalanceSheet | FileType::Income | FileType::CashFlow => {
                Self::parse_fixed(data, STRIDE_REPORT, |body| {
                    let mut cols = Vec::with_capacity(80);
                    for i in 0..80 {
                        cols.push(Self::read_f64(body, i * 8).unwrap_or(f64::NAN));
                    }
                    FinanceData::Report { columns: cols }
                })
            }
            FileType::Ratios => {
                Self::parse_fixed(data, STRIDE_RATIOS, |body| {
                    let mut cols = Vec::with_capacity(41);
                    for i in 0..41 {
                        cols.push(Self::read_f64(body, i * 8).unwrap_or(f64::NAN));
                    }
                    FinanceData::Ratios { ratios: cols }
                })
            }
            FileType::Capital => {
                Self::parse_fixed(data, STRIDE_CAPITAL, |body| {
                    // Body Offset (Header=16): 0=Total, 8=Flow, 16=Restricted, 24=FreeFloat
                    FinanceData::Capital {
                        total_share: Self::read_f64(body, 0).unwrap_or(0.0),
                        flow_share: Self::read_f64(body, 8).unwrap_or(0.0),
                        restricted: Self::read_f64(body, 16).unwrap_or(0.0),
                        free_float_share: Self::read_f64(body, 24).unwrap_or(0.0),
                    }
                })
            }
            FileType::HolderCount => {
                // 7005 特殊 Header 顺序: [Announce] [Report]. parse_fixed 默认读 [Report][Announce]
                // 需要特殊处理? 不，QMT 的 7005 Header 顺序根据 Hex 是:
                // 00..07: AnnounceDate, 08..15: ReportDate
                // 我们在 parse_fixed 内部交换一下即可，或者在回调里处理
                // 为了统一，我们使用专门的 parse_7005
                Self::parse_7005_fixed(data)
            }
            FileType::TopFlowHolder | FileType::TopHolder => {
                Self::parse_top_holders(data)
            }
        }
    }

    /// 从文件名中解析 TypeId 并映射到枚举
    fn detect_type(path: &Path) -> Result<FileType, FinanceError> {
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let id_part = stem.split('_').last().unwrap_or("");
        let id = id_part.parse::<u16>().map_err(|_| FinanceError::Parse("Invalid Filename".into()))?;
        FileType::from_id(id).ok_or(FinanceError::UnsupportedType(id))
    }

    // --- 定长解析器 (7001-7004, 7008) ---
    /// 通用定长表解析器，接收回调解析正文部分
    fn parse_fixed<F>(data: &[u8], stride: usize, parser: F) -> Result<Vec<FinanceRecord>, FinanceError>
    where F: Fn(&[u8]) -> FinanceData {
        let mut results = Vec::new();
        let mut cursor = 0;
        let len = data.len();

        while cursor + 16 <= len {
            // 扫描 Header
            let ts1 = LittleEndian::read_i64(&data[cursor..cursor+8]);
            let ts2 = LittleEndian::read_i64(&data[cursor+8..cursor+16]);

            // 7001-7004, 7008 顺序: Report, Announce
            if Self::is_valid_ts(ts1) {
                // 如果发现有效头
                if cursor + stride <= len {
                    let report_date = Self::ts_to_bj(ts1);
                    // Announce Date 可能是 0，如果是 0，回退到 Report Date
                    let announce_date = if Self::is_valid_ts(ts2) {
                        Self::ts_to_bj(ts2)
                    } else {
                        report_date // Fallback
                    };

                    let body = &data[cursor+16..cursor+stride];
                    results.push(FinanceRecord {
                        report_date,
                        announce_date,
                        data: parser(body)
                    });

                    cursor += stride;
                    continue;
                }
            }
            // 滑动窗口寻找下一个有效头
            cursor += 8;
        }
        Ok(results)
    }

    // --- 7005 专用解析 (Header 顺序颠倒) ---
    /// 针对 7005 (股东人数) 的定长解析
    fn parse_7005_fixed(data: &[u8]) -> Result<Vec<FinanceRecord>, FinanceError> {
        let mut results = Vec::new();
        let mut cursor = 0;
        let stride = STRIDE_HOLDER;

        while cursor + 16 <= data.len() {
            let ts1 = LittleEndian::read_i64(&data[cursor..cursor+8]); // Announce
            let ts2 = LittleEndian::read_i64(&data[cursor+8..cursor+16]); // Report

            // 只要有一个有效，就尝试解析
            if Self::is_valid_ts(ts2) {
                if cursor + stride <= data.len() {
                    let report_date = Self::ts_to_bj(ts2);
                    let announce_date = if Self::is_valid_ts(ts1) { Self::ts_to_bj(ts1) } else { report_date };

                    let body = &data[cursor+16..cursor+stride];
                    let total_holders = Self::read_f64(body, 0).unwrap_or(0.0) as i64;
                    let a_holders = Self::read_f64(body, 8).unwrap_or(0.0) as i64;
                    let b_holders = Self::read_f64(body, 16).unwrap_or(0.0) as i64;
                    let h_holders = Self::read_f64(body, 24).unwrap_or(0.0) as i64;
                    let float_holders = Self::read_f64(body, 32).unwrap_or(0.0) as i64;
                    let other_holders = Self::read_f64(body, 40).unwrap_or(0.0) as i64;

                    results.push(FinanceRecord {
                        report_date,
                        announce_date,
                        data: FinanceData::HolderCount {
                            total_holders,
                            a_holders,
                            b_holders,
                            h_holders,
                            float_holders,
                            other_holders,
                        }
                    });
                    cursor += stride;
                    continue;
                }
            }
            cursor += 8;
        }
        Ok(results)
    }

    /// 解析 7006/7007 十大(流通)股东定长记录，并按同一报告期聚合。
    fn parse_top_holders(data: &[u8]) -> Result<Vec<FinanceRecord>, FinanceError> {
        let mut results = Vec::new();
        let mut current_report_ts = 0i64;
        let mut current_announce_ts = 0i64;
        let mut current_holders = Vec::new();

        for chunk in data.chunks_exact(STRIDE_TOP_HOLDER) {
            let announce_ts = LittleEndian::read_i64(&chunk[0..8]);
            let report_ts = LittleEndian::read_i64(&chunk[8..16]);
            if !Self::is_valid_ts(announce_ts) || !Self::is_valid_ts(report_ts) {
                continue;
            }

            let holder = Self::parse_top_holder_record(chunk);
            if current_holders.is_empty() {
                current_report_ts = report_ts;
                current_announce_ts = announce_ts;
            }

            if report_ts != current_report_ts || announce_ts != current_announce_ts {
                results.push(FinanceRecord {
                    report_date: Self::ts_to_bj(current_report_ts),
                    announce_date: Self::ts_to_bj(current_announce_ts),
                    data: FinanceData::TopHolder {
                        holders: std::mem::take(&mut current_holders),
                    },
                });
                current_report_ts = report_ts;
                current_announce_ts = announce_ts;
            }

            current_holders.push(holder);
        }

        if !current_holders.is_empty() {
            results.push(FinanceRecord {
                report_date: Self::ts_to_bj(current_report_ts),
                announce_date: Self::ts_to_bj(current_announce_ts),
                data: FinanceData::TopHolder {
                    holders: current_holders,
                },
            });
        }

        Ok(results)
    }

    fn parse_top_holder_record(record: &[u8]) -> Shareholder {
        Shareholder {
            name: Self::read_string(record, 16, 192),
            holder_type: Self::read_string(record, 216, 56),
            hold_amount: Self::read_f64(record, 272).unwrap_or(0.0),
            change_reason: Self::read_string(record, 280, 16),
            hold_ratio: Self::read_f64(record, 304).unwrap_or(0.0),
            share_type: Self::read_string(record, 312, 96),
            rank: LittleEndian::read_u32(&record[412..416]),
        }
    }

    fn is_valid_ts(ts: i64) -> bool {
        ts >= MIN_VALID_TS && ts <= MAX_VALID_TS
    }

    /// 将毫秒时间戳转换为北京时间
    fn ts_to_bj(ts: i64) -> BjDateTime {
        // 构建 UTC+8
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let secs = ts / 1000;
        let nsecs = (ts % 1000) * 1_000_000;
        tz.timestamp_opt(secs, nsecs as u32).single().unwrap_or_default()
    }

    /// 读取 f64 并处理哨兵值
    fn read_f64(data: &[u8], offset: usize) -> Option<f64> {
        if offset + 8 > data.len() { return None; }
        let u = LittleEndian::read_u64(&data[offset..offset+8]);
        if u == QMT_NAN_HEX { return None; }
        let f = f64::from_bits(u);
        if f.is_nan() { None } else { Some(f) }
    }

    /// 从定长缓冲区读取 UTF-8 字符串
    fn read_string(data: &[u8], offset: usize, max_len: usize) -> String {
        if offset >= data.len() { return String::new(); }
        let end = (offset + max_len).min(data.len());
        let slice = &data[offset..end];
        // 找 \0 结尾
        let actual_len = slice.iter().position(|&c| c == 0).unwrap_or(slice.len());
        String::from_utf8_lossy(&slice[..actual_len]).trim().to_string()
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn get_fixture(file: &str) -> PathBuf {
        PathBuf::from("/home/sunnysab/Code/trade-rs/qmt-parser/finance/").join(file)
    }

    // 辅助函数：打印前5条
    fn print_head(type_id: u16, records: &[FinanceRecord]) {
        println!("\n>>> [Type {}] Found {} records. Showing first 5:", type_id, records.len());
        for (i, rec) in records.iter().take(5).enumerate() {
            println!("#{:03} | Report: {} | Announce: {}",
                     i,
                     rec.report_date.format("%Y-%m-%d"),
                     rec.announce_date.format("%Y-%m-%d")
            );
            // 打印具体的 Data 枚举内容，使用 {:#?} 美化输出
            println!("Data: {:#?}\n", rec.data);
        }
        if records.is_empty() {
            println!("(No records found)\n");
        } else {
            println!("... (remaining {} records omitted)\n", records.len().saturating_sub(5));
        }
    }

    #[test]
    fn test_7001_balance_sheet() {
        let path = get_fixture("002419_7001.DAT");
        if !path.exists() { eprintln!("Skipping 7001: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7001");
        assert!(!res.is_empty(), "7001 should not be empty");

        // 验证类型是否正确
        if let FinanceData::Report { columns } = &res[0].data {
            assert_eq!(columns.len(), 80);
        } else {
            panic!("7001 parsed as wrong type");
        }

        print_head(7001, &res);
    }

    #[test]
    fn test_7002_income() {
        let path = get_fixture("002419_7002.DAT");
        if !path.exists() { eprintln!("Skipping 7002: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7002");
        assert!(!res.is_empty(), "7002 should not be empty");

        if let FinanceData::Report { columns } = &res[0].data {
            assert_eq!(columns.len(), 80);
        } else {
            panic!("7002 parsed as wrong type");
        }

        print_head(7002, &res);
    }

    #[test]
    fn test_7003_cashflow() {
        let path = get_fixture("002419_7003.DAT");
        if !path.exists() { eprintln!("Skipping 7003: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7003");
        assert!(!res.is_empty(), "7003 should not be empty");

        if let FinanceData::Report { columns } = &res[0].data {
            assert_eq!(columns.len(), 80);
        } else {
            panic!("7003 parsed as wrong type");
        }

        print_head(7003, &res);
    }

    #[test]
    fn test_7004_capital() {
        let path = get_fixture("002419_7004.DAT");
        if !path.exists() { eprintln!("Skipping 7004: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7004");
        assert!(!res.is_empty(), "7004 should not be empty");

        if let FinanceData::Capital {
            total_share,
            flow_share,
            restricted,
            free_float_share,
        } = &res[0].data {
            assert_eq!(*total_share, 400_100_000.0);
            assert_eq!(*flow_share, 40_080_000.0);
            assert_eq!(*restricted, 0.0);
            assert_eq!(*free_float_share, 40_080_000.0);
        } else {
            panic!("7004 parsed as wrong type");
        }

        print_head(7004, &res);
    }

    #[test]
    fn test_7005_holder_count() {
        let path = get_fixture("002419_7005.DAT");
        if !path.exists() { eprintln!("Skipping 7005: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7005");
        assert!(!res.is_empty(), "7005 should not be empty");

        if let FinanceData::HolderCount {
            total_holders,
            a_holders,
            b_holders,
            h_holders,
            float_holders,
            other_holders,
        } = &res[0].data {
            assert_eq!(*total_holders, 35_719);
            assert_eq!(*a_holders, 35_719);
            assert_eq!(*b_holders, 0);
            assert_eq!(*h_holders, 0);
            assert_eq!(*float_holders, 0);
            assert_eq!(*other_holders, 0);
        } else {
            panic!("7005 parsed as wrong type");
        }

        print_head(7005, &res);
    }

    #[test]
    fn test_7006_top_float_holder() {
        let path = get_fixture("002419_7006.DAT");
        if !path.exists() { eprintln!("Skipping 7006: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7006");
        assert!(!res.is_empty(), "7006 should not be empty");

        if let FinanceData::TopHolder { holders } = &res[0].data {
            assert_eq!(holders.len(), 40);
            assert_eq!(holders[0].name, "中国航空技术深圳有限公司");
            assert_eq!(holders[0].holder_type, "机构投资账户");
            assert_eq!(holders[0].hold_amount, 158_128_000.0);
            assert_eq!(holders[0].change_reason, "不变");
            assert_eq!(holders[0].hold_ratio, 39.52);
            assert_eq!(holders[0].share_type, "流通A股");
            assert_eq!(holders[0].rank, 1);
        } else {
            panic!("7006 parsed as wrong type");
        }

        print_head(7006, &res);
    }

    #[test]
    fn test_7007_top_holder() {
        let path = get_fixture("002419_7007.DAT");
        if !path.exists() { eprintln!("Skipping 7007: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7007");
        assert!(!res.is_empty(), "7007 should not be empty");

        if let FinanceData::TopHolder { holders } = &res[0].data {
            assert_eq!(holders.len(), 10);
            assert_eq!(holders[0].name, "中国工商银行-诺安股票证券投资基金");
            assert_eq!(holders[0].holder_type, "机构投资账户");
            assert_eq!(holders[0].hold_amount, 1_799_860.0);
            assert_eq!(holders[0].change_reason, "不变");
            assert_eq!(holders[0].hold_ratio, 0.45);
            assert_eq!(holders[0].share_type, "流通A股");
            assert_eq!(holders[0].rank, 1);
        } else {
            panic!("7007 parsed as wrong type");
        }

        print_head(7007, &res);
    }

    #[test]
    fn test_7008_ratios() {
        let path = get_fixture("002419_7008.DAT");
        if !path.exists() { eprintln!("Skipping 7008: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7008");
        assert!(!res.is_empty(), "7008 should not be empty");

        if let FinanceData::Ratios { ratios } = &res[0].data {
            assert_eq!(ratios.len(), 41);
        } else {
            panic!("7008 parsed as wrong type");
        }

        print_head(7008, &res);
    }
}
