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
        reserved: f64,
        reason: String,
    },
    /// 7005: 股东人数
    HolderCount {
        total_holders: i64,
        avg_share: f64,
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
    pub hold_amount: f64,
    pub hold_ratio: f64, // 比例 (如 0.05 代表 5%)
    pub change: String,  // 变动情况 (e.g. "未变")
    pub nature: String,  // 股东性质 (e.g. "自然人")
    pub share_type: String, // 股份类型 (e.g. "流通A股")
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
                    // Body Offset (Header=16): 0=Total, 8=Flow, 16=Restricted, 24=Reserved, 32=String
                    FinanceData::Capital {
                        total_share: Self::read_f64(body, 0).unwrap_or(0.0),
                        flow_share: Self::read_f64(body, 8).unwrap_or(0.0),
                        restricted: Self::read_f64(body, 16).unwrap_or(0.0),
                        reserved: Self::read_f64(body, 24).unwrap_or(0.0),
                        reason: Self::read_string(body, 32, 24), // 剩余空间读取字符串
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
                Self::parse_variable_blocks(data)
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
                    let avg_share = Self::read_f64(body, 8).unwrap_or(0.0);

                    results.push(FinanceRecord {
                        report_date,
                        announce_date,
                        data: FinanceData::HolderCount { total_holders, avg_share }
                    });
                    cursor += stride;
                    continue;
                }
            }
            cursor += 8;
        }
        Ok(results)
    }

    // --- 变长解析器 (7006, 7007) ---
    // 逻辑：扫描 Block Header [Count(4) + Report(8) + Announce(8)]
    // 然后根据 Next Header 的位置动态计算 Stride，解析内部的 Count 条记录
    /// 启发式解析 7006/7007 变长股东数据
    fn parse_variable_blocks(data: &[u8]) -> Result<Vec<FinanceRecord>, FinanceError> {
        let mut results = Vec::new();
        let mut headers = Vec::new(); // (offset, count, ts_rep, ts_ann)

        // 1. 第一遍扫描：找出所有的 Block Header
        let mut cursor = 0;
        while cursor + 20 <= data.len() {
            // 特征：[Count(4)] [Report(8)] [Announce(8)]
            // 有时候 Count 在日期前面。根据 hexdump：
            // 7006/7007: ... [Count: 4B] [Report: 8B] [Announce: 8B] ...
            // 或者 [Report: 8B] [Announce: 8B] ... [Count] ???
            // 之前的 Hexdump 显示: 1d00 Dates. 1cfc Count. -> Count 在 Dates 前面 4 字节。

            let ts1 = LittleEndian::read_i64(&data[cursor+4..cursor+12]);
            let ts2 = LittleEndian::read_i64(&data[cursor+12..cursor+20]);

            if Self::is_valid_ts(ts1) {
                let count = LittleEndian::read_u32(&data[cursor..cursor+4]) as usize;
                // Count 合理性检查 (1 ~ 500)
                if count > 0 && count < 1000 {
                    headers.push((cursor, count, ts1, ts2));
                    cursor += 20; // Skip header
                    continue;
                }
            }
            cursor += 4; // 步长 4 扫描
        }

        // 2. 第二遍：根据 Header 间距切分数据
        for i in 0..headers.len() {
            let (curr_off, count, ts_rep, ts_ann) = headers[i];

            // 计算数据区结束位置
            let data_start = curr_off + 20;
            let data_end = if i < headers.len() - 1 {
                headers[i+1].0 // 下一个 Header 的开始
            } else {
                data.len() // 文件末尾
            };

            if data_end <= data_start { continue; }

            let block_data = &data[data_start..data_end];

            // 尝试解析 Block 里的 Shareholder
            // 策略：简单粗暴的“字符串+Double”提取
            // 因为不知道具体 Struct 对齐，我们在 Block 数据中寻找 Double 特征
            let holders = Self::extract_holders_from_block(block_data, count);

            // 构造 Record
            // 只有当解析出数据才添加
            if !holders.is_empty() {
                let report_date = Self::ts_to_bj(ts_rep);
                let announce_date = if Self::is_valid_ts(ts_ann) { Self::ts_to_bj(ts_ann) } else { report_date };

                results.push(FinanceRecord {
                    report_date,
                    announce_date,
                    data: FinanceData::TopHolder { holders }
                });
            }
        }

        Ok(results)
    }

    /// 对单个变长 block 的启发式解析
    fn extract_holders_from_block(block: &[u8], count: usize) -> Vec<Shareholder> {
        // 这是一个启发式解析器 (Heuristic Parser)
        // 假设：每个 Shareholder 记录包含 [Name String] ... [Shares Double] ... [Ratio Double]
        // 我们可以根据 Record 数量平均分割 Block

        let mut holders = Vec::new();
        if count == 0 { return holders; }

        let stride = block.len() / count;
        if stride < 16 { return holders; } // 太短了不可能是记录

        for i in 0..count {
            let start = i * stride;
            let end = (start + stride).min(block.len());
            let rec_bytes = &block[start..end];

            // 解析 Name: 通常在开头，UTF-8 字符串
            let name = Self::read_string(rec_bytes, 0, 100); // 尝试读前100字节里的字符串

            // 解析 Shares 和 Ratio:
            // 扫描 record 里的 f64。通常 Shares 是很大的数，Ratio 是很小的数 (0~100)
            // 且 Shares 通常在前
            let mut found_doubles = Vec::new();
            let mut off = 0;
            while off + 8 <= rec_bytes.len() {
                if let Some(val) = Self::read_f64(rec_bytes, off) {
                    // 简单的过滤器
                    if val.abs() > 0.0001 {
                        found_doubles.push(val);
                    }
                }
                off += 4; // 4字节对齐扫描
            }

            // 启发式赋值
            let hold_amount = found_doubles.iter().find(|&&v| v > 10000.0).copied().unwrap_or(0.0);
            let hold_ratio = found_doubles.iter().find(|&&v| v < 100.0 && v > 0.0).copied().unwrap_or(0.0);

            // 尝试找“不变”、“新进”等关键词
            let change = if Self::bytes_contain(rec_bytes, "不变") { "不变".to_string() }
            else if Self::bytes_contain(rec_bytes, "新进") { "新进".to_string() }
            else { "".to_string() };

            if !name.is_empty() {
                holders.push(Shareholder {
                    name,
                    hold_amount,
                    hold_ratio,
                    change,
                    nature: "".to_string(), // 较难精确定位
                    share_type: "".to_string(),
                });
            }
        }
        holders
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

    /// 字节序列中是否包含某个 UTF-8 片段
    fn bytes_contain(data: &[u8], pattern: &str) -> bool {
        let pat_bytes = pattern.as_bytes();
        data.windows(pat_bytes.len()).any(|w| w == pat_bytes)
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

        // 验证关键字段是否解析成功
        if let FinanceData::Capital { total_share, .. } = &res[0].data {
            assert!(*total_share > 0.0, "Total share should be positive");
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

        if let FinanceData::HolderCount { total_holders, .. } = &res[0].data {
            assert!(*total_holders > 0, "Holders count should be positive");
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
        // 7006 可能为空（如果数据未更新），但通常会有数据
        // 注意：这里的 Record 代表一个“报告期”，里面的 data.holders 是该期的列表

        if !res.is_empty() {
            if let FinanceData::TopHolder { holders } = &res[0].data {
                // 验证是否解析出了股东名称
                if !holders.is_empty() {
                    assert!(!holders[0].name.is_empty(), "Shareholder name should not be empty");
                }
            } else {
                panic!("7006 parsed as wrong type");
            }
        }

        print_head(7006, &res);
    }

    #[test]
    fn test_7007_top_holder() {
        let path = get_fixture("002419_7007.DAT");
        if !path.exists() { eprintln!("Skipping 7007: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7007");

        if !res.is_empty() {
            if let FinanceData::TopHolder { holders } = &res[0].data {
                // 验证 UTF-8 中文解析
                if !holders.is_empty() {
                    println!("Sample 7007 Holder Name: {}", holders[0].name);
                }
            } else {
                panic!("7007 parsed as wrong type");
            }
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
