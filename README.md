# qmt-parser

`qmt-parser` 是一个面向 MiniQMT / QMT 本地数据目录的 Rust 解析库，用来读取历史二进制 `.dat` 文件，以及部分本地元数据存储。

它的目标很直接：

- 给应用层提供稳定、typed 的 Rust API。
- 给量化分析场景提供可选的 Polars `DataFrame` 输出。
- 把 QMT 文件里的时间戳、价格缩放、停牌判断等细节收敛到库内部。

## 支持的内容

- Tick 分笔文件
  - 解析为 `Vec<TickData>`
  - 在启用 `polars` feature 时解析为 `DataFrame`
- 1 分钟 K 线文件
  - 解析为 `Vec<MinKlineData>`
  - 可选 `DataFrame`
- 日 K 线文件
  - 支持字符串日期范围与 `NaiveDate` typed 日期范围
  - `DataFrame` 输出会补齐 `preClose`、`suspendFlag`
- 财务 `.DAT`
  - 自动识别 `7001` 到 `7008` 类型
  - 返回 `Vec<FinanceRecord>`，并附带 `FinanceData` typed 载荷
- 分红送配 LevelDB
  - 通过 `DividendDb` 查询指定证券的除权除息记录
- xtquant 本地资料文件
  - `holiday.csv` / `holiday.dat`
  - `IndustryData.txt`
  - `systemSectorWeightData.txt`
  - `customSectorWeightData.txt`
  - `sectorlist.DAT`
  - `sectorWeightData.txt`

## 安装

默认会启用 `polars` feature。

```toml
[dependencies]
qmt-parser = "0.1.0"
```

如果你只需要纯 Rust 结构体，不需要 `DataFrame` 输出，可以关闭默认 feature：

```toml
[dependencies]
qmt-parser = { version = "0.1.0", default-features = false }
```

如果你在本地联调，还可以直接用 Git 仓库：

```toml
[dependencies]
qmt-parser = { git = "https://github.com/sunnysab/qmt-parser" }
```

## 快速开始

### 解析 Tick 为结构体

```rust
use qmt_parser::parse_ticks_to_structs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ticks = parse_ticks_to_structs("data/000001-20250529-tick.dat")?;
    if let Some(first) = ticks.first() {
        println!("{} {} {:?}", first.symbol, first.date, first.last_price);
    }
    Ok(())
}
```

### 解析分钟线为 DataFrame

```rust
#[cfg(feature = "polars")]
use qmt_parser::parse_min_to_dataframe;

#[cfg(feature = "polars")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let df = parse_min_to_dataframe("data/000001-1m.dat")?;
    println!("{:?}", df.shape());
    Ok(())
}

#[cfg(not(feature = "polars"))]
fn main() {}
```

### 解析日线并限制日期范围

```rust
use qmt_parser::parse_daily_to_structs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows = parse_daily_to_structs("data/day/000001.dat", "20230101", "20231231")?;
    println!("rows = {}", rows.len());
    Ok(())
}
```

### 读取财务文件

```rust
use qmt_parser::finance::FinanceReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let records = FinanceReader::read_file("finance/002419_7001.DAT")?;
    if let Some(first) = records.first() {
        println!("{:?} {:?}", first.file_type, first.report_date);
    }
    Ok(())
}
```

### 查询分红送配数据库

```rust
use qmt_parser::dividend::DividendDb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = DividendDb::new("/path/to/DividData")?;
    let records = db.query("SH", "600000")?;
    println!("records = {}", records.len());
    Ok(())
}
```

## API 结构

- `qmt_parser::tick`
  - Tick 分笔解析、列名常量、`TickReader`
- `qmt_parser::min`
  - 1 分钟 K 线解析、`MinReader`
- `qmt_parser::day`
  - 日线解析、日期过滤、DataFrame 派生逻辑
- `qmt_parser::finance`
  - 财务 `.DAT` 自动识别与 typed 记录
- `qmt_parser::dividend`
  - LevelDB 分红送配查询
- `qmt_parser::metadata`
  - xtquant 本地资料文件解析
- `qmt_parser::error`
  - Tick / Min / Day 解析错误类型

crate 根模块已经重导出了最常用的解析函数和结构体，常见调用可以直接从 `qmt_parser::*` 获取。

## xtquant 本地资料文件

如果你只想复用 xtquant 已经落地到本地的数据文件，可以直接使用：

- `parse_holiday_file`
- `parse_industry_file`
- `parse_sector_name_file`
- `parse_sector_weight_index`
- `parse_sectorlist_dat`
- `parse_sector_weight_members`

如果你希望按 xtquant 约定目录自动发现这些文件，可以直接使用：

- `load_holidays_from_standard_paths`
- `load_industry_from_standard_paths`
- `load_sector_names_from_standard_paths`
- `load_sectorlist_from_standard_paths`
- `load_sector_weight_members_from_standard_paths`
- `load_sector_weight_index_from_standard_paths`

## 时间与数值语义

- Tick `raw_qmt_timestamp` 保留原始值，`DataFrame` 路径还会额外生成 `time`
- Tick 价格字段按 `1000.0` 缩放
- 日线成交额按 `100.0` 缩放
- 日线 `DataFrame` 中的 `preClose` 不是文件原值，而是库内按业务规则重算后的结果
- 分红模块保留 `timestamp_raw` 与 `unknown64_raw`，避免未确认字段被静默丢失

## Feature

- `polars`（默认开启）
  - 提供 `parse_*_to_dataframe` 系列接口
  - 暴露当前 DataFrame 输出列名常量和 helper

## 适用边界

- 这个库面向 QMT 本地数据格式，字段语义以实测样本和逆向结果为准
- `finance` 与 `dividend` 模块里仍有少数字段保留原始含义或启发式命名
- README 给的是总览；更细的字段说明、返回值语义和示例请直接看 rustdoc

## 生成文档

```bash
cargo doc --no-deps
```

如果需要同时看到 `polars` 相关接口，请确保在默认 feature 下生成，或者显式执行：

```bash
cargo doc --no-deps --features polars
```
