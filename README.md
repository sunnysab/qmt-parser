# qmt-parser

**qmt-parser** 是一个高性能的 Rust 库，专门用于解析 MiniQMT (QMT) 交易终端生成的二进制历史数据文件（`.dat`）。

该库旨在提供极致的解析速度和内存效率，同时提供灵活的 API，支持将数据解析为原生的 Rust 结构体（`Vec<Struct>`）或直接生成 **Polars DataFrame** 以进行高效的数据分析。

## 概要

*   **支持格式**：
    *   **Tick (分笔数据)**：包含买卖五档盘口、成交量、成交额、状态码等。
    *   **1分钟 K线 (MinKline)**：包含 OHLC、成交量、成交额、持仓量等。
    *   **日 K线 (DailyKline)**：支持日期范围过滤，包含复权前收盘价计算、停牌标识等业务逻辑。
*   **双层 API**：底层 Iterator 模式支持流式读取，顶层提供 DataFrame 转换。
*   **高性能**：使用缓冲 IO (`BufReader`)、预分配内存 (`Vec::with_capacity`) 和定长数组优化堆内存分配。
*   **准确性**：自动处理 QMT 内部的 UTC 时间戳到 **Asia/Shanghai** 时区的转换。

## 设计理念

1.  **解析即迭代 (Parser as Iterator)**：
    核心解析逻辑实现为 `Iterator`。这意味着解析过程是惰性的，只有在消费数据时才会进行 IO 操作。这使得库可以轻松处理超大文件而不会爆内存。

2.  **分层架构**：
    *   **Level 1 (Data)**: 纯粹的 Rust数据结构 (`TickData`, `MinKlineData`)。
    *   **Level 2 (Reader)**: 负责处理二进制协议和字节序 (`byteorder`)。
    *   **Level 3 (API)**: 提供对用户友好的函数，分别适配标准库 (`Vec`) 和数据分析库 (`Polars`)。

3.  **零开销抽象**：
    在 Tick 解析中，买卖五档（Ask/Bid）使用栈上分配的固定大小数组 `[Option<f64>; 5]` 代替 `Vec`，显著减少了数百万次微小的堆内存分配。

## 安装

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
qmt-parser = { path = "." } # 或指向 git 仓库
polars = { version = "0.52", features = ["lazy", "temporal", "dtype-datetime", "timezones"] }
anyhow = "1.0"
```

> **注意**：本库依赖 Polars **0.52+** 版本，使用了最新的数组排序和表达式 API。

##  API 介绍

### 1. Tick 数据 (分笔)

解析高频 Tick 数据文件。

#### `parse_ticks_to_structs`
解析为 Rust 结构体列表，适合应用开发。

| 参数 | 类型 | 说明 |
| :--- | :--- | :--- |
| `path` | `impl AsRef<Path>` | `.dat` 文件路径 |

*   **返回值**: `Result<Vec<TickData>>`
*   **示例**:
    ```rust
    use qmt_parser::parse_ticks_to_structs;
    
    let ticks = parse_ticks_to_structs("data/000001.dat")?;
    println!("第一笔成交价: {:?}", ticks[0].last_price);
    ```

#### `parse_ticks_to_dataframe`
解析为 Polars DataFrame，适合量化分析。

*   **返回值**: `Result<DataFrame>`
*   **包含列**: `time`, `last_price`, `askPrice` (List), `bidPrice` (List), `volume`, `amount` 等。

可通过 `tick_api_field_names()` 取得 QMT `get_full_tick` 文档中的正式字段名列表，通过 `tick_dataframe_column_names()` 取得本库当前 DataFrame 输出列名。

---

### 2. 1分钟 K线 (MinKline)

解析 1 分钟频率的历史 K 线数据。

#### `parse_min_to_structs` / `parse_min_to_dataframe`

| 参数 | 类型 | 说明 |
| :--- | :--- | :--- |
| `path` | `impl AsRef<Path>` | `.dat` 文件路径 |

*   **返回值**: `Result<Vec<MinKlineData>>` 或 `Result<DataFrame>`
*   **示例**:
    ```rust
    use qmt_parser::parse_min_to_dataframe;

    let df = parse_min_to_dataframe("data/000001-1m.dat")?;
    println!("{}", df);
    ```

本库也导出了 `min_dataframe_column_names()`，便于上层复用当前输出 schema。

---

### 3. 日 K线 (DailyKline)

解析日线数据。**包含特殊的业务逻辑处理**。

#### `parse_daily_to_structs`
仅返回文件中的原始数据，**并根据日期范围进行过滤**。

| 参数 | 类型 | 说明 |
| :--- | :--- | :--- |
| `path` | `impl AsRef<Path>` | `.dat` 文件路径 |
| `start_date` | `&str` | 开始日期，格式 `"YYYYMMDD"` |
| `end_date` | `&str` | 结束日期，格式 `"YYYYMMDD"` |

*   **返回值**: `Result<Vec<DailyKlineData>>`

#### `parse_daily_to_dataframe`
返回经过业务逻辑处理的 DataFrame。

*   **业务逻辑**:
    1.  **停牌判断 (`suspendFlag`)**: 当 `volume == 0` 且 `amount == 0` 时标记为 1。
    2.  **昨收价修正 (`preClose`)**: 文件中的昨收价可能不准。此函数会通过 `close.shift(1)` 重新计算昨收价；若当日停牌，则昨收价等于当日收盘价。
    3.  **时区转换**: 时间戳统一转换为 Asia/Shanghai。

*   **示例**:
    ```rust
    use qmt_parser::parse_daily_to_dataframe;

    let df = parse_daily_to_dataframe(
        "data/day/000001.dat", 
        "20230101", 
        "20231231"
    )?;
    // df 包含 'preClose', 'suspendFlag' 等衍生列
    ```

本库也导出了 `daily_dataframe_column_names()`，便于上层复用当前输出 schema。

---

### 4. 财务数据 (Finance) —— 逆向工程，仍在补齐字段

> 注意：财务模块基于 QMT 本地 .DAT 格式的逆向结果，解析逻辑仍在迭代；当前已确认 `7001`/`7002`/`7003`/`7004`/`7005`/`7006`/`7007`/`7008` 的样本布局。三大报表和比率表仍主要返回数值列数组，列名映射尚未完全结构化。

#### `FinanceReader::read_file`
按文件名自动识别 TypeId（形如 `XXXXXX_7001.DAT`），返回 `Vec<FinanceRecord>`，内部枚举 `FinanceData` 区分各类型。

*   **返回值**: `Result<Vec<FinanceRecord>, FinanceError>`
*   **示例**:
    ```rust
    use qmt_parser::finance::{FinanceReader, FileType};
    let records = FinanceReader::read_file("finance/002419_7001.DAT")?;
    if let Some(first) = records.first() {
        println!("type: {:?}, report_date: {}", FileType::BalanceSheet, first.report_date);
    }
    ```

可配合 `FinanceReader::column_names(file_type)` 取得当前已确认的字段名列表；对 `Report` / `Ratios` 数据还可以用 `FinanceData::named_values(file_type)` 拿到 `(字段名, 数值)` 列表。

> 暂未提供 DataFrame 包装，若需要可在上层自行转换。

---

##  注意事项

1.  **价格精度**：
    QMT 二进制文件中价格使用 `u32` 存储（原始价格 * 1000）。本库在解析时会自动除以 1000.0 并转换为 `f64`。虽然 `f64` 在金融计算中是工业标准，但在极少数高精度场景下请留意浮点数误差。

2.  **成交额单位**：
    *   **Tick / 分钟线**：通常为元。
    *   **日线**：QMT 文件中存储的单位通常较大，本库解析时已按照 `amount / 100.0` 进行处理以还原标准单位。

3.  **指数数据**：
    如果是上证指数（如 `000001.SH`）的 Tick 数据，其买卖盘（Ask/Bid）数据在文件中通常全为 `0`，这是交易所数据的特性，并非解析错误。

4.  **Polars 版本兼容性**：
    由于 Polars 0.52 进行了大量 API 变更（如 `sort` 参数类型变化），请确保你的项目依赖版本与本库一致。
