这是一个基于逆向工程分析得出的 **QMT 本地财务数据文件 (.DAT) 格式技术规范**。

该文档旨在为开发者提供底层读取能力，无需依赖 QMT 客户端 API 即可直接解析数据。

---

# QMT 财务数据 (.DAT) 文件格式规范 v1.0

**最后更新**: 2025-12-13
**适用版本**: QMT / MiniQMT (XtQuant) 财务数据模块
**状态**: 逆向工程 (非官方)

## 1. 概述 (General Overview)

QMT 的财务数据存储在本地文件系统中，采用 **二进制序列化** 格式。不同类型的财务表（如资产负债表、十大股东等）使用不同的后缀 ID（如 `_7001.DAT`, `_7007.DAT`），但共享相似的基础数据类型和字节序。

### 1.1 文件存储路径
通常位于 QMT 安装目录下的 `datadir` 文件夹内：
```text
{QMT_ROOT}/datadir/Finance/{Market}/{Block}/{Code}_{TypeID}.DAT
```
*   **Market**: 市场标识，如 `SH` (上海), `SZ` (深圳), `BJ` (北京)。
*   **Block**: 分块目录，通常是股票代码的前几位（如 `86400`），用于避免单目录文件过多。
*   **Code**: 证券代码（如 `430047`）。
*   **TypeID**: 数据表类型 ID（见 1.4）。

### 1.2 基础数据类型 (Primitives)
除非另有说明，所有数据均采用 **小端序 (Little-Endian)**。

| 类型 | 长度 (Bytes) | 说明 |
| :--- | :--- | :--- |
| **Int64 (i64)** | 8 | 有符号 64 位整数，通常用于时间戳。 |
| **Int32 (i32)** | 4 | 有符号 32 位整数。 |
| **Double (f64)**| 8 | IEEE 754 双精度浮点数。 |
| **Char[]** | 变长/定长 | 字节数组，编码通常为 **UTF-8** (长文本) 或 **UTF-16LE** (短标签)。 |

### 1.3 特殊值 (Sentinel Values)
*   **NaN (空值)**: QMT 使用特定的位模式表示“无数据”。
    *   Hex: `FF FF FF FF FF FF EF 7F`
    *   Value: `DBL_MAX` 附近或标准 NaN。
    *   **处理建议**: 读取时若遇到此值，应视为 `Null` / `None`。
*   **Timestamp**: 毫秒级 Unix 时间戳。
    *   例: `1640966400000` -> `2021-12-31 16:00:00 UTC`。

### 1.4 文件类型索引

| Type ID | 内容描述 | 格式类型 | 记录步长 (Stride) |
| :--- | :--- | :--- | :--- |
| **7001** | 资产负债表 (Balance Sheet) | 定长 (Fixed) | 656 Bytes |
| **7002** | 利润表 (Income Statement) | 定长 (Fixed) | 656 Bytes |
| **7003** | 现金流量表 (Cash Flow) | 定长 (Fixed) | 656 Bytes |
| **7004** | 股本结构 (Capital Structure) | 定长 (Fixed) | 56 Bytes |
| **7005** | 股东户数 (Shareholder Num) | 定长 (Fixed) | 64 Bytes |
| **7006** | 十大流通股东 (Top 10 Float) | **变长 (Variable)** | N/A |
| **7007** | 十大股东 (Top 10 Holders) | **变长 (Variable)** | N/A |
| **7008** | 财务比率/每股指标 (Ratios) | 定长 (Fixed) | 344 Bytes |

---

## 2. 定长格式规范 (Fixed-Length Format)

适用于 Type ID: **7001, 7002, 7003, 7004, 7005, 7008**。
文件由连续的定长记录块（Record Block）组成，无文件头，直接存储数据。

### 2.1 财务报表 (7001, 7002, 7003)
这是最常见的三大报表格式，采用稀疏矩阵存储。

*   **记录长度 (Stride)**: 656 Bytes
*   **结构定义**:

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **ReportDate** | 报告期 (e.g., 2024-12-31) |
| `0x08` | Int64 | **AnnounceDate**| 公告日期 (e.g., 2025-04-20) |
| `0x10` ... `0x28F` | Double[80] | **Columns** | 财务数据列数组，共 80 个 Double。 |

*   **注意**: 这是一个通用容器，不同行业的公司只会填充其中一部分列，其余填充 `NaN`。列的具体含义需映射外部定义的 `table_xml`。

### 2.2 财务比率 (7008)
存储计算后的财务指标（如 EPS, ROE）。

*   **记录长度 (Stride)**: 344 Bytes
*   **结构定义**:

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **ReportDate** | 报告期 |
| `0x08` | Int64 | **AnnounceDate**| 公告日期 |
| `0x10` ... `0x157` | Double[41] | **Ratios** | 指标数据列，共 41 个 Double。 |

### 2.3 股本结构 (7004)
存储总股本、流通股本及变动原因。

*   **记录长度 (Stride)**: 56 Bytes
*   **结构定义**:

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **ReportDate** | 变动日期/报告期 |
| `0x08` | Int64 | **AnnounceDate**| 公告日期 |
| `0x10` | Double | **TotalShare** | 总股本 |
| `0x18` | Double | **FlowShare** | 流通股本 |
| `0x20` | Double | **Restricted** | 限售股本 |
| `0x28` | Double | **Reserved** | 保留字段 (通常为 0) |
| `0x30` ... `0x37` | Byte[8] | **Reason** | 变动原因/标签 (String) |

### 2.4 股东户数 (7005)

*   **记录长度 (Stride)**: 64 Bytes
*   **结构定义**:

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **AnnounceDate**| **注意**: 7005 的时间戳顺序可能反转，需按数值大小判断。 |
| `0x08` | Int64 | **ReportDate** | 报告期 (截止日) |
| `0x10` | Double | **TotalHolders**| 股东总户数 |
| `0x18` | Double | **AvgShare** | 平均持股数 / A股户数 |
| `0x20` ... `0x3F` | Byte[32]| **Padding** | 填充/保留字段 (通常全 0) |

---

## 3. 变长格式规范 (Variable-Length Format)

适用于 Type ID: **7006 (十大流通), 7007 (十大股东)**。
由于包含不确定长度的 UTF-8 字符串（如股东名称），该格式采用 **Block-Array** 结构。

### 3.1 总体结构
文件由多个 **时间块 (Time Block)** 组成。每个块包含该时间点的所有股东记录。

```text
[Block 1 Header]
[Record 1]
[Record 2]
...
[Record N]
----------------
[Block 2 Header]
[Record 1]
...
```

### 3.2 块头定义 (Block Header)
虽然是变长，但可以通过扫描时间戳特征来定位块头。

| 相对偏移 | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **ReportDate** | 报告期 |
| `0x08` | Int64 | **AnnounceDate**| 公告日期 |
| `0x10` | Int32/64 | **Count** | (推测) 该块包含的股东记录数，或直接接第一条记录。 |

### 3.3 股东记录定义 (Shareholder Record)
每条记录紧跟在头部或其他记录之后。记录长度固定（包含定长的字符串缓冲区），但不同版本的 QMT 可能调整缓冲区大小。**当前观测版本**结构如下：

*   **估算长度**: 约 260 ~ 300 Bytes (取决于字符串预留空间)

| 数据域 | 类型 | 内容 | 说明 |
| :--- | :--- | :--- | :--- |
| **Info** | String | **股东名称** | UTF-8 编码，定长 Buffer (如 128 bytes)，以 `\0` 结尾。 |
| **Type** | String | **股东性质** | UTF-8, e.g., "自然人", "国有法人"。 |
| **Shares** | Double | **持股数量** | |
| **Ratio** | Double | **持股比例** | e.g., 1.25 表示 1.25%。 |
| **Change** | String | **变动状态** | UTF-8, e.g., "不变", "新进"。 |
| **Class** | String | **股份类型** | UTF-8, e.g., "流通A股"。 |
| **Rank** | Int32 | **排名** | 第几大股东。 |

**解析策略建议**:
由于字符串字段可能有 Padding，建议读取时采用 **边界扫描法**：
1.  定位 Block Header（根据时间戳特征）。
2.  顺序读取字段，读取字符串时读取直到遇到 `0x00`，并跳过剩余 Padding 字节直到下一个字段的起始位置（通常对齐到 4 或 8 字节）。
3.  或直接将整个 Block 读入内存，利用 UTF-8 字符串的特征进行正则提取。

---

## 4. 解析算法参考 (Pseudocode)

### 4.1 定长文件读取器
```rust
struct FixedReader {
    stride: usize, // 656, 344, 56, etc.
}

impl FixedReader {
    fn read_file(path) {
        let bytes = load_file(path);
        let count = bytes.len() / self.stride;
        
        for i in 0..count {
            let offset = i * self.stride;
            let report_date = bytes.read_i64(offset);
            let anno_date = bytes.read_i64(offset + 8);
            
            // 根据 stride 类型解析剩余 Body
            let body = parse_body(bytes[offset+16 .. offset+stride]);
        }
    }
}
```

### 4.2 智能类型判断
在未知 Type ID 的情况下，可通过以下逻辑自动探测：

1.  读取文件前 16 字节，解析为两个 Int64。
2.  若数值在 `631152000000` (1990年) 到 `2524608000000` (2050年) 之间，则确认为 Header。
3.  寻找下一个 Header 的位置 `Pos2`。
4.  `Stride = Pos2 - 0`。
    *   若 Stride == 656 -> 报表 (7001-7003)
    *   若 Stride == 344 -> 比率 (7008)
    *   若 Stride == 56 -> 股本 (7004)
    *   若 Stride == 64 -> 股东数 (7005)
    *   若 Stride 不固定 -> 十大股东 (7006/7007)

---

## 5. 附录

### 5.1 字符串编码
*   **7006/7007**: 明确使用 **UTF-8**。
*   **7004**: 某些版本可能使用 **UTF-16LE** 或 ASCII。

### 5.2 字段映射 (Schema Mapping)
由于 DAT 文件只存储数值 (Value) 不存储列名 (Key)，由于列名固定，开发者需在代码中维护映射表。
*   *Mapping Source*: 可参考 QMT 目录下的 `jshistory/table_{id}.xml` 或同花顺 F10 网页结构。

### 5.3 注意事项
1.  **文件锁**: QMT 运行时会锁定部分文件，建议以 `ReadOnly` 模式打开或复制到临时目录读取。
2.  **数据更新**: 文件为追加写入模式 (Append-only)，最新数据通常在文件末尾，但也可能存在这就数据修正（Insert），建议读取全部并按 ReportDate 排序去重。