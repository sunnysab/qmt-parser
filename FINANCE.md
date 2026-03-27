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
| **7001** | 资产负债表 (Balance Sheet) | 定长 (Fixed) | 1264 Bytes |
| **7002** | 利润表 (Income Statement) | 定长 (Fixed) | 664 Bytes |
| **7003** | 现金流量表 (Cash Flow) | 定长 (Fixed) | 920 Bytes |
| **7004** | 股本结构 (Capital Structure) | 定长 (Fixed) | 56 Bytes |
| **7005** | 股东户数 (Shareholder Num) | 定长 (Fixed) | 64 Bytes |
| **7006** | 十大流通股东 (Top Float Holder) | 定长 (Fixed) | 416 Bytes |
| **7007** | 十大股东 (Top Holders) | 定长 (Fixed) | 416 Bytes |
| **7008** | 财务比率/每股指标 (Ratios) | 定长 (Fixed) | 344 Bytes |

---

## 2. 定长格式规范 (Fixed-Length Format)

适用于 Type ID: **7001, 7002, 7003, 7004, 7005, 7008**。
文件由连续的定长记录块（Record Block）组成，无文件头，直接存储数据。

### 2.1 财务报表 (7001, 7002, 7003)
三大报表并不共享统一步长。当前样本可确认它们都采用定长记录，但具体布局按类型不同。

#### 7001: 资产负债表

*   **记录长度 (Stride)**: 1264 Bytes

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **ReportDate** | 报告期 |
| `0x08` | Int64 | **AnnounceDate**| 公告日期 |
| `0x10` ... `0x4EF` | Double[156] | **Columns** | 财务数据列数组，共 156 个 Double。 |

#### 7002: 利润表

*   **记录长度 (Stride)**: 664 Bytes

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **LeadDate** | 额外前导日期槽，当前仍视为原始元数据 |
| `0x08` | Int64 | **ReportDate** | 报告期 |
| `0x10` | Int64 | **AnnounceDate**| 公告日期 |
| `0x18` ... `0x297` | Double[80] | **Columns** | 财务数据列数组，共 80 个 Double。 |

#### 7003: 现金流量表

*   **记录长度 (Stride)**: 920 Bytes

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **LeadDate** | 额外前导日期槽，当前仍视为原始元数据 |
| `0x08` | Int64 | **ReportDate** | 报告期 |
| `0x10` | Int64 | **AnnounceDate**| 公告日期 |
| `0x18` ... `0x397` | Double[111] | **Columns** | 财务数据列数组，共 111 个 Double。 |

*   **注意**: 以上报表仍是“数值列数组”容器。空值通常以 `0x7FEFFFFFFFFFFFFF` 写入，应在上层视作缺失值。

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
存储总股本、流通股本、限售股和自由流通股。

*   **记录长度 (Stride)**: 56 Bytes
*   **结构定义**:

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **ReportDate** | 变动日期/报告期 |
| `0x08` | Int64 | **AnnounceDate**| 公告日期 |
| `0x10` | Double | **TotalShare** | 总股本 |
| `0x18` | Double | **FlowShare** | 流通股本 |
| `0x20` | Double | **Restricted** | 限售股本 |
| `0x28` | Double | **FreeFloatShare** | 自由流通股份 |
| `0x30` ... `0x37` | Byte[8] | **Padding** | 保留字段 |

### 2.4 股东户数 (7005)

*   **记录长度 (Stride)**: 64 Bytes
*   **结构定义**:

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **AnnounceDate**| **注意**: 7005 的时间戳顺序可能反转，需按数值大小判断。 |
| `0x08` | Int64 | **ReportDate** | 报告期 (截止日) |
| `0x10` | Double | **TotalHolders**| 股东总户数 |
| `0x18` | Double | **AHolders** | A股股东户数 |
| `0x20` | Double | **BHolders** | B股股东户数 |
| `0x28` | Double | **HHolders** | H股股东户数 |
| `0x30` | Double | **FloatHolders** | 已流通股东户数 |
| `0x38` | Double | **OtherHolders** | 未流通股东户数 |

---

## 3. 十大股东定长格式 (7006 / 7007)

适用于 Type ID: **7006 (十大流通股东)**、**7007 (十大股东)**。

这两类文件并非变长块，而是 **416 字节定长记录**。每条记录都自带一组时间戳；同一报告期的多条连续记录应在上层按 `(ReportDate, AnnounceDate)` 聚合。

### 3.1 单条记录结构

| 偏移 (Offset) | 类型 | 字段名 | 说明 |
| :--- | :--- | :--- | :--- |
| `0x00` | Int64 | **AnnounceDate** | 公告日期 |
| `0x08` | Int64 | **ReportDate** | 截止日期 / 报告期 |
| `0x10` ... `0xCF` | Char[192] | **Name** | 股东名称，UTF-8，`\0` 截断 |
| `0xD8` ... `0x10F` | Char[56] | **HolderType** | 股东类型，如“机构投资账户” |
| `0x110` | Double | **HoldAmount** | 持股数量 |
| `0x118` ... `0x127` | Char[16] | **ChangeReason** | 变动原因，如“不变” |
| `0x130` | Double | **HoldRatio** | 持股比例 |
| `0x138` ... `0x197` | Char[96] | **ShareType** | 股份性质，如“流通A股”/“流通受限股份” |
| `0x19C` | UInt32 | **Rank** | 持股排名 |

### 3.2 聚合方式

解析器应顺序读取 416 字节定长记录，并将连续的同日期记录聚合成一个逻辑报告期：

```text
[AnnounceDate, ReportDate, Holder #1]
[AnnounceDate, ReportDate, Holder #2]
...
[NextAnnounceDate, NextReportDate, Holder #1]
```

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
    *   若 Stride == 1264 -> 资产负债表 (7001)
    *   若 Stride == 664 -> 利润表 (7002)
    *   若 Stride == 920 -> 现金流量表 (7003)
    *   若 Stride == 344 -> 比率 (7008)
    *   若 Stride == 56 -> 股本 (7004)
    *   若 Stride == 64 -> 股东数 (7005)
    *   若 Stride == 416 -> 十大股东 / 十大流通股东 (7006/7007)

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
