use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::MetadataParseError;

fn parse_holiday_token(token: &str) -> Option<i64> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    if token.len() == 8 {
        return token.parse::<i64>().ok();
    }
    token
        .parse::<i64>()
        .ok()
        .map(|value| value.div_euclid(86_400_000))
}

fn detect_holiday_roots() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(path) = std::env::var("XTQUANT_HOLIDAY_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_APPDATA_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_DATA_DIR") {
        out.push(PathBuf::from(path));
    }
    out
}

fn detect_weight_roots() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(path) = std::env::var("XTQUANT_WEIGHT_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_APPDATA_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_DATA_DIR") {
        out.push(PathBuf::from(path));
    }
    out
}

fn detect_industry_roots() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(path) = std::env::var("XTQUANT_INDUSTRY_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_APPDATA_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_DATA_DIR") {
        out.push(PathBuf::from(path));
    }
    out
}

fn detect_sector_roots() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(path) = std::env::var("XTQUANT_SECTOR_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_APPDATA_DIR") {
        out.push(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("XTQUANT_DATA_DIR") {
        out.push(PathBuf::from(path));
    }
    out
}

fn resolve_holiday_csv() -> Result<PathBuf, MetadataParseError> {
    for root in detect_holiday_roots() {
        if !root.exists() {
            continue;
        }
        for path in [root.join("holiday.csv"), root.join("holiday").join("holiday.csv")] {
            if path.exists() {
                return Ok(path);
            }
        }
    }
    Err(MetadataParseError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "holiday.csv not found",
    )))
}

fn resolve_holiday_dat() -> Result<PathBuf, MetadataParseError> {
    for root in detect_holiday_roots() {
        if !root.exists() {
            continue;
        }
        for path in [root.join("holiday.dat"), root.join("holiday").join("holiday.dat")] {
            if path.exists() {
                return Ok(path);
            }
        }
    }
    Err(MetadataParseError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "holiday.dat not found",
    )))
}

fn resolve_sector_name_files() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in detect_weight_roots() {
        if !root.exists() {
            continue;
        }
        for path in [
            root.join("systemSectorWeightData.txt"),
            root.join("customSectorWeightData.txt"),
            root.join("Weight").join("systemSectorWeightData.txt"),
            root.join("Weight").join("customSectorWeightData.txt"),
        ] {
            if path.exists() {
                out.push(path);
            }
        }
    }
    out
}

fn resolve_sector_weight_file() -> Result<PathBuf, MetadataParseError> {
    for root in detect_weight_roots() {
        if !root.exists() {
            continue;
        }
        for path in [
            root.join("sectorWeightData.txt"),
            root.join("Weight").join("sectorWeightData.txt"),
        ] {
            if path.exists() {
                return Ok(path);
            }
        }
    }
    Err(MetadataParseError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "sectorWeightData.txt not found",
    )))
}

fn resolve_sectorlist_dat() -> Result<PathBuf, MetadataParseError> {
    for root in detect_sector_roots() {
        if !root.exists() {
            continue;
        }
        for path in [
            root.join("sectorlist.DAT"),
            root.join("Sector").join("sectorlist.DAT"),
            root.join("Sector").join("Temple").join("sectorlist.DAT"),
        ] {
            if path.exists() {
                return Ok(path);
            }
        }
    }
    Err(MetadataParseError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "sectorlist.DAT not found",
    )))
}

fn resolve_industry_file() -> Result<PathBuf, MetadataParseError> {
    for root in detect_industry_roots() {
        if !root.exists() {
            continue;
        }
        for path in [
            root.join("IndustryData.txt"),
            root.join("Industry").join("IndustryData.txt"),
        ] {
            if path.exists() {
                return Ok(path);
            }
        }
    }
    Err(MetadataParseError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "IndustryData.txt not found",
    )))
}

/// 解析 xtquant `holiday.csv` / `holiday.dat`，返回 `YYYYMMDD` 数字列表。
pub fn parse_holiday_file(path: impl AsRef<Path>) -> Result<Vec<i64>, MetadataParseError> {
    let text = fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(first) = line.split(',').next() else {
            continue;
        };
        let Some(day) = parse_holiday_token(first) else {
            continue;
        };
        out.push(day);
    }
    if out.is_empty() {
        return Err(MetadataParseError::NoRecords("holiday"));
    }
    Ok(out)
}

/// 从 xtquant 约定路径自动发现并解析节假日文件。
pub fn load_holidays_from_standard_paths() -> Result<Vec<i64>, MetadataParseError> {
    resolve_holiday_csv()
        .and_then(parse_holiday_file)
        .or_else(|_| resolve_holiday_dat().and_then(parse_holiday_file))
}

/// 解析 xtquant split sector 文件，返回 sector 名称列表。
pub fn parse_sector_name_file(path: impl AsRef<Path>) -> Result<Vec<String>, MetadataParseError> {
    let text = fs::read_to_string(path)?;
    let mut out = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split(';').next().map(str::trim))
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    if out.is_empty() {
        return Err(MetadataParseError::NoRecords("sector names"));
    }
    Ok(out)
}

/// 从 xtquant 约定路径自动发现并解析 split sector 文件。
pub fn load_sector_names_from_standard_paths() -> Result<Vec<String>, MetadataParseError> {
    let paths = resolve_sector_name_files();
    if paths.is_empty() {
        return load_sectorlist_from_standard_paths();
    }
    let mut out = Vec::new();
    for path in paths {
        out.extend(parse_sector_name_file(path)?);
    }
    out.sort();
    out.dedup();
    if out.is_empty() {
        return Err(MetadataParseError::NoRecords("sector names"));
    }
    Ok(out)
}

/// 解析 xtquant `sectorlist.DAT`，返回板块名称列表。
pub fn parse_sectorlist_dat(path: impl AsRef<Path>) -> Result<Vec<String>, MetadataParseError> {
    let bytes = fs::read(path)?;
    let text = String::from_utf8_lossy(&bytes);
    let mut out = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    if out.is_empty() {
        return Err(MetadataParseError::NoRecords("sectorlist"));
    }
    Ok(out)
}

/// 从 xtquant 约定路径自动发现并解析 `sectorlist.DAT`。
pub fn load_sectorlist_from_standard_paths() -> Result<Vec<String>, MetadataParseError> {
    parse_sectorlist_dat(resolve_sectorlist_dat()?)
}

/// 解析 xtquant `sectorWeightData.txt`，返回 `sector -> members`。
pub fn parse_sector_weight_members(
    path: impl AsRef<Path>,
) -> Result<BTreeMap<String, Vec<String>>, MetadataParseError> {
    let text = fs::read_to_string(path)?;
    let mut out = BTreeMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts = line
            .split(';')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if parts.len() < 3 {
            continue;
        }
        let sector = parts[0].to_ascii_uppercase();
        let mut stocks = Vec::new();
        for chunk in parts[1..].chunks(2) {
            let [stock_code, _weight] = chunk else {
                break;
            };
            stocks.push(stock_code.to_ascii_uppercase());
        }
        stocks.sort();
        stocks.dedup();
        out.insert(sector, stocks);
    }

    if out.is_empty() {
        return Err(MetadataParseError::NoRecords("sector weight members"));
    }
    Ok(out)
}

/// 从 xtquant 约定路径自动发现并解析 `sectorWeightData.txt` 成员映射。
pub fn load_sector_weight_members_from_standard_paths(
) -> Result<BTreeMap<String, Vec<String>>, MetadataParseError> {
    parse_sector_weight_members(resolve_sector_weight_file()?)
}

/// 解析 xtquant `sectorWeightData.txt`，返回指定 index/sector 的 `stock -> weight`。
pub fn parse_sector_weight_index(
    path: impl AsRef<Path>,
    index_code: &str,
) -> Result<BTreeMap<String, f64>, MetadataParseError> {
    let text = fs::read_to_string(path)?;
    let mut out = BTreeMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts = line
            .split(';')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if parts.len() < 3 || !parts[0].eq_ignore_ascii_case(index_code) {
            continue;
        }
        for chunk in parts[1..].chunks(2) {
            let [stock_code, weight] = chunk else {
                break;
            };
            let Ok(weight) = weight.parse::<f64>() else {
                continue;
            };
            out.insert(stock_code.to_ascii_uppercase(), weight);
        }
    }

    if out.is_empty() {
        return Err(MetadataParseError::NoRecords("sector weight index"));
    }
    Ok(out)
}

/// 从 xtquant 约定路径自动发现并解析 `sectorWeightData.txt` 指定 index/sector 权重映射。
pub fn load_sector_weight_index_from_standard_paths(
    index_code: &str,
) -> Result<BTreeMap<String, f64>, MetadataParseError> {
    parse_sector_weight_index(resolve_sector_weight_file()?, index_code)
}

/// 解析 xtquant `IndustryData.txt`，返回 `industry -> members`。
pub fn parse_industry_file(
    path: impl AsRef<Path>,
) -> Result<BTreeMap<String, Vec<String>>, MetadataParseError> {
    let text = fs::read_to_string(path)?;
    let mut out = BTreeMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts = line
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if parts.len() < 2 {
            continue;
        }
        let industry = parts[0].to_string();
        let mut stocks = parts[1..]
            .iter()
            .map(|s| s.to_ascii_uppercase())
            .collect::<Vec<_>>();
        stocks.sort();
        stocks.dedup();
        out.insert(industry, stocks);
    }

    if out.is_empty() {
        return Err(MetadataParseError::NoRecords("industry"));
    }
    Ok(out)
}

/// 从 xtquant 约定路径自动发现并解析 `IndustryData.txt`。
pub fn load_industry_from_standard_paths(
) -> Result<BTreeMap<String, Vec<String>>, MetadataParseError> {
    parse_industry_file(resolve_industry_file()?)
}
