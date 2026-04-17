use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

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
