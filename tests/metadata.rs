use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use qmt_parser::{
    parse_holiday_file, parse_industry_file, parse_sector_name_file, parse_sector_weight_index,
    parse_sector_weight_members, parse_sectorlist_dat,
};

fn temp_dir(label: &str) -> PathBuf {
    let unique = format!(
        "qmt-parser-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    fs::create_dir_all(&root).expect("create temp dir");
    root
}

#[test]
fn parses_holiday_csv_and_dat() {
    let root = temp_dir("holiday");
    let csv = root.join("holiday.csv");
    let dat = root.join("holiday.dat");
    fs::write(&csv, "20240101,NewYear\n20240102,Holiday\n").expect("write csv");
    fs::write(&dat, "20240103\n20240105,Holiday\n").expect("write dat");

    assert_eq!(
        parse_holiday_file(&csv).expect("parse csv"),
        vec![20240101, 20240102]
    );
    assert_eq!(
        parse_holiday_file(&dat).expect("parse dat"),
        vec![20240103, 20240105]
    );
}

#[test]
fn parses_sector_name_file() {
    let root = temp_dir("sector-names");
    let file = root.join("systemSectorWeightData.txt");
    fs::write(
        &file,
        "沪深A股;600000.SH;1;\n上证期权;10000001.SHO;1;\n",
    )
    .expect("write sector names");

    assert_eq!(
        parse_sector_name_file(&file).expect("parse sector names"),
        vec!["上证期权".to_string(), "沪深A股".to_string()]
    );
}

#[test]
fn parses_sectorlist_dat_lines() {
    let root = temp_dir("sectorlist");
    let file = root.join("sectorlist.DAT");
    fs::write(&file, "沪深A股\n上证期权\n我的自选\n").expect("write sectorlist");

    assert_eq!(
        parse_sectorlist_dat(&file).expect("parse sectorlist"),
        vec![
            "上证期权".to_string(),
            "我的自选".to_string(),
            "沪深A股".to_string(),
        ]
    );
}

#[test]
fn parses_sector_weight_members_file() {
    let root = temp_dir("sector-weight");
    let file = root.join("sectorWeightData.txt");
    fs::write(
        &file,
        "000300.SH;600000.SH;0.42;000001.SZ;0.58;\n000905.SH;600000.SH;0.11;\n",
    )
    .expect("write sector weight");

    assert_eq!(
        parse_sector_weight_members(&file).expect("parse sector weight"),
        BTreeMap::from([
            (
                "000300.SH".to_string(),
                vec!["000001.SZ".to_string(), "600000.SH".to_string()]
            ),
            ("000905.SH".to_string(), vec!["600000.SH".to_string()]),
        ])
    );
}

#[test]
fn parses_sector_weight_index_file() {
    let root = temp_dir("sector-weight-index");
    let file = root.join("sectorWeightData.txt");
    fs::write(
        &file,
        "000300.SH;600000.SH;0.42;000001.SZ;0.58;\n000905.SH;600000.SH;0.11;\n",
    )
    .expect("write sector weight");

    assert_eq!(
        parse_sector_weight_index(&file, "000300.SH").expect("parse sector weight index"),
        BTreeMap::from([
            ("000001.SZ".to_string(), 0.58),
            ("600000.SH".to_string(), 0.42),
        ])
    );
}

#[test]
fn parses_industry_file() {
    let root = temp_dir("industry");
    let file = root.join("IndustryData.txt");
    fs::write(
        &file,
        "银行,600000.SH,601398.SH\n券商,600030.SH\n",
    )
    .expect("write industry");

    assert_eq!(
        parse_industry_file(&file).expect("parse industry"),
        BTreeMap::from([
            (
                "券商".to_string(),
                vec!["600030.SH".to_string()]
            ),
            (
                "银行".to_string(),
                vec!["600000.SH".to_string(), "601398.SH".to_string()]
            ),
        ])
    );
}
