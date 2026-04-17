use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use qmt_parser::{
    MetadataParseError, load_holidays_from_root, load_industry_from_root,
    load_sector_names_from_root, load_sector_weight_index_from_root,
    load_sector_weight_members_from_root, load_sectorlist_from_root, parse_holiday_file,
    parse_industry_file, parse_sector_name_file, parse_sector_weight_index,
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

fn write_text(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, content).expect("write fixture");
}

fn metadata_root_fixture() -> PathBuf {
    let root = temp_dir("metadata-root");
    write_text(
        &root,
        "holiday/holiday.csv",
        "20240101,NewYear\n20240102,Holiday\n",
    );
    write_text(
        &root,
        "Weight/systemSectorWeightData.txt",
        "沪深A股;600000.SH;1;\n上证期权;10000001.SHO;1;\n",
    );
    write_text(
        &root,
        "Weight/customSectorWeightData.txt",
        "我的自选;000001.SZ;1;\n",
    );
    write_text(
        &root,
        "Weight/sectorWeightData.txt",
        "000300.SH;600000.SH;0.42;000001.SZ;0.58;\n000905.SH;600000.SH;0.11;\n",
    );
    write_text(
        &root,
        "Industry/IndustryData.txt",
        "银行,600000.SH,601398.SH\n券商,600030.SH\n",
    );
    write_text(
        &root,
        "Sector/Temple/sectorlist.DAT",
        "沪深A股\n上证期权\n我的自选\n",
    );
    root
}

fn has_any(root: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .map(|relative| PathBuf::from(root).join(relative))
        .any(|path| path.exists())
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
    fs::write(&file, "沪深A股;600000.SH;1;\n上证期权;10000001.SHO;1;\n")
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
fn parses_sector_weight_members_compact_entry_format() {
    let root = temp_dir("sector-weight-compact-members");
    let file = root.join("sectorWeightData.txt");
    fs::write(
        &file,
        "899050;920001.BJ,0.707000;920002.BJ,0.729000;\n000300.SH;600000.SH,0.42;000001.SZ,0.58;\n",
    )
    .expect("write compact sector weight");

    assert_eq!(
        parse_sector_weight_members(&file).expect("parse compact sector weight"),
        BTreeMap::from([
            (
                "000300.SH".to_string(),
                vec!["000001.SZ".to_string(), "600000.SH".to_string()]
            ),
            (
                "899050".to_string(),
                vec!["920001.BJ".to_string(), "920002.BJ".to_string()]
            ),
        ])
    );
}

#[test]
fn parses_sector_weight_index_compact_entry_format() {
    let root = temp_dir("sector-weight-compact-index");
    let file = root.join("sectorWeightData.txt");
    fs::write(
        &file,
        "899050;920001.BJ,0.707000;920002.BJ,0.729000;\n000300.SH;600000.SH,0.42;000001.SZ,0.58;\n",
    )
    .expect("write compact sector weight");

    assert_eq!(
        parse_sector_weight_index(&file, "899050").expect("parse compact sector index"),
        BTreeMap::from([
            ("920001.BJ".to_string(), 0.707),
            ("920002.BJ".to_string(), 0.729),
        ])
    );
}

#[test]
fn parses_industry_file() {
    let root = temp_dir("industry");
    let file = root.join("IndustryData.txt");
    fs::write(&file, "银行,600000.SH,601398.SH\n券商,600030.SH\n").expect("write industry");

    assert_eq!(
        parse_industry_file(&file).expect("parse industry"),
        BTreeMap::from([
            ("券商".to_string(), vec!["600030.SH".to_string()]),
            (
                "银行".to_string(),
                vec!["600000.SH".to_string(), "601398.SH".to_string()]
            ),
        ])
    );
}

#[test]
fn loads_metadata_from_root_fixture() {
    let root = metadata_root_fixture();

    assert_eq!(
        load_holidays_from_root(&root).expect("load holidays from root"),
        vec![20240101, 20240102]
    );
    assert_eq!(
        load_sector_names_from_root(&root).expect("load sector names from root"),
        vec![
            "上证期权".to_string(),
            "我的自选".to_string(),
            "沪深A股".to_string(),
        ]
    );
    assert_eq!(
        load_sectorlist_from_root(&root).expect("load sectorlist from root"),
        vec![
            "上证期权".to_string(),
            "我的自选".to_string(),
            "沪深A股".to_string(),
        ]
    );
    assert_eq!(
        load_sector_weight_members_from_root(&root).expect("load sector members from root"),
        BTreeMap::from([
            (
                "000300.SH".to_string(),
                vec!["000001.SZ".to_string(), "600000.SH".to_string()]
            ),
            ("000905.SH".to_string(), vec!["600000.SH".to_string()]),
        ])
    );
    assert_eq!(
        load_sector_weight_index_from_root(&root, "000300.SH")
            .expect("load sector index from root"),
        BTreeMap::from([
            ("000001.SZ".to_string(), 0.58),
            ("600000.SH".to_string(), 0.42),
        ])
    );
    assert_eq!(
        load_industry_from_root(&root).expect("load industry from root"),
        BTreeMap::from([
            ("券商".to_string(), vec!["600030.SH".to_string()]),
            (
                "银行".to_string(),
                vec!["600000.SH".to_string(), "601398.SH".to_string()]
            ),
        ])
    );
}

#[test]
fn load_sector_names_from_root_falls_back_to_sectorlist() {
    let root = temp_dir("metadata-sector-fallback");
    write_text(&root, "Sector/Temple/sectorlist.DAT", "沪深A股\n上证期权\n");

    assert_eq!(
        load_sector_names_from_root(&root).expect("fallback to sectorlist"),
        vec!["上证期权".to_string(), "沪深A股".to_string()]
    );
}

#[test]
fn reads_real_metadata_from_local_datadir_when_available() {
    let root = PathBuf::from("/mnt/data/trade/qmtdata/datadir");
    if !root.is_dir() {
        eprintln!(
            "skip real datadir metadata test: {} not found",
            root.display()
        );
        return;
    }

    let holiday_exists = has_any(
        "/mnt/data/trade/qmtdata/datadir",
        &[
            "holiday.csv",
            "holiday/holiday.csv",
            "holiday.dat",
            "holiday/holiday.dat",
        ],
    );
    let sectorlist_exists = has_any(
        "/mnt/data/trade/qmtdata/datadir",
        &[
            "sectorlist.DAT",
            "Sector/sectorlist.DAT",
            "Sector/Temple/sectorlist.DAT",
        ],
    );
    let sector_names_exists = has_any(
        "/mnt/data/trade/qmtdata/datadir",
        &[
            "systemSectorWeightData.txt",
            "customSectorWeightData.txt",
            "Weight/systemSectorWeightData.txt",
            "Weight/customSectorWeightData.txt",
        ],
    );
    let sector_weight_exists = has_any(
        "/mnt/data/trade/qmtdata/datadir",
        &["sectorWeightData.txt", "Weight/sectorWeightData.txt"],
    );
    let industry_exists = has_any(
        "/mnt/data/trade/qmtdata/datadir",
        &["IndustryData.txt", "Industry/IndustryData.txt"],
    );

    match load_holidays_from_root(&root) {
        Ok(days) => {
            assert!(
                holiday_exists,
                "holidays loaded but no holiday file was detected"
            );
            assert!(
                !days.is_empty(),
                "real holiday file should not parse to empty"
            );
        }
        Err(MetadataParseError::Io(err)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
            assert!(
                !holiday_exists,
                "holiday file exists but root loader returned not found"
            );
        }
        Err(err) => panic!("unexpected holiday root error: {err}"),
    }

    match load_sectorlist_from_root(&root) {
        Ok(names) => {
            assert!(
                sectorlist_exists,
                "sectorlist loaded but no sectorlist file was detected"
            );
            assert!(
                !names.is_empty(),
                "real sectorlist should not parse to empty"
            );
        }
        Err(MetadataParseError::Io(err)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
            assert!(
                !sectorlist_exists,
                "sectorlist file exists but root loader returned not found"
            );
        }
        Err(err) => panic!("unexpected sectorlist root error: {err}"),
    }

    let sector_names =
        load_sector_names_from_root(&root).expect("load real sector names from root");
    assert!(
        sector_names_exists || sectorlist_exists,
        "sector names loaded but neither sector name files nor sectorlist was detected"
    );
    assert!(
        !sector_names.is_empty(),
        "real sector names should not be empty"
    );

    let members =
        load_sector_weight_members_from_root(&root).expect("load real sector members from root");
    assert!(
        sector_weight_exists,
        "sectorWeightData.txt should exist for real members load"
    );
    assert!(
        !members.is_empty(),
        "real sector weight members should not be empty"
    );

    let sample_index_code = members
        .keys()
        .next()
        .expect("real sector members should contain at least one index")
        .clone();
    let index = load_sector_weight_index_from_root(&root, &sample_index_code)
        .expect("load real index from root");
    assert!(
        sector_weight_exists,
        "sectorWeightData.txt should exist for real index load"
    );
    assert!(
        !index.is_empty(),
        "real 000300.SH index should not be empty"
    );

    let industry = load_industry_from_root(&root).expect("load real industry from root");
    assert!(
        industry_exists,
        "IndustryData.txt should exist for real industry load"
    );
    assert!(
        !industry.is_empty(),
        "real industry map should not be empty"
    );
}
