use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{ByteOrder, LittleEndian};
use chrono::NaiveDate;
use qmt_parser::{DataDirError, FileType, Market, QmtDataDir, parse_security_code};
use rusty_leveldb::{DB, Options};

fn temp_dir(label: &str) -> PathBuf {
    let unique = format!(
        "qmt-parser-datadir-{label}-{}-{}",
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

fn copy_fixture(from: impl AsRef<Path>, to: impl AsRef<Path>) {
    let to = to.as_ref();
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::copy(from, to).expect("copy fixture");
}

fn write_text(path: impl AsRef<Path>, content: &str) {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, content).expect("write text fixture");
}

fn write_sectorlist(path: impl AsRef<Path>) {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, "沪深A股\n上证期权\n").expect("write sectorlist");
}

fn write_dividend_db(path: impl AsRef<Path>) {
    let path = path.as_ref();
    fs::create_dir_all(path).expect("create db dir");

    let options = Options {
        create_if_missing: true,
        ..Options::default()
    };
    let mut db = DB::open(path, options).expect("open leveldb");

    let key = b"SH|600000|cash|1711929600000";
    let mut value = vec![0u8; 96];
    LittleEndian::write_i64(&mut value[8..16], 1_711_929_600_000);
    LittleEndian::write_f64(&mut value[16..24], 0.12);
    LittleEndian::write_f64(&mut value[24..32], 0.01);
    LittleEndian::write_f64(&mut value[32..40], 0.02);
    LittleEndian::write_f64(&mut value[40..48], 0.0);
    LittleEndian::write_f64(&mut value[48..56], 0.0);
    LittleEndian::write_f64(&mut value[56..64], 0.0);
    LittleEndian::write_f64(&mut value[64..72], 0.0);
    LittleEndian::write_f64(&mut value[72..80], 1.234);
    LittleEndian::write_u32(&mut value[80..84], 20240329);
    LittleEndian::write_u32(&mut value[88..92], 20240401);
    db.put(key, &value).expect("put dividend");
    db.flush().expect("flush dividend");
}

fn make_datadir_fixture() -> PathBuf {
    let root = temp_dir("fixture");

    copy_fixture(
        "data/000001-20250529-tick.dat",
        root.join("SZ/0/000001/20250529.dat"),
    );
    copy_fixture("data/000000-1m.dat", root.join("SZ/60/000000.dat"));
    copy_fixture("data/000001-1d.dat", root.join("SZ/86400/000001.DAT"));
    copy_fixture(
        "finance/002419_7001.DAT",
        root.join("financial/002419_7001.DAT"),
    );

    write_text(
        root.join("holiday.csv"),
        "20240101,NewYear\n20240102,Holiday\n",
    );
    write_text(
        root.join("Weight/systemSectorWeightData.txt"),
        "沪深A股;600000.SH;1;\n上证期权;10000001.SHO;1;\n",
    );
    write_text(
        root.join("Weight/customSectorWeightData.txt"),
        "我的自选;000001.SZ;1;\n",
    );
    write_text(
        root.join("Weight/sectorWeightData.txt"),
        "000300.SH;600000.SH;0.42;000001.SZ;0.58;\n000905.SH;600000.SH;0.11;\n",
    );
    write_text(
        root.join("Industry/IndustryData.txt"),
        "银行,600000.SH,601398.SH\n券商,600030.SH\n",
    );
    write_sectorlist(root.join("Sector/Temple/sectorlist.DAT"));

    write_dividend_db(root.join("DividData"));
    root
}

#[test]
fn rejects_missing_datadir_root() {
    let missing = std::env::temp_dir().join("qmt-parser-datadir-missing-root");
    let err = QmtDataDir::new(&missing).expect_err("missing root should error");
    assert!(matches!(err, DataDirError::InvalidRoot(_)));
}

#[test]
fn discovers_expected_paths() {
    let root = make_datadir_fixture();
    let qmt = QmtDataDir::new(&root).expect("build datadir");

    assert_eq!(
        qmt.tick_path(Market::Sz, "000001", "20250529")
            .expect("tick path"),
        root.join("SZ/0/000001/20250529.dat")
    );
    assert_eq!(
        qmt.min_path(Market::Sz, "000000").expect("min path"),
        root.join("SZ/60/000000.dat")
    );
    assert_eq!(
        qmt.day_path(Market::Sz, "000001").expect("day path"),
        root.join("SZ/86400/000001.DAT")
    );
    assert_eq!(
        qmt.finance_path("002419", FileType::BalanceSheet)
            .expect("finance path"),
        root.join("financial/002419_7001.DAT")
    );
    assert_eq!(
        qmt.dividend_db_path().expect("dividend db path"),
        root.join("DividData")
    );
}

#[test]
fn parses_tick_min_day_and_finance_from_datadir() {
    let root = make_datadir_fixture();
    let qmt = QmtDataDir::new(&root).expect("build datadir");

    let ticks = qmt
        .parse_ticks_to_structs(Market::Sz, "000001", "20250529")
        .expect("parse ticks");
    assert!(!ticks.is_empty());
    assert_eq!(ticks[0].market.as_deref(), Some("SZ"));
    assert_eq!(ticks[0].symbol, "000001");

    let mins = qmt
        .parse_min_to_structs(Market::Sz, "000000")
        .expect("parse min");
    assert!(!mins.is_empty());

    let daily = qmt
        .parse_daily_file_to_structs(Market::Sz, "000001")
        .expect("parse daily file");
    assert!(!daily.is_empty());

    let range = qmt
        .parse_daily_to_structs(Market::Sz, "000001", "20240101", "20251231")
        .expect("parse daily range");
    assert!(!range.is_empty());
    assert!(range.len() <= daily.len());

    let typed_range = qmt
        .parse_daily_to_structs_in_range(
            Market::Sz,
            "000001",
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).expect("start")),
            Some(NaiveDate::from_ymd_opt(2025, 12, 31).expect("end")),
        )
        .expect("parse daily typed range");
    assert_eq!(typed_range.len(), range.len());

    let finance = qmt
        .read_finance("002419", FileType::BalanceSheet)
        .expect("read finance");
    assert!(!finance.is_empty());
    assert_eq!(finance[0].file_type, FileType::BalanceSheet);
}

#[cfg(feature = "polars")]
#[test]
fn parses_dataframes_from_datadir() {
    let root = make_datadir_fixture();
    let qmt = QmtDataDir::new(&root).expect("build datadir");

    let tick_df = qmt
        .parse_ticks_to_dataframe(Market::Sz, "000001", "20250529")
        .expect("tick df");
    assert!(tick_df.height() > 0);

    let min_df = qmt
        .parse_min_to_dataframe(Market::Sz, "000000")
        .expect("min df");
    assert!(min_df.height() > 0);

    let daily_df = qmt
        .parse_daily_file_to_dataframe(Market::Sz, "000001")
        .expect("daily df");
    assert!(daily_df.height() > 0);

    let daily_range_df = qmt
        .parse_daily_to_dataframe(Market::Sz, "000001", "20240101", "20251231")
        .expect("daily range df");
    assert!(daily_range_df.height() > 0);
    assert!(daily_range_df.height() <= daily_df.height());

    let daily_typed_df = qmt
        .parse_daily_to_dataframe_in_range(
            Market::Sz,
            "000001",
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).expect("start")),
            Some(NaiveDate::from_ymd_opt(2025, 12, 31).expect("end")),
        )
        .expect("daily typed df");
    assert_eq!(daily_range_df.height(), daily_typed_df.height());
}

#[test]
fn loads_metadata_and_dividend_from_datadir() {
    let root = make_datadir_fixture();
    let qmt = QmtDataDir::new(&root).expect("build datadir");

    assert_eq!(
        qmt.load_holidays().expect("holidays"),
        vec![20240101, 20240102]
    );
    assert_eq!(
        qmt.load_sector_names().expect("sector names"),
        vec![
            "上证期权".to_string(),
            "我的自选".to_string(),
            "沪深A股".to_string(),
        ]
    );
    assert_eq!(
        qmt.load_sectorlist().expect("sectorlist"),
        vec!["上证期权".to_string(), "沪深A股".to_string()]
    );
    assert_eq!(
        qmt.load_sector_weight_members().expect("sector members"),
        BTreeMap::from([
            (
                "000300.SH".to_string(),
                vec!["000001.SZ".to_string(), "600000.SH".to_string()]
            ),
            ("000905.SH".to_string(), vec!["600000.SH".to_string()]),
        ])
    );
    assert_eq!(
        qmt.load_sector_weight_index("000300.SH")
            .expect("sector index"),
        BTreeMap::from([
            ("000001.SZ".to_string(), 0.58),
            ("600000.SH".to_string(), 0.42),
        ])
    );
    assert_eq!(
        qmt.load_industry().expect("industry"),
        BTreeMap::from([
            ("券商".to_string(), vec!["600030.SH".to_string()]),
            (
                "银行".to_string(),
                vec!["600000.SH".to_string(), "601398.SH".to_string()]
            ),
        ])
    );

    let _dividend = qmt.open_dividend_db().expect("open dividend db");
}

#[test]
fn returns_path_not_found_for_missing_symbol() {
    let root = make_datadir_fixture();
    let qmt = QmtDataDir::new(&root).expect("build datadir");

    let err = qmt
        .day_path(Market::Sz, "999999")
        .expect_err("missing day path");
    assert!(matches!(err, DataDirError::PathNotFound { .. }));
}

#[test]
fn parses_market_enum_and_security_code() {
    assert_eq!(Market::try_from("sz").expect("sz"), Market::Sz);
    assert_eq!(Market::try_from("SH").expect("sh"), Market::Sh);
    assert_eq!(Market::try_from("bj").expect("bj"), Market::Bj);

    assert_eq!(
        parse_security_code("SZ000001").expect("prefix format"),
        (Market::Sz, "000001".to_string())
    );
    assert_eq!(
        parse_security_code("000001.SZ").expect("suffix format"),
        (Market::Sz, "000001".to_string())
    );
    assert_eq!(
        parse_security_code("BJ430017").expect("bj prefix format"),
        (Market::Bj, "430017".to_string())
    );
    assert_eq!(
        parse_security_code("430017.BJ").expect("bj suffix format"),
        (Market::Bj, "430017".to_string())
    );

    let err = parse_security_code("000001").expect_err("missing market should fail");
    assert!(matches!(err, DataDirError::InvalidInput(_)));
}
