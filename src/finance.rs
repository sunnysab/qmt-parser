//! QMT 本地财务 .DAT 解析（逆向工程版本，字段可能不完全）

use std::fs::File;
use std::io;
use std::path::Path;

use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, FixedOffset, TimeZone};
use memmap2::MmapOptions;
use thiserror::Error;

const STRIDE_BALANCE: usize = 1264;  // 7001
const STRIDE_INCOME: usize = 664;    // 7002
const STRIDE_CASHFLOW: usize = 920;  // 7003
const STRIDE_RATIOS: usize = 344;  // 7008
const STRIDE_CAPITAL: usize = 56;  // 7004
const STRIDE_HOLDER: usize = 64;   // 7005
const STRIDE_TOP_HOLDER: usize = 416; // 7006, 7007
const COLUMNS_BALANCE: usize = 156;
const COLUMNS_INCOME: usize = 80;
const COLUMNS_CASHFLOW: usize = 111;
const COLUMNS_RATIOS: usize = 41;

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
        free_float_share: f64,
    },
    /// 7005: 股东人数
    HolderCount {
        total_holders: i64,
        a_holders: i64,
        b_holders: i64,
        h_holders: i64,
        float_holders: i64,
        other_holders: i64,
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
    pub holder_type: String,
    pub hold_amount: f64,
    pub change_reason: String,
    pub hold_ratio: f64, // 比例 (如 0.05 代表 5%)
    pub share_type: String, // 股份性质 (e.g. "流通A股")
    pub rank: u32,
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

const BALANCE_COLUMN_NAMES: [&str; 156] = [
    "internal_shoule_recv",
    "fixed_capital_clearance",
    "should_pay_money",
    "settlement_payment",
    "receivable_premium",
    "accounts_receivable_reinsurance",
    "reinsurance_contract_reserve",
    "dividends_payable",
    "tax_rebate_for_export",
    "subsidies_receivable",
    "deposit_receivable",
    "apportioned_cost",
    "profit_and_current_assets_with_deal",
    "current_assets_one_year",
    "long_term_receivables",
    "other_long_term_investments",
    "original_value_of_fixed_assets",
    "net_value_of_fixed_assets",
    "depreciation_reserves_of_fixed_assets",
    "productive_biological_assets",
    "public_welfare_biological_assets",
    "oil_and_gas_assets",
    "development_expenditure",
    "right_of_split_share_distribution",
    "other_non_mobile_assets",
    "handling_fee_and_commission",
    "other_payables",
    "margin_payable",
    "internal_accounts_payable",
    "advance_cost",
    "insurance_contract_reserve",
    "broker_buying_and_selling_securities",
    "acting_underwriting_securities",
    "international_ticket_settlement",
    "domestic_ticket_settlement",
    "deferred_income",
    "short_term_bonds_payable",
    "long_term_deferred_income",
    "undetermined_investment_losses",
    "quasi_distribution_of_cash_dividends",
    "provisions_not",
    "cust_bank_dep",
    "provisions",
    "less_tsy_stk",
    "cash_equivalents",
    "loans_to_oth_banks",
    "tradable_fin_assets",
    "derivative_fin_assets",
    "bill_receivable",
    "account_receivable",
    "advance_payment",
    "int_rcv",
    "other_receivable",
    "red_monetary_cap_for_sale",
    "agency_bus_assets",
    "inventories",
    "other_current_assets",
    "total_current_assets",
    "loans_and_adv_granted",
    "fin_assets_avail_for_sale",
    "held_to_mty_invest",
    "long_term_eqy_invest",
    "invest_real_estate",
    "accumulated_depreciation",
    "fix_assets",
    "constru_in_process",
    "construction_materials",
    "long_term_liabilities",
    "intang_assets",
    "goodwill",
    "long_deferred_expense",
    "deferred_tax_assets",
    "total_non_current_assets",
    "tot_assets",
    "shortterm_loan",
    "borrow_central_bank",
    "loans_oth_banks",
    "tradable_fin_liab",
    "derivative_fin_liab",
    "notes_payable",
    "accounts_payable",
    "advance_peceipts",
    "fund_sales_fin_assets_rp",
    "empl_ben_payable",
    "taxes_surcharges_payable",
    "int_payable",
    "dividend_payable",
    "other_payable",
    "non_current_liability_in_one_year",
    "other_current_liability",
    "total_current_liability",
    "long_term_loans",
    "bonds_payable",
    "longterm_account_payable",
    "grants_received",
    "deferred_tax_liab",
    "other_non_current_liabilities",
    "non_current_liabilities",
    "tot_liab",
    "cap_stk",
    "cap_rsrv",
    "specific_reserves",
    "surplus_rsrv",
    "prov_nom_risks",
    "undistributed_profit",
    "cnvd_diff_foreign_curr_stat",
    "tot_shrhldr_eqy_excl_min_int",
    "minority_int",
    "total_equity",
    "tot_liab_shrhldr_eqy",
    "inventory_depreciation_reserve",
    "current_ratio",
    "m_cashAdepositsCentralBank",
    "m_nobleMetal",
    "m_depositsOtherFinancialInstitutions",
    "m_currentInvestment",
    "m_redemptoryMonetaryCapitalSale",
    "m_netAmountSubrogation",
    "m_refundableDeposits",
    "m_netAmountLoanPledged",
    "m_fixedTimeDeposit",
    "m_netLongtermDebtInvestments",
    "m_permanentInvestment",
    "m_depositForcapitalRecognizance",
    "m_netBalConstructionProgress",
    "m_separateAccountAssets",
    "m_capitalInvicariousBussiness",
    "m_otherAssets",
    "m_depositsWithBanksOtherFinancialIns",
    "m_indemnityPayable",
    "m_policyDividendPayable",
    "m_guaranteeInvestmentFunds",
    "m_premiumsReceivedAdvance",
    "m_insuranceLiabilities",
    "m_liabilitiesIndependentAccounts",
    "m_liabilitiesVicariousBusiness",
    "m_otherLiablities",
    "m_capitalPremium",
    "m_petainedProfit",
    "m_provisionTransactionRisk",
    "m_otherReserves",
    "__extra_141",
    "__extra_142",
    "__extra_143",
    "__extra_144",
    "__extra_145",
    "__extra_146",
    "__extra_147",
    "__extra_148",
    "__extra_149",
    "__extra_150",
    "__extra_151",
    "__extra_152",
    "__extra_153",
    "__extra_154",
    "__extra_155",
];

const INCOME_COLUMN_NAMES: [&str; 80] = [
    "revenue_inc",
    "earned_premium",
    "real_estate_sales_income",
    "total_operating_cost",
    "real_estate_sales_cost",
    "research_expenses",
    "surrender_value",
    "net_payments",
    "net_withdrawal_ins_con_res",
    "policy_dividend_expenses",
    "reinsurance_cost",
    "change_income_fair_value",
    "futures_loss",
    "trust_income",
    "subsidize_revenue",
    "other_business_profits",
    "net_profit_excl_merged_int_inc",
    "int_inc",
    "handling_chrg_comm_inc",
    "less_handling_chrg_comm_exp",
    "other_bus_cost",
    "plus_net_gain_fx_trans",
    "il_net_loss_disp_noncur_asset",
    "inc_tax",
    "unconfirmed_invest_loss",
    "net_profit_excl_min_int_inc",
    "less_int_exp",
    "other_bus_inc",
    "revenue",
    "total_expense",
    "less_taxes_surcharges_ops",
    "sale_expense",
    "less_gerl_admin_exp",
    "financial_expense",
    "less_impair_loss_assets",
    "plus_net_invest_inc",
    "incl_inc_invest_assoc_jv_entp",
    "oper_profit",
    "plus_non_oper_rev",
    "less_non_oper_exp",
    "tot_profit",
    "net_profit_incl_min_int_inc",
    "net_profit_incl_min_int_inc_after",
    "minority_int_inc",
    "s_fa_eps_basic",
    "s_fa_eps_diluted",
    "total_income",
    "total_income_minority",
    "other_compreh_inc",
    "operating_revenue",
    "cost_of_goods_sold",
    "m_netinterestIncome",
    "m_netFeesCommissions",
    "m_insuranceBusiness",
    "m_separatePremium",
    "m_asideReservesUndueLiabilities",
    "m_paymentsInsuranceClaims",
    "m_amortizedCompensationExpenses",
    "m_netReserveInsuranceLiability",
    "m_policyReserve",
    "m_amortizeInsuranceReserve",
    "m_nsuranceFeesCommissionExpenses",
    "m_operationAdministrativeExpense",
    "m_amortizedReinsuranceExpenditure",
    "m_netProfitLossdisposalNonassets",
    "m_otherItemsAffectingNetProfit",
    "__extra_66",
    "__extra_67",
    "__extra_68",
    "__extra_69",
    "__extra_70",
    "__extra_71",
    "__extra_72",
    "__extra_73",
    "__extra_74",
    "__extra_75",
    "__extra_76",
    "__extra_77",
    "__extra_78",
    "__extra_79",
];

const CASHFLOW_COLUMN_NAMES: [&str; 111] = [
    "cash_received_ori_ins_contract_pre",
    "net_cash_received_rei_ope",
    "net_increase_insured_funds",
    "net_increase_in_disposal",
    "cash_for_interest",
    "net_increase_in_repurchase_funds",
    "cash_for_payment_original_insurance",
    "cash_payment_policy_dividends",
    "disposal_other_business_units",
    "net_cash_deal_subcompany",
    "cash_received_from_pledges",
    "cash_paid_for_investments",
    "net_increase_in_pledged_loans",
    "cash_paid_by_subsidiaries",
    "increase_in_cash_paid",
    "fix_intan_other_asset_dispo_cash_payment",
    "cash_from_mino_s_invest_sub",
    "cass_received_sub_abs",
    "cass_received_sub_investments",
    "minority_shareholder_profit_loss",
    "unrecognized_investment_losses",
    "ncrease_deferred_income",
    "projected_liability",
    "increase_operational_payables",
    "reduction_outstanding_amounts_less",
    "reduction_outstanding_amounts_more",
    "goods_sale_and_service_render_cash",
    "net_incr_dep_cob",
    "net_incr_loans_central_bank",
    "net_incr_fund_borr_ofi",
    "net_incr_fund_borr_ofi",
    "tax_levy_refund",
    "cash_paid_invest",
    "other_cash_pay_ral_inv_act",
    "other_cash_recp_ral_oper_act",
    "stot_cash_inflows_oper_act",
    "goods_and_services_cash_paid",
    "net_incr_clients_loan_adv",
    "net_incr_dep_cbob",
    "handling_chrg_paid",
    "cash_pay_beh_empl",
    "pay_all_typ_tax",
    "other_cash_pay_ral_oper_act",
    "stot_cash_outflows_oper_act",
    "net_cash_flows_oper_act",
    "cash_recp_disp_withdrwl_invest",
    "cash_recp_return_invest",
    "net_cash_recp_disp_fiolta",
    "other_cash_recp_ral_inv_act",
    "stot_cash_inflows_inv_act",
    "cash_pay_acq_const_fiolta",
    "stot_cash_outflows_inv_act",
    "net_cash_flows_inv_act",
    "cash_recp_cap_contrib",
    "cash_recp_borrow",
    "proc_issue_bonds",
    "other_cash_recp_ral_fnc_act",
    "stot_cash_inflows_fnc_act",
    "cash_prepay_amt_borr",
    "cash_pay_dist_dpcp_int_exp",
    "other_cash_pay_ral_fnc_act",
    "stot_cash_outflows_fnc_act",
    "net_cash_flows_fnc_act",
    "eff_fx_flu_cash",
    "net_incr_cash_cash_equ",
    "cash_cash_equ_beg_period",
    "cash_cash_equ_end_period",
    "net_profit",
    "plus_prov_depr_assets",
    "depr_fa_coga_dpba",
    "amort_intang_assets",
    "amort_lt_deferred_exp",
    "decr_deferred_exp",
    "incr_acc_exp",
    "loss_disp_fiolta",
    "loss_scr_fa",
    "loss_fv_chg",
    "fin_exp",
    "invest_loss",
    "decr_deferred_inc_tax_assets",
    "incr_deferred_inc_tax_liab",
    "decr_inventories",
    "decr_oper_payable",
    "others",
    "im_net_cash_flows_oper_act",
    "conv_debt_into_cap",
    "conv_corp_bonds_due_within_1y",
    "fa_fnc_leases",
    "end_bal_cash",
    "less_beg_bal_cash",
    "plus_end_bal_cash_equ",
    "less_beg_bal_cash_equ",
    "im_net_incr_cash_cash_equ",
    "m_netDecreaseUnwindingFunds",
    "m_netReductionPurchaseRebates",
    "m_netIncreaseDepositsBanks",
    "m_netCashReinsuranceBusiness",
    "m_netReductionDeposInveFunds",
    "m_netIncreaseUnwindingFunds",
    "m_netReductionAmountBorrowedFunds",
    "m_netReductionSaleRepurchaseProceeds",
    "m_paymentOtherCashRelated",
    "m_cashOutFlowsInvesactivities",
    "m_absorbCashEquityInv",
    "m_otherImpactsOnCash",
    "m_addOperatingReceivableItems",
    "__extra_106",
    "__extra_107",
    "__extra_108",
    "__extra_109",
    "__extra_110",
];

const RATIO_COLUMN_NAMES: [&str; 41] = [
    "s_fa_ocfps",
    "s_fa_bps",
    "s_fa_eps_basic",
    "s_fa_eps_diluted",
    "s_fa_undistributedps",
    "s_fa_surpluscapitalps",
    "adjusted_earnings_per_share",
    "du_return_on_equity",
    "sales_gross_profit",
    "inc_revenue_rate",
    "du_profit_rate",
    "inc_net_profit_rate",
    "adjusted_net_profit_rate",
    "inc_total_revenue_annual",
    "inc_net_profit_to_shareholders_annual",
    "adjusted_profit_to_profit_annual",
    "equity_roe",
    "net_roe",
    "total_roe",
    "gross_profit",
    "net_profit",
    "actual_tax_rate",
    "pre_pay_operate_income",
    "sales_cash_flow",
    "pre_pay_operate_income",
    "sales_cash_flow",
    "gear_ratio",
    "inventory_turnover",
    "m_anntime",
    "m_timetag",
    "inc_revenue",
    "inc_gross_profit",
    "inc_profit_before_tax",
    "du_profit",
    "inc_net_profit",
    "adjusted_net_profit",
    "__extra_36",
    "__extra_37",
    "__extra_38",
    "__extra_39",
    "__extra_40",
];

impl FinanceData {
    pub fn named_values(&self, file_type: FileType) -> Option<Vec<(&'static str, f64)>> {
        match self {
            FinanceData::Report { columns } => {
                let names = FinanceReader::column_names(file_type)?;
                Some(
                    names
                        .iter()
                        .copied()
                        .zip(columns.iter().copied())
                        .collect(),
                )
            }
            FinanceData::Ratios { ratios } => {
                let names = FinanceReader::column_names(FileType::Ratios)?;
                Some(
                    names
                        .iter()
                        .copied()
                        .zip(ratios.iter().copied())
                        .collect(),
                )
            }
            _ => None,
        }
    }
}


/// 财务文件读取器
pub struct FinanceReader;

impl FinanceReader {
    pub fn column_names(file_type: FileType) -> Option<&'static [&'static str]> {
        match file_type {
            FileType::BalanceSheet => Some(&BALANCE_COLUMN_NAMES),
            FileType::Income => Some(&INCOME_COLUMN_NAMES),
            FileType::CashFlow => Some(&CASHFLOW_COLUMN_NAMES),
            FileType::Ratios => Some(&RATIO_COLUMN_NAMES),
            _ => None,
        }
    }

    /// 读取文件并解析为 Struct 列表
    pub fn read_file(path: impl AsRef<Path>) -> Result<Vec<FinanceRecord>, FinanceError> {
        let path = path.as_ref();
        let file_type = Self::detect_type(path)?;
        let file = File::open(path)?;

        // mmap 零拷贝读取
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let data = &mmap[..];

        match file_type {
            FileType::BalanceSheet => {
                Self::parse_fixed(data, STRIDE_BALANCE, 0, |body| {
                    let mut cols = Vec::with_capacity(COLUMNS_BALANCE);
                    for i in 0..COLUMNS_BALANCE {
                        cols.push(Self::read_f64(body, i * 8).unwrap_or(f64::NAN));
                    }
                    FinanceData::Report { columns: cols }
                })
            }
            FileType::Income => {
                Self::parse_fixed(data, STRIDE_INCOME, 8, |body| {
                    let mut cols = Vec::with_capacity(COLUMNS_INCOME);
                    for i in 0..COLUMNS_INCOME {
                        cols.push(Self::read_f64(body, i * 8).unwrap_or(f64::NAN));
                    }
                    FinanceData::Report { columns: cols }
                })
            }
            FileType::CashFlow => {
                Self::parse_fixed(data, STRIDE_CASHFLOW, 8, |body| {
                    let mut cols = Vec::with_capacity(COLUMNS_CASHFLOW);
                    for i in 0..COLUMNS_CASHFLOW {
                        cols.push(Self::read_f64(body, i * 8).unwrap_or(f64::NAN));
                    }
                    FinanceData::Report { columns: cols }
                })
            }
            FileType::Ratios => {
                Self::parse_fixed(data, STRIDE_RATIOS, 0, |body| {
                    let mut cols = Vec::with_capacity(COLUMNS_RATIOS);
                    for i in 0..COLUMNS_RATIOS {
                        cols.push(Self::read_f64(body, i * 8).unwrap_or(f64::NAN));
                    }
                    FinanceData::Ratios { ratios: cols }
                })
            }
            FileType::Capital => {
                Self::parse_fixed(data, STRIDE_CAPITAL, 0, |body| {
                    // Body Offset (Header=16): 0=Total, 8=Flow, 16=Restricted, 24=FreeFloat
                    FinanceData::Capital {
                        total_share: Self::read_f64(body, 0).unwrap_or(0.0),
                        flow_share: Self::read_f64(body, 8).unwrap_or(0.0),
                        restricted: Self::read_f64(body, 16).unwrap_or(0.0),
                        free_float_share: Self::read_f64(body, 24).unwrap_or(0.0),
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
                Self::parse_top_holders(data)
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
    fn parse_fixed<F>(
        data: &[u8],
        stride: usize,
        header_offset: usize,
        parser: F,
    ) -> Result<Vec<FinanceRecord>, FinanceError>
    where F: Fn(&[u8]) -> FinanceData {
        let mut results = Vec::new();
        let mut cursor = 0;
        let len = data.len();

        while cursor + header_offset + 16 <= len {
            // 扫描 Header
            let header_start = cursor + header_offset;
            let ts1 = LittleEndian::read_i64(&data[header_start..header_start+8]);
            let ts2 = LittleEndian::read_i64(&data[header_start+8..header_start+16]);

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

                    let body = &data[header_start+16..cursor+stride];
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
                    let a_holders = Self::read_f64(body, 8).unwrap_or(0.0) as i64;
                    let b_holders = Self::read_f64(body, 16).unwrap_or(0.0) as i64;
                    let h_holders = Self::read_f64(body, 24).unwrap_or(0.0) as i64;
                    let float_holders = Self::read_f64(body, 32).unwrap_or(0.0) as i64;
                    let other_holders = Self::read_f64(body, 40).unwrap_or(0.0) as i64;

                    results.push(FinanceRecord {
                        report_date,
                        announce_date,
                        data: FinanceData::HolderCount {
                            total_holders,
                            a_holders,
                            b_holders,
                            h_holders,
                            float_holders,
                            other_holders,
                        }
                    });
                    cursor += stride;
                    continue;
                }
            }
            cursor += 8;
        }
        Ok(results)
    }

    /// 解析 7006/7007 十大(流通)股东定长记录，并按同一报告期聚合。
    fn parse_top_holders(data: &[u8]) -> Result<Vec<FinanceRecord>, FinanceError> {
        let mut results = Vec::new();
        let mut current_report_ts = 0i64;
        let mut current_announce_ts = 0i64;
        let mut current_holders = Vec::new();

        for chunk in data.chunks_exact(STRIDE_TOP_HOLDER) {
            let announce_ts = LittleEndian::read_i64(&chunk[0..8]);
            let report_ts = LittleEndian::read_i64(&chunk[8..16]);
            if !Self::is_valid_ts(announce_ts) || !Self::is_valid_ts(report_ts) {
                continue;
            }

            let holder = Self::parse_top_holder_record(chunk);
            if current_holders.is_empty() {
                current_report_ts = report_ts;
                current_announce_ts = announce_ts;
            }

            if report_ts != current_report_ts || announce_ts != current_announce_ts {
                results.push(FinanceRecord {
                    report_date: Self::ts_to_bj(current_report_ts),
                    announce_date: Self::ts_to_bj(current_announce_ts),
                    data: FinanceData::TopHolder {
                        holders: std::mem::take(&mut current_holders),
                    },
                });
                current_report_ts = report_ts;
                current_announce_ts = announce_ts;
            }

            current_holders.push(holder);
        }

        if !current_holders.is_empty() {
            results.push(FinanceRecord {
                report_date: Self::ts_to_bj(current_report_ts),
                announce_date: Self::ts_to_bj(current_announce_ts),
                data: FinanceData::TopHolder {
                    holders: current_holders,
                },
            });
        }

        Ok(results)
    }

    fn parse_top_holder_record(record: &[u8]) -> Shareholder {
        Shareholder {
            name: Self::read_string(record, 16, 192),
            holder_type: Self::read_string(record, 216, 56),
            hold_amount: Self::read_f64(record, 272).unwrap_or(0.0),
            change_reason: Self::read_string(record, 280, 16),
            hold_ratio: Self::read_f64(record, 304).unwrap_or(0.0),
            share_type: Self::read_string(record, 312, 96),
            rank: LittleEndian::read_u32(&record[412..416]),
        }
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

        if let FinanceData::Report { columns } = &res[0].data {
            assert_eq!(columns.len(), 156);
            assert!(columns[0].is_nan());
            assert!((columns[11] - 273_297_896.39).abs() < 1e-6);
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
            assert!((columns[0] - 4_809_251_460.5).abs() < 1e-6);
            assert!(columns[1].is_nan());
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
            assert_eq!(columns.len(), 111);
            assert!(columns[0].is_nan());
            assert!((columns[23] - 5_506_707_615.58).abs() < 1e-6);
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

        if let FinanceData::Capital {
            total_share,
            flow_share,
            restricted,
            free_float_share,
        } = &res[0].data {
            assert_eq!(*total_share, 400_100_000.0);
            assert_eq!(*flow_share, 40_080_000.0);
            assert_eq!(*restricted, 0.0);
            assert_eq!(*free_float_share, 40_080_000.0);
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

        if let FinanceData::HolderCount {
            total_holders,
            a_holders,
            b_holders,
            h_holders,
            float_holders,
            other_holders,
        } = &res[0].data {
            assert_eq!(*total_holders, 35_719);
            assert_eq!(*a_holders, 35_719);
            assert_eq!(*b_holders, 0);
            assert_eq!(*h_holders, 0);
            assert_eq!(*float_holders, 0);
            assert_eq!(*other_holders, 0);
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
        assert!(!res.is_empty(), "7006 should not be empty");

        if let FinanceData::TopHolder { holders } = &res[0].data {
            assert_eq!(holders.len(), 40);
            assert_eq!(holders[0].name, "中国航空技术深圳有限公司");
            assert_eq!(holders[0].holder_type, "机构投资账户");
            assert_eq!(holders[0].hold_amount, 158_128_000.0);
            assert_eq!(holders[0].change_reason, "不变");
            assert_eq!(holders[0].hold_ratio, 39.52);
            assert_eq!(holders[0].share_type, "流通A股");
            assert_eq!(holders[0].rank, 1);
        } else {
            panic!("7006 parsed as wrong type");
        }

        print_head(7006, &res);
    }

    #[test]
    fn test_7007_top_holder() {
        let path = get_fixture("002419_7007.DAT");
        if !path.exists() { eprintln!("Skipping 7007: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7007");
        assert!(!res.is_empty(), "7007 should not be empty");

        if let FinanceData::TopHolder { holders } = &res[0].data {
            assert_eq!(holders.len(), 10);
            assert_eq!(holders[0].name, "中国工商银行-诺安股票证券投资基金");
            assert_eq!(holders[0].holder_type, "机构投资账户");
            assert_eq!(holders[0].hold_amount, 1_799_860.0);
            assert_eq!(holders[0].change_reason, "不变");
            assert_eq!(holders[0].hold_ratio, 0.45);
            assert_eq!(holders[0].share_type, "流通A股");
            assert_eq!(holders[0].rank, 1);
        } else {
            panic!("7007 parsed as wrong type");
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

    #[test]
    fn test_report_column_names() {
        let balance = FinanceReader::column_names(FileType::BalanceSheet).expect("7001 names");
        assert_eq!(balance.len(), 156);
        assert_eq!(balance[0], "internal_shoule_recv");
        assert_eq!(balance[44], "cash_equivalents");
        assert_eq!(balance[140], "m_otherReserves");
        assert_eq!(balance[141], "__extra_141");

        let income = FinanceReader::column_names(FileType::Income).expect("7002 names");
        assert_eq!(income.len(), 80);
        assert_eq!(income[0], "revenue_inc");
        assert_eq!(income[3], "total_operating_cost");
        assert_eq!(income[65], "m_otherItemsAffectingNetProfit");
        assert_eq!(income[66], "__extra_66");

        let cashflow = FinanceReader::column_names(FileType::CashFlow).expect("7003 names");
        assert_eq!(cashflow.len(), 111);
        assert_eq!(cashflow[0], "cash_received_ori_ins_contract_pre");
        assert_eq!(cashflow[26], "goods_sale_and_service_render_cash");
        assert_eq!(cashflow[105], "m_addOperatingReceivableItems");
        assert_eq!(cashflow[106], "__extra_106");

        let ratios = FinanceReader::column_names(FileType::Ratios).expect("7008 names");
        assert_eq!(ratios.len(), 41);
        assert_eq!(ratios[0], "s_fa_ocfps");
        assert_eq!(ratios[1], "s_fa_bps");
        assert_eq!(ratios[35], "adjusted_net_profit");
        assert_eq!(ratios[36], "__extra_36");
    }

    #[test]
    fn test_named_values_for_ratios() {
        let path = get_fixture("002419_7008.DAT");
        if !path.exists() { eprintln!("Skipping 7008: File not found"); return; }

        let res = FinanceReader::read_file(&path).expect("Failed to parse 7008");
        let named = res[0]
            .data
            .named_values(FileType::Ratios)
            .expect("named ratios");

        assert_eq!(named[0].0, "s_fa_ocfps");
        assert!((named[0].1 - 0.5646).abs() < 1e-9);
        assert_eq!(named[1].0, "s_fa_bps");
        assert!((named[1].1 - 7.62).abs() < 1e-9);
    }
}
