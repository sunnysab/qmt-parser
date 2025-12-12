import struct
import pandas as pd
import numpy as np
import os
from typing import Optional, Dict, Any, List


def is_leap_year(year: int) -> bool:
    """判断是否为闰年"""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def get_days_in_month(year: int, month: int) -> int:
    """获取指定年月的天数"""
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if month == 2 and is_leap_year(year):
        return 29
    return days_in_month[month - 1]


def validate_date_format(date_str: str) -> str:
    """验证并转换8位日期格式为标准格式"""
    if not date_str.isdigit() or len(date_str) != 8:
        raise ValueError(f"日期格式错误，必须是8位数字格式(YYYYMMDD): {date_str}")
    
    try:
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        
        # 验证年份范围 (1900-2100)
        if year < 1900 or year > 2100:
            raise ValueError(f"年份超出范围(1900-2100): {year}")
        
        # 验证月份范围 (1-12)
        if month < 1 or month > 12:
            raise ValueError(f"月份超出范围(1-12): {month}")
        
        # 验证日期范围
        max_days = get_days_in_month(year, month)
        if day < 1 or day > max_days:
            raise ValueError(f"日期超出范围(1-{max_days}): {day}")
        
        return date_str  # 返回原始8位格式
    except (ValueError, IndexError) as e:
        raise ValueError(f"无效的日期: {date_str}, 错误: {e}")


def parse_daily_kline(path: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """
    解析QMT的日线数据

    Args:
        path (str): 日线数据文件路径，必须是.dat或.DAT格式
        start (str): 开始日期，格式为'YYYYMMDD'（8位数字）
        end (str): 结束日期，格式为'YYYYMMDD'（8位数字）
        
    Returns:
        Optional[pd.DataFrame]: 解析成功返回DataFrame，失败返回None
        
    Raises:
        TypeError: 当参数类型不正确时
        ValueError: 当参数值不正确时
    """
    # 参数校验
    for param_name, param_value in [('path', path), ('start', start), ('end', end)]:
        if not isinstance(param_value, str):
            raise TypeError(f"{param_name}必须是字符串类型，当前类型: {type(param_value)}")
        if not param_value.strip():
            raise ValueError(f"{param_name}不能为空")
    
    if not path.lower().endswith('.dat'):
        raise ValueError("文件必须是.dat或.DAT格式")
    
    # 验证8位日期格式
    try:
        start_date_validated = validate_date_format(start)
        end_date_validated = validate_date_format(end)
    except ValueError as e:
        raise ValueError(f"日期格式错误: {e}")
    
    if not os.path.exists(path):
        print(f"错误: 文件 {path} 不存在。")
        return None

    kline_data_raw: List[Dict[str, Any]] = []
    record_size: int = 64          # 每条日线记录的字节大小
    price_scale: float = 1000.0    # 价格缩放因子，原始价格需要除以1000
    amount_scale: float = 100.0    # 成交金额缩放因子，根据分钟线和000002日线推断，单位是分

    try:
        with open(path, 'rb') as f:
            # 循环读取所有64字节的记录
            while True:
                record_bytes: bytes = f.read(record_size)
                if not record_bytes or len(record_bytes) < record_size:
                    break

                kline: Dict[str, Any] = {}

                # --- 应用统一的、经过验证的结构 ---

                # Bytes 8-11: Unix时间戳解析
                ts_seconds: int = struct.unpack('<I', record_bytes[8:12])[0]
                # 将UTC时间戳转换为上海时区时间
                kline['time'] = pd.to_datetime(ts_seconds, unit='s', origin='unix').tz_localize('UTC').tz_convert('Asia/Shanghai')

                # Bytes 12-27: OHLC价格数据解析（开高低收，缩放因子1000）
                kline['open'] = struct.unpack('<I', record_bytes[12:16])[0] / price_scale   # 开盘价
                kline['high'] = struct.unpack('<I', record_bytes[16:20])[0] / price_scale   # 最高价
                kline['low'] = struct.unpack('<I', record_bytes[20:24])[0] / price_scale    # 最低价
                kline['close'] = struct.unpack('<I', record_bytes[24:28])[0] / price_scale  # 收盘价

                # Bytes 32-35: 成交量（无符号32位整数）
                kline['volume'] = struct.unpack('<I', record_bytes[32:36])[0]

                # Bytes 40-47: 成交金额（无符号64位整数，缩放因子100）
                amount_raw: int = struct.unpack('<Q', record_bytes[40:48])[0]
                kline['amount'] = float(amount_raw) / amount_scale

                # Bytes 48-51: 持仓量/开放权益（无符号32位整数）
                kline['openInterest'] = struct.unpack('<I', record_bytes[48:52])[0]

                # Bytes 60-63: 前收盘价（缩放因子1000）
                kline['preClose'] = struct.unpack('<I', record_bytes[60:64])[0] / price_scale

                kline_data_raw.append(kline)

    except Exception as e:
        print(f"解析日线文件时发生错误: {e}")
        return None

    if not kline_data_raw:
        return pd.DataFrame()

    # --- 后处理，模拟API业务逻辑 ---
    df_raw: pd.DataFrame = pd.DataFrame(kline_data_raw)
    
    # 将时间转换为日期格式，去除时分秒信息
    df_raw['time'] = df_raw['time'].dt.date
    df_raw['time'] = pd.to_datetime(df_raw['time'])
    df_raw.set_index('time', inplace=True)
    
    # 去除重复日期，保留第一个记录
    df_raw = df_raw[~df_raw.index.duplicated(keep='first')]

    # 生成工作日日期范围（排除周末）
    start_date_formatted = f"{start_date_validated[:4]}-{start_date_validated[4:6]}-{start_date_validated[6:8]}"
    end_date_formatted = f"{end_date_validated[:4]}-{end_date_validated[4:6]}-{end_date_validated[6:8]}"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date_formatted, end=end_date_formatted, freq='B')
    
    # 重新索引并前向填充缺失数据
    df_final: pd.DataFrame = df_raw.reindex(date_range)
    df_final.fillna(method='ffill', inplace=True)
    df_final.dropna(inplace=True)

    # 业务逻辑1: 根据成交量和成交额判断是否停牌
    # 当成交量为0且成交金额为0时，认为是停牌
    api_like_suspend_condition: pd.Series = (df_final['volume'] == 0) & (df_final['amount'] == 0.0)
    df_final['suspendFlag'] = np.where(api_like_suspend_condition, 1, 0)

    # 业务逻辑2: 计算昨收价 (上一交易日的收盘价)
    df_final['preClose_calculated'] = df_final['close'].shift(1)

    # 业务逻辑3: 对于第一条数据，或停牌日，API的preClose等于当天的close
    first_day_index: pd.Timestamp = df_final.index[0]
    df_final.loc[first_day_index, 'preClose_calculated'] = df_final.loc[first_day_index, 'close']
    # 停牌日的前收价使用当日收盘价
    df_final['preClose'] = np.where(df_final['suspendFlag'] == 1, df_final['close'], df_final['preClose_calculated'])

    # 重置索引并重命名
    df_final.reset_index(inplace=True)
    df_final.rename(columns={'index': 'time_date'}, inplace=True)

    # 将日期转换为毫秒时间戳
    df_final['time'] = (df_final['time_date'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')
    df_final['settlementPrice'] = 0.0  # 结算价固定为0.0

    # 确保数值类型正确
    float_columns: List[str] = ['open', 'high', 'low', 'close', 'amount', 'preClose']
    for col in float_columns:
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    int_columns: List[str] = ['volume', 'openInterest', 'suspendFlag']
    for col in int_columns:
         df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)

    df_final['settlementPrice'] = 0.0  # 结算价固定为0.0

    # 按照标准顺序重新组织列
    column_order: List[str] = [
        'time', 'open', 'high', 'low', 'close', 
        'volume', 'amount', 'settlementPrice', 
        'openInterest', 'preClose', 'suspendFlag'
    ]
    df_final = df_final[column_order]

    return df_final

if __name__ == "__main__":
    daily_path = "/mnt/data/trade/qmtdata/datadir/SZ/86400/000001.DAT"

    if os.path.exists(daily_path):
        # 需要提供与API调用相同的起止时间（8位格式）
        start_time = '19910401'  # 8位格式
        end_time = '19910425'    # 8位格式

        df_daily_parsed = parse_daily_kline(daily_path, start_time, end_time)

        if df_daily_parsed is not None and not df_daily_parsed.empty:
            print("--- 日线文件解析结果 ---")

            # 插入一个与API输出一样的索引列，方便对比
            df_daily_parsed.index = pd.to_datetime(df_daily_parsed['time'], unit='ms').dt.strftime('%Y%m%d')
            print(df_daily_parsed.to_string())
