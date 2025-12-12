import struct
import pandas as pd
import os
from typing import Optional, Dict, Any, List


def parse_1min_kline(file_path: str) -> Optional[pd.DataFrame]:
    """
    解析QMT的1分钟K线数据文件(.dat)
    
    Args:
        file_path (str): K线数据文件路径，必须是.dat或.DAT格式
        
    Returns:
        Optional[pd.DataFrame]: 解析成功返回DataFrame，失败返回None
        
    Raises:
        TypeError: 当file_path不是字符串时
        ValueError: 当文件路径为空或格式不正确时
    """
    # 参数校验
    if not isinstance(file_path, str):
        raise TypeError(f"file_path必须是字符串类型，当前类型: {type(file_path)}")
    
    if not file_path.strip():
        raise ValueError("文件路径不能为空")
    
    if not file_path.lower().endswith('.dat'):
        raise ValueError("文件必须是.dat或.DAT格式")
    
    if not os.path.exists(file_path):
        print(f"错误: 文件 {file_path} 不存在。")
        return None

    kline_data: List[Dict[str, Any]] = []
    record_size: int = 64        # 每条K线记录的字节大小
    price_scale: float = 1000.0  # 价格缩放因子，原始价格需要除以1000

    try:
        with open(file_path, 'rb') as f:
            while True:
                # 读取一条K线记录
                record_bytes: bytes = f.read(record_size)
                if not record_bytes or len(record_bytes) < record_size:
                    break

                kline: Dict[str, Any] = {}

                # Bytes 8-11: Unix时间戳解析
                ts_seconds: int = struct.unpack('<I', record_bytes[8:12])[0]
                # 将UTC时间戳转换为上海时区时间
                kline['time'] = pd.to_datetime(ts_seconds, unit='s', utc=True).tz_convert('Asia/Shanghai')

                # Bytes 12-27: OHLC价格数据解析（开高低收）
                kline['open'] = struct.unpack('<I', record_bytes[12:16])[0] / price_scale   # 开盘价
                kline['high'] = struct.unpack('<I', record_bytes[16:20])[0] / price_scale   # 最高价
                kline['low'] = struct.unpack('<I', record_bytes[20:24])[0] / price_scale    # 最低价
                kline['close'] = struct.unpack('<I', record_bytes[24:28])[0] / price_scale  # 收盘价

                # Bytes 32-35: 成交量（经过校正的偏移量）
                kline['volume'] = struct.unpack('<I', record_bytes[32:36])[0]

                # Bytes 40-47: 成交金额（使用8字节长整型，经过校正的偏移量）
                amount_raw: int = struct.unpack('<Q', record_bytes[40:48])[0]
                kline['amount'] = float(amount_raw)

                # Bytes 48-51: 持仓量/开放权益（经过校正的偏移量）
                kline['openInterest'] = struct.unpack('<I', record_bytes[48:52])[0]

                # Bytes 60-63: 前收盘价
                kline['preClose'] = struct.unpack('<I', record_bytes[60:64])[0] / price_scale

                # 添加固定字段（当前K线结构中不可用的数据）
                kline['settlementPrice'] = 0.0  # 结算价
                kline['suspendFlag'] = 0        # 停牌标志

                kline_data.append(kline)

    except Exception as e:
        print(f"解析文件 {file_path} 时发生错误: {e}")
        return None

    if not kline_data:
        return pd.DataFrame()

    df: pd.DataFrame = pd.DataFrame(kline_data)
    
    # 按照标准顺序重新组织列
    column_order: List[str] = [
        'time', 'open', 'high', 'low', 'close', 
        'volume', 'amount', 'settlementPrice', 
        'openInterest', 'preClose', 'suspendFlag'
    ]
    df = df[column_order]
    return df

if __name__ == "__main__":
    kline_file_path = "/mnt/data/trade/qmtdata/datadir/SZ/60/000001.DAT"
    # 确保文件存在才能运行
    if os.path.exists(kline_file_path):
        df_kline_parsed = parse_1min_kline(kline_file_path)

        if df_kline_parsed is not None and not df_kline_parsed.empty:
            print("成功解析1分钟K线数据。")

            # 将时间转换为与原始DF一样的格式以便对比
            df_kline_parsed['time_str'] = df_kline_parsed['time'].dt.strftime('%Y%m%d%H%M%S')

            print("解析结果后5行:")
            print(df_kline_parsed.tail())

            print("\n与原始DataFrame最后一行 (15:00:00) 对比:")
            last_row_parsed = df_kline_parsed.iloc[-1]

            print(f"  Parsed Time: {last_row_parsed['time_str']}")
            print(f"  Parsed Open: {last_row_parsed['open']:.2f}")
            print(f"  Parsed High: {last_row_parsed['high']:.2f}")
            print(f"  Parsed Low: {last_row_parsed['low']:.2f}")
            print(f"  Parsed Close: {last_row_parsed['close']:.2f}")
            print(f"  Parsed Volume: {last_row_parsed['volume']}")
            print(f"  Parsed Amount: {last_row_parsed['amount']:.1f}")
            print(f"  Parsed OpenInterest: {last_row_parsed['openInterest']}")
            print(f"  Parsed PreClose: {last_row_parsed['preClose']:.2f}")
    else:
        print(f"测试文件未找到: {kline_file_path}")
