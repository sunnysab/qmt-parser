import struct
import pandas as pd
import os
import numpy as np
from typing import Optional, Dict, Any, List

def parse_ticks(path: str) -> Optional[pd.DataFrame]:
    """
    解析miniqmt的tick数据文件 (.dat) 并返回一个Pandas DataFrame。
    
    Args:
        path (str): tick数据文件路径，必须是.dat格式
        
    Returns:
        Optional[pd.DataFrame]: 解析成功返回DataFrame，失败返回None
        
    Raises:
        TypeError: 当path不是字符串时
        ValueError: 当文件路径为空或格式不正确时
    """
    # 参数校验
    if not isinstance(path, str):
        raise TypeError(f"path必须是字符串类型，当前类型: {type(path)}")
    
    if not path.strip():
        raise ValueError("文件路径不能为空")
    
    if not path.lower().endswith('.dat'):
        raise ValueError("文件必须是.dat格式")
    
    if not os.path.exists(path):
        print(f"错误: 文件 {path} 不存在。")
        return None

    # 从文件名提取日期信息
    filename: str = os.path.basename(path)
    date_str: str = filename.split('.')[0]

    ticks_data: List[Dict[str, Any]] = []
    record_size: int = 144  # 每条tick记录的字节大小
    price_scale: float = 1000.0  # 价格缩放因子，原始价格需要除以1000

    # 集合竞价阶段的状态码
    CALL_AUCTION_PHASE_CODES: List[int] = [12]

    try:
        with open(path, 'rb') as f:
            while True:
                # 读取一条tick记录
                record_bytes: bytes = f.read(record_size)
                if not record_bytes or len(record_bytes) < record_size:
                    break

                tick: Dict[str, Any] = {}

                # 基础时间和状态信息解析
                tick['date'] = date_str
                tick['raw_qmt_timestamp'] = struct.unpack('<I', record_bytes[0:4])[0]  # QMT原始时间戳

                # QMT状态字段（用于调试和完整性检查）
                tick['qmt_status_field_1_raw'] = struct.unpack('<I', record_bytes[4:8])[0]
                tick['qmt_status_field_2_raw'] = struct.unpack('<I', record_bytes[12:16])[0]

                # 核心交易数据解析
                raw_last_price: int = struct.unpack('<I', record_bytes[8:12])[0]
                raw_amount: int = struct.unpack('<I', record_bytes[16:20])[0]
                raw_volume: int = struct.unpack('<I', record_bytes[24:28])[0]

                # 市场阶段状态（12=集合竞价，其他=连续竞价）
                market_phase_status: int = struct.unpack('<I', record_bytes[28:32])[0]
                tick['market_phase_status'] = market_phase_status

                # 昨收价
                tick['last_close'] = struct.unpack('<I', record_bytes[60:64])[0] / price_scale

                # 初始化核心交易数据字段
                tick['last_price'] = np.nan
                tick['amount'] = np.nan
                tick['volume'] = np.nan

                # 初始化买卖盘数据（5档）
                ask_prices_list: List[float] = [np.nan] * 5  # 卖价
                ask_vols_list: List[int] = [0] * 5           # 卖量
                bid_prices_list: List[float] = [np.nan] * 5  # 买价
                bid_vols_list: List[int] = [0] * 5           # 买量

                # 根据市场阶段处理不同的数据结构
                if market_phase_status in CALL_AUCTION_PHASE_CODES:
                    # 集合竞价阶段：没有实际成交，只有参考价和累计委托量
                    tick['last_price'] = 0.0
                    tick['amount'] = 0.0
                    tick['volume'] = 0

                    # 集合竞价参考价（买卖双方都使用同一个参考价）
                    ref_price: float = struct.unpack('<I', record_bytes[64:68])[0] / price_scale
                    ask_prices_list[0] = ref_price
                    bid_prices_list[0] = ref_price

                    # 集合竞价累计委托量
                    ask_vols_list[0] = struct.unpack('<I', record_bytes[84:88])[0]   # askVol[0]
                    ask_vols_list[1] = struct.unpack('<I', record_bytes[88:92])[0]   # askVol[1]
                    bid_vols_list[0] = struct.unpack('<I', record_bytes[124:128])[0] # bidVol[0]

                else:
                    # 连续竞价阶段：处理实际成交数据和5档买卖盘
                    tick['last_price'] = raw_last_price / price_scale
                    tick['amount'] = float(raw_amount)
                    tick['volume'] = raw_volume

                    # 解析5档买卖盘数据
                    for i in range(5):
                        # 卖盘价格和数量
                        ask_prices_list[i] = struct.unpack('<I', record_bytes[64 + i * 4 : 68 + i * 4])[0] / price_scale
                        ask_vols_list[i] = struct.unpack('<I', record_bytes[84 + i * 4 : 88 + i * 4])[0]
                        # 买盘价格和数量
                        bid_prices_list[i] = struct.unpack('<I', record_bytes[104 + i * 4 : 108 + i * 4])[0] / price_scale
                        bid_vols_list[i] = struct.unpack('<I', record_bytes[124 + i * 4 : 128 + i * 4])[0]

                # 存储买卖盘数据
                tick['askPrice'] = ask_prices_list
                tick['askVol'] = ask_vols_list
                tick['bidPrice'] = bid_prices_list
                tick['bidVol'] = bid_vols_list

                # 添加其他必要字段（当前tick结构中无法直接获取的数据）
                tick['open'] = np.nan
                tick['high'] = np.nan
                tick['low'] = np.nan
                tick['pvolume'] = 0
                tick['tickvol'] = 0
                tick['stockStatus'] = 0
                tick['lastSettlementPrice'] = np.nan
                tick['settlementPrice'] = np.nan
                tick['transactionNum'] = np.nan
                tick['pe'] = np.nan
                tick['time'] = np.nan

                ticks_data.append(tick)

    except FileNotFoundError:
        print(f"错误: 文件 {path} 未找到。")
        return None
    except Exception as e:
        print(f"解析文件 {path} 时发生错误: {e}")
        return None

    if not ticks_data:
        return pd.DataFrame()

    df: pd.DataFrame = pd.DataFrame(ticks_data)

    # 按照标准顺序重新组织列
    desired_columns: List[str] = [
        'date', 'raw_qmt_timestamp', 'time',
        'last_price', 'open', 'high', 'low', 'last_close',
        'amount', 'volume', 'pvolume', 'tickvol',
        'market_phase_status', 'stockStatus',
        'qmt_status_field_1_raw', 'qmt_status_field_2_raw',
        'lastSettlementPrice',
        'askPrice', 'bidPrice', 'askVol', 'bidVol',
        'settlementPrice', 'transactionNum', 'pe'
    ]
    present_columns: List[str] = [col for col in desired_columns if col in df.columns]
    df = df[present_columns]

    return df


if __name__ == "__main__":
    file_to_parse = "/mnt/data/trade/qmtdata/datadir/SZ/0/000001/20250606.dat"
    df_ticks = parse_ticks(file_to_parse)

    if df_ticks is not None and not df_ticks.empty:
        print(f"解析器: 成功解析 {len(df_ticks)} 条tick数据。")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)

        print("DataFrame 前5行:")
        print(df_ticks.head())
        print("\nDataFrame 后5行:")
        print(df_ticks.tail())

        print("\n对比集合竞价 (第一行) askPrice/askVol 和 bidPrice/bidVol:")
        if len(df_ticks) > 0:
            first_tick = df_ticks.iloc[0]
            print(f"  Ask Price: {first_tick['askPrice']}")
            print(f"  Ask Volume: {first_tick['askVol']}")
            print(f"  Bid Price: {first_tick['bidPrice']}")
            print(f"  Bid Volume: {first_tick['bidVol']}")

        print("\n对比连续竞价 (最后一行) askPrice/askVol 和 bidPrice/bidVol:")
        if len(df_ticks) > 0:
            last_tick = df_ticks.iloc[-1]
            print(f"  Ask Price: {last_tick['askPrice']}")
            print(f"  Ask Volume: {last_tick['askVol']}")
            print(f"  Bid Price: {last_tick['bidPrice']}")
            print(f"  Bid Volume: {last_tick['bidVol']}")
    else:
        print("解析器: 未能解析数据或文件为空。")