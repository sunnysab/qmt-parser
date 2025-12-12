"""
xtquant数据解析器包

提供tick数据、分钟K线数据和日线K线数据的解析功能
"""

from .tick import parse_ticks
from .min import parse_1min_kline  
from .day import parse_daily_kline

__all__ = ['parse_ticks', 'parse_1min_kline', 'parse_daily_kline']
__version__ = '1.0.0'