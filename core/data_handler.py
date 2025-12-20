# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from core.data_client.api_client import APIClient
from core.data_client.mysql_client import MySQLClient
from conf.config import SYSTEM_CONFIG
from core.data_manager import StockListManager


class DataHandler:
    """
    实时合并历史数据
    """
    def __init__(self):
        # 1. 初始化客户端
        self.mysql_client = MySQLClient()
        self.api_client = APIClient()
        # 2. 初始化名单管理器
        self.manager = StockListManager(self.mysql_client)
        # 3. 实时快照缓存
        self.realtime_cache = None

    def get_target_list(self):
        """获取清洗后的股票代码列表 (List类型)"""
        df = self.manager.get_stock_list()
        if df is None or df.empty:
            return []
        return df['code'].tolist()

    @staticmethod
    def chunk_symbols(symbols_list, size):
        """
        纯粹的切片工具，确保输入的是 list
        """
        if not isinstance(symbols_list, list):
            # 这里的防御：如果不是 list，尝试转换
            symbols_list = list(symbols_list)

        for i in range(0, len(symbols_list), size):
            yield symbols_list[i: i + size]

    def prepare_realtime_data(self):
        """预先拉取全量快照"""
        if SYSTEM_CONFIG.get("USE_REAL_TIME_DATA", True):
            print("🚀 [系统] 正在预取全市场实时快照...")
            self.realtime_cache = self.api_client.fetch_realtime_snapshot()

    def get_full_data(self, symbol):
        """获取 历史 + 实时 拼接后的数据"""
        df_daily = self.mysql_client.fetch_daily_data(symbol)
        if not SYSTEM_CONFIG.get("USE_REAL_TIME_DATA") or self.realtime_cache is None:
            return df_daily
        return self._append_snapshot(symbol, df_daily)

    def _append_snapshot(self, symbol, df_daily):
        if self.realtime_cache is None or df_daily.empty:
            return df_daily

        today = datetime.datetime.now().date()
        spot_row = self.realtime_cache[self.realtime_cache['code'] == symbol]
        if spot_row.empty:
            return df_daily

        latest_data = spot_row.iloc[0]
        # 日期重复检查
        last_date = pd.to_datetime(df_daily['date']).dt.date.iloc[-1]
        if last_date >= today:
            return df_daily

        new_row = {
            'date': today, 'code': symbol,
            'open': latest_data['open'], 'high': latest_data['high'],
            'low': latest_data['low'], 'close': latest_data['close'],
            'volume': latest_data['volume'], 'amount': latest_data['amount']
        }
        df_new = pd.DataFrame([new_row])
        return pd.concat([df_daily, df_new], ignore_index=True)