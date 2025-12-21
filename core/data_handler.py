# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from core.data_client.api_client import APIClient
from core.data_client.mysql_client import MySQLClient
from conf.config import SYSTEM_CONFIG
from core.data_manager import StockListManager

class DataHandler:
    def __init__(self):
        self.mysql_client = MySQLClient()
        self.api_client = APIClient()
        self.manager = StockListManager(self.mysql_client)
        self.realtime_cache = None

    def get_target_list(self):
        df = self.manager.get_stock_list()
        return df['code'].tolist() if df is not None else []

    @staticmethod
    def chunk_symbols(symbols_list, size):
        symbols_list = list(symbols_list)
        for i in range(0, len(symbols_list), size):
            yield symbols_list[i: i + size]

    def prepare_realtime_data(self):
        """由 Scanner 在主线程调用一次"""
        if SYSTEM_CONFIG.get("USE_REAL_TIME_DATA", True):
            print("🚀 [系统] 正在预取全市场实时快照...")
            self.realtime_cache = self.api_client.fetch_realtime_snapshot()

    def get_full_data(self, symbol):
        """此方法在线程池中运行，无需 async"""
        df_daily = self.mysql_client.fetch_daily_data(symbol)
        if not SYSTEM_CONFIG.get("USE_REAL_TIME_DATA") or self.realtime_cache is None:
            return df_daily
        return self._append_snapshot(symbol, df_daily)

    def _append_snapshot(self, symbol, df_daily):
        if self.realtime_cache is None or df_daily.empty:
            return df_daily

        today = datetime.datetime.now().date()
        # 优化：从 cache 中查找
        spot_row = self.realtime_cache[self.realtime_cache['code'] == symbol]
        if spot_row.empty: return df_daily

        latest_data = spot_row.iloc[0]
        # 重复日期检查
        last_date = pd.to_datetime(df_daily['date']).dt.date.iloc[-1]
        if last_date >= today: return df_daily

        new_row = {
            'date': today, 'code': symbol,
            'open': latest_data['open'], 'high': latest_data['high'],
            'low': latest_data['low'], 'close': latest_data['close'],
            'volume': latest_data['volume'], 'amount': latest_data['amount']
        }
        return pd.concat([df_daily, pd.DataFrame([new_row])], ignore_index=True)