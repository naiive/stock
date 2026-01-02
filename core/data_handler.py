#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module: DataHandler
Description: 负责量化系统的数据供应。核心功能是实现“历史数据+实时数据”的内存级无缝拼接。
Design Pattern: 门面模式 (Facade)，屏蔽了 MySQL 和 API 调用的复杂性。
"""

import datetime
import pandas as pd
from core.data_client.api_client import APIClient
from core.data_client.mysql_client import MySQLClient
from conf.config import SYSTEM_CONFIG
from core.data_manager import StockListManager

class DataHandler:
    """
    实时数据中心处理器
    职责：
    1. 实例化各类基础数据客户端。
    2. 管理全市场实时快照的本地内存缓存（realtime_cache）。
    3. 为策略提供“今天+过去”的完整 K 线序列，确保指标计算的实时性。
    """
    def __init__(self):
        # 初始化数据库客户端，用于读取历史日线、周线等持久化数据
        self.mysql_client = MySQLClient()
        # 初始化行情接口客户端，用于获取当前交易日的即时价格
        self.api_client = APIClient()
        # 初始化股票清单管理器，处理如“排除ST”、“筛选板块”等初筛逻辑
        self.manager = StockListManager(self.mysql_client)
        # 内存缓存池：存储全市场 5000+ 股票的实时快照，避免在循环中重复请求网络
        self.realtime_cache = None

    def get_target_list(self):
        """
        获取本次扫描的目标股票池。
        Returns:
            list: 包含所有待扫描股票代码的列表，例如 ['600000', '000001', ...]
        """
        df = self.manager.get_stock_list()
        # 如果获取失败则返回空列表，防止后续迭代报错
        return df['code'].tolist() if df is not None else []

    @staticmethod
    def chunk_symbols(symbols_list, size):
        """
        静态工具方法：将海量股票列表切分为指定大小的批次。
        Args:
            symbols_list (list): 原始股票代码列表。
            size (int): 每个批次的容量（如 500）。
        Yields:
            list: 切片后的子列表。
        """
        symbols_list = list(symbols_list)  # 强制转换以确保可切片
        for i in range(0, len(symbols_list), size):
            yield symbols_list[i: i + size]

    def prepare_realtime_data(self):
        """
        核心性能优化方法：
        由扫描引擎在启动前【在主线程中】统一调用一次。
        将全市场的实时行情一次性拉入内存，使后续并发扫描达到 O(1) 的查询速度。
        """
        if SYSTEM_CONFIG.get("USE_REAL_TIME_DATA", True):
            print("🚀 [系统] 正在预取全市场实时快照，存入内存缓存...")
            # 将 API 返回的 DataFrame 存入实例变量
            self.realtime_cache = self.api_client.fetch_realtime_snapshot()

    def get_full_data(self, symbol):
        """
        为单只股票构建完整的时间序列。
        【高频调用点】：此方法通常运行在线程池的工作线程中。

        Args:
            symbol (str): 股票代码。
        Returns:
            pd.DataFrame: 包含历史记录及（可选）今日实时行的 DataFrame。
        """
        # 1. 首先从数据库获取该股票所有的历史日线数据
        df_daily = self.mysql_client.fetch_daily_data(symbol)

        # 2. 检查配置：如果不开启实时数据，或者缓存为空（非交易时间），则直接返回历史数据
        if not SYSTEM_CONFIG.get("USE_REAL_TIME_DATA") or self.realtime_cache is None:
            return df_daily

        # 3. 尝试将实时快照“缝合”到历史数据末尾
        return self._append_snapshot(symbol, df_daily)

    def _append_snapshot(self, symbol, df_daily):
        """
        私有方法：执行数据缝合的具体逻辑。
        处理边界条件：数据为空、日期冲突、重复数据。
        """
        # 安全检查：无缓存或无历史数据则无法缝合
        if self.realtime_cache is None or df_daily.empty:
            return df_daily

        today = datetime.datetime.now().date()

        # 从全市场缓存中精准定位当前股票的实时行
        spot_row = self.realtime_cache[self.realtime_cache['code'] == symbol]
        if spot_row.empty:
            return df_daily

        # 提取第一行（理论上代码唯一，只有一行）
        latest_data = spot_row.iloc[0]

        # 【重要逻辑】：重复日期检查
        # 目的：防止在收盘后数据库已更新的情况下，再次重复添加今天的数据
        last_date = pd.to_datetime(df_daily['date']).dt.date.iloc[-1]
        if last_date >= today:
            return df_daily

        # 构造符合日线格式的字典数据
        new_row = {
            'date': today,
            'code': symbol,
            'open': latest_data['open'],
            'high': latest_data['high'],
            'low': latest_data['low'],
            'close': latest_data['close'],
            'volume': latest_data['volume'],
            'amount': latest_data['amount']
        }

        # 将构造的新行转为 DataFrame 并追加，ignore_index 确保索引连续
        # 注意：此处操作仅在内存中，不影响数据库，专为策略计算使用
        return pd.concat([df_daily, pd.DataFrame([new_row])], ignore_index=True)