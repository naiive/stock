# -*- coding: utf-8 -*-
"""
Module: MarketScanner
Description: 全市场异步扫描引擎。采用 "Async IO + ThreadPool Multi-threading" 混合架构。
- Async: 负责非阻塞的分批调度、等待和进度条刷新。
- ThreadPool: 负责密集的 CPU 计算（策略指标计算）和阻塞式 IO（数据库读取）。
"""

import pandas as pd
from core.data_handler import DataHandler
from conf.config import SYSTEM_CONFIG
from strategies.squeeze_resistance_strategy import run_strategy
from core.utils.notify import export_and_notify
from core.utils.enrich import enrich_results
from core.utils.dispatcher import run_dispatch


class MarketScanner:
    """
    扫描调度引擎类
    """

    def __init__(self):
        # 初始化数据处理器，内部包含数据库和 API 客户端
        self.handler = DataHandler()
        # 存储所有批次筛选出的买入信号结果
        self.matched_list = []

    def _worker(self, symbol):
        """
        单只股票的计算单元 (Worker)
        【运行环境】：此方法运行在线程池的工作线程中，不占用异步事件循环的主线程。

        Args:
            symbol (str): 股票代码。
        Returns:
            dict or None: 如果命中策略返回结果字典，否则返回 None。
        """
        # 1. 获取缝合后的完整数据 (MySQL 历史 + 内存实时快照)
        df = self.handler.get_full_data(symbol)

        # 2. 调用核心策略函数计算信号
        # 这里传入 df 和 symbol，策略内部执行指标计算逻辑
        return run_strategy(df, symbol)

    async def run_full_scan(self, symbols=None):
        """
        高性能异步并发调度工具
        只负责“调度”与“并发控制”，不涉及具体的业务逻辑
        """
        # 1. 如果没传股票列表，自己去捞完整名单（把决定权留在业务层）
        target_symbols = symbols or self.handler.get_target_list()

        # 2. 调用通用调度器
        self.matched_list = await run_dispatch(
            # 代码池
            symbols=target_symbols,
            # 传入计算逻辑
            worker_func=self._worker,
            # 传入预取逻辑【避免多次实时接口请求，浪费资源，传入后，只会调用一次，而且保证数据的时间一致性】
            # ***********************************************
            prepare_hook=self.handler.prepare_realtime_data,
            # ***********************************************
            # 将 self.export_results 作为函数引用传入【注意：这里去掉了括号】
            finalize_hook=self.export_results,
            desc="A股市场扫描"
        )

    def export_results(self, results: list = None):
        """
        [导出功能] 负责结果的最终落盘
        有参数控制是否导出：名称、市值大小、市盈率等信息
        """
        # 【修正】：优先使用 dispatch 传回的 results
        # 如果 results 为 None (手动调用时)，则回退到 self.matched_list
        target_list = results if results is not None else self.matched_list

        if not target_list:
            print("\n🏁 扫描完成，未发现匹配信号。")
            return

        # 1. 转换为初始 DataFrame
        final_df = pd.DataFrame(target_list)

        # 2. 调用独立增强函数 (根据开关参数)，名称、市盈率、总市值等信息
        if SYSTEM_CONFIG.get("ENABLE_RESULT_ENRICHMENT", False):
            print(f"🔍 [系统] 正在执行数据增强：处理 {len(target_list)} 条命中数据...")
            final_df = enrich_results(final_df, handler=self.handler)
        else:
            pass

        # 3. 控制台输出
        print(final_df)

        # 4. 导出 + 通知（由 notify.export_and_notify 统一处理）
        export_and_notify(final_df)