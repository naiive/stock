# -*- coding: utf-8 -*-
"""
Module: MarketScanner
Description: 全市场异步扫描引擎。采用 "Async IO + ThreadPool Multi-threading" 混合架构。
- Async: 负责非阻塞的分批调度、等待和进度条刷新。
- ThreadPool: 负责密集的 CPU 计算（策略指标计算）和阻塞式 IO（数据库读取）。
"""

import asyncio
import time
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from core.data_handler import DataHandler
from conf.config import SYSTEM_CONFIG, PATH_CONFIG
from strategies.breakout_strategy import run_breakout_strategy


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
        return run_breakout_strategy(df, symbol)

    async def run_full_scan(self, symbols=None):
        """
        全市场扫描异步主入口

        Args:
            symbols (list, optional): 指定扫描的股票列表。若为 None 则扫描数据库全量名单。
        """
        # 如果未传入名单，则通过 handler 从数据库加载清洗后的全量代码
        if symbols is None:
            symbols = self.handler.get_target_list()

        if not symbols:
            print("❌ 扫描终止：待处理名单为空。")
            return

        # 1. 【性能关键点】：预取全市场实时快照
        # 在进入多线程计算前，一次性拉取所有股票当前价，后续线程直接从内存读，避免 5000 次网络请求
        self.handler.prepare_realtime_data()

        # 2. 任务分批化处理 (Batching)
        # 目的：防止一次性提交几千个线程导致内存溢出，同时给底层接口留出响应间隙
        batch_size = SYSTEM_CONFIG.get("BATCH_SIZE", 500)
        batches = list(self.handler.chunk_symbols(symbols, batch_size))
        print(f"✅ 异步扫描就绪，共 {len(symbols)} 只，分 {len(batches)} 批。")

        # 读取并发参数
        max_workers = SYSTEM_CONFIG.get("MAX_WORKERS", 10)  # 并行线程数
        interval = SYSTEM_CONFIG.get("BATCH_INTERVAL_SEC", 1)  # 批次间休息时间

        # 获取当前的异步事件循环句柄，用于在线程池中跑任务
        loop = asyncio.get_running_loop()

        for i, batch in enumerate(batches):
            print(f"\n📦 批次 {i + 1}/{len(batches)} (规模: {len(batch)})")
            batch_matched = []

            # 3. 线程池上下文管理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 使用 loop.run_in_executor 将同步的 _worker 包装成异步任务 (Task)
                # 这样可以利用 await 等待结果，而不会阻塞异步主循环
                tasks = [
                    loop.run_in_executor(executor, self._worker, s)
                    for s in batch
                ]

                # 使用 tqdm 实时展示当前批次的进度
                # asyncio.as_completed(tasks) 保证谁先算完，进度条就先走一步
                pbar = tqdm(asyncio.as_completed(tasks),
                            total=len(tasks),
                            desc=f"进度{i + 1}",
                            ncols=80)

                for task in pbar:
                    res = await task  # 异步等待线程返回结果
                    if res:
                        batch_matched.append(res)
                        # 实时更新进度条右侧显示的累计命中数
                        pbar.set_postfix({"命中": len(batch_matched) + len(self.matched_list)})

            # 汇总当前批次的筛选结果
            self.matched_list.extend(batch_matched)

            # 4. 【非阻塞休息】：防止 CPU 持续满载，给系统“喘息”时间
            # 使用 await asyncio.sleep 而不是 time.sleep，确保异步循环不被挂起
            if i < len(batches) - 1 and interval > 0:
                await asyncio.sleep(interval)

        # 5. 扫描结束，导出 CSV 报表
        self.export_results()

    def export_results(self):
        """
        结果持久化：将命中的信号导出为 CSV 文件。
        """
        if not self.matched_list:
            print("\n🏁 扫描完成，未发现匹配信号。")
            return

        # 1. 整理数据为 DataFrame
        df_res = pd.DataFrame(self.matched_list)

        # 2. 构建保存路径（按日期分文件夹存储）
        date_str = time.strftime('%Y%m%d')
        save_dir = os.path.join(PATH_CONFIG["OUTPUT_FOLDER_BASE"], date_str)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 3. 以时分秒命名文件，防止多次扫描覆盖结果
        file_path = os.path.join(save_dir, f"scan_res_{time.strftime('%H%M%S')}.csv")

        # 4. 写入文件，utf-8-sig 编码支持 Excel 直接打开且不乱码
        df_res.to_csv(file_path, index=False, encoding='utf-8-sig')

        print(f"\n🎉 扫描结束！")
        print(f"📈 累计命中数量: {len(self.matched_list)}")
        print(f"💾 结果文件已保存至: {file_path}")