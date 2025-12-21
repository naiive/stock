# -*- coding: utf-8 -*-
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
    def __init__(self):
        self.handler = DataHandler()
        self.matched_list = []

    def _worker(self, symbol):
        """
        同步计算逻辑，由线程池驱动

        *** 策略计算 ***
        *** 策略计算 ***
        *** 策略计算 ***
        """
        df = self.handler.get_full_data(symbol)
        # 确保 strategy 内部逻辑已经适配最新的参数
        return run_breakout_strategy(df, symbol)

    async def run_full_scan(self, symbols=None):
        """异步扫描主入口"""
        if symbols is None:
            symbols = self.handler.get_target_list()

        if not symbols:
            print("❌ 扫描终止：待处理名单为空。")
            return

        # 1. 预取实时快照 (同步方法，一次性拉取)
        self.handler.prepare_realtime_data()

        batch_size = SYSTEM_CONFIG.get("BATCH_SIZE", 500)
        batches = list(self.handler.chunk_symbols(symbols, batch_size))
        print(f"✅ 异步扫描就绪，共 {len(symbols)} 只，分 {len(batches)} 批。")

        max_workers = SYSTEM_CONFIG.get("MAX_WORKERS", 10)
        interval = SYSTEM_CONFIG.get("BATCH_INTERVAL_SEC", 1)

        # 获取当前异步事件循环
        loop = asyncio.get_running_loop()

        for i, batch in enumerate(batches):
            print(f"\n📦 批次 {i + 1}/{len(batches)} (规模: {len(batch)})")
            batch_matched = []

            # 2. 线程池配合异步 run_in_executor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务到线程池
                tasks = [
                    loop.run_in_executor(executor, self._worker, s)
                    for s in batch
                ]

                # 使用 tqdm 配合 as_completed 展示进度
                pbar = tqdm(asyncio.as_completed(tasks),
                            total=len(tasks),
                            desc=f"进度{i + 1}",
                            ncols=80)

                for task in pbar:
                    res = await task  # 异步等待线程结果
                    if res:
                        batch_matched.append(res)
                        pbar.set_postfix({"命中": len(batch_matched) + len(self.matched_list)})

            self.matched_list.extend(batch_matched)

            # 3. 异步非阻塞休息
            if i < len(batches) - 1 and interval > 0:
                await asyncio.sleep(interval)

        # 4. 导出结果
        self.export_results()

    def export_results(self):
        """(保持原样)"""
        if not self.matched_list:
            print("\n🏁 扫描完成，未发现匹配信号。")
            return
        df_res = pd.DataFrame(self.matched_list)
        date_str = time.strftime('%Y%m%d')
        save_dir = os.path.join(PATH_CONFIG["OUTPUT_FOLDER_BASE"], date_str)
        if not os.path.exists(save_dir): os.makedirs(save_dir)
        file_path = os.path.join(save_dir, f"scan_res_{time.strftime('%H%M%S')}.csv")
        df_res.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"\n🎉 扫描结束！命中: {len(self.matched_list)} | 文件: {file_path}")