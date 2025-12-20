# -*- coding: utf-8 -*-
import time
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # <--- 导入进度条库
from core.data_handler import DataHandler
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator
from conf.config import SYSTEM_CONFIG, PATH_CONFIG


class MarketScanner:
    def __init__(self):
        self.handler = DataHandler()
        self.matched_list = []

    def _worker(self, symbol):
        """单只股票的扫描逻辑"""
        try:
            df = self.handler.get_full_data(symbol)
            if df is None or len(df) < 35:
                return None

            df = squeeze_momentum_indicator(df)
            if df.empty: return None

            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]

            # 策略信号：SQZ释放
            if last_row['sqz_status'] == 'OFF' and prev_row['sqz_status'] == 'ON':
                return {
                    "代码": symbol,
                    "最新价": last_row['close'],
                    "动能值": round(last_row['sqz_hvalue'], 4),
                    "扫描时间": time.strftime("%H:%M:%S")
                }
        except Exception:
            return None
        return None

    def run_full_scan(self, symbols=None):
        if symbols is None:
            symbols = self.handler.get_target_list()

        if not symbols:
            print("❌ 扫描终止：待处理名单为空。")
            return

        # 1. 预取实时快照
        self.handler.prepare_realtime_data()

        # 2. 分批
        batch_size = SYSTEM_CONFIG.get("BATCH_SIZE", 500)
        batches = list(self.handler.chunk_symbols(symbols, batch_size))

        print(f"✅ 扫描准备就绪，共 {len(symbols)} 只，分为 {len(batches)} 批次。")

        max_workers = SYSTEM_CONFIG.get("MAX_WORKERS", 10)
        interval = SYSTEM_CONFIG.get("BATCH_INTERVAL_SEC", 2)

        # 3. 执行多线程并发扫描
        for i, batch in enumerate(batches):
            print(f"\n📦 正在处理第 {i + 1}/{len(batches)} 批 (规模: {len(batch)})...")

            batch_matched = []

            # 使用 as_completed 配合 tqdm 显示进度条
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_symbol = {executor.submit(self._worker, s): s for s in batch}

                # tqdm 装饰器：total 为本批次总数，desc 为说明文字，leave=False 结束后自动清理
                pbar = tqdm(as_completed(future_to_symbol),
                            total=len(batch),
                            desc=f"批次{i + 1}进度",
                            unit="stock",
                            ncols=80)

                for future in pbar:
                    res = future.result()
                    if res:
                        batch_matched.append(res)
                        # 在进度条右侧动态显示命中数量
                        pbar.set_postfix({"命中": len(batch_matched) + len(self.matched_list)})

            self.matched_list.extend(batch_matched)

            # 批次间隔
            if i < len(batches) - 1 and interval > 0:
                time.sleep(interval)

        # 4. 导出结果
        self.export_results()

    def export_results(self):
        # ... (保持之前的导出代码不变)
        if not self.matched_list:
            print("\n🏁 扫描完成，未发现匹配信号。")
            return

        df_res = pd.DataFrame(self.matched_list)
        date_str = time.strftime('%Y%m%d')
        save_dir = os.path.join(PATH_CONFIG["OUTPUT_FOLDER_BASE"], date_str)
        if not os.path.exists(save_dir): os.makedirs(save_dir)
        file_path = os.path.join(save_dir, f"scan_res_{time.strftime('%H%M%S')}.csv")
        df_res.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"\n🎉 扫描结束！发现信号: {len(self.matched_list)} 条 | 存至: {file_path}")