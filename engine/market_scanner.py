# -*- coding: utf-8 -*-
import time
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # 导入进度条库，用于实时显示扫描进度
from core.data_handler import DataHandler
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator
from conf.config import SYSTEM_CONFIG, PATH_CONFIG


class MarketScanner:
    def __init__(self):
        # 初始化数据处理器，负责数据库连接和实时数据拼接
        self.handler = DataHandler()
        # 用于存储所有批次扫描到的符合策略的股票结果
        self.matched_list = []

    def _worker(self, symbol):
        """
        核心工作函数：单只股票的扫描逻辑。
        由线程池调用，实现并发执行。
        """
        try:
            # 1. 获取完整数据：内部自动判断是否需要追加今日实时 K 线
            df = self.handler.get_full_data(symbol)

            # 2. 预检过滤：如果数据量太少（不足以计算长周期指标），直接跳过
            # 提示：在此处加入 price > 5 或 MA200 过滤，可以极大地减少后面的计算量
            if df is None or len(df) < 35:
                return None

            # 3. 计算技术指标：调用你指定的 SQZ 指标函数
            df = squeeze_momentum_indicator(df)
            if df.empty:
                return None

            # 4. 获取最新的两行数据，用于判断“拐点”或“突破”
            last_row = df.iloc[-1]  # 当前时刻（可能是实时）
            prev_row = df.iloc[-2]  # 前一交易日

            # 5. 策略信号判断逻辑
            # 示例策略：SQZ 状态从“ON(挤压)”变为“OFF(释放)”
            if last_row['sqz_status'] == 'OFF' and prev_row['sqz_status'] == 'ON':
                # 如果符合条件，返回结果字典
                return {
                    "代码": symbol,
                    "最新价": last_row['close'],
                    "动能值": round(last_row['sqz_hvalue'], 4),
                    "扫描时间": time.strftime("%H:%M:%S")
                }
        except Exception as e:
            # 异常捕获：确保单只股票报错不会导致整个程序崩溃
            # 错误信息会被 LogRedirector 捕获并写入日志
            # print(f"解析 {symbol} 出错: {e}")
            return None
        return None

    def run_full_scan(self, symbols=None):
        """
        扫描主入口：支持全量扫描或传入特定列表（抽样模式）。
        """
        # 如果 main.py 没有传入抽样列表，则默认去数据库加载全量名单
        if symbols is None:
            symbols = self.handler.get_target_list()

        if not symbols:
            print("❌ 扫描终止：待处理名单为空。")
            return

        # 1. 预取实时快照：一次性拉取全市场数据，存入内存供 _worker 查询，避免重复请求
        self.handler.prepare_realtime_data()

        # 2. 名单分批：根据配置将几千只票切成若干个 Batch
        batch_size = SYSTEM_CONFIG.get("BATCH_SIZE", 500)
        batches = list(self.handler.chunk_symbols(symbols, batch_size))

        print(f"✅ 扫描准备就绪，共 {len(symbols)} 只，分为 {len(batches)} 批次。")

        # 读取并发线程数和批次间隔时间
        max_workers = SYSTEM_CONFIG.get("MAX_WORKERS", 10)
        interval = SYSTEM_CONFIG.get("BATCH_INTERVAL_SEC", 2)

        # 3. 循环处理每一个批次
        for i, batch in enumerate(batches):
            print(f"\n📦 正在处理第 {i + 1}/{len(batches)} 批 (规模: {len(batch)})...")

            batch_matched = []

            # 开启线程池并行处理本批次内的股票
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务到线程池：future -> symbol 的映射
                future_to_symbol = {executor.submit(self._worker, s): s for s in batch}

                # 配置进度条
                # as_completed(future_to_symbol): 谁先算完谁就先返回，让进度条动起来
                pbar = tqdm(as_completed(future_to_symbol),
                            total=len(batch),
                            desc=f"批次{i + 1}进度",
                            unit="stock",
                            ncols=80)

                for future in pbar:
                    res = future.result()  # 获取 _worker 的返回值
                    if res:
                        batch_matched.append(res)
                        # 在进度条右侧实时更新累计命中数量
                        pbar.set_postfix({"命中": len(batch_matched) + len(self.matched_list)})

            # 将本批次结果汇总
            self.matched_list.extend(batch_matched)

            # 批次间强制休息：给数据库连接池和 API 接口“喘息”机会，防止并发过高被封
            if i < len(batches) - 1 and interval > 0:
                time.sleep(interval)

        # 4. 扫描结束，导出结果文件
        self.export_results()

    def export_results(self):
        """
        结果导出：自动创建日期文件夹并保存 CSV 文件。
        """
        if not self.matched_list:
            print("\n🏁 扫描完成，未发现匹配信号。")
            return

        # 转换为 DataFrame 方便保存
        df_res = pd.DataFrame(self.matched_list)

        # 路径处理：stocks/YYYYMMDD/scan_res_HHMMSS.csv
        date_str = time.strftime('%Y%m%d')
        save_dir = os.path.join(PATH_CONFIG["OUTPUT_FOLDER_BASE"], date_str)

        # 如果文件夹不存在则递归创建
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        file_path = os.path.join(save_dir, f"scan_res_{time.strftime('%H%M%S')}.csv")

        # 导出 CSV，使用 utf-8-sig 确保 Excel 打开中文不乱码
        df_res.to_csv(file_path, index=False, encoding='utf-8-sig')

        print(f"\n🎉 扫描结束！")
        print(f"📊 累计命中数量: {len(self.matched_list)} 条")
        print(f"💾 结果文件位置: {file_path}")