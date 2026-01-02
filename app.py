# -*- coding: utf-8 -*-
"""
Module: app.py
Description: 量化交易系统的启动入口。
职责：
1. 建立基础环境（日志、计时、异常捕获）。
2. 初始化数据库连接与基础名单。
3. 根据配置决定是“抽样测试”还是“全量扫描”。
4. 启动 asyncio 异步回路并点火执行扫描引擎。
"""

import asyncio
import random
from core.utils.decorator import timer
from engine.market_scanner import MarketScanner
from core.data_manager import StockListManager
from core.data_client.mysql_client import MySQLClient
from core.utils.logger import LogRedirector
from conf.config import SYSTEM_CONFIG, PATH_CONFIG

async def start_app():
    """
    异步业务主逻辑 (协程)
    """
    # 1. 初始化数据库连接
    db = MySQLClient()

    # 2. 初始化名单管理器 会根据数据库中的基础表，筛选出当前上市、非停牌、非ST的股票池
    manager = StockListManager(db)

    # 3. 从数据库获取初始股票名单 (DataFrame 格式)
    symbols_df = manager.get_stock_list()
    if symbols_df is None or symbols_df.empty:
        print("❌ 错误：无法从数据库获取股票列表，请检查网络或数据库配置。")
        return

    all_codes = symbols_df['code'].tolist()

    # 4. 模式判断
    sample_size = SYSTEM_CONFIG.get("SAMPLE_SIZE")

    if sample_size and isinstance(sample_size, int) and sample_size > 0:
        target_symbols = random.sample(all_codes, min(sample_size, len(all_codes)))
        print(f"🧪 [模式] 测试模式开启 (SAMPLE_SIZE={sample_size})")
    else:
        # 全量扫描：处理所有符合条件的 A 股（约 5000+ 支）
        target_symbols = all_codes
        print(f"🚀 [模式] 全量扫描模式启动")

    # 5. 实例化扫描引擎 此时会联动初始化 DataHandler，准备好后续的实时快照功能
    scanner = MarketScanner()

    # 6. await 会挂起 start_app，直到 run_full_scan 完成所有批次的扫描、计算并导出文件
    await scanner.run_full_scan(target_symbols)

@timer
def main():
    """
    负责系统环境的包裹（Wrapper）和异常的最外层捕获
    """
    log_dir = PATH_CONFIG.get("OUTPUT_LOG", "logs")

    with LogRedirector(log_folder=log_dir):
        print(f"{'=' * 50}\n🚀 Naive扫描器\n{'=' * 50}")

        try:
            # 【核心启动命令】
            # asyncio.run() 是 Python 3.7+ 引入的入口，它负责：
            # 1. 创建全新的事件循环 (Event Loop)。
            # 2. 运行传入的协程 (start_app)。
            # 3. 在运行结束后关闭事件循环。
            asyncio.run(start_app())

        except KeyboardInterrupt:
            print("\n🛑 [停止] 用户手动中断了程序运行。")
        except Exception as e:
            print(f"\n❌ [崩溃] 系统发生严重异常: {e}")

if __name__ == "__main__":
    main()