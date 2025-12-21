# -*- coding: utf-8 -*-
import asyncio
import random
from core.utils.decorator import timer
from engine.market_scanner import MarketScanner
from core.data_manager import StockListManager
from core.data_client.mysql_client import MySQLClient
from core.utils.logger import LogRedirector
from conf.config import SYSTEM_CONFIG, PATH_CONFIG

async def start_app():
    """异步业务主逻辑"""
    # 1. 初始化
    db = MySQLClient()
    manager = StockListManager(db)

    # 2. 获取名单
    symbols_df = manager.get_stock_list()
    if symbols_df is None or symbols_df.empty:
        print("❌ 错误：无法获取股票列表")
        return
    all_codes = symbols_df['code'].tolist()

    # 3. 模式判断
    sample_size = SYSTEM_CONFIG.get("SAMPLE_SIZE")
    if sample_size and isinstance(sample_size, int) and sample_size > 0:
        target_symbols = random.sample(all_codes, min(sample_size, len(all_codes)))
        print(f"🧪 [模式] 测试模式 (SAMPLE_SIZE={sample_size})")
    else:
        target_symbols = all_codes
        print(f"🚀 [模式] 全量扫描模式")

    # 4. 执行异步扫描
    scanner = MarketScanner()
    await scanner.run_full_scan(target_symbols)

@timer
def main():
    log_dir = PATH_CONFIG.get("OUTPUT_LOG", "logs")

    with LogRedirector(log_folder=log_dir):
        print(f"{'=' * 50}\n🚀 量化交易全市场扫描系统 v2.0 (Async)\n{'=' * 50}")
        try:
            # 启动异步事件循环
            asyncio.run(start_app())
        except KeyboardInterrupt:
            print("\n🛑 用户手动停止")
        except Exception as e:
            print(f"\n❌ 系统异常: {e}")

if __name__ == "__main__":
    main()