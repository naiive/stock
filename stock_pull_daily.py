#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股历史数据批量导入 MySQL 脚本 (Bulk Insert Optimized)
功能：并发抓取全市场 A 股日线数据，合并后批量导入，使用全局 DELETE 实现覆盖更新。
============================================================
"""
import os
import json
import time
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, wait, TimeoutError as ThreadingTimeoutError

import pandas as pd
import akshare as ak
import asyncio
from tqdm import tqdm

# --- MySQL 依赖 ---
from sqlalchemy import create_engine, text

# ============================================================
# 模块 1：配置 (Configuration)
# ============================================================
CONFIG = {
    # --- MySQL 配置 ---
    "DB": {
        "host": "localhost",
        "port": 3306,
        "user": "root",  # 请替换
        "password": "Elaiza112233",  # 请替换
        "database": "stock"  # 请替换
    },
    "MYSQL_TABLE": "a_stock_daily",  # 目标表名

    # --- 时间范围 ---
    "DAYS": 500,  # 抓取的历史数据时长

    # --- 过滤条件 ---
    "EXCLUDE_GEM": True,  # 排除创业板（300）
    "EXCLUDE_KCB": True,  # 排除科创板（688）
    "EXCLUDE_BJ": True,   # 排除北交所（8、4）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",  # 复权方式: 'qfq' (前复权)

    # --- 抽样/并发 ---
    "SAMPLE_SIZE": 0,  # 0 或 None 表示全量，>0 表示随机抽样数量
    "MAX_WORKERS": 8,  # 线程数
    "REQUEST_TIMEOUT": 30,  # AkShare 单次请求整体超时保护（秒）
    "CACHE_FILE": "stock_list_cache.json",
}


# ============================================================
# 工具：数据库连接
# ============================================================
def get_db_engine():
    """创建并返回数据库连接引擎"""
    db_conf = CONFIG["DB"]
    url = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}?charset=utf8mb4"
    try:
        engine = create_engine(url, pool_recycle=3600)
        return engine
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        raise


# ============================================================
# 工具：重试装饰器
# ============================================================
def retry(max_retries=3, delay=2):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries - 1:
                        raise
                    time.sleep(delay)

        return wrapper

    return decorator


# ============================================================
# 模块 2：获取/缓存 全市场股票列表
# ============================================================
@retry(max_retries=2, delay=1)
def fetch_stock_list_safe():
    """获取全市场股票列表，采用降级策略以提高稳定性。"""
    try:
        df = ak.stock_info_a_code_name()
        if not df.empty and "code" in df.columns:
            return df[["code", "name"]]
    except Exception:
        pass

    try:
        df = ak.stock_zh_a_spot_em()
        return df[["code", "name"]]
    except Exception as e:
        raise Exception(f"所有股票列表接口均不可用: {e}")


def get_stock_list_manager():
    """缓存管理器：优先读取本地缓存，过期或不存在则联网更新。"""
    cache_file = CONFIG["CACHE_FILE"]
    today_str = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("time") == today_str:
                return pd.DataFrame(cache["data"])
        except Exception:
            pass

    print("[系统] 正在获取全量股票列表...")
    df = fetch_stock_list_safe()

    if not df.empty:
        with open(cache_file, "w", encoding="utf-8") as f:
            data = {
                "time": today_str,
                "data": df.to_dict(orient="records")
            }
            json.dump(data, f, ensure_ascii=False, indent=2)

    return df


def filter_stock_list(df):
    if df is None or df.empty:
        return []
    df["code"] = df["code"].astype(str)
    mask = pd.Series(False, index=df.index)
    if CONFIG["EXCLUDE_GEM"]:
        mask |= df["code"].str.startswith("300")
    if CONFIG["EXCLUDE_KCB"]:
        mask |= df["code"].str.startswith(("688", "689"))
    if CONFIG["EXCLUDE_BJ"]:
        mask |= df["code"].str.startswith(("8", "4", "92"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns:
        mask |= df["name"].str.contains("ST|退", na=False)  # 排除 ST/退市股

    # 构造 AkShare 所需的 symbol 格式 (sh600xxx, sz00xxxx)
    df['symbol'] = df["code"].apply(
        lambda x: f"sh{x}" if x.startswith("6") else f"sz{x}"
    )
    df['symbol'] = df['symbol'].astype(str)  # 确保 symbol 是字符串

    return df[~mask][['code', 'name', 'symbol']].to_dict('records')


# ============================================================
# 模块 3：数据抓取 (仅抓取，不导入)
# ============================================================
def fetch_data_with_timeout(symbol, start_date, end_date, adjust, timeout):
    """
    一个辅助函数，在独立的线程中执行 akshare 请求，并使用 Future/wait 实施超时。
    """

    def _fetch():
        # akshare.stock_zh_a_daily 默认返回 9 列
        return ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_fetch)
        try:
            done, not_done = wait([future], timeout=timeout)
            if future in done:
                return future.result()
            elif future in not_done:
                future.cancel()
                raise ThreadingTimeoutError(f"请求超时 ({timeout}s)")
        except Exception as e:
            raise e


def fetch_data_only(item: dict, start_date: str, end_date: str):
    """
    🎯 核心抓取函数：获取单只股票的日线数据并返回 DataFrame。
    """
    # 🔴 新增：引入随机延时，模拟人类操作，减轻服务器压力
    time.sleep(random.uniform(0.8, 1))  # 随机等待 0.1 到 0.5 秒

    code = item['code']
    symbol = item['symbol']
    name = item['name']
    adjust_type = CONFIG["ADJUST"]

    try:
        # 1. 获取数据 (带超时保护)
        df = fetch_data_with_timeout(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust_type,
            timeout=CONFIG["REQUEST_TIMEOUT"]
        )

        if df is None or df.empty:
            return None

        # 2. 数据清洗与准备 (AkShare 返回 9 列，只保留核心 7 列)
        # 字段顺序: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 换手率
        # 确保 DataFrame 有足够的列数
        if df.shape[1] < 7:
            return None

        df = df.iloc[:, :7]
        df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount']

        # 调整列顺序
        df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']].copy()

        # 添加联合主键需要的字段
        df['code'] = code
        df['adjust'] = adjust_type

        # 转换日期格式
        df['date'] = pd.to_datetime(df['date']).dt.date

        return df

    except ThreadingTimeoutError:
        print(f"[超时] {name} ({code}) 请求超时，跳过。")
        return None

    except Exception as e:
        print(f"[失败] 获取 {name} ({code}) 失败: {e}")
        return None


# ============================================================
# 模块 4：并发调度器 (Async Scheduler)
# ============================================================
async def main_scheduler(target_list):
    """
    主调度器：并发抓取所有数据，并执行批量导入。
    """
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")
    table_name = CONFIG["MYSQL_TABLE"]
    adjust_type = CONFIG["ADJUST"]
    total_stocks = len(target_list)

    print(f"\n[任务启动] 抓取范围: {start_date} ~ {end_date}")
    print(f"[配置] 目标: {total_stocks} 支 | 线程: {CONFIG['MAX_WORKERS']} | 复权: {adjust_type}")

    loop = asyncio.get_running_loop()
    all_results_df = []

    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:
        tasks = [
            loop.run_in_executor(pool, fetch_data_only, item, start_date, end_date)
            for item in target_list
        ]

        # 使用 tqdm 进行进度条显示
        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")

        fetched_count = 0

        for coro in pbar:
            df_single = await coro

            if df_single is not None and not df_single.empty:
                all_results_df.append(df_single)
                fetched_count += 1

            pbar.set_postfix({"成功抓取": fetched_count, "总数": total_stocks})

    if not all_results_df:
        print("\n[警告] 未抓取到任何有效数据，导入终止。")
        return

    # 1. 🟢 批量插入优化步骤 1: 合并所有数据
    final_df = pd.concat(all_results_df, ignore_index=True)

    print(f"\n[导入] 正在准备批量导入 {len(final_df)} 条数据...")

    try:
        engine = get_db_engine()

        # 2. 🟢 批量插入优化步骤 2: 全局删除 (一次性清除所有旧数据)
        with engine.connect() as connection:
            delete_sql = f"DELETE FROM {table_name} WHERE adjust='{adjust_type}'"
            connection.execute(text(delete_sql))
            connection.commit()
            print(f"[导入] 已删除所有旧的 {adjust_type} 历史数据。")

        # 3. 🟢 批量插入优化步骤 3: 一次性导入
        # 使用 chunksize 优化 Pandas 导入性能
        final_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=50000
        )
        print(f"[导入] 批量导入成功。共导入 {len(final_df)} 条记录。")

    except Exception as e:
        print(f"❌ 批量导入失败: {e}")


# ============================================================
# 模块 5：主入口
# ============================================================
def main():
    start_time = time.time()

    # 1. 获取并过滤股票列表
    try:
        df_base = get_stock_list_manager()
    except Exception as e:
        print(f"[终止] 无法获取股票列表: {e}")
        return

    target_list = filter_stock_list(df_base)
    if not target_list:
        print("[终止] 股票列表为空，请检查过滤条件。")
        return

    # 抽样处理
    sample_size = CONFIG["SAMPLE_SIZE"]
    if sample_size > 0 and len(target_list) > sample_size:
        print(f"[抽样模式] 随机抽取 {sample_size} 支股票进行测试...")
        target_list = random.sample(target_list, sample_size)
    else:
        print(f"[全量模式] 扫描所有 {len(target_list)} 支有效股票...")

    # 2. 并发调度执行
    try:
        asyncio.run(main_scheduler(target_list))
    except Exception as e:
        print(f"\n❌ 主调度器运行出错: {e}")

    print("\n" + "=" * 60)
    print(f"✅ 历史数据导入任务完成 | 总耗时: {time.time() - start_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()