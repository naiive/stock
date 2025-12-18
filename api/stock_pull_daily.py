#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股历史数据批量导入 MySQL 脚本（Upsert 优化版 - 附带 Logging 和动态目录）
说明：
- 日志文件将保存到当前目录下的 stocks/YYYYMMDD 文件夹中。
- 核心功能改为基于主键的 **更新或插入 (Upsert)**。
============================================================
"""

import os
import json
import time
import random
import math
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import asyncio

import pandas as pd
import akshare as ak
from tqdm import tqdm

from sqlalchemy import create_engine, text

import conf.config as conf

# ------------------- 动态日志目录配置 -------------------

# 1. 确定日期和基础目录
CURRENT_DATE_STR = datetime.now().strftime("%Y%m%d")
LOG_BASE_DIR = "../logs"
LOG_DAILY_DIR = os.path.join(LOG_BASE_DIR, CURRENT_DATE_STR)

# 2. 创建目录 (如果不存在)
# exist_ok=True 确保如果目录已存在，不会报错
try:
    os.makedirs(LOG_DAILY_DIR, exist_ok=True)
except Exception as e:
    # 如果创建目录失败（权限等问题），则退回到当前目录
    print(f"警告：无法创建日志目录 {LOG_DAILY_DIR}。日志将保存到当前目录。错误: {e}")
    LOG_DAILY_DIR = ".."

LOG_FILE = os.path.join(LOG_DAILY_DIR, "stock_data_pull.log")
# ------------------- /动态日志目录配置 -------------------


# ------------------- 日志配置 -------------------
# 获取并配置 Logger
logger = logging.getLogger('StockPullLogger')
logger.setLevel(logging.INFO)

# 创建文件处理器 (File Handler)
fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
fh.setLevel(logging.INFO)

# 创建控制台处理器 (Stream Handler)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# 定义输出格式
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
)
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# 添加处理器到 Logger
# 使用 len(logger.handlers) 检查，防止重复添加 Handler
if not logger.handlers:
    logger.addHandler(fh)
    logger.addHandler(ch)
# ------------------- /日志配置 -------------------


# ------------------- 配置 -------------------
CONFIG = {
    # MySQL
    "DB": conf.DB_CONFIG,

    "MYSQL_TABLE": conf.MYSQL_TABLE,

    # !!! 数据库要求：目标表 a_stock_daily 必须设置 (date, code, adjust) 为联合主键。

    # 抓取范围控制（优先级从高到低）
    "TARGET_STOCKS": [],      # 优先级最高：指定需要更新的股票代码列表["600519", "600520"]。空列表 [] 表示全量。
    "TARGET_START_DATE": "",  # 优先级次之：指定开始日期，格式 "YYYYMMDD"。
    "TARGET_END_DATE": "",    # 优先级次之：指定结束日期，格式 "YYYYMMDD"。
    "DAYS": 1,                # 优先级最低：如果 TARGET_START_DATE 为空，则抓取最近 DAYS 天的数据。

    # 过滤
    "EXCLUDE_GEM": True,  # 排除创业板（300、301）
    "EXCLUDE_KCB": True,  # 排除科创板（688）
    "EXCLUDE_BJ": True,  # 排除北交所（8、4、92）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",  # 'qfq' / 'hfq' / None

    # 并发与超时
    "MAX_WORKERS": 6,       # 建议 2~4 更稳
    "REQUEST_TIMEOUT": 28,  # 单次 akshare 请求超时（秒）
    "CACHE_FILE": "../conf/stock_list_cache.json",

    # 重试策略（fetch_data_only 内部）
    "RETRY_TIMES": 2,
    "RETRY_BACKOFF_BASE": 1.6,  # 指数退避基数
}


# ------------------- /配置 -------------------


# ------------------- 数据库连接 -------------------
def get_db_engine():
    db_conf = CONFIG["DB"]
    url = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}?charset=utf8mb4&local_infile=1"
    engine = create_engine(url, pool_recycle=3600)
    return engine


# ------------------- MySQL Upsert 方法定义 (最终且兼容命名参数版) -------------------
def mysql_upsert_method(table, conn, keys, data_iter):
    """
    Pandas to_sql 自定义方法，实现 MySQL 的 ON DUPLICATE KEY UPDATE (Upsert)
    强制使用命名参数 (字典列表) 兼容 PyMySQL 的批量执行要求。
    """

    # 1. 将行数据转换为字典列表（命名参数格式）
    data = [dict(zip(keys, row)) for row in data_iter]

    # 2. 构建 INSERT 语句 (使用命名占位符)
    cols = ", ".join([f"`{col}`" for col in keys])  # 列名用反引号包裹
    named_placeholders = ", ".join([f":{col}" for col in keys])
    insert_sql = f"INSERT INTO {table.name} ({cols}) VALUES ({named_placeholders})"

    # 3. 构建 ON DUPLICATE KEY UPDATE 语句
    primary_keys = ['date', 'code', 'adjust']
    update_cols = [col for col in keys if col not in primary_keys]

    if not update_cols:
        update_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']

    # ON DUPLICATE KEY UPDATE 仍然引用 VALUES()
    update_parts = [f"`{col}`=VALUES(`{col}`)" for col in update_cols]
    update_sql = "ON DUPLICATE KEY UPDATE " + ", ".join(update_parts)

    final_sql = insert_sql + " " + update_sql

    # 4. 准备参数列表
    params = data

    # 5. 执行批量操作
    try:
        conn.execute(text(final_sql), params)
    except Exception as e:
        logger.error(f"批量 Upsert 失败，请检查主键或数据格式。", exc_info=True)
        # 打印调试信息到日志
        logger.debug(f"SQL: {final_sql}")
        if params:
            logger.debug(f"First Row Parameters: {params[0]}")
        raise  # 重新抛出异常


# ------------------- 重试装饰器（同步） -------------------
def sync_retry(max_retries=2, backoff_base=1.6):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt == max_retries:
                        stock_item = args[0]
                        logger.error(f"股票 {stock_item.get('code', 'N/A')} - 最终重试失败。", exc_info=False)
                        raise
                    sleep_for = (backoff_base ** (attempt - 1)) + random.uniform(0, 0.5)
                    logger.warning(
                        f"股票 {args[0].get('code', 'N/A')} 尝试 {attempt} 失败: {e}，等待 {sleep_for:.2f}s 后重试...")
                    time.sleep(sleep_for)
            raise last_exc

        return wrapper

    return decorator


# ------------------- 获取/缓存全市场股票列表 -------------------
@sync_retry(max_retries=2, backoff_base=1.5)
def fetch_stock_list_safe():
    """尝试多接口获取股票列表"""
    try:
        df = ak.stock_info_a_code_name()
        if not df.empty and "code" in df.columns:
            return df[["code", "name"]]
    except Exception:
        pass

    try:
        df = ak.stock_zh_a_spot_em()
        if "code" in df.columns and "name" in df.columns:
            return df[["code", "name"]]
        if "代码" in df.columns and "名称" in df.columns:
            df = df.rename(columns={"代码": "code", "名称": "name"})
            return df[["code", "name"]]
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}", exc_info=True)
        raise Exception(f"获取股票列表失败: {e}")

    raise Exception("未能从任何接口获取到股票列表")


def get_stock_list_manager():
    cache_file = CONFIG["CACHE_FILE"]
    today_str = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("time") == today_str:
                logger.info("[系统] 从缓存加载全量股票列表。")
                return pd.DataFrame(cache["data"])
        except Exception:
            logger.warning("[系统] 缓存文件损坏或读取失败，将重新获取。")
            pass

    logger.info("[系统] 正在获取全量股票列表...")
    df = fetch_stock_list_safe()
    if df is None or df.empty:
        raise Exception("股票列表为空")

    with open(cache_file, "w", encoding="utf-8") as f:
        data = {
            "time": today_str,
            "data": df.to_dict(orient="records")
        }
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"[系统] 成功获取并缓存 {len(df)} 支股票列表。")

    return df


def filter_stock_list(df):
    if df is None or df.empty:
        return []
    df["code"] = df["code"].astype(str)

    target_stocks = CONFIG["TARGET_STOCKS"]
    if target_stocks:
        logger.info(f"[过滤] 仅抓取 TARGET_STOCKS 中指定的 {len(target_stocks)} 支股票。")
        df = df[df["code"].isin([str(c) for c in target_stocks])]

    mask = pd.Series(False, index=df.index)
    if CONFIG["EXCLUDE_GEM"]:
        mask |= df["code"].str.startswith(("300", "301"))
    if CONFIG["EXCLUDE_KCB"]:
        mask |= df["code"].str.startswith(("688", "689"))
    if CONFIG["EXCLUDE_BJ"]:
        mask |= df["code"].str.startswith(("8", "4", "92"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns:
        mask |= df["name"].str.contains("ST|退", na=False)

    df_filtered = df[~mask].copy()

    df_filtered['symbol'] = df_filtered["code"].apply(lambda x: f"sh{x}" if x.startswith("6") else f"sz{x}")
    df_filtered['symbol'] = df_filtered['symbol'].astype(str)

    return df_filtered[['code', 'name', 'symbol']].to_dict('records')


# ------------------- 数据抓取（同步函数） -------------------
@sync_retry(max_retries=CONFIG["RETRY_TIMES"], backoff_base=CONFIG["RETRY_BACKOFF_BASE"])
def fetch_data_only_sync(item: dict, start_date: str, end_date: str):
    """同步抓取单只股票（内部带重试）"""
    time.sleep(random.uniform(0.6, 1.2))

    code = item['code']
    symbol = item['symbol']
    adjust_type = CONFIG["ADJUST"]

    df = ak.stock_zh_a_daily(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust_type
    )

    if df is None or df.empty:
        raise Exception(f"接口返回空或无数据: {symbol}")

    if df.shape[1] < 5:
        raise Exception("返回列数过少")

    df = df.iloc[:, :7]
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']

    df['code'] = code
    df['adjust'] = adjust_type
    df['date'] = pd.to_datetime(df['date']).dt.date

    df = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjust']]

    return df


# ------------------- 异步调度器 -------------------
async def main_scheduler(target_list):
    # 根据配置计算最终的 start_date 和 end_date
    if CONFIG["TARGET_START_DATE"] and CONFIG["TARGET_END_DATE"]:
        start_date = CONFIG["TARGET_START_DATE"]
        end_date = CONFIG["TARGET_END_DATE"]
        logger.info(f"[范围] 使用用户指定日期: {start_date} ~ {end_date}")
    else:
        end_date = datetime.now().strftime("%Y%m%d")
        days_to_subtract = max(0, CONFIG["DAYS"] - 1)
        start_date = (datetime.now() - timedelta(days=days_to_subtract)).strftime("%Y%m%d")
        logger.info(f"[范围] 基于 DAYS={CONFIG['DAYS']} 计算的范围: {start_date} ~ {end_date}")

    table_name = CONFIG["MYSQL_TABLE"]
    total_stocks = len(target_list)

    logger.info(f"\n[任务启动] 抓取范围: {start_date} ~ {end_date}")
    logger.info(
        f"[配置] 目标: {total_stocks} 支 | 并发上限: {CONFIG['MAX_WORKERS']} | 单次超时: {CONFIG['REQUEST_TIMEOUT']}s")

    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(CONFIG["MAX_WORKERS"])
    all_results = []
    failed_items = []

    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:

        async def fetch_with_limit(item):
            async with sem:
                try:
                    coro = loop.run_in_executor(pool, fetch_data_only_sync, item, start_date, end_date)
                    df = await asyncio.wait_for(coro, timeout=CONFIG["REQUEST_TIMEOUT"])
                    logger.debug(f"[{item['code']}] 成功抓取 {len(df)} 条数据。")
                    return df
                except asyncio.TimeoutError:
                    logger.warning(f"[{item['code']}] 超时 ({CONFIG['REQUEST_TIMEOUT']}s)。")
                    return ("timeout", item)
                except Exception as e:
                    logger.error(f"[{item['code']}] 抓取失败: {e}", exc_info=False)
                    return ("error", item, str(e))

        tasks = [asyncio.create_task(fetch_with_limit(item)) for item in target_list]
        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")
        success_count = 0

        for coro in pbar:
            res = await coro
            if isinstance(res, tuple):
                tag = res[0]
                if tag in ("timeout", "error"):
                    failed_items.append(res[1])
            elif res is not None:
                all_results.append(res)
                success_count += 1

            pbar.set_postfix({"成功抓取": success_count, "总数": total_stocks, "失败待重试": len(failed_items)})

    # 第一轮完成
    logger.info(f"\n[第一轮完成] 成功 {len(all_results)} / {total_stocks}，失败 {len(failed_items)}。")

    # 低并发重试
    if failed_items:
        retry_results = []
        retry_workers = min(2, max(1, CONFIG["MAX_WORKERS"] // 2))
        logger.info(f"[重试] 对 {len(failed_items)} 支股票进行低并发重试（并发 {retry_workers}）...")

        async def retry_run(item):
            async with asyncio.Semaphore(retry_workers):
                try:
                    with ThreadPoolExecutor(max_workers=1) as temp_pool:
                        coro = loop.run_in_executor(temp_pool, fetch_data_only_sync, item, start_date, end_date)
                        df = await asyncio.wait_for(coro, timeout=max(CONFIG["REQUEST_TIMEOUT"] * 1.2, 30))
                        logger.info(f"[{item['code']}] 重试成功补抓。")
                        return df
                except Exception as e:
                    logger.warning(f"[{item['code']}] 重试再次失败并放弃: {e}", exc_info=False)
                    return None

        retry_tasks = [asyncio.create_task(retry_run(it)) for it in failed_items]
        for r in tqdm(asyncio.as_completed(retry_tasks), total=len(retry_tasks), unit="retry"):
            df = await r
            if df is not None:
                retry_results.append(df)

        logger.info(f"[重试结果] 成功补抓 {len(retry_results)} 条")
        all_results.extend(retry_results)

    if not all_results:
        logger.warning("\n[警告] 未抓取到任何有效数据，导入终止。")
        return

    # 合并并导入数据库
    final_df = pd.concat(all_results, ignore_index=True)
    logger.info(f"\n[导入] 准备批量 Upsert {len(final_df)} 条数据到表 {table_name} ...")

    try:
        engine = get_db_engine()
        final_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=5000,
            method=mysql_upsert_method
        )
        logger.info(f"[导入] 批量 Upsert 成功。共处理 {len(final_df)} 条记录。")

    except Exception as e:
        logger.critical(f"❌ 任务终止：批量 Upsert 失败。")
        print(f"❌ 批量 Upsert 失败: {e}")
        print("请确保 MySQL 表已设置 (date, code, adjust) 为联合主键！")


# ------------------- 主入口 -------------------
def main():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("📢 历史数据导入任务启动")
    logger.info(f"日志文件路径: {os.path.abspath(LOG_FILE)}")
    logger.info("=" * 60)

    try:
        df_base = get_stock_list_manager()
    except Exception as e:
        logger.critical(f"[终止] 无法获取股票列表: {e}", exc_info=True)
        return

    target_list = filter_stock_list(df_base)
    if not target_list:
        logger.warning("[终止] 股票列表为空，请检查过滤条件或 TARGET_STOCKS。")
        return

    try:
        asyncio.run(main_scheduler(target_list))
    except Exception as e:
        logger.critical(f"\n❌ 主调度器运行出错: {e}", exc_info=True)

    elapsed_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"✅ 历史数据 Upsert 任务完成 | 总耗时: {elapsed_time:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":

    # 有数据就执行 main，没有数就等有数据后在执行
    # 一般16:00开始有数据
    if ak.stock_zh_a_daily(
        symbol="sh600519",
        start_date= CONFIG["TARGET_START_DATE"],
        end_date= CONFIG["TARGET_END_DATE"],
        adjust="qfq"
    ).empty:
        date = CONFIG["TARGET_START_DATE"]
        logger.warning(f"✅今天 {date}还没数据，等【ak.stock_zh_a_daily】接口有数据在执行！")
    else:
        main()