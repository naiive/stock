#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股历史数据批量导入 MySQL 脚本（Upsert 优化版）
说明：
- 核心功能改为基于主键的 **更新或插入 (Upsert)**，不再是先删除后插入。
- 使用 pandas to_sql + MySQL ON DUPLICATE KEY UPDATE 实现高效批量更新。
- 支持通过 DAYS、TARGET_START_DATE/TARGET_END_DATE 和 TARGET_STOCKS 控制抓取范围。
- 沿用单层线程池 + asyncio.Semaphore + asyncio.wait_for 控制并发和超时。
- 沿用同步重试（指数退避）机制。
============================================================
"""

import os
import json
import time
import random
import math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import asyncio

import pandas as pd
import akshare as ak
from tqdm import tqdm

from sqlalchemy import create_engine, text

# ------------------- 配置 -------------------
CONFIG = {
    # MySQL
    "DB": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "Elaiza112233",
        "database": "stock"
    },

    "MYSQL_TABLE": "a_stock_daily",

    # !!! 数据库要求：目标表 a_stock_daily 必须设置 (date, code, adjust) 为联合主键，
    # !!! 否则 to_sql 的 method 参数将无法实现 Upsert。

    # 抓取范围控制（优先级从高到低）
    "TARGET_STOCKS": [],  # 优先级最高：指定需要更新的股票代码列表，例如 ["600000", "000001"]。空列表 [] 表示全量。
    "TARGET_START_DATE": "",  # 优先级次之：指定开始日期，格式 "YYYYMMDD"，例如 "20240101"。非空时覆盖 DAYS。
    "TARGET_END_DATE": "",  # 优先级次之：指定结束日期，格式 "YYYYMMDD"，例如 "20241231"。
    "DAYS": 1,  # 优先级最低：如果 TARGET_START_DATE 为空，则抓取最近 DAYS 天的数据。

    # 过滤
    "EXCLUDE_GEM": True,  # 排除创业板（300、301）
    "EXCLUDE_KCB": True,  # 排除科创板（688）
    "EXCLUDE_BJ": True,  # 排除北交所（8、4、92）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",  # 'qfq' / 'hfq' / None

    # 并发与超时
    "MAX_WORKERS": 6,  # 建议 2~4 更稳
    "REQUEST_TIMEOUT": 28,  # 单次 akshare 请求超时（秒），由 asyncio.wait_for 控制
    "SAMPLE_SIZE": 0,  # 0 表示全量
    "CACHE_FILE": "stock_list_cache.json",

    # 重试策略（fetch_data_only 内部）
    "RETRY_TIMES": 2,
    "RETRY_BACKOFF_BASE": 1.6,  # 指数退避基数
}
# ------------------- /配置 -------------------


# ------------------- 数据库连接 -------------------
def get_db_engine():
    db_conf = CONFIG["DB"]
    # 添加 local_infile=1 兼容性参数，虽然不直接用于 Upsert，但对某些环境有用
    url = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}?charset=utf8mb4&local_infile=1"
    # 增加连接池回收，防止连接超时
    engine = create_engine(url, pool_recycle=3600)
    return engine


# ------------------- MySQL Upsert 方法定义 (最终且兼容命名参数版) -------------------
def mysql_upsert_method(table, conn, keys, data_iter):
    """
    Pandas to_sql 自定义方法，实现 MySQL 的 ON DUPLICATE KEY UPDATE (Upsert)
    强制使用命名参数 (字典列表) 兼容 PyMySQL 的批量执行要求。
    """

    # 1. 将行数据转换为字典列表（命名参数格式）
    # keys 是列名列表，data_iter 是行数据元组的迭代器
    data = [dict(zip(keys, row)) for row in data_iter]

    # 2. 构建 INSERT 语句 (使用命名占位符)
    cols = ", ".join(keys)
    # 命名占位符，例如 :date, :code, :open
    named_placeholders = ", ".join([f":{col}" for col in keys])
    insert_sql = f"INSERT INTO {table.name} ({cols}) VALUES ({named_placeholders})"

    # 3. 构建 ON DUPLICATE KEY UPDATE 语句
    primary_keys = ['date', 'code', 'adjust']

    # 筛选出需要更新的非主键列 (必须是 keys 中包含的列)
    update_cols = [col for col in keys if col not in primary_keys]

    # 确保至少有交易数据列被更新
    if not update_cols:
        update_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']

    # ON DUPLICATE KEY UPDATE 仍然引用 VALUES()
    update_parts = [f"`{col}`=VALUES(`{col}`)" for col in update_cols]
    update_sql = "ON DUPLICATE KEY UPDATE " + ", ".join(update_parts)

    final_sql = insert_sql + " " + update_sql

    # 4. 准备参数列表
    # params 已经是字典列表
    params = data

    # 5. 执行批量操作
    try:
        # 使用 conn.execute(text(sql), params) 传递字典列表
        conn.execute(text(final_sql), params)
    except Exception as e:
        # 打印调试信息
        print(f"\n[DEBUG - NOMINAL] SQL: {final_sql}")
        if params:
            print(f"[DEBUG - NOMINAL] First Row Parameters: {params[0]}")
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
                        # 最后一次失败，抛出异常
                        raise
                    # 指数退避 + 随机抖动
                    sleep_for = (backoff_base ** (attempt - 1)) + random.uniform(0, 0.5)
                    time.sleep(sleep_for)
            raise last_exc

        return wrapper

    return decorator


# ------------------- 获取/缓存全市场股票列表 (代码省略，沿用原版) -------------------
@sync_retry(max_retries=2, backoff_base=1.5)
def fetch_stock_list_safe():
    """尝试多接口获取股票列表"""
    # ... (与原版相同)
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
        raise Exception(f"获取股票列表失败: {e}")

    raise Exception("未能从任何接口获取到股票列表")


def get_stock_list_manager():
    # ... (与原版相同)
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
    if df is None or df.empty:
        raise Exception("股票列表为空")

    with open(cache_file, "w", encoding="utf-8") as f:
        data = {
            "time": today_str,
            "data": df.to_dict(orient="records")
        }
        json.dump(data, f, ensure_ascii=False, indent=2)

    return df


# ------------------- 获取/缓存全市场股票列表 (Upsert 优化版) -------------------
def filter_stock_list(df):
    if df is None or df.empty:
        return []
    df["code"] = df["code"].astype(str)

    target_stocks = CONFIG["TARGET_STOCKS"]
    if target_stocks:
        print(f"[过滤] 仅抓取 TARGET_STOCKS 中指定的 {len(target_stocks)} 支股票。")
        df = df[df["code"].isin([str(c) for c in target_stocks])]

    # 排除规则
    mask = pd.Series(False, index=df.index)
    # ... (保持排除逻辑不变)
    if CONFIG["EXCLUDE_GEM"]:
        mask |= df["code"].str.startswith(("300", "301"))
    if CONFIG["EXCLUDE_KCB"]:
        mask |= df["code"].str.startswith(("688", "689"))
    if CONFIG["EXCLUDE_BJ"]:
        mask |= df["code"].str.startswith(("8", "4", "92"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns:
        mask |= df["name"].str.contains("ST|退", na=False)

    # 关键修改：显式使用 .copy() 确保创建一个新的、独立的基础 DataFrame
    df_filtered = df[~mask].copy()

    # 在新的独立副本上安全地添加 'symbol' 列，避免 SettingWithCopyWarning
    df_filtered['symbol'] = df_filtered["code"].apply(lambda x: f"sh{x}" if x.startswith("6") else f"sz{x}")
    df_filtered['symbol'] = df_filtered['symbol'].astype(str)

    return df_filtered[['code', 'name', 'symbol']].to_dict('records')


# ------------------- 数据抓取（同步函数） -------------------
@sync_retry(max_retries=CONFIG["RETRY_TIMES"], backoff_base=CONFIG["RETRY_BACKOFF_BASE"])
def fetch_data_only_sync(item: dict, start_date: str, end_date: str):
    """
    同步抓取单只股票（内部带重试）
    说明：此函数是同步阻塞的，交由 ThreadPoolExecutor 执行。
    """
    # 适度随机延时（减轻服务器压力）
    time.sleep(random.uniform(0.6, 1.2))

    code = item['code']
    symbol = item['symbol']
    name = item.get('name', code)
    adjust_type = CONFIG["ADJUST"]

    # 调用 akshare
    # 注意：这里抓取的是目标范围 start_date 到 end_date 之间的所有数据
    df = ak.stock_zh_a_daily(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust_type
    )

    # 基本校验
    if df is None or df.empty:
        # 在 upsert 模式下，接口返回空可能是因为该股票在该时间段没有交易数据，
        # 不一定算作致命错误，但为了保持原版逻辑，仍抛出异常。
        raise Exception(f"接口返回空: {symbol}")

    # AkShare 返回格式多样：尝试截取前 7 列并标准化
    if df.shape[1] < 5:
        raise Exception("返回列数过少")
    df = df.iloc[:, :7]
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']

    # 字段补全
    df['code'] = code
    df['adjust'] = adjust_type

    # 转换日期，这是主键的一部分
    df['date'] = pd.to_datetime(df['date']).dt.date

    # 确保列顺序，特别是主键 (date, code, adjust)
    # to_sql 的 method 模式依赖于列顺序和主键定义
    df = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjust']]

    return df


# ------------------- 异步调度器 -------------------
async def main_scheduler(target_list):
    # 根据配置计算最终的 start_date 和 end_date
    if CONFIG["TARGET_START_DATE"] and CONFIG["TARGET_END_DATE"]:
        # 使用指定日期
        start_date = CONFIG["TARGET_START_DATE"]
        end_date = CONFIG["TARGET_END_DATE"]
        print(f"[范围] 使用用户指定日期: {start_date} ~ {end_date}")
    else:
        # 使用 DAYS 计算日期
        end_date = datetime.now().strftime("%Y%m%d")

        days_to_subtract = max(0, CONFIG["DAYS"] - 1)  # 修正点: DAYS=1 时，days_to_subtract=0

        # 抓取范围设置为 DAYS 天前，确保能抓到足够覆盖更新所需的数据
        start_date = (datetime.now() - timedelta(days=days_to_subtract)).strftime("%Y%m%d")
        print(f"[范围] 基于 DAYS={CONFIG['DAYS']} 计算的范围: {start_date} ~ {end_date}")

    table_name = CONFIG["MYSQL_TABLE"]
    total_stocks = len(target_list)

    print(f"\n[任务启动] 抓取范围: {start_date} ~ {end_date}")
    print(
        f"[配置] 目标: {total_stocks} 支 | 并发上限: {CONFIG['MAX_WORKERS']} | 单次超时: {CONFIG['REQUEST_TIMEOUT']}s")

    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(CONFIG["MAX_WORKERS"])
    all_results = []
    failed_items = []

    # 单层线程池，用于 run_in_executor
    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:

        # 内部函数：抓取、限速、超时控制
        async def fetch_with_limit(item):
            async with sem:
                try:
                    coro = loop.run_in_executor(pool, fetch_data_only_sync, item, start_date, end_date)
                    df = await asyncio.wait_for(coro, timeout=CONFIG["REQUEST_TIMEOUT"])
                    return df
                except asyncio.TimeoutError:
                    return ("timeout", item)
                except Exception as e:
                    return ("error", item, str(e))

        # 构造任务列表并使用 as_completed 实时处理结果
        tasks = [asyncio.create_task(fetch_with_limit(item)) for item in target_list]
        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")
        success_count = 0

        for coro in pbar:
            res = await coro
            if isinstance(res, tuple):
                tag = res[0]
                if tag in ("timeout", "error"):
                    failed_items.append(res[1])
                    if tag == "error":
                        print(f"[失败] {res[1].get('code')} - {res[2]}")
            elif res is not None:
                all_results.append(res)
                success_count += 1

            pbar.set_postfix({"成功抓取": success_count, "总数": total_stocks, "失败待重试": len(failed_items)})

    # 第一轮完成
    print(f"\n[第一轮完成] 成功 {len(all_results)} / {total_stocks}，失败 {len(failed_items)}。")

    # 低并发重试（MAX_WORKERS_RETRY 固定为 2）
    if failed_items:
        retry_results = []
        retry_workers = min(2, max(1, CONFIG["MAX_WORKERS"] // 2))
        print(f"[重试] 对 {len(failed_items)} 支股票进行低并发重试（并发 {retry_workers}）...")

        async def retry_run(item):
            # 重试时使用单独的线程池 (max_workers=1) 并通过 Semaphore 限制总并发
            async with asyncio.Semaphore(retry_workers):
                try:
                    coro = loop.run_in_executor(ThreadPoolExecutor(max_workers=1), fetch_data_only_sync, item,
                                                start_date, end_date)
                    df = await asyncio.wait_for(coro, timeout=max(CONFIG["REQUEST_TIMEOUT"] * 1.2, 30))
                    return df
                except Exception:
                    # 再次失败，直接返回 None，不再进行第三轮重试
                    return None

        retry_tasks = [asyncio.create_task(retry_run(it)) for it in failed_items]
        for r in tqdm(asyncio.as_completed(retry_tasks), total=len(retry_tasks), unit="retry"):
            df = await r
            if df is not None:
                retry_results.append(df)

        print(f"[重试结果] 成功补抓 {len(retry_results)} 条")
        all_results.extend(retry_results)

    if not all_results:
        print("\n[警告] 未抓取到任何有效数据，导入终止。")
        return

    # 合并并导入数据库 (Upsert 逻辑在此实现)
    final_df = pd.concat(all_results, ignore_index=True)
    print(f"\n[导入] 准备批量 Upsert {len(final_df)} 条数据到表 {table_name} ...")

    try:
        engine = get_db_engine()
        # **核心变更：使用自定义 method 进行 Upsert**
        final_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',  # 保持 'append' 配合 method 参数
            index=False,
            chunksize=5000,
            method=mysql_upsert_method  # 指定自定义的 Upsert 方法
        )
        print(f"[导入] 批量 Upsert 成功。共处理 {len(final_df)} 条记录。")

    except Exception as e:
        print(f"❌ 批量 Upsert 失败: {e}")
        print("请确保 MySQL 表已设置 (date, code, adjust) 为联合主键！")


# ------------------- 主入口 -------------------
def main():
    start_time = time.time()

    # 获取并过滤股票列表
    try:
        df_base = get_stock_list_manager()
    except Exception as e:
        print(f"[终止] 无法获取股票列表: {e}")
        return

    target_list = filter_stock_list(df_base)
    if not target_list:
        print("[终止] 股票列表为空，请检查过滤条件或 TARGET_STOCKS。")
        return

    # 抽样处理
    sample_size = CONFIG["SAMPLE_SIZE"]
    if sample_size and sample_size > 0 and len(target_list) > sample_size:
        print(f"[抽样模式] 随机抽取 {sample_size} 支股票进行测试...")
        target_list = random.sample(target_list, sample_size)
    else:
        print(f"[全量模式] 扫描 {len(target_list)} 支有效股票...")

    # 运行调度器
    try:
        asyncio.run(main_scheduler(target_list))
    except Exception as e:
        print(f"\n❌ 主调度器运行出错: {e}")

    print("\n" + "=" * 60)
    print(f"✅ 历史数据 Upsert 任务完成 | 总耗时: {time.time() - start_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()