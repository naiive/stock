#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股历史数据批量导入 MySQL 脚本（稳定版）
说明：
- 单层线程池 + asyncio.Semaphore 控制真实并发
- asyncio.wait_for 控制单次 akshare 请求超时（不会再创建额外线程）
- fetch_data_only 内部带同步重试（指数退避）
- 第一轮失败后进行低并发重试
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

    # 抓取范围
    "DAYS": 500,

    # 过滤
    "EXCLUDE_GEM": True,   # 排除创业板（300、301）
    "EXCLUDE_KCB": True,   # 排除科创板（688）
    "EXCLUDE_BJ": True,    # 排除北交所（8、4、92）
    "EXCLUDE_ST": False,   # 排除 ST/退
    "ADJUST": "qfq",       # 'qfq' / 'hfq' / None

    # 并发与超时
    "MAX_WORKERS": 6,         # 建议 2~4 更稳
    "REQUEST_TIMEOUT": 28,    # 单次 akshare 请求超时（秒），由 asyncio.wait_for 控制
    "SAMPLE_SIZE": 0,         # 0 表示全量
    "CACHE_FILE": "stock_list_cache.json",

    # 重试策略（fetch_data_only 内部）
    "RETRY_TIMES": 2,
    "RETRY_BACKOFF_BASE": 1.6,  # 指数退避基数
}
# ------------------- /配置 -------------------


# ------------------- 数据库连接 -------------------
def get_db_engine():
    db_conf = CONFIG["DB"]
    url = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}?charset=utf8mb4"
    engine = create_engine(url, pool_recycle=3600)
    return engine


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
                        raise
                    # 指数退避 + 随机抖动
                    sleep_for = (backoff_base ** (attempt - 1)) + random.uniform(0, 0.5)
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
        # ak.stock_zh_a_spot_em 返回列名可能不同，确保 code/name 存在
        if "code" in df.columns and "name" in df.columns:
            return df[["code", "name"]]
        # 尝试其他列名映射
        if "代码" in df.columns and "名称" in df.columns:
            df = df.rename(columns={"代码": "code", "名称": "name"})
            return df[["code", "name"]]
    except Exception as e:
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


def filter_stock_list(df):
    if df is None or df.empty:
        return []
    df["code"] = df["code"].astype(str)
    mask = pd.Series(False, index=df.index)
    if CONFIG["EXCLUDE_GEM"]:
        mask |= df["code"].str.startswith(("300", "301"))
    if CONFIG["EXCLUDE_KCB"]:
        mask |= df["code"].str.startswith(("688", "689"))
    if CONFIG["EXCLUDE_BJ"]:
        mask |= df["code"].str.startswith(("8", "4", "92"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns:
        mask |= df["name"].str.contains("ST|退", na=False)

    df['symbol'] = df["code"].apply(lambda x: f"sh{x}" if x.startswith("6") else f"sz{x}")
    df['symbol'] = df['symbol'].astype(str)

    return df[~mask][['code', 'name', 'symbol']].to_dict('records')


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
    df = ak.stock_zh_a_daily(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust_type
    )

    # 基本校验
    if df is None or df.empty:
        raise Exception(f"接口返回空: {symbol}")

    # AkShare 返回格式多样：尝试截取前 7 列并标准化
    if df.shape[1] < 5:
        raise Exception("返回列数过少")
    df = df.iloc[:, :7]
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']

    # 字段补全
    df['code'] = code
    df['adjust'] = adjust_type

    # 转换日期
    df['date'] = pd.to_datetime(df['date']).dt.date

    # 确保列顺序
    df = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjust']]

    return df


# ------------------- 异步调度器 -------------------
async def main_scheduler(target_list):
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")
    table_name = CONFIG["MYSQL_TABLE"]
    adjust_type = CONFIG["ADJUST"]
    total_stocks = len(target_list)

    print(f"\n[任务启动] 抓取范围: {start_date} ~ {end_date}")
    print(f"[配置] 目标: {total_stocks} 支 | 并发上限: {CONFIG['MAX_WORKERS']} | 单次超时: {CONFIG['REQUEST_TIMEOUT']}s")

    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(CONFIG["MAX_WORKERS"])
    all_results = []
    failed_items = []

    # 单层线程池，用于 run_in_executor
    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:

        async def fetch_with_limit(item):
            """在 semaphore 限制下通过 executor 调用同步抓取，并用 asyncio.wait_for 加超时"""
            async with sem:
                try:
                    # run_in_executor 返回的是协程对象，在这里用 wait_for 控制超时
                    coro = loop.run_in_executor(pool, fetch_data_only_sync, item, start_date, end_date)
                    df = await asyncio.wait_for(coro, timeout=CONFIG["REQUEST_TIMEOUT"])
                    return df
                except asyncio.TimeoutError:
                    # 超时
                    return ("timeout", item)
                except Exception as e:
                    return ("error", item, str(e))

        # 构造任务列表（不立即 submit — 我们会使用 asyncio.as_completed）
        tasks = [asyncio.create_task(fetch_with_limit(item)) for item in target_list]

        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")
        success_count = 0

        for coro in pbar:
            res = await coro
            # 可能返回 DataFrame 或者 ("timeout", item) / ("error", item, msg)
            if isinstance(res, tuple):
                tag = res[0]
                if tag == "timeout":
                    failed_items.append(res[1])
                elif tag == "error":
                    failed_items.append(res[1])
                    # 打印错误信息
                    print(f"[失败] {res[1].get('code')} - {res[2]}")
            elif res is None:
                # treat as failed
                # can't determine item here - skip
                pass
            else:
                # 成功
                all_results.append(res)
                success_count += 1

            pbar.set_postfix({"成功抓取": success_count, "总数": total_stocks, "失败待重试": len(failed_items)})

    # 第一轮完成
    print(f"\n[第一轮完成] 成功 {len(all_results)} / {total_stocks}，失败 {len(failed_items)}。")

    # 如果有失败，进行一次低并发重试（MAX_WORKERS_RETRY 固定为 2）
    if failed_items:
        retry_results = []
        retry_workers = min(2, max(1, CONFIG["MAX_WORKERS"] // 2))
        print(f"[重试] 对 {len(failed_items)} 支股票进行低并发重试（并发 {retry_workers}）...")

        async def retry_run(item):
            async with asyncio.Semaphore(retry_workers):
                try:
                    coro = loop.run_in_executor(ThreadPoolExecutor(max_workers=1), fetch_data_only_sync, item, start_date, end_date)
                    # 给重试稍长一些的超时
                    df = await asyncio.wait_for(coro, timeout=max(CONFIG["REQUEST_TIMEOUT"] * 1.2, 30))
                    return df
                except asyncio.TimeoutError:
                    return None
                except Exception:
                    return None

        # 批量顺序重试（串行化也可以，但这里用少量并发）
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

    # 合并并导入数据库
    final_df = pd.concat(all_results, ignore_index=True)
    print(f"\n[导入] 准备批量导入 {len(final_df)} 条数据到表 {table_name} ...")

    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            delete_sql = f"DELETE FROM {table_name} WHERE adjust='{adjust_type}'"
            conn.execute(text(delete_sql))
            conn.commit()
            print(f"[导入] 已删除旧数据 adjust={adjust_type}")

        # to_sql 插入，chunksize 可按数据量调整
        final_df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=50000)
        print(f"[导入] 批量导入成功。共导入 {len(final_df)} 条记录。")

    except Exception as e:
        print(f"❌ 批量导入失败: {e}")


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
        print("[终止] 股票列表为空，请检查过滤条件。")
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
    print(f"✅ 历史数据导入任务完成 | 总耗时: {time.time() - start_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()