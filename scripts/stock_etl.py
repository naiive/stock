#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import random
import logging
import asyncio
import traceback
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import akshare as ak
from tqdm import tqdm
from sqlalchemy import create_engine, text

# 配置文件存在
try:
    import conf.config as conf
except ImportError:
    # 兼容测试环境
    class MockConf:
        DB_CONFIG = {
            "USER": "root", "PASS": "password", "HOST": "127.0.0.1", "PORT": 3306, "DB_NAME": "asian_quant"
        }
    conf = MockConf()

# ---------------------------------------------------------
# 0. 终端视觉常量 (ANSI 颜色)
# ---------------------------------------------------------
C_END, C_BOLD, C_RED, C_GREEN, C_YELLOW, C_BLUE, C_CYAN = "\033[0m", "\033[1m", "\033[31m", "\033[32m", "\033[33m", "\033[34m", "\033[36m"

# ---------------------------------------------------------
# 1. 全局配置字典 (字典展开排版，拒绝单行)
# ---------------------------------------------------------
CONFIG = {
    # 数据周期：daily(日线), weekly(周线), monthly(月线)
    "PERIOD": "daily",

    # 【优先级最高】相对天数模式：0-今天, 1-昨天, 2-前天... 3-None-使用固定日期
    "LOOKBACK_DAYS": 0,

    # 固定日期模式 (当 LOOKBACK_DAYS 为 None 时生效) 格式：20260106
    "START_DATE": "20260106",
    "END_DATE": "20260106",

    # 定向同步清单：若填入代码如 ["600519"]，则只同步这些，忽略过滤逻辑
    "TARGET_STOCKS": [],

    # 并发与重试设置
    "MAX_WORKERS": 8,  # 最大并发数
    "MAX_RETRIES": 2,  # 单只股票失败后的重试次数

    # 数据库表映射关系
    "TABLE_MAP": {
        "daily": "asian_quant_stock_daily",
        "weekly": "asian_quant_stock_weekly",
        "monthly": "asian_quant_stock_monthly"
    },

    # 复权方式：qfq(前复权), hfq(后复权), None(不复权)
    "ADJUST": "qfq",

    # 过滤器配置
    "EXCLUDE_GEM": True,     # 创业板过滤
    "EXCLUDE_KCB": True,     # 科创板过滤
    "EXCLUDE_BJ": True,      # 北交所及新三板过滤
    "EXCLUDE_ST": False,     # 排除 ST/*ST
    "EXCLUDE_DELIST": True,  # 排除退市股
}

# 数据库连接配置
DB_CONFIG = conf.DB_CONFIG

# 路径管理
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CURRENT_DATE_STR = datetime.now().strftime("%Y%m%d")
LOG_DIR = os.path.join(BASE_DIR, "data/logs", CURRENT_DATE_STR)
CACHE_DIR = os.path.join(BASE_DIR, "data/cache")
CACHE_PATH = os.path.join(CACHE_DIR, "stock_list_cache.json")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)


# ---------------------------------------------------------
# 2. 深度定制日志系统 (支持颜色 & 兼容 Tqdm)
# ---------------------------------------------------------
class ColoredFormatter(logging.Formatter):
    """自定义彩色格式化类"""
    MAPPING = {
        logging.INFO: f"{C_BLUE}%(asctime)s [INFO]{C_END} %(message)s",
        logging.WARNING: f"{C_YELLOW}%(asctime)s [WARN]{C_END} %(message)s",
        logging.ERROR: f"{C_RED}%(asctime)s [ERROR]{C_END} %(message)s",
        logging.CRITICAL: f"{C_BOLD}{C_RED}%(asctime)s [CRIT]{C_END} %(message)s",
    }

    def format(self, record):
        fmt = self.MAPPING.get(record.levelno)
        return logging.Formatter(fmt, datefmt='%H:%M:%S').format(record)


class TqdmLoggingHandler(logging.Handler):
    """确保日志输出不破坏进度条行的处理器"""

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg, file=sys.stdout)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logger():
    """初始化全局日志记录器"""
    l = logging.getLogger('StockETL')
    l.setLevel(logging.INFO)
    if l.handlers:
        l.handlers.clear()
    l.propagate = False

    # 1. 物理文件记录
    file_fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    fh = logging.FileHandler(os.path.join(LOG_DIR, "etl.log"), encoding='utf-8')
    fh.setFormatter(file_fmt)
    l.addHandler(fh)

    # 2. 终端彩色输出
    th = TqdmLoggingHandler()
    th.setFormatter(ColoredFormatter())
    l.addHandler(th)
    return l


logger = setup_logger()


# ---------------------------------------------------------
# 3. 基础工具函数 (过滤、数据库、缓存)
# ---------------------------------------------------------
def get_engine():
    """创建 SQLAlchemy 引擎"""
    c = DB_CONFIG
    conn_url = (
        f"mysql+pymysql://{c['USER']}:{c['PASS']}@"
        f"{c['HOST']}:{c['PORT']}/{c['DB_NAME']}?charset=utf8mb4"
    )
    return create_engine(conn_url, pool_recycle=3600)


def apply_filters(df):
    """
    对股票列表进行板块和风险过滤
    """
    before_count = len(df)

    # 1. 风险警示股过滤 (ST/*ST)
    if CONFIG.get("EXCLUDE_ST", True):
        df = df[~df['name'].str.contains(r'ST|\*ST', flags=re.IGNORECASE)]

    # 2. 终止上市股过滤 (退市整理期)
    if CONFIG.get("EXCLUDE_DELIST", True):
        df = df[~df['name'].str.contains(r'退市|退')]

    # 3. 非 A 股业务过滤 (B 股排除)
    df = df[~df['code'].str.startswith(('900', '200'))]

    # 4. 创业板过滤
    if CONFIG.get("EXCLUDE_GEM", True):
        df = df[~df['code'].str.startswith(('300', '301'))]

    # 5. 科创板过滤
    if CONFIG.get("EXCLUDE_KCB", True):
        df = df[~df['code'].str.startswith(('688', '689'))]

    # 6. 北交所及新三板过滤
    if CONFIG.get("EXCLUDE_BJ", True):
        df = df[~df['code'].str.startswith(('4', '8', '92'))]

    after_count = len(df)
    logger.info(
        f"🔍 {C_BOLD}{C_YELLOW}过滤报表:{C_END} 总样本: {C_CYAN}{before_count}{C_END}支 | "
        f"剔除: {C_RED}{before_count - after_count}{C_END}支 | "
        f"有效: {C_GREEN}{after_count}{C_END}支"
    )
    return df


def get_stock_list_with_cache():
    """获取全市场股票清单"""
    today = datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                c = json.load(f)
                if c.get("time") == today:
                    logger.info(f"{C_GREEN}✅ 缓存命中:{C_END} 使用今日代码清单")
                    return pd.DataFrame(c['data'])
        except Exception:
            pass

    logger.info("📡 接口更新: 抓取最新代码列表...")
    df = ak.stock_zh_a_spot_em()[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
    df['code'] = df['code'].astype(str)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump({"time": today, "data": df.to_dict(orient="records")}, f, ensure_ascii=False, indent=2)
    return df


def get_downloaded_codes():
    """检查数据库内已同步的代码"""
    table = CONFIG["TABLE_MAP"].get(CONFIG["PERIOD"])
    adj = CONFIG["ADJUST"] or 'none'
    sql = f"SELECT DISTINCT code FROM {table} WHERE date BETWEEN :s AND :e AND adjust = :adj"
    try:
        with get_engine().connect() as conn:
            res = conn.execute(text(sql), {"s": CONFIG["START_DATE"], "e": CONFIG["END_DATE"], "adj": adj})
            return {row[0] for row in res}
    except Exception:
        return set()


# ---------------------------------------------------------
# 4. 抓取与 Upsert 逻辑
# ---------------------------------------------------------
def fetch_stock_data(item, s_date, e_date):
    """抓取单只股票详情"""
    code = item['code']
    time.sleep(random.uniform(0.3, 0.6))
    last_err = "未知错误"

    for attempt in range(1, CONFIG["MAX_RETRIES"] + 1):
        try:
            # 支持历史加实时数据
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=CONFIG['PERIOD'],
                start_date=s_date,
                end_date=e_date,
                adjust=CONFIG['ADJUST']
            )
            if df is None or df.empty:
                raise ValueError("空数据")

            # 字段重命名与对齐
            mapping = {
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌额": "chg",
                "涨跌幅": "pct_chg",
                "换手率": "turnover_rate"
            }
            df = df.rename(columns=mapping)
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['code'] = code
            df['adjust'] = CONFIG['ADJUST'] if CONFIG['ADJUST'] else 'none'

            # 严格筛选数据库列
            db_columns = [
                'code', 'date', 'open', 'high', 'low', 'close',
                'volume', 'amount', 'amplitude', 'pct_chg',
                'chg', 'turnover_rate', 'adjust'
            ]
            return {"code": code, "df": df[db_columns], "error": None}

        except Exception as e:
            last_err = str(e)
            if attempt < CONFIG["MAX_RETRIES"]:
                time.sleep(1.5)
    return {"code": code, "df": None, "error": last_err}


def mysql_upsert_logic(table, conn, keys, data_iter):
    """MySQL 批量 Upsert 语句"""
    data_list = [dict(zip(keys, row)) for row in data_iter]
    cols = ", ".join([f"`{k}`" for k in keys])
    plh = ", ".join([f":{k}" for k in keys])
    upd = ", ".join([f"`{k}`=VALUES(`{k}`)" for k in keys if k not in ['date', 'code', 'adjust']])
    sql = f"INSERT INTO {table.name} ({cols}) VALUES ({plh}) ON DUPLICATE KEY UPDATE {upd}"
    conn.execute(text(sql), data_list)


# ---------------------------------------------------------
# 5. 核心引擎 (异步调度)
# ---------------------------------------------------------
async def start_engine(todo_jobs, total_query, already_exist):
    sem = asyncio.Semaphore(CONFIG["MAX_WORKERS"])
    loop = asyncio.get_running_loop()
    success_dfs, failed_logs = [], []

    async def worker(item):
        async with sem:
            with ThreadPoolExecutor() as pool:
                return await loop.run_in_executor(pool, fetch_stock_data, item, CONFIG["START_DATE"],
                                                  CONFIG["END_DATE"])

    pbar = tqdm(
        total=len(todo_jobs),
        desc=f"{C_BOLD}📊 同步进度{C_END}",
        bar_format="{l_bar}%s{bar:25}%s{r_bar}" % (C_GREEN, C_END),
        dynamic_ncols=True
    )

    tasks = [worker(it) for it in todo_jobs]
    for future in asyncio.as_completed(tasks):
        res = await future
        if res["df"] is not None:
            success_dfs.append(res["df"])
        else:
            failed_logs.append(res)
        pbar.update(1)
        pbar.set_postfix({
            "🎯总需": total_query,
            "📦已有": f"{C_CYAN}{already_exist}{C_END}",
            "✅成功": f"{C_GREEN}{len(success_dfs)}{C_END}",
            "❌失败": f"{C_RED}{len(failed_logs)}{C_END}"
        })
    pbar.close()

    if failed_logs:
        logger.error(f"{C_BOLD}{' 失败明细 ':=^40}{C_END}")
        for log in failed_logs:
            logger.error(f"代码: {log['code']} | 原因: {log['error']}")

    if success_dfs:
        target = CONFIG["TABLE_MAP"].get(CONFIG["PERIOD"])
        logger.info(f"{C_GREEN}💾 正在入库 {len(success_dfs)} 条数据包...{C_END}")
        try:
            pd.concat(success_dfs).to_sql(
                name=target, con=get_engine(), if_exists='append',
                index=False, chunksize=2000, method=mysql_upsert_logic
            )
            logger.info(f"{C_BOLD}{C_GREEN}✨ ETL 同步圆满完成！{C_END}")
        except Exception as e:
            logger.error(f"💔 入库崩溃: {e}")


# ---------------------------------------------------------
# 6. 主入口 (参数优先级与日期计算)
# ---------------------------------------------------------
def main():
    start_ts = time.time()
    logger.info(f"{C_BOLD}{'=' * 75}{C_END}")
    logger.info(f"{C_BOLD}AsianQuant ETL 系统启动{C_END}")

    # --- 日期优先级处理逻辑 ---
    # --- 日期优先级处理逻辑 (修正后的区间逻辑) ---
    lookback = CONFIG.get("LOOKBACK_DAYS")
    if lookback is not None:
        today_dt = datetime.now()

        if lookback == 0:
            # 模式 0: 今天 -> 今天
            start_dt = today_dt
            end_dt = today_dt
        else:
            # 模式 N: (昨天 - N + 1) -> 昨天
            # 例如 lookback = 3: (昨天-2天) 到 (昨天)
            yesterday_dt = today_dt - timedelta(days=1)
            start_dt = yesterday_dt - timedelta(days=int(lookback) - 1)
            end_dt = yesterday_dt

        CONFIG["START_DATE"] = start_dt.strftime("%Y%m%d")
        CONFIG["END_DATE"] = end_dt.strftime("%Y%m%d")

        mode_desc = f"{C_YELLOW}回溯区间模式 (N={lookback}){C_END} -> {C_RED}{CONFIG['START_DATE']} 至 {CONFIG['END_DATE']}{C_END}"
    elif CONFIG.get("START_DATE") and CONFIG.get("END_DATE"):
        # 优先级2：固定日期模式
        mode_desc = f"{C_BLUE}固定日期模式{C_END} -> 范围: {C_RED}{CONFIG['START_DATE']} 至 {CONFIG['END_DATE']}{C_END}"
    else:
        # 兜底：未配置报错
        logger.error(f"{C_RED}致命错误: LOOKBACK_DAYS 和 START/END_DATE 均未配置！{C_END}")
        raise ValueError("Configuration Error: No date range specified.")

    logger.info(f"📅 当前运行模式: {mode_desc}")
    logger.info(f"⏱️ 任务周期: {CONFIG['PERIOD']} | 复权: {CONFIG['ADJUST']}")

    try:
        # 1. 任务准备
        df_all = get_stock_list_with_cache()
        if CONFIG["TARGET_STOCKS"]:
            full_jobs = [{"code": str(c), "name": "定向同步"} for c in CONFIG["TARGET_STOCKS"]]
        else:
            df_filtered = apply_filters(df_all)
            full_jobs = df_filtered.to_dict(orient="records")

        # 2. 查漏补缺 (断点续传)
        downloaded = get_downloaded_codes()
        todo_jobs = [j for j in full_jobs if j['code'] not in downloaded]
        already_exist = len(full_jobs) - len(todo_jobs)

        logger.info(
            f"📋 {C_BOLD}任务统计:{C_END} 总量={len(full_jobs)} | "
            f"跳过已存在={already_exist} | 实际待同步={len(todo_jobs)}"
        )

        # 3. 异步启动
        if todo_jobs:
            asyncio.run(start_engine(todo_jobs, len(full_jobs), already_exist))
        else:
            logger.info(f"{C_GREEN}✅ 目标时间段数据已完整，无需操作。{C_END}")

    except Exception as e:
        logger.critical(f"🛑 程序崩溃: {e}\n{traceback.format_exc()}")

    logger.info(f"{C_BOLD}🏁 总耗时: {time.time() - start_ts:.2f}s{C_END}")
    logger.info(f"{C_BOLD}{'=' * 75}{C_END}")


if __name__ == "__main__":
    main()