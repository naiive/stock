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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import akshare as ak
from tqdm import tqdm
from sqlalchemy import create_engine, text

import conf.config as conf

C_END = "\033[0m"
C_BOLD = "\033[1m"
C_RED = "\033[31m"
C_GREEN = "\033[32m"
C_YELLOW = "\033[33m"
C_BLUE = "\033[34m"
C_CYAN = "\033[36m"

CONFIG = {
    # 定向同步清单：若填入代码如 ["600519"]，则只同步这些，忽略过滤逻辑
    "TARGET_STOCKS": [],

    # 数据周期：daily(日线), weekly(周线), monthly(月线)
    "PERIOD": "daily",

    # 时间范围 (YYYYMMDD)
    "START_DATE": "20260101",
    "END_DATE": "20260105",

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
    "EXCLUDE_GEM": True,    # 创业板过滤
    "EXCLUDE_KCB": True,    # 科创板过滤
    "EXCLUDE_BJ": True,     # 北交所及新三板过滤
    "EXCLUDE_ST": False,    # 排除 ST/*ST (特别处理/退市风险警示)
    "EXCLUDE_DELIST": True, # 排除退市股 (包含名称含"退"或"退市"的整理期股票)
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
# 深度定制日志系统 (支持颜色 & 兼容 Tqdm)
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
            # 关键：使用 tqdm.write 替代 sys.stdout.write
            tqdm.write(msg, file=sys.stdout)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logger():
    """初始化全局日志记录器"""
    l = logging.getLogger('StockETL')
    l.setLevel(logging.INFO)

    # 清理历史 Handler，防止重复打印
    if l.handlers:
        l.handlers.clear()
    l.propagate = False

    # 1. 物理文件 Handler (记录原始文本，无 ANSI 颜色码)
    file_fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    fh = logging.FileHandler(os.path.join(LOG_DIR, "etl.log"), encoding='utf-8')
    fh.setFormatter(file_fmt)
    l.addHandler(fh)

    # 2. 终端 Tqdm Handler (彩色)
    th = TqdmLoggingHandler()
    th.setFormatter(ColoredFormatter())
    l.addHandler(th)
    return l


logger = setup_logger()


# ---------------------------------------------------------
# 基础工具函数 (过滤、数据库、缓存)
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
    对 A 股全市场股票列表进行多维度清洗。
    主要任务：剔除 ST 风险股、非 A 股板块（B 股）、以及根据配置排除特定交易规则的板块。
    """
    before_count = len(df)
    # ---------------------------------------------------------
    # 1. 风险警示股过滤 (ST/*ST)
    # 逻辑：匹配名称中包含 'ST' 但不含 '退' 的股票
    # ---------------------------------------------------------
    if CONFIG.get("EXCLUDE_ST", True):
        # 仅匹配 ST 和 *ST
        df = df[~df['name'].str.contains(r'ST|\*ST', flags=re.IGNORECASE)]

    # ---------------------------------------------------------
    # 2. 终止上市股过滤 (退市整理期)
    # 逻辑：匹配名称中包含 '退' 或 '退市' 的股票
    # 这些股票通常处于 15 个交易日的退市整理期，流动性极差且面临归零风险
    # ---------------------------------------------------------
    if CONFIG.get("EXCLUDE_DELIST", True):
        # 匹配包含 "退" 字或 "退市" 字样的股票
        df = df[~df['name'].str.contains(r'退市|退')]

    # ---------------------------------------------------------
    # 3. 非 A 股业务过滤 (B 股排除)
    # 逻辑：防止接口混入以美元或港币计价的 B 股。
    # 沪市 B 股开头为 900；深市 B 股开头为 200。
    # ---------------------------------------------------------
    df = df[~df['code'].str.startswith(('900', '200'))]

    # ---------------------------------------------------------
    # 4. 创业板过滤 (Growth Enterprise Market)
    # 逻辑：排除 300 和 301 开头的股票。
    # 交易规则：20% 涨跌幅，且需要专门的开通权限。
    # ---------------------------------------------------------
    if CONFIG.get("EXCLUDE_GEM", True):
        df = df[~df['code'].str.startswith(('300', '301'))]

    # ---------------------------------------------------------
    # 5. 科创板过滤 (STAR Market)
    # 逻辑：排除 688 和 689 开头的股票。
    # 特别说明：688 为普通科创板，689 为科创板 CDR (存托凭证，如九号公司)。
    # 交易规则：20% 涨跌幅，50万资金门槛。
    # ---------------------------------------------------------
    if CONFIG.get("EXCLUDE_KCB", True):
        df = df[~df['code'].str.startswith(('688', '689'))]

    # ---------------------------------------------------------
    # 6. 北交所及新三板过滤 (Beijing Stock Exchange)
    # 逻辑：排除 4, 8, 92 开头的股票。
    # 43/83/87/88：北交所及精选层；920：北交所专用段；400：老三板。
    # 交易规则：30% 涨跌幅。
    # ---------------------------------------------------------
    if CONFIG.get("EXCLUDE_BJ", True):
        df = df[~df['code'].str.startswith(('4', '8', '92'))]

    after_count = len(df)

    # 打印详细的清洗报告，方便核对数据规模
    logger.info(
        f"🔍 {C_BOLD}{C_YELLOW}过滤报表:{C_END} 总样本: {C_CYAN}{before_count}{C_END}支 | 剔除: {C_RED}{before_count - after_count}{C_END}支 | 有效: {C_GREEN}{after_count}{C_END}支"
    )

    return df


def get_stock_list_with_cache():
    """获取 A 股列表，带物理缓存机制"""
    today = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                c = json.load(f)
                if c.get("update_at") == today:
                    logger.info(f"{C_GREEN}✅ 缓存命中:{C_END} 使用今日已存列表")
                    return pd.DataFrame(c['data'])
        except Exception:
            pass

    logger.info("📡 接口更新: 抓取全市场最新代码...")
    df = ak.stock_zh_a_spot_em()[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
    df['code'] = df['code'].astype(str)

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "update_at": today,
                "data": df.to_dict(orient="records")
            },
            f,
            ensure_ascii=False,
            indent=2
        )
    return df


def get_downloaded_codes():
    """扫描数据库，计算哪些数据已经在目标区间内存在"""
    table = CONFIG["TABLE_MAP"].get(CONFIG["PERIOD"])
    adj = CONFIG["ADJUST"] or 'none'

    # 构造条件查询 SQL
    sql = f"""
        SELECT DISTINCT code 
        FROM {table} 
        WHERE date BETWEEN :s AND :e AND adjust = :adj
    """

    try:
        with get_engine().connect() as conn:
            res = conn.execute(
                text(sql),
                {
                    "s": CONFIG["START_DATE"],
                    "e": CONFIG["END_DATE"],
                    "adj": adj
                }
            )
            return {row[0] for row in res}
    except Exception:
        return set()


# ---------------------------------------------------------
# 抓取与 Upsert 逻辑
# ---------------------------------------------------------
def fetch_stock_data(item, s_date, e_date):
    """执行单只股票抓取 (工作线程内运行)"""
    code = item['code']
    time.sleep(random.uniform(0.3, 0.6))

    last_err = "未知错误"
    for attempt in range(1, CONFIG["MAX_RETRIES"] + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=CONFIG['PERIOD'],
                start_date=s_date,
                end_date=e_date,
                adjust=CONFIG['ADJUST']
            )

            if df is None or df.empty:
                raise ValueError("接口返回空数据")

            # ✅ 1. 字段映射
            mapping = {
                "日期": "date",
                "股票代码": "code",
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

            # ✅ 2. 处理日期格式
            df['date'] = pd.to_datetime(df['date']).dt.date

            # ✅ 3. 补全/覆盖元数据 (确保 code 和 adjust 存在)
            df['code'] = code
            df['adjust'] = CONFIG['ADJUST'] if CONFIG['ADJUST'] else 'none'

            # ✅ 4. 关键步骤：过滤掉数据库中不存在的列
            # 确保 DataFrame 的列顺序或存在性与数据库表结构完全匹配
            db_columns = [
                'code', 'date', 'open', 'high', 'low', 'close',
                'volume', 'amount', 'amplitude', 'pct_chg',
                'chg', 'turnover_rate', 'adjust'
            ]
            # 只保留数据库需要的列，防止多余的中文列干扰
            df = df[db_columns]

            return {
                "code": code,
                "df": df,
                "error": None
            }
        except Exception as e:
            last_err = str(e)
            logger.warning(f"🔄 {code:<9} | 尝试 {attempt}/{CONFIG['MAX_RETRIES']} | {last_err[:30]}")
            if attempt < CONFIG["MAX_RETRIES"]:
                time.sleep(1.5)

    return {"code": code, "df": None, "error": last_err}


def mysql_upsert_logic(table, conn, keys, data_iter):
    """
    执行 MySQL ON DUPLICATE KEY UPDATE 逻辑
    """
    data_list = [dict(zip(keys, row)) for row in data_iter]

    # 构造 SQL
    cols = ", ".join([f"`{k}`" for k in keys])
    plh = ", ".join([f":{k}" for k in keys])
    upd = ", ".join([
        f"`{k}`=VALUES(`{k}`)"
        for k in keys
        if k not in ['date', 'code', 'adjust']
    ])

    sql = f"""
        INSERT INTO {table.name} ({cols}) 
        VALUES ({plh}) 
        ON DUPLICATE KEY UPDATE {upd}
    """
    conn.execute(text(sql), data_list)


# ---------------------------------------------------------
# 核心同步引擎
# ---------------------------------------------------------
async def start_engine(todo_jobs, total_query, already_exist):
    """多线程协程调度器"""
    sem = asyncio.Semaphore(CONFIG["MAX_WORKERS"])
    loop = asyncio.get_running_loop()

    success_dfs = []
    failed_logs = []

    async def worker(item):
        async with sem:
            with ThreadPoolExecutor() as pool:
                return await loop.run_in_executor(
                    pool,
                    fetch_stock_data,
                    item,
                    CONFIG["START_DATE"],
                    CONFIG["END_DATE"]
                )

    # 初始化进度条 UI
    pbar = tqdm(
        total=len(todo_jobs),
        desc=f"{C_BOLD}📊 同步进度{C_END}",
        bar_format="{l_bar}%s{bar:25}%s{r_bar}" % (C_GREEN, C_END),
        file=sys.stdout,
        dynamic_ncols=True
    )

    # 提交所有任务并监听完成
    tasks = [worker(it) for it in todo_jobs]
    for future in asyncio.as_completed(tasks):
        res = await future

        if res["df"] is not None:
            success_dfs.append(res["df"])
        else:
            failed_logs.append(res)

        pbar.update(1)
        # 实时刷新四维度 postfix
        pbar.set_postfix({
            "🎯查询": f"{total_query}",
            "📦已有": f"{C_CYAN}{already_exist}{C_END}",
            "✅成功": f"{C_GREEN}{len(success_dfs)}{C_END}",
            "❌失败": f"{C_RED}{len(failed_logs)}{C_END}"
        })

    pbar.close()

    # 处理最终结果
    if failed_logs:
        logger.error(f"{C_BOLD}{' 最终失败汇总 ':=^50}{C_END}")
        for log in failed_logs:
            logger.error(f"代码: {log['code']:<10} | 原因: {log['error']}")

    if success_dfs:
        target = CONFIG["TABLE_MAP"].get(CONFIG["PERIOD"])
        logger.info(f"{C_GREEN}💾 正在入库 {len(success_dfs)} 支股票数据...{C_END}")
        try:
            pd.concat(success_dfs).to_sql(
                name=target,
                con=get_engine(),
                if_exists='append',
                index=False,
                chunksize=2000,
                method=mysql_upsert_logic
            )
            logger.info(f"{C_BOLD}{C_GREEN}✨ 数据同步圆满结束！{C_END}")
        except Exception as e:
            logger.error(f"💔 入库异常: {e}")


def main():
    start_ts = time.time()
    logger.info(f"{C_BOLD}{'=' * 75}{C_END}")
    logger.info(f"{C_BOLD}AsianQuant ETL 启动{C_END} | {C_CYAN}周期: {CONFIG['PERIOD']}{C_END} | {C_RED}范围: {CONFIG['START_DATE']}-{CONFIG['END_DATE']}{C_END}")

    try:
        # 1. 确定初始任务集
        df_all = get_stock_list_with_cache()

        if CONFIG["TARGET_STOCKS"]:
            # 如果指定了目标，则跳过过滤逻辑
            full_jobs = [{"code": str(c)} for c in CONFIG["TARGET_STOCKS"]]
        else:
            # 否则执行板块过滤
            df_filtered = apply_filters(df_all)
            full_jobs = df_filtered.to_dict(orient="records")

        query_total = len(full_jobs)

        # 2. 差集计算：排除库中已存在的 (断点续传核心)
        downloaded = get_downloaded_codes()
        todo_jobs = [j for j in full_jobs if j['code'] not in downloaded]
        already_exist = query_total - len(todo_jobs)

        logger.info(
            f"📋 {C_BOLD}任务初始: {C_END}总量={query_total} | "
            f"已有={already_exist} | 待抓={len(todo_jobs)}"
        )

        # 3. 异步启动
        if todo_jobs:
            asyncio.run(start_engine(todo_jobs, query_total, already_exist))
        else:
            logger.info(f"{C_GREEN}✅ 库中数据已是最新，无需同步。{C_END}")

    except Exception as e:
        logger.critical(f"🛑 崩溃: {e}\n{traceback.format_exc()}")

    logger.info(f"{C_BOLD}🏁 总耗时: {time.time() - start_ts:.1f}s{C_END}")
    logger.info(f"{C_BOLD}{'=' * 75}{C_END}")


if __name__ == "__main__":
    main()