
import os
import json
import time
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, wait, TimeoutError as ThreadingTimeoutError

import pandas as pd
import numpy as np
import akshare as ak
import asyncio
from tqdm import tqdm

# ============================================================
# 模块 1：配置 (Configuration)
# ============================================================
CONFIG = {
    # --- 时间范围 ---
    "DAYS": 365,  # 扫描回溯天数 (用于计算 MA200)

    # --- 过滤条件 ---
    "EXCLUDE_GEM": True,  # 排除创业板（300）
    "EXCLUDE_KCB": True,  # 排除科创板（688）
    "EXCLUDE_BJ": True,  # 排除北交所（8、4）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",  # 复权方式

    # --- SQZ策略参数 ---
    "SQZ": {
        "length": 20,
        "mult": 2.0,
        "lengthKC": 20,
        "multKC": 1.5,
        "useTrueRange": True
    },

    # --- PIVOT策略参数 ---
    "PIVOT_LEFT": 15,  # 左侧 K 线数量 (确认高点所需的左侧天数)
    "PIVOT_RIGHT": 15,  # 右侧 K 线数量 (确认高点所需的右侧天数)

    # --- 文件路径/名称 ---
    "CACHE_FILE": "stock_list_cache.json",
    "EXPORT_ENCODING": "utf-8-sig",  # CSV文件导出编码
    "OUTPUT_FILENAME_BASE": "Buy_Stocks",  # 输出文件基础名称
    "OUTPUT_FOLDER_BASE": "Day_Stocks",  # 结果文件存放的根文件夹

    # --- 抽样/并发 ---
    "SAMPLE_SIZE": 0,  # 0 或 None 表示全量，>0 表示随机抽样数量
    "MAX_WORKERS": 32,
    "REQUEST_TIMEOUT": 15,  # 🆕 **关键：akshare 单次请求整体超时保护（秒）**

    # --- 🆕 手动输入 ---
    # 示例: ["600519", "000001", "300751"]。如果非空，则跳过全量扫描。
    "MANUAL_STOCK_LIST": [
                            "000807",
                            "000708",
                            "002830",
                            "301517",
                            "000408",
                            "600879",
                            "600595",
                            "601168",
                            "002595",
                            "301028",
                            "002429"
                        ]
}

# 1️⃣ 程序启动时获取交易日列表
df_cal = ak.tool_trade_date_hist_sina()
df_cal['trade_date'] = pd.to_datetime(df_cal['trade_date'], format="%Y%m%d").dt.date
trade_dates_set = set(df_cal['trade_date'])  # 转成 set 方便快速查找

# 2️⃣ 判断今天是否交易日
today = datetime.today().date()  - timedelta(days=2)
if today not in trade_dates_set:
    print("今天非交易日，不查询实时K")
else:
    print("今天交易日，可以查询实时K")

print(trade_dates_set)
print(today)