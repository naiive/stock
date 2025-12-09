#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股突破扫描系统（Pivot + SQZMOM + MA200）
版本：v1.5 (交易日判断 + 实时数据追加修正版)

【核心修改】
1. 废弃高失败率的分钟数据接口。
2. 实时数据获取改为串行调用腾讯实时快照接口 (stock_zh_a_spot)，并增加重试和延迟。
3. 实时快照数据 (df_spot) 作为参数传递给并发执行器，实现 O(1) 查找最新价。
4. 修复 append_today_realtime_snapshot 函数中的列名兼容性问题。
5. 引入交易日历加载和判断机制，确保只在交易日追加实时数据。
6. 修正 append_today_realtime_snapshot 逻辑：
    - 严格根据日期判断是否追加（非交易日/历史数据已包含今日数据则不追加）。
    - 使用实时快照提供的 O/H/L/C/Volume 构造当日 K 线。
============================================================
"""
import os
import sys
import json
import time
import random
import math
import datetime
from concurrent.futures import ThreadPoolExecutor, wait, TimeoutError as ThreadingTimeoutError

import pandas as pd
import numpy as np
import akshare as ak
import asyncio
from tqdm import tqdm

try:
    from api.stock_query import stock_zh_a_daily_mysql
except ImportError:
    print("[警告] 无法导入 stock_zh_a_daily_mysql。请确保您的 stock_query.py 文件存在。")
    def stock_zh_a_daily_mysql(*args, **kwargs):
        raise NameError("stock_zh_a_daily_mysql 尚未定义或导入失败。")

# ============================================================
# 模块 1：配置 (Configuration)
# ============================================================
CONFIG = {
    # --- 🆕 时间范围 ---
    "DAYS": 365,  # 扫描回溯天数 (用于计算 MA200)

    # --- 🆕 过滤条件 ---
    "EXCLUDE_GEM": True,  # 排除创业板（300、301）
    "EXCLUDE_KCB": True,  # 排除科创板（688、689）
    "EXCLUDE_BJ": True,   # 排除北交所（8、4、92）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",      # 复权方式

    # --- 🆕 SQZ策略参数 ---
    "SQZ": {
        "length": 20,
        "mult": 2.0,
        "lengthKC": 20,
        "multKC": 1.5,
        "useTrueRange": True
    },

    # --- 🆕 PIVOT策略参数 ---
    "PIVOT_LEFT": 15,   # 左侧 K 线数量
    "PIVOT_RIGHT": 15,  # 右侧 K 线数量

    # --- 🆕 文件路径/名称 ---
    "CACHE_FILE": "stock_list_cache.json",
    "EXPORT_ENCODING": "utf-8-sig",        # CSV文件导出编码
    "OUTPUT_FILENAME_BASE": "Buy_Stocks",  # 输出文件前缀
    "OUTPUT_FOLDER_BASE": "stocks",    # LogRedirector 也使用此文件夹

    # --- 🆕 并发 ---
    "MAX_WORKERS": 10,      # 降低线程数到 10
    "REQUEST_TIMEOUT": 20,  # 增加超时时间到 20s

    # --- 🆕 数据源控制 ---
    # True:  使用本地 stock_zh_a_daily_mysql 函数
    # False: 使用 ak.stock_zh_a_daily (AkShare)
    "USE_LOCAL_MYSQL": True,

    # --- 🆕 实时数据开关 ---
    # True:  使用腾讯实时股票全量接口 (fetch_realtime_snapshot)
    # False: 不使用，跳过实时数据获取（用于离线回测或非交易日）
    "USE_REAL_TIME_DATA": True,

    # --- 🆕 是否全量/分批控制 ---
    "SAMPLE_SIZE": 0,         # 0 或 None 表示全量
    "BATCH_SIZE": 500,        # SAMPLE_SIZE 全量才开启分批功能，每批次处理的股票数量
    "BATCH_INTERVAL_SEC": 5,  # 批次间隔休息时间（秒）

    # --- 🆕 手动输入 ---
    # 示例: ["600519", "000001", "300751"]。如果非空，则跳过全量扫描。
    "MANUAL_STOCK_LIST": []
}


# ============================================================
# 模块 2：日志重定向类
# ============================================================
class LogRedirector:
    """
    将 sys.stdout 的输出同时重定向到终端和日志文件，并实现按大小轮换（保留旧文件）。
    """
    # 20MB 轮换限制
    MAX_BYTES = 20 * 1024 * 1024

    def __init__(self, folder="stocks"):
        # 日志路径: stocks/logs/YYYYMMDD/
        self.today_str = datetime.datetime.now().strftime('%Y%m%d')
        self.log_dir = os.path.join(folder, "logs", self.today_str)
        os.makedirs(self.log_dir, exist_ok=True)

        self.terminal = sys.stdout
        self.log_file = None
        self.current_log_path = None
        self.is_active = False

    def _get_new_log_path(self):
        """生成新的日志文件名：YYYYMMDD_HHMMSS.log"""
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f"{timestamp}.log"
        return os.path.join(self.log_dir, log_filename)

    def _check_and_rotate(self):
        """检查文件大小，如果超过限制则关闭旧文件并创建新文件。"""
        if self.log_file is None:
            # 首次创建
            self.current_log_path = self._get_new_log_path()
            self.log_file = open(self.current_log_path, 'a', encoding='utf-8')
            return True

        # 检查大小
        if os.path.getsize(self.current_log_path) > self.MAX_BYTES:
            self.log_file.write(f"\n[轮换] 日志达到 {self.MAX_BYTES / 1024 / 1024:.0f}MB 限制，正在切换文件...\n")
            self.log_file.close()  # <--- 关闭旧文件，保留在磁盘上

            # 创建新文件
            self.current_log_path = self._get_new_log_path()  # <--- 生成全新的、不重复的文件名
            self.log_file = open(self.current_log_path, 'a', encoding='utf-8')  # <--- 打开新文件继续写入
            return True

        return False

    def __enter__(self):
        try:
            self._check_and_rotate()  # 首次创建日志文件
            sys.stdout = self
            self.is_active = True
            self.write(
                f"\n{'=' * 70}\n[会话开始] {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n[日志文件] {self.current_log_path}\n{'=' * 70}\n")
            return self
        except Exception as e:
            print(f"[错误] 日志系统启动失败: {e}", file=self.terminal)
            sys.stdout = self.terminal
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_active:
            self.write(f"\n{'=' * 70}\n[会话结束] {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 70}\n")
            sys.stdout = self.terminal
            if self.log_file:
                self.log_file.close()
            print(f"日志文件已保存至: {self.current_log_path}")

    def write(self, message):
        self.terminal.write(message)
        self.terminal.flush()

        if self.log_file:
            self._check_and_rotate()  # 写入前检查是否需要轮换
            if not message.startswith('\r'):
                self.log_file.write(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {message}")
            else:
                self.log_file.write(message)
            self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        if self.log_file:
            self.log_file.flush()

# ============================================================
# 模块3：交易日历相关的全局变量和函数
# ============================================================
_TRADE_CALENDAR = set()

def is_trade_day(date_obj):
    """检查给定的日期是否在已加载的交易日历中"""
    # 如果日历加载失败，_TRADE_CALENDAR 为空，则认为不是交易日，以防止错误追加
    return date_obj in _TRADE_CALENDAR

def load_trade_calendar():
    """
    加载最近几年的交易日历到全局集合中。
    修正了 akshare 接口返回 datetime.date 对象导致的类型错误。
    """
    global _TRADE_CALENDAR
    _TRADE_CALENDAR.clear()  # 清空旧的日历集合

    try:
        print("[系统] 正在加载交易日历...")

        # 1. 调用 AkShare 接口
        calendar_df = ak.tool_trade_date_hist_sina()

        if calendar_df.empty or 'trade_date' not in calendar_df.columns:
            raise ValueError("交易日历数据结构不正确或为空。")

        trade_dates = calendar_df['trade_date'].tolist()

        # 2. 防御性处理日期类型
        for d in trade_dates:
            if isinstance(d, str):
                # 如果是字符串 (例如 '20230101')，进行解析
                date_obj = datetime.datetime.strptime(d, '%Y%m%d').date()
            # 明确引用 datetime 模块中的 datetime.datetime 类，解决 isinstance 报错
            elif isinstance(d, datetime.datetime):
                # 如果是 datetime.datetime 对象
                date_obj = d.date()
            # 明确引用 datetime 模块中的 datetime.date 类
            elif isinstance(d, datetime.date):
                # 这是 AkShare 接口实际返回的类型，直接使用
                date_obj = d
            else:
                # 遇到其他类型，跳过
                continue

            _TRADE_CALENDAR.add(date_obj)

        print(f"[系统] 交易日历加载完成，共 {len(_TRADE_CALENDAR)} 个交易日。")

    except Exception as e:
        # 如果加载失败，设置为空集，is_trade_day 将返回 False，从而禁用实时追加
        print(f"[警告] 无法加载交易日历，实时数据追加功能将失效（将始终跳过）: {e}")

# ============================================================
# 工具：重试装饰器 (Retry Decorator)
# ============================================================
def retry(max_retries=10, delay=15):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries - 1:
                        raise
                    print(f"[警告] {func.__name__} 失败 ({type(e).__name__})，{delay}s 后重试...")
                    time.sleep(delay)

        return wrapper

    return decorator


# ============================================================
# 模块 4：获取/缓存 全市场股票列表
# ============================================================
@retry(max_retries=2, delay=1)
def fetch_stock_list_safe():
    print("[系统] 正在尝试获取全量股票列表...")
    try:
        df = ak.stock_info_a_code_name()
        if not df.empty and "code" in df.columns:
            print("[系统] 成功: 使用 stock_info_a_code_name 接口")
            return df[["code", "name"]]
    except Exception as e:
        print(f"[警告] 轻量接口失败 ({e})，尝试备用接口...")
    try:
        df = ak.stock_zh_a_spot_em()
        print("[系统] 成功: 使用 stock_zh_a_spot_em 接口")
        if '代码' in df.columns:
            df = df.rename(columns={'代码': 'code', '名称': 'name'})
        return df[["code", "name"]]
    except Exception as e:
        raise Exception(f"所有股票列表接口均不可用: {e}")


def get_stock_list_manager():
    cache_file = CONFIG["CACHE_FILE"]
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("time") == today_str:
                print(f"[系统] 加载本日缓存，共 {len(cache['data'])} 支股票")
                return pd.DataFrame(cache["data"])
        except Exception:
            pass

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
    df["code"] = df["code"].astype(str).str.zfill(6)
    mask = pd.Series(False, index=df.index)
    if CONFIG["EXCLUDE_GEM"]:
        mask |= df["code"].str.startswith(("300", "301"))
    if CONFIG["EXCLUDE_KCB"]:
        mask |= df["code"].str.startswith(("688", "689"))
    if CONFIG["EXCLUDE_BJ"]:
        mask |= df["code"].str.startswith(("8", "4", "92"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns:
        mask |= df["name"].str.contains("ST|退", na=False)
    return df[~mask]["code"].tolist()


# ============================================================
# 模块 5：技术指标（SQZMOM / linreg / true_range / color / sqz_id）
# ============================================================
def tv_linreg(y, length):
    if pd.isna(y).any(): return np.nan
    x = np.arange(length)
    y = y.values
    if len(y) < 2: return np.nan
    A = np.vstack([x, np.ones(length)]).T
    try:
        m, b = np.linalg.lstsq(A, y, rcond=None)[0]
    except np.linalg.LinAlgError:
        return np.nan
    return m * (length - 1) + b


def true_range(df):
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def get_color_cn(v, v_prev):
    if pd.isna(v) or pd.isna(v_prev): return None
    if v == 0: return "中性"
    if v > 0: return "强多" if v > v_prev else "弱多"
    return "强空" if v < v_prev else "弱空"


def add_squeeze_counter(df):
    counter = 0
    current_state = None
    sqz_id_list = []
    for status in df["sqz_status"]:
        if status in ["挤压", "释放"]:
            if status == current_state:
                counter += 1
            else:
                current_state = status
                counter = 1
            sqz_id_list.append(counter)
        else:
            current_state = None
            counter = 0
            sqz_id_list.append(0)
    df["sqz_id"] = sqz_id_list
    return df


def squeeze_momentum(df, length=None, mult=None, lengthKC=None, multKC=None, useTrueRange=True):
    if length is None: length = CONFIG["SQZ"]["length"]
    if mult is None: mult = CONFIG["SQZ"]["mult"]
    if lengthKC is None: lengthKC = CONFIG["SQZ"]["lengthKC"]
    if multKC is None: multKC = CONFIG["SQZ"]["multKC"]

    close, high, low = df['close'], df['high'], df['low']

    basis = close.rolling(length).mean()
    dev = multKC * close.rolling(length).std(ddof=0)
    upperBB, lowerBB = basis + dev, basis - dev
    bb_width = (upperBB - lowerBB) / basis.replace(0, np.nan)

    ma = close.rolling(lengthKC).mean()
    r = true_range(df) if useTrueRange else (high - low)
    rangema = r.rolling(lengthKC).mean()
    upperKC, lowerKC = ma + rangema * multKC, ma - rangema * multKC

    sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)
    df["sqz_status"] = np.select([sqzOn, sqzOff], ["挤压", "释放"], default="无")

    highest_h = high.rolling(lengthKC).max()
    lowest_l = low.rolling(lengthKC).min()
    avg_hl = (highest_h + lowest_l) / 2
    sma_close = close.rolling(lengthKC).mean()
    mid = (avg_hl + sma_close) / 2
    source_mid = close - mid

    val = source_mid.rolling(lengthKC).apply(lambda x: tv_linreg(pd.Series(x), lengthKC), raw=False)
    df["val"] = val
    df["val_prev"] = val.shift(1)
    df["val_color"] = df.apply(lambda r: get_color_cn(r["val"], r["val_prev"]), axis=1)

    df["BB_pct"] = bb_width
    df = add_squeeze_counter(df)
    return df


# ============================================================
# 模块 6：Pivot 高点（前阻力位）
# ============================================================
def calculate_pivot_high_vectorized(df, left=None, right=None):
    if left is None: left = CONFIG["PIVOT_LEFT"]
    if right is None: right = CONFIG["PIVOT_RIGHT"]

    highs = df['high'].values
    n = len(highs)
    pivots = np.full(n, np.nan)

    for i in range(left, n - right):
        left_max = np.max(highs[i - left:i])
        right_max = np.max(highs[i + 1:i + 1 + right])
        if highs[i] > left_max and highs[i] > right_max:
            pivots[i] = highs[i]

    return pd.Series(pivots, index=df.index).ffill()


# ============================================================
# 模块 7：今日实时K补充
# ============================================================
@retry(max_retries=10, delay=15)
def fetch_realtime_snapshot():
    """
    获取全市场实时行情快照 (akshare)，并标准化列名和代码格式。
    使用正则表达式剥离所有非数字前缀。
    """
    print("[系统] 正在尝试获取全市场实时行情快照...")
    try:
        df = ak.stock_zh_a_spot()
    except Exception as e:
        print(f"[错误] 获取实时快照失败: {e}")
        return pd.DataFrame()

    # --- 关键修正 1：统一列名映射 ---
    df = df.rename(columns={
        '代码': 'code',
        '最新价': 'close',
        '成交量': 'volume',
        '今开': 'open',
        '最高': 'high',
        '最低': 'low',
        '成交额': 'amount',
        '金额': 'amount',
    })

    # 强制转换为纯数字 code
    if 'code' in df.columns:
        # ----------------------------------------------------
        # 🆕 最终修正：使用正则表达式去除所有非数字字符
        # ----------------------------------------------------
        # 1. 转换为字符串，并替换所有非数字字符为空白 ('\D' 代表非数字)
        df['code'] = df['code'].astype(str).str.replace(r'\D', '', regex=True)

        # 2. 确保代码是 6 位字符串
        df['code'] = df['code'].str.zfill(6)
    else:
        print("[警告] 实时快照数据中缺少 '代码'/'code' 列。")
        return pd.DataFrame()

    # --- 后续列筛选、填充和类型转换不变 ---
    required_cols = ['code', 'open', 'high', 'low', 'close', 'volume', 'amount']

    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[required_cols]

    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    print(f"[系统] 成功获取 {len(df)} 条实时快照数据。")
    return df

def append_today_realtime_snapshot(code: str, df_daily: pd.DataFrame, df_spot: pd.DataFrame) -> pd.DataFrame:
    """
    将实时行情快照作为当日 K 线追加到历史日线数据中。
    增加了详细的调试信息。
    """

    latest_date = datetime.datetime.now().date()

    # ----------------------------------------------------
    # 调试点 1：检查是否为交易日
    # ----------------------------------------------------
    if not is_trade_day(latest_date):
        print(f"[{code}] 调试 1: {latest_date} 不是交易日，跳过拼接。")
        return df_daily

    # ----------------------------------------------------
    # 调试点 2：检查实时快照中是否存在该股票 (最常见失败原因)
    # ----------------------------------------------------
    spot_row = df_spot[df_spot['code'] == code]
    if spot_row.empty:
        print(f"[{code}] 调试 2: 实时快照 df_spot 中未找到该股票数据。")
        return df_daily

    latest_data = spot_row.iloc[0]  # latest_data 是一个 Series

    # ----------------------------------------------------
    # 调试点 3：检查历史数据是否已包含今日数据
    # ----------------------------------------------------
    if not df_daily.empty:
        df_daily_dates = pd.to_datetime(df_daily['date']).dt.date
        last_history_date = df_daily_dates.iloc[-1]

        if last_history_date == latest_date:
            print(f"[{code}] 调试 3: 历史数据中已包含 {latest_date} 的数据，跳过实时拼接。")
            return df_daily

        # 额外检查：确保历史数据日期是升序且连续的，否则可能导致错误判断
        if last_history_date > latest_date:
            print(f"[{code}] 警告: 历史数据日期 {last_history_date} 晚于当前日期 {latest_date}，跳过拼接。")
            return df_daily

    new_row_data = {
        'date': latest_date,
        # 直接使用标准化后的列名
        'open': latest_data.get('open'),
        'high': latest_data.get('high'),
        'low': latest_data.get('low'),
        'close': latest_data.get('close'),
        'volume': latest_data.get('volume'),
        'amount': latest_data.get('amount'),

        # 填充历史数据中其他需要的列
        'outstanding_share': None,
        'turnover': None,
        'adjust': CONFIG.get("ADJUST", ""),  # 假设 CONFIG 是可用的全局变量
        'code': code,
    }

    # 构造新的 DataFrame 行，并确保列名与历史数据一致
    # ⚠️ 必须确保 df_daily.columns 包含 new_row_data 中所有的键
    df_new_day = pd.DataFrame([new_row_data], columns=df_daily.columns)

    # 直接追加到历史数据末尾
    df_final = pd.concat([df_daily, df_new_day], ignore_index=True)

    return df_final


def fetch_data_with_timeout(symbol, start_date, end_date, adjust, timeout):
    def _fetch():
        if CONFIG["USE_LOCAL_MYSQL"]:
            try:
                return stock_zh_a_daily_mysql(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
            except NameError:
                print(f"\n[错误] {symbol} 尝试使用 MySQL 接口失败 (NameError)，自动降级到 AkShare。\n")
                pass

        return ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)

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


# ============================================================
# 模块 8：单只股票策略（整合） - 增加 "突破趋势" 和 "得分"
# ============================================================
def strategy_single_stock(code, start_date, end_date, df_spot):
    symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"

    try:
        df = fetch_data_with_timeout(symbol=symbol, start_date=start_date, end_date=end_date, adjust=CONFIG["ADJUST"],
                                     timeout=CONFIG["REQUEST_TIMEOUT"])

        if df is None or df.empty or len(df) < 220: return None

        # 调用实时股票行情拼接接口
        if CONFIG["USE_REAL_TIME_DATA"]:
            df = append_today_realtime_snapshot(code, df, df_spot)

        # 确保数据长度足够计算前3天
        if len(df) < 5: return None

        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        ma200_series = df['close'].rolling(200).mean()
        if pd.isna(ma200_series.iloc[-1]): return None
        ma200 = ma200_series.iloc[-1]

        pivot_series = calculate_pivot_high_vectorized(df)
        if pd.isna(pivot_series.iloc[-1]): return None
        last_pivot = pivot_series.iloc[-1]

        condition_trend = current_close > ma200
        condition_break = current_close > last_pivot
        condition_up = pct_chg > 0

        if not (condition_trend and condition_break and condition_up): return None

        df = squeeze_momentum(df, useTrueRange=CONFIG["SQZ"]["useTrueRange"])
        last = df.iloc[-1]
        prev = df.iloc[-2]

        break_strength = (current_close - last_pivot) / last_pivot * 100
        signal = "无"

        cond_now_strong_release = (
                last.get("val_color") == "强多" and
                last.get("sqz_status") == "释放" and
                int(last.get("sqz_id", 0)) == 1
        )
        cond_prev_squeeze_long = (
                prev.get("sqz_status") == "挤压" and
                int(prev.get("sqz_id", 0)) >= 6
        )

        if cond_now_strong_release and cond_prev_squeeze_long:
            signal = "买入"

        last_val = last.get("val")

        # ===================================================
        # 🆕 增加 "突破趋势" 和 "得分" 的计算逻辑
        # ===================================================

        # 获取倒数第 4, 3, 2 天的收盘价
        close_D1 = df['close'].iloc[-4]  # 倒数第4天
        close_D2 = df['close'].iloc[-3]  # 倒数第3天
        close_D3 = df['close'].iloc[-2]  # 倒数第2天

        # 规则：收盘价 > 前阻力位价格 (last_pivot) -> "高"；否则 "低"
        # 顺序：倒数第4天-倒数第3天-倒数第2天

        # 计算每个日期的突破状态 (0: 高, 1: 低)
        status_D1 = 0 if close_D1 > last_pivot else 1
        status_D2 = 0 if close_D2 > last_pivot else 1
        status_D3 = 0 if close_D3 > last_pivot else 1

        # 构造 "突破趋势" 字符串
        trend_D1 = "高" if status_D1 == 0 else "低"
        trend_D2 = "高" if status_D2 == 0 else "低"
        trend_D3 = "高" if status_D3 == 0 else "低"

        break_trend = f"{trend_D1}-{trend_D2}-{trend_D3}"

        # 计算 "得分" (低为 1 分，高为 0 分)
        score = status_D1 + status_D2 + status_D3
        # ===================================================

        return {
            "代码": code,
            "得分": score,
            # 🆕 新增结果
            "突破趋势": break_trend,
            "当前价": round(current_close, 2),
            "涨幅%": round(pct_chg, 2),
            "MA200": round(ma200, 2),
            "前阻力位": round(float(last_pivot), 2),
            "突破力度%": round(break_strength, 2),
            "BB值": None if pd.isna(last_val) else round(float(last_val), 2),
            "BB中文": last.get("val_color"),
            "信号": signal
        }

    except ThreadingTimeoutError:
        print(f"[超时] {code} 请求历史数据超时，跳过。")
        return None

    except Exception as e:
        print(f"[错误] {code} 处理失败: {e}")
        return None

# ============================================================
# 模块 9：并发扫描 (Async Scheduler)
# ============================================================
async def main_scanner_async(stock_codes, df_spot):
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    start_date = (datetime.datetime.now() - datetime.timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")

    results = []
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:

        tasks = [
            loop.run_in_executor(pool, strategy_single_stock, code, start_date, end_date, df_spot)
            for code in stock_codes
        ]

        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")
        for coro in pbar:
            res = await coro
            if res:
                results.append(res)
                pbar.set_postfix({"命中": len(results)})

    return results


async def batch_scan_manager_async(target_codes, df_spot):
    all_results = []
    batch_size = CONFIG.get("BATCH_SIZE", 120)
    interval = CONFIG.get("BATCH_INTERVAL_SEC", 8)
    total_stocks = len(target_codes)
    num_batches = math.ceil(total_stocks / batch_size)

    print(f"\n[调度器] 总计 {total_stocks} 支股票，将分为 {num_batches} 批次 ({batch_size} 支/批)。")

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_stocks)
        batch_codes = target_codes[start_index:end_index]

        print("\n" + "=" * 60)
        print(f"--- 🚀 开始处理批次 {i + 1}/{num_batches} ({len(batch_codes)} 支) ---")

        batch_results = await main_scanner_async(batch_codes, df_spot)
        all_results.extend(batch_results)

        if i < num_batches - 1:
            print(f"--- 😴 批次 {i + 1} 完成，当前总命中: {len(all_results)}，休息 {interval} 秒... ---")
            time.sleep(interval)
        else:
            print("--- 🎉 所有批次处理完成。---")

    return all_results


# ============================================================
# 模块 10：主入口
# ============================================================
def main():
    start_time = time.time()

    # --- 🆕 加载交易日历 (新增步骤) ---
    if CONFIG["USE_REAL_TIME_DATA"]:
        load_trade_calendar()

    # 使用 LogRedirector 启动日志管理
    with LogRedirector(folder=CONFIG['OUTPUT_FOLDER_BASE']) as log_redirector:

        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")
        print(f"\n[任务启动] 扫描范围: {start_date} ~ {end_date}")
        print(f"[配置] 目标线程: {CONFIG['MAX_WORKERS']} | 超时: {CONFIG['REQUEST_TIMEOUT']}s")

        # 1. 串行获取实时快照 (受开关控制)
        df_spot = pd.DataFrame()

        if CONFIG["USE_REAL_TIME_DATA"]:
            try:
                # 只有在交易日才尝试获取实时快照
                if is_trade_day:
                    df_spot = fetch_realtime_snapshot()
                    if df_spot.empty:
                        # ⚠️ 关键修改：当 df_spot 为空时，程序退出
                        print("[终止] 无法获取实时行情快照，终止扫描。")
                        sys.exit(1)
                else:
                    print("[配置] 当前为非交易日，跳过实时快照获取。")
            except Exception as e:
                # ⚠️ 关键修改：当 df_spot 为空时，程序退出
                print(f"[致命终止] 获取实时行情快照失败: {e}")
                sys.exit(1)
        else:
            print("[配置] 实时数据获取开关关闭 (USE_REAL_TIME_DATA=False)，跳过全市场快照获取。")

        # 2. 获取股票列表和过滤
        manual_list = CONFIG["MANUAL_STOCK_LIST"]
        df_base = pd.DataFrame()
        target_codes = []

        if manual_list and len(manual_list) > 0:
            target_codes = [str(c).zfill(6) for c in manual_list]
            print(f"[手动模式] 使用手动输入列表，共 {len(target_codes)} 支股票。")
            try:
                df_base = get_stock_list_manager()
            except Exception:
                df_base = pd.DataFrame({"code": target_codes, "name": ["未知"] * len(target_codes)})
        else:
            try:
                df_base = get_stock_list_manager()
            except Exception as e:
                print(f"[终止] 无法获取股票列表: {e}")
                return

            valid_codes = filter_stock_list(df_base)

            sample_size = CONFIG["SAMPLE_SIZE"]
            if isinstance(sample_size, int) and sample_size > 0 and len(valid_codes) > sample_size:
                print(f"[抽样模式] 随机抽取 {sample_size} 支股票进行测试...")
                target_codes = random.sample(valid_codes, sample_size)
            else:
                print(f"[全量模式] 扫描所有 {len(valid_codes)} 支有效股票...")
                target_codes = valid_codes

        # 3. 并发扫描
        if CONFIG["SAMPLE_SIZE"] > 0 or len(target_codes) <= CONFIG["BATCH_SIZE"]:
            final_data = asyncio.run(main_scanner_async(target_codes, df_spot))
        else:
            final_data = asyncio.run(batch_scan_manager_async(target_codes, df_spot))

        # 4. 结果整理与导出
        if final_data:
            res_df = pd.DataFrame(final_data)
            res_df = res_df[res_df["信号"] == "买入"].copy()

            if res_df.empty:
                print("\n[结果] 过滤后没有发现符合策略的股票。")
                return

            if not df_base.empty:
                name_map = dict(zip(df_base["code"].astype(str), df_base["name"]))
                res_df.insert(1, "名称", res_df["代码"].map(name_map).fillna("未知"))
            else:
                res_df.insert(1, "名称", "未知")

            # =============================================================
            # 🆕 排序逻辑修改：
            # 1. 主要排序键：'得分' (越高越好，ascending=False)
            # 2. 次要排序键：'涨幅%' (越高越好，ascending=False)
            # =============================================================
            res_df = res_df.sort_values(["得分", "涨幅%"], ascending=[False, False]).reset_index(drop=True)

            # 导出 CSV
            today_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
            folder_path = os.path.join(CONFIG["OUTPUT_FOLDER_BASE"], today_date_str)
            os.makedirs(folder_path, exist_ok=True)
            timestamp = datetime.datetime.now().strftime('%H%M%S')
            file_name = f"{CONFIG['OUTPUT_FILENAME_BASE']}_{timestamp}.csv"
            full_file_path = os.path.join(folder_path, file_name)
            res_df.to_csv(full_file_path, index=False, encoding=CONFIG["EXPORT_ENCODING"])

            print("\n" + "=" * 60)
            print(f"✅ 扫描完成 | 耗时: {time.time() - start_time:.1f}s")
            print(f"📄 结果文件已保存至: {full_file_path}")
            # 注意：日志文件的路径由 LogRedirector 自身的 __exit__ 方法在退出时打印
            print(f"📈 命中数量: {len(res_df)}")
            print("=" * 60)
            print("--- 命中股票 Top 10 ---")
            print(res_df.head(10).to_string(index=False))
            return res_df

        else:
            print("\n[结果] 没有发现符合策略的股票。")
            return pd.DataFrame()


if __name__ == "__main__":
    main()