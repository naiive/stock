#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股突破扫描系统（StochRSI + EMA 趋势过滤）
版本：v3.0 (修复 numpy.ndarray 错误)

【核心策略】
1. StochRSI 信号: K 线上穿超卖水平 (默认 20)，且 K > D (金叉)。
2. 趋势过滤: 当前收盘价 > EMA50 > EMA200 (强势上涨趋势)。
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

# --- 导入交易数据接口 (保持不变) ---
try:
    # 尝试从本地导入自定义数据接口
    from api.stock_query import stock_zh_a_daily_mysql
except ImportError:
    # 警告：如果无法导入，则定义一个替代函数，并允许降级到 AkShare
    print("[警告] 无法导入 stock_zh_a_daily_mysql。请确保您的 stock_query.py 文件存在。")


    def stock_zh_a_daily_mysql(*args, **kwargs):
        raise NameError("stock_zh_a_daily_mysql 尚未定义或导入失败。")


# ============================================================
# 模块 A：Pine Script 核心平滑函数 (StochRSI/EMA 计算基础)
# **【修复 1：增加 Series 类型检查和转换】**
# ============================================================
def pine_rma(series, length):
    """ RMA (Wilder's Smoothing) - 用于精确 RSI 计算 """
    # 强制转换为 Series 以确保 .ewm() 可用
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    alpha = 1 / length
    return series.ewm(alpha=alpha, adjust=False).mean()


def pine_sma(series, length):
    """ Simple Moving Average (SMA) - 用于精确 StochRSI K/D 平滑 """
    # 强制转换为 Series 以确保 .rolling() 可用
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    return series.rolling(length).mean()


def pine_ema(series, length):
    """ EMA (Exponential Moving Average) - 用于精确 EMA 50/200 趋势过滤 """
    # 强制转换为 Series 以确保 .ewm() 可用
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    alpha = 2 / (length + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()


# ============================================================
# 模块 B：StochRSI 核心计算
# **【修复 2：将 NumPy 数组计算结果转换回 Pandas Series】**
# ============================================================
def calculate_stoch_rsi_values(series, length_rsi, length_stoch):
    """计算 StochRSI 的原始 K 值 (未平滑)"""
    # 强制输入为 Series，并保持索引
    if not isinstance(series, pd.Series):
        series = pd.Series(series)

    # 1. 计算 RSI: 使用 RMA 模拟 ta.rsi 内部的平滑逻辑
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    up_avg = pine_rma(up, length_rsi)
    down_avg = pine_rma(down, length_rsi)

    # 避免除以零导致警告
    rs_arr = np.where(down_avg != 0, up_avg / down_avg, np.inf)
    rsi_arr = 100 - (100 / (1 + rs_arr))

    # ！！！关键修正！！！：将 NumPy 数组转换回 Pandas Series，并携带原始索引
    rsi = pd.Series(rsi_arr, index=series.index)

    # 2. 计算 StochRSI
    lowest_rsi = rsi.rolling(length_stoch).min()
    highest_rsi = rsi.rolling(length_stoch).max()

    denominator = highest_rsi - lowest_rsi

    # 这里的 stoch_rsi_raw 已经是 Series
    stoch_rsi_raw = np.where(denominator != 0, (rsi - lowest_rsi) / denominator, 0)
    stoch_rsi_raw = pd.Series(stoch_rsi_raw, index=series.index) * 100

    return stoch_rsi_raw


def calculate_stoch_rsi_signal_and_values(df, length_rsi=14, length_stoch=14, smooth_k=3, smooth_d=3,
                                          oversold_level=20):
    """
    计算 StochRSI K, D 值及超卖突破买入信号。
    返回当前最新的 K 值, D 值, 以及布尔型信号。
    """
    stoch_rsi_raw = calculate_stoch_rsi_values(df['close'], length_rsi, length_stoch)

    k = pine_sma(stoch_rsi_raw, smooth_k)
    d = pine_sma(k, smooth_d)

    # 核心信号逻辑 (金叉和超卖区突破)
    k_crossover_level = (k.shift(1) <= oversold_level) & (k > oversold_level)
    k_gt_d = (k > d)
    buy_signal_raw = k_crossover_level & k_gt_d

    # 返回最新的 K 值, D 值和信号 (布尔值)
    # iloc[-1] 确保返回的是数值而非 Series
    return k.iloc[-1], d.iloc[-1], buy_signal_raw.iloc[-1]


# ============================================================
# 模块 1：配置 (Configuration) (保持不变)
# ============================================================
CONFIG = {
    # --- 🆕 时间范围 ---
    "DAYS": 365,  # 扫描回溯天数 (用于计算 MA200/EMA200)

    # --- 🆕 过滤条件 ---
    "EXCLUDE_GEM": True,  # 排除创业板（300、301）
    "EXCLUDE_KCB": True,  # 排除科创板（688、689）
    "EXCLUDE_BJ": True,  # 排除北交所（8、4、92）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",  # 复权方式

    # --- 🆕 StochRSI 参数 ---
    "STOCH_RSI": {
        "lengthRSI": 14,
        "lengthStoch": 14,
        "smoothK": 3,
        "smoothD": 3,
        "oversoldLevel": 20
    },

    # --- 🆕 文件路径/名称 ---
    "CACHE_FILE": "../conf/stock_list_cache.json",
    "EXPORT_ENCODING": "utf-8-sig",  # CSV文件导出编码
    "OUTPUT_FILENAME_BASE": "Buy_Stocks_StochRSI_EMA",  # 输出文件前缀
    "OUTPUT_FOLDER_BASE": "../stocks",  # csv输出 文件夹
    "OUTPUT_LOG": "../logs",  # LogRedirector 日志输出文件夹

    # --- 🆕 并发 ---
    "MAX_WORKERS": 10,  # 降低线程数到 10
    "REQUEST_TIMEOUT": 20,  # 增加超时时间到 20s

    # --- 🆕 数据源控制 ---
    "USE_LOCAL_MYSQL": True,

    # --- 🆕 实时数据开关 ---
    "USE_REAL_TIME_DATA": False,

    # --- 🆕 是否全量/分批控制 ---
    "SAMPLE_SIZE": 0,  # 0 或 None 表示全量
    "BATCH_SIZE": 1000,  # SAMPLE_SIZE 全量才开启分批功能，每批次处理的股票数量
    "BATCH_INTERVAL_SEC": 1,  # 批次间隔休息时间（秒）

    # --- 🆕 手动输入 ---
    "MANUAL_STOCK_LIST": []
}


# ============================================================
# 模块 2：日志重定向类 (LogRedirector) (保持不变)
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
        self.log_dir = os.path.join(folder, self.today_str)
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
            self.write(
                f"\n{'=' * 70}\n[会话结束] {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 70}\n")
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
# 模块 3：交易日历相关的全局变量和函数 (保持不变)
# ============================================================
_TRADE_CALENDAR = set()


def is_trade_day(date_obj):
    """检查给定的日期是否在已加载的交易日历中"""
    return date_obj in _TRADE_CALENDAR


def load_trade_calendar():
    """
    加载最近几年的交易日历到全局集合中。
    """
    global _TRADE_CALENDAR
    _TRADE_CALENDAR.clear()  # 清空旧的日历集合

    try:
        print("[系统] 正在加载交易日历...")

        calendar_df = ak.tool_trade_date_hist_sina()

        if calendar_df.empty or 'trade_date' not in calendar_df.columns:
            raise ValueError("交易日历数据结构不正确或为空。")

        trade_dates = calendar_df['trade_date'].tolist()

        # 防御性处理日期类型
        for d in trade_dates:
            if isinstance(d, str):
                date_obj = datetime.datetime.strptime(d, '%Y%m%d').date()
            elif isinstance(d, datetime.datetime):
                date_obj = d.date()
            elif isinstance(d, datetime.date):
                date_obj = d
            else:
                continue

            _TRADE_CALENDAR.add(date_obj)

        print(f"[系统] 交易日历加载完成，共 {len(_TRADE_CALENDAR)} 个交易日。")

    except Exception as e:
        print(f"[警告] 无法加载交易日历，实时数据追加功能可能失效: {e}")


# ============================================================
# 工具：重试装饰器 (Retry Decorator) (保持不变)
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
# 模块 4：获取/缓存 全市场股票列表 (保持不变)
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
# 模块 7：今日实时K补充 (保持不变)
# ============================================================
@retry(max_retries=10, delay=15)
def fetch_realtime_snapshot():
    """
    获取全市场实时行情快照 (akshare)，并标准化列名和代码格式。
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
        df['code'] = df['code'].astype(str).str.replace(r'\D', '', regex=True)
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
    """

    latest_date = datetime.datetime.now().date()

    if not is_trade_day(latest_date):
        # print(f"[{code}] 调试: {latest_date} 不是交易日，跳过拼接。")
        return df_daily

    spot_row = df_spot[df_spot['code'] == code]
    if spot_row.empty:
        # print(f"[{code}] 调试: 实时快照 df_spot 中未找到该股票数据。")
        return df_daily

    latest_data = spot_row.iloc[0]

    if not df_daily.empty:
        df_daily_dates = pd.to_datetime(df_daily['date']).dt.date
        last_history_date = df_daily_dates.iloc[-1]

        if last_history_date == latest_date:
            # print(f"[{code}] 调试: 历史数据中已包含 {latest_date} 的数据，跳过实时拼接。")
            return df_daily

        if last_history_date > latest_date:
            # print(f"[{code}] 警告: 历史数据日期 {last_history_date} 晚于当前日期 {latest_date}，跳过拼接。")
            return df_daily

    # 构造新的 DataFrame 行，确保列名与历史数据一致
    new_row_data = {
        'date': latest_date,
        'open': latest_data.get('open'),
        'high': latest_data.get('high'),
        'low': latest_data.get('low'),
        'close': latest_data.get('close'),
        'volume': latest_data.get('volume'),
        'amount': latest_data.get('amount'),

        # 填充历史数据中其他需要的列
        'outstanding_share': None,
        'turnover': None,
        'adjust': CONFIG.get("ADJUST", ""),
        'code': code,
    }

    # ⚠️ 必须确保 df_daily.columns 包含 new_row_data 中所有的键
    try:
        df_new_day = pd.DataFrame([new_row_data], columns=df_daily.columns)
    except ValueError:
        # 如果列名不匹配，可能是历史数据 df_daily 结构不标准，尝试使用快照数据提供的列
        df_new_day = pd.DataFrame([new_row_data])

    df_final = pd.concat([df_daily, df_new_day], ignore_index=True)

    return df_final


def fetch_data_with_timeout(symbol, start_date, end_date, adjust, timeout):
    def _fetch():
        if CONFIG["USE_LOCAL_MYSQL"]:
            try:
                # 尝试使用本地 MySQL 接口
                return stock_zh_a_daily_mysql(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
            except NameError:
                # 如果 NameError 说明本地接口未定义或导入失败，降级
                print(f"\n[错误] {symbol} 尝试使用 MySQL 接口失败 (NameError)，自动降级到 AkShare。\n")
                pass
            except Exception as e:
                # 其他连接或查询错误，降级
                print(f"\n[错误] {symbol} 尝试使用 MySQL 接口失败 ({type(e).__name__})，自动降级到 AkShare。\n")
                pass

        # 使用 AkShare 接口
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
# 模块 8：单只股票策略（整合 StochRSI + EMA 过滤）(保持不变，但依赖于修复后的模块 A/B)
# ============================================================
def strategy_single_stock(code, start_date, end_date, df_spot):
    symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"

    try:
        df = fetch_data_with_timeout(symbol=symbol, start_date=start_date, end_date=end_date, adjust=CONFIG["ADJUST"],
                                     timeout=CONFIG["REQUEST_TIMEOUT"])

        # 确保数据长度足够计算 EMA200 (至少 200 + 1)
        if df is None or df.empty or len(df) < 220: return None

        # 调用实时股票行情拼接接口
        if CONFIG["USE_REAL_TIME_DATA"]:
            df = append_today_realtime_snapshot(code, df, df_spot)

        # 确保数据长度在拼接后仍足够
        if len(df) < 220: return None

        # 确保日期列是 datetime.date 或可排序类型
        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df.sort_values('date').reset_index(drop=True)

        # --- 策略核心计算：StochRSI + EMA 趋势过滤 ---

        # 1. 计算 StochRSI 信号
        # 这会调用已修复的 calculate_stoch_rsi_signal_and_values
        k_val, d_val, stoch_rsi_buy_signal = calculate_stoch_rsi_signal_and_values(
            df,
            length_rsi=CONFIG["STOCH_RSI"]["lengthRSI"],
            length_stoch=CONFIG["STOCH_RSI"]["lengthStoch"],
            smooth_k=CONFIG["STOCH_RSI"]["smoothK"],
            smooth_d=CONFIG["STOCH_RSI"]["smoothD"],
            oversold_level=CONFIG["STOCH_RSI"]["oversoldLevel"]
        )

        # 如果没有 StochRSI 信号，则直接返回 None
        if not stoch_rsi_buy_signal:
            return None

        # 2. 计算 EMA 趋势
        # 这会调用已修复的 pine_ema
        ema50 = pine_ema(df['close'], 50).iloc[-1]
        ema200 = pine_ema(df['close'], 200).iloc[-1]
        current_close = df['close'].iloc[-1]

        # 3. 趋势过滤条件: close > EMA50 > EMA200
        # ⚠️ 注意：此处要求严格的 C > E50 > E200 顺序
        trend_filter = (current_close > ema50) and (ema50 > ema200)

        # 4. 最终信号判定
        if not trend_filter:
            # StochRSI 信号命中，但趋势不满足，跳过
            return None

        # --- 满足所有条件，构建返回结果 ---

        # 计算涨幅 (与前一日相比)
        pct_chg = (current_close / df['close'].iloc[-2] - 1) * 100 if len(df) >= 2 else 0

        return {
            "代码": code,
            "日期": df['date'].iloc[-1].strftime('%Y-%m-%d'),
            "信号": 'StochRSI/EMA Buy',
            "当前价": round(current_close, 2),
            "涨幅%": round(pct_chg, 2),
            "StochK": round(float(k_val), 2),
            "StochD": round(float(d_val), 2),
            "EMA50": round(float(ema50), 2),
            "EMA200": round(float(ema200), 2),
            "趋势过滤": "满足 (C>E50>E200)",
        }

    except ThreadingTimeoutError:
        print(f"[超时] {code} 请求历史数据超时，跳过。")
        return None

    except Exception as e:
        # 打印详细错误信息，帮助调试
        print(f"[错误] {code} 处理失败: {type(e).__name__}: {e}")
        return None


# ============================================================
# 模块 9：并发扫描 (Async Scheduler) (保持不变)
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
    batch_size = CONFIG.get("BATCH_SIZE", 500)
    interval = CONFIG.get("BATCH_INTERVAL_SEC", 2)
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
# 模块 10：主入口 (保持不变)
# ============================================================
def main():
    start_time = time.time()

    # --- 🆕 加载交易日历 (新增步骤) ---
    if CONFIG["USE_REAL_TIME_DATA"]:
        load_trade_calendar()

    # 使用 LogRedirector 启动日志管理
    with LogRedirector(folder=CONFIG['OUTPUT_LOG']) as log_redirector:

        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")
        print(f"\n[任务启动] 扫描范围: {start_date} ~ {end_date}")
        print(f"[配置] 目标线程: {CONFIG['MAX_WORKERS']} | 超时: {CONFIG['REQUEST_TIMEOUT']}s")

        # 1. 串行获取实时快照 (受开关控制)
        df_spot = pd.DataFrame()

        if CONFIG["USE_REAL_TIME_DATA"]:
            try:
                # 检查当前日期是否为交易日 (is_trade_day 函数需要一个 date 对象)
                today_date = datetime.datetime.now().date()
                if is_trade_day(today_date):
                    df_spot = fetch_realtime_snapshot()
                    if df_spot.empty:
                        print("[终止] 无法获取实时行情快照，终止扫描。")
                        sys.exit(1)
                else:
                    print("[配置] 当前为非交易日，跳过实时快照获取。")
            except Exception as e:
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
        # 如果是抽样模式或总数小于批次大小，则直接运行，否则使用批次管理
        if CONFIG["SAMPLE_SIZE"] > 0 or len(target_codes) <= CONFIG["BATCH_SIZE"]:
            final_data = asyncio.run(main_scanner_async(target_codes, df_spot))
        else:
            final_data = asyncio.run(batch_scan_manager_async(target_codes, df_spot))

        # 4. 结果整理与导出
        if final_data:
            res_df = pd.DataFrame(final_data)

            # 合并股票名称
            name_map = df_base.set_index('code')['name'].to_dict()
            res_df['名称'] = res_df['代码'].map(name_map).fillna('未知')

            # 导出 CSV
            today_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
            folder_path = os.path.join(CONFIG["OUTPUT_FOLDER_BASE"], today_date_str)
            os.makedirs(folder_path, exist_ok=True)
            timestamp = datetime.datetime.now().strftime('%H%M%S')
            file_name = f"{CONFIG['OUTPUT_FILENAME_BASE']}_{timestamp}.csv"
            full_file_path = os.path.join(folder_path, file_name)

            # 重新排序结果列
            ordered_cols = ["代码", "名称", "日期", "当前价", "涨幅%", "StochK", "StochD", "EMA50", "EMA200"]
            res_df = res_df[ordered_cols]

            res_df = res_df.sort_values(["涨幅%"], ascending=[False]).reset_index(drop=True)


            res_df.to_csv(full_file_path, index=False, encoding=CONFIG["EXPORT_ENCODING"])

            print("\n" + "=" * 60)
            print(f"✅ 扫描完成 | 耗时: {time.time() - start_time:.1f}s")
            print(f"📄 结果文件已保存至: {full_file_path}")
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