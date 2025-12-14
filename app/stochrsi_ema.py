#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股突破扫描系统（StochRSI + EMA 趋势过滤 + ADX 趋势强度）
版本：v5.5 (增强版：多趋势层级 + 多 StochRSI 信号 + DI 相对强度)

【核心策略 V5.5】
1. StochRSI 信号: K 线上穿 (20/30/40) 超卖水平，且 K > D (金叉)。
2. 趋势过滤: 支持 Level 1 (C>E50>E200) 和 Level 2 (C>E20>E50, 启动趋势)。
3. ADX 趋势强度: ADX > 25 且 +DI > -DI，并通过 (DI+ - DI-)/ADX 进行相对强度确认。
4. 排除低价股: 股价低于 5.0 元的股票被排除。
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

from api.stock_query import stock_zh_a_daily_mysql

# ============================================================
# 模块 1：配置 (Configuration)
# ============================================================
CONFIG = {
    # --- 🆕 时间范围 ---
    "DAYS": 365,  # 扫描回溯天数 (用于计算 MA200/EMA200)

    # --- 🆕 过滤条件 (保持不变) ---
    "EXCLUDE_GEM": True,  # 排除创业板（300、301）
    "EXCLUDE_KCB": True,  # 排除科创板（688、689）
    "EXCLUDE_BJ": True,  # 排除北交所（8、4、92）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",  # 复权方式

    # --- 🚀 优化 1: 趋势过滤参数 (新增 E20) ---
    "EMA_SETTING": {
        "lengthEMA20": 20,  # 新增 E20 周期
        "lengthEMA50": 50,
        "lengthEMA200": 200,
        # Level 2 趋势是否启用 (C > E20 > E50 & E50 < E200)
        "LEVEL_2_TREND_ENABLED": True,
    },

    # --- 🚀 优化 2: StochRSI 参数 (支持多信号等级) ---
    "STOCH_RSI": {
        "lengthRSI": 14,
        "lengthStoch": 14,
        "smoothK": 3,
        "smoothD": 3,
        # 信号 1 (深度回调/高可靠)：要求 K 线上穿 20
        "oversoldLevel1": 20,
        # 信号 2 (中度回调/中可靠)：要求 K 线上穿 30
        "oversoldLevel2": 30,
        "LEVEL_2_SIGNAL_ENABLED": True,  # 是否启用中度回调信号 (Level 2)
    },

    # --- 🆕 ATR 止盈止损参数 (短线优化参数) ---
    "ATR_SETTING": {
        "lengthATR": 7,
        "stop_loss_multiplier": 2.0,  # 止损倍数 M
        "take_profit_multiplier": 4.0  # 止盈倍数
    },

    # --- 🚀 优化 3: ADX 趋势强度参数 (提高门槛 + DI相对强度) ---
    "ADX_SETTING": {
        "lengthADX": 14,
        "adx_threshold": 25.0,  # 提高至 25.0
        "di_relative_strength": 0.15,  # DI 相对强度门槛 ( (DI+ - DI-) / ADX )
    },

    # --- 🆕 文件路径/名称 (保持不变) ---
    "CACHE_FILE": "../conf/stock_list_cache.json",
    "EXPORT_ENCODING": "utf-8-sig",  # CSV文件导出编码
    "OUTPUT_FILENAME_BASE": "Buy_Stocks_StochRSI_EMA_ADX_ATR_V5.5",  # 输出文件前缀
    "OUTPUT_FOLDER_BASE": "../stocks",  # csv输出 文件夹
    "OUTPUT_LOG": "../logs",  # LogRedirector 日志输出文件夹

    # --- 🆕 并发 (保持不变) ---
    "MAX_WORKERS": 10,
    "REQUEST_TIMEOUT": 20,

    # --- 🆕 数据源控制 (保持不变) ---
    "USE_LOCAL_MYSQL": True,
    "USE_REAL_TIME_DATA": False,
    "SAMPLE_SIZE": 0,
    "BATCH_SIZE": 1000,
    "BATCH_INTERVAL_SEC": 1,

    # --- 🆕 手动输入 (保持不变) ---
    "MANUAL_STOCK_LIST": []
}


# ============================================================
# 模块 A：Pine Script 核心平滑函数 (保持不变)
# ============================================================
def pine_rma(series, length):
    """ RMA (Wilder's Smoothing) - 用于精确 RSI/ATR/ADX 计算 """
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    alpha = 1 / length
    return series.ewm(alpha=alpha, adjust=False).mean()


def pine_sma(series, length):
    """ Simple Moving Average (SMA) - 用于精确 StochRSI K/D 平滑 """
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    return series.rolling(length).mean()


def pine_ema(series, length):
    """ EMA (Exponential Moving Average) - 用于精确 EMA 20/50/200 趋势过滤 """
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    alpha = 2 / (length + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()


# ============================================================
# 模块 B：StochRSI 核心计算 & ATR & ADX 计算 (适应多信号)
# ============================================================
def calculate_stoch_rsi_values(series, length_rsi, length_stoch):
    """计算 StochRSI 的原始 K 值 (保持不变)"""
    if not isinstance(series, pd.Series):
        series = pd.Series(series)

    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    up_avg = pine_rma(up, length_rsi)
    down_avg = pine_rma(down, length_rsi)

    rs_arr = np.where(down_avg != 0, up_avg / down_avg, np.inf)
    rsi_arr = 100 - (100 / (1 + rs_arr))

    rsi = pd.Series(rsi_arr, index=series.index)

    lowest_rsi = rsi.rolling(length_stoch).min()
    highest_rsi = rsi.rolling(length_stoch).max()

    denominator = highest_rsi - lowest_rsi

    stoch_rsi_raw = np.where(denominator != 0, (rsi - lowest_rsi) / denominator, 0)
    stoch_rsi_raw = pd.Series(stoch_rsi_raw, index=series.index) * 100

    return stoch_rsi_raw


def check_stoch_rsi_signal(k, d, oversold_level):
    """检查单个超卖水平的 StochRSI 信号"""
    k_crossover_level = (k.shift(1) <= oversold_level) & (k > oversold_level)
    k_gt_d = (k > d)
    return k_crossover_level.iloc[-1] & k_gt_d.iloc[-1]


def calculate_stoch_rsi_signal_and_values(df, config):
    """计算 StochRSI K, D 值及多层级买入信号。"""
    stoch_rsi_raw = calculate_stoch_rsi_values(df['close'], config["lengthRSI"], config["lengthStoch"])

    k = pine_sma(stoch_rsi_raw, config["smoothK"])
    d = pine_sma(k, config["smoothD"])

    k_val = k.iloc[-1]
    d_val = d.iloc[-1]

    # 信号 1: 深度回调 (K 线上穿 20 且金叉)
    signal_level1 = check_stoch_rsi_signal(k, d, config["oversoldLevel1"])

    # 信号 2: 中度回调 (K 线上穿 30 且金叉)
    signal_level2 = False
    if config["LEVEL_2_SIGNAL_ENABLED"]:
        signal_level2 = check_stoch_rsi_signal(k, d, config["oversoldLevel2"])
        # 排除 Level 1 信号，避免重复计数 (若 Level 1 满足，则 Level 2 自动不算)
        if signal_level1:
            signal_level2 = False

    # 返回 K, D 值，以及两个信号等级
    return k_val, d_val, signal_level1, signal_level2


def calculate_atr(df, length=14):
    """ 计算 Average True Range (ATR)，使用 RMA (Wilder's Smoothing) 平滑。"""
    df_temp = df.copy()
    high = df_temp['high']
    low = df_temp['low']
    close_prev = df_temp['close'].shift(1)

    tr1 = high - low
    tr2 = (high - close_prev).abs()
    tr3 = (low - close_prev).abs()

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr_series = pine_rma(true_range, length)

    return atr_series.iloc[-1]


def calculate_adx_values(df, length=14):
    """
    计算 ADX, +DI (PDI) 和 -DI (MDI)，使用 RMA 平滑。
    (保持不变)
    """
    df_temp = df.copy()
    high = df_temp['high']
    low = df_temp['low']

    # Directional Movement (+DM 和 -DM)
    up = high - high.shift(1)
    down = low.shift(1) - low

    pdm = np.where((up > down) & (up > 0), up, 0)
    mdm = np.where((down > up) & (down > 0), down, 0)

    # 辅助计算 TR
    df['TR_Temp'] = pd.concat([high - low, (high - df['close'].shift(1)).abs(), (low - df['close'].shift(1)).abs()],
                              axis=1).max(axis=1)

    # 平滑
    atr_smooth = pine_rma(df['TR_Temp'], length)
    pdm_smooth = pine_rma(pd.Series(pdm, index=df.index), length)
    mdm_smooth = pine_rma(pd.Series(mdm, index=df.index), length)

    # DI
    pdi = (pdm_smooth / atr_smooth) * 100
    mdi = (mdm_smooth / atr_smooth) * 100

    # DX
    sum_di = pdi + mdi
    dx = np.where(sum_di != 0, (pdi - mdi).abs() / sum_di * 100, 0)

    # ADX
    adx = pine_rma(pd.Series(dx, index=df.index), length)

    # 返回最新的 ADX, PDI, MDI 值
    return adx.iloc[-1], pdi.iloc[-1], mdi.iloc[-1]


# ============================================================
# 模块 2 - 7 (LogRedirector, 交易日历, 重试装饰器, 股票列表, 实时数据)
# (保持不变)
# ============================================================

class LogRedirector:
    MAX_BYTES = 20 * 1024 * 1024

    def __init__(self, folder="stocks"):
        self.today_str = datetime.datetime.now().strftime('%Y%m%d')
        self.log_dir = os.path.join(folder, self.today_str)
        os.makedirs(self.log_dir, exist_ok=True)
        self.terminal = sys.stdout
        self.log_file = None
        self.current_log_path = None
        self.is_active = False

    def _get_new_log_path(self):
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f"{timestamp}.log"
        return os.path.join(self.log_dir, log_filename)

    def _check_and_rotate(self):
        if self.log_file is None:
            self.current_log_path = self._get_new_log_path()
            self.log_file = open(self.current_log_path, 'a', encoding='utf-8')
            return True
        if os.path.getsize(self.current_log_path) > self.MAX_BYTES:
            self.log_file.write(f"\n[轮换] 日志达到 {self.MAX_BYTES / 1024 / 1024:.0f}MB 限制，正在切换文件...\n")
            self.log_file.close()
            self.current_log_path = self._get_new_log_path()
            self.log_file = open(self.current_log_path, 'a', encoding='utf-8')
            return True
        return False

    def __enter__(self):
        try:
            self._check_and_rotate()
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
            self._check_and_rotate()
            if not message.startswith('\r'):
                self.log_file.write(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {message}")
            else:
                self.log_file.write(message)
            self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        if self.log_file: self.log_file.flush()


_TRADE_CALENDAR = set()


def is_trade_day(date_obj): return date_obj in _TRADE_CALENDAR


def load_trade_calendar():
    global _TRADE_CALENDAR
    _TRADE_CALENDAR.clear()
    try:
        print("[系统] 正在加载交易日历...")
        calendar_df = ak.tool_trade_date_hist_sina()
        if calendar_df.empty or 'trade_date' not in calendar_df.columns: raise ValueError(
            "交易日历数据结构不正确或为空。")
        trade_dates = calendar_df['trade_date'].tolist()
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


def retry(max_retries=10, delay=15):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries - 1: raise
                    print(f"[警告] {func.__name__} 失败 ({type(e).__name__})，{delay}s 后重试...")
                    time.sleep(delay)

        return wrapper

    return decorator


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
        if '代码' in df.columns: df = df.rename(columns={'代码': 'code', '名称': 'name'})
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
            data = {"time": today_str, "data": df.to_dict(orient="records")}
            json.dump(data, f, ensure_ascii=False, indent=2)
    return df


def filter_stock_list(df):
    if df is None or df.empty: return []
    df["code"] = df["code"].astype(str).str.zfill(6)
    mask = pd.Series(False, index=df.index)
    if CONFIG["EXCLUDE_GEM"]: mask |= df["code"].str.startswith(("300", "301"))
    if CONFIG["EXCLUDE_KCB"]: mask |= df["code"].str.startswith(("688", "689"))
    if CONFIG["EXCLUDE_BJ"]: mask |= df["code"].str.startswith(("8", "4", "92"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns: mask |= df["name"].str.contains("ST|退", na=False)
    return df[~mask]["code"].tolist()


@retry(max_retries=10, delay=15)
def fetch_realtime_snapshot():
    print("[系统] 正在尝试获取全市场实时行情快照...")
    try:
        df = ak.stock_zh_a_spot()
    except Exception as e:
        print(f"[错误] 获取实时快照失败: {e}")
        return pd.DataFrame()
    df = df.rename(
        columns={'代码': 'code', '最新价': 'close', '成交量': 'volume', '今开': 'open', '最高': 'high', '最低': 'low',
                 '成交额': 'amount', '金额': 'amount', })
    if 'code' in df.columns:
        df['code'] = df['code'].astype(str).str.replace(r'\D', '', regex=True)
        df['code'] = df['code'].str.zfill(6)
    else:
        return pd.DataFrame()
    required_cols = ['code', 'open', 'high', 'low', 'close', 'volume', 'amount']
    for col in required_cols:
        if col not in df.columns: df[col] = np.nan
    df = df[required_cols]
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce')
    print(f"[系统] 成功获取 {len(df)} 条实时快照数据。")
    return df


def append_today_realtime_snapshot(code: str, df_daily: pd.DataFrame, df_spot: pd.DataFrame) -> pd.DataFrame:
    latest_date = datetime.datetime.now().date()
    if not is_trade_day(latest_date): return df_daily
    spot_row = df_spot[df_spot['code'] == code]
    if spot_row.empty: return df_daily
    latest_data = spot_row.iloc[0]
    if not df_daily.empty:
        df_daily_dates = pd.to_datetime(df_daily['date']).dt.date
        last_history_date = df_daily_dates.iloc[-1]
        if last_history_date == latest_date: return df_daily
        if last_history_date > latest_date: return df_daily
    new_row_data = {
        'date': latest_date, 'open': latest_data.get('open'), 'high': latest_data.get('high'),
        'low': latest_data.get('low'), 'close': latest_data.get('close'),
        'volume': latest_data.get('volume'), 'amount': latest_data.get('amount'),
        'outstanding_share': None, 'turnover': None, 'adjust': CONFIG.get("ADJUST", ""), 'code': code,
    }
    try:
        df_new_day = pd.DataFrame([new_row_data], columns=df_daily.columns)
    except ValueError:
        df_new_day = pd.DataFrame([new_row_data])
    df_final = pd.concat([df_daily, df_new_day], ignore_index=True)
    return df_final


def fetch_data_with_timeout(symbol, start_date, end_date, adjust, timeout):
    def _fetch():
        if CONFIG["USE_LOCAL_MYSQL"]:
            try:
                return stock_zh_a_daily_mysql(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
            except NameError:
                print(f"\n[错误] {symbol} 尝试使用 MySQL 接口失败 (NameError)，自动降级到 AkShare。\n")
            except Exception as e:
                print(f"\n[错误] {symbol} 尝试使用 MySQL 接口失败 ({type(e).__name__})，自动降级到 AkShare。\n")
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
# 模块 8：单只股票策略 (V5.5 核心逻辑)
# ============================================================
def strategy_single_stock(code, start_date, end_date, df_spot):
    symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"

    try:
        df = fetch_data_with_timeout(symbol=symbol, start_date=start_date, end_date=end_date, adjust=CONFIG["ADJUST"],
                                     timeout=CONFIG["REQUEST_TIMEOUT"])

        # 检查数据长度
        if df is None or df.empty or len(df) < CONFIG["EMA_SETTING"]["lengthEMA200"] + 20: return None  # 预留空间

        # 实时数据拼接
        if CONFIG["USE_REAL_TIME_DATA"]:
            df = append_today_realtime_snapshot(code, df, df_spot)

        if len(df) < CONFIG["EMA_SETTING"]["lengthEMA200"]: return None

        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df.sort_values('date').reset_index(drop=True)
        current_close = df['close'].iloc[-1]
        current_low = df['low'].iloc[-1]

        # --- 价格过滤 (低于 5.0 元直接排除) ---
        if current_close < 5.0: return None

        # --- 策略核心计算 ---

        # 1. StochRSI 信号 (多层级)
        stoch_config = CONFIG["STOCH_RSI"]
        k_val, d_val, sig_level1, sig_level2 = calculate_stoch_rsi_signal_and_values(df, stoch_config)

        # 必须至少满足一个 StochRSI 信号
        if not (sig_level1 or sig_level2): return None

        # 2. EMA 趋势过滤 (多层级)
        ema_config = CONFIG["EMA_SETTING"]
        ema20 = pine_ema(df['close'], ema_config["lengthEMA20"]).iloc[-1]
        ema50 = pine_ema(df['close'], ema_config["lengthEMA50"]).iloc[-1]
        ema200 = pine_ema(df['close'], ema_config["lengthEMA200"]).iloc[-1]

        # 强势趋势 (Level 1): C > E50 > E200
        trend_level1 = (current_close > ema50) and (ema50 > ema200)

        # 启动趋势 (Level 2): C > E20 > E50, E50 < E200
        trend_level2 = False
        if ema_config["LEVEL_2_TREND_ENABLED"]:
            trend_level2 = (current_close > ema20) and (ema20 > ema50) and (ema50 < ema200)

        # 必须至少满足一个趋势过滤
        if not (trend_level1 or trend_level2): return None

        # 确定最终的信号描述
        if trend_level1 and sig_level1:
            signal_desc = "L1/L1 (强势/深回调)"
            trend_desc = "Level 1: C>E50>E200"
        elif trend_level1 and sig_level2:
            signal_desc = "L1/L2 (强势/浅回调)"
            trend_desc = "Level 1: C>E50>E200"
        elif trend_level2 and (sig_level1 or sig_level2):
            signal_desc = "L2/L(1/2) (启动/回调)"
            trend_desc = "Level 2: C>E20>E50"
        else:
            # 理论上不会发生，但作为保底
            return None

        # 3. ADX 趋势强度过滤 (ADX > 25.0 且 DI 相对强度)
        adx_config = CONFIG["ADX_SETTING"]
        adx_val, pdi_val, mdi_val = calculate_adx_values(df, length=adx_config["lengthADX"])

        # ADX 趋势强度 (ADX > 门槛) 且 方向正确 (+DI > -DI)
        adx_direction_filter = (adx_val > adx_config["adx_threshold"]) and (pdi_val > mdi_val)

        # DI 相对强度 (DI+ - DI-) / ADX > 0.15
        sum_di = pdi_val + mdi_val
        di_relative_strength = (pdi_val - mdi_val) / adx_val if adx_val != 0 else 0
        adx_relative_filter = (adx_val > 15) and (di_relative_strength >= adx_config["di_relative_strength"])

        # ADX 最终过滤：同时满足 ADX 门槛和 DI 相对强度（或 ADX 信号极强）
        adx_filter = adx_direction_filter and adx_relative_filter

        adx_signal = 'Buy' if adx_filter else '待确认'  # 新增待确认状态

        if not adx_filter: return None

        # 4. ATR 及止盈止损位
        atr_length = CONFIG["ATR_SETTING"]["lengthATR"]
        sl_mult = CONFIG["ATR_SETTING"]["stop_loss_multiplier"]
        tp_mult = CONFIG["ATR_SETTING"]["take_profit_multiplier"]

        current_atr = calculate_atr(df, length=atr_length)

        stop_loss_price = current_low - (sl_mult * current_atr)
        take_profit_price = current_close + (tp_mult * current_atr)

        # --- 满足所有条件，构建返回结果 ---

        pct_chg = (current_close / df['close'].iloc[-2] - 1) * 100 if len(df) >= 2 else 0

        return {
            "代码": code,
            "日期": df['date'].iloc[-1].strftime('%Y-%m-%d'),
            "信号等级": signal_desc,  # 🆕 信号等级
            "当前价": round(current_close, 2),
            "涨幅%": round(pct_chg, 2),
            "StochK": round(float(k_val), 2),
            "StochD": round(float(d_val), 2),
            "EMA20": round(float(ema20), 2),  # 🆕 EMA20
            "EMA50": round(float(ema50), 2),
            "EMA200": round(float(ema200), 2),
            "ADX": round(float(adx_val), 2),
            "DI+": round(float(pdi_val), 2),
            "DI-": round(float(mdi_val), 2),
            "DI相对强": round(float(di_relative_strength), 2),  # 🆕 DI 相对强度
            "ADX信号": adx_signal,
            "ATR": round(float(current_atr), 3),
            "止损价": round(stop_loss_price, 2),
            "止盈价": round(take_profit_price, 2),
            "趋势过滤": trend_desc,  # 🆕 趋势描述
        }

    except ThreadingTimeoutError:
        print(f"[超时] {code} 请求历史数据超时，跳过。")
        return None

    except Exception as e:
        print(f"[错误] {code} 处理失败: {type(e).__name__}: {e}")
        return None


# ============================================================
# 模块 9 & 10：并发扫描 & 主入口 (更新输出列)
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


def main():
    start_time = time.time()
    if CONFIG["USE_REAL_TIME_DATA"]: load_trade_calendar()

    with LogRedirector(folder=CONFIG['OUTPUT_LOG']) as log_redirector:
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")
        print(f"\n[任务启动] 扫描范围: {start_date} ~ {end_date}")
        print(f"[配置] 目标线程: {CONFIG['MAX_WORKERS']} | 超时: {CONFIG['REQUEST_TIMEOUT']}s")
        print(
            f"[配置] 趋势过滤: L1 (C>E50>E200), L2 (C>E20>E50) 启用={CONFIG['EMA_SETTING']['LEVEL_2_TREND_ENABLED']}")
        print(
            f"[配置] StochRSI: L1 (K>20), L2 (K>30) 启用={CONFIG['STOCH_RSI']['LEVEL_2_SIGNAL_ENABLED']}")
        print(
            f"[配置] ADX过滤: 门槛={CONFIG['ADX_SETTING']['adx_threshold']}, DI相对强度={CONFIG['ADX_SETTING']['di_relative_strength']}")

        df_spot = pd.DataFrame()
        if CONFIG["USE_REAL_TIME_DATA"]:
            try:
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

        if CONFIG["SAMPLE_SIZE"] > 0 or len(target_codes) <= CONFIG["BATCH_SIZE"]:
            final_data = asyncio.run(main_scanner_async(target_codes, df_spot))
        else:
            final_data = asyncio.run(batch_scan_manager_async(target_codes, df_spot))

        # 4. 结果整理与导出
        if final_data:
            res_df = pd.DataFrame(final_data)

            name_map = df_base.set_index('code')['name'].to_dict()
            res_df['名称'] = res_df['代码'].map(name_map).fillna('未知')

            today_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
            folder_path = os.path.join(CONFIG["OUTPUT_FOLDER_BASE"], today_date_str)
            os.makedirs(folder_path, exist_ok=True)
            timestamp = datetime.datetime.now().strftime('%H%M%S')
            file_name = f"{CONFIG['OUTPUT_FILENAME_BASE']}_{timestamp}.csv"
            full_file_path = os.path.join(folder_path, file_name)

            # 重新排序结果列 (新增 EMA20, 信号等级, DI相对强度)
            ordered_cols = [
                "日期", "代码", "名称", "信号等级", "趋势过滤",
                "当前价", "涨幅%",
                "StochK", "StochD", "EMA20", "EMA50", "EMA200",
                "ADX", "DI+", "DI-", "DI相对强", "ADX信号",
                "ATR", "止损价", "止盈价",
            ]

            # 确保只包含实际存在的列
            ordered_cols = [col for col in ordered_cols if col in res_df.columns]
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