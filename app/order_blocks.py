#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
查询当前收盘价是否在最近的订单块，没有预测功能
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
    "DAYS": 365,  # 扫描回溯天数 (用于计算 MA200)

    # --- 🆕 过滤条件 ---
    "EXCLUDE_GEM": True,  # 排除创业板（300、301）
    "EXCLUDE_KCB": True,  # 排除科创板（688、689）
    "EXCLUDE_BJ": True,  # 排除北交所（8、4、92）
    "EXCLUDE_ST": False,  # 排除 ST/退
    "ADJUST": "qfq",  # 复权方式

    # --- 🆕 OrderBlock 策略参数 ---
    "OB_STRATEGY": {
        "ENABLED": True,
        "PERIODS": 5,
        "THRESHOLD": 0.0,  # 突破阈值，例如 0.5 表示后续 K 线突破幅度需大于 0.5%
        "USE_WICKS": False
    },

    # --- 🆕 文件路径/名称 ---
    "CACHE_FILE": "../conf/stock_list_cache.json",
    "EXPORT_ENCODING": "utf-8-sig",  # CSV文件导出编码
    "OUTPUT_FILENAME_BASE": "Buy_Stocks_ORDER_BLOCKS",  # 输出文件前缀
    "OUTPUT_FOLDER_BASE": "../stocks",  # csv输出 文件夹
    "OUTPUT_LOG": "../logs",  # LogRedirector 日志输出文件夹

    # --- 🆕 并发 ---
    "MAX_WORKERS": 10,  # 降低线程数到 10
    "REQUEST_TIMEOUT": 20,  # 增加超时时间到 20s

    # --- 🆕 数据源控制 ---
    # True:  使用本地 stock_zh_a_daily_mysql 函数
    # False: 使用 ak.stock_zh_a_daily (AkShare)
    "USE_LOCAL_MYSQL": True,

    # --- 🆕 实时数据开关 ---
    # True:  使用腾讯实时股票全量接口 (fetch_realtime_snapshot)
    # False: 不使用，跳过实时数据获取（用于离线回测或非交易日）
    "USE_REAL_TIME_DATA": False,

    # --- 🆕 是否全量/分批控制 ---
    "SAMPLE_SIZE": 0,  # 0 或 None 表示全量
    "BATCH_SIZE": 500,  # SAMPLE_SIZE 全量才开启分批功能，每批次处理的股票数量
    "BATCH_INTERVAL_SEC": 2,  # 批次间隔休息时间（秒）

    # --- 🆕 手动输入 ---
    # 示例: ["600519", "000001", "300751"]。如果非空，则跳过全量扫描。
    "MANUAL_STOCK_LIST": [],

    # --- 🆕 手动输入 ---
    # 如果指定一个日期字符串 (e.g., "2024-06-01")，则扫描该日信号。
    # 如果为 None，则扫描最新交易日信号。
    "TARGET_DATE": ""
}


# ============================================================
# 日志管理
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
        return os.path.join(self.log_dir, f"{timestamp}.log")

    def _check_and_rotate(self):
        if self.log_file is None or os.path.getsize(self.current_log_path) > self.MAX_BYTES:
            if self.log_file: self.log_file.close()
            self.current_log_path = self._get_new_log_path()
            self.log_file = open(self.current_log_path, 'a', encoding='utf-8')

    def __enter__(self):
        self._check_and_rotate()
        sys.stdout = self
        self.is_active = True
        self.write(f"\n[会话开始] {datetime.datetime.now()}\n日志文件: {self.current_log_path}\n")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_active:
            self.write(f"\n[会话结束] {datetime.datetime.now()}\n")
            sys.stdout = self.terminal
            if self.log_file: self.log_file.close()
            print(f"日志文件已保存: {self.current_log_path}")

    def write(self, message):
        self.terminal.write(message)
        self.terminal.flush()
        if self.log_file:
            self._check_and_rotate()
            self.log_file.write(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {message}")
            self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        if self.log_file: self.log_file.flush()


# ============================================================
# 交易日历
# ============================================================
_TRADE_CALENDAR = set()


def is_trade_day(date_obj):
    return date_obj in _TRADE_CALENDAR


def load_trade_calendar():
    global _TRADE_CALENDAR
    _TRADE_CALENDAR.clear()
    try:
        print("[系统] 加载交易日历...")
        df = ak.tool_trade_date_hist_sina()
        for d in df['trade_date']:
            if isinstance(d, str):
                _TRADE_CALENDAR.add(datetime.datetime.strptime(d, '%Y%m%d').date())
            elif isinstance(d, datetime.datetime):
                _TRADE_CALENDAR.add(d.date())
            elif isinstance(d, datetime.date):
                _TRADE_CALENDAR.add(d)
        print(f"[系统] 交易日历加载完成: {len(_TRADE_CALENDAR)} 个交易日")
    except Exception as e:
        print(f"[警告] 交易日历加载失败: {e}")


# ============================================================
# 重试装饰器
# ============================================================
def retry(max_retries=10, delay=15):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries - 1: raise
                    print(f"[警告] {func.__name__}失败({e})，{delay}s后重试...")
                    time.sleep(delay)
            raise Exception("Retry limit exceeded")

        return wrapper

    return decorator


# ============================================================
# 股票列表
# ============================================================
@retry(max_retries=2, delay=1)
def fetch_stock_list_safe():
    try:
        df = ak.stock_info_a_code_name()
        return df[["code", "name"]]
    except:
        df = ak.stock_zh_a_spot_em()
        df = df.rename(columns={'代码': 'code', '名称': 'name'})
        return df[["code", "name"]]


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
        except:
            pass
    df = fetch_stock_list_safe()
    if not df.empty:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump({"time": today_str, "data": df.to_dict(orient="records")}, f, ensure_ascii=False, indent=2)
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


# ============================================================
# 实时快照
# ============================================================
@retry(max_retries=10, delay=15)
def fetch_realtime_snapshot():
    print("[系统] 获取实时行情快照...")
    try:
        df = ak.stock_zh_a_spot()
    except:
        return pd.DataFrame()
    df = df.rename(
        columns={'代码': 'code', '最新价': 'close', '今开': 'open', '最高': 'high', '最低': 'low', '成交量': 'volume'})
    df['code'] = df['code'].astype(str).str.replace(r'\D', '', regex=True).str.zfill(6)
    return df[['code', 'open', 'high', 'low', 'close', 'volume']]


def append_today_realtime_snapshot(code, df_daily, df_spot):
    latest_date = datetime.datetime.now().date()
    if not is_trade_day(latest_date): return df_daily
    spot_row = df_spot[df_spot['code'] == code]
    if spot_row.empty: return df_daily
    latest_data = spot_row.iloc[0]
    if not df_daily.empty and pd.to_datetime(df_daily['date']).dt.date.iloc[-1] == latest_date:
        return df_daily
    new_row = {
        'date': latest_date,
        'open': latest_data['open'],
        'high': latest_data['high'],
        'low': latest_data['low'],
        'close': latest_data['close'],
        'volume': latest_data['volume'],
        'code': code
    }
    df_new = pd.DataFrame([new_row], columns=df_daily.columns)
    return pd.concat([df_daily, df_new], ignore_index=True)


# ============================================================
# Order Block 策略
# ============================================================
def order_blocks(df, periods=5, threshold=0.0, use_wicks=False):
    """
    完全模拟 Pine Script 逻辑的 Order Block
    Bullish OB: 最后一根下降 K 后 periods 根上涨 K
    Bearish OB: 最后一根上涨 K 后 periods 根下降 K
    """
    df = df.copy()
    df['OB'] = False
    df['OB_type'] = None
    df['OB_high'] = np.nan
    df['OB_low'] = np.nan
    df['OB_mid'] = np.nan

    n = len(df)
    for i in range(n - periods - 1):
        # OB candle
        ob_candle = df.iloc[i]
        # 随后的 periods 根 K
        future_candles = df.iloc[i+1:i+1+periods]

        # Bullish OB
        if ob_candle['close'] < ob_candle['open']:
            if (future_candles['close'] > future_candles['open']).all():
                move = abs(future_candles['close'].iloc[-1] - ob_candle['close']) / ob_candle['close'] * 100
                if move >= threshold:
                    df.loc[df.index[i], 'OB'] = True
                    df.loc[df.index[i], 'OB_type'] = 'Bullish'
                    df.loc[df.index[i], 'OB_high'] = ob_candle['high'] if use_wicks else ob_candle['open']
                    df.loc[df.index[i], 'OB_low'] = ob_candle['low']
                    df.loc[df.index[i], 'OB_mid'] = (df.loc[df.index[i], 'OB_high'] + df.loc[df.index[i], 'OB_low']) / 2

        # Bearish OB
        if ob_candle['close'] > ob_candle['open']:
            if (future_candles['close'] < future_candles['open']).all():
                move = abs(future_candles['close'].iloc[-1] - ob_candle['close']) / ob_candle['close'] * 100
                if move >= threshold:
                    df.loc[df.index[i], 'OB'] = True
                    df.loc[df.index[i], 'OB_type'] = 'Bearish'
                    df.loc[df.index[i], 'OB_high'] = ob_candle['high']
                    df.loc[df.index[i], 'OB_low'] = ob_candle['low'] if use_wicks else ob_candle['open']
                    df.loc[df.index[i], 'OB_mid'] = (df.loc[df.index[i], 'OB_high'] + df.loc[df.index[i], 'OB_low']) / 2

    return df


def ob_signal(df):
    """
    返回最近一根 K 的 OB 信号（收盘确认）
    """
    df = df.copy()
    today = df.iloc[-1]
    signal = None

    # 最近一个 OB
    ob_df = df[df['OB']]
    if ob_df.empty:
        return None

    ob = ob_df.iloc[-1]

    # Bullish OB 信号
    if ob['OB_type'] == 'Bullish':
        touch = (today['low'] <= ob['OB_high']) and (today['high'] >= ob['OB_low'])
        reclaim = today['close'] > ob['OB_mid']
        if touch and reclaim:
            signal = {
                "date": today['date'],
                "type": "BULLISH_OB_BUY",
                "price": round(today['close'], 2),
                "OB_high": round(ob['OB_high'], 2),
                "OB_low": round(ob['OB_low'], 2),
                "OB_mid": round(ob['OB_mid'], 2),
            }

    # Bearish OB 信号
    if ob['OB_type'] == 'Bearish':
        touch = (today['high'] >= ob['OB_low']) and (today['low'] <= ob['OB_high'])
        reject = today['close'] < ob['OB_mid']
        if touch and reject:
            signal = {
                "date": today['date'],
                "type": "BEARISH_OB_SELL",
                "price": round(today['close'], 2),
                "OB_high": round(ob['OB_high'], 2),
                "OB_low": round(ob['OB_low'], 2),
                "OB_mid": round(ob['OB_mid'], 2),
            }

    return signal

# ============================================================
# 数据获取
# ============================================================
def fetch_data_with_timeout(symbol, start_date, end_date, adjust, timeout):
    def _fetch():
        if CONFIG["USE_LOCAL_MYSQL"]:
            try:
                return stock_zh_a_daily_mysql(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
            except NameError:
                pass
        return ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_fetch)
        done, not_done = wait([future], timeout=timeout)
        if future in done:
            return future.result()
        elif future in not_done:
            future.cancel(); raise ThreadingTimeoutError(f"请求超时 ({timeout}s)")


# ============================================================
# 单只股票策略
# ============================================================
def strategy_single_stock(code, start_date, end_date, df_spot):
    symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"
    try:
        df = fetch_data_with_timeout(symbol, start_date, end_date, CONFIG["ADJUST"], CONFIG["REQUEST_TIMEOUT"])

        # 实时数据追加 (如果开启且是交易日，df.iloc[-1] 是今天)
        if CONFIG["USE_REAL_TIME_DATA"]:
            df = append_today_realtime_snapshot(code, df, df_spot)

        # 确保数据足够：至少需要 OB+periods+1 根 K，且至少有 2 根 K 线用于信号判断
        if df is None or df.empty or len(df) < CONFIG["OB_STRATEGY"]["PERIODS"] + 2 or len(df) < 2:
            return None

        # 策略计算
        df = order_blocks(df,
                          periods=CONFIG["OB_STRATEGY"]["PERIODS"],
                          threshold=CONFIG["OB_STRATEGY"]["THRESHOLD"],
                          use_wicks=CONFIG["OB_STRATEGY"]["USE_WICKS"])

        # 信号检查 (调用修改后的 ob_signal_lagged 函数，返回前一天的信号)
        signal_data = ob_signal(df)

        if signal_data:
            # 信号结果处理：将字典展平成一行结果
            result = {"代码": code}
            result.update(signal_data)
            return result
        else:
            return None

    except Exception as e:
        # print(f"[错误] {code} 处理失败: {e}")
        return None


# ============================================================
# 并发扫描
# ============================================================
async def main_scanner_async(stock_codes, df_spot, start_date, end_date):
    results = []
    loop = asyncio.get_running_loop()

    # 查找股票名称 (为结果增加可读性)
    df_base = get_stock_list_manager()
    code_name_map = df_base.set_index('code')['name'].to_dict()

    def run_strategy_and_enhance(code, start, end, spot, c_map):
        # 内部调用 strategy_single_stock 仍然使用传入的 start/end date
        res = strategy_single_stock(code, start, end, spot)
        if res and isinstance(res, dict) and '代码' in res:
            res['名称'] = c_map.get(code, 'N/A')
        return res

    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:
        tasks = [
            # 传递 start_date 和 end_date 给线程池执行器
            loop.run_in_executor(pool, run_strategy_and_enhance, code, start_date, end_date, df_spot, code_name_map) for
            code in stock_codes]
        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")

        hit_count = 0
        for coro in pbar:
            res = await coro
            if res:
                results.append(res)
                hit_count += 1
                pbar.set_postfix({"命中": hit_count})
    return results

async def batch_scan_manager_async(target_codes, df_spot, start_date, end_date):
    all_results = []
    batch_size = CONFIG["BATCH_SIZE"]
    interval = CONFIG["BATCH_INTERVAL_SEC"]
    total_stocks = len(target_codes)
    num_batches = math.ceil(total_stocks / batch_size)
    print(f"\n[调度器] 总计 {total_stocks} 支股票，分 {num_batches} 批次 ({batch_size} 支/批)")
    for i in range(num_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_stocks)
        batch_codes = target_codes[start_index:end_index]
        print(f"\n--- 批次 {i + 1}/{num_batches} ---")
        # 传递 start_date 和 end_date
        batch_results = await main_scanner_async(batch_codes, df_spot, start_date, end_date)
        all_results.extend(batch_results)
        if i < num_batches - 1:
            print(f"休息 {interval} 秒...")
            time.sleep(interval)
    return all_results

# ============================================================
# 主入口
# ============================================================
# ============================================================
# 主入口
# ============================================================
def main():
    start_time = time.time()

    # 🚩🚩🚩 确定扫描日期范围 🚩🚩🚩
    df_spot = pd.DataFrame()  # 实时快照默认为空

    if CONFIG["TARGET_DATE"]:
        # 模式 1: 历史回测模式
        try:
            # 解析目标日期并将其设置为数据获取的结束日期
            end_date_obj = datetime.datetime.strptime(CONFIG["TARGET_DATE"], '%Y-%m-%d').date()
            print(f"[配置] 目标扫描日期设置为: {CONFIG['TARGET_DATE']}")
            # 在历史模式下，强制关闭实时数据，避免错误追加今日数据
            CONFIG["USE_REAL_TIME_DATA"] = False
        except ValueError:
            print(f"[错误] TARGET_DATE 格式错误: {CONFIG['TARGET_DATE']}，应为 YYYY-MM-DD。使用今日日期继续。")
            end_date_obj = datetime.datetime.now().date()
    else:
        # 模式 2: 最新日期模式
        end_date_obj = datetime.datetime.now().date()

    end_date = end_date_obj.strftime("%Y%m%d")
    # 往前推 DAYS+30 天作为起始日期，以保证数据足够长
    start_date = (end_date_obj - datetime.timedelta(days=CONFIG["DAYS"] + 30)).strftime("%Y%m%d")

    # 只有在最新日期模式且开启开关时才加载交易日历和实时数据
    if CONFIG["USE_REAL_TIME_DATA"]:
        load_trade_calendar()
        if is_trade_day(end_date_obj):
            df_spot = fetch_realtime_snapshot()
            if df_spot.empty:
                print("[警告] 无法获取实时行情快照，将只使用历史数据。")
        else:
            print("[配置] 非交易日，跳过实时快照")

    # 注意：LogRedirector 的路径需要确保存在
    log_folder = os.path.join(CONFIG['OUTPUT_FOLDER_BASE'], CONFIG['OUTPUT_LOG'])
    os.makedirs(log_folder, exist_ok=True)

    with LogRedirector(folder=log_folder) as log_redirector:

        # 2. 获取并过滤股票列表
        df_base = get_stock_list_manager()
        valid_codes = filter_stock_list(df_base)
        target_codes = valid_codes

        if CONFIG["MANUAL_STOCK_LIST"]:
            target_codes = [c for c in CONFIG["MANUAL_STOCK_LIST"] if c in valid_codes]
            print(f"[配置] 采用手动股票列表，共 {len(target_codes)} 支")
        elif CONFIG["SAMPLE_SIZE"] > 0 and len(valid_codes) > CONFIG["SAMPLE_SIZE"]:
            target_codes = random.sample(valid_codes, CONFIG["SAMPLE_SIZE"])
            print(f"[配置] 采用随机采样，共 {len(target_codes)} 支")

        # 3. 扫描
        if not target_codes:
            print("[终止] 无有效目标股票进行扫描。")
            return

        # 🚩🚩🚩 调用时传入 start_date 和 end_date 🚩🚩🚩
        if len(target_codes) <= CONFIG["BATCH_SIZE"]:
            final_data = asyncio.run(main_scanner_async(target_codes, df_spot, start_date, end_date))
        else:
            final_data = asyncio.run(batch_scan_manager_async(target_codes, df_spot, start_date, end_date))

        # 4. 结果处理
        if final_data:
            res_df = pd.DataFrame(final_data)
            # 过滤只保留 Bullish OB 买入信号
            res_df = res_df[res_df['type'] == 'BULLISH_OB_BUY']

            # 文件名和路径使用 TARGET_DATE 或 end_date_obj
            output_date_str = end_date_obj.strftime('%Y-%m-%d')
            folder_path = os.path.join(CONFIG["OUTPUT_FOLDER_BASE"], output_date_str)
            os.makedirs(folder_path, exist_ok=True)
            timestamp = datetime.datetime.now().strftime('%H%M%S')
            file_name = f"{CONFIG['OUTPUT_FILENAME_BASE']}_{timestamp}.csv"
            full_file_path = os.path.join(folder_path, file_name)

            # 确保列顺序
            if not res_df.empty:
                cols = ['代码', '名称', 'date', 'type', 'price', 'OB_high', 'OB_low', 'OB_mid']
                res_df = res_df.reindex(columns=cols, fill_value='-')

            res_df.to_csv(full_file_path, index=False, encoding=CONFIG["EXPORT_ENCODING"])

            print(f"\n✅ 扫描完成，耗时 {time.time() - start_time:.1f}s")
            print(f"📄 CSV 文件已保存: {full_file_path}")

            if not res_df.empty:
                print(f"--- 🎯 Bullish OB 买入信号 ({output_date_str}) ---")
                print(res_df.head(10).to_string(index=False))
                print(f"\n📢 信号确认日期 (date) 为 {output_date_str}。")
            else:
                print(f"\n[结果] {output_date_str} 没有命中 Bullish OB 买入信号。")
        else:
            print("\n[结果] 没有命中任何股票。")


if __name__ == "__main__":
    main()
