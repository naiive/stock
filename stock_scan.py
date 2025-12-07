#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
A 股突破扫描系统（Pivot + SQZMOM + MA200）
作者：yinwang（由 ChatGPT 辅助生成）
创建时间：2025-12-06
版本：v1.1

【功能简介】
- 扫描随机一批 A 股股票（可配置 sample）
- 获取过去 N 天行情数据（不需要填写日期）
- 非实时数据
- 自动计算：
    1. MA200 趋势判断
    2. LazyBear SQZMOM（完整复刻 TradingView）
    3. 前方 pivot 阻力位（15/15 可配置）
    4. 上破阻力位 + 今日上涨
    5. Squeeze→Release 条件的第一根（sqz_id==1）
- 最终输出信号：
    ✓ 买入
    ✓ 观察
- 自动导出 CSV（每日新建文件夹）

【信号过滤】
导出结果已自动排除 信号 = "无" 的所有个股。

【运行方法】
1. 安装依赖：
    pip install pandas numpy akshare tqdm

2. 运行脚本：
    python3 stock_scan.py

3. 查看输出：
    -> Scan_Results/YYYY-MM-DD/Pivot_Breakout_Stocks_xxxxxx.csv

【适用人群】
需要本地离线批量扫描 A 股突破信号的量化用户。

============================================================
"""
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
from stock_query import stock_zh_a_daily_mysql

# ============================================================
# 模块 1：配置 (Configuration)
# ============================================================
CONFIG = {
    # --- 时间范围 ---
    "DAYS": 365,  # 扫描回溯天数 (用于计算 MA200)

    # --- 过滤条件 ---
    "EXCLUDE_GEM": True,  # 排除创业板（300）
    "EXCLUDE_KCB": True,  # 排除科创板（688）
    "EXCLUDE_BJ": True,   # 排除北交所（8、4、92）
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
    "MAX_WORKERS": 20,
    "REQUEST_TIMEOUT": 15,  # 🆕 **关键：akshare 单次请求整体超时保护（秒）**

    # --- 🆕 手动输入 ---
    # 示例: ["600519", "000001", "300751"]。如果非空，则跳过全量扫描。
    "MANUAL_STOCK_LIST": [
                            # "000807",
                            # "000708",
                            # "002830",
                            # "301517",
                            # "000408",
                            # "600879",
                            # "600595",
                            # "601168",
                            # "002595",
                            # "301028",
                            # "002429"
                        ]
}

# ============================================================
# 工具：重试装饰器
# ============================================================
def retry(max_retries=3, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries - 1:
                        raise
                    time.sleep(delay)

        return wrapper

    return decorator


# ============================================================
# 模块 2：获取/缓存 全市场股票列表（优先 stock_zh_a_spot）
# ============================================================
@retry(max_retries=2, delay=1)
def fetch_stock_list_safe():
    """获取全市场股票列表，采用降级策略以提高稳定性。"""
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
        return df[["code", "name"]]
    except Exception as e:
        raise Exception(f"所有股票列表接口均不可用: {e}")


def get_stock_list_manager():
    """缓存管理器：优先读取本地缓存，过期或不存在则联网更新。"""
    cache_file = CONFIG["CACHE_FILE"]
    today_str = datetime.now().strftime("%Y-%m-%d")

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
    df["code"] = df["code"].astype(str)
    mask = pd.Series(False, index=df.index)
    if CONFIG["EXCLUDE_GEM"]:
        mask |= df["code"].str.startswith("300")
    if CONFIG["EXCLUDE_KCB"]:
        mask |= df["code"].str.startswith("688", "689")
    if CONFIG["EXCLUDE_BJ"]:
        mask |= df["code"].str.startswith(("8", "4", "92"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns:
        mask |= df["name"].str.contains("ST|退", na=False)
    return df[~mask]["code"].tolist()


# ============================================================
# 模块 3：技术指标（SQZMOM / linreg / true_range / color / sqz_id）
# ============================================================
def tv_linreg(y, length):
    if pd.isna(y).any():
        return np.nan
    x = np.arange(length)
    y = y.values
    # 避免 numpy.linalg.LinAlgError: Singular matrix in least squares
    if len(y) < 2:
        return np.nan

    A = np.vstack([x, np.ones(length)]).T
    try:
        m, b = np.linalg.lstsq(A, y, rcond=None)[0]
    except np.linalg.LinAlgError:
        return np.nan  # 极少数情况出现奇异矩阵

    return m * (length - 1) + b


def true_range(df):
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def get_color_cn(v, v_prev):
    if pd.isna(v) or pd.isna(v_prev):
        return None
    if v == 0:
        return "中性"
    if v > 0:
        return "强多" if v > v_prev else "弱多"
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
    # 允许通过参数覆盖 CONFIG
    if length is None:
        length = CONFIG["SQZ"]["length"]
    if mult is None:
        mult = CONFIG["SQZ"]["mult"]
    if lengthKC is None:
        lengthKC = CONFIG["SQZ"]["lengthKC"]
    if multKC is None:
        multKC = CONFIG["SQZ"]["multKC"]

    close = df['close']
    high = df['high']
    low = df['low']

    # Bollinger Bands (注意 LazyBear 使用 multKC)
    basis = close.rolling(length).mean()
    dev = multKC * close.rolling(length).std(ddof=0)
    upperBB = basis + dev
    lowerBB = basis - dev
    # 为输出 BB 值 (用 % 距离 basis)
    bb_width = (upperBB - lowerBB) / basis.replace(0, np.nan)

    # Keltner Channel
    ma = close.rolling(lengthKC).mean()
    r = true_range(df) if useTrueRange else (high - low)
    rangema = r.rolling(lengthKC).mean()
    upperKC = ma + rangema * multKC
    lowerKC = ma - rangema * multKC

    sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)

    df["sqz_status"] = np.select([sqzOn, sqzOff], ["挤压", "释放"], default="无")

    highest_h = high.rolling(lengthKC).max()
    lowest_l = low.rolling(lengthKC).min()
    avg_hl = (highest_h + lowest_l) / 2
    sma_close = close.rolling(lengthKC).mean()
    mid = (avg_hl + sma_close) / 2
    source_mid = close - mid

    # 使用 apply 和 tv_linreg 来计算 momentum
    val = source_mid.rolling(lengthKC).apply(lambda x: tv_linreg(pd.Series(x), lengthKC), raw=False)
    df["val"] = val
    df["val_prev"] = val.shift(1)
    df["val_color"] = df.apply(lambda r: get_color_cn(r["val"], r["val_prev"]), axis=1)

    df["BB_pct"] = bb_width  # 用于输出 BB 值比例（可按需调整）
    df = add_squeeze_counter(df)
    return df


# ============================================================
# 模块 4：Pivot 高点（前阻力位）
# ============================================================
def calculate_pivot_high_vectorized(df, left=None, right=None):
    if left is None:
        left = CONFIG["PIVOT_LEFT"]
    if right is None:
        right = CONFIG["PIVOT_RIGHT"]

    highs = df['high'].values
    n = len(highs)
    pivots = np.full(n, np.nan)

    # 简单明了的遍历（相对安全）
    for i in range(left, n - right):
        left_max = np.max(highs[i - left:i])
        right_max = np.max(highs[i + 1:i + 1 + right])
        # 严格高点：左侧和右侧的最高价都低于当前高点
        if highs[i] > left_max and highs[i] > right_max:
            pivots[i] = highs[i]

    return pd.Series(pivots, index=df.index).ffill()


# ============================================================
# 模块 5：今日实时K补充 + 单股策略，
# 交易日期：历史+实时，非交易日期就是：历史
# ============================================================
def append_today_realtime(symbol: str, df_daily: pd.DataFrame, period: str = "1" ):
    """
    day  open  high   low  close volume
    2025-12-05 15:00:00  7.43  7.43  7.43   7.43  22200

    date  open  high   low  close     volume      amount  outstanding_share  turnover
    2025-12-03  8.23  8.23  8.23   8.23  3681100.0  30295453.0        699623237.0  0.005262
    2025-12-04  7.82  7.82  7.82   7.82   446900.0   3494758.0        699623237.0  0.000639

    :param symbol: 股票代码
    :param df_daily: 历史数据df
    :param period: 实时接口周期 默认 1 分钟
    :return:
    """

    df_min = ak.stock_zh_a_minute(
        symbol=symbol,
        period=period,
        adjust=CONFIG["ADJUST"]
    )
    # 只有实时数据最后一条最新的
    df_min.tail(1)

    df_min['date'] = pd.to_datetime(df_min['day']).dt.date

    for _, row in df_min.iterrows():
        new_date = row['date']
        if new_date not in pd.to_datetime(df_daily['date']).dt.date.values:
            new_row = {
                'date': new_date,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'amount': None,  # 如果没有数据的话，可以设置为 None 或其他缺省值
                'outstanding_share': None,
                'turnover': None
            }

            df_daily = pd.concat([df_daily, pd.DataFrame([new_row])], ignore_index=True)

    return df_daily


def fetch_data_with_timeout(symbol, start_date, end_date, adjust, timeout):
    """
    一个辅助函数，在独立的线程中执行 akshare 请求，并使用 Future/wait 实施超时。
    """

    def _fetch():
        # ak包接口
        # return ak.stock_zh_a_daily(
        #     symbol=symbol,
        #     start_date=start_date,
        #     end_date=end_date,
        #     adjust=adjust
        # )

        # 本地mysql接口
        return  stock_zh_a_daily_mysql(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )

    # 使用一个临时的线程池来管理超时任务
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_fetch)
        try:
            # 等待 Future 完成，设置超时时间
            done, not_done = wait([future], timeout=timeout)
            if future in done:
                return future.result()
            elif future in not_done:
                # 超时，取消任务并抛出异常
                future.cancel()
                raise ThreadingTimeoutError(f"请求超时 ({timeout}s)")
        except Exception as e:
            # 捕获其他可能的异常，如 akshare 内部错误
            raise e


# ============================================================
# 模块 6：单只股票策略（整合 SQZMOM + MA200 + Pivot + 信号）
# 关键：akshare 请求增加超时保护
# ============================================================
def strategy_single_stock(code, start_date, end_date):
    """
    输入 code: '600519' / '002596' 等六位字符串（不带 sh/sz）
    返回 dict 或 None
    """
    symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"

    try:
        # 🆕 使用带超时保护的函数获取数据
        df = fetch_data_with_timeout(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=CONFIG["ADJUST"],
            timeout=CONFIG["REQUEST_TIMEOUT"]
        )

        # 需要至少 200+ 天的历史数据才能正确计算指标
        if df is None or df.empty or len(df) < 220:
            return None

        # 添加实时数据【看是否需要】
        df = append_today_realtime(symbol, df)

        # --- 核心优化：先计算 MA200、前阻力位和涨幅，只要有一个不满足就直接排除 ---
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        # MA200
        ma200_series = df['close'].rolling(200).mean()
        if ma200_series.empty or pd.isna(ma200_series.iloc[-1]):
            return None
        ma200 = ma200_series.iloc[-1]

        # Pivot (前阻力位)
        pivot_series = calculate_pivot_high_vectorized(df)
        if pivot_series.empty or pd.isna(pivot_series.iloc[-1]):
            return None
        last_pivot = pivot_series.iloc[-1]

        # 三个策略条件
        condition_trend = current_close > ma200
        condition_break = current_close > last_pivot
        condition_up = pct_chg > 0

        # 🟢 短路优化：只要有一个条件不满足，直接返回 None
        if not (condition_trend and condition_break and condition_up):
            return None

        # --- 下面开始计算 SQZMOM ---
        df = squeeze_momentum(df, useTrueRange=CONFIG["SQZ"]["useTrueRange"])
        last = df.iloc[-1]
        prev = df.iloc[-2]

        break_strength = (current_close - last_pivot) / last_pivot * 100

        # 信号判定
        signal = "无"

        # --- 买入信号 ---
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

        # 修复：确保 val 可以转换为 float
        last_val = last.get("val")

        return {
            "代码": code,
            "信号": signal,
            "当前价": round(current_close, 2),
            "涨幅%": round(pct_chg, 2),
            "MA200": round(ma200, 2),
            "前阻力位": round(float(last_pivot), 2),
            "突破力度%": round(break_strength, 2),
            "BB值": None if pd.isna(last_val) else round(float(last_val), 2),
            "BB中文": last.get("val_color")
        }

    except ThreadingTimeoutError:
        # 捕获请求超时异常
        # print(f"[超时] {code} 请求超时，跳过。")
        return None

    except Exception as e:
        # 捕获 akshare 返回空数据或其他处理异常
        # print(f"[错误] {code} 处理失败: {e} - 类型: {type(e).__name__}")
        return None

# ============================================================
# 模块 7：并发扫描 (Async Scheduler)
# ============================================================
async def main_scanner_async(stock_codes):
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")

    print(f"\n[任务启动] 扫描范围: {start_date} ~ {end_date}")
    print(f"[配置] 目标: {len(stock_codes)} 支 | 线程: {CONFIG['MAX_WORKERS']} | 超时: {CONFIG['REQUEST_TIMEOUT']}s")

    results = []
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:

        # 任务列表保持不变
        tasks = [
            loop.run_in_executor(pool, strategy_single_stock, code, start_date, end_date)
            for code in stock_codes
        ]

        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")
        for coro in pbar:
            res = await coro
            if res:
                results.append(res)
                pbar.set_postfix({"命中": len(results)})

    return results


# ============================================================
# 模块 8：主入口：整合流程（缓存、过滤、抽样、扫描、导出）
# ============================================================
def main():
    start_time = time.time()

    # 1. 手动模式优先
    manual_list = CONFIG["MANUAL_STOCK_LIST"]
    df_base = pd.DataFrame()

    if manual_list and isinstance(manual_list, (list, tuple)) and len(manual_list) > 0:
        target_codes = [str(c).zfill(6) for c in manual_list]
        print(f"[手动模式] 使用手动输入列表，共 {len(target_codes)} 支股票。")
        try:
            df_base = get_stock_list_manager()
        except Exception:
            # 如果获取不到全量列表，手动模式下也给它们一个默认名
            df_base = pd.DataFrame({"code": target_codes, "name": ["未知"] * len(target_codes)})

    else:
        # 全市场获取并过滤
        try:
            df_base = get_stock_list_manager()
        except Exception as e:
            print(f"[终止] 无法获取股票列表: {e}")
            return

        valid_codes = filter_stock_list(df_base)
        if not valid_codes:
            print("[终止] 股票列表为空，请检查缓存或过滤条件。")
            return

        sample_size = CONFIG["SAMPLE_SIZE"]
        if isinstance(sample_size, int) and sample_size > 0 and len(valid_codes) > sample_size:
            print(f"[抽样模式] 随机抽取 {sample_size} 支股票进行测试...")
            target_codes = random.sample(valid_codes, sample_size)
        else:
            print(f"[全量模式] 扫描所有 {len(valid_codes)} 支有效股票...")
            target_codes = valid_codes

    # 2. 并发扫描
    final_data = asyncio.run(main_scanner_async(target_codes))

    # 3. 结果整理与导出
    if final_data:
        res_df = pd.DataFrame(final_data)

        # 🆕 关键修改：只要信号为 "买入" 的数据
        res_df = res_df[res_df["信号"] == "买入"].copy()

        if res_df.empty:
            print("\n[结果] 过滤后没有发现符合策略的股票。")
            return pd.DataFrame()

        # 补全股票名称（使用 df_base）
        if not df_base.empty:
            name_map = dict(zip(df_base["code"].astype(str), df_base["name"]))
            res_df.insert(1, "名称", res_df["代码"].map(name_map).fillna("未知"))
        else:
            res_df.insert(1, "名称", "未知")

        # 排序：先按信号、再按突破力度
        signal_order = {"买入": 0, "观察": 1, "无": 2}
        res_df["信号排序"] = res_df["信号"].map(signal_order).fillna(3)
        res_df = res_df.sort_values(["信号排序", "突破力度%"], ascending=[True, False]).drop(columns=["信号排序"])

        # 导出 CSV
        today_date_str = datetime.now().strftime('%Y-%m-%d')
        folder_path = os.path.join(CONFIG["OUTPUT_FOLDER_BASE"], today_date_str)
        os.makedirs(folder_path, exist_ok=True)
        timestamp = datetime.now().strftime('%H%M%S')
        file_name = f"{CONFIG['OUTPUT_FILENAME_BASE']}_{timestamp}.csv"
        full_file_path = os.path.join(folder_path, file_name)
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


# ============================================================
# 入口
# ============================================================
if __name__ == "__main__":
    main()