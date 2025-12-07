import pandas as pd
import numpy as np
import asyncio
import akshare as ak
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json
import os
import time
import random

# ============================================================
# 模块 1：配置 (Configuration)
# ============================================================
CONFIG = {
    "DAYS": 365,  # 扫描回溯天数 (用于计算 MA200)
    "SAMPLE_SIZE": 30,  # 随机抽样数量 (None 或 0 表示扫描全量)
    "MAX_WORKERS": 32,  # 并发线程数 (针对 akshare 接口建议 16-64)
    "TIMEOUT": 15,  # 单次请求超时时间(秒)

    # --- 过滤条件 ---
    "EXCLUDE_GEM": True,  # 排除创业板 (300)
    "EXCLUDE_KCB": True,  # 排除科创板 (688)
    "EXCLUDE_BJ": True,  # 排除北交所 (8, 4)
    "EXCLUDE_ST": True,  # 排除 ST/退市股

    # --- 策略参数 ---
    "PIVOT_LEFT": 15,  # 左侧 K 线数量 (确认高点所需的左侧天数)
    "PIVOT_RIGHT": 15,  # 右侧 K 线数量 (确认高点所需的右侧天数)

    # --- 文件路径/名称 ---
    "CACHE_FILE": "stock_list_cache.json",
    "EXPORT_ENCODING": "utf-8-sig",  # CSV文件导出编码
    "OUTPUT_FILENAME_BASE": "Pivot_Breakout_Stocks",  # 输出文件基础名称
    "OUTPUT_FOLDER_BASE": "Scan_Results",  # 结果文件存放的根文件夹

    # --- 🆕 手动输入 ---
    # 示例: ["600519", "000001", "300751"]。如果非空，则跳过全量扫描。
    "MANUAL_STOCK_LIST": [],
}


# ============================================================
# 模块 2：工具与数据源 (Utils & Data Source)
# ============================================================
def retry(max_retries=3, delay=1):
    """[装饰器] 网络请求自动重试机制。"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries - 1:
                        raise e
                    time.sleep(delay)

        return wrapper

    return decorator


@retry(max_retries=3, delay=2)
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
    """根据配置清洗股票列表。"""
    if df.empty: return []

    df["code"] = df["code"].astype(str)
    mask = pd.Series(False, index=df.index)

    if CONFIG["EXCLUDE_GEM"]: mask |= df["code"].str.startswith("300")
    if CONFIG["EXCLUDE_KCB"]: mask |= df["code"].str.startswith("688")
    if CONFIG["EXCLUDE_BJ"]:  mask |= df["code"].str.startswith(("8", "4"))
    if CONFIG["EXCLUDE_ST"] and "name" in df.columns:
        mask |= df["name"].str.contains("ST|退")

    return df[~mask]["code"].tolist()


# ============================================================
# 模块 3：量化计算核心 (Quant Engine)
# ============================================================
def calculate_pivot_high_vectorized(df, left, right):
    """向量化计算 Pivot High，使用 Pandas Rolling Window 实现高性能。"""
    window = left + right + 1
    df['local_max'] = df['high'].rolling(window=window, center=True).max()
    df['is_pivot'] = (df['high'] == df['local_max'])

    if right > 0:
        df.iloc[-right:, df.columns.get_loc('is_pivot')] = False

    pivot_prices = np.where(df['is_pivot'], df['high'], np.nan)
    pivot_series = pd.Series(pivot_prices, index=df.index).ffill().shift(1)

    return pivot_series


@retry(max_retries=2, delay=1)
def strategy_single_stock(code, start_date, end_date):
    """单只股票数据获取和策略计算。"""
    try:
        symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"

        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )

        if df is None or df.empty or len(df) < 200:
            return None

        ma200 = df['close'].rolling(200).mean().iloc[-1]
        pivot_series = calculate_pivot_high_vectorized(df, CONFIG["PIVOT_LEFT"], CONFIG["PIVOT_RIGHT"])
        last_pivot = pivot_series.iloc[-1]
        current_close = df['close'].iloc[-1]
        prev_close = df['close'].iloc[-2]
        pct_chg = (current_close - prev_close) / prev_close * 100

        if pd.isna(last_pivot) or pd.isna(ma200):
            return None

        condition_trend = current_close > ma200
        condition_break = current_close > last_pivot
        condition_up = pct_chg > 0

        if condition_trend and condition_break and condition_up:
            break_strength = (current_close - last_pivot) / last_pivot * 100

            return {
                "代码": code,
                "当前价": round(current_close, 2),
                "涨幅%": round(pct_chg, 2),
                "MA200": round(ma200, 2),
                "前阻力位": round(last_pivot, 2),
                "突破力度%": round(break_strength, 2)
            }

    except Exception:
        return None

    return None


# ============================================================
# 模块 4：异步并发调度 (Async Scheduler)
# ============================================================
async def main_scanner_async(stock_codes):
    """异步主程序：负责调度线程池，并行执行单股票扫描任务。"""
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")

    print(f"\n[任务启动] 扫描范围: {start_date} ~ {end_date}")
    print(f"[配置] 目标: {len(stock_codes)} 支 | 线程: {CONFIG['MAX_WORKERS']}")

    results = []
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:

        tasks = [
            loop.run_in_executor(pool, strategy_single_stock, code, start_date, end_date)
            for code in stock_codes
        ]

        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="stock")
        for f in pbar:
            res = await f
            if res:
                results.append(res)
                pbar.set_postfix({"命中": len(results)})

    return results


# ============================================================
# 模块 5：主入口 (Entry Point)
# ============================================================
if __name__ == "__main__":
    start_time = time.time()

    # 1. 优先检查手动列表
    manual_list = CONFIG["MANUAL_STOCK_LIST"]
    df_base = pd.DataFrame()  # 预设 df_base 为空

    if manual_list and isinstance(manual_list, list) and len(manual_list) > 0:

        target_codes = [str(c).zfill(6) for c in manual_list]  # 确保代码是6位字符串
        print(f"[手动模式] 使用手动输入列表，共 {len(target_codes)} 支股票。")

        # 尝试获取全量基础数据，用于后续补全名称 (非必须，但提高用户体验)
        try:
            df_base = get_stock_list_manager()
        except Exception:
            print("[警告] 无法获取全量股票列表缓存，结果中将缺少股票名称。")
            df_base = pd.DataFrame({"code": target_codes, "name": ["未知"] * len(target_codes)})


    else:
        # 2. 走全量/抽样逻辑

        # 获取并过滤股票列表
        try:
            df_base = get_stock_list_manager()
        except Exception as e:
            print(f"[终止] 无法获取股票列表: {e}")
            exit()

        valid_codes = filter_stock_list(df_base)

        if not valid_codes:
            print("[终止] 股票列表为空，请检查缓存或过滤条件。")
            exit()

        # 抽样逻辑 (根据 SAMPLE_SIZE 配置)
        sample_size = CONFIG["SAMPLE_SIZE"]
        if isinstance(sample_size, int) and sample_size > 0 and len(valid_codes) > sample_size:
            print(f"[抽样模式] 随机抽取 {sample_size} 支股票进行测试...")
            target_codes = random.sample(valid_codes, sample_size)
        else:
            print(f"[全量模式] 扫描所有 {len(valid_codes)} 支有效股票...")
            target_codes = valid_codes

    # 3. 启动异步扫描
    final_data = asyncio.run(main_scanner_async(target_codes))

    # 4. 结果导出 (CSV 文件写入到日期文件夹)
    if final_data:
        res_df = pd.DataFrame(final_data)

        # 补全股票名称
        name_map = dict(zip(df_base["code"], df_base["name"]))
        res_df.insert(1, "名称", res_df["代码"].map(name_map).fillna("未知"))

        # 排序
        res_df = res_df.sort_values("突破力度%", ascending=False)

        # 文件路径处理
        today_date_str = datetime.now().strftime('%Y-%m-%d')

        # 文件夹路径：根目录/日期
        folder_path = os.path.join(CONFIG["OUTPUT_FOLDER_BASE"], today_date_str)

        # 确保文件夹存在
        os.makedirs(folder_path, exist_ok=True)

        # 完整文件名：路径/基础名称_时间戳.csv
        base_name = CONFIG["OUTPUT_FILENAME_BASE"]
        timestamp = datetime.now().strftime('%H%M%S')
        file_name = f"{base_name}_{timestamp}.csv"

        full_file_path = os.path.join(folder_path, file_name)

        # 写入 CSV 文件
        # res_df.to_csv(full_file_path, index=False, encoding=CONFIG["EXPORT_ENCODING"])

        print("\n" + "=" * 60)
        print(f"✅ 扫描完成 | 耗时: {time.time() - start_time:.1f}s")
        print(f"📄 结果文件已保存至: {full_file_path}")
        print(f"📈 命中数量: {len(res_df)}")
        print("=" * 60)
        print("--- 命中股票 Top 10 ---")
        print(res_df.head(10).to_string(index=False))
    else:
        print("\n[结果] 没有发现符合策略的股票。")