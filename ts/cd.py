#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# ================== CONFIG ==================
DB_URI = "mysql+pymysql://root:Elaiza112233@localhost:3306/stock"
TABLE_NAME = "a_stock_daily"
THREADS = 6
MIN_SCORE = 80  # 保持 70，但由于总分上限提升，筛选将更灵活
LOOKBACK_DAYS = 5  # 只输出最近 N 天信号
# ============================================


engine = create_engine(DB_URI)


# ================== INDICATORS ==================
def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def macd(close):
    dif = ema(close, 12) - ema(close, 26)
    dea = ema(dif, 9)
    return dif, dea


def wvf(df, pd=22):
    """
    WVF (Williams Value Fraction) - 衡量低点与近期收盘最高价的距离
    数值越低，代表越弱势/超卖
    """
    highest_close = df["close"].rolling(pd).max()
    return (highest_close - df["low"]) / highest_close * 100


# ================== SCORE (改进版) ==================
def score_bar(df: pd.DataFrame, i: int) -> int:
    score = 0

    # 确保有前一日数据可供计算
    if i < 1:
        return 0

    # 1. ---- RSI 超卖拐头 (30)
    # 核心信号：超卖区开始反转
    if df["rsi"].iloc[i - 1] < 30 and df["rsi"].iloc[i] > df["rsi"].iloc[i - 1]:
        score += 30

    # 2. ---- RSI 单纯超卖 (10)
    # 补充信号：处于超卖区，但尚未拐头
    elif df["rsi"].iloc[i] < 30:
        score += 10

    # 3. ---- 反向 WVF（极弱）(最高 25)
    # 确认超卖深度
    wvf_val = df["wvf"].iloc[i]
    if wvf_val < 15:
        score += 25
    elif wvf_val < 20:
        score += 15
    elif wvf_val < 25:
        score += 5

    # 4. ---- 结构止跌 (20)
    # 价格行为确认企稳
    if df["low"].iloc[i] >= df["low"].iloc[i - 1]:
        score += 20

    # 5. ---- EMA 趋势确认 (10)
    # 改进：放宽条件，只要收盘价站上短期均线，即视为短期多头趋势
    if df["close"].iloc[i] > df["ema20"].iloc[i]:
        score += 10

    # 6. ---- MACD 水下金叉 (10)
    # 趋势指标确认反转酝酿
    if (
            df["dif"].iloc[i - 1] < df["dea"].iloc[i - 1]
            and df["dif"].iloc[i] > df["dea"].iloc[i]
            and df["dif"].iloc[i] < 0  # 必须在零轴以下
    ):
        score += 10

    # 7. ---- 放量确认 (5)
    # 资金关注度
    if df["volume"].iloc[i] > df["volume"].rolling(10).mean().iloc[i]:
        score += 5

    return score


# ================== STOCK SCANNING ==================
def scan_one_stock(code: str) -> list:
    """
    扫描单只股票，计算指标并评分
    """
    sql = text(f"""
        SELECT date, open, high, low, close, volume
        FROM {TABLE_NAME}
        WHERE code=:code AND adjust='qfq'
        ORDER BY date
    """)

    # 使用 with engine.connect() 确保连接管理，但 read_sql 内部已处理
    df = pd.read_sql(sql, engine, params={"code": code})

    # 需要至少 60 根 K 线计算指标
    if len(df) < 60:
        return []

    # 计算指标
    df["ema20"] = ema(df["close"], 20)
    df["rsi"] = rsi(df["close"])
    df["dif"], df["dea"] = macd(df["close"])
    df["wvf"] = wvf(df)

    # 计算 10 日均量，用于放量确认
    df["vol_mean10"] = df["volume"].rolling(10).mean()

    signals = []

    # 从第 60 根 K 线开始计算评分
    for i in range(60, len(df)):
        s = score_bar(df, i)
        if s >= MIN_SCORE:
            signals.append({
                "code": code,
                "date": df["date"].iloc[i],
                "close": df["close"].iloc[i],
                "score": s
            })

    return signals


def scan_all():
    """
    多线程扫描整个市场
    """
    print("📥 Loading all stock codes...")
    codes = pd.read_sql(
        f"SELECT DISTINCT code FROM {TABLE_NAME}",
        engine
    )["code"].tolist()

    print(f"🚀 Starting multi-threaded scan for {len(codes)} stocks...")

    results = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(scan_one_stock, code) for code in codes]

        for future in tqdm(
                futures,
                total=len(codes),
                desc="📈 Market Scanning Progress",
                ncols=100
        ):
            results.extend(future.result())

    df = pd.DataFrame(results)

    if df.empty:
        print("\n⚠️ 扫描完成，未发现符合高分条件的信号。")
        return df

    df["date"] = pd.to_datetime(df["date"])

    # 筛选 LOOKBACK_DAYS 内的信号
    cutoff = df["date"].max() - pd.Timedelta(days=LOOKBACK_DAYS)

    print("\n✅ 信号筛选完成。")
    return df[df["date"] >= cutoff].sort_values(
        ["date", "score"],
        ascending=[False, False]
    )

def backtest_signals(
    df_signals: pd.DataFrame,
    hold_days: int = 10,
    win_threshold: float = 0.05
):
    """
    回测：信号日买入，未来 hold_days 内表现
    """
    results = []

    for _, row in tqdm(df_signals.iterrows(), total=len(df_signals), desc="📊 Backtesting"):
        code = row["code"]
        signal_date = row["date"]
        entry_price = row["close"]
        score = row["score"]

        sql = text(f"""
            SELECT date, high, low
            FROM {TABLE_NAME}
            WHERE code=:code AND adjust='qfq' AND date > :signal_date
            ORDER BY date
            LIMIT :limit
        """)

        future = pd.read_sql(
            sql,
            engine,
            params={
                "code": code,
                "signal_date": signal_date,
                "limit": hold_days
            }
        )

        if future.empty:
            continue

        max_high = future["high"].max()
        min_low = future["low"].min()

        max_return = (max_high / entry_price - 1) * 100
        max_drawdown = (min_low / entry_price - 1) * 100

        win = max_return >= win_threshold * 100

        results.append({
            "code": code,
            "date": signal_date,
            "score": score,
            "max_return_pct": max_return,
            "max_drawdown_pct": max_drawdown,
            "win": win
        })

    return pd.DataFrame(results)


# ================== MAIN EXECUTION ==================
if __name__ == "__main__":
    df_signal = scan_all()

    if df_signal.empty:
        exit()

    print("\n🧪 开始回测 (未来 10 天，目标 5%)...\n")

    df_bt = backtest_signals(
        df_signal,
        hold_days=10,
        win_threshold=0.05
    )

    if df_bt.empty:
        print("⚠️ 无回测结果")
        exit()

    print("\n✅ 回测统计结果")
    print("-" * 40)
    print(f"信号次数     : {len(df_bt)}")
    print(f"胜率         : {df_bt['win'].mean() * 100:.2f}%")
    print(f"平均最大涨幅 : {df_bt['max_return_pct'].mean():.2f}%")
    print(f"平均最大回撤 : {df_bt['max_drawdown_pct'].mean():.2f}%")

    print("\n📌 高分信号样本：")
    print(df_bt.sort_values("score", ascending=False).head(10).to_string(index=False))
