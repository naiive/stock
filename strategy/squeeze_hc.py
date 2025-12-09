#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
SQZ + MA200 + Pivot 突破策略（MySQL 本地回测版）
============================================================
"""

import datetime
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
from tqdm import tqdm

# ✅ 使用你自己的 MySQL 接口
from api.stock_query import stock_zh_a_daily_mysql


# =========================
# 回测参数
# =========================
DAYS_BACK = 900
FUTURE_DAYS = 10
TARGET_PCT = 0.05
MAX_WORKERS = 10
ADJUST = "qfq"


# =========================
# 技术指标
# =========================
def tv_linreg(y, length):
    x = np.arange(length)
    y = y.values
    A = np.vstack([x, np.ones(length)]).T
    m, b = np.linalg.lstsq(A, y, rcond=None)[0]
    return m * (length - 1) + b


def true_range(df):
    prev = df['close'].shift(1)
    return pd.concat([
        df['high'] - df['low'],
        (df['high'] - prev).abs(),
        (df['low'] - prev).abs()
    ], axis=1).max(axis=1)


def squeeze_momentum(df, length=20, lengthKC=20):
    close = df['close']
    high = df['high']
    low = df['low']

    ma = close.rolling(lengthKC).mean()
    tr = true_range(df)
    range_ma = tr.rolling(lengthKC).mean()

    upperKC = ma + range_ma * 1.5
    lowerKC = ma - range_ma * 1.5

    basis = close.rolling(length).mean()
    dev = close.rolling(length).std(ddof=0) * 2
    upperBB = basis + dev
    lowerBB = basis - dev

    sqz_on = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqz_off = (lowerBB < lowerKC) & (upperBB > upperKC)

    df['sqz'] = np.select([sqz_on, sqz_off], ['on', 'off'], default='none')

    highest = high.rolling(lengthKC).max()
    lowest = low.rolling(lengthKC).min()
    mid = (highest + lowest) / 2
    src = close - mid

    df['val'] = src.rolling(lengthKC).apply(
        lambda x: tv_linreg(pd.Series(x), lengthKC), raw=False
    )
    return df


def pivot_high(df, left=15, right=15):
    piv = [np.nan] * len(df)
    h = df['high'].values

    for i in range(left, len(h) - right):
        if h[i] > max(h[i-left:i]) and h[i] > max(h[i+1:i+1+right]):
            piv[i] = h[i]

    return pd.Series(piv).ffill()


# =========================
# 单股票回测
# =========================
def backtest_single(code):
    symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"

    try:
        df = stock_zh_a_daily_mysql(
            symbol=symbol,
            start_date=(datetime.datetime.now() - datetime.timedelta(days=DAYS_BACK)).strftime("%Y%m%d"),
            end_date=datetime.datetime.now().strftime("%Y%m%d"),
            adjust=ADJUST
        )

        if df is None or df.empty or len(df) < 260:
            return []

        df = df.sort_values("date").reset_index(drop=True)

        df['ma200'] = df['close'].rolling(200).mean()
        df['pivot'] = pivot_high(df)
        df = squeeze_momentum(df)

        results = []

        for i in range(200, len(df) - FUTURE_DAYS):
            row = df.iloc[i]

            # === 信号逻辑（与你扫描版一致）===
            if row['close'] <= row['ma200']:
                continue
            if row['close'] <= row['pivot']:
                continue
            if row['sqz'] != 'off':
                continue
            if df.iloc[i-1]['sqz'] != 'on':
                continue
            if row['val'] <= 0:
                continue

            entry_price = row['close']
            future = df.iloc[i+1:i+1+FUTURE_DAYS]

            max_ret = (future['high'].max() - entry_price) / entry_price
            max_dd = (future['low'].min() - entry_price) / entry_price

            results.append({
                "code": code,
                "date": row['date'],
                "max_return": round(max_ret * 100, 2),
                "max_drawdown": round(max_dd * 100, 2),
                "win": max_ret >= TARGET_PCT
            })

        return results

    except Exception as e:
        print(f"[Error] {code}: {e}")
        return []


# =========================
# 主入口
# =========================
def main():
    # ✅ 股票列表直接来自你数据库已有范围即可
    # 如果你有 stock_list 表，这里可以直接 SELECT
    # 这里假设你仍然用 AkShare 拿代码列表（只拿列表，不拿行情）
    import akshare as ak
    codes = ak.stock_info_a_code_name()['code'].tolist()

    all_trades = []

    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        for res in tqdm(pool.map(backtest_single, codes), total=len(codes)):
            all_trades.extend(res)

    df = pd.DataFrame(all_trades)

    if df.empty:
        print("❌ 没有任何交易信号")
        return

    print("\n✅ 回测完成")
    print("-" * 50)
    print(f"交易次数: {len(df)}")
    print(f"胜率: {df['win'].mean():.2%}")
    print(f"平均最大涨幅: {df['max_return'].mean():.2f}%")
    print(f"平均最大回撤: {df['max_drawdown'].mean():.2f}%")

    print("\n📌 高收益样本：")
    print(df.sort_values("max_return", ascending=False).head(10))


if __name__ == "__main__":
    main()
