#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
MACD Histogram 普通背离 - Python 完整实现

信号定义:
    +1  底部背离（Bullish）
    -1  顶部背离（Bearish）
============================================================
"""

import pandas as pd
import numpy as np
from api.stock_query import stock_zh_a_daily_mysql


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# ============================================================
# 1. MACD Histogram
# ============================================================
def calc_macd(df, fast=12, slow=26, signal=9):

    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()

    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()

    hist = dif - dea

    df['macd_hist'] = hist
    return df


# ============================================================
# 2. 寻找局部高低点
# ============================================================
def find_pivots(series, window=5, mode='high'):

    pivots = []

    for i in range(window, len(series) - window):
        c = series.iloc[i]
        l = series.iloc[i - window:i]
        r = series.iloc[i + 1:i + 1 + window]

        if mode == 'high':
            if c > l.max() and c > r.max():
                pivots.append(i)
        else:
            if c < l.min() and c < r.min():
                pivots.append(i)

    return pivots


# ============================================================
# 3. 普通背离检测
# ============================================================
def detect_macd_divergence(df):
    """
    返回:
        macd_div
        +1 底背离
        -1 顶背离
    """

    df['macd_div'] = 0

    highs = find_pivots(df['close'], mode='high')
    lows = find_pivots(df['close'], mode='low')

    # ---------- 顶部背离 ----------
    for i in range(len(highs) - 1):
        i1, i2 = highs[i], highs[i + 1]

        p1, p2 = df['close'].iloc[[i1, i2]]
        m1, m2 = df['macd_hist'].iloc[[i1, i2]]

        if (
            p2 > p1 and        # 价格创新高
            m2 < m1 and        # 动能下降
            m1 > 0 and m2 > 0  # 0 轴上方
        ):
            df.loc[df.index[i2], 'macd_div'] = -1

    # ---------- 底部背离 ----------
    for i in range(len(lows) - 1):
        i1, i2 = lows[i], lows[i + 1]

        p1, p2 = df['close'].iloc[[i1, i2]]
        m1, m2 = df['macd_hist'].iloc[[i1, i2]]

        if (
            p2 < p1 and        # 价格创新低
            m2 > m1 and        # 动能回升
            m1 < 0 and m2 < 0  # 0 轴下方
        ):
            df.loc[df.index[i2], 'macd_div'] = 1

    return df


# ============================================================
# 4. 主程序
# ============================================================
def main():

    df = stock_zh_a_daily_mysql(
        symbol='sh601138',
        start_date='20240101',
        end_date='20251231',
        adjust='qfq'
    )

    if df is None or df.empty:
        print("数据为空")
        return

    df.columns = [c.lower() for c in df.columns]

    df = calc_macd(df)
    df = detect_macd_divergence(df)

    signal_df = df[df['macd_div'] != 0][
        ['date', 'close', 'macd_hist', 'macd_div']
    ].copy()

    signal_df['close'] = signal_df['close'].round(2)
    signal_df['macd_hist'] = signal_df['macd_hist'].round(4)

    signal_df['signal_text'] = signal_df['macd_div'].map({
        1: '🟢 底部背离',
       -1: '🔴 顶部背离'
    })

    print(signal_df)


if __name__ == "__main__":
    main()
