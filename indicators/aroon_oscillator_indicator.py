#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

"""
# ============================================================
# 指标 Aroon Oscillator 【TradingView 等价】
# ============================================================
"""

def aroon_oscillator_indicator(
        df: pd.DataFrame,
        length: int = 14
):
    """
      返回字段：
      - aroon_up
      - aroon_down
      - aroon_osc

      数值范围：
      aroon_up/down : [0, 100]
      aroon_osc     : [-100, 100]

        code        date   open   high    low  close       volume        amount    aroon_up  aroon_down  aroon_osc
        000592  2025-01-22   2.90   2.95   2.86   2.90   91042353.0  2.640850e+08    7.142857   50.000000 -42.857143
        000592  2025-01-23   2.94   2.98   2.85   2.85  123229303.0  3.585729e+08   28.571429   42.857143 -14.285714
        000592  2025-01-24   2.82   2.93   2.75   2.88  124897339.0  3.530088e+08   21.428571   35.714286 -14.285714
        000592  2025-01-27   2.89   2.94   2.83   2.85   79966803.0  2.301223e+08   14.285714   28.571429 -14.285714
    """

    df = df.copy()

    # 设置日期列为索引列
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame 没有 'date' 列，也没有日期索引，无法设置索引")

    # =========================
    # 1. 计算 bars since high / low（无未来函数）
    # =========================
    def bars_since_high(x):
        return len(x) - 1 - np.argmax(x)

    def bars_since_low(x):
        return len(x) - 1 - np.argmin(x)

    bars_high = (
        df['high']
        .rolling(length)
        .apply(bars_since_high, raw=True)
    )

    bars_low = (
        df['low']
        .rolling(length)
        .apply(bars_since_low, raw=True)
    )

    # =========================
    # 2. Aroon Up / Down
    # =========================
    df['aroon_up'] = 100 * (length - bars_high) / length
    df['aroon_down'] = 100 * (length - bars_low) / length

    # =========================
    # 3. Aroon Oscillator
    # =========================
    df['aroon_osc'] = df['aroon_up'] - df['aroon_down']

    return df

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from scripts.stock_query import stock_zh_a_daily_mysql

    df = stock_zh_a_daily_mysql(
        symbol='000592',
        start_date='20250101',
        end_date='20251231',
        adjust='qfq'
    )

    print(aroon_oscillator_indicator(df))
