#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

"""
# ============================================================
# 指标 Aroon Oscillator 【TradingView 等价】
# ============================================================
"""

def aroon_oscillator_indicator(df: pd.DataFrame,length: int = 14):
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

        # 确保 length 是整数
        length = int(length)

        # 这里的关键是 rolling(length + 1)
        # 因为 Aroon 计算的是“过去 n 个周期内”，包含当前点共有 n+1 个点

        def bars_since_high(x):
            # np.argmax 返回的是第一个最大值的索引
            # 在 TV 中，如果有多个相同的最高价，通常取最近的一个
            # 因此我们用 argmax 处理反转后的数组，或者直接用下面的逻辑：
            return (len(x) - 1) - np.where(x == np.max(x))[0][-1]

        def bars_since_low(x):
            return (len(x) - 1) - np.where(x == np.min(x))[0][-1]

        # 使用 window = length + 1 来匹配 TradingView 的 14 周期逻辑
        rolling_window = length + 1

        df['bars_high'] = df['high'].rolling(rolling_window).apply(bars_since_high, raw=True)
        df['bars_low'] = df['low'].rolling(rolling_window).apply(bars_since_low, raw=True)

        # 计算公式：100 * (n - bars_since) / n
        df['aroon_up'] = 100 * (length - df['bars_high']) / length
        df['aroon_down'] = 100 * (length - df['bars_low']) / length
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
