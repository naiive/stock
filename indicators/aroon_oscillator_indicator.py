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
    """

    df = df.copy()

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
