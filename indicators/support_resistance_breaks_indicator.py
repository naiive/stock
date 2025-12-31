#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

"""
# ============================================================
# 指标 Support and Resistance Levels with Breaks 【LuxAlgo】
# ============================================================
"""

def support_resistance_breaks_indicator(
        df: pd.DataFrame,
        left_bars: int = 15,
        right_bars: int = 15,
        volume_thresh: float = 20.0
):
    """
      code        date   open   high    low  close       volume        amount  srb_support  srb_resistance
      000592  2025-12-08  12.21  12.88  11.87  12.70  519120239.0  6.418927e+09         2.93            3.92
      000592  2025-12-09  12.30  12.65  11.68  12.02  448231739.0  5.462689e+09         2.93            3.92
      000592  2025-12-10  12.30  12.80  12.06  12.49  422710508.0  5.264146e+09         2.93            3.92
      000592  2025-12-11  12.23  13.00  11.88  12.85  466741565.0  5.813562e+09         2.93            3.92
      000592  2025-12-12  12.86  13.75  12.59  13.51  462085171.0  6.146839e+09         2.93            3.92
      000592  2025-12-15  13.58  14.85  13.57  14.81  451750751.0  6.422190e+09         2.93            3.92
      000592  2025-12-16  14.36  15.97  14.10  15.36  509298688.0  7.681421e+09         2.93            3.92
      000592  2025-12-17  14.72  15.80  13.82  13.82  563511594.0  8.198770e+09         2.93            3.92
      000592  2025-12-18  13.72  14.72  13.21  14.70  491084168.0  6.907378e+09         2.93            3.92
      000592  2025-12-19  14.49  15.66  13.51  13.60  433186594.0  6.533406e+09         2.93            3.92
    """
    df = df.copy()

    # 设置日期列为索引列
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame 没有 'date' 列，也没有日期索引，无法设置索引")

    # =========================
    # 1. Pivot 计算（无未来函数）
    # =========================
    window = left_bars + right_bars + 1

    # Pivot Low
    df['pivot_low_flag'] = (
        df['low']
        .rolling(window)
        .apply(lambda x: 1 if x[left_bars] == np.min(x) else 0, raw=True)
    )

    # Pivot High
    df['pivot_high_flag'] = (
        df['high']
        .rolling(window)
        .apply(lambda x: 1 if x[left_bars] == np.max(x) else 0, raw=True)
    )

    # =========================
    # 2. 对齐 pivot 价格（确认后才生效）
    # =========================
    # pivot 出现于 N，但在 N+right_bars 确认
    df['support_raw'] = np.where(
        df['pivot_low_flag'] == 1,
        df['low'].shift(right_bars),
        np.nan
    )

    df['resistance_raw'] = np.where(
        df['pivot_high_flag'] == 1,
        df['high'].shift(right_bars),
        np.nan
    )

    # 只向未来延续（不向过去泄漏）
    df['srb_support'] = df['support_raw'].ffill()
    df['srb_resistance'] = df['resistance_raw'].ffill()

    # =========================
    # 3. Volume Oscillator（和 Pine 一致）
    # =========================
    df['vol_ema_short'] = df['volume'].ewm(span=5, adjust=False).mean()
    df['vol_ema_long'] = df['volume'].ewm(span=10, adjust=False).mean()

    df['vol_osc'] = 100 * (  (df['vol_ema_short'] - df['vol_ema_long']) / df['vol_ema_long'])

    # 删除一些结果列
    df.drop(columns=["pivot_low_flag"], inplace=True)
    df.drop(columns=["pivot_high_flag"], inplace=True)
    df.drop(columns=["support_raw"], inplace=True)
    df.drop(columns=["resistance_raw"], inplace=True)
    df.drop(columns=["vol_ema_short"], inplace=True)
    df.drop(columns=["vol_ema_long"], inplace=True)
    df.drop(columns=["vol_osc"], inplace=True)

    return df

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from scripts.stock_query import stock_zh_a_daily_mysql

    print(support_resistance_breaks_indicator(stock_zh_a_daily_mysql(
        symbol='000592',
        start_date='20250101',
        end_date='20251219',
        adjust='qfq')))
