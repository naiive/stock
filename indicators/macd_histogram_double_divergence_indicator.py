#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

def macd_histogram_double_divergence_indicator(
        df: pd.DataFrame,
        fast: int = 13,
        slow: int = 34,
        signal: int = 9,
        lb_left: int = 2,          # 左侧回溯数量
        lb_right: int = 2,         # 右侧回溯数量
        size_factor: float = 0.1,  # 最小高度门槛系数
        size_ratio: float = 1.2,   # 左坑 / 右坑 深度比值
        max_green: int = 0         # 允许最大绿柱数量
):
    """
    MACD Histogram 双底背离（严格形态学定义）
    信号记录在“确认完成的那根 K 线收盘”

date        code   open   high    low  close       volume        amount      macd    signal    hist  min_threshold  macd_bull macd_l_date  macd_l_hist macd_r_date  macd_r_hist
2022-10-10  002352  44.09  44.19  42.23  42.32   12142769.0  5.597914e+08 -0.952977 -0.773765 -0.1792       0.065333      False         NaT          NaN         NaT          NaN
2022-10-11  002352  42.60  42.66  41.50  41.88   15974383.0  7.188769e+08 -1.089944 -0.837001 -0.2529       0.065117      False         NaT          NaN         NaT          NaN
2022-10-12  002352  41.79  42.56  40.96  42.51   13670971.0  6.128951e+08 -1.137739 -0.897148 -0.2406       0.065035      False         NaT          NaN         NaT          NaN
2022-10-13  002352  42.19  42.98  42.06  42.51   11500925.0  5.270049e+08 -1.167078 -0.951134 -0.2159       0.065074       True  2022-09-26      -0.3252  2022-10-11      -0.2529
2022-10-14  002352  44.19  46.28  44.04  46.00   39387470.0  1.935313e+09 -0.882118 -0.937331  0.0552       0.065148      False         NaT          NaN         NaT          NaN
    """

    df = df.copy()

    # 设置日期列为索引列
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame 没有 'date' 列，也没有日期索引，无法设置索引")

    # ──────────────
    # 1. MACD计算
    # ──────────────
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    df['macd'] = ema_fast - ema_slow
    df['signal'] = df['macd'].ewm(span=signal, adjust=False).mean()
    df['hist'] = (df['macd'] - df['signal']).round(4)

    # 动态最小门槛
    df['min_threshold'] = df['signal'].abs().rolling(100).mean() * size_factor

    # ──────────────
    # 2. 输出列初始化
    # ──────────────
    df['macd_bull'] = False
    df['macd_l_date'] = pd.NaT
    df['macd_l_hist'] = np.nan
    df['macd_r_date'] = pd.NaT
    df['macd_r_hist'] = np.nan

    # ──────────────
    # 3. 状态变量
    # ──────────────
    left_peak = None  # (idx, hist, low, date)
    green_count = 0

    hist_vals = df['hist'].values
    low_vals = df['low'].values
    date_vals = df.index.values
    thr_vals = df['min_threshold'].values
    n = len(df)

    # ──────────────
    # 4. 主循环
    # ──────────────
    for i in range(n):
        # 绿柱计数
        if hist_vals[i] > 0:
            green_count += 1
        else:
            if left_peak is None:
                green_count = 0

        # 熔断
        if green_count > max_green:
            left_peak = None

        # 右侧确认
        detect_idx = i - lb_right
        if detect_idx < lb_left:
            continue

        center_h = hist_vals[detect_idx]
        is_peak = False

        if center_h < 0:
            left_win = hist_vals[detect_idx - lb_left: detect_idx]
            right_win = hist_vals[detect_idx + 1: detect_idx + lb_right + 1]

            if not (np.any(left_win < center_h) or np.any(right_win < center_h)):
                if abs(center_h) >= thr_vals[detect_idx]:
                    is_peak = True

        if is_peak:
            curr_h = center_h
            curr_l = low_vals[detect_idx]
            curr_d = date_vals[detect_idx]

            if left_peak is None:
                left_peak = (detect_idx, curr_h, curr_l, curr_d)
                green_count = 0
            else:
                prev_idx, prev_h, prev_l, prev_d = left_peak
                ratio_ok = abs(prev_h / curr_h) >= size_ratio
                price_ok = curr_l <= prev_l
                energy_ok = curr_h > prev_h

                if ratio_ok and price_ok and energy_ok:
                    # ✅ 信号写在确认完成的当前 K 线 i
                    df.iloc[i, df.columns.get_loc('macd_bull')] = True
                    df.iloc[i, df.columns.get_loc('macd_l_date')] = prev_d
                    df.iloc[i, df.columns.get_loc('macd_l_hist')] = prev_h
                    df.iloc[i, df.columns.get_loc('macd_r_date')] = curr_d
                    df.iloc[i, df.columns.get_loc('macd_r_hist')] = curr_h

                left_peak = (detect_idx, curr_h, curr_l, curr_d)
                green_count = 0

    return df

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from scripts.stock_query import stock_zh_a_daily_mysql

    df = stock_zh_a_daily_mysql(
        symbol='002352',
        start_date='20210101',
        end_date='20251214',
        adjust='qfq'
    )

    result = macd_histogram_double_divergence_indicator(df)
    print(result)
