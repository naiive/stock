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
        max_green: int = 3         # 允许最大绿柱数量
):
    """
    MACD Histogram 双底背离（严格形态学定义）
    信号记录在“确认完成的那根 K 线收盘”

    date        code   open   high    low  close      volume       amount      macd    signal    hist  min_threshold macd_bull macd_l_date  macd_l_hist macd_r_date  macd_r_hist
    2022-10-13  002352  42.19  42.98  42.06  42.51  11500925.0  527004904.0 -1.167078 -0.951134 -0.2159       0.065074       Yes   2022-09-26       -0.3252   2022-10-11       -0.2529
    2023-03-09  002352  49.59  49.77  48.70  48.84  10725106.0  566835128.0 -1.353248 -1.180551 -0.1727       0.104108       Yes   2023-02-17       -0.6060   2023-03-07       -0.1876
    2023-06-02  002352  44.45  45.70  44.40  45.47  12603658.0  609825139.0 -1.888747 -1.790827 -0.0979       0.094730       Yes   2023-05-12       -0.7056   2023-05-31       -0.2335
    2023-09-25  002352  38.22  38.22  37.61  37.77  18921624.0  765691355.0 -1.692218 -1.518574 -0.1736       0.097649       Yes   2023-08-25       -0.5294   2023-09-21       -0.2181
    2024-12-09  002352  39.76  40.38  39.44  40.00  19787895.0  808077051.0 -0.258906  0.100025 -0.3589       0.094894       Yes   2024-11-22       -0.6094   2024-12-05       -0.4587
    2024-12-20  002352  39.92  40.14  39.44  39.44  19051703.0  772512443.0 -0.351906 -0.232028 -0.1199       0.091168       Yes   2024-12-05       -0.4587   2024-12-18       -0.1725
    """

    # ─────────────────────
    # 0. 数据准备
    # ─────────────────────
    df = df.copy()

    df.columns = [c.lower() for c in df.columns]
    required_cols = {'open', 'high', 'low', 'close'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"DataFrame 必须包含列: {required_cols}")

    # ─────────────────────
    # 1. MACD 计算
    # ─────────────────────
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()

    df['macd'] = ema_fast - ema_slow
    df['signal'] = df['macd'].ewm(span=signal, adjust=False).mean()
    df['hist'] = (df['macd'] - df['signal']).round(4)

    # 动态最小门槛
    df['min_threshold'] = df['signal'].abs().rolling(100).mean() * size_factor

    # ─────────────────────
    # 2. 输出列初始化
    # ─────────────────────
    df['macd_bull'] = 'No'
    df['macd_l_date'] = None
    df['macd_l_hist'] = np.nan
    df['macd_r_date'] = None
    df['macd_r_hist'] = np.nan

    # ─────────────────────
    # 3. 状态变量
    # ─────────────────────
    left_peak = None        # (idx, hist, low, date)
    green_count = 0

    hist_vals = df['hist'].values
    low_vals = df['low'].values
    date_vals = df.index.values
    thr_vals = df['min_threshold'].values

    n = len(df)

    # ─────────────────────
    # 4. 主循环
    # ─────────────────────
    for i in range(n):

        # ---- 绿柱计数（严格等价 Pine 语义） ----
        if hist_vals[i] > 0:
            green_count += 1
        else:
            if left_peak is None:
                green_count = 0

        # ---- 物理熔断 ----
        if green_count > max_green:
            left_peak = None

        # ---- 右侧确认完成后的检测点 ----
        detect_idx = i - lb_right
        if detect_idx < lb_left:
            continue

        # ---- checkPeak：严格极值 ----
        center_h = hist_vals[detect_idx]
        is_peak = False

        if center_h < 0:
            left_win = hist_vals[detect_idx - lb_left: detect_idx]
            right_win = hist_vals[detect_idx + 1: detect_idx + lb_right + 1]

            if not (np.any(left_win < center_h) or np.any(right_win < center_h)):
                if abs(center_h) >= thr_vals[detect_idx]:
                    is_peak = True

        # ---- 波峰匹配逻辑 ----
        if is_peak:
            curr_h = center_h
            curr_l = low_vals[detect_idx]
            curr_d = date_vals[detect_idx]

            if left_peak is None:
                # 锁定左坑
                left_peak = (detect_idx, curr_h, curr_l, curr_d)
                green_count = 0
            else:
                prev_idx, prev_h, prev_l, prev_d = left_peak

                ratio_ok = abs(prev_h / curr_h) >= size_ratio
                price_ok = curr_l <= prev_l
                energy_ok = curr_h > prev_h   # 右坑更浅

                # ✅ 信号写在“确认完成的当前 K 线 i”
                if ratio_ok and price_ok and energy_ok:
                    df.iloc[i, df.columns.get_loc('macd_bull')] = 'Yes'
                    df.iloc[i, df.columns.get_loc('macd_l_date')] = prev_d
                    df.iloc[i, df.columns.get_loc('macd_l_hist')] = prev_h
                    df.iloc[i, df.columns.get_loc('macd_r_date')] = curr_d
                    df.iloc[i, df.columns.get_loc('macd_r_hist')] = curr_h

                # 滑动窗口
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
    print(result[result['macd_bull'] == 'Yes'])
