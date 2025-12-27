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
    """

    # ─────────────────────
    # 0. 数据准备
    # ─────────────────────
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    required_cols = {'open', 'high', 'low', 'close', 'date'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"DataFrame 必须包含列: {required_cols}")

    # 统一日期格式（保证输出是日期而不是序号）
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

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
    date_vals = df['date'].values          # ✅ 关键修复点
    thr_vals = df['min_threshold'].values

    n = len(df)

    # ─────────────────────
    # 4. 主循环
    # ─────────────────────
    for i in range(n):

        # ---- 绿柱计数（严格 Pine 语义） ----
        if hist_vals[i] > 0:
            green_count += 1
        else:
            if left_peak is None:
                green_count = 0

        # ---- 熔断 ----
        if green_count > max_green:
            left_peak = None

        # ---- 右侧确认完成后的检测点 ----
        detect_idx = i - lb_right
        if detect_idx < lb_left:
            continue

        # ---- 严格极小值（双底） ----
        center_h = hist_vals[detect_idx]
        is_peak = False

        if center_h < 0:
            left_win = hist_vals[detect_idx - lb_left: detect_idx]
            right_win = hist_vals[detect_idx + 1: detect_idx + lb_right + 1]

            if not (np.any(left_win < center_h) or np.any(right_win < center_h)):
                if abs(center_h) >= thr_vals[detect_idx]:
                    is_peak = True

        # ---- 双坑匹配 ----
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

                # ✅ 信号写在确认完成的当前 K 线 i
                if ratio_ok and price_ok and energy_ok:
                    df.at[i, 'macd_bull'] = 'Yes'
                    df.at[i, 'macd_l_date'] = prev_d
                    df.at[i, 'macd_l_hist'] = prev_h
                    df.at[i, 'macd_r_date'] = curr_d
                    df.at[i, 'macd_r_hist'] = curr_h

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
