# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(df, symbol):
    """
    SQZMOM 底背离策略逻辑实现
    """
    try:
        # 计算指标
        df = squeeze_momentum_indicator(df)

        if len(df) < 100:
            return None

        # --- 变量定义 ---
        lookback = 80
        p2_min_bright = 5
        p1_confirm_win = 3

        # --- 价格与均线前置过滤 (快速剪枝，提升扫描速度) ---
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        # --- 关键修正：从列中获取日期而非 index ---
        dates = df['date'].values
        dates = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d').values
        mom = df['sqz_hvalue'].values
        low = df['low'].values
        curr_idx = len(df) - 1

        # --- 1. 判定 P2 (当前触发点) ---
        # 处理可能存在的 NaN 值
        if np.isnan(mom[curr_idx]) or np.isnan(mom[curr_idx - 1]):
            return None

        # 当前必须是红色暗红 (数值比前一根大，且小于0)
        if not (mom[curr_idx] < 0 and mom[curr_idx] > mom[curr_idx - 1]):
            return None

        # 向上追溯亮红柱数量
        bright_count = 0
        p2_valley_idx = curr_idx - 1

        for i in range(curr_idx - 1, 0, -1):
            if np.isnan(mom[i]) or np.isnan(mom[i - 1]):
                break
            # 绝对值连续放大或等于：负数越来越小
            if mom[i] <= mom[i - 1] < 0:
                bright_count += 1
            else:
                p2_valley_idx = i
                break

        if bright_count < (p2_min_bright - 1):
            return None

        # P2 对比数据
        p2_val_mom = mom[p2_valley_idx]
        p2_val_price = low[p2_valley_idx]
        p2_date = dates[p2_valley_idx]  # 获取正确的 P2 日期

        # --- 2. 寻找历史参考点 P1 (80根内) ---
        p1_idx = None
        search_end = p2_valley_idx - 1
        search_start = max(p1_confirm_win, search_end - lookback)

        for i in range(search_end, search_start, -1):
            curr_m = mom[i]
            if not np.isnan(curr_m) and curr_m < 0:
                window_data = mom[i - p1_confirm_win: i + p1_confirm_win + 1]
                # 确保窗口内没有 NaN 且当前是最小值
                if not np.isnan(window_data).any() and curr_m == np.min(window_data):
                    p1_idx = i
                    break

        if p1_idx is None:
            return None

        # P1 对比数据
        p1_val_mom = mom[p1_idx]
        p1_val_price = low[p1_idx]
        p1_date = dates[p1_idx]  # 获取正确的 P1 日期

        # --- 3. 最终底背离对比 (包含等于) ---
        if p2_val_price <= p1_val_price and p2_val_mom >= p1_val_mom:
            # 组织返回结果
            res = {
                '日期': dates[curr_idx],
                '代码': symbol,
                '背离': '底背离',
                '当前价': round(current_close, 2),
                '涨幅(%)': round(pct_chg, 2),
                '左波谷': {
                    '日期': p1_date,
                    '最低价': round(float(p1_val_price), 2),
                    '动能柱值': round(float(p1_val_mom), 4)
                },
                '右波谷': {
                    '日期': p2_date,
                    '最低价': round(float(p2_val_price), 2),
                    '动能柱值': round(float(p2_val_mom), 4)
                }
            }

            return res

    except Exception as e:
        raise e

    return None