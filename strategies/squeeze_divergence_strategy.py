# -*- coding: utf-8 -*-

import numpy as np

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

        # 这里的 df.index 假设是时间格式
        times = df.index
        mom = df['sqz_hvalue'].values
        low = df['low'].values
        curr_idx = len(df) - 1

        # --- 1. 判定 P2 (当前触发点) ---
        # 当前必须是红色暗红 (数值比前一根大，且小于0)
        if not (mom[curr_idx] < 0 and mom[curr_idx] > mom[curr_idx - 1]):
            return None

        # 向上追溯亮红柱数量
        bright_count = 0
        p2_valley_idx = curr_idx - 1

        for i in range(curr_idx - 1, 0, -1):
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
        p2_time = times[p2_valley_idx]  # 获取P2发生的时间

        # --- 2. 寻找历史参考点 P1 (80根内) ---
        p1_idx = None
        search_end = p2_valley_idx - 1
        search_start = max(p1_confirm_win, search_end - lookback)

        for i in range(search_end, search_start, -1):
            if mom[i] < 0:
                window_data = mom[i - p1_confirm_win: i + p1_confirm_win + 1]
                if mom[i] == np.min(window_data):
                    p1_idx = i
                    break

        if p1_idx is None:
            return None

        # P1 对比数据
        p1_val_mom = mom[p1_idx]
        p1_val_price = low[p1_idx]
        p1_time = times[p1_idx]  # 获取P1发生的时间

        # --- 3. 最终底背离对比 (包含等于) ---
        if p2_val_price <= p1_val_price and p2_val_mom >= p1_val_mom:
            # 组织返回结果
            res = {
                'symbol': symbol,
                'signal': 'Bullish_Divergence',
                'time_now': times[curr_idx],  # 当前触发时间
                'p1_info': {
                    'time': p1_time,
                    'price': round(float(p1_val_price), 2),
                    'mom': round(float(p1_val_mom), 4)
                },
                'p2_info': {
                    'time': p2_time,
                    'price': round(float(p2_val_price), 2),
                    'mom': round(float(p2_val_mom), 4)
                },
                'p2_bright_len': bright_count + 1
            }

            # 打印信息方便调试
            print(f"[{symbol}] 底背离触发!")
            print(f"P1(历史): {p1_time} | 价格: {p1_val_price}")
            print(f"P2(当前): {p2_time} | 价格: {p2_val_price}")

            return res

    except Exception as e:
        raise e

    return None