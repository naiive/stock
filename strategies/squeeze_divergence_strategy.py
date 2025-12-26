# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator


def run_strategy(df, symbol,
                 lookback=60,         # P1 搜索回溯最大范围
                 min_dist=10,         # P1 与 P2 两个低点之间的最小 K 线间隔
                 p2_bright_len=5,     # P2 及其左侧必须连续多少根“Red” (亮红)
                 p1_valley_win=5,     # P1 局部波谷确认窗口
                 p1_mom_buffer=1.15): # P1 动能绝对值必须是 P2 的 1.15 倍以上
        """
        SQZMOM 底背离策略 - 动能衰减增强版
        """
        try:
            df = squeeze_momentum_indicator(df)
            if len(df) < lookback + 20: return None

            moms = df['sqz_hvalue'].values
            hcolors = df['sqz_hcolor'].values
            lows = df['low'].values
            dates = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d').values
            curr_idx = len(df) - 1

            # --- 第一阶段：判定 P2 (暗红转折 + 连续 Red) ---
            # 1. 今天必须是 Maroon (暗红)
            if hcolors[curr_idx] != 'maroon':
                return None

            # 2. 昨天 (P2) 及其左侧共 5 根必须是 Red (亮红)
            p2_idx = curr_idx - 1
            for i in range(p2_idx, p2_idx - p2_bright_len, -1):
                if i <= 0 or hcolors[i] != 'red':
                    return None

            # 3. 锁定 P2 数值
            p2_mom = moms[p2_idx]
            p2_price = lows[p2_idx]
            p2_date = dates[p2_idx]

            # --- 第二阶段：回溯 P1 (寻找更深的左坑) ---
            p1_idx = None
            search_end = p2_idx - min_dist
            search_start = max(p1_valley_win, search_end - lookback)

            for i in range(search_end, search_start, -1):
                curr_m = moms[i]
                if curr_m < 0:
                    # P1 局部波谷确认
                    window = moms[i - p1_valley_win: i + p1_valley_win + 1]
                    if curr_m == np.min(window):
                        # 关键判定：动能衰减必须达标 (P1 的坑必须比 P2 深 buffer 倍)
                        # 负数比较：p1_mom 必须更小 (例如 -11.5 < -5 * 1.15)
                        if curr_m <= (p2_mom * p1_mom_buffer):
                            p1_idx = i
                            break

            if p1_idx is None: return None

            p1_mom = moms[p1_idx]
            p1_price = lows[p1_idx]
            p1_date = dates[p1_idx]

            # --- 第三阶段：最终背离确认与质量打分 ---
            # 1. 价格新低或双底 (P2 <= P1)
            # 2. 动能衰减 (P2 > P1)
            if p2_price < p1_price and p2_mom > p1_mom:
                # 计算衰减比：P1深/P2深。比值越大，质量越高。
                mom_ratio = round(p1_mom / p2_mom, 2) if p2_mom != 0 else 0

                return {
                    'symbol': symbol,
                    'signal': 'Bullish_Divergence',
                    'p1_info': {'date': p1_date, 'price': p1_price, 'mom': round(p1_mom, 4)},
                    'p2_info': {'date': p2_date, 'price': p2_price, 'mom': round(p2_mom, 4)},
                    'quality_metrics': {
                        'mom_ratio': mom_ratio,  # 越高越好
                        'price_drop': round((p1_price - p2_price) / p1_price * 100, 2),  # 价格破位比例
                        'time_dist': p2_idx - p1_idx  # 两个波谷的时间间距
                    }
                }

        except Exception as e:
            print(f"Error {symbol}: {e}")
            return None
        return None