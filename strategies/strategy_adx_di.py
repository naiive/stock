#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ============================================================
# 策略 ADX and DI [BeikabuOyaji]
# ============================================================
"""

import pandas as pd
import numpy as np
from conf.config import INDICATOR_CONFIG

def wilder_smoothing(series, length):
    """
    实现 Pine Script 中 ADX/DI 所使用的 Wilder's Smoothing 逻辑。
    SmoothedValue = Prev_SmoothedValue - (Prev_SmoothedValue / length) + CurrentValue
    """
    # 转换为 numpy 数组以便进行迭代计算
    values = series.values
    smoothed = np.empty_like(values)
    smoothed.fill(np.nan)

    # 初始化第一个值（通常为前 length 个值的 SMA，但Pine Script中是基于累积的逻辑）
    # 在许多技术分析库中，第一个平滑值直接使用前 length 个值的简单平均。
    # 为了简化且不引入复杂的迭代，我们采用技术分析库常用的惯例：
    # 第一个平滑值设置为前 length 个值的 SMA
    smoothed[length - 1] = np.sum(values[:length])

    # 从第 length 个值开始应用 Wilder's Smoothing
    for i in range(length, len(values)):
        smoothed[i] = smoothed[i - 1] - (smoothed[i - 1] / length) + values[i]

    return pd.Series(smoothed, index=series.index)

def adx_di_indicator(
        df: pd.DataFrame,
        length: int = 14,
        threshold: int = 20
) -> pd.DataFrame:
    """
    计算 ADX, +DI, -DI 指标

    :param df: 包含 'high', 'low', 'close' 的 DataFrame
    :param length: ADX/DI 计算周期 (默认 14)
    :param threshold: ADX 阈值 (默认 20)
    :return:
          code        date   open   high    low  close      volume        amount   adx_plus  adx_minus        adx
        600218  2024-02-04   7.81   7.90   7.69   7.80   6532800.0  5.111882e+07        NaN        NaN        NaN
        600218  2025-02-05   7.62   7.70   7.54   7.67   6697900.0  5.131817e+07  20.923897  23.944273   6.061578
        600218  2025-02-06   7.64   7.76   7.62   7.76   7486197.0  5.821225e+07  21.635697  23.167209   4.484860
        600218  2025-02-07   7.78   7.99   7.76   7.90   9639047.0  7.676530e+07  25.890815  21.909249   4.198591
    """
    length = INDICATOR_CONFIG.get("ADX", {}).get("length", length)
    threshold = INDICATOR_CONFIG.get("ADX", {}).get("threshold", threshold)

    # --- 1. 计算 True Range (TR) ---
    high_low = df['high'] - df['low']
    high_prev_close = np.abs(df['high'] - df['close'].shift(1))
    low_prev_close = np.abs(df['low'] - df['close'].shift(1))

    df['TrueRange'] = high_low.combine(high_prev_close, max).combine(low_prev_close, max)

    # --- 2. 计算 Directional Movement (+DM, -DM) ---
    up_move = df['high'] - df['high'].shift(1)
    down_move = df['low'].shift(1) - df['low']

    # +DM 逻辑: UpMove > DownMove 且 UpMove > 0
    df['adx_plus'] = np.where((up_move > down_move) & (up_move > 0), up_move, 0)

    # -DM 逻辑: DownMove > UpMove 且 DownMove > 0
    df['adx_minus'] = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

    # --- 3. Wilder's Smoothing (TR, +DM, -DM) ---
    df['SmoothedTR'] = wilder_smoothing(df['TrueRange'], length)
    df['SmoothedDMPlus'] = wilder_smoothing(df['adx_plus'], length)
    df['SmoothedDMMinus'] = wilder_smoothing(df['adx_minus'], length)

    # --- 4. 计算 +DI 和 -DI ---
    # 乘以 100
    df['adx_plus'] = (df['SmoothedDMPlus'] / df['SmoothedTR']) * 100
    df['adx_minus'] = (df['SmoothedDMMinus'] / df['SmoothedTR']) * 100

    # --- 5. 计算 DX (Directional Index) ---
    # DX = |+DI - -DI| / (+DI + -DI) * 100
    # 避免除以零
    sum_di = df['adx_plus'] + df['adx_minus']
    df['DX'] = np.where(sum_di != 0, np.abs(df['adx_plus'] - df['adx_minus']) / sum_di * 100, 0)

    # --- 6. 计算 ADX (DX 的 SMA) ---
    # Pine Script 中 ADX = sma(DX, len)。在 ADX/DMI 系统中，这通常也意味着 Wilder's Smoothing
    # 但为严格遵循您的 Pine Script 代码，我们使用标准的 SMA：
    df['adx'] = df['DX'].rolling(window=length).mean()

    # --- 7. 删除一些中间结果列 ---
    df.drop(columns=['TrueRange', 'SmoothedTR', 'SmoothedDMPlus', 'SmoothedDMMinus', 'DX'], inplace=True)

    return df

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from api.stock_query import stock_zh_a_daily_mysql

    print(adx_di_indicator(stock_zh_a_daily_mysql(
        symbol='600519',
        start_date='20250101',
        end_date='20251219',
        adjust='qfq')))