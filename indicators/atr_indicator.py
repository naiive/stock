#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ============================================================
# 指标 Average True Range Stop Loss Finder [veryfid]
# ============================================================
"""

import numpy as np
import pandas as pd
from conf.config import INDICATOR_CONFIG

def true_range(df: pd.DataFrame) -> pd.Series:
    """
    计算 True Range（真实波幅）
    参数：
        df : DataFrame
        必须包含列：high, low, close
    返回：
        Series 每根 K 线对应的真实波幅
    """
    # 前一根 K 线的收盘价
    prev_close = df["close"].shift(1)

    # 三种波幅的最大值
    tr = pd.concat(
        [
            df["high"] - df["low"],                 # 当根振幅
            (df["high"] - prev_close).abs(),        # 向上跳空
            (df["low"] - prev_close).abs(),         # 向下跳空
        ],
        axis=1,
    ).max(axis=1)

    return tr

def ma_smoothing(series: pd.Series, length: int, method: str) -> pd.Series:
    """
    对序列进行平滑处理，用于 ATR 计算
    参数：
        series : Series
            输入序列（通常是 True Range）
        length : int
            平滑周期
        method : str
            平滑方法：RMA / SMA / EMA / WMA
    返回：
        Series
            平滑后的结果
    """
    method = method.upper()

    # --- SMA（简单移动平均）---
    if method == "SMA":
        return series.rolling(length).mean()

    # --- EMA（指数移动平均）---
    if method == "EMA":
        # adjust=False → 与 TradingView ema() 行为一致
        return series.ewm(span=length, adjust=False).mean()

    # --- RMA（Wilder Moving Average，ATR 默认）---
    if method == "RMA":
        rma = np.full(len(series), np.nan)

        # 数据不足直接返回 NaN
        if len(series) < length:
            return pd.Series(rma, index=series.index)

        # Pine / Wilder 定义：
        # 第一根 RMA = 前 length 根 SMA
        rma[length - 1] = series.iloc[:length].mean()

        alpha = 1 / length
        for i in range(length, len(series)):
            rma[i] = alpha * series.iloc[i] + (1 - alpha) * rma[i - 1]

        return pd.Series(rma, index=series.index)

    # --- WMA（加权移动平均）---
    if method == "WMA":
        weights = np.arange(1, length + 1)

        return series.rolling(length).apply(
            lambda x: np.dot(x, weights) / weights.sum(),
            raw=True,
        )

    raise ValueError(f"Unknown smoothing method: {method}")

def atr_indicator(
    df: pd.DataFrame,
    length: int = 14,
    multiplier: float = 1.5,
    smoothing: str = "RMA",
) -> pd.DataFrame:
    """
    ATR Stop Loss Finder（核心策略）

    length : int ATR 周期（默认 14）
    multiplier : float ATR 倍数（止损距离）
    smoothing : str 平滑方式：RMA / SMA / EMA / WMA
    返回：
        code        date  open  high   low  close       volume        amount       atr  atr_mult  atr_short_stop  atr_long_stop
        600628  2025-12-08  7.77  7.83  7.71   7.77   13255088.0  1.028871e+08  0.209239  0.313858        8.143858       7.396142
        600628  2025-12-09  7.75  7.95  7.67   7.86   25256601.0  1.982662e+08  0.214293  0.321440        8.271440       7.348560
        600628  2025-12-10  7.93  8.10  7.85   7.90   26390258.0  2.096010e+08  0.216844  0.325265        8.425265       7.524735
        600628  2025-12-11  7.85  7.92  7.56   7.64   23619600.0  1.814402e+08  0.227069  0.340604        8.260604       7.219396
        600628  2025-12-12  7.65  7.65  7.39   7.40   20526300.0  1.531835e+08  0.229421  0.344132        7.994132       7.045868
        600628  2025-12-15  7.43  8.14  7.41   7.95   52808074.0  4.177851e+08  0.265891  0.398837        8.538837       7.011163
        600628  2025-12-16  8.00  8.47  8.00   8.10   68590070.0  5.611935e+08  0.284042  0.426063        8.896063       7.573937
        600628  2025-12-17  8.22  8.31  7.99   8.08   47891061.0  3.907951e+08  0.286610  0.429915        8.739915       7.560085
        600628  2025-12-18  8.00  8.89  7.99   8.89   86095168.0  7.394343e+08  0.330424  0.495636        9.385636       7.494364
        600628  2025-12-19  9.00  9.77  8.88   9.11  121830916.0  1.129386e+09  0.370394  0.555590       10.325590       8.324410
    """

    length = INDICATOR_CONFIG.get("ATR", {}).get("length", length)
    multiplier = INDICATOR_CONFIG.get("ATR", {}).get("multiplier", multiplier)
    smoothing = INDICATOR_CONFIG.get("ATR", {}).get("smoothing", smoothing)

    # --- True Range ---
    tr = true_range(df)

    # --- ATR（TR 平滑）---
    atr = ma_smoothing(tr, length, smoothing)

    # Pine Script:
    # a = ATR * m
    # x = ATR * m + high
    # x2 = low - ATR * m
    df["atr"] = atr
    df["atr_mult"] = atr * multiplier

    # 空头止损（价格向上突破）
    df["atr_short_stop"] = df["high"] + df["atr_mult"]

    # 多头止损（价格向下跌破）
    df["atr_long_stop"] = df["low"] - df["atr_mult"]

    return df

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from scripts.stock_query import stock_zh_a_daily_mysql

    print(atr_indicator(stock_zh_a_daily_mysql(
        symbol="600628",
        start_date="20240101",
        end_date="20251219",
        adjust="qfq" )).tail(10))