#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ============================================================
# 指标 WaveTrend with Crosses [LazyBear]
# ============================================================
"""

import pandas as pd
import numpy as np

from conf.config import INDICATOR_CONFIG

def wavetrend_with_crosses_indicator(
        df: pd.DataFrame,
        channelLength: int = 10,
        averageLength: int = 21,
        smaLength: int = 4,
        obLevel1: int = 60,
        obLevel2: int = 53,
        osLevel1: int = -60,
        osLevel2: int = -53
) -> pd.DataFrame:
    """
    WaveTrend (LazyBear) 指标计算

    返回：
    code        date     open     high      low    close      volume        amount   wtc_green     wtc_red    wtc_value wtc_signal
    600519  2025-01-02  1494.71  1495.19  1451.55  1459.40   5002870.0  7.490884e+09         NaN         NaN        NaN         no
    600519  2025-01-03  1465.77  1466.25  1438.81  1446.65   3262836.0  4.836610e+09 -366.666667         NaN        NaN         no
    600519  2025-01-06  1425.07  1434.54  1405.26  1412.32   4425512.0  6.392480e+09 -358.798141         NaN        NaN         no
    600519  2025-01-07  1416.89  1423.98  1411.34  1412.52   2422118.0  3.494634e+09 -340.908870         NaN        NaN         no
    600519  2025-01-08  1412.32  1423.91  1399.24  1414.77   3525821.0  5.071821e+09 -320.814422 -346.797025  25.982603         no
    600519  2025-02-24  1459.40  1470.70  1445.67  1450.64   3474373.0  5.157907e+09    0.963244   -7.993121   8.956365         no
    600519  2025-02-25  1441.75  1445.07  1424.09  1426.05   2838743.0  4.142814e+09   -2.549806   -4.160060   1.610255         no
    600519  2025-02-26  1427.47  1436.80  1417.22  1431.95   2636609.0  3.835949e+09   -6.374483   -3.000268  -3.374215        red
    600519  2025-02-27  1431.96  1461.26  1426.05  1457.00   4976217.0  7.368002e+09   -2.199842   -2.540222   0.340380      green
    600519  2025-03-04  1456.46  1457.44  1437.05  1441.85   2521121.0  3.710676e+09   11.319208    8.035323   3.283885         no
    600519  2025-03-05  1443.71  1445.67  1432.03  1438.18   2460522.0  3.606932e+09    6.204590   10.136431  -3.931841        red
    600519  2025-03-06  1445.67  1481.17  1443.78  1477.03   4216764.0  6.297117e+09   12.823677   11.173236   1.650441      green
    600519  2025-03-07  1474.11  1498.98  1474.11  1491.76   3799135.0  5.760609e+09   22.674227   13.255426   9.418802         no
    600519  2025-03-10  1490.60  1497.53  1477.19  1493.63   3136907.0  4.758789e+09   29.129179   17.707918  11.421260         no
    600519  2025-03-20  1604.27  1606.12  1564.41  1573.17   3673677.0  5.903879e+09   63.326572   63.601947  -0.275374        red
    600519  2025-03-21  1568.26  1581.21  1538.67  1543.50   3361430.0  5.324973e+09   57.717900   62.716781  -4.998881         no

    """
    channelLength = INDICATOR_CONFIG.get("WTC", {}).get("channelLength", channelLength)
    averageLength = INDICATOR_CONFIG.get("WTC", {}).get("averageLength", averageLength)
    smaLength = INDICATOR_CONFIG.get("WTC", {}).get("smaLength", smaLength)

    # 上下超买、卖水平线1和2
    obLevel1 = INDICATOR_CONFIG.get("WTC", {}).get("obLevel1", obLevel1)
    obLevel2 = INDICATOR_CONFIG.get("WTC", {}).get("obLevel2", obLevel2)
    osLevel1 = INDICATOR_CONFIG.get("WTC", {}).get("osLevel1", osLevel1)
    osLevel2 = INDICATOR_CONFIG.get("WTC", {}).get("osLevel2", osLevel2)

    # ---------- HLC3 (TradingView: hlc3) ----------
    # 用于平滑价格，减少单一 close 噪声
    ap = (df['high'] + df['low'] + df['close']) / 3

    # ---------- ESA (EMA of price) ----------
    # 类似“价格基准线”
    esa = ap.ewm(span=channelLength, adjust=False).mean()

    # ---------- D (价格偏离的 EMA) ----------
    # 衡量波动强度，避免除以 0
    d = (ap - esa).abs().ewm(span=channelLength, adjust=False).mean()

    # ---------- CI (Composite Index) ----------
    # 核心震荡源
    ci = (ap - esa) / (0.015 * d)

    # ---------- wtc_green (主线) ----------
    wtc_green = ci.ewm(span=averageLength, adjust=False).mean()

    # ---------- wtc_red (信号线) ----------
    wtc_red = wtc_green.rolling(window=smaLength).mean()

    # ---------- wtc 差值（蓝色动能柱） ----------
    wtc_value = wtc_green - wtc_red

    # ---------- 交叉判断 ----------
    # 金叉：wtc_green 从下向上穿过 wtc_red
    cross_up = (wtc_green.shift(1) < wtc_red.shift(1)) & (wtc_green > wtc_red)

    # 死叉：wtc_green 从上向下穿过 wtc_red
    cross_down = (wtc_green.shift(1) > wtc_red.shift(1)) & (wtc_green < wtc_red)

    # ---------- 信号编码 ----------
    signal = np.where(
        cross_up, "green",
        np.where(cross_down, "red", "no")
    )

    # ---------- 写回 DataFrame ----------
    df['wtc_green'] = wtc_green
    df['wtc_red'] = wtc_red
    df['wtc_value'] = wtc_value
    df['wtc_signal'] = signal

    return df

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from scripts.stock_query import stock_zh_a_daily_mysql

    print(wavetrend_with_crosses_indicator(stock_zh_a_daily_mysql(
        symbol='600519',
        start_date='20250101',
        end_date='20251219',
        adjust='qfq')))
