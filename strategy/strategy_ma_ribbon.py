#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ============================================================
# 策略 MA Ribbon
  注意：根据历史K线的多少会有点误差，不能完全和tradingview一样
# ============================================================
"""

import pandas as pd
from conf.config import INDICATOR_CONFIG

def ma_ribbon_indicator(
    df: pd.DataFrame,
    length: int = 200
) -> pd.DataFrame:
    """
    直接用自带的吧，不要手写
    :return:
        code        date  open  high   low  close       volume        amount   ema_200
        600628  2025-12-08  7.77  7.83  7.71   7.77   13255088.0  1.028871e+08  7.485253
        600628  2025-12-09  7.75  7.95  7.67   7.86   25256601.0  1.982662e+08  7.488981
        600628  2025-12-10  7.93  8.10  7.85   7.90   26390258.0  2.096010e+08  7.493071
        600628  2025-12-11  7.85  7.92  7.56   7.64   23619600.0  1.814402e+08  7.494533
        600628  2025-12-12  7.65  7.65  7.39   7.40   20526300.0  1.531835e+08  7.493592
        600628  2025-12-15  7.43  8.14  7.41   7.95   52808074.0  4.177851e+08  7.498134
        600628  2025-12-16  8.00  8.47  8.00   8.10   68590070.0  5.611935e+08  7.504123
        600628  2025-12-17  8.22  8.31  7.99   8.08   47891061.0  3.907951e+08  7.509853
        600628  2025-12-18  8.00  8.89  7.99   8.89   86095168.0  7.394343e+08  7.523586
        600628  2025-12-19  9.00  9.77  8.88   9.11  121830916.0  1.129386e+09  7.539371
    """

    length = INDICATOR_CONFIG.get("EMA", {}).get("length", length)

    col_name = f"ema_{length}"

    df[col_name] = df["close"].ewm(span=length, adjust=False).mean()

    return df


if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from api.stock_query import stock_zh_a_daily_mysql

    print(ma_ribbon_indicator(stock_zh_a_daily_mysql(
        symbol='600628',
        start_date='20240101',
        end_date='20251219',
        adjust='qfq')).tail(10))