#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
SuperTrend 买点（TradingView v4 等价实现）
- ATR Period: 10
- Multiplier: 3.0
- Buy Signal: trend 从 -1 翻转为 1
============================================================
"""

import pandas as pd
import numpy as np
import akshare as ak


# ====================== SuperTrend ======================
def supertrend(df, period=9, multiplier=3.9, change_atr=True):
    """
    df 必须包含: high, low, close
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    src = (high + low) / 2  # hl2

    # ===== True Range =====
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    # ===== ATR =====
    if change_atr:
        # 等价 TV atr()
        atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    else:
        atr = tr.rolling(period).mean()

    # ===== 基础上下轨 =====
    up = src - multiplier * atr
    dn = src + multiplier * atr

    # ===== 递归修正轨道 =====
    up_final = up.copy()
    dn_final = dn.copy()

    for i in range(1, len(df)):
        if close.iloc[i - 1] > up_final.iloc[i - 1]:
            up_final.iloc[i] = max(up.iloc[i], up_final.iloc[i - 1])
        else:
            up_final.iloc[i] = up.iloc[i]

        if close.iloc[i - 1] < dn_final.iloc[i - 1]:
            dn_final.iloc[i] = min(dn.iloc[i], dn_final.iloc[i - 1])
        else:
            dn_final.iloc[i] = dn.iloc[i]

    # ===== Trend 方向 =====
    trend = np.ones(len(df))

    for i in range(1, len(df)):
        if trend[i - 1] == -1 and close.iloc[i] > dn_final.iloc[i - 1]:
            trend[i] = 1
        elif trend[i - 1] == 1 and close.iloc[i] < up_final.iloc[i - 1]:
            trend[i] = -1
        else:
            trend[i] = trend[i - 1]

    df["trend"] = trend
    df["supertrend"] = np.where(trend == 1, up_final, dn_final)

    # ===== 买点 =====
    df["buy_signal"] = (df["trend"] == 1) & (df["trend"].shift() == -1)

    return df


# ====================== 主程序 ======================
def main():
    symbol = "000701"   # 贵州茅台
    print(f"📥 下载 {symbol} 日线数据...")

    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        adjust="qfq"
    )

    # === 字段标准化 ===
    df = df.rename(columns={
        "日期": "date",
        "最高": "high",
        "最低": "low",
        "收盘": "close"
    })

    df["date"] = pd.to_datetime(df["date"])

    # === 计算 SuperTrend ===
    df = supertrend(df)

    # === 输出买点 ===
    buy_df = df[df["buy_signal"]]

    print("\n✅ SuperTrend 买点：")
    print(buy_df[["date", "close"]].tail(10))

    print(f"\n📊 共发现买点 {len(buy_df)} 个")


if __name__ == "__main__":
    main()
