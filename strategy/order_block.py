#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from api.stock_query import stock_zh_a_daily_mysql

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# ============================================================
# Order Block Finder
# ============================================================
def order_blocks(df, periods=5, threshold=0.0, use_wicks=False):
    """
    完全模拟 Pine Script 逻辑的 Order Block
    Bullish OB: 最后一根下降 K 后 periods 根上涨 K
    Bearish OB: 最后一根上涨 K 后 periods 根下降 K
    """
    df = df.copy()
    df['OB'] = False
    df['OB_type'] = None
    df['OB_high'] = np.nan
    df['OB_low'] = np.nan
    df['OB_mid'] = np.nan

    n = len(df)
    for i in range(n - periods - 1):
        # OB candle
        ob_candle = df.iloc[i]
        # 随后的 periods 根 K
        future_candles = df.iloc[i+1:i+1+periods]

        # Bullish OB
        if ob_candle['close'] < ob_candle['open']:
            if (future_candles['close'] > future_candles['open']).all():
                move = abs(future_candles['close'].iloc[-1] - ob_candle['close']) / ob_candle['close'] * 100
                if move >= threshold:
                    df.loc[df.index[i], 'OB'] = True
                    df.loc[df.index[i], 'OB_type'] = 'Bullish'
                    df.loc[df.index[i], 'OB_high'] = ob_candle['high'] if use_wicks else ob_candle['open']
                    df.loc[df.index[i], 'OB_low'] = ob_candle['low']
                    df.loc[df.index[i], 'OB_mid'] = (df.loc[df.index[i], 'OB_high'] + df.loc[df.index[i], 'OB_low']) / 2

        # Bearish OB
        if ob_candle['close'] > ob_candle['open']:
            if (future_candles['close'] < future_candles['open']).all():
                move = abs(future_candles['close'].iloc[-1] - ob_candle['close']) / ob_candle['close'] * 100
                if move >= threshold:
                    df.loc[df.index[i], 'OB'] = True
                    df.loc[df.index[i], 'OB_type'] = 'Bearish'
                    df.loc[df.index[i], 'OB_high'] = ob_candle['high']
                    df.loc[df.index[i], 'OB_low'] = ob_candle['low'] if use_wicks else ob_candle['open']
                    df.loc[df.index[i], 'OB_mid'] = (df.loc[df.index[i], 'OB_high'] + df.loc[df.index[i], 'OB_low']) / 2

    return df

# ============================================================
# OB 信号（收盘确认）
# ============================================================
def ob_signal(df):
    """
    返回最近一根 K 的 OB 信号（收盘确认）
    """
    df = df.copy()
    today = df.iloc[-1]
    signal = None

    # 最近一个 OB
    ob_df = df[df['OB']]
    if ob_df.empty:
        return None

    ob = ob_df.iloc[-1]

    # Bullish OB 信号
    if ob['OB_type'] == 'Bullish':
        touch = (today['low'] <= ob['OB_high']) and (today['high'] >= ob['OB_low'])
        reclaim = today['close'] > ob['OB_mid']
        if touch and reclaim:
            signal = {
                "date": today['date'],
                "type": "BULLISH_OB_BUY",
                "price": round(today['close'], 2),
                "OB_high": round(ob['OB_high'], 2),
                "OB_low": round(ob['OB_low'], 2),
                "OB_mid": round(ob['OB_mid'], 2),
            }

    # Bearish OB 信号
    if ob['OB_type'] == 'Bearish':
        touch = (today['high'] >= ob['OB_low']) and (today['low'] <= ob['OB_high'])
        reject = today['close'] < ob['OB_mid']
        if touch and reject:
            signal = {
                "date": today['date'],
                "type": "BEARISH_OB_SELL",
                "price": round(today['close'], 2),
                "OB_high": round(ob['OB_high'], 2),
                "OB_low": round(ob['OB_low'], 2),
                "OB_mid": round(ob['OB_mid'], 2),
            }

    return signal

# ============================================================
# 主程序
# ============================================================
def main():
    df = stock_zh_a_daily_mysql(
        symbol='sh600588',
        start_date='20240101',
        end_date='20251231',
        adjust='qfq'
    )

    if df is None or df.empty:
        print("数据为空")
        return

    df = order_blocks(
        df,
        periods=5,
        threshold=0.0,
        use_wicks=False
    )

    signal = ob_signal(df)

    if signal:
        print("✅ Order Block 信号（收盘确认）")
        print(pd.DataFrame([signal]))
    else:
        print("❌ 今日无 Order Block 信号")

    print("\n最近 Order Blocks：")
    print(
        df[df['OB']]
        [['date', 'OB_type', 'OB_high', 'OB_low', 'OB_mid']]
        .tail(5)
    )

if __name__ == "__main__":
    main()
