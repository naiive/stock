# -*- coding: utf-8 -*-

import pandas as pd
from indicators.atr_indicator import atr_indicator
from indicators.wavetrend_with_crosses_indicator import wavetrend_with_crosses_indicator
from indicators.ma_ribbon_indicator import ma_ribbon_indicator

def run_strategy(df, symbol):
    """
    策略：cross 为 green 且在 超卖区 wtc_red < -45
    """
    try:
        # 1. 基础数据量检查 (确保能支撑滚动计算)
        if df is None or len(df) < 220:
            return None

        # 2. 价格与均线前置过滤 (快速剪枝，提升扫描速度)
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        # 3.1 涨幅大于等于0
        if pct_chg <= 0:
            return None

        # 3.2 价格在ema200上
        df = ma_ribbon_indicator(df, length=200)
        ema200 = float(df['ema_200'].iloc[-1])
        if current_close < ema200:
            return None

        # 3.4 crosses
        df = wavetrend_with_crosses_indicator(df)
        current = df.iloc[-1]
        wtc_green = pd.to_numeric(current.get('wtc_green'), errors='coerce')
        wtc_red = pd.to_numeric(current.get('wtc_red'), errors='coerce')
        wtc_signal = current.get('wtc_signal')

        # 金叉
        if wtc_signal == 'green' and wtc_red < -45:
            return {
                "日期": current.name.strftime('%Y-%m-%d'),
                "代码": symbol,
                "现价": round(current_close, 2),
                "涨幅(%)": round(pct_chg, 2),
                "EMA200": round(ema200, 2),
                "绿线": round(wtc_green, 2),
                "红线": round(wtc_red, 2)
            }

    except Exception:
        return None

    return None