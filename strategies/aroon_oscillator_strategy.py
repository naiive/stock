# -*- coding: utf-8 -*-

import pandas as pd
from indicators.aroon_oscillator_indicator import aroon_oscillator_indicator
from indicators.atr_indicator import atr_indicator

def run_strategy(df, symbol):
    """
    策略：aroon_osc = -100
    """
    try:
        if df is None or len(df) < 220:
            return None

        df = aroon_oscillator_indicator(df)
        current = df.iloc[-1]

        # aroon_osc 值
        aroon_osc = pd.to_numeric(current.get('aroon_osc'), errors='coerce')

        # 返回结果
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        if aroon_osc == -100:
            return {
                "日期": current.name.strftime('%Y-%m-%d'),
                "代码": symbol,
                "现价": round(current_close, 2),
                "涨幅(%)": round(pct_chg, 2),
                "AROON": round(aroon_osc, 2)
            }

    except Exception:
        return None

    return None