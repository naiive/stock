# -*- coding: utf-8 -*-

import pandas as pd
from indicators.aroon_oscillator_indicator import aroon_oscillator_indicator
from indicators.atr_indicator import atr_indicator


def run_strategy(df, symbol):
    """
    A股全市场扫描策略
    策略：
         prev_aroon_osc <= -95 且 current_aroon_osc > pre_aroon_osc 抄底指标

    :param df: DataFrame
    :param symbol: 股票代码
    :return: 命中则返回字典，未命中返回 None
    """
    try:
        # 1. 基础数据量检查 (确保能支撑滚动计算)
        if df is None or len(df) < 220:
            return None

        df = aroon_oscillator_indicator(df)
        current = df.iloc[-1]
        prev = df.iloc[-1]

        # aroon_osc 值
        current_aroon_osc = pd.to_numeric(current.get('aroon_osc'), errors='coerce')
        prev_aroon_osc = pd.to_numeric(prev.get('aroon_osc'), errors='coerce')

        # 输出结果
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        signal = None
        if prev_aroon_osc <= -95 and current_aroon_osc > prev_aroon_osc:
            signal = "buy"

        # 返回结果
        if signal == "buy":
            # 4. 只有信号触发，才计算 ATR 止损
            df = atr_indicator(df, length=14, multiplier=1.5)
            last_atr = df.iloc[-1]
            trade_date = str(current.get('date'))
            return {
                "日期": trade_date,
                "代码": symbol,
                "当前价": round(current_close, 2),
                "涨幅(%)": round(pct_chg, 2),
                "当前AROON": round(current_aroon_osc, 2),
                "上个AROON": round(prev_aroon_osc, 2),
                "建议止损价": round(last_atr.get('atr_long_stop'), 2)
            }

    except Exception as e:
        return None

    return None