# -*- coding: utf-8 -*-

import pandas as pd
from indicators.macd_histogram_double_divergence_indicator import macd_histogram_double_divergence_indicator
from indicators.atr_indicator import atr_indicator


def run_strategy(df, symbol):
    """
    A股全市场扫描策略
    策略：
         macd histogram 双峰底背离

    :param df: DataFrame
    :param symbol: 股票代码
    :return: 命中则返回字典，未命中返回 None
    """
    try:
        # 1. 基础数据量检查 (确保能支撑滚动计算)
        if df is None or len(df) < 220:
            return None

        df = macd_histogram_double_divergence_indicator(df)
        current = df.iloc[-1]

        # macd_bull 值
        macd_bull = current.get('macd_bull')
        macd_l_date = current.get('macd_l_date')
        macd_r_date = current.get('macd_r_date')

        # 输出结果
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        signal = None
        if macd_bull == "Yes":
            signal = "buy"

        # 返回结果
        if signal == "buy":
            # 只有信号触发，才计算 ATR 止损
            df = atr_indicator(df, length=14, multiplier=1.5)
            last_atr = df.iloc[-1]
            return {
                "日期": str(current.get('date')),
                "代码": symbol,
                "当前价": round(current_close, 2),
                "涨幅(%)": round(pct_chg, 2),
                "左波峰日期": str(macd_l_date),
                "右波峰日期": str(macd_r_date),
                "建议止损价": round(last_atr.get('atr_long_stop'), 2)
            }

    except Exception as e:
        return None

    return None