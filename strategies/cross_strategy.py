# -*- coding: utf-8 -*-

import datetime
import pandas as pd
from indicators.atr_indicator import atr_indicator
from indicators.wavetrend_with_crosses_indicator import wavetrend_with_crosses_indicator
from indicators.ma_ribbon_indicator import ma_ribbon_indicator

def run_strategy(df, symbol):
    """
    A股全市场扫描策略
    策略：
         cross 为 green

    :param df: DataFrame
    :param symbol: 股票代码
    :return: 命中则返回字典，未命中返回 None
    """
    try:
        # 1. 基础数据量检查 (确保能支撑滚动计算)
        if df is None or len(df) < 220:
            return None

        # 2. 价格与均线前置过滤 (快速剪枝，提升扫描速度)
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        # 3. 策略计算，一定要断点，就是先把简单的指标计算，不满足就跳过后面复杂的计算

        # 3.1 涨幅大于等于0
        if pct_chg <= 0:
            return None

        # 3.2 价格在ema200上
        df = ma_ribbon_indicator(df, length=200)
        ema200 = float(df['ema_200'].iloc[-1])
        if current_close < ema200:
            return None

        # 3.4 crosses
        df = wavetrend_with_crosses_indicator(df, channelLength=10, averageLength=21, smaLength=4, obLevel1=60, obLevel2=53, osLevel1=-60, osLevel2=-53)
        last = df.iloc[-1]

        wtc_green = pd.to_numeric(last.get('wtc_green'), errors='coerce')
        wtc_red = pd.to_numeric(last.get('wtc_red'), errors='coerce')

        wtc_signal = last.get('wtc_signal')

        # 金叉
        signal = None
        if wtc_signal == 'green' and wtc_red < -53 and wtc_green < -53 :
            signal = "买入"

        # 返回结果
        if signal == "买入":
            # 4. 只有信号触发，才计算 ATR 止损
            df = atr_indicator(df, length=14, multiplier=1.5)
            last_atr = df.iloc[-1]
            trade_date = str(last.get('date'))
            return {
                "日期": trade_date,
                "代码": symbol,
                "当前价": round(current_close, 2),
                "涨幅(%)": round(pct_chg, 2),
                "EMA200": round(ema200, 2),
                "建议止损价": round(last_atr.get('atr_long_stop'), 2),
                "绿线": round(wtc_green, 2),
                "红线": round(wtc_red, 2),
            }

    except Exception as e:
        # 这里不需要打印，错误会抛给 _worker 的 try...except
        return None

    return None