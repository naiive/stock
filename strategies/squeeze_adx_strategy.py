# -*- coding: utf-8 -*-

import pandas as pd

from indicators.adx_di_indicator import adx_di_indicator
from indicators.atr_indicator import atr_indicator
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(df, symbol):
    """
    A股全市场扫描策略
    策略：
         涨幅 => 正的
         EMA200 => 价格站在均线上
         ADX => 大于25
         SQZMOM挤压释放 => 至少6天挤压，当天释放，且是亮绿动能柱
         ATR Stop Loss

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
        ema200_series = df['close'].rolling(200).mean()
        if current_close <= ema200_series.iloc[-1]:
            return None

        # 3.3 ADX > 25
        df = adx_di_indicator(df, length=14, threshold=25)
        last_adx = df.iloc[-1]
        adx_val = last_adx.get('adx')
        if pd.isna(adx_val) or adx_val <= 20:
            return None

        # 3.4 计算动能指标 (SQZ)
        df = squeeze_momentum_indicator(df, lengthKC=20, multKC=1.5, useTrueRange=True)
        last = df.iloc[-1]
        prev = df.iloc[-2]
        prev_sqz_id = pd.to_numeric(prev['sqz_id'], errors='coerce')

        # 3.4.1 SQZ信号定义
        sqz_status = last.get('sqz_status')
        prev_status = prev.get('sqz_status')
        sqz_hcolor = last.get('sqz_hcolor', '')

        # 3.4.2. SQZ逻辑判断
        signal = "无"
        if sqz_status == 'OFF' and prev_status == 'ON' and prev_sqz_id >= 6:
            if sqz_hcolor == 'lime':
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
                "EMA200": round(ema200_series.iloc[-1], 2),
                "ADX": round(adx_val, 2),
                "建议止损价": round(last_atr.get('atr_long_stop'), 2)
            }

    except Exception as e:
        # 这里不需要打印，错误会抛给 _worker 的 try...except
        return None

    return None