# -*- coding: utf-8 -*-

from indicators.atr_indicator import atr_indicator
from indicators.fair_value_gap_indicator import fair_value_gap_indicator

def run_strategy(df, symbol):
    """
    A股全市场扫描策略
    策略：
         fvg_type : bull

    :param df: DataFrame
    :param symbol: 股票代码
    :return: 命中则返回字典，未命中返回 None
    """
    try:

        df = fair_value_gap_indicator(df, threshold_pct=0.0, auto_threshold=False )
        last = df.iloc[-1]
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100
        fvg_type = last.get('fvg_type')

        if fvg_type != 'bull':
            return None

        df = atr_indicator(df, length=14, multiplier=1.5)
        last_atr = df.iloc[-1]
        trade_date = str(last.get('date'))

        return {
            "日期": trade_date,
            "代码": symbol,
            "当前价": round(current_close, 2),
            "涨幅(%)": round(pct_chg, 2),
            "建议止损价": round(last_atr.get('atr_long_stop'), 2),
            "FVG": fvg_type
        }
    except Exception as e:
        return None