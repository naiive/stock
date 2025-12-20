# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator


def run_breakout_strategy(df, symbol):
    """
    核心策略逻辑：SQZ动能
    :param df: 已经拼接好实时数据的完整 DataFrame
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

        # 3. 计算 Pivot High (前高阻力位)
        # 定义：左 15 天和右 15 天内的最高点
        left, right = 15, 15
        highs = df['high'].values
        pivots = np.full(len(highs), np.nan)
        for i in range(left, len(highs) - right):
            if highs[i] == np.max(highs[i - left: i + right + 1]):
                pivots[i] = highs[i]

        # 4. 计算动能指标 (SQZ)
        df = squeeze_momentum_indicator(df)
        last = df.iloc[-1]
        prev = df.iloc[-2]
        curr_sqz_id = pd.to_numeric(last['sqz_id'], errors='coerce')
        # 信号定义：挤压释放 (OFF) 且 动能向上 (强多)
        signal = "无"
        if last['sqz_status'] == 'OFF' and prev['sqz_status'] == 'ON' and curr_sqz_id >= 5 :
            if last.get('sqz_hcolor') == 'green':
                signal = "买入"

        # 6. 返回结果 (仅返回 signal == "买入" 的，或者你可以根据得分返回)
        if signal == "买入":
            return {
                "代码": symbol,
                "当前价": round(current_close, 2),
                "涨幅%": round(pct_chg, 2)
            }

    except Exception as e:
        # 这里不需要打印，错误会抛给 _worker 的 try...except
        return None

    return None