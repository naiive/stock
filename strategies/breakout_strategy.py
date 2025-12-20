# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator


def run_breakout_strategy(df, symbol):
    """
    核心策略逻辑：突破趋势 + SQZ动能 + 得分系统
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

        # 计算 MA200
        ma200 = df['close'].rolling(200).mean().iloc[-1]

        # 过滤掉：不在 MA200 之上、或者今日收跌的票
        if not (current_close > ma200 and pct_chg > 0):
            return None

        # 3. 计算 Pivot High (前高阻力位)
        # 定义：左 15 天和右 15 天内的最高点
        left, right = 15, 15
        highs = df['high'].values
        pivots = np.full(len(highs), np.nan)
        for i in range(left, len(highs) - right):
            if highs[i] == np.max(highs[i - left: i + right + 1]):
                pivots[i] = highs[i]

        # 填充阻力位，获取最新阻力位价格
        last_pivot = pd.Series(pivots).ffill().iloc[-1]
        if pd.isna(last_pivot): return None

        # 4. 计算突破强度与趋势
        # 核心逻辑：当前价必须站上阻力位
        if current_close <= last_pivot:
            return None

        break_strength = (current_close - last_pivot) / last_pivot * 100

        # --- [新增] 突破趋势记录 (过去3天的状态) ---
        # 规则：收盘价 > 阻力位 -> "高"；否则 "低"
        past_closes = df['close'].iloc[-4:-1].values  # 倒数第4, 3, 2天
        trend_list = ["高" if c > last_pivot else "低" for c in past_closes]
        break_trend = "-".join(trend_list)

        # --- [新增] 得分系统 ---
        # 过去3天中，处于“低”位的次数越多，说明今日突破越有爆发力
        score = sum([1 for t in trend_list if t == "低"])

        # 5. 计算动能指标 (SQZ)
        df = squeeze_momentum_indicator(df)
        last = df.iloc[-1]
        prev = df.iloc[-2]

        # 信号定义：挤压释放 (OFF) 且 动能向上 (强多)
        signal = "无"
        if last['sqz_status'] == 'OFF' and prev['sqz_status'] == 'ON':
            if last.get('val_color') == '强多':
                signal = "买入"

        # 6. 返回结果 (仅返回 signal == "买入" 的，或者你可以根据得分返回)
        if signal == "买入":
            return {
                "代码": symbol,
                "得分": score,
                "突破趋势": break_trend,
                "当前价": round(current_close, 2),
                "涨幅%": round(pct_chg, 2),
                "MA200": round(ma200, 2),
                "前阻力位": round(float(last_pivot), 2),
                "突破力度%": round(break_strength, 2),
                "信号": signal
            }

    except Exception as e:
        # 这里不需要打印，错误会抛给 _worker 的 try...except
        return None

    return None