# -*- coding: utf-8 -*-

import pandas as pd

from indicators.atr_indicator import atr_indicator
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(df, symbol):
    """
    A股全市场扫描策略
    策略：
         SQZMOM挤压释放 => 至少6天挤压，当天释放，且是亮绿动能柱
         突破前高

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

        # 3. 计算动能指标 (SQZ)
        df = squeeze_momentum_indicator(df, lengthKC=20, multKC=1.5, useTrueRange=True)
        last = df.iloc[-1]
        prev = df.iloc[-2]
        prev_sqz_id = pd.to_numeric(prev['sqz_id'], errors='coerce')

        # 4. SQZ信号定义
        sqz_status = last.get('sqz_status')
        prev_status = prev.get('sqz_status')
        sqz_hcolor = last.get('sqz_hcolor')

        # 5. SQZ逻辑判断
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

            # iloc[-7:-1] 表示从倒数第7个开始，到倒数第2个结束（不含倒数第1个）
            pre_signal_colors = df['sqz_hcolor'].iloc[-7:-1].tolist()
            color_parts = []
            for i in range(6):
                # pre_signal_colors 的最后一个元素是昨天
                color_val = pre_signal_colors[-(i + 1)]
                color_parts.append(f"前{i + 1}日:{color_val}")

            color_str = " | ".join(color_parts)

            return {
                "日期": trade_date,
                "代码": symbol,
                "当前价": round(current_close, 2),
                "涨幅(%)": round(pct_chg, 2),
                "信号前6日颜色": color_str,
                "建议止损价": round(last_atr.get('atr_long_stop'), 2)
            }

    except Exception as e:
        # 这里不需要打印，错误会抛给 _worker 的 try...except
        return None

    return None