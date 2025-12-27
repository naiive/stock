# -*- coding: utf-8 -*-

import pandas as pd

from indicators.atr_indicator import atr_indicator
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator
from indicators.support_resistance_breaks_indicator import support_resistance_breaks_indicator

def run_strategy(df, symbol):
    """
    A股全市场扫描策略
    策略：
         涨幅 => 正的
         EMA200 => 价格站在均线上
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

        # 3. 策略计算，一定要断点，就是先把简单的指标计算，不满足就跳过后面复杂的计算

        # 3.1 涨幅大于等于0
        if pct_chg <= 0:
            return None

        # 3.2 价格在ema200上
        ema200_series = df['close'].rolling(200).mean()
        if current_close <= ema200_series.iloc[-1]:
            return None

        # 3.3 破前高
        df = support_resistance_breaks_indicator(df, left_bars=15, right_bars=15, volume_thresh=20.0)
        last_srb = df.iloc[-1]
        srb_resistance = pd.to_numeric(last_srb.get('srb_resistance'), errors='coerce')
        if current_close < srb_resistance :
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

            # ===================================================
            # 🆕 增加 "突破趋势" 和 "得分" 的计算逻辑
            # ===================================================

            # 获取倒数第 4, 3, 2 天的收盘价
            close_D1 = df['close'].iloc[-4]  # 倒数第4天
            close_D2 = df['close'].iloc[-3]  # 倒数第3天
            close_D3 = df['close'].iloc[-2]  # 倒数第2天

            # 规则：收盘价 > 前阻力位价格 (srb_resistance) -> "高"；否则 "低"
            # 顺序：倒数第4天-倒数第3天-倒数第2天

            # 计算每个日期的突破状态 (0: 高, 1: 低)
            status_D1 = 0 if close_D1 > srb_resistance else 1
            status_D2 = 0 if close_D2 > srb_resistance else 1
            status_D3 = 0 if close_D3 > srb_resistance else 1

            # 构造 "突破趋势" 字符串
            trend_D1 = "高" if status_D1 == 0 else "低"
            trend_D2 = "高" if status_D2 == 0 else "低"
            trend_D3 = "高" if status_D3 == 0 else "低"

            break_trend = f"{trend_D1}-{trend_D2}-{trend_D3}"

            # 计算 "得分" (低为 1 分，高为 0 分)
            score = status_D1 + status_D2 + status_D3

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
                "得分": score,
                # 规则：收盘价 > 前阻力位价格 (srb_resistance) -> "高"；否则 "低"
                # 顺序：倒数第4天-倒数第3天-倒数第2天
                "突破趋势": break_trend,
                "信号前6日颜色": color_str,
                "EMA200": round(ema200_series.iloc[-1], 2),
                "前高": round(srb_resistance, 2),
                "建议止损价": round(last_atr.get('atr_long_stop'), 2)
            }

    except Exception as e:
        # 这里不需要打印，错误会抛给 _worker 的 try...except
        return None

    return None