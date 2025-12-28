# -*- coding: utf-8 -*-

import pandas as pd
from indicators.atr_indicator import atr_indicator
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator
from indicators.adx_di_indicator import adx_di_indicator
from indicators.support_resistance_breaks_indicator import support_resistance_breaks_indicator

# ==================================================
# SQZMOM 颜色映射
# ==================================================
COLOR_MAP = {
    'lime': '绿|亮',    # 强多头动能
    'green': '绿|暗',   # 弱多头动能
    'red': '红|亮',     # 弱空头动能
    'maroon': '红|暗'   # 强空头动能
}

# SQZMOM 颜色评分系数（红色权重大，靠近信号日权重大）
COLOR_SCORE = {
    '红|暗': 1.0,
    '红|亮': 0.8,
    '绿|暗': 0.3,
    '绿|亮': 0.1
}

def run_strategy(df, symbol, adx_length=14, adx_threshold=20):
    """
    综合扫描策略：
    - SQZMOM 挤压释放
    - ADX 趋势确认（参数可配置）
    - 前高突破（SRB）
    - ATR 动态止损
    每个指标独立计算命中状态
    返回字典包含命中状态、评分及相关信息
    """

    try:
        # -----------------------------
        # 1️⃣ 基础数据检查
        # -----------------------------
        if df is None or len(df) < 220:
            return None  # 数据不足，无法计算指标

        # -----------------------------
        # 2️⃣ 当日价格及涨幅
        # -----------------------------
        current_close = float(df['close'].iloc[-1])  # 当前收盘价
        prev_close = float(df['close'].iloc[-2])     # 前日收盘价
        pct_chg = float(round((current_close - prev_close) / prev_close * 100, 2))  # 当日涨幅 %

        # 初始化返回字典
        result = {
            "日期": str(df['date'].iloc[-1]),   # 信号日期
            "代码": symbol,                      # 股票代码
            "当前价": current_close,             # 当日收盘价
            "涨幅(%)": pct_chg                   # 当日涨幅 %
        }

        # -----------------------------
        # 3️⃣ SQZMOM 挤压释放
        # -----------------------------
        df = squeeze_momentum_indicator(df, lengthKC=20, multKC=1.5, useTrueRange=True)
        last = df.iloc[-1]  # 当天数据
        prev = df.iloc[-2]  # 前一天数据

        # 前一天的连续挤压天数
        prev_sqz_id = int(pd.to_numeric(prev.get('sqz_id'), errors='coerce') or 0)
        sqz_status = last.get('sqz_status')       # 当天 SQZ 状态
        prev_status = prev.get('sqz_status')      # 昨天 SQZ 状态
        sqz_hcolor = last.get('sqz_hcolor')       # 当天动能柱颜色

        # ************* SQZ命中作为大前提 *************
        # ************* SQZ命中作为大前提 *************
        # SQZ 命中条件：OFF & 前一天 ON & 挤压 ≥ 6 天 & 当天亮绿
        if not (sqz_status == 'OFF' and prev_status == 'ON' and prev_sqz_id >= 6 and sqz_hcolor == 'lime'):
            return None  # 不满足大前提，直接结束策略

        # 信号前6日柱状颜色
        raw_colors = df['sqz_hcolor'].iloc[-7:-1].tolist()[::-1]  # 不含当天，倒序排列
        color_cols = {}
        for i in range(6):
            raw = raw_colors[i] if i < len(raw_colors) else None
            color_cols[f"前{i+1}日"] = COLOR_MAP.get(raw, '未知')
        result.update(color_cols)  # 合并到结果字典

        # SQZ释放评分（红色权重大，越靠近信号日分值越高）
        score = 0.0
        for i in range(6):
            color_name = color_cols.get(f"前{i+1}日")
            weight = 6 - i
            color_factor = COLOR_SCORE.get(color_name, 0)
            score += weight * color_factor
        result["SQZ释放评分"] = float(round(score,2))

        # -----------------------------
        # 4️⃣ ADX 趋势确认
        # -----------------------------
        df = adx_di_indicator(df, length=adx_length)
        adx_val = float(df['adx'].iloc[-1])
        plus_di = float(df['adx_plus'].iloc[-1])
        minus_di = float(df['adx_minus'].iloc[-1])
        adx_hit = adx_val >= adx_threshold and plus_di > minus_di  # 命中条件

        if adx_hit:
            result["adx命中"] = {
                "adx值": adx_val,  # ADX 强度
                "+DI": plus_di,    # 多方趋势指标
                "-DI": minus_di    # 空方趋势指标
            }

        # -----------------------------
        # 5️⃣ 前高突破（SRB）
        # -----------------------------
        df = support_resistance_breaks_indicator(df, left_bars=15, right_bars=15, volume_thresh=20.0)
        last_srb = df.iloc[-1]
        srb_resistance = float(pd.to_numeric(last_srb.get('srb_resistance'), errors='coerce') or 0)
        resistance_hit = current_close > srb_resistance if srb_resistance > 0 else False

        if resistance_hit:
            ema200_val = float(df['close'].rolling(200).mean().iloc[-1])  # EMA200 均线
            close_D1 = float(df['close'].iloc[-4])
            close_D2 = float(df['close'].iloc[-3])
            close_D3 = float(df['close'].iloc[-2])
            # 过去三天与前高比较，高为0，低为1
            status_D1 = 0 if close_D1 > srb_resistance else 1
            status_D2 = 0 if close_D2 > srb_resistance else 1
            status_D3 = 0 if close_D3 > srb_resistance else 1
            trend_D1 = "高" if status_D1 == 0 else "低"
            trend_D2 = "高" if status_D2 == 0 else "低"
            trend_D3 = "高" if status_D3 == 0 else "低"
            break_trend = f"{trend_D1}-{trend_D2}-{trend_D3}"     # 趋势字符串
            break_score = int(status_D1 + status_D2 + status_D3)  # 趋势得分

            result["resistance命中"] = {
                "突破趋势": break_trend,
                "突破得分": break_score,
                "EMA200": ema200_val,
                "前高": srb_resistance
            }

        # -----------------------------
        # 6️⃣ ATR 止损
        # -----------------------------
        df = atr_indicator(df, length=14, multiplier=1.5)
        last_atr = df.iloc[-1]
        atr_stop = float(round(last_atr.get('atr_long_stop'),2))
        result["建议止损价"] = atr_stop

        return result

    except Exception:
        return None
