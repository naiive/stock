# -*- coding: utf-8 -*-

import pandas as pd

from indicators.atr_indicator import atr_indicator
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator
from indicators.support_resistance_breaks_indicator import support_resistance_breaks_indicator

def run_strategy(df, symbol):
    """
    A股全市场扫描策略

    策略结构：
    1. 当日涨幅 > 0
    2. 价格站上 EMA200
    3. 突破前高（SRB）
    4. SQZ 挤压 ≥ 6 天，ON → OFF
    5. 第一根亮绿动能柱
    6. SQZ 释放质量评分
    7. ATR 动态止损
    """
    try:
        # ==================================================
        # 1️⃣ 数据长度检查
        # ==================================================
        if df is None or len(df) < 220:
            return None

        # ==================================================
        # 2️⃣ 当日涨幅 > 0
        # ==================================================
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        if pct_chg <= 0:
            return None

        # ==================================================
        # 3️⃣ EMA200 趋势过滤
        # ==================================================
        ema200_series = df['close'].rolling(200).mean()
        ema200_val = ema200_series.iloc[-1]

        if current_close <= ema200_val:
            return None

        # ==================================================
        # 4️⃣ 前高突破（SRB）
        # ==================================================
        df = support_resistance_breaks_indicator(df)

        last_srb = df.iloc[-1]
        srb_resistance = pd.to_numeric(
            last_srb.get('srb_resistance'),
            errors='coerce'
        )

        if pd.isna(srb_resistance) or current_close < srb_resistance:
            return None

        # ==================================================
        # 5️⃣ SQZMOM 指标
        # ==================================================
        df = squeeze_momentum_indicator(df)

        last = df.iloc[-1]
        prev = df.iloc[-2]

        prev_sqz_id = pd.to_numeric(prev.get('sqz_id'), errors='coerce')
        sqz_status = last.get('sqz_status')
        prev_status = prev.get('sqz_status')
        sqz_hcolor = last.get('sqz_hcolor')

        # ==================================================
        # 6️⃣ SQZ 释放条件
        # ==================================================
        if not (
            sqz_status == 'OFF'
            and prev_status == 'ON'
            and prev_sqz_id >= 6
            and sqz_hcolor == 'lime'
        ):
            return None

        # ==================================================
        # 7️⃣ 突破趋势（三天）
        # 倒数第4、3、2天 vs 前高
        # ==================================================
        close_D1 = df['close'].iloc[-4]
        close_D2 = df['close'].iloc[-3]
        close_D3 = df['close'].iloc[-2]

        status_D1 = 0 if close_D1 > srb_resistance else 1
        status_D2 = 0 if close_D2 > srb_resistance else 1
        status_D3 = 0 if close_D3 > srb_resistance else 1

        trend_D1 = "高" if status_D1 == 0 else "低"
        trend_D2 = "高" if status_D2 == 0 else "低"
        trend_D3 = "高" if status_D3 == 0 else "低"

        break_trend = f"{trend_D1}-{trend_D2}-{trend_D3}"
        break_score = status_D1 + status_D2 + status_D3

        # ==================================================
        # 8️⃣ 信号前 6 日 SQZ 颜色
        # ==================================================
        raw_colors = (
            df['sqz_hcolor']
            .iloc[-7:-1]
            .tolist()[::-1]
        )

        color_cols = {}
        for i in range(6):
            raw = raw_colors[i] if i < len(raw_colors) else None
            color_cols[f"前{i+1}日"] = COLOR_MAP.get(raw, '未知')

        # ==================================================
        # 9️⃣ SQZ 释放评分（越靠近红色权重越高）
        # ==================================================
        sqz_score = 0.0
        for i in range(6):
            color_name = color_cols.get(f"前{i+1}日")
            weight = 6 - i
            sqz_score += weight * COLOR_SCORE.get(color_name, 0)

        sqz_score = round(sqz_score, 2)

        # ==================================================
        # 🔟 ATR 止损
        # ==================================================
        df = atr_indicator(df)
        last_atr = df.iloc[-1]

        # ==================================================
        # 11️⃣ 返回结果
        # ==================================================
        return {
            "日期": str(last.get('date')),
            "代码": symbol,
            "当前价": round(current_close, 2),
            "涨幅(%)": round(pct_chg, 2),
            "连续挤压个数": prev_sqz_id,
            # 突破结构
            "突破趋势": break_trend,
            "突破得分": break_score,
            # SQZ 颜色结构
            **color_cols,
            "SQZ释放评分": sqz_score,
            # 趋势与关键价
            "EMA200": round(ema200_val, 2),
            "前高": round(srb_resistance, 2),
            # 风控
            "建议止损价": round(last_atr.get('atr_long_stop'), 2)
        }

    except Exception:
        return None

# ==================================================
# SQZMOM 颜色映射（统一成四种）
# ==================================================
COLOR_MAP = {
    'lime': '绿|亮',     # 强多头动能
    'green': '绿|暗',    # 弱多头动能
    'red': '红|亮',      # 强空头动能
    'maroon': '红|暗'    # 弱空头动能
}

# ==================================================
# SQZ 颜色评分系数（红色权重大）
# ==================================================
COLOR_SCORE = {
    '红|暗': 1.0,
    '红|亮': 0.8,
    '绿|暗': 0.3,
    '绿|亮': 0.1
}