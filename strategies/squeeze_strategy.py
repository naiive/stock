# -*- coding: utf-8 -*-

import pandas as pd

from indicators.atr_indicator import atr_indicator
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

# ==================================================
# SQZMOM 颜色映射（统一成四种）
# ==================================================
COLOR_MAP = {
    'lime': '绿|亮',     # 强多头动能
    'green': '绿|暗',    # 弱多头动能
    'red': '红|亮',      # 弱空头动能
    'maroon': '红|暗'    # 强空头动能（深度压缩）
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

def run_strategy(df, symbol):
    """
    A股全市场扫描策略（SQZ 挤压释放 + 释放质量评分）

    策略逻辑：
    1. SQZ 至少挤压 6 天
    2. 当天从 ON → OFF
    3. 第一根亮绿动能柱
    4. 用前 6 日柱状颜色评估释放质量
    """
    try:
        # ==========================================================
        # 1️⃣ 数据长度检查
        # ==========================================================
        if df is None or len(df) < 220:
            return None

        # ==========================================================
        # 2️⃣ 当日涨幅（仅用于展示，不作为过滤）
        # ==========================================================
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100

        # ==========================================================
        # 3️⃣ 计算 SQZMOM 指标
        # ==========================================================
        df = squeeze_momentum_indicator(
            df,
            lengthKC=20,
            multKC=1.5,
            useTrueRange=True
        )

        last = df.iloc[-1]     # 今天
        prev = df.iloc[-2]     # 昨天

        prev_sqz_id = pd.to_numeric(
            prev.get('sqz_id'),
            errors='coerce'
        )

        sqz_status = last.get('sqz_status')
        prev_status = prev.get('sqz_status')
        sqz_hcolor = last.get('sqz_hcolor')

        # ==========================================================
        # 4️⃣ SQZ 释放信号定义
        # ON → OFF + 挤压 ≥ 6 天 + 第一根亮绿
        # ==========================================================
        if not (
            sqz_status == 'OFF'
            and prev_status == 'ON'
            and prev_sqz_id >= 6
            and sqz_hcolor == 'lime'
        ):
            return None

        # ==========================================================
        # 5️⃣ 提取信号前 6 日柱状颜色
        # 前1日 = 昨天
        # ==========================================================
        raw_colors = (
            df['sqz_hcolor']
            .iloc[-7:-1]      # 不包含信号当天
            .tolist()[::-1]   # 反转顺序
        )

        color_cols = {}
        for i in range(6):
            raw = raw_colors[i] if i < len(raw_colors) else None
            color_cols[f"前{i+1}日"] = COLOR_MAP.get(raw, '未知')

        # ==========================================================
        # 6️⃣ SQZ 释放评分
        # 越靠近信号日，红色权重越高
        # ==========================================================
        score = 0.0
        for i in range(6):
            color_name = color_cols.get(f"前{i+1}日")

            weight = 6 - i          # 前1日权重 6
            color_factor = COLOR_SCORE.get(color_name, 0)

            score += weight * color_factor

        score = round(score, 2)

        # ==========================================================
        # 7️⃣ ATR 止损（只有命中信号才算，节省性能）
        # ==========================================================
        df = atr_indicator(df, length=14, multiplier=1.5)
        last_atr = df.iloc[-1]

        # ==========================================================
        # 8️⃣ 返回结果
        # ==========================================================
        return {
            "日期": str(last.get('date')),
            "代码": symbol,
            "当前价": round(current_close, 2),
            "涨幅(%)": round(pct_chg, 2),
            # 信号前 6 日动能柱颜色
            **color_cols,
            # SQZ 释放质量评分
            "SQZ释放评分": score,
            # ATR 动态止损
            "建议止损价": round(last_atr.get('atr_long_stop'), 2)
        }

    except Exception:
        # 扫描场景下，单票异常直接跳过
        return None
