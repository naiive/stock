# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from core.map.squeeze_color_map import squeeze_color_map
from indicators.adx_di_indicator import adx_di_indicator
from indicators.atr_indicator import atr_indicator
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(df, symbol):
    """
    A股全市场扫描策略（含 SQZ 释放评分）

    核心思想：
    1. 大趋势向上（EMA200 + ADX）
    2. 波动被长期压缩（SQZ ON ≥ 6）
    3. 当天刚释放（ON → OFF）
    4. 第一根亮绿动能柱
    5. 用前 6 日柱状颜色给“释放质量”打分
    """
    try:
        # ==========================================================
        # 1️⃣ 基础数据长度检查
        # EMA200 / ADX / SQZ 都需要较长历史
        # ==========================================================
        if df is None or len(df) < 220:
            return None

        # ==========================================================
        # 2️⃣ 当日涨幅必须为正
        # 避免选到刚释放但当日被砸的标的
        # ==========================================================
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100
        if pct_chg <= 0:
            return None

        # ==========================================================
        # 3️⃣ 价格必须站在 EMA200 上
        # 保证只在长期上升趋势中找机会
        # ==========================================================
        ema200 = df['close'].rolling(200).mean().iloc[-1]
        if current_close <= ema200:
            return None

        # ==========================================================
        # 4️⃣ ADX 趋势强度过滤
        # ADX > 25 表示趋势明确
        # ==========================================================
        df = adx_di_indicator(df)
        adx_val = df.iloc[-1].get('adx')
        if pd.isna(adx_val) or adx_val <= 25:
            return None

        # ==========================================================
        # 5️⃣ 计算 SQZMOM（挤压动能）
        # ==========================================================
        df = squeeze_momentum_indicator(df)
        current = df.iloc[-1]    # 今天
        prev = df.iloc[-2]       # 昨天

        sqz_status = current.get('sqz_status')        # ON / OFF
        prev_status = prev.get('sqz_status')
        sqz_hcolor = current.get('sqz_hcolor')        # 动能柱颜色
        prev_sqz_id = pd.to_numeric(prev.get('sqz_id'), errors='coerce')

        # ==========================================================
        # 6️⃣ SQZ 释放信号定义
        # 条件解释：
        # ① 昨天还在挤压（ON）
        # ② 今天刚释放（OFF）
        # ③ 挤压持续 ≥ 6 天
        # ④ 第一根亮绿柱
        # ==========================================================
        if not (
            sqz_status == 'OFF' and
            prev_status == 'ON' and
            prev_sqz_id >= 6 and
            sqz_hcolor == 'lime'
        ):
            return None

        # ==================================================
        # 7️⃣ 信号前6天的SQZMOM根柱子颜色和值（顺序：前6、5、4、3、2、1）
        # ==================================================
        raw_colors = df['sqz_hcolor'].iloc[-7:-1].tolist()[::-1]
        raw_values = df['sqz_hvalue'].iloc[-7:-1].tolist()[::-1]

        color_value_list = []
        green_count = 0
        for i in range(6):
            raw_color = raw_colors[i] if i < len(raw_colors) else None
            raw_value = raw_values[i] if i < len(raw_values) else None
            color_str = squeeze_color_map.get(raw_color, 'NA')
            # ✅ 统计绿色
            if color_str is not None and "绿" in color_str:
                green_count += 1
            if raw_value is None or np.isnan(raw_value):
                value_str = "NA"
            else:
                if raw_value > 0:
                    value_str = f"+{raw_value:.2f}"
                elif raw_value < 0:
                    value_str = f"-{abs(raw_value):.2f}"
                else:
                    value_str = f"{raw_value:.2f}"

            color_value_list.append(f"{color_str}[{value_str}]")
        color_value_cols = "-".join(color_value_list)

        # ==========================================================
        # 8️⃣ ATR 止损计算
        # ==========================================================
        df = atr_indicator(df)
        last_atr = df.iloc[-1]

        # ==========================================================
        # 9️⃣ 返回扫描结果
        # ==========================================================
        historical_high = df['high'].max()
        is_ath = "是" if current_close >= historical_high else "否"
        return {
            "日期": current.name.strftime('%Y-%m-%d'),
            "代码": symbol,
            "现价": round(current_close, 2),
            "涨幅(%)": round(pct_chg, 2),
            "连续挤压天数": prev_sqz_id,
            "前6天绿色数": green_count,
            # 前6日柱状颜色和值
            "前6天柱状图": color_value_cols,
            # ATR 动态止损
            "建议止损价": round(last_atr.get('atr_long_stop'), 2),
            "最近是否ATH": is_ath
        }

    except Exception:
        # 扫描场景下，单票异常直接跳过
        return None