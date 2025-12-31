# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator
from indicators.support_resistance_breaks_indicator import support_resistance_breaks_indicator

def run_strategy(df, symbol):
    """
    1. 当日涨幅 > 0
    2. 价格站上 EMA200
    3. 突破前高
    4. SQZ 挤压 ≥ 6 天，ON → OFF
    5. 第一根亮绿动能柱
    """
    try:
        # ==================================================
        # 1.数据长度检查
        # ==================================================
        if df is None or len(df) < 220:
            return None

        # ==================================================
        # 2.当日涨幅 > 0
        # ==================================================
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100
        if pct_chg <= 0:
            return None

        # ==================================================
        # 3.EMA200 趋势过滤
        # ==================================================
        ema200_series = df['close'].rolling(200).mean()
        ema200_val = ema200_series.iloc[-1]
        if current_close <= ema200_val:
            return None

        # ==================================================
        # 4.前高突破
        # ==================================================
        df = support_resistance_breaks_indicator(df)
        last_srb = df.iloc[-1]
        srb_resistance = pd.to_numeric(last_srb.get('srb_resistance'), errors='coerce')
        if pd.isna(srb_resistance) or current_close < srb_resistance:
            return None

        # ==================================================
        # 5.历史最高价
        # ==================================================
        historical_high = df['high'].max()
        is_ath = current_close >= historical_high

        # # ==================================================
        # # 6.计算SQZMOM指标
        # # ==================================================
        df = squeeze_momentum_indicator(df)
        current = df.iloc[-1]
        prev = df.iloc[-2]

        prev_sqz_id = pd.to_numeric(prev.get('sqz_id'), errors='coerce')
        sqz_status = current.get('sqz_status')
        prev_status = prev.get('sqz_status')
        sqz_hcolor = current.get('sqz_hcolor')

        # ==================================================
        # 7.SQZMOM释放条件
        # ==================================================
        if not (
            sqz_status == 'OFF'
            and prev_status == 'ON'
            and prev_sqz_id >= 6
            and sqz_hcolor == 'lime'
        ):
            return None

        # ==================================================
        # 8.突破趋势（前3天）
        # 倒数第4、3、2天 vs 前高
        # ==================================================
        close_D1 = df['close'].iloc[-4]
        close_D2 = df['close'].iloc[-3]
        close_D3 = df['close'].iloc[-2]

        trend_D1 = "高" if close_D1 > srb_resistance else "低"
        trend_D2 = "高" if close_D2 > srb_resistance else "低"
        trend_D3 = "高" if close_D3 > srb_resistance else "低"
        break_trend = f"{trend_D1}-{trend_D2}-{trend_D3}"

        # ==================================================
        # 9.信号前6天的SQZMOM根柱子颜色和绝对值
        # ==================================================
        raw_colors = df['sqz_hcolor'].iloc[-7:-1].tolist()[::-1]
        raw_values = df['sqz_hvalue'].iloc[-7:-1].tolist()[::-1]

        color_value_cols = {}
        for i in range(6):
            raw_color = raw_colors[i] if i < len(raw_colors) else None
            raw_value = raw_values[i] if i < len(raw_values) else None

            color_str = COLOR_MAP.get(raw_color, 'NA')

            if raw_value is None or np.isnan(raw_value):
                value_str = "NA"
            else:
                if raw_value > 0:
                    value_str = f"+{raw_value:.2f}"
                elif raw_value < 0:
                    value_str = f"-{abs(raw_value):.2f}"
                else:
                    value_str = f"{raw_value:.2f}"

            color_value_cols[f"前{i + 1}日"] = f"{color_str}[{value_str}]"

        # ==================================================
        # 10.返回结果
        # ==================================================
        return {
            "日期": current.name.strftime('%Y-%m-%d'),
            "代码": symbol,
            "当前价": round(current_close, 2),
            "涨幅(%)": round(pct_chg, 2),
            "最近ATH": is_ath,
            "连续挤压": prev_sqz_id,
            **color_value_cols,
            "前3天突破": break_trend
        }

    except Exception:
        return None

# ==================================================
# SQZMOM 颜色映射
# ==================================================
COLOR_MAP = {
    'lime': '绿亮',     # 强多头动能
    'green': '绿暗',    # 弱多头动能
    'red': '红亮',      # 强空头动能
    'maroon': '红暗'    # 弱空头动能
}