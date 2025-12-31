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
        is_ath = "是" if current_close >= historical_high else "否"

        # ==================================================
        # 6.计算SQZMOM指标
        # ==================================================
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
        # 8.信号前突破趋势（顺序：前6、5、4、3、2、1）
        # ==================================================
        break_trend_list = []
        for i in range(6, 0, -1):
            close_i = df['close'].iloc[-(i + 1)]
            trend_i = "高" if close_i > srb_resistance else "低"
            break_trend_list.append(trend_i)
        break_trend = "-".join(break_trend_list)

        # ==================================================
        # 9.信号前6天的SQZMOM根柱子颜色和值（顺序：前6、5、4、3、2、1）
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

            color_value_cols[f"前{6 - i}日"] = f"{color_str}[{value_str}]"

        # ==================================================
        # 10.返回结果
        # ==================================================
        return {
            "日期": current.name.strftime('%Y-%m-%d'),
            "代码": symbol,
            "现价": round(current_close, 2),
            "涨幅(%)": round(pct_chg, 2),
            "连续挤压天数": prev_sqz_id,
            # 前6天挤压状态
            **color_value_cols,
            "前6天突趋势": break_trend,
            "最近是否ATH": is_ath,
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