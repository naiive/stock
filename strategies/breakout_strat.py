# -*- coding: utf-8 -*-
import pandas as pd
from conf.config import INDICATOR_CONFIG


class SqueezeBreakoutStrategy:
    """
    Squeeze Momentum 突破策略逻辑
    """

    def __init__(self):
        self.conf = INDICATOR_CONFIG["SQZ"]

    def check(self, df: pd.DataFrame) -> dict:
        """
        输入计算好指标的 DF，输出信号字典或 None
        """
        if df is None or len(df) < 5:
            return None

        last_row = df.iloc[-1]  # 今日数据
        prev_row = df.iloc[-2]  # 昨日数据

        # --- 核心信号逻辑 ---
        # 1. 挤压释放：昨日是 ON (挤压)，今日变 OFF (释放)
        is_release = (prev_row['sqz_status'] == 'ON') and (last_row['sqz_status'] == 'OFF')

        # 2. 动能方向：动能柱由负转正，或者在正值区间继续走强
        momentum_up = (last_row['sqz_hvalue'] > 0) and (last_row['sqz_hvalue'] > prev_row['sqz_hvalue'])

        # 3. 辅助过滤：收盘价在 MA200 之上 (长趋势向上)
        ma200 = df['close'].rolling(200).mean().iloc[-1]
        trend_ok = last_row['close'] > ma200

        if is_release and momentum_up and trend_ok:
            return {
                "signal": "🔥 Squeeze Release",
                "price": round(last_row['close'], 2),
                "sqz_id": prev_row['sqz_id'],  # 记录之前憋了多久
                "hvalue": round(last_row['sqz_hvalue'], 4)
            }

        return None