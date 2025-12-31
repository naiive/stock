#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
Fair Value Gap Indicator (LuxAlgo logic, Quant version)
============================================================
"""

import numpy as np
import pandas as pd

def fair_value_gap_indicator(
    df: pd.DataFrame,
    threshold_pct: float = 0.0,
    auto_threshold: bool = False,
) -> pd.DataFrame:
    """
    Fair Value Gap (FVG) 指标（LuxAlgo 核心逻辑还原）

    参数：
        df : DataFrame
            必须包含 high / low / close
        threshold_pct : float
            FVG 最小缺口百分比（0~100）
        auto_threshold : bool
            是否使用自动阈值（Pine 的 auto）

    返回字段：
        fvg_type        : bull / bear / None
        fvg_high        : FVG 上沿
        fvg_low         : FVG 下沿
        fvg_gap_pct     : 缺口比例
        fvg_mitigated   : 是否已被回补
    """

    df = df.copy()

    # 设置日期列为索引列
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame 没有 'date' 列，也没有日期索引，无法设置索引")

    # -------------------------
    # Threshold 计算
    # -------------------------
    if auto_threshold:
        # Pine: ta.cum((high - low) / low) / bar_index
        threshold_series = ((df["high"] - df["low"]) / df["low"]).cumsum() / (
            np.arange(len(df)) + 1
        )
    else:
        threshold_series = threshold_pct / 100.0

    # -------------------------
    # 初始化字段
    # -------------------------
    df["fvg_type"] = None
    df["fvg_high"] = np.nan
    df["fvg_low"] = np.nan
    df["fvg_gap_pct"] = np.nan
    df["fvg_mitigated"] = False

    # -------------------------
    # FVG 检测
    # -------------------------
    for i in range(2, len(df)):
        high_2 = df["high"].iloc[i - 2]
        low_2 = df["low"].iloc[i - 2]
        close_1 = df["close"].iloc[i - 1]

        high = df["high"].iloc[i]
        low = df["low"].iloc[i]

        threshold = threshold_series.iloc[i] if auto_threshold else threshold_series

        # ---------- Bullish FVG ----------
        bull_gap_pct = (low - high_2) / high_2 if high_2 != 0 else 0

        bull_fvg = (
            low > high_2
            and close_1 > high_2
            and bull_gap_pct > threshold
        )

        # ---------- Bearish FVG ----------
        bear_gap_pct = (low_2 - high) / high if high != 0 else 0

        bear_fvg = (
            high < low_2
            and close_1 < low_2
            and bear_gap_pct > threshold
        )

        if bull_fvg:
            df.at[df.index[i], "fvg_type"] = "bull"
            df.at[df.index[i], "fvg_high"] = low
            df.at[df.index[i], "fvg_low"] = high_2
            df.at[df.index[i], "fvg_gap_pct"] = bull_gap_pct

        elif bear_fvg:
            df.at[df.index[i], "fvg_type"] = "bear"
            df.at[df.index[i], "fvg_high"] = low_2
            df.at[df.index[i], "fvg_low"] = high
            df.at[df.index[i], "fvg_gap_pct"] = bear_gap_pct

    # -------------------------
    # Mitigation 检测（回补）
    # -------------------------
    active_fvgs = []

    for i in range(len(df)):
        row = df.iloc[i]

        # 新 FVG 入队
        if row["fvg_type"] in ("bull", "bear"):
            active_fvgs.append(
                {
                    "type": row["fvg_type"],
                    "high": row["fvg_high"],
                    "low": row["fvg_low"],
                }
            )

        # 检查是否被回补
        close = row["close"]
        for fvg in active_fvgs[:]:
            if fvg["type"] == "bull" and close < fvg["low"]:
                df.at[df.index[i], "fvg_mitigated"] = True
                active_fvgs.remove(fvg)

            elif fvg["type"] == "bear" and close > fvg["high"]:
                df.at[df.index[i], "fvg_mitigated"] = True
                active_fvgs.remove(fvg)

    return df


if __name__ == "__main__":
    # 1. 设置 Pandas 打印参数，确保不折叠列、显示所有行
    pd.set_option("display.max_rows", None)  # 显示所有行
    pd.set_option("display.max_columns", None)  # 显示所有列
    pd.set_option("display.width", 2000)  # 打印总宽度
    pd.set_option("display.max_colwidth", None)  # 单元格内容不截断
    # 模拟数据测试
    from scripts.stock_query import stock_zh_a_daily_mysql

    # 获取数据并计算
    raw_df = stock_zh_a_daily_mysql(symbol="600628", start_date="20240101", end_date="20251231")
    fvg_df = fair_value_gap_indicator(raw_df)

    # 筛选出产生过 FVG 的日期查看
    print(fvg_df)