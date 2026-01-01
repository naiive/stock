#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ============================================================
# 指标 Volume Expansion Up Indicator（放量上涨）
# ============================================================
"""

import pandas as pd

def volume_expand_up_indicator(
    df: pd.DataFrame,
    vol_length: int = 5,
    vol_ratio_threshold: float = 2,
) -> pd.DataFrame:
    """
    放量上涨指标

    参数：
        df : DataFrame
            必须包含：open, close, volume
        vol_length : int
            成交量均线周期（默认 5）
        vol_ratio_threshold : float
            放量阈值（默认 2）

    返回：
        在原 df 基础上新增字段：
            vol_ma  成交量均线
            vol_ratio 量比
            is_up 今天有没有涨
            is_volume_expand 有没有放量
            is_volume_up 放量上涨
    """

    df = df.copy()

    # --- 日期索引处理（与 ATR 风格保持一致）---
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame 没有 'date' 列，也没有日期索引")

    # --- 成交量均线 ---
    df["vol_ma"] = df["volume"].rolling(vol_length).mean()

    # --- 量比 ---
    df["vol_ratio"] = df["volume"] / df["vol_ma"]

    # --- 是否上涨 ---
    df["prev_close"] = df["close"].shift(1)
    df["is_up"] = df["close"] > df["prev_close"]

    # --- 是否放量 ---
    df["is_volume_expand"] = df["vol_ratio"] >= vol_ratio_threshold

    # --- 放量上涨（核心信号）---
    df["is_volume_up"] = df["is_up"] & df["is_volume_expand"]

    return df


if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from scripts.stock_query import stock_zh_a_daily_mysql

    df = stock_zh_a_daily_mysql(
        symbol="600628",
        start_date="20240101",
        end_date="20251219",
        adjust="qfq",
    )

    print(
        volume_expand_up_indicator(df)
    )
