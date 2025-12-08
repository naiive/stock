import pandas as pd
import numpy as np
import akshare as ak


def cm_williams_vix_fix_signal(
    df: pd.DataFrame,
    pd_len=22,
    bbl=20,
    mult=2.0,
    lb=50,
    ph=0.85
):
    """
    返回 DataFrame，包含：
    - date
    - close
    - wvf
    - is_bright（是否亮）
    """

    # 1️⃣ 最高收盘价
    highest_close = df["close"].rolling(pd_len).max()

    # 2️⃣ WVF
    wvf = (highest_close - df["low"]) / highest_close * 100

    # 3️⃣ Bollinger Band（基于 WVF）
    mid = wvf.rolling(bbl).mean()
    stdev = wvf.rolling(bbl).std()
    upper_band = mid + mult * stdev

    # 4️⃣ 百分位极值
    range_high = wvf.rolling(lb).max() * ph

    # 5️⃣ 是否“亮”
    is_bright = (wvf >= upper_band) | (wvf >= range_high)

    result = df[["date", "close"]].copy()
    result["wvf"] = wvf
    result["is_bright"] = is_bright.astype(int)  # 1=亮，0=不亮

    return result


# ======================
# 示例：A 股
# ======================
symbol = "600588"

df = ak.stock_zh_a_hist(
    symbol=symbol,
    period="daily",
    adjust="qfq"
)

df.rename(columns={
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low"
}, inplace=True)

df.reset_index(drop=True, inplace=True)

result = cm_williams_vix_fix_signal(df)

# ✅ 输出最近 20 天是否“亮”
print(result.tail(50))
