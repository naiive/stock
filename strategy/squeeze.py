import pandas as pd
import numpy as np
import akshare as ak

def tv_linreg(y, length):
    if y.isna().any():
        return np.nan

    x = np.arange(length)
    y = y.values

    A = np.vstack([x, np.ones(length)]).T
    m, b = np.linalg.lstsq(A, y, rcond=None)[0]

    return m * (length - 1) + b


# ============ True Range ============
def true_range(df):
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


# ============ 中文颜色转换（完全对应 LazyBear）===========
def get_color_cn(v, v_prev):
    if pd.isna(v) or pd.isna(v_prev):
        return None

    if v == 0:
        return "中性"

    if v > 0:  # 多头
        return "强多" if v > v_prev else "弱多"

    # 空头 v < 0（更负 = 强空）
    return "强空" if v < v_prev else "弱空"


# ============ 添加挤压 / 释放连续编号 ============
def add_squeeze_counter(df):
    counter = 0
    current_state = None
    sqz_id_list = []

    for status in df["sqz_status"]:
        if status in ["挤压", "释放"]:
            if status == current_state:
                counter += 1
            else:
                current_state = status
                counter = 1
            sqz_id_list.append(counter)
        else:
            current_state = None
            counter = 0
            sqz_id_list.append(0)

    df["sqz_id"] = sqz_id_list
    return df


# ============ 完全复刻 LazyBear SQZMOM ============
def squeeze_momentum(
    df, length=20, mult=2.0, lengthKC=20, multKC=1.5, useTrueRange=True
):
    close = df['close']
    high = df['high']
    low = df['low']

    # ==== BB（注意 LazyBear 使用 multKC!) ====
    basis = close.rolling(length).mean()
    dev = multKC * close.rolling(length).std(ddof=0)
    upperBB = basis + dev
    lowerBB = basis - dev

    # ==== KC ====
    ma = close.rolling(lengthKC).mean()

    if useTrueRange:
        range_ = true_range(df)
    else:
        range_ = high - low

    rangema = range_.rolling(lengthKC).mean()
    upperKC = ma + rangema * multKC
    lowerKC = ma - rangema * multKC

    # ==== 挤压状态 ====
    sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)

    df["sqz_status"] = np.select([sqzOn, sqzOff], ["挤压", "释放"], default="无")

    # ==== 动能（关键：TV linreg）====
    highest_h = high.rolling(lengthKC).max()
    lowest_l = low.rolling(lengthKC).min()
    avg_hl = (highest_h + lowest_l) / 2
    sma_close = close.rolling(lengthKC).mean()
    mid = (avg_hl + sma_close) / 2

    source_mid = close - mid

    val = source_mid.rolling(lengthKC).apply(
        lambda x: tv_linreg(pd.Series(x), lengthKC), raw=False
    )

    df["val"] = val
    df["val_prev"] = val.shift(1)
    df["val_color"] = df.apply(lambda r: get_color_cn(r["val"], r["val_prev"]), axis=1)

    return df


# ============ 主调用 ============
def get_squeeze_with_akshare(stock_code):

    df = ak.stock_zh_a_daily(
        symbol=stock_code,
        start_date="20250101",
        end_date="20251205",
        adjust="qfq"
    )


    df = squeeze_momentum(df)
    df = add_squeeze_counter(df)

    print(df[['date','close','val','val_color','sqz_status','sqz_id']].tail(50))
    return df


# ============ 入口 ============
if __name__ == "__main__":
    get_squeeze_with_akshare("sh600461")
