import pandas as pd
import numpy as np
from api.stock_query import stock_zh_a_daily_mysql

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# ======================================================
# Pine EMA (adjust=false)
# ======================================================
def pine_ema(series, length):
    return series.ewm(alpha=2 / (length + 1), adjust=False).mean()


# ======================================================
# DEMA
# ======================================================
def dema(series, length):
    ma1 = pine_ema(series, length)
    ma2 = pine_ema(ma1, length)
    return 2 * ma1 - ma2


# ======================================================
# calcSlope —— Pine 完整复刻
# ======================================================
def calc_slope_intercept_pine(series, length, offset=0):
    def regress(arr):
        if np.any(np.isnan(arr)):
            return np.nan

        sumX = sumY = sumXSqr = sumXY = 0.0

        for i in range(1, length + 1):
            val = arr[length - i]
            per = i + 1.0
            sumX += per
            sumY += val
            sumXSqr += per * per
            sumXY += val * per

        denom = length * sumXSqr - sumX * sumX
        if denom == 0:
            return np.nan

        slope = (length * sumXY - sumX * sumY) / denom
        avg = sumY / length
        intercept = avg - slope * sumX / length + slope

        return intercept + slope * (length - offset)

    return series.rolling(length).apply(regress, raw=True)


# ======================================================
# b5 / oc —— TradingView 状态机级复刻
# ======================================================
def calculate_b5_and_oc_pine(tt1_series):
    tt1 = tt1_series.values
    n = len(tt1)

    b5 = np.full(n, np.nan)
    oc = np.full(n, 0.0)

    cum_abs = 0.0
    bar_index = 0   # 等价 cum(1)

    for i in range(n):
        src15 = tt1[i]
        bar_index += 1
        n5 = bar_index - 1

        prev_b5 = b5[i - 1] if i > 0 and not np.isnan(b5[i - 1]) else src15

        # ---- cum(abs())：每一根 bar 都前进
        if np.isnan(src15):
            diff = 0.0
        else:
            diff = abs(src15 - prev_b5)

        cum_abs += diff

        if n5 <= 0:
            b5[i] = src15
            oc[i] = 0
            continue

        a15 = cum_abs / n5

        if src15 > prev_b5 + a15:
            b5[i] = src15
        elif src15 < prev_b5 - a15:
            b5[i] = src15
        else:
            b5[i] = prev_b5

        # ---- oc 状态（完全等价 Pine）
        if i == 0:
            oc[i] = 0
        elif b5[i] > b5[i - 1]:
            oc[i] = 1
        elif b5[i] < b5[i - 1]:
            oc[i] = -1
        else:
            oc[i] = oc[i - 1]

    return (
        pd.Series(b5, index=tt1_series.index),
        pd.Series(oc, index=tt1_series.index)
    )


# ======================================================
# OBV MACD 主逻辑（图形级一致）
# ======================================================
def calculate_obv_macd_signal(df):
    WINDOW_LEN = 28
    V_LEN = 14
    OBV_LEN = 1
    MA_LEN = 9
    SLOW_LEN = 26
    SLOPE_LEN = 2

    # ---------- OBV ----------
    chg = df['close'].diff().fillna(0)
    v = (np.sign(chg) * df['volume']).cumsum()

    smooth = v.rolling(V_LEN).mean()

    v_spread = (v - smooth).rolling(WINDOW_LEN).std(ddof=0)
    price_spread = (df['high'] - df['low']).rolling(WINDOW_LEN).std(ddof=0)

    shadow = np.where(
        v_spread != 0,
        (v - smooth) / v_spread * price_spread,
        0
    )

    out = np.where(
        shadow > 0,
        df['high'] + shadow,
        df['low'] + shadow
    )

    src = pine_ema(pd.Series(out, index=df.index), OBV_LEN)

    # ---------- MACD ----------
    ma_fast = dema(src, MA_LEN)
    slow_ma = pine_ema(df['close'], SLOW_LEN)
    macd = ma_fast - slow_ma

    # ---------- slope / tt1 ----------
    tt1 = calc_slope_intercept_pine(macd, SLOPE_LEN)

    # ---------- b5 / oc ----------
    b5, oc = calculate_b5_and_oc_pine(tt1)

    # ---------- 信号（TradingView offset = -1） ----------
    signal = pd.Series('', index=df.index)
    signal[(oc == 1) & (oc.shift(1) == -1)] = 'BUY'
    signal[(oc == -1) & (oc.shift(1) == 1)] = 'SELL'
    signal = signal.shift(-1)

    return pd.DataFrame({
        'date': df['date'],
        'close': df['close'],
        'macd': macd,
        'tt1': tt1,
        'b5': b5,
        'oc': oc,
        'signal': signal
    })


# ======================================================
# main
# ======================================================
def main():
    df = stock_zh_a_daily_mysql(
        symbol='sh002651',
        start_date='20240101',
        end_date='20251231',
        adjust='qfq'
    )

    df = df.rename(columns={'trade_date': 'date'})
    df = df.sort_values('date').reset_index(drop=True)

    result = calculate_obv_macd_signal(df)

    print("\n=== OBV MACD TradingView 图形级一致信号 ===")
    print(result[result['signal'] != ''].tail(200))


if __name__ == "__main__":
    main()
