import pandas as pd
import numpy as np
import datetime
import math

from api.stock_query import stock_zh_a_daily_mysql

# 打印结果（只显示最后几行和有信号的行）
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# ------------------------------------------
# 模块 1: Pine Script 核心平滑函数
# ------------------------------------------
def pine_rma(series, length):
    """ RMA (Wilder's Smoothing) """
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    alpha = 1 / length
    return series.ewm(alpha=alpha, adjust=False).mean()


def pine_sma(series, length):
    """ Simple Moving Average (SMA) """
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    return series.rolling(length).mean()


def manual_ema(series, length):
    """ 指数移动平均线 (EMA) """
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    alpha = 2 / (length + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()


def calculate_atr(df, length=14):
    """ 计算 Average True Range (ATR)，使用 RMA 平滑 """
    df_temp = df.copy()
    high = df_temp['high']
    low = df_temp['low']
    close_prev = df_temp['close'].shift(1)

    tr1 = high - low
    tr2 = (high - close_prev).abs()
    tr3 = (low - close_prev).abs()

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_series = pine_rma(true_range, length)
    return atr_series


# ------------------------------------------
# 模块 2: StochRSI 核心计算
# ------------------------------------------
def calculate_stoch_rsi_values(series, length_rsi, length_stoch):
    """计算 StochRSI 的原始 K 值"""
    if not isinstance(series, pd.Series):
        series = pd.Series(series)

    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    up_avg = pine_rma(up, length_rsi)
    down_avg = pine_rma(down, length_rsi)

    rs_arr = np.where(down_avg != 0, up_avg / down_avg, np.inf)
    rsi_arr = 100 - (100 / (1 + rs_arr))
    rsi = pd.Series(rsi_arr, index=series.index)

    lowest_rsi = rsi.rolling(length_stoch).min()
    highest_rsi = rsi.rolling(length_stoch).max()
    denominator = highest_rsi - lowest_rsi
    stoch_rsi_raw = np.where(denominator != 0, (rsi - lowest_rsi) / denominator, 0)
    stoch_rsi_raw = pd.Series(stoch_rsi_raw, index=series.index) * 100
    return stoch_rsi_raw


def calculate_stoch_rsi_signal(df, length_rsi=14, length_stoch=14, smooth_k=3, smooth_d=3, oversold_level=30):
    """计算 StochRSI K, D 值及超卖突破买入信号。"""
    stoch_rsi_raw = calculate_stoch_rsi_values(df['close'], length_rsi, length_stoch)

    k = pine_sma(stoch_rsi_raw, smooth_k)
    d = pine_sma(k, smooth_d)

    k_crossover_level = (k.shift(1) <= oversold_level) & (k > oversold_level)
    k_gt_d = (k > d)
    buy_signal_raw = k_crossover_level & k_gt_d

    buy_signal_series = np.where(buy_signal_raw == True, 'STOCH_RSI_BUY', '')

    return k, d, pd.Series(buy_signal_series, index=df.index)


# ------------------------------------------
# 模块 3: ADX 核心计算
# ------------------------------------------
def calculate_adx(df, length=14):
    """
    计算 ADX, +DI (PDI) 和 -DI (MDI)。
    """
    df_temp = df.copy()
    high = df_temp['high']
    low = df_temp['low']

    up = high - high.shift(1)
    down = low.shift(1) - low

    pdm = np.where((up > down) & (up > 0), up, 0)
    mdm = np.where((down > up) & (down > 0), down, 0)

    tr = calculate_atr(df, length=1)

    atr_smooth = pine_rma(tr, length)
    pdm_smooth = pine_rma(pd.Series(pdm, index=df.index), length)
    mdm_smooth = pine_rma(pd.Series(mdm, index=df.index), length)

    pdi = (pdm_smooth / atr_smooth) * 100
    mdi = (mdm_smooth / atr_smooth) * 100

    sum_di = pdi + mdi
    dx = np.where(sum_di != 0, (pdi - mdi).abs() / sum_di * 100, 0)

    adx = pine_rma(pd.Series(dx, index=df.index), length)

    return adx, pdi, mdi


# ==========================================
# main (整合 ADX 过滤和输出优化)
# ==========================================
def main(code):
    # --- ATR 参数配置 (短线/隔夜交易优化) ---
    ATR_SETTING = {
        "lengthATR": 7,  # 周期缩短，更灵敏
        "stop_loss_multiplier": 2.0,  # 止损倍数 M 设为 2.0
        "take_profit_multiplier": 4.0  # 止盈倍数，保持 2:1 风险回报比
    }

    # --- ADX 过滤参数配置 ---
    ADX_LENGTH = 14
    ADX_THRESHOLD = 25.0

    # --- 短期评估周期配置 ---
    LOOKUP_DAYS = [1, 2, 3]

    # 1. 获取数据
    df = stock_zh_a_daily_mysql(
        symbol="sh" + code,
        start_date='20240101',
        end_date='20251231',
        adjust='qfq'
    )

    df = df.rename(columns={'trade_date': 'date'})
    df = df.sort_values('date').reset_index(drop=True)

    # 2. 计算未来 N 日涨幅
    for days in LOOKUP_DAYS:
        future_close = df['close'].shift(-days)
        df[f'Gain_{days}D'] = ((future_close / df['close']) - 1) * 100

    # 3. 计算指标：StochRSI, EMA, ATR
    stoch_k, stoch_d, stoch_rsi_signal = calculate_stoch_rsi_signal(df)
    df['stoch_k'] = stoch_k
    df['stoch_d'] = stoch_d
    df['stoch_rsi_signal'] = stoch_rsi_signal

    df['EMA50'] = manual_ema(df['close'], 50)
    df['EMA200'] = manual_ema(df['close'], 200)

    atr_length = ATR_SETTING["lengthATR"]
    sl_mult = ATR_SETTING["stop_loss_multiplier"]
    tp_mult = ATR_SETTING["take_profit_multiplier"]

    df['ATR'] = calculate_atr(df, length=atr_length)

    # 4. 计算 ADX, PDI, MDI
    adx, pdi, mdi = calculate_adx(df, length=ADX_LENGTH)
    df['ADX'] = adx
    df['PDI'] = pdi
    df['MDI'] = mdi

    # 5. ATR 止损/止盈计算
    df['Stop_Loss_Price'] = df['low'] - (sl_mult * df['ATR'])
    df['Take_Profit_Price'] = df['close'] + (tp_mult * df['ATR'])

    print("\n=== StochRSI 买入信号、EMA 过滤、ADX 趋势强度与短期表现评估结果 ===")

    # 6. 定义所有过滤条件
    trend_filter = (df['close'] > df['EMA50']) & \
                   (df['EMA50'] > df['EMA200'])  # 长期趋势向上过滤

    # ADX 过滤：趋势强度 ADX > 20 且 多头方向 +DI > -DI (PDI > MDI)
    adx_filter = (df['ADX'] > ADX_THRESHOLD) & (df['PDI'] > df['MDI'])

    # 7. 应用所有过滤条件
    filtered_signals = df[
        (df['stoch_rsi_signal'] != '') &
        (trend_filter) &
        (adx_filter)
        ].copy()

    # 8. 格式化并打印结果
    if filtered_signals.empty:
        print(f"在指定日期范围内未找到符合 (StochRSI BUY AND C > E50 > E200 AND ADX > {ADX_THRESHOLD}) 策略的信号。")
    else:
        # --- 应用四舍五入 ---
        cols_to_round_2 = ['close', 'low', 'stoch_k', 'stoch_d', 'EMA50', 'EMA200', 'Stop_Loss_Price',
                           'Take_Profit_Price', 'ADX', 'PDI', 'MDI']
        for col in cols_to_round_2:
            if col in filtered_signals.columns:
                filtered_signals[col] = filtered_signals[col].round(2)
        filtered_signals['ATR'] = filtered_signals['ATR'].round(3)

        gain_cols = [f'Gain_{d}D' for d in LOOKUP_DAYS]
        for col in gain_cols:
            filtered_signals[col] = filtered_signals[col].round(2)

        # ----------------------------------------
        # 🆕 关键优化：重命名 ADX 相关的列并定义最终输出顺序
        # ----------------------------------------
        filtered_signals = filtered_signals.rename(columns={
            'PDI': 'DI+',
            'MDI': 'DI-'
        })

        # 定义最终输出列的顺序
        result_cols_final = [
                                'date', 'close', 'low', 'stoch_k', 'stoch_d',
                                'EMA50', 'EMA200',
                                'ADX', 'DI+', 'DI-',  # 使用重命名后的 DI+ 和 DI-
                                'ATR', 'Stop_Loss_Price', 'Take_Profit_Price', 'stoch_rsi_signal'
                            ] + gain_cols

        print(
            f"--- 满足 StochRSI 金叉 & 趋势过滤 & ADX > {ADX_THRESHOLD} 的买入信号 (ATR={atr_length}, SL={sl_mult}x) ---")
        print(filtered_signals.loc[:, result_cols_final])


if __name__ == "__main__":
    main("000546")