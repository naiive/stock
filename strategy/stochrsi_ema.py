import pandas as pd
import numpy as np
import datetime
import math

from api.stock_query import stock_zh_a_daily_mysql

# 打印结果（只显示最后几行和有信号的行）
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# ==========================================
# 模块 1: Pine Script 核心平滑函数
# ==========================================
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
    """ 计算 Average True Range (ATR) """
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


# ==========================================
# 模块 2: StochRSI 核心计算
# ==========================================
def calculate_stoch_rsi_values(series, length_rsi, length_stoch):
    """计算 StochRSI 的原始 K 值 (已修正类型，确保 Series.rolling 可用)"""
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


def calculate_stoch_rsi_signal(df, length_rsi=14, length_stoch=14, smooth_k=3, smooth_d=3, oversold_level=20):
    """计算 StochRSI K, D 值及超卖突破买入信号。"""
    stoch_rsi_raw = calculate_stoch_rsi_values(df['close'], length_rsi, length_stoch)

    k = pine_sma(stoch_rsi_raw, smooth_k)
    d = pine_sma(k, smooth_d)

    k_crossover_level = (k.shift(1) <= oversold_level) & (k > oversold_level)
    k_gt_d = (k > d)
    buy_signal_raw = k_crossover_level & k_gt_d

    buy_signal_series = np.where(buy_signal_raw == True, 'STOCH_RSI_BUY', '')

    return k, d, pd.Series(buy_signal_series, index=df.index)


# ==========================================
# main (整合短期涨幅评估)
# ==========================================
def main(code):
    # --- ATR 参数配置 ---
    ATR_SETTING = {
        "lengthATR": 14,
        "stop_loss_multiplier": 3.0,
        "take_profit_multiplier": 6.0
    }

    # --- 🆕 短期评估周期配置 ---
    LOOKUP_DAYS = [1, 2, 3, 4, 5, 6, 7, 8, 9]

    # 1. 获取数据
    df = stock_zh_a_daily_mysql(
        symbol="sh" + code,  # 假设您的 code 已经包含了 sh/sz/等等
        start_date='20240101',
        end_date='20251231',
        adjust='qfq'
    )

    df = df.rename(columns={'trade_date': 'date'})
    df = df.sort_values('date').reset_index(drop=True)

    # 2. 【新增】计算未来 N 日涨幅
    for days in LOOKUP_DAYS:
        # 获取未来 N 日的收盘价
        future_close = df['close'].shift(-days)
        # 计算百分比涨幅：((未来价 / 当前价) - 1) * 100
        df[f'Gain_{days}D'] = ((future_close / df['close']) - 1) * 100

    # 3. 计算 StochRSI 信号、EMA 和 ATR (保持不变)
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
    df['Stop_Loss_Price'] = df['close'] - (sl_mult * df['ATR'])
    df['Take_Profit_Price'] = df['close'] + (tp_mult * df['ATR'])

    print("\n=== StochRSI 买入信号、EMA 过滤与短期表现评估结果 ===")

    # 4. 定义趋势过滤条件: close > EMA50 > EMA200
    trend_filter = (df['close'] > df['EMA50']) & \
                   (df['EMA50'] > df['EMA200'])

    # 5. 应用所有过滤条件
    filtered_signals = df[
        (df['stoch_rsi_signal'] != '') &  # 必须有 StochRSI 买入信号
        (trend_filter)  # 必须满足上涨趋势条件
        ].copy()  # 解决 SettingWithCopyWarning

    # 6. 格式化并打印结果
    if filtered_signals.empty:
        print("在指定日期范围内未找到符合 (StochRSI BUY AND C > E50 > E200) 策略的信号。")
    else:
        # --- 🆕 定义所有输出列 ---
        base_cols = [
            'date', 'close', 'stoch_k', 'stoch_d', 'EMA50', 'EMA200',
            'ATR', 'Stop_Loss_Price', 'Take_Profit_Price', 'stoch_rsi_signal'
        ]

        # 添加新的涨幅列名
        gain_cols = [f'Gain_{d}D' for d in LOOKUP_DAYS]
        result_cols = base_cols + gain_cols

        # 应用四舍五入 (针对指标和价格)
        for col in ['close', 'stoch_k', 'stoch_d', 'EMA50', 'EMA200', 'Stop_Loss_Price', 'Take_Profit_Price']:
            filtered_signals[col] = filtered_signals[col].round(2)
        filtered_signals['ATR'] = filtered_signals['ATR'].round(3)

        # 应用四舍五入 (针对涨幅，保留两位百分比)
        for col in gain_cols:
            filtered_signals[col] = filtered_signals[col].round(2)

        print("--- 满足 StochRSI 金叉 & C > E50 > E200 趋势过滤的买入信号 (含短期表现) ---")
        # 🚨 使用 .loc 避免再次触发 SettingWithCopyWarning
        print(filtered_signals.loc[:, result_cols])


if __name__ == "__main__":
    # 运行代码时，记得将您的股票代码 002946 传入 main 函数
    main("002946")