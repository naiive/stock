import pandas as pd
import numpy as np

from api.stock_query import stock_zh_a_daily_mysql

# 打印结果（只显示最后几行和有信号的行）
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# ADX and DI for V4
def wilder_smoothing(series, length):
    """
    实现 Pine Script 中 ADX/DI 所使用的 Wilder's Smoothing 逻辑。
    SmoothedValue = Prev_SmoothedValue - (Prev_SmoothedValue / length) + CurrentValue
    """
    # 转换为 numpy 数组以便进行迭代计算
    values = series.values
    smoothed = np.empty_like(values)
    smoothed.fill(np.nan)

    # 初始化第一个值（通常为前 length 个值的 SMA，但Pine Script中是基于累积的逻辑）
    # 在许多技术分析库中，第一个平滑值直接使用前 length 个值的简单平均。
    # 为了简化且不引入复杂的迭代，我们采用技术分析库常用的惯例：
    # 第一个平滑值设置为前 length 个值的 SMA
    smoothed[length - 1] = np.sum(values[:length])

    # 从第 length 个值开始应用 Wilder's Smoothing
    for i in range(length, len(values)):
        smoothed[i] = smoothed[i - 1] - (smoothed[i - 1] / length) + values[i]

    return pd.Series(smoothed, index=series.index)

def calculate_adx_dmi(df, period=14, threshold=20):
    """
    计算 ADX, +DI, -DI 指标，并基于给定的规则生成买卖点。

    :param df: 包含 'high', 'low', 'close' 的 DataFrame
    :param period: ADX/DI 计算周期 (默认 14)
    :param threshold: ADX 阈值 (默认 20)
    :return: 包含指标和信号的 DataFrame
    """
    df = df.copy()

    # --- 1. 计算 True Range (TR) ---
    high_low = df['high'] - df['low']
    high_prev_close = np.abs(df['high'] - df['close'].shift(1))
    low_prev_close = np.abs(df['low'] - df['close'].shift(1))

    df['TrueRange'] = high_low.combine(high_prev_close, max).combine(low_prev_close, max)

    # --- 2. 计算 Directional Movement (+DM, -DM) ---
    up_move = df['high'] - df['high'].shift(1)
    down_move = df['low'].shift(1) - df['low']

    # +DM 逻辑: UpMove > DownMove 且 UpMove > 0
    df['DMPlus'] = np.where((up_move > down_move) & (up_move > 0), up_move, 0)

    # -DM 逻辑: DownMove > UpMove 且 DownMove > 0
    df['DMMinus'] = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

    # --- 3. Wilder's Smoothing (TR, +DM, -DM) ---
    df['SmoothedTR'] = wilder_smoothing(df['TrueRange'], period)
    df['SmoothedDMPlus'] = wilder_smoothing(df['DMPlus'], period)
    df['SmoothedDMMinus'] = wilder_smoothing(df['DMMinus'], period)

    # --- 4. 计算 +DI 和 -DI ---
    # 乘以 100
    df['DIPlus'] = (df['SmoothedDMPlus'] / df['SmoothedTR']) * 100
    df['DIMinus'] = (df['SmoothedDMMinus'] / df['SmoothedTR']) * 100

    # --- 5. 计算 DX (Directional Index) ---
    # DX = |+DI - -DI| / (+DI + -DI) * 100
    # 避免除以零
    sum_di = df['DIPlus'] + df['DIMinus']
    df['DX'] = np.where(sum_di != 0, np.abs(df['DIPlus'] - df['DIMinus']) / sum_di * 100, 0)

    # --- 6. 计算 ADX (DX 的 SMA) ---
    # Pine Script 中 ADX = sma(DX, len)。在 ADX/DMI 系统中，这通常也意味着 Wilder's Smoothing
    # 但为严格遵循您的 Pine Script 代码，我们使用标准的 SMA：
    df['ADX'] = df['DX'].rolling(window=period).mean()

    # --- 7. 生成买卖信号 ---
    df['Signal'] = ''

    # buy 的逻辑：+DI 在 -DI 上方，且ADX >= 20
    buy_condition = (df['DIPlus'] > df['DIMinus']) & (df['ADX'] >= threshold)

    # sell 的逻辑：+DI 在 -DI 下方，且ADX >= 20
    sell_condition = (df['DIPlus'] < df['DIMinus']) & (df['ADX'] >= threshold)

    # 标记信号
    df.loc[buy_condition, 'Signal'] = 'Buy'
    df.loc[sell_condition, 'Signal'] = 'Sell'

    # 优化输出
    cols_to_keep = ['date', 'high', 'low', 'close', 'DIPlus', 'DIMinus', 'ADX', 'Signal']
    return df[cols_to_keep]

# CM_Ult_MacD_MTF
def compute_macd(df, fast_length=12, slow_length=26, signal_length=9):
    # 1. 计算快速和慢速 EMA
    df['EMA_fast'] = df['close'].ewm(span=fast_length, adjust=False).mean()
    df['EMA_slow'] = df['close'].ewm(span=slow_length, adjust=False).mean()

    # 2. 计算 MACD (DIF)
    df['MACD'] = df['EMA_fast'] - df['EMA_slow']

    # 3. 计算 Signal Line (DEA) - ***** 此处已修改为 SMA *****
    # Pine Script 中您的代码是：signal = sma(macd, signalLength)
    df['Signal'] = df['MACD'].rolling(window=signal_length).mean()  # 使用 .rolling().mean() 计算 SMA

    # 4. 计算 Histogram (柱状图)
    df['Histogram'] = df['MACD'] - df['Signal']

    # 识别交叉买入和卖出信号
    # 定义交叉买入信号（MACD 向上突破 Signal）- 金叉
    df['cross_buy'] = (df['MACD'] > df['Signal']) & (df['MACD'].shift(1) <= df['Signal'].shift(1))

    # 定义交叉卖出信号（MACD 向下突破 Signal）- 死叉
    df['cross_sell'] = (df['MACD'] < df['Signal']) & (df['MACD'].shift(1) >= df['Signal'].shift(1))

    # 输出买入和卖出信号
    signals = []
    for idx, row in df.iterrows():
        if row['cross_buy']:
            signals.append('Buy')
        elif row['cross_sell']:
            signals.append('Sell')
        else:
            signals.append('')

    df['Signal_Type'] = signals  # 重命名列以避免与 MACD Signal 值混淆
    return df


def main():
    try:
        df = stock_zh_a_daily_mysql(symbol='sh000917', start_date='20250101', end_date='20251231', adjust='qfq')
        if df.empty:
            print("警告：获取的数据集为空。请检查数据源和查询参数。")
            return
    except Exception as e:
        print(f"数据获取失败：{e}")
        return


    # 计算【ADX and DI for V4】指标和信号
    period = 14
    threshold_value = 20
    adx_df = calculate_adx_dmi(df, period=period, threshold=threshold_value)

    # 计算 【CM_Ult_MacD_MTF】指标和信号
    fast_length = 12   # 快速 EMA 长度
    slow_length = 26   # 慢速 EMA 长度
    signal_length = 9  # 信号线长度

    macd_df = compute_macd(df, fast_length=fast_length, slow_length=slow_length, signal_length=signal_length)

    # 融合
    merged_df = pd.merge(
        left=adx_df,
        right=macd_df,
        on='date',
        how='left'
    )

    merged_df = merged_df[['date','Signal_x','Signal_Type']].rename(columns={'Signal_x': 'MACD','Signal_Type': 'ADX'})

    # 1. 确保 NaN 被视为空字符串，以便比较 (如果需要)
    merged_df['MACD'] = merged_df['MACD'].fillna('')
    merged_df['ADX'] = merged_df['ADX'].fillna('')

    # 2. 创建新列 'SIGNAL'
    merged_df['SIGNAL'] = ''

    # 我们使用 np.where 来高效地应用条件
    consensus_condition = (merged_df['MACD'] != '') & \
                          (merged_df['ADX'] != '') & \
                          (merged_df['MACD'] == merged_df['ADX'])

    merged_df.loc[consensus_condition, 'SIGNAL'] = merged_df['ADX']

    df_filtered = merged_df[merged_df['SIGNAL'] != '']

    print(df_filtered[['date','SIGNAL']])




if __name__ == "__main__":
    main()