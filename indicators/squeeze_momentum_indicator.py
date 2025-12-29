# -*- coding: utf-8 -*-

"""
# ============================================================
# 指标 Squeeze Momentum Indicator [LazyBear]
# ============================================================
"""

import pandas as pd
import numpy as np
from enum import Enum

class MomentumHistogramColor(Enum):
    BULL_ACCELERATING = "lime"     # 亮绿：多头动能 在增强
    BULL_DECELERATING = "green"    # 暗绿：多头动能 在减弱
    BEAR_ACCELERATING = "red"      # 亮红：空头动能 在增强
    BEAR_DECELERATING = "maroon"   # 暗红：空头动能 在减弱
    NEUTRAL = "neutral"            # 中性
    UNDEFINED = "undefined"        # 数据不足

def tv_linreg(y: pd.Series, length: int) -> float:
    """线性回归拟合工具"""
    if pd.isna(y).any() or len(y) < length:
        return np.nan
    x = np.arange(length)
    y_vals = y.values[-length:] # 确保只取最新长度
    A = np.vstack([x, np.ones(length)]).T
    try:
        m, b = np.linalg.lstsq(A, y_vals, rcond=None)[0]
        return m * (length - 1) + b
    except:
        return np.nan

def true_range(df: pd.DataFrame) -> pd.Series:
    """计算真实波幅 TR"""
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

def get_squeeze_momentum_histogram_color(val, val_prev):
    """
    Squeeze Momentum Histogram: 动能柱颜色
    """
    if pd.isna(val) or pd.isna(val_prev):
        return MomentumHistogramColor.UNDEFINED.value

    if val > 0:
        return (
            MomentumHistogramColor.BULL_ACCELERATING.value
            if val > val_prev
            else MomentumHistogramColor.BULL_DECELERATING.value
        )
    elif val < 0:
        return (
            MomentumHistogramColor.BEAR_ACCELERATING.value
            if val < val_prev
            else MomentumHistogramColor.BEAR_DECELERATING.value
        )
    else:
        return MomentumHistogramColor.NEUTRAL.value

def add_squeeze_counter(df: pd.DataFrame)-> pd.DataFrame:
    """
    作用：给每根K线打上一个 连续积压/释放计数
    逻辑：
        1.遍历每根K线的 sqz_status（状态：ON:挤压、OFF:释放、NO:无）
        2.如果当前状态和上一个相同，则计数加1
        3.如果状态改变，则计数重新从1开始
    输出：新增一列 sqz_id，表示当前K线是连续第几根挤压/释放柱

    用途：判断"Squeeze Momentum Indicator"连续积压至少5根K线的条件，和第1根释放K线
    """
    counter = 0
    current_state = None
    sqz_id_list = []
    for status in df["sqz_status"]:
        if status in ["ON", "OFF"]:
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

def squeeze_momentum_indicator(
        df: pd.DataFrame,
        length: int = 20,
        lengthKC: int = 20,
        multKC: float = 1.2,
        useTrueRange: bool = True
)-> pd.DataFrame:
    """
    作用：核心函数，用于计算 Squeeze Momentum Indicator
    返回：
          code        date     open     high      low    close      volume        amount sqz_status  sqz_hvalue  sqz_id sqz_hcolor
        600519  2025-01-27  1409.38  1416.21  1399.59  1407.41   2935646.0  4.213535e+09         NO         NaN       0  undefined
        600519  2025-02-05  1412.32  1415.26  1375.26  1376.82   4075652.0  5.753730e+09         NO         NaN       0  undefined
        600519  2025-02-06  1373.10  1394.66  1373.10  1385.68   2735582.0  3.859568e+09         ON         NaN       1  undefined
        600519  2025-02-18  1441.74  1464.29  1433.98  1446.65   2779992.0  4.113845e+09         ON         NaN       9  undefined
        600519  2025-02-19  1446.70  1465.71  1436.74  1462.34   3239331.0  4.803378e+09         ON         NaN      10  undefined
        600519  2025-02-26  1427.47  1436.80  1417.22  1431.95   2636609.0  3.835949e+09         ON         NaN      15  undefined
        600519  2025-02-27  1431.96  1461.26  1426.05  1457.00   4976217.0  7.368002e+09         ON         NaN      16  undefined
        600519  2025-02-28  1456.95  1499.00  1453.51  1471.94   5612895.0  8.475738e+09        OFF         NaN       1  undefined
        600519  2025-03-03  1473.72  1491.75  1453.02  1458.44   3159566.0  4.736680e+09         ON         NaN       1  undefined
        600519  2025-03-04  1456.46  1457.44  1437.05  1441.85   2521121.0  3.710676e+09         ON         NaN       2  undefined
        600519  2025-03-05  1443.71  1445.67  1432.03  1438.18   2460522.0  3.606932e+09         ON   31.932643       3  undefined
        600519  2025-03-06  1445.67  1481.17  1443.78  1477.03   4216764.0  6.297117e+09         ON   31.094471       4      green
        600519  2025-03-07  1474.11  1498.98  1474.11  1491.76   3799135.0  5.760609e+09         ON   34.022550       5       lime
        600519  2025-03-17  1625.15  1626.12  1595.61  1606.38   5468913.0  8.985139e+09        OFF   67.415261       2       lime
        600519  2025-04-16  1522.17  1545.71  1507.46  1529.20   3115605.0  4.834881e+09         ON  -35.621407       7     maroon
        600519  2025-04-17  1524.13  1546.20  1520.20  1539.82   2384605.0  3.733925e+09         ON  -25.492939       8     maroon
    """
    close, high, low = df['close'], df['high'], df['low']

    # 计算Bollinger Bands (BB)
    # 通过移动平均+标准差计算BB上下轨
    basis = close.rolling(length).mean()
    dev = multKC * close.rolling(length).std(ddof=0)
    upperBB, lowerBB = basis + dev, basis - dev

    # 计算Keltner Channels (KC)
    # 通过ATR或高低差计算KC上下轨
    # 用于判断市场是否处于低波动（挤压）状态
    ma = close.rolling(lengthKC).mean()
    r = true_range(df) if useTrueRange else (high - low)
    rangema = r.rolling(lengthKC).mean()
    upperKC, lowerKC = ma + rangema * multKC, ma - rangema * multKC

    # 判断Squeeze状态 {"ON":"积压", "OFF":"释放", "NO":无}
    sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)
    df["sqz_status"] = np.select([sqzOn, sqzOff], ["ON", "OFF"], default="NO")

    # 计算Momentum柱的线性趋势
    highest_h = high.rolling(lengthKC).max()
    lowest_l = low.rolling(lengthKC).min()
    avg_hl = (highest_h + lowest_l) / 2
    sma_close = close.rolling(lengthKC).mean()
    mid = (avg_hl + sma_close) / 2
    source_mid = close - mid
    # 柱状图值大小，0轴上为正，0轴下为负
    histogram_value = source_mid.rolling(lengthKC).apply(lambda x: tv_linreg(pd.Series(x), lengthKC), raw=False)

    # 动能柱数值
    df["sqz_hvalue"] = histogram_value
    # 前一根动能柱数值，用于判断动能柱颜色：亮绿色、绿色、亮红色、红色
    df["sqz_pre_hvalue"] = histogram_value.shift(1)
    # 给每根K线打上一个连续积压或释放计数值，用于判断连续积压
    df = add_squeeze_counter(df)

    # 柱状图颜色
    df["sqz_hcolor"] = df.apply(lambda re: get_squeeze_momentum_histogram_color(re["sqz_hvalue"], re["sqz_pre_hvalue"]), axis=1)

    # 删除一些中间结果列
    df.drop(columns=["sqz_pre_hvalue"], inplace=True)

    return df

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from scripts.stock_query import stock_zh_a_daily_mysql

    print(squeeze_momentum_indicator(stock_zh_a_daily_mysql(
        symbol='600519',
        start_date='20250101',
        end_date='20251219',
        adjust='qfq')))
