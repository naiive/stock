import ccxt
import pandas as pd
import talib


pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.max_colwidth", None)


# --- 配置 ---
SYMBOL = 'BTC/USDT'
TIMEFRAME = '1h'
LIMIT = 1000  # 获取 1000 根 K 线


# --- 1. 获取数据 ---
def fetch_ohlcv(symbol, timeframe, limit):
    print(f"正在从 Binance 获取 {symbol} 的 {timeframe} 数据...")
    try:
        exchange = ccxt.binance()
        # 获取 OHLCV 数据 (Open, High, Low, Close, Volume)
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

        # 转换为 DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        print("数据获取成功！")
        print(df)
        return df
    except Exception as e:
        print(f"数据获取失败: {e}")
        return None


df = fetch_ohlcv(SYMBOL, TIMEFRAME, LIMIT)
if df is None:
    # 如果数据获取失败，结束脚本
    exit()

# --- 2. 计算指标 ---
# 假设的策略指标：
# 1. 长期趋势确认：MA200
# 2. 短期趋势信号：快均线 (MA20) 和 慢均线 (MA50)

df['MA200'] = talib.SMA(df['close'], timeperiod=200)
df['MA20'] = talib.SMA(df['close'], timeperiod=20)
df['MA50'] = talib.SMA(df['close'], timeperiod=50)

# 清理 NaN 值 (通常是指标计算初期的前 N 根 K 线)
df.dropna(inplace=True)
print(f"数据清洗完毕，用于回测的 K 线数量: {len(df)}")
print("\n数据预览:")
print(df.tail())