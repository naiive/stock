import yfinance as yf
import pandas as pd
import json
from datetime import datetime, timedelta
import os

# 定义 JSON 文件名
JSON_FILE_NAME = "../conf/ondo_finance_cache.json"


def load_stock_data_from_json(file_path):
    """
    从指定的 JSON 文件中加载股票代码列表。
    """
    if not os.path.exists(file_path):
        print(f"❌ 错误: 文件未找到 - {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

            # 假设 JSON 结构是 {"data": [{"code": "...", "name": "..."}, ...]}
            if 'data' in data and isinstance(data['data'], list):
                # 提取所有股票代码 (Ticker Symbol)
                tickers = [item['code'] for item in data['data'] if 'code' in item]
                print(f"✅ 成功从 {file_path} 中加载 {len(tickers)} 个股票代码。")
                return tickers
            else:
                print(f"❌ 错误: JSON 文件结构不正确，未找到 'data' 列表。")
                return None

    except json.JSONDecodeError:
        print(f"❌ 错误: 文件 {file_path} 不是有效的 JSON 格式。")
        return None
    except Exception as e:
        print(f"❌ 错误: 读取文件时发生未知错误 - {e}")
        return None


def get_multiple_stock_data(stock_list, days_ago=10):
    """
    获取列表中所有股票代码过去指定天数的每日收盘价和成交量。
    """
    if not stock_list:
        print("列表为空，无需查询。")
        return pd.DataFrame(), []

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_ago * 2)

    all_data = []
    failed_tickers = []

    print(f"--- 🚀 开始查询 {len(stock_list)} 个股票/ETF的历史数据 (最近 {days_ago} 个交易日) ---")

    for ticker in stock_list:
        try:
            # 使用 yf.download 批量获取数据
            df = yf.download(ticker,
                             start=start_date,
                             end=end_date,
                             progress=False,
                             auto_adjust=True)  # 显式设置 auto_adjust=True 消除警告
            if df.empty:
                failed_tickers.append(ticker)
                continue

            # 仅保留最近 N 个交易日的数据，并选择需要的列
            df_recent = df.tail(days_ago)[['Close', 'Volume']].copy()
            df_recent.columns = [f'{ticker}_Close', f'{ticker}_Volume']
            all_data.append(df_recent)

        except Exception as e:
            failed_tickers.append(ticker)

    # 将所有股票数据按日期（Index）合并
    if all_data:
        # 使用 join 合并，确保日期对齐
        final_df = all_data[0]
        for df in all_data[1:]:
            final_df = final_df.join(df, how='outer')
        return final_df, failed_tickers
    else:
        return pd.DataFrame(), failed_tickers


# --- 主程序运行 ---
if __name__ == "__main__":

    # 1. 从 JSON 文件中加载股票代码
    TICKERS = load_stock_data_from_json(JSON_FILE_NAME)

    if TICKERS:
        # 2. 执行查询
        data_df, failures = get_multiple_stock_data(TICKERS, days_ago=10)

        print("\n" + "=" * 50)

        if not data_df.empty:
            print("✅ 整合后的数据 (最近10个交易日):")
            # 打印整合后的数据
            pd.set_option('display.max_columns', None)
            print(data_df.tail())

            print(f"\n总共获取了 {len(data_df)} 条数据 (每日收盘价和成交量)。")
        else:
            print("❌ 未能获取任何股票数据。")

        if failures:
            print("\n⚠️ 以下代码未能成功获取数据，请检查代码:")
            print(failures)