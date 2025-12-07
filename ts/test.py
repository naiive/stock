import akshare as ak
import pandas as pd
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.max_colwidth", None)

def get_daily_data(code: str) -> pd.DataFrame:
    df = ak.stock_zh_a_daily(
        symbol=code,
        start_date="2025-12-01",
        end_date="2025-12-04",
        adjust="qfq"  # 前复权
    )

    return df

def get_minute_data(code: str, period: str = "1") -> pd.DataFrame:

    df = ak.stock_zh_a_minute(
        symbol=code,
        period=period,
        adjust="qfq"
    )



    return df.tail(1)

if __name__ == "__main__":
    df = get_daily_data("sh600269")
    print("------------历史-----------")
    print(df)
    print("------------历史-----------")

    # period: 分钟周期，支持 '1' '5' '15' '30' '60'
    df2 = get_minute_data("sh600269", period="5")
    print("------------实时-----------")
    print(df2)
    print("------------实时-----------")


    # # 1. 提取 df2 中 day 列的日期部分（去除时间）
    # df2['date'] = pd.to_datetime(df2['day']).dt.date

    # # 2. 检查 df 中是否存在该日期
    # for _, row in df2.iterrows():
    #     new_date = row['date']
    #
    #     # 3. 如果 df 中没有这个日期，则添加新的一行数据
    #     if new_date not in pd.to_datetime(df['date']).dt.date.values:
    #         new_row = {
    #             'date': new_date,
    #             'open': row['open'],
    #             'high': row['high'],
    #             'low': row['low'],
    #             'close': row['close'],
    #             'volume': row['volume'],
    #             'amount': None,  # 如果没有数据的话，可以设置为 None 或其他缺省值
    #             'outstanding_share': None,
    #             'turnover': None
    #         }
    #
    #         # 4. 将新的一行添加到 df 中
    #         df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    #
    # print("------------合计-----------")
    # print(df)
    # print("------------合计-----------")