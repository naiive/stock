import akshare as ak
import pandas as pd
import datetime

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.max_colwidth", None)

CONFIG = { "DAYS": 10 }
end_date = datetime.datetime.now().strftime("%Y%m%d")
start_date = (datetime.datetime.now() - datetime.timedelta(days=CONFIG["DAYS"])).strftime("%Y%m%d")

def get_daily_data(code: str) -> pd.DataFrame:
    daily_df = ak.stock_zh_a_daily(
        symbol=code,
        start_date=start_date,
        end_date=end_date,
        adjust="qfq"  # 前复权
    )
    return daily_df

def get_minute_data(code: str, period: str = "1") -> pd.DataFrame:

    min_df = ak.stock_zh_a_minute(
        symbol=code,
        period=period,
        adjust="qfq"
    )
    return min_df.tail(10)


def fetch_realtime_snapshot():
    try:
        df = ak.stock_zh_a_spot()
        # 规范化列名 (腾讯接口的列名可能不同，需要根据实际情况调整)
        df = df.rename(columns={'代码': 'code', '最新价': 'close', '成交量': 'volume'})
        df['code'] = df['code'].astype(str).str.zfill(6)
        return df
    except Exception as e:
        raise Exception(f"实时行情接口失败: {e}")

if __name__ == "__main__":
    daily = get_daily_data("sh600519")
    print("------------历史-----------")
    print(daily)
    print("------------历史-----------")
    #
    # period: 分钟周期，支持 '1' '5' '15' '30' '60'
    min = get_minute_data("sh600519", period="1")
    print("------------实时-----------")
    print(min)
    print("------------实时-----------")

    # df3 = fetch_realtime_snapshot()
    # print("------------实时全量-----------")
    # print(df3)
    # print("------------实时全量-----------")