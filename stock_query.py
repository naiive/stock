import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from typing import Optional, Literal

# =========================================================
# 配置信息 (与您的导入脚本保持一致)
# =========================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "Elaiza112233",
    "database": "stock"
}
TABLE_NAME = "a_stock_daily"


# 数据库连接函数
def get_db_engine():
    """创建并返回数据库连接引擎"""
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
    )
    return create_engine(url)


# =========================================================
# 模拟查询接口：stock_zh_a_daily_mysql
# =========================================================

def stock_zh_a_daily_mysql(
        symbol: str,
        start_date: Optional[str] = "20000101",
        end_date: Optional[str] = None,
        adjust: Optional[Literal['qfq', 'hfq', '']] = 'qfq'
) -> pd.DataFrame:
    """
    模拟 AkShare 的 stock_zh_a_daily 接口，从 MySQL 数据库查询 A 股历史数据。

    :param symbol: 股票代码，格式为 'sh600000' 或 'sz000001'。
    :param start_date: 起始日期，格式 'YYYYMMDD'。
    :param end_date: 结束日期，格式 'YYYYMMDD'。默认为今天。
    :param adjust: 复权类型，'qfq' (前复权) 或 'hfq' (后复权)。默认为 'qfq'。
    :return: 包含日线数据的 Pandas DataFrame。
    """

    # 1. 参数处理
    code = symbol[2:]  # 提取 6 位股票代码 (e.g., '600000')
    adjust_type = adjust if adjust in ['qfq', 'hfq'] else ''

    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")

    # 2. 构造 SQL 查询语句
    # 数据库中的 date 字段是 DATE 类型，可以直接进行日期比较
    sql = f"""
    SELECT 
        date, 
        open, 
        high, 
        low, 
        close, 
        volume, 
        amount 
    FROM {TABLE_NAME}
    WHERE 
        code = '{code}' AND 
        adjust = '{adjust_type}' AND 
        date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date ASC;
    """

    # 3. 执行查询
    try:
        engine = get_db_engine()
        df = pd.read_sql(sql, engine)

        if df.empty:
            print(f"警告：未找到 {symbol} ({adjust}) 在 {start_date} 到 {end_date} 期间的数据。")
            return pd.DataFrame()

        # 4. 格式化输出 (模拟 AkShare 格式)
        # 确保列名顺序和类型符合您的需求
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # 模拟 AkShare 输出的列名（注意 AkShare 默认输出9列，这里只返回您存储的7列）
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']

        return df

    except Exception as e:
        print(f"查询数据库失败: {e}")
        return pd.DataFrame()


# =========================================================
# 示例用法
# =========================================================

if __name__ == '__main__':
    print("--- 正在测试查询接口 ---")

    # 沪市主板 - 前复权数据
    df_600519 = stock_zh_a_daily_mysql(
        symbol='sh000016',
        start_date='20250717',
        end_date='20250726',
        adjust='qfq'
    )
    print("\n--- 前复权数据（部分）---")
    print(df_600519.tail())
