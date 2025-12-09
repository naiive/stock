import pandas as pd
from sqlalchemy import create_engine, Engine
from datetime import datetime
from typing import Optional, Literal
import conf.config as conf

"""
============================================================
# 查询股票历史数据接口
============================================================
"""

# =========================================================
# 1. 配置信息
# =========================================================
DB_CONFIG = conf.DB_CONFIG
TABLE_NAME = conf.MYSQL_TABLE

# **全局变量：存储唯一的 Engine 实例**
# 使用单例模式的关键：确保 Engine 只被创建一次
_GLOBAL_DB_ENGINE: Optional[Engine] = None


# =========================================================
# 2. 数据库连接引擎 (单例实现)
# =========================================================

def get_db_engine() -> Engine:
    """
    创建或返回数据库连接引擎的单例实例。
    该函数只会在首次调用时创建 Engine，并启用连接池。
    """
    global _GLOBAL_DB_ENGINE

    # 如果 Engine 尚未创建，则创建它
    if _GLOBAL_DB_ENGINE is None:
        url = (
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
        )

        # 配置连接池参数，防止高并发下连接数溢出
        # pool_size=10: 连接池中至少维护 10 个空闲连接。
        # max_overflow=20: 允许连接池在满时额外创建最多 20 个连接。
        # pool_recycle=3600: 每 3600 秒 (1 小时) 回收一次连接，防止 MySQL 服务器超时断开。
        _GLOBAL_DB_ENGINE = create_engine(
            url,
            pool_size=10,
            max_overflow=20,
            pool_recycle=3600
        )
        # print("✅ 数据库引擎已首次创建并启用连接池。")

    return _GLOBAL_DB_ENGINE


# =========================================================
# 3. 模拟查询接口：stock_zh_a_daily_mysql
# =========================================================

def stock_zh_a_daily_mysql(
        symbol: str,
        start_date: Optional[str] = "20000101",
        end_date: Optional[str] = None,
        adjust: Optional[Literal['qfq', 'hfq', '']] = 'qfq'
) -> pd.DataFrame:
    """
    从 MySQL 数据库查询 A 股历史数据。

    :param symbol: 股票代码，格式为 'sh600000' 或 'sz000001'。
    :param start_date: 起始日期，格式 'YYYYMMDD'。
    :param end_date: 结束日期，格式 'YYYYMMDD'。
    :param adjust: 复权类型，'qfq' (前复权) 或 'hfq' (后复权)。
    :return: 包含日线数据的 Pandas DataFrame。
    """

    # 1. 参数处理
    code = symbol[2:]
    adjust_type = adjust if adjust in ['qfq', 'hfq'] else ''

    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")

    # 2. 构造 SQL 查询语句
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
        # 获取单例 Engine 实例
        engine = get_db_engine()

        # 使用 pd.read_sql，它会自动从 Engine 的连接池中获取和释放连接
        df = pd.read_sql(sql, engine)

        if df.empty:
            print(f"警告：未找到 {symbol} ({adjust}) 在 {start_date} 到 {end_date} 期间的数据。")
            return pd.DataFrame()

        # 4. 格式化输出
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']

        return df

    except Exception as e:
        print(f"查询数据库失败: {e}")
        return pd.DataFrame()


# =========================================================
# 4. 示例用法
# =========================================================

if __name__ == '__main__':
    print("--- 正在测试查询接口（首次调用将创建 Engine）---")

    # 第一次查询：Engine 被创建
    df_first = stock_zh_a_daily_mysql(
        symbol='sh000001',
        start_date='20250101',
        end_date='20250131',
        adjust='qfq'
    )
    print("\n--- 首次查询数据 (sh000001) ---")
    print(df_first.tail())

    print("\n--- 正在执行第二次查询（复用已创建的 Engine 和连接池）---")

    # 第二次查询：复用已有的 Engine，从连接池中获取连接
    df_second = stock_zh_a_daily_mysql(
        symbol='sh000001',
        start_date='20250101',
        end_date='20250131',
        adjust='hfq'
    )
    print("\n--- 第二次查询数据 (sh000001) ---")
    print(df_second.head())