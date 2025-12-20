# -*- coding: utf-8 -*-
import pandas as pd
import datetime
from sqlalchemy import create_engine
from conf.config import TABLE_CONFIG, DB_CONFIG


class MySQLClient:
    """
    MySQL 客户端：负责所有结构化数据的读取
    """
    def __init__(self):
        # 数据库连接配置 (建议从环境变量读取或在 settings.py 中定义连接串)
        # 示例：mysql+pymysql://用户名:密码@地址:端口/数据库名
        self.db_url = (
            f"mysql+pymysql://{DB_CONFIG['USER']}:{DB_CONFIG['PASS']}@"
            f"{DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['DB_NAME']}?"
            f"charset={DB_CONFIG['CHARSET']}"
        )
        self.engine = create_engine(self.db_url, pool_size=10, max_overflow=20)

    def fetch_daily_data(self, symbol, days=500):
        """
        优化版：利用复合索引，通过日期范围直接定位数据
        """
        # 1. 获取配置中的表名
        table_name = TABLE_CONFIG.get("QUERY_DAILY_TABLE", "a_stock_daily")

        # 1. 计算起始日期
        # 注意：交易日不等于自然日。如果要保证 365 根 K 线，
        # 考虑到周末和节假日，建议回溯天数为 days * 1.5
        start_buffer_days = int(days * 1.5)
        start_date = (datetime.datetime.now() - datetime.timedelta(days=start_buffer_days)).strftime('%Y-%m-%d')
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # 2. 构建查询
        # 利用 (code, date) 的索引顺序，极快定位
        # 记得带上 adjust 条件，否则主键索引不完整
        query = f"""
            SELECT date, open, high, low, close, volume, amount 
            FROM {table_name} 
            WHERE code = '{symbol}' 
              AND date >= '{start_date}'
              AND date <= '{end_date}'
              AND adjust = 'qfq'
            ORDER BY date ASC
        """
        try:
            # 3. 直接读取，已经是 ASC 升序，无需再次 sort
            df = pd.read_sql(query, self.engine)

            # 4. 如果多取了，只保留最近的指定根数
            if len(df) > days:
                df = df.tail(days).reset_index(drop=True)

            return df
        except Exception as e:
            print(f"❌ 读取 {symbol} 历史数据异常: {e}")
            return pd.DataFrame()