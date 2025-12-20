# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine
from conf.config import INDICATOR_CONFIG, DB_CONFIG


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

    def get_stock_pool(self):
        """
        从基础信息表获取全量 A 股代码清单，并应用 EXCLUDE 规则
        """
        query = "SELECT code, name FROM stock_basic"
        df = pd.read_sql(query, self.engine)

        exclude = INDICATOR_CONFIG["EXCLUDE"]

        # 1. 过滤创业板
        if exclude["EXCLUDE_GEM"]:
            df = df[~df['code'].str.startswith(('300', '301'))]

        # 2. 过滤科创板
        if exclude["EXCLUDE_KCB"]:
            df = df[~df['code'].str.startswith(('688', '689'))]

        # 3. 过滤北交所
        if exclude["EXCLUDE_BJ"]:
            df = df[~df['code'].str.startswith(('8', '4', '92'))]

        # 4. 过滤 ST (模糊匹配)
        if exclude["EXCLUDE_ST"]:
            df = df[~df['name'].str.contains("ST|退")]

        return df['code'].tolist()

    def fetch_daily_data(self, symbol, days=365):
        """
        读取单只股票的日线历史数据。
        注意：这里返回的顺序是根据日期升序排列，方便 indicators 计算
        """
        # 使用参数化查询防止 SQL 注入
        query = f"""
            SELECT date, open, high, low, close, volume, amount 
            FROM a_stock_daily 
            WHERE code = '{symbol}' 
            ORDER BY date DESC 
            LIMIT {days}
        """
        try:
            df = pd.read_sql(query, self.engine)
            # 策略需要历史在前，最新在后
            df = df.sort_values('date', ascending=True).reset_index(drop=True)
            return df
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()