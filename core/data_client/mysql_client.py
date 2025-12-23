# -*- coding: utf-8 -*-
"""
Module: MySQLClient
Description: 数据库访问层。基于 SQLAlchemy 构建高效连接池，负责从本地 MySQL 读取历史日线数据。
关键点：
1. 连接池管理：pool_size 防止频繁建连消耗资源，max_overflow 处理瞬时高并发请求。
2. 索引优化：通过 (code, date) 复合条件查询，实现毫秒级数据检索。
3. 业务规范：强制过滤 adjust='qfq'（前复权），确保价格序列在技术指标计算时不会因除权产生“虚假跳空”。
"""

import pandas as pd
import datetime
from sqlalchemy import create_engine
from conf.config import STRATEGY_CONFIG, TABLE_CONFIG, DB_CONFIG

class MySQLClient:
    """
    MySQL 客户端：负责所有结构化 K 线数据的读取
    """

    def __init__(self):
        # 1. 构造数据库连接字符串 (SQLAlchemy 标准格式)
        # 驱动使用 pymysql，支持 UTF-8 字符集以兼容中文表名或字段
        self.db_url = (
            f"mysql+pymysql://{DB_CONFIG['USER']}:{DB_CONFIG['PASS']}@"
            f"{DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['DB_NAME']}?"
            f"charset={DB_CONFIG['CHARSET']}"
        )

        # 2. 创建数据库引擎并配置连接池
        # pool_size: 保持 10 个常驻连接
        # max_overflow: 并发高峰时允许额外开启 20 个连接，总计 30 个并发能力
        self.engine = create_engine(
            self.db_url,
            pool_size=10,
            max_overflow=20,
            pool_recycle=3600  # 每小时回收连接，防止 MySQL 服务器主动断开导致报错
        )

    def fetch_daily_data(self, symbol, days=500):
        """
        根据股票代码读取指定数量的历史日线数据。
        【优化思路】：
        - 利用 (code, date, adjust) 的复合索引。
        - 仅查询必要的列，减少网络传输负载。
        - 预过滤复权类型。

        Args:
            symbol (str): 股票代码（如 '600000'）。
            days (int): 需要回溯的 K 线根数，默认 500 根。

        Returns:
            pd.DataFrame: 包含 ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'] 的 DataFrame。
        """
        # 1. 获取配置中的表名，若无则使用默认值
        table_name = TABLE_CONFIG.get("QUERY_DAILY_TABLE", "a_stock_daily")

        # 2. 计算日期搜索范围
        # 重要：因为股市有周末和节假日，自然日的 365 天只有约 240 个交易日。
        # 为了保证取到足够的 days 根 K 线，我们将日期回溯范围扩大 1.5 倍（start_buffer_days）。
        start_buffer_days = int(days * 1.5)
        now = datetime.datetime.now()
        start_date = (now - datetime.timedelta(days=start_buffer_days)).strftime('%Y-%m-%d')
        # 有历史结束日期就用，没有就用当前日期
        end_date = STRATEGY_CONFIG.get("HISTORY_END_DAY") or datetime.datetime.now().strftime('%Y-%m-%d')

        # 3. 构建原生 SQL 查询语句
        # WHERE 条件顺序应尽量匹配数据库索引：code -> date -> adjust
        # ORDER BY date ASC 确保返回的数据从旧到新排列，直接符合 pandas/talib 计算习惯
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
            # 4. 使用 Pandas 直接读取 SQL 结果
            # read_sql 会自动管理连接的借出与归还
            df = pd.read_sql(query, self.engine)

            # 5. 后置截断逻辑
            # 由于我们多取了 1.5 倍的数据，此处通过 tail 确保输出结果精确为用户要求的 days 根
            if len(df) > days:
                df = df.tail(days).reset_index(drop=True)

            return df

        except Exception as e:
            # 这里的异常捕获非常关键：防止单只股票的数据缺失导致整个扫描引擎崩溃
            print(f"❌ 读取 {symbol} 历史数据异常: {e}")
            return pd.DataFrame()