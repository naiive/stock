# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
from core.utils.decorator import retry
from conf.config import SYSTEM_CONFIG

class APIClient:
    """
    三方接口客户端：负责实时行情获取
    """
    def __init__(self):
        self.timeout = SYSTEM_CONFIG.get("REQUEST_TIMEOUT", 20)

    @retry(max_retries=5, delay=5)
    def fetch_realtime_snapshot(self):
        """
        获取全市场实时行情快照 (akshare)
        """
        try:
            # 获取 A 股实时行情
            df = ak.stock_zh_a_spot_em()  # 建议用 em (东方财富) 接口，通常更稳定

            # --- 列名标准化映射 ---
            column_map = {
                '代码': 'code',
                '最新价': 'close',
                '成交量': 'volume',
                '今开': 'open',
                '最高': 'high',
                '最低': 'low',
                '成交额': 'amount',
            }
            df = df.rename(columns=column_map)

            # --- 关键修正：纯数字代码处理 ---
            if 'code' in df.columns:
                # 正则去除所有非数字字符 (如 sh, sz 等前缀)
                df['code'] = df['code'].astype(str).str.replace(r'\D', '', regex=True)
                df['code'] = df['code'].str.zfill(6)
            else:
                return pd.DataFrame()

            # 筛选必要列并处理数值
            required_cols = ['code', 'open', 'high', 'low', 'close', 'volume', 'amount']
            df = df[[c for c in required_cols if c in df.columns]]

            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            return df

        except Exception as e:
            raise Exception(f"获取实时快zo失败: {e}")