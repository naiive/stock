# -*- coding: utf-8 -*-
"""
Module: APIClient
Description: 三方行情接口封装。负责从公网（如 AkShare/东方财富）拉取实时的市场快照。
关键点：
1. 健壮性：通过 @retry 装饰器应对不稳定的网络请求。
2. 数据清洗：统一接口返回的中文列名为系统内部的标准英文 ID。
3. 容错性：处理数值类型的强制转换，防止出现非法字符导致的计算崩溃。
"""

import akshare as ak
import pandas as pd
from core.utils.decorator import retry
from conf.config import SYSTEM_CONFIG


class APIClient:
    """
    三方接口客户端：负责实时行情获取
    """

    def __init__(self):
        # 从配置中获取请求超时时间，默认 20 秒
        self.timeout = SYSTEM_CONFIG.get("REQUEST_TIMEOUT", 20)

    @retry(max_retries=5, delay=5)
    def fetch_realtime_snapshot(self):
        """
        获取全市场 A 股实时行情快照。

        【设计意图】：
        由于全量扫描涉及 5000+ 股票，如果在循环中逐一获取，会触发接口频率限制（封 IP）。
        因此，该方法被设计为“一次请求，获取全市场”，然后由 DataHandler 存入内存缓存。

        Returns:
            pd.DataFrame: 包含全市场最新价格、成交量等核心字段。
        """
        try:
            # 使用 akshare 东方财富 (em) 接口。该接口涵盖了上交所、深交所、北交所的所有实时变动。
            # 它是目前公开行情中响应速度最快、字段最全的快照接口。
            df = ak.stock_zh_a_spot_em()

            # --- 步骤 1: 列名标准化映射 ---
            # 接口原始返回的是中文字段名，为了方便后续策略计算（df['close']），此处进行统一映射。
            column_map = {
                '代码': 'code',
                '名称': 'name',
                '最新价': 'close',
                '成交量': 'volume',
                '今开': 'open',
                '最高': 'high',
                '最低': 'low',
                '成交额': 'amount',
                '换手率': 'turnover',
                '市盈率-动态': 'pe',
                '总市值': 'mcap',
                '流通市值': 'ffmc',
                '年初至今涨跌幅': 'ytd'
            }
            df = df.rename(columns=column_map)

            # A股代码在不同平台可能有 'sh600000', '000001.SZ' 等不同格式。
            # 系统内部统一采用 6 位纯数字字符串。
            if 'code' in df.columns:
                # regex=True: 寻找所有非数字字符 (\D) 并替换为空，只保留 6 位数字
                df['code'] = df['code'].astype(str).str.replace(r'\D', '', regex=True)
                # zfill(6): 确保代码为 6 位。例如将 '1' 补全为 '000001'
                df['code'] = df['code'].str.zfill(6)
            else:
                # 如果代码列不存在，说明接口返回格式彻底改变，需返回空表触发异常
                return pd.DataFrame()

            # 强制类型转换：确保所有价格和成交量都是 float/int 类型
            # errors='coerce': 如果遇到无法转换的异常字符，则标记为 NaN，而不是直接报错崩溃
            money_cols = ['mcap', 'ffmc'] # 转化为亿元
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turnover', 'pe', 'mcap', 'ffmc', 'ytd']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if col in money_cols:
                        # 核心逻辑：转换为亿元并保留两位小数
                        df[col] = (df[col] / 100000000).round(2)

            return df
        except Exception as e:
            # 这里抛出的异常会被 @retry 捕捉，从而在 5 秒后执行下一次尝试
            raise Exception(f"获取全市场实时快照失败: {e}")