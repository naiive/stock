# -*- coding: utf-8 -*-
"""
Module: StockListManager
Description: 股票池管理器。负责全市场股票名单的拉取、本地持久化缓存、以及根据规则过滤（如剔除 ST、创业板等）。
关键点：
1. 减少网络请求：通过 JSON 缓存本日名单，避免频繁调用 API 被封。
2. 容灾机制：集成多个数据接口，当一个失效时自动尝试另一个。
3. 动态过滤：支持通过配置文件开关，快速排除不想要的板块。
"""

import os
import json
import datetime
import pandas as pd
import akshare as ak
from conf.config import PATH_CONFIG, INDICATOR_CONFIG  # 导入路径和指标过滤配置
from core.utils.decorator import retry  # 导入重试装饰器


class StockListManager:
    """
    股票清单管理器：负责获取、过滤以及本地 JSON 缓存
    """

    def __init__(self, db_client=None):
        """
        初始化管理器
        Args:
            db_client: 数据库客户端实例，用于后续扩展（如存储详细财务数据）
        """
        self.db_client = db_client
        # 从配置中读取缓存文件的存储路径
        self.cache_file = PATH_CONFIG["CACHE_FILE"]

    def get_stock_list(self):
        """
        核心调度方法：获取原始名单（优先读缓存） -> 统一执行过滤 -> 返回
        Returns:
            pd.DataFrame: 经过过滤后的 ['code', 'name'] 数据框
        """
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        df_raw = pd.DataFrame()

        # --- 步骤 1: 尝试获取原始数据（优先读取本日本地缓存） ---
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    cache = json.load(f)

                # 检查缓存日期：如果缓存是今天的，则直接加载，不再访问网络
                if cache.get("time") == today_str:
                    print(f"📦 [系统] 发现当日全股票代码JSON缓存，使用缓存")
                    df_raw = pd.DataFrame(cache["data"])
            except Exception as e:
                print(f"⚠️ [警告] 缓存读取异常: {e}")

        # --- 步骤 2: 网络拉取（如果缓存不存在或已过期） ---
        if df_raw.empty:
            print("🔍 [系统] 缓存失效，正在从 API 获取全量名单...")
            # 调用带重试机制的接口获取方法
            df_raw = self.fetch_stock_list_safe()

            # 存入缓存（存储的是过滤前的“原始全家福”，方便以后在不联网的情况下修改过滤规则）
            self._save_to_cache(df_raw, today_str)

        # --- 步骤 3: 执行过滤逻辑 ---
        # 无论数据来源是缓存还是 API，都必须统一执行过滤规则，确保 EXCLUDE 配置生效
        df_filtered = self._apply_exclude_rules(df_raw)

        return df_filtered

    def _apply_exclude_rules(self, df):
        """
        内部方法：执行具体的板块过滤逻辑。
        涉及：创业板(GEM)、科创板(KCB)、北交所(BJ)、ST 及退市股票。
        """
        if df.empty:
            return df

        # 从配置字典中提取过滤开关
        exclude_cfg = INDICATOR_CONFIG.get("EXCLUDE", {})
        total_before = len(df)

        # 【预处理】：确保 code 是 6 位字符串格式（补 0），防止 000001 变成 1
        df['code'] = df['code'].astype(str).str.zfill(6)

        # 1. 过滤创业板 (代码以 300 或 301 开头)
        if exclude_cfg.get("EXCLUDE_GEM"):
            df = df[~df['code'].str.startswith(('300', '301'))]

        # 2. 过滤科创板 (代码以 688 或 689 开头)
        if exclude_cfg.get("EXCLUDE_KCB"):
            df = df[~df['code'].str.startswith(('688', '689'))]

        # 3. 过滤北交所 (涉及多种开头形式：8, 4, 92等)
        if exclude_cfg.get("EXCLUDE_BJ"):
            df = df[~df['code'].str.startswith(('8', '4', '9', '43', '83', '87'))]

        # 4. 过滤 ST 和 退市股
        if exclude_cfg.get("EXCLUDE_ST"):
            # 使用正则或包含逻辑排除名称中带有 ST、*ST 或 退 字样的股票
            # str.upper() 确保能兼容大小写 st
            df = df[~df['name'].str.upper().str.contains("ST|退")]

        print(f"✅ [过滤] 原始: {total_before} 支 -> 过滤后: {len(df)} 支")
        return df

    def _save_to_cache(self, df, date_str):
        """
        将原始股票代码信息写入本地 JSON 文件。
        """
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                cache_data = {
                    "time": date_str,
                    "data": df.to_dict(orient="records")  # 将 DataFrame 转为字典列表存储
                }
                # ensure_ascii=False 确保中文名称不被转码，indent=2 提高可读性
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            print(f"💾 [系统] 原始清单已缓存至: {self.cache_file}")
        except Exception as e:
            print(f"❌ [错误] 缓存写入失败: {e}")

    @retry(max_retries=2, delay=1)
    def fetch_stock_list_safe(self):
        """
        带重试机制的 API 获取逻辑。
        为了防止单一接口被封或数据异常，采用双接口备份方案。
        """
        # --- 方案 A: 尝试获取基础 A 股代码名称接口 ---
        try:
            df = ak.stock_info_a_code_name()
            if not df.empty:
                return df[["code", "name"]]
        except:
            # 如果接口 A 失败，静默跳过，尝试接口 B
            pass

        # --- 方案 B: 尝试获取实时行情快照接口（覆盖面更广） ---
        try:
            df = ak.stock_zh_a_spot_em()
            if not df.empty:
                # 统一列名为标准的 code 和 name
                rename_map = {'代码': 'code', '名称': 'name'}
                df = df.rename(columns=rename_map)
                return df[["code", "name"]]
        except Exception as e:
            # 如果两个接口都挂了，抛出异常触发 @retry 重试
            raise Exception(f"所有 API 接口均失效: {e}")

        return pd.DataFrame()