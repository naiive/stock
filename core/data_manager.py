# -*- coding: utf-8 -*-
import os
import json
import datetime
import pandas as pd
import akshare as ak
from conf.config import PATH_CONFIG, INDICATOR_CONFIG  # 导入过滤配置
from core.utils.retry import retry


class StockListManager:
    """
    股票清单管理器：负责获取、过滤以及本地 JSON 缓存
    """
    def __init__(self, db_client=None):
        self.db_client = db_client
        self.cache_file = PATH_CONFIG["CACHE_FILE"]

    def get_stock_list(self):
        """
        核心方法：获取原始名单（缓存或接口） -> 统一执行过滤 -> 返回
        """
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        df_raw = pd.DataFrame()

        # 1. 尝试获取原始数据（优先读缓存）
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                if cache.get("time") == today_str:
                    print(f"📦 [系统] 发现本日缓存，正在加载原始数据...")
                    df_raw = pd.DataFrame(cache["data"])
            except Exception as e:
                print(f"⚠️ [警告] 缓存读取异常: {e}")

        # 2. 如果没缓存或缓存失效，从接口获取
        if df_raw.empty:
            print("🔍 [系统] 缓存失效，正在从 API 获取全量名单...")
            df_raw = self.fetch_stock_list_safe()
            # 存入缓存（存的是过滤前的原始数据，方便以后修改过滤规则）
            self._save_to_cache(df_raw, today_str)

        # 3. 核心步骤：执行过滤逻辑
        # 无论数据从哪来，都必须经过这一步，你的 EXCLUDE 规则才会起作用
        df_filtered = self._apply_exclude_rules(df_raw)

        return df_filtered

    def _apply_exclude_rules(self, df):
        """
        根据配置文件执行过滤：GEM, KCB, BJ, ST
        """
        if df.empty:
            return df

        exclude_cfg = INDICATOR_CONFIG.get("EXCLUDE", {})
        total_before = len(df)

        # 确保 code 是字符串格式
        df['code'] = df['code'].astype(str).str.zfill(6)

        # 1. 过滤创业板 (300, 301)
        if exclude_cfg.get("EXCLUDE_GEM"):
            df = df[~df['code'].str.startswith(('300', '301'))]

        # 2. 过滤科创板 (688, 689)
        if exclude_cfg.get("EXCLUDE_KCB"):
            df = df[~df['code'].str.startswith(('688', '689'))]

        # 3. 过滤北交所 (8, 4, 92)
        if exclude_cfg.get("EXCLUDE_BJ"):
            df = df[~df['code'].str.startswith(('8', '4', '9', '43', '83', '87'))]

        # 4. 过滤 ST 和 退市
        if exclude_cfg.get("EXCLUDE_ST"):
            # 过滤名称包含 ST, *ST, 退 的股票
            df = df[~df['name'].str.upper().str.contains("ST|退")]

        print(f"✅ [过滤] 原始: {total_before} 支 -> 过滤后: {len(df)} 支")
        return df

    def _save_to_cache(self, df, date_str):
        """将股票代码信息写入本地缓存JSON文件"""
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                cache_data = {
                    "time": date_str,
                    "data": df.to_dict(orient="records")
                }
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            print(f"💾 [系统] 原始清单已缓存至: {self.cache_file}")
        except Exception as e:
            print(f"❌ [错误] 缓存写入失败: {e}")

    @retry(max_retries=2, delay=1)
    def fetch_stock_list_safe(self):
        """
        带重试机制的 API 获取
        """
        # 尝试接口 1
        try:
            df = ak.stock_info_a_code_name()
            if not df.empty:
                return df[["code", "name"]]
        except:
            pass
        # 尝试接口 2
        try:
            df = ak.stock_zh_a_spot_em()
            if not df.empty:
                # 统一列名
                rename_map = {'代码': 'code', '名称': 'name'}
                df = df.rename(columns=rename_map)
                return df[["code", "name"]]
        except Exception as e:
            raise Exception(f"所有 API 接口均失效: {e}")

        return pd.DataFrame()