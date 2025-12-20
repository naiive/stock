# -*- coding: utf-8 -*-
import os
import json
import datetime
import pandas as pd
import akshare as ak
from conf.config import PATH_CONFIG
from core.utils.retry import retry


class StockListManager:
    """
    股票清单管理器：负责获取、过滤以及本地 JSON 缓存
    """

    def __init__(self, db_client):
        self.db_client = db_client
        self.cache_file = PATH_CONFIG["CACHE_FILE"]

    def get_stock_list(self):
        """
        核心方法：优先读缓存，失效则读数据库并过滤，最后存缓存
        """
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")

        # 1. 尝试读取本地 JSON 缓存
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                if cache.get("time") == today_str:
                    print(f"📦 [系统] 加载本日缓存清单，共 {len(cache['data'])} 支股票")
                    return pd.DataFrame(cache["data"])
            except Exception as e:
                print(f"⚠️ [警告] 缓存读取异常: {e}")

        # 2. 缓存失效，从数据库获取并执行过滤
        print("🔍 [系统] 缓存失效或不存在，正在从数据库构建股票池...")
        df = self.fetch_stock_list_safe()  # 原始数据库拉取

        if not df.empty:

            # 3. 写入本地缓存
            try:
                with open(self.cache_file, "w", encoding="utf-8") as f:
                    cache_data = {
                        "time": today_str,
                        "data": df.to_dict(orient="records")
                    }
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)
                print(f"✅ [系统] 缓存已更新至: {self.cache_file}")
            except Exception as e:
                print(f"❌ [错误] 缓存写入失败: {e}")

        return df

    # ============================================================
    # 模块 4：获取/缓存 全市场股票列表
    # ============================================================
    @retry(max_retries=2, delay=1)
    def fetch_stock_list_safe(self):
        print("[系统] 正在尝试获取全量股票列表...")
        try:
            df = ak.stock_info_a_code_name()
            if not df.empty and "code" in df.columns:
                print("[系统] 成功: 使用 stock_info_a_code_name 接口")
                return df[["code", "name"]]
        except Exception as e:
            print(f"[警告] 轻量接口失败 ({e})，尝试备用接口...")
        try:
            df = ak.stock_zh_a_spot_em()
            print("[系统] 成功: 使用 stock_zh_a_spot_em 接口")
            if '代码' in df.columns:
                df = df.rename(columns={'代码': 'code', '名称': 'name'})
            return df[["code", "name"]]
        except Exception as e:
            raise Exception(f"所有股票列表接口均不可用: {e}")