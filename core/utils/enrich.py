#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据增强工具

提供对扫描结果的增强：把实时快照中的名称、换手率、市盈率、市值、年涨幅等字段
映射到结果 DataFrame 上，便于导出与后续分析。

对外接口：enrich_results(df_res, handler=None, df_live=None)
- df_res: 必填，包含至少列 `代码` 的结果 DataFrame。
- handler: 可选，若传入则优先使用 handler.api_client.fetch_realtime_snapshot() 获取快照。
- df_live: 可选，若外部已获取实时快照，可直接传入以避免重复请求。

返回：增强后的 DataFrame；若过程出错或数据缺失，原样返回 df_res。
"""

from __future__ import annotations
import pandas as pd

try:
    # 仅在需要自行获取快照且未提供 handler/df_live 时使用
    from core.data_client.api_client import APIClient
except Exception:
    APIClient = None  # 兜底，占位

def enrich_results(df_res: pd.DataFrame, handler=None, df_live: pd.DataFrame | None = None) -> pd.DataFrame:
    if df_res is None or df_res.empty:
        return df_res

    try:
        # 1) 获取实时快照 DataFrame
        if df_live is None:
            if handler is not None and getattr(handler, 'api_client', None) is not None:
                df_live = handler.api_client.fetch_realtime_snapshot()
            elif APIClient is not None:
                df_live = APIClient().fetch_realtime_snapshot()
            else:
                df_live = pd.DataFrame()

        if df_live is None or df_live.empty:
            return df_res

        # 2) 选择需要映射的字段
        info_cols = ['code', 'name', 'turnover', 'pe', 'mcap', 'ffmc', 'ytd']
        df_info = df_live[[c for c in info_cols if c in df_live.columns]]

        # 3) 合并
        df_enriched = pd.merge(df_res, df_info, left_on='代码', right_on='code', how='left')

        # 4) 清理与列顺序调整
        if 'code' in df_enriched.columns:
            df_enriched.drop(columns=['code'], inplace=True)

        head_cols = ['turnover', 'pe', 'mcap', 'ffmc', 'ytd']
        head_cols = [c for c in head_cols if c in df_enriched.columns]
        others = [c for c in df_enriched.columns if c not in head_cols]
        # 将名称靠前显示（放到第3位）
        if 'name' in others:
            others = [c for c in others if c != 'name']
            others.insert(2, 'name')

        sorted_df = df_enriched[others + head_cols]

        # 5) 重命名为中文导出友好列
        export_map = {
            'name': '名称',
            'turnover': '换手率(%)',
            'pe': '市盈率(动)',
            'mcap': '总市值(亿)',
            'ffmc': '流通市值(亿)',
            'ytd': '年涨幅(%)'
        }
        return sorted_df.rename(columns=export_map)

    except Exception as e:
        print(f"⚠️ [警告] enrich_results 过程出错: {e}")
        return df_res
