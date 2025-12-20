# engine/result_schema.py
"""
============================================================
📌 扫描结果标准结构定义（ScanResult）

目的：
- 统一策略输出格式
- 支持评分 / 排序 / 推送 / 回测复用
============================================================
"""

from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class ScanResult:
    """
    单只股票扫描结果
    """

    # --- 基础信息 ---
    code: str
    date: str
    close: float

    # --- 信号状态 ---
    signal: bool                # 是否满足入场条件
    score: float                # 综合评分（用于排序）

    # --- 策略子信号 ---
    signals: Dict[str, Any] = field(default_factory=dict)

    # --- 额外信息 ---
    reason: str = ""            # 触发原因（日志 / 推送用）
