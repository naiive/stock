# -*- coding: utf-8 -*-
"""
Module: AsyncDispatcher
Description: 高性能异步并发调度工具。
设计理念：只负责“调度”与“并发控制”，不涉及具体的业务逻辑（策略计算或数据抓取）。
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Sequence, Any, Callable, List
from tqdm import tqdm
from conf.config import SYSTEM_CONFIG


async def run_dispatch(
        symbols: Sequence[str],
        worker_func: Callable[[str], Any],
        prepare_hook: Optional[Callable] = None,
        prepare_msg: str = "正在执行预处理...", # 👈 增加自定义消息参数
        finalize_hook: Optional[Callable[[List[Any]], None]] = None,
        desc: str = "扫描进度"
) -> List[Any]:
    if not symbols: return []

    # 1. 预处理
    if prepare_hook:
        print(f"🛠️ [系统] {prepare_msg}") # 👈 动态打印
        if asyncio.iscoroutinefunction(prepare_hook):
            await prepare_hook()
        else:
            prepare_hook()

    # 2. 分批
    batch_size = SYSTEM_CONFIG.get("BATCH_SIZE", 500)
    batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    all_matched = []
    max_workers = SYSTEM_CONFIG.get("MAX_WORKERS", 10)
    interval = SYSTEM_CONFIG.get("BATCH_INTERVAL_SEC", 1)
    loop = asyncio.get_running_loop()

    for i, batch in enumerate(batches):
        # 使用 print 打印大标题，确保它在进度条上方
        print(f"\n📦 正在执行第 {i + 1}/{len(batches)} 批次 (规模: {len(batch)})")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 这里的顺序很重要：先建立 tasks
            tasks = [loop.run_in_executor(executor, worker_func, s) for s in batch]

            # 使用 with 确保 tqdm 生命周期完整
            # leave=True 配合 position=0 可以防止跳行
            with tqdm(total=len(tasks), desc=f" > {desc}", dynamic_ncols=True, leave=True) as pbar:
                pbar.set_postfix({"总命中": len(all_matched)})

                # 关键：手动遍历已完成的任务并更新进度条
                for coro in asyncio.as_completed(tasks):
                    res = await coro  # 等待每一个具体任务完成
                    if res:
                        all_matched.append(res)

                    # 强行刷新进度条和右侧数值
                    pbar.update(1)
                    pbar.set_postfix({"总命中": len(all_matched)})

        # 批次间休息
        if i < len(batches) - 1 and interval > 0:
            await asyncio.sleep(interval)

    # 3. 收尾
    if finalize_hook:
        finalize_hook(all_matched)

    return all_matched