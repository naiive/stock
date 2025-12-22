# -*- coding: utf-8 -*-
"""
Module: AsyncDispatcher
Description: 高性能异步并发调度工具 (Async IO + ThreadPoolExecutor)。
设计理念：
    1. 职责分离：只负责并发任务调度、批次控制与进度展示，不介入任何业务计算逻辑。
    2. 混合架构：利用异步(Asyncio)管理非阻塞调度，利用线程池(ThreadPool)处理密集计算，防止阻塞主循环。
    3. 鲁棒性：支持预处理钩子(Prepare Hook)和收尾钩子(Finalize Hook)，适配多种扫描场景。
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
        finalize_hook: Optional[Callable[[List[Any]], None]] = None,
        desc: str = "任务名称"
) -> List[Any]:
    """
    通用异步分批调度器

    Args:
        symbols (Sequence[str]): 待处理的任务标识列表（如股票代码列表）。
        worker_func (Callable): 核心工作函数，接收单个 symbol，返回结果或 None。运行在线程池中。
        prepare_hook (Optional[Callable]): 【钩子】执行前的准备工作函数（如预取实时行情快照）。
        prepare_msg (str): 预处理时的提示信息。
        finalize_hook (Optional[Callable]): 【钩子】全部完成后执行的函数（如结果导出、发送通知）。
        desc (str): 进度条左侧显示的描述文字。

    Returns:
        List[Any]: 汇总后的所有命中结果列表。
    """

    # 0. 空值检查：若无任务列表，直接返回空结果，避免后续逻辑空转
    if not symbols:
        return []

    # 1. 【预处理阶段】：在并发任务开始前，执行特定的环境准备工作
    if prepare_hook:
        # print(f"🛠️ [系统] {prepare_msg}")
        # 兼容性处理：支持异步(await)和同步(call)两种形态的钩子函数
        if asyncio.iscoroutinefunction(prepare_hook):
            await prepare_hook()
        else:
            prepare_hook()

    # 2. 【分批策略】：将海量任务切分为固定大小的批次(Batch)
    # 目的：控制瞬时内存占用，避免线程池过载，同时防止被数据源接口判定为恶意爬虫
    batch_size = SYSTEM_CONFIG.get("BATCH_SIZE", 500)
    batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    all_matched = []  # 全局汇总：存储所有批次中命中策略的结果
    max_workers = SYSTEM_CONFIG.get("MAX_WORKERS", 10)  # 并发线程数限制
    interval = SYSTEM_CONFIG.get("BATCH_INTERVAL_SEC", 1)  # 批次间的强制休息时间(秒)
    loop = asyncio.get_running_loop()  # 获取当前主线程的异步事件循环句柄

    # 3. 【批次循环】：逐个批次提交并监控执行情况
    for i, batch in enumerate(batches):
        # 打印当前批次的状态，使用 \n 确保与上一个进度条有视觉间隔
        print(f"\n📦 正在执行第 {i + 1}/{len(batches)} 批次 (规模: {len(batch)})")

        # 使用上下文管理器启动线程池，确保批次结束时资源能被正确回收
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 将同步的 worker_func 提交给线程池执行，并包装为异步 Task (Future 对象)
            # loop.run_in_executor 是连接 Async 和 ThreadPool 的桥梁
            tasks = [loop.run_in_executor(executor, worker_func, s) for s in batch]

            # 4. 【进度监控】：使用 tqdm 实时渲染进度条
            # dynamic_ncols=True 让进度条自动根据窗口宽度伸缩，解决文字溢出导致的换行问题
            with tqdm(total=len(tasks), desc=f" > {desc}", dynamic_ncols=True, leave=True) as pbar:
                pbar.set_postfix({"累计命中": len(all_matched)})

                # asyncio.as_completed：谁先算完就先返回谁，保证进度条响应的实时性
                for coro in asyncio.as_completed(tasks):
                    res = await coro  # 等待具体任务返回结果字典或 None
                    if res:
                        all_matched.append(res)

                    # 手动更新进度条（步进 1），并同步更新右侧的全局命中计数
                    pbar.update(1)
                    pbar.set_postfix({"累计命中": len(all_matched)})

        # 5. 【流控机制】：批次间非阻塞休息，释放 CPU 压力，防止网络 IO 拥塞
        if i < len(batches) - 1 and interval > 0:
            await asyncio.sleep(interval)

    # 6. 【收尾阶段】：将全量结果交给收尾钩子处理（如落盘、通知、分析）
    if finalize_hook:
        # 这里的 finalize_hook 需要定义为接收一个 list 参数的函数
        finalize_hook(all_matched)

    # 返回所有收集到的信号结果，供调用者进一步处理
    return all_matched