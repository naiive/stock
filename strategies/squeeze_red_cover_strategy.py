# -*- coding: utf-8 -*-

from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator


def run_strategy(df, symbol, min_red_days=6, max_allowed_greens=2):
    """
    TTM Squeeze 严苛形态筛选策略 - 完整版

    核心逻辑：
    1. 关键K线（当天 T0）：
       - 颜色：亮绿色 (lime)
       - 状态：释放状态 (OFF / 灰白点)
    2. 回溯过程 (T-1 往前回溯)：
       - 必须在挤压状态 (ON / 黑点) 下统计颜色。
       - 红色系 (maroon/red) 累积必须满足至少 6 天。
       - 绿色系 (green/lime) 允许作为“杂质”出现，总量为 0, 1 或 2 天。
    3. 结束条件 (Boundary)：
       - 往前回溯直到遇见第一个 【OFF 释放】 且同时为 【绿色系】。
       - 这代表了上一波多头行情的终点，作为本次回溯的硬边界。
    """
    try:
        # 1. 基础数据安全性检查：至少需要 15-20 根 K 线才能计算指标并有效回溯
        if df is None or len(df) < 20:
            return None

        # 2. 计算 SQZ 指标 (假设返回 df 包含：sqz_hcolor, sqz_status, sqz_hvalue)
        # 颜色定义：'lime': 亮绿, 'green': 暗绿, 'red': 亮红, 'maroon': 暗红
        # 状态定义：'ON': 挤压(黑点), 'OFF': 释放(灰白点)
        df = squeeze_momentum_indicator(df)

        # 3. 判定【当天 T0】是否触发信号
        last = df.iloc[-1]
        curr_color = last.get('sqz_hcolor')
        curr_status = last.get('sqz_status')

        if not (curr_color == 'lime' and curr_status == 'OFF'):
            return None

        # 4. 进入动态回溯逻辑
        red_on_days = 0  # 统计挤压期内的红色天数
        green_on_days = 0  # 统计挤压期内的绿色杂质天数
        total_back_span = 0  # 统计总回溯跨度（天数）
        found_origin = False  # 是否找到了合法的回溯起始点

        # 从昨天(索引 -2)开始倒序回溯
        for i in range(2, len(df)):
            bar = df.iloc[-i]
            b_color = bar.get('sqz_hcolor')
            b_status = bar.get('sqz_status')

            # --- 需求点 3：结束回溯条件 ---
            # 遇到第一个【绿色释放点】停止，这被定义为本轮调整的起点
            if b_status == 'OFF' and b_color in ['green', 'lime']:
                found_origin = True
                break

            # --- 统计计数 ---
            total_back_span += 1

            # 仅统计处于 ON (挤压) 状态下的柱子颜色
            if b_status == 'ON':
                if b_color in ['maroon', 'red']:
                    red_on_days += 1
                elif b_color in ['green', 'lime']:
                    green_on_days += 1

                # --- 需求点 2 (修正)：杂质超过 2 天则失效 ---
                if green_on_days > max_allowed_greens:
                    return None

            # 如果中间遇到红色释放 (OFF + red/maroon)，不计入红绿天数，继续穿透回溯
            else:
                continue

        # 5. 最终形态符合性校验
        # 条件 A: 必须找到了上一波绿色的终点 (found_origin)
        # 条件 B: 红色挤压天数必须满 6 天 (red_on_days)
        if not found_origin or red_on_days < min_red_days:
            return None

        # 6. 命中结果输出
        trade_date = str(last.get('date'))
        return {
            "日期": trade_date,
            "代码": symbol,
            "收盘价": round(float(last.get('close', 0)), 2),
            "回溯总天数": total_back_span,
            "红色挤压天数": red_on_days,
            "绿色杂质天数": green_on_days,
            "形态描述": f"历经 {total_back_span} 天调整(其中红柱挤压{red_on_days}天)，今日首日亮绿释放。"
        }

    except Exception as e:
        return None