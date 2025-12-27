# -*- coding: utf-8 -*-

import pandas as pd
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator


def run_strategy(df, symbol, min_red_days=6, max_allowed_greens=2):
    """
    TTM Squeeze 严苛形态筛选策略 (深度封装版)

    该策略旨在捕获“弹簧效应”：
    即股价经历了一段长时间（>=6天）的低波挤压蓄能，
    且该蓄能区间紧跟在上一波上涨结束之后，
    今日首次出现动能由负转正并释放的“起爆点”。
    """
    try:
        # --- 1. 数据预检查 ---
        # 考虑到回溯至少6天+起始边界，数据量少于25根K线无法计算完整的形态
        if df is None or len(df) < 25:
            return None

        # --- 2. 指标计算 ---
        # 调用底层函数计算 SQZ 指标。
        # sqz_hcolor: 柱体颜色 (lime/green/red/maroon)
        # sqz_status: 挤压状态 (ON:黑点/正在挤压, OFF:白点/动能释放)
        # sqz_hvalue: 动能柱绝对值 (正数为多头，负数为多头衰减或空头)
        df = squeeze_momentum_indicator(df)

        # --- 3. 判定触发点 (T0/当天) ---
        last = df.iloc[-1]
        curr_color = last.get('sqz_hcolor')
        curr_status = last.get('sqz_status')

        # 需求点：当天必须是亮绿柱(lime)且处于释放状态(OFF)
        # 逻辑含义：能量积攒到临界点，今天正式向上“破局”释放
        if not (curr_color == 'lime' and curr_status == 'OFF'):
            return None

        # --- 4. 核心变量初始化 ---
        red_on_days = 0  # 累计在挤压状态(ON)下的红色/暗红色天数
        green_on_days = 0  # 累计在挤压状态(ON)下的绿色/暗绿色天数 (作为干扰杂质统计)
        momentum_sum = 0.0  # 统计挤压期间所有动能柱的数值总和，用于量化蓄能厚度
        total_back_span = 0  # 统计从今天到回溯起点的总日历天数
        found_origin = False  # 逻辑开关：是否成功定位到“上一波上涨的终点”
        origin_idx = -1  # 记录回溯终点的索引位置，用于后续计算区间跌幅

        # --- 5. 动态回溯判定逻辑 (从 T-1 往前找) ---
        # 循环从昨天(index -2)开始，一直往历史方向探测
        for i in range(2, len(df)):
            bar = df.iloc[-i]
            b_color = bar.get('sqz_hcolor')
            b_status = bar.get('sqz_status')
            b_val = bar.get('sqz_hvalue', 0)

            # A. 判定边界条件 (Boundary Condition)
            # 需求点：遇到第一个【OFF释放】且颜色为【绿色系】时停止回溯
            # 逻辑含义：这代表我们找到了本轮调整之前的“那一波上涨的末尾”
            if b_status == 'OFF' and b_color in ['green', 'lime']:
                found_origin = True
                origin_idx = len(df) - i
                break

            # B. 过程统计
            total_back_span += 1
            momentum_sum += b_val  # 持续累加动能值，负值越小代表调整越深

            # C. 统计挤压期(ON)内的颜色分布
            if b_status == 'ON':
                if b_color in ['maroon', 'red']:
                    red_on_days += 1
                elif b_color in ['green', 'lime']:
                    green_on_days += 1

                # 容错机制：如果挤压期内出现的绿色天数超过 2 天，说明调整不纯粹，直接剔除
                if green_on_days > max_allowed_greens:
                    return None
            else:
                # 如果遇到红色/暗红色的 OFF 释放，通常代表下跌加速，我们选择“穿透”它继续往回找
                continue

        # --- 6. 最终形态符合性校验 ---
        # 必须满足两个硬性指标：
        # 1. 成功回溯到了绿色起点 (确保不是在阴跌泥潭中)
        # 2. 蓄能红柱天数必须达标 (确保蓄力足够久)
        if not found_origin or red_on_days < min_red_days:
            return None

        # --- 7. 涨幅与能量维度计算 ---
        # 计算当日涨幅：今天的收盘价相对于昨天的变化
        prev_close = df['close'].iloc[-2]
        curr_close = last['close']
        daily_change = ((curr_close - prev_close) / prev_close) * 100

        # 计算区间涨跌幅：从回溯起点价格到爆发前一天的价格
        # 逻辑含义：判断蓄能期是“横盘强势震荡”还是“向下深幅回调”
        origin_price = df['close'].iloc[origin_idx]
        before_breakout_price = df['close'].iloc[-2]
        period_change = ((before_breakout_price - origin_price) / origin_price) * 100

        # --- 8. 形态综合评分逻辑 (1-5星) ---
        score = 1
        if red_on_days >= 8: score += 1  # 挤压超过8天，属于“长程蓄力”，加1星
        if -3 < period_change < 1:
            score += 2  # 股价在高位横着走，极度强势，加2星
        elif period_change > 1:
            score += 1  # 边涨边挤压，也属于强势，加1星
        if momentum_sum > -0.2: score += 1  # 动能柱虽然是红的但很浅，说明空头极弱，加1星

        # --- 9. 详细结果输出封装 ---
        return {
            "核心数据": {
                "代码": symbol,
                "分析日期": str(last.get('date')),
                "当前价": round(float(curr_close), 2),
                "涨幅(%)": f"{round(daily_change, 2)}%",
                "形态综合评分": "★" * min(score, 5)
            },
            "蓄能质量分析": {
                "红色挤压天数": f"{red_on_days}天",
                "绿色杂质天数": f"{green_on_days}天",
                "回溯总长度": f"{total_back_span}天",
                "区间涨跌幅(%)": f"{round(period_change, 2)}%",
                "能量池累加值": round(momentum_sum, 4),  # 越接近0说明压制越轻
                "单日平均动能": round(momentum_sum / total_back_span, 4)
            },
            "逻辑判定细节": {
                "今日状态": "亮绿动能释放 (Lime OFF)",
                "边界点日期": str(df['date'].iloc[origin_idx]),
                "边界点状态": "前序多头释放终点 (Confirmed)"
            },
            "实战建议": (
                "【极强形态】属于高位横盘后二次起爆，胜率较高" if score >= 4 else
                "【标准形态】回踩洗盘后的正常转势信号" if score >= 2 else
                "【弱势信号】动能偏弱，建议观察是否有成交量配合"
            )
        }

    except Exception as e:
        return None