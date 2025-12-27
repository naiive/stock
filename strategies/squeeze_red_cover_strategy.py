# -*- coding: utf-8 -*-

from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator


def run_strategy(df, symbol, min_red_days=6, max_allowed_greens=2):
    """
    TTM Squeeze 爆发形态筛选策略 (进阶版)

    逻辑：
    1. 关键K线（当天）：亮绿柱 (lime) + OFF 释放
    2. 回溯过程：要求处于 ON 挤压状态，且：
       - 柱状图主要为红色系 (maroon/red)
       - 允许其中夹杂最多 2 次绿色系 (green/lime)
       - 红色系的总天数必须满足至少 6 天
    3. 结束条件：回溯遇到第一个 OFF (释放点) 或 绿色天数超标
    """
    try:
        # 1. 基础数据量检查
        if df is None or len(df) < (min_red_days + 5):
            return None

        # 2. 计算 SQZ 指标
        df = squeeze_momentum_indicator(df)

        # 3. 判定【关键K线】（当天 T0）
        last = df.iloc[-1]
        if not (last.get('sqz_hcolor') == 'lime' and last.get('sqz_status') == 'OFF'):
            return None

        # 4. 动态回溯逻辑
        red_days = 0  # 红色挤压天数计数
        green_days = 0  # 绿色挤压天数计数
        total_back = 0  # 挤压区总长度

        # 从昨天(索引-2)开始倒序回溯
        for i in range(2, len(df)):
            bar = df.iloc[-i]
            b_color = bar.get('sqz_hcolor')
            b_status = bar.get('sqz_status')

            # 如果状态变为 OFF，说明进入了上一个释放周期，回溯自然结束
            if b_status == 'OFF':
                break

            # 统计 ON 状态下的颜色分布
            if b_status == 'ON':
                if b_color in ['maroon', 'red']:
                    red_days += 1
                elif b_color in ['green', 'lime']:
                    green_days += 1

                # 实时检查：如果挤压期内的绿色天数超过 2 天，该形态不合格
                if green_days > max_allowed_greens:
                    return None

            total_back += 1

        # 5. 最终形态校验
        # 必须满足红色天数至少 6 天
        if red_days < min_red_days:
            return None

        # 6. 结果封装
        return {
            "日期": str(last.get('date')),
            "代码": symbol,
            "当前价": round(float(df['close'].iloc[-1]), 2),
            "回溯详情": f"总挤压{total_back}天 [红柱:{red_days}, 绿柱:{green_days}]",
            "状态": "符合爆发条件"
        }

    except Exception:
        return None