# -*- coding: utf-8 -*-

from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(df, symbol, min_red_days=6, max_allowed_greens=2):
    """
    TTM Squeeze 纯净挤压爆发策略 (严苛版)

    逻辑：
    1. 当天 (T0)：动能值 > 0, 颜色为亮绿 (Lime), 状态为释放 (OFF/白点)。
    2. 回溯过程：从昨日往前，必须全程为挤压状态 (ON/黑点)，绝对禁止出现释放 (OFF)。
    3. 蓄能要求：ON状态下的红柱天数 >= 6，允许夹杂 0-2 天绿柱杂质。
    4. 起始边界：回溯直到遇见第一个“绿色释放点 (OFF + Green/Lime)”。
    """
    try:
        # 1. 基础数据安全性检查
        if df is None or len(df) < 25:
            return None

        # 2. 计算 SQZ 指标 (获取颜色、状态、动能值)
        # 返回字段映射：sqz_hcolor (颜色), sqz_status (状态), sqz_hvalue (数值)
        df = squeeze_momentum_indicator(df)

        # 3. 【当天触发点判定 T0】
        last = df.iloc[-1]
        curr_hval = last.get('sqz_hvalue', 0)
        curr_color = last.get('sqz_hcolor')
        curr_status = last.get('sqz_status')

        # 严苛触发：动能必须站上0轴 且 为亮绿色 且 为首日OFF释放
        if not (curr_hval > 0 and curr_color == 'lime' and curr_status == 'OFF'):
            return None

        # 4. 初始化回溯计数变量
        red_on_days = 0  # 统计挤压状态下的红柱
        green_on_days = 0  # 统计挤压状态下的绿柱
        momentum_sum = 0.0  # 能量池累加值
        total_back_span = 0  # 统计总天数
        found_origin = False
        origin_idx = -1

        # 5. 【严苛回溯过程判定】 (从 T-1 倒序往历史回溯)
        for i in range(2, len(df)):
            bar = df.iloc[-i]
            b_color = bar.get('sqz_hcolor')
            b_status = bar.get('sqz_status')
            b_val = bar.get('sqz_hvalue', 0)

            # --- A. 结束边界判定 ---
            # 遇见第一个“绿色释放点”则视为找到了蓄能周期的起点
            if b_status == 'OFF' and b_color in ['green', 'lime']:
                found_origin = True
                origin_idx = len(df) - i
                break

            # --- B. 严苛状态过滤 ---
            # 需求：除了起始边界点，中间回溯过程必须全部是 ON (黑点)
            # 一旦发现 OFF 释放点，该形态被视为不够纯净，直接剔除
            if b_status == 'OFF':
                return None

                # --- C. 数据统计 (此时 b_status 确定为 ON) ---
            total_back_span += 1
            momentum_sum += b_val  # 累加蓄能期间的动能

            if b_color in ['maroon', 'red']:
                red_on_days += 1
            elif b_color in ['green', 'lime']:
                green_on_days += 1
                # 绿色杂质超过2天则形态不合格
                if green_on_days > max_allowed_greens:
                    return None

        # 6. 【形态达标校验】
        # 必须找到绿色起点，且红柱蓄能时间达到 6 天及以上
        if not found_origin or red_on_days < min_red_days:
            return None

        # 7. 【量价维度计算】
        # 当日涨幅计算
        prev_close = df['close'].iloc[-2]
        curr_close = last['close']
        daily_change = ((curr_close - prev_close) / prev_close) * 100

        # 区间蓄能期涨跌幅计算 (从边界点到爆发前一日)
        origin_price = df['close'].iloc[origin_idx]
        before_breakout_price = df['close'].iloc[-2]
        period_change = ((before_breakout_price - origin_price) / origin_price) * 100

        # 8. 【终极结果封装】
        return {
            "日期": str(last.get('date')),
            "代码": symbol,
            "现价": round(float(curr_close), 2),
            "涨幅(%)": round(daily_change, 2),
            "今日动能值": round(curr_hval, 4),
            "蓄能形态质量": {
                "状态校验": "全程纯净 ON 挤压",
                "红柱蓄能天数": f"{red_on_days}天",
                "绿柱干扰天数": f"{green_on_days}天",
                "区间价格变动(%)": round(period_change, 2),
                "蓄能池总量": round(momentum_sum, 4),
                "平均动能压制": round(momentum_sum / total_back_span, 4) if total_back_span > 0 else 0
            },
            "形态评分": self_define_score(daily_change, period_change, red_on_days),
            "交易语义": f"该股经历 {total_back_span} 天纯黑点挤压后，今日动能站上0轴并首次变绿释放。"
        }

    except Exception:
        return None

def self_define_score(daily_change, period_change, red_days):
    """辅助评分函数：根据形态质量给出星级"""
    score = 1
    if red_days >= 10: score += 1  # 蓄能时间长
    if -3 < period_change < 2: score += 2  # 横盘抗跌最强
    if daily_change > 3: score += 1  # 爆发力度强
    return "★" * min(score, 5)