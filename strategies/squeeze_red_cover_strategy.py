# -*- coding: utf-8 -*-

from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(df, symbol, min_red_days=6, max_allowed_greens=2):
    """
    TTM Squeeze 终极严苛纯净版策略（实盘稳定版）

    核心逻辑：
    T0：
        - 动能 > 0
        - 颜色 = lime
        - 状态 = OFF（释放）
    回溯：
        - 全程必须 ON（黑点）
        - 绿色 ON 仅允许出现在最近 2 根
        - 红柱 ON 累计 >= min_red_days
    起点：
        - 第一个【绿色系】且【动能 >= 0】的柱（ON / OFF 均可）
    """

    # ─────────────────────────
    # 1. 基础安全检查
    # ─────────────────────────
    if df is None or len(df) < 25:
        return None

    # ─────────────────────────
    # 2. 计算 SQZ 指标
    # ─────────────────────────
    df = squeeze_momentum_indicator(df)

    # 必须字段存在
    required_cols = ['close', 'sqz_hvalue', 'sqz_hcolor', 'sqz_status']
    for col in required_cols:
        if col not in df.columns:
            return None

    # ─────────────────────────
    # 3. 今日触发点 T0
    # ─────────────────────────
    last = df.iloc[-1]

    curr_hval   = float(last['sqz_hvalue'])
    curr_color  = last['sqz_hcolor']
    curr_status = last['sqz_status']

    if not (curr_hval > 0 and curr_color == 'lime' and curr_status == 'OFF'):
        return None

    # ─────────────────────────
    # 4. 回溯初始化
    # ─────────────────────────
    red_on_days     = 0
    green_on_days   = 0
    momentum_sum    = 0.0
    total_back_span = 0

    found_origin = False
    origin_idx   = -1

    # ─────────────────────────
    # 5. 严苛回溯（T-1 → 历史）
    # ─────────────────────────
    for i in range(2, len(df)):
        bar = df.iloc[-i]

        b_color  = bar['sqz_hcolor']
        b_status = bar['sqz_status']
        b_val    = float(bar['sqz_hvalue'])

        # 回溯步数先 +1（修复 off-by-one）
        total_back_span += 1

        # ── A. 起点判定（>=0，ON / OFF 都行）
        if b_color in ['green', 'lime'] and b_val >= 0:
            found_origin = True
            origin_idx = len(df) - i
            break

        # ── B. 纯净度：中途任何 OFF 直接失败
        if b_status == 'OFF':
            return None

        # ── C. 绿色干扰限制（仅最近 2 根）
        if b_color in ['green', 'lime']:
            if total_back_span > 2:
                return None
            green_on_days += 1
            if green_on_days > max_allowed_greens:
                return None

        # ── D. 红柱蓄能（仅 ON）
        if b_color in ['maroon', 'red'] and b_status == 'ON':
            red_on_days += 1
            momentum_sum += abs(b_val)

    # ─────────────────────────
    # 6. 最终形态校验
    # ─────────────────────────
    if not found_origin:
        return None

    if red_on_days < min_red_days:
        return None

    if origin_idx <= 0 or origin_idx >= len(df) - 1:
        return None

    # ─────────────────────────
    # 7. 性能计算
    # ─────────────────────────
    prev_close = float(df['close'].iloc[-2])
    curr_close = float(last['close'])

    daily_change = (curr_close - prev_close) / prev_close * 100

    origin_price = float(df['close'].iloc[origin_idx])
    before_breakout_price = prev_close

    period_change = (before_breakout_price - origin_price) / origin_price * 100

    # 日期安全处理
    analysis_date = (
        str(last['date'])
        if 'date' in df.columns
        else str(df.index[-1])
    )

    origin_date = (
        str(df['date'].iloc[origin_idx])
        if 'date' in df.columns
        else str(df.index[origin_idx])
    )

    # ─────────────────────────
    # 8. 结果输出
    # ─────────────────────────
    return {
        "日期": analysis_date,
        "代码": symbol,
        "当前价": round(curr_close, 2),
        "涨幅(%)": f"{round(daily_change, 2)}%",
        "起爆动能值": round(curr_hval, 4),

        "蓄能详情": {
            "红柱挤压天数": f"{red_on_days}天",
            "绿色干扰天数": f"{green_on_days}天(仅限前2日)",
            "全程挤压校验": "通过（纯 ON 黑点区）",
            "区间价格表现(%)": f"{round(period_change, 2)}%",
            "能量池累加": round(momentum_sum, 4),
            "平均压制力度": round(momentum_sum / red_on_days, 4) if red_on_days > 0 else 0
        },
        "退出边界点": {
            "起点日期": origin_date,
            "起点状态": df['sqz_status'].iloc[origin_idx],
            "起点动能": round(float(df['sqz_hvalue'].iloc[origin_idx]), 4)
        },
        "形态综合判定": (
            "空中加油（强势）" if -3 < period_change < 1 else
            "洗盘蓄能（标准）" if period_change <= -3 else
            "边涨边压（谨慎）"
        )
    }
