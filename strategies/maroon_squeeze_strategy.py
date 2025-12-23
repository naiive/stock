# -*- coding: utf-8 -*-

import datetime
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(df, symbol, min_bright_red_days=5):
    """
    TTM Squeeze 严苛形态筛选策略

    逻辑：
    1. 关键K线（今天）：必须是【深红 maroon】且轴线为【黑点 ON】。
    2. 回溯过程：从昨天起往回数，必须全部是【亮红 red】且轴线为【黑点 ON】。
    3. 动能要求：亮红期间动能柱值 (sqz_hvalue) 必须【严格连续递增】。
    4. 终止条件：回溯直到遇到【亮绿 lime】或【深绿 green】且【黑点 ON】为止。
    5. 杂质判定：回溯途中若出现深红、OFF状态或非递增，直接剔除。
    """
    try:
        # 1. 基础数据量检查
        if df is None or len(df) < 30:
            return None

        # 2. 计算 SQZ 指标 (请确保该指标返回 sqz_hcolor, sqz_status, sqz_hvalue)
        df = squeeze_momentum_indicator(df)

        # 3. 判定【关键K线】（今天）
        last = df.iloc[-1]
        curr_color = last.get('sqz_hcolor')
        curr_status = last.get('sqz_status')
        curr_val = last.get('sqz_hvalue', 0)

        # 必须是深红且在挤压中
        if not (curr_color == 'maroon' and curr_status == 'ON'):
            return None

        # 4. 严苛动态回溯：统计连续亮红天数
        momentum_bars = []

        # 从昨天(索引-2)开始倒序回溯
        for i in range(2, len(df)):
            bar = df.iloc[-i]
            b_color = bar.get('sqz_hcolor')
            b_status = bar.get('sqz_status')
            b_val = bar.get('sqz_hvalue', 0)

            # 情况A：如果是【亮红+黑点】，记录并继续回溯
            if b_color == 'red' and b_status == 'ON':
                momentum_bars.append(b_val)
                continue

                # 情况B：如果是【绿柱+黑点】，视为本波行情起点，正常结束回溯
            elif (b_color == 'green') and b_status == 'ON':
                break

            # 情况C：出现任何杂质（其他颜色或轴线变色），直接返回None
            else:
                return None

        # 5. 最终形态校验
        # A. 连续亮红天数是否达到配置要求
        if len(momentum_bars) < min_bright_red_days:
            return None

        # 6. 命中结果封装
        trade_date = str(last.get('date'))

        return {
            "日期": trade_date,
            "代码": symbol,
            "当前价": round(float(df['close'].iloc[-1]), 2),
            "连续亮红天数": len(momentum_bars),
            "今日深红值": round(curr_val, 4),
            "形态描述": f"检测到连续 {len(momentum_bars)} 天亮红挤压增长后，首日转深红"
        }

    except Exception:
        # 捕获异常，确保扫描器不因单只股票数据问题崩溃
        return None