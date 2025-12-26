# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from indicators.squeeze_momentum_indicator import squeeze_momentum_indicator

def run_strategy(
    df,
    symbol,
    lookback = 90,          # P1 最大回溯长度，防止结构过老
    min_dist = 12,          # P1 与 P2 的最小 K 线间隔，防止噪声
    p2_bright_len = 6,      # P2 左侧连续“亮红”数量（确认强下跌）
    p1_valley_win = 5,      # P1 局部波谷确认窗口
    p1_mom_buffer = 1.2     # P1 动能深度必须 ≥ P2 的 1.2 倍
):
    """
    SQZMOM + OBV 底背离【打分系统 · 实盘稳健版】

    策略核心思想：
    1️⃣ 先出现一段「强下跌」（亮红）
    2️⃣ 再进入「下跌衰竭」（暗红）
    3️⃣ 价格创新低，但动能不再创新低
    4️⃣ 下跌过程中 OBV 不再同步走低（资金提前进场）

    返回的是【右侧确认后的底部结构】，不是左侧抄底
    """

    try:
        # ============================================================
        # 一、指标准备
        # ============================================================
        df = squeeze_momentum_indicator(df)
        if len(df) < lookback + 20:
            return None

        # ------------------------------------------------------------
        # OBV 计算（标准定义，不花里胡哨）
        # 上涨：+volume
        # 下跌：-volume
        # 平盘：0
        # ------------------------------------------------------------
        direction = np.where(
            df['close'] > df['close'].shift(1), 1,
            np.where(df['close'] < df['close'].shift(1), -1, 0)
        )
        df['obv'] = (direction * df['volume']).fillna(0).cumsum()

        # 转成 numpy，加快遍历
        moms = df['sqz_hvalue'].values       # SQZMOM 柱子值（动能）
        hcolors = df['sqz_hcolor'].values    # SQZMOM 柱子颜色
        lows = df['low'].values              # 最低价
        obvs = df['obv'].values              # OBV 累积值
        dates = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d').values

        curr_idx = len(df) - 1

        # ============================================================
        # 二、第一阶段：确认 P2（右侧衰竭点）
        # ============================================================
        # 当前柱必须是「暗红」—— 下跌仍在继续，但动能在减弱
        if hcolors[curr_idx] != 'maroon':
            return None

        # P2 定义为暗红柱的前一根
        p2_idx = curr_idx - 1

        # P2 左侧必须是连续的「亮红」—— 确认之前是强下跌
        for i in range(p2_idx, p2_idx - p2_bright_len, -1):
            if i <= 0 or hcolors[i] != 'red':
                return None

        # 记录 P2 信息
        p2_mom = moms[p2_idx]      # 动能
        p2_price = lows[p2_idx]    # 价格
        p2_obv = obvs[p2_idx]      # OBV
        p2_date = dates[p2_idx]

        # ============================================================
        # 三、第二阶段：回溯 P1（左侧恐慌低点）
        # ============================================================
        p1_idx = None

        # 搜索区间：
        # - 至少与 P2 间隔 min_dist
        # - 最多回溯 lookback
        search_end = p2_idx - min_dist
        search_start = max(p1_valley_win, search_end - lookback)

        for i in range(search_end, search_start, -1):
            # 只考虑负动能区域（下跌段）
            if moms[i] < 0:
                # 局部最小值判断（防止噪声）
                window = moms[i - p1_valley_win: i + p1_valley_win + 1]
                if moms[i] == np.min(window):
                    # P1 动能必须明显「更差」
                    # 注意：动能是负数，数值越小越“深”
                    if moms[i] <= (p2_mom * p1_mom_buffer):
                        p1_idx = i
                        break

        if p1_idx is None:
            return None

        # 记录 P1 信息
        p1_mom = moms[p1_idx]
        p1_price = lows[p1_idx]
        p1_obv = obvs[p1_idx]
        p1_date = dates[p1_idx]

        # ============================================================
        # 四、第三阶段：底背离成立条件
        # ============================================================
        # 1️⃣ 价格创新低（或持平）
        # 2️⃣ 动能不再创新低（衰竭）
        if not (p2_price <= p1_price and p2_mom > p1_mom):
            return None

        # ============================================================
        # 五、打分系统（用于排序 & 分级）
        # ============================================================
        score = 0

        # ------------------------------------------------------------
        # 1️⃣ 动能衰减分（核心，Max 40）
        # P1 动能 / P2 动能
        # ------------------------------------------------------------
        mom_ratio = p1_mom / p2_mom
        if mom_ratio >= 2.0:
            score += 40
        elif mom_ratio >= 1.5:
            score += 30
        elif mom_ratio >= 1.2:
            score += 20
        else:
            score += 10

        # ------------------------------------------------------------
        # 2️⃣ OBV 资金分（Max 30）
        # OBV 相对承接强度（尺度无关）
        # 逻辑：下跌过程中资金是否提前进场
        # ------------------------------------------------------------
        avg_vol = df["volume"].iloc[p1_idx:p2_idx].mean()
        obv_strength = (p2_obv - p1_obv) / avg_vol

        if obv_strength >= 2.0:
            score += 30
        elif obv_strength >= 0.5:
            score += 20
        elif obv_strength > -0.5:
            score += 10
        else:
            score += 0

        # ------------------------------------------------------------
        # 3️⃣ 时间结构分（Max 15）
        # ------------------------------------------------------------
        dist = p2_idx - p1_idx
        if 15 <= dist <= 25:
            score += 15
        elif 10 <= dist < 15:
            score += 10
        else:
            score += 5

        # ------------------------------------------------------------
        # 4️⃣ 价格形态分（Max 15）
        # 微破新低最强，深破位要扣分
        # ------------------------------------------------------------
        drop_pct = (p1_price - p2_price) / p1_price * 100
        if 0 <= drop_pct <= 2.5:
            score += 15
        elif drop_pct <= 6:
            score += 5
        else:
            score -= 5           # 深破位，假背离风险

        # ============================================================
        # 六、返回完整结构信息（方便回测 & UI 展示）
        # ============================================================
        # 当前 K 线信息（用于 UI / JSON 输出）
        curr_date = str(dates[curr_idx])
        curr_price = float(df['close'].iloc[curr_idx])

        # 涨幅（对前一根 K）
        if curr_idx > 0:
            prev_close = float(df['close'].iloc[curr_idx - 1])
            pct_change = round((curr_price - prev_close) / prev_close * 100, 2)
        else:
            pct_change = 0.0

        return {
            "日期": curr_date,
            "代码": symbol,
            "当前价": round(float(curr_price), 2),
            "涨幅(%)": float(pct_change),
            "综合得分": score,
            "信号强度": "强烈做多" if score >= 80 else "谨慎做多",
            "动能衰减": mom_display(mom_ratio),
            "资金承接": obv_display_relative(obv_strength),
            "结构间隔": dist_display(p2_idx - p1_idx),
            "破位幅度": price_drop_display(drop_pct),
            "左波峰": {
                "日期": str(p1_date),
                "价格": round(float(p1_price), 2),
                "动能值": round(float(p1_mom), 4),
                "obv": int(p1_obv)
            },
            "右波峰": {
                "日期": str(p2_date),
                "价格": round(float(p2_price), 2),
                "动能值": round(float(p2_mom), 4),
                "obv": int(p2_obv)
            }
        }

    except Exception as e:
        return None

def mom_display(ratio):
    """1️⃣ 动能衰减比（mom_ratio）"""
    r = round(ratio, 2)
    if r >= 2.0:
        return f"【≥2.0】{r} → 恐慌充分释放"
    elif r >= 1.5:
        return f"【1.5–2.0】{r} → 下跌明显衰竭"
    elif r >= 1.2:
        return f"【1.2–1.5】{r} → 下跌开始衰竭"
    else:
        return f"【<1.2】{r} → 下跌未衰竭"

def obv_display_relative(obv_strength):
    """资金承接（obv_strength）"""
    v = round(obv_strength, 2)
    if v >= 2.0:
        return f"【≥2.0】{v} → 强力吸筹"
    elif v >= 0.5:
        return f"【0.5–2.0】{v} → 资金承接"
    elif v > -0.5:
        return f"【-0.5–0.5】{v} → 资金观望"
    else:
        return f"【<-0.5】{v} → 资金流出"

def dist_display(dist):
    """3️⃣ 两次低点间隔（time_dist）"""
    if dist < 10:
        return f"【<10】{dist} → 结构过近（噪声）"
    elif dist < 15:
        return f"【10–15】{dist} → 结构勉强成立"
    elif dist <= 25:
        return f"【15–25】{dist} → 结构健康（理想）"
    else:
        return f"【>25】{dist} → 结构偏老"



def price_drop_display(pct):
    """4️⃣ 二次下探破位幅度（price_drop_pct）"""
    p = round(pct, 2)
    if 0 <= p <= 2:
        return f"【0–2%】{p}% → 诱空假破位"
    elif p <= 5:
        return f"【2–5%】{p}% → 正常破位"
    else:
        return f"【>5%】{p}% → 深度破位（风险）"

