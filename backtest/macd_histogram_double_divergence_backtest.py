#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ============================================================
# ATR 计算（Wilder 风格）
# ============================================================
def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df['high']
    low = df['low']
    close = df['close']
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    return tr.ewm(span=period, adjust=False).mean()


# ============================================================
# A 股实盘级回测
# 100 股整数手 + T+1 + ATR 移动止损（无未来函数）
# ============================================================
def backtest_macd_divergence_atr_trailing(
    df: pd.DataFrame,
    signal_col: str = 'macd_bull',
    atr_period: int = 14,
    atr_mult: float = 1.5,
    init_cash: float = 100000.0,
    buy_fee: float = 0.0003,
    sell_fee: float = 0.0003,
    stamp_tax: float = 0.001,
    lot: int = 100
):
    df = df.copy()
    df['atr'] = calc_atr(df, atr_period)

    cash = float(init_cash)
    position = 0
    shares = 0

    entry_price = None
    entry_date = None
    highest_price = None
    stop_price = None

    equity_curve = []
    trades = []

    for i in range(1, len(df)):
        today = df.index[i]
        close_price = float(df.iloc[i]['close'])
        low_price = float(df.iloc[i]['low'])

        signal = bool(df.iloc[i][signal_col])
        atr_yesterday = df.iloc[i - 1]['atr']  # ✅ 只能用昨天 ATR

        # =====================
        # 持仓状态
        # =====================
        if position == 1:
            # 更新最高价（今天盘中已经发生）
            highest_price = max(highest_price, close_price)

            # 计算“昨天就已确定”的新止损价
            new_stop = highest_price - atr_mult * atr_yesterday
            stop_price = max(stop_price, new_stop)

            # T+1：至少持有一天
            can_sell = today > entry_date

            # === 盘中触发止损 ===
            if can_sell and low_price <= stop_price:
                exit_price = stop_price  # ✅ 成交在止损价

                proceeds = shares * exit_price
                fee = proceeds * sell_fee
                tax = proceeds * stamp_tax

                cash += proceeds - fee - tax

                pnl = (
                    (exit_price - entry_price) * shares
                    - entry_price * shares * buy_fee
                    - fee - tax
                )

                trades.append({
                    'ATR倍数': atr_mult,
                    '入场日期': entry_date,
                    '出场日期': today,
                    '入场价': entry_price,
                    '出场价': exit_price,
                    '股数': shares,
                    '盈亏金额': round(pnl, 2),
                    '盈亏比例': f"{(pnl / (entry_price * shares)):.2%}",
                    '出场原因': 'ATR移动止损'
                })

                # 清仓
                position = 0
                shares = 0
                entry_price = None
                entry_date = None
                highest_price = None
                stop_price = None

        # =====================
        # 空仓 → 开仓
        # =====================
        if position == 0 and signal and not np.isnan(atr_yesterday):
            max_lots = int(cash // (close_price * lot))
            qty = max_lots * lot

            if qty > 0:
                cost = qty * close_price
                fee = cost * buy_fee

                cash -= cost + fee

                position = 1
                shares = qty
                entry_price = close_price
                entry_date = today
                highest_price = close_price
                stop_price = close_price - atr_mult * atr_yesterday

        equity_curve.append(cash + shares * close_price)

    return (
        pd.DataFrame(trades),
        pd.Series(equity_curve, index=df.index[1:])
    )


# ============================================================
# 策略统计报告（中文）
# ============================================================
def generate_trade_report(trades: pd.DataFrame, equity: pd.Series):
    if trades.empty:
        return {}

    wins = trades[trades['盈亏金额'] > 0]
    losses = trades[trades['盈亏金额'] <= 0]

    win_rate = len(wins) / len(trades)
    avg_win = wins['盈亏金额'].mean() if not wins.empty else 0
    avg_loss = losses['盈亏金额'].mean() if not losses.empty else 0
    expectancy = win_rate * avg_win + (1 - win_rate) * avg_loss

    peak = equity.cummax()
    drawdown = (equity - peak) / peak

    return {
        '交易次数': len(trades),
        '胜率': f"{win_rate:.2%}",
        '平均盈利': round(avg_win, 2),
        '平均亏损': round(avg_loss, 2),
        '盈亏比': round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else np.inf,
        '期望值': round(expectancy, 2),
        '最大回撤':f"{drawdown.min():.2%}"
    }


# ============================================================
# ATR 参数敏感性分析（明细 + 报告）
# ============================================================
def atr_sensitivity_with_details(
    df_signal: pd.DataFrame,
    atr_mult_list: list,
    init_cash: float = 100000
):
    all_trades = []
    all_reports = []

    for atr_mult in atr_mult_list:
        trades, equity = backtest_macd_divergence_atr_trailing(
            df_signal,
            atr_mult=atr_mult,
            init_cash=init_cash
        )

        if trades.empty:
            continue

        all_trades.append(trades)

        report = generate_trade_report(trades, equity)
        report['ATR倍数'] = atr_mult
        all_reports.append(report)

    return (
        pd.concat(all_trades, ignore_index=True),
        pd.DataFrame(all_reports)
    )


def run_multi_stock_atr_sensitivity(
    symbols: list,
    start_date: str,
    end_date: str,
    atr_mult_list: list,
    init_cash: float = 100000
):
    """
    对多只股票进行 ATR 倍数敏感性分析
    每只股票资金独立
    """

    all_trades = []
    all_reports = []

    for symbol in symbols:
        print(f"\n🚀 回测股票：{symbol}")

        try:
            # ===== 1️⃣ 取数 =====
            df = stock_zh_a_daily_mysql(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust='qfq'
            )

            if df is None or df.empty:
                print(f"⚠️ {symbol} 无有效数据，跳过")
                continue

            # ===== 2️⃣ 生成 MACD 背离信号 =====
            df_signal = macd_histogram_double_divergence_indicator(df)

            # ===== 3️⃣ ATR 参数敏感性 =====
            df_trades, df_report = atr_sensitivity_with_details(
                df_signal,
                atr_mult_list,
                init_cash=init_cash
            )

            if df_trades.empty:
                print(f"⚠️ {symbol} 无交易信号")
                continue

            # ===== 4️⃣ 打股票标签 =====
            df_trades['股票代码'] = symbol
            df_report['股票代码'] = symbol

            all_trades.append(df_trades)
            all_reports.append(df_report)

        except Exception as e:
            print(f"❌ {symbol} 回测失败：{e}")

    # ===== 汇总 =====
    df_trades_all = (
        pd.concat(all_trades, ignore_index=True)
        if all_trades else pd.DataFrame()
    )

    df_report_all = (
        pd.concat(all_reports, ignore_index=True)
        if all_reports else pd.DataFrame()
    )

    return df_trades_all, df_report_all


if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", None)

    from indicators.macd_histogram_double_divergence_indicator import  macd_histogram_double_divergence_indicator

    from scripts.stock_query import stock_zh_a_daily_mysql

    symbols = [
        '002016',
        '600381',
        '600546',
        '603099',
    ]

    atr_mult_list = np.arange(0.8, 3.1, 0.2)

    df_trades_all, df_report_all = run_multi_stock_atr_sensitivity(
        symbols=symbols,
        start_date='20210101',
        end_date='20251230',
        atr_mult_list=atr_mult_list,
        init_cash=100000
    )

    print("\n========== 📋 交易明细 ==========")
    print(df_trades_all)

    print("\n========== 📊 策略统计报告 ==========")
    print(df_report_all)


