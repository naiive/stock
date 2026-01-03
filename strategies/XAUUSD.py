#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import numpy as np
from enum import Enum
import urllib.request
import urllib.parse
import json
import datetime
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from conf.config import TELEGRAM_CONFIG

######## xau数据 ########
def get_exness_final_data():
    account_id = "232766341"
    url = f"https://rtapi-sl.eccweb.mobi/rtapi/mt5/real5/v2/accounts/{account_id}/instruments/XAUUSDm/candles"
    params = {
        "time_frame": "5",
        "from": "9007199254740991",
        "count": "-300",
        "price": "bid"
    }
    headers = {
        "authority": "rtapi-sl.eccweb.mobi",
        "authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6InVzZXIiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiJmZDgxZWJhNjkwNmU0YzJjYmNkYmIxMGMwN2ZlNDE2MyIsImV4cCI6MTc2NzQ1NDk5NSwiaXNzIjoiQXV0aGVudGljYXRpb24iLCJpYXQiOjE3Njc0MzMzOTUsInN1YiI6IjYxNjFhNmE1NTE0ZTQ1YjRiOWRhNTdmODU0N2UxMDIyIiwiYXVkIjpbIndlYiJdLCJhZGRpdGlvbmFsX3BhcmFtcyI6eyJ3bF9pZCI6Ijg3MTFiOGFhLWNjNjgtNDEzYS04MDM0LWMyNzE2YTJjZTE0YSJ9fQ.MyAO8VjWj2Luq5DgfjeDWI4sKWJ1Wt-TmeorrZ6_bSPG0_tDK8kU18Mu3FDsIuhJi-WRPfaZEmzFNxbZFSBqjtl0QEAqf-Ah-8i4kH9B2x_uz27MI57k0-Qbb7KL55eF73dxDqSSdSoLtZ04ua8U1XE2e4MfROmpQvU_w463hFF7I-s7EFAD85rVc0VB4AYm7ePvM5PAwasVDzyUKUzMMVcSDnVbMEGSk77dCRz4H545V6Hkox2iQsZFeg1Eo7a2lAXj3BEHYlauYxV16HXo0nLGZkJq6b_nSwQKFZw8tpQKsLhbyzQD22Zmxt7P_2TwuSSsZ9lfwcMMPWNoXSlZZMpnhttsuabTJhAzp3KJhcNo9opsk330hoToh-2JQF5WTRCVVkQ0m4H7pT-sef0zegCuYmYNpLg5D1l0xCfEsOV7ALLecPdVhRcinDqQngi8Uz0CGpX1An2SF7OLDZhxkT1kfCxq_4jRFlzQ6wDhijfzzUBQnTvMm_1Mksp_XezGGeDTDDEF307avtz--dqoyUwhBt88jhlSSrfj8oUmUfgu5t82vnyvWSLZVrlK8oLyq-FrATCoIkMuZ36M0thYBUm13pjgr3hYpGfHRvQzM71pw9RtZtSllz6rr3ySGaRyKJeMBEF4EVw3QckzdqxKKNPtxP1PMUs09kwpCqdUy-I",
        "referer": "https://my.exness.com/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "accept": "application/json"
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            res_json = response.json()
            data_list = res_json.get('price_history', [])
            if not data_list:
                print("未获取到 price_history 数据")
                return None
            df = pd.DataFrame(data_list)
            df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
            df['date'] = pd.to_datetime(df['time'], unit='ms') + pd.Timedelta(hours=8)
            df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
            return df
        else:
            print(f"请求失败: {response.status_code}")
            return None
    except Exception as e:
        print(f"解析报错: {e}")
        return None
######## xau数据 ########

######## squeeze指标 ########
class MomentumHistogramColor(Enum):
    BULL_ACCELERATING = "lime"     # 亮绿：多头动能 在增强
    BULL_DECELERATING = "green"    # 暗绿：多头动能 在减弱
    BEAR_ACCELERATING = "red"      # 亮红：空头动能 在增强
    BEAR_DECELERATING = "maroon"   # 暗红：空头动能 在减弱
    NEUTRAL = "neutral"            # 中性
    UNDEFINED = "undefined"        # 数据不足
def tv_linreg(y: pd.Series, length: int) -> float:
    if pd.isna(y).any() or len(y) < length:
        return np.nan
    x = np.arange(length)
    y_vals = y.values[-length:] # 确保只取最新长度
    A = np.vstack([x, np.ones(length)]).T
    try:
        m, b = np.linalg.lstsq(A, y_vals, rcond=None)[0]
        return m * (length - 1) + b
    except:
        return np.nan
def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
def get_squeeze_momentum_histogram_color(val, val_prev):
    if pd.isna(val) or pd.isna(val_prev):
        return MomentumHistogramColor.UNDEFINED.value
    if val > 0:
        return (
            MomentumHistogramColor.BULL_ACCELERATING.value
            if val > val_prev
            else MomentumHistogramColor.BULL_DECELERATING.value
        )
    elif val < 0:
        return (
            MomentumHistogramColor.BEAR_ACCELERATING.value
            if val < val_prev
            else MomentumHistogramColor.BEAR_DECELERATING.value
        )
    else:
        return MomentumHistogramColor.NEUTRAL.value
def add_squeeze_counter(df: pd.DataFrame)-> pd.DataFrame:
    counter = 0
    current_state = None
    sqz_id_list = []
    for status in df["sqz_status"]:
        if status in ["ON", "OFF"]:
            if status == current_state:
                counter += 1
            else:
                current_state = status
                counter = 1
            sqz_id_list.append(counter)
        else:
            current_state = None
            counter = 0
            sqz_id_list.append(0)
    df["sqz_id"] = sqz_id_list
    return df
def squeeze_momentum_indicator( df: pd.DataFrame, length: int = 20, lengthKC: int = 20, multKC: float = 1.2, useTrueRange: bool = True)-> pd.DataFrame:
    df = df.copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame 没有 'date' 列，也没有日期索引，无法设置索引")
    close, high, low = df['close'], df['high'], df['low']
    basis = close.rolling(length).mean()
    dev = multKC * close.rolling(length).std(ddof=0)
    upperBB, lowerBB = basis + dev, basis - dev
    ma = close.rolling(lengthKC).mean()
    r = true_range(df) if useTrueRange else (high - low)
    rangema = r.rolling(lengthKC).mean()
    upperKC, lowerKC = ma + rangema * multKC, ma - rangema * multKC
    sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)
    df["sqz_status"] = np.select([sqzOn, sqzOff], ["ON", "OFF"], default="NO")
    highest_h = high.rolling(lengthKC).max()
    lowest_l = low.rolling(lengthKC).min()
    avg_hl = (highest_h + lowest_l) / 2
    sma_close = close.rolling(lengthKC).mean()
    mid = (avg_hl + sma_close) / 2
    source_mid = close - mid
    histogram_value = source_mid.rolling(lengthKC).apply(lambda x: tv_linreg(pd.Series(x), lengthKC), raw=False)
    df["sqz_hvalue"] = histogram_value
    df["sqz_pre_hvalue"] = histogram_value.shift(1)
    df = add_squeeze_counter(df)
    df["sqz_hcolor"] = df.apply(lambda re: get_squeeze_momentum_histogram_color(re["sqz_hvalue"], re["sqz_pre_hvalue"]), axis=1)
    df.drop(columns=["sqz_pre_hvalue"], inplace=True)
    return df
######## squeeze指标 ########

######## support_resistance指标 ########
def support_resistance_breaks_indicator( df: pd.DataFrame, left_bars: int = 15, right_bars: int = 15):
    df = df.copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame 没有 'date' 列，也没有日期索引，无法设置索引")
    window = left_bars + right_bars + 1
    df['pivot_low_flag'] = (
        df['low']
        .rolling(window)
        .apply(lambda x: 1 if x[left_bars] == np.min(x) else 0, raw=True)
    )
    df['pivot_high_flag'] = (
        df['high']
        .rolling(window)
        .apply(lambda x: 1 if x[left_bars] == np.max(x) else 0, raw=True)
    )
    df['support_raw'] = np.where(
        df['pivot_low_flag'] == 1,
        df['low'].shift(right_bars),
        np.nan
    )
    df['resistance_raw'] = np.where(
        df['pivot_high_flag'] == 1,
        df['high'].shift(right_bars),
        np.nan
    )
    df['srb_support'] = df['support_raw'].ffill()
    df['srb_resistance'] = df['resistance_raw'].ffill()
    df['vol_ema_short'] = df['volume'].ewm(span=5, adjust=False).mean()
    df['vol_ema_long'] = df['volume'].ewm(span=10, adjust=False).mean()
    df['vol_osc'] = 100 * (  (df['vol_ema_short'] - df['vol_ema_long']) / df['vol_ema_long'])
    df.drop(columns=["pivot_low_flag"], inplace=True)
    df.drop(columns=["pivot_high_flag"], inplace=True)
    df.drop(columns=["support_raw"], inplace=True)
    df.drop(columns=["resistance_raw"], inplace=True)
    df.drop(columns=["vol_ema_short"], inplace=True)
    df.drop(columns=["vol_ema_long"], inplace=True)
    df.drop(columns=["vol_osc"], inplace=True)
    return df
######## support_resistance指标 ########

######## squeeze策略 ########
def run_strategy(df, symbol):
    try:
        if df is None or len(df) < 220:
            return None
        current_close = float(df['close'].iloc[-1])
        prev_close = float(df['close'].iloc[-2])
        pct_chg = (current_close - prev_close) / prev_close * 100
        ema200_series = df['close'].rolling(200).mean()
        ema200_val = ema200_series.iloc[-1]
        df = support_resistance_breaks_indicator(df)
        last_srb = df.iloc[-1]
        srb_resistance = pd.to_numeric(last_srb.get('srb_resistance'), errors='coerce')
        srb_support = pd.to_numeric(last_srb.get('srb_support'), errors='coerce')
        df = squeeze_momentum_indicator(df)
        current = df.iloc[-1]
        prev = df.iloc[-2]
        prev_sqz_id = pd.to_numeric(prev.get('sqz_id'), errors='coerce')
        sqz_status = current.get('sqz_status')
        prev_status = prev.get('sqz_status')
        sqz_hcolor = current.get('sqz_hcolor')
        signal = None
        break_trend = None
        if (
            sqz_status == 'OFF'
            and prev_status == 'ON'
            and prev_sqz_id >= 6
            and sqz_hcolor == 'lime'
        ):
            if current_close > ema200_val:
                b_break_trend_list = []
                for b_i in range(6, 0, -1):
                    b_close_i = df['close'].iloc[-(b_i + 1)]
                    b_trend_i = "高" if b_close_i > srb_resistance else "低"
                    b_break_trend_list.append(b_trend_i)
                break_trend = "-".join(b_break_trend_list)
                signal = "buy"

        if (
            sqz_status == 'OFF'
            and prev_status == 'ON'
            and prev_sqz_id >= 6
            and sqz_hcolor == 'red'
        ):
            if current_close < ema200_val:
                s_break_trend_list = []
                for s_i in range(6, 0, -1):
                    s_close_i = df['close'].iloc[-(s_i + 1)]
                    s_trend_i = "高" if s_close_i > srb_support else "低"
                    s_break_trend_list.append(s_trend_i)
                break_trend = "-".join(s_break_trend_list)
                signal = "sell"
        raw_colors = df['sqz_hcolor'].iloc[-7:-1].tolist()
        raw_values = df['sqz_hvalue'].iloc[-7:-1].tolist()
        color_value_list = []
        for i in range(6):
            raw_color = raw_colors[i] if i < len(raw_colors) else None
            raw_value = raw_values[i] if i < len(raw_values) else None
            squeeze_color_map = {
                'lime': '绿亮',
                'green': '绿暗',
                'red': '红亮',
                'maroon': '红暗'
            }
            color_str = squeeze_color_map.get(raw_color, 'NA')
            if raw_value is None or np.isnan(raw_value):
                value_str = "NA"
            else:
                if raw_value > 0:
                    value_str = f"+{raw_value:.2f}"
                elif raw_value < 0:
                    value_str = f"-{abs(raw_value):.2f}"
                else:
                    value_str = f"{raw_value:.2f}"
            color_value_list.append(f"{color_str}[{value_str}]")
        color_value_cols = "-".join(color_value_list)
        if signal:
            return {
                "日期": current.name.strftime('%Y-%m-%d %H:%M:%S'),
                "代码": symbol,
                "信号": signal,
                "现价": round(current_close, 2),
                "涨幅(%)": round(pct_chg, 2),
                "挤压天数": int(prev_sqz_id),
                "动能情况": color_value_cols,
                "突破趋势": break_trend
            }
        else:
            return None
    except Exception:
        return None
######## squeeze策略 ########

# ######## telegram ########
def format_signal_message(data: dict) -> str:
    if data is None:
        return "错误：数据为空，无法生成信号消息。"
    momentum_str = data.get("动能情况", "")
    momentum_list = momentum_str.split("-") if momentum_str else []
    def momentum_to_emoji(item):
        if "绿" in item:
            return "🟢"
        elif "红" in item:
            return "🔴"
        else:
            return "❓"
    momentum_emoji = "".join(momentum_to_emoji(i) for i in momentum_list) if momentum_list else "NA"
    trend_str = data.get("突破趋势", "")
    trend_list = trend_str.split("-") if trend_str else []
    def trend_to_emoji(item):
        if item == "高":
            return "⬆️"
        elif item == "低":
            return "⬇️"
        else:
            return "❓"
    trend_emoji = "".join(trend_to_emoji(i) for i in trend_list) if trend_list else "NA"
    signal_map = {"buy": "买入", "sell": "卖出"}
    signal_cn = signal_map.get(data.get("信号", "").lower(), data.get("信号", ""))
    message = (
        f"📊 黄金信号通知\n"
        f"------------------------\n"
        f"🗓 日期: {data.get('日期', '未知')}\n"
        f"💹 代码: {data.get('代码', '未知')}\n"
        f"⚡️ 信号: {signal_cn}\n"
        f"💰 现价: {data.get('现价', '未知')}\n"
        f"📈 涨幅: {data.get('涨幅(%)', '未知')}%\n"
        f"⏱️ 挤压: {data.get('挤压天数', '未知')}\n"
        f"📊 动能 {momentum_emoji}\n"
        f"🚀 趋势 {trend_emoji}"
    )

    return message
def _http_post_form(url: str, data: dict) -> dict:
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=encoded)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))
def send_telegram( bot_token: str, chat_id: str, text: str, disable_web_page_preview: bool = True, parse_mode: str = "HTML",) -> bool:
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
            "parse_mode": parse_mode,
        }
        resp = _http_post_form(url, payload)
        if not resp.get("ok"):
            raise RuntimeError(resp)
        print("🤖 Telegram sent")
        return True
    except Exception as e:
        print(f"⚠️ Telegram failed: {e}")
        return False
# ######## telegram ########

# ######## 定时任务 ########
def start_xau_robot():
    """启动 XAUUSD 5分钟定时策略监控函数"""
    symbol = "XAUUSD"
    tz = pytz.timezone('Asia/Shanghai')

    def is_market_open():
        """判断当前是否在黄金交易时段 (自动处理夏/冬令时)"""
        now = datetime.datetime.now(tz)
        weekday = now.weekday()  # 0=周一, 5=周六, 6=周日
        hour = now.hour
        # 简单判定夏令时逻辑 (3月-11月大致为夏令时)
        is_dst = 3 <= now.month <= 11
        close_hour = 5 if is_dst else 6  # 夏令时5点关盘，冬令时6点
        open_hour = 6 if is_dst else 7   # 夏令时6点开盘，冬令时7点
        if weekday == 5 and hour >= close_hour: return False  # 周六收盘后
        if weekday == 6: return False  # 周日全天
        if weekday == 0 and hour < open_hour: return False    # 周一开盘前
        return True
    def job():
        """单次执行的任务"""
        now = datetime.datetime.now(tz)
        now_str = now.strftime('%H:%M:%S')

        # 强制打印一条心跳，证明定时器在动
        print(f"[{now_str}] 定时器触发成功，正在检查市场状态...", flush=True)

        if not is_market_open():
            print(f"[{now_str}] 判定结果：当前处于休市时段。", flush=True)
            return

        try:
            df = get_exness_final_data()
            if df is not None:
                df_sqz = squeeze_momentum_indicator(df)
                res_df = run_strategy(df_sqz, symbol)
                if res_df:
                    send_telegram(bot_token=TELEGRAM_CONFIG.get("BOT_TOKEN"), chat_id=TELEGRAM_CONFIG.get("CHAT_ID"), text=format_signal_message(res_df))
                    print(f"[{now_str}] 信号已发送！", flush=True)
                else:
                    print(f"[{now_str}] 扫描完毕：当前无交易信号。", flush=True)
        except Exception as e:
            print(f"[{now_str}] 运行时报错: {e}", flush=True)
    # 2. 定时器配置
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'cron', minute='*/5', timezone=tz)
    print(f"黄金监控已启动！当前北京时间: {datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("监控已停止")
# ######## 定时任务 ########


if __name__ == "__main__":
    start_xau_robot()