#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import numpy as np
import pandas as pd
import asyncio
from aiohttp import web
import aiohttp
import logging
from datetime import datetime, timedelta
import time
from typing import Dict, Optional, Any

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
WECOM_WEBHOOK = os.getenv("WECOM_WEBHOOK")
EXNESS_BASE_URL = os.getenv("EXNESS_BASE_URL")
AUTHORIZATION_TOKEN = os.getenv("AUTHORIZATION_TOKEN")
# TZ -> Asia/Shanghai
TZ = os.getenv("TZ")

CONFIG = {
    "watch_list" : ["XAUUSDm", "TSLAm", "AAPLm", "NVDAm", "AMZNm"],

    # 监听的时间周期
    "intervals": ["5M"],

    "api": {
        "EXNESS_BASE_URL": EXNESS_BASE_URL,
        "AUTHORIZATION_TOKEN": AUTHORIZATION_TOKEN,
        "MAX_CONCURRENT": 2,
    },

    "strategy": {
        "bb_length": 20,
        "bb_mult": 2.0,
        "kc_length": 20,
        "kc_mult": 1.2,
        "use_true_range": True,
        "min_sqz_bars": 6,

        "ema_length": 200,

        "srb_left": 15,
        "srb_right": 15
    },

    "time": {
        "market_groups": {
            "forex_gold": ["XAU", "OIL", "USD", "EUR", "GBP"],
            "us_stocks": ["TSLA", "AAPL", "NVDA", "MSFT", "AMZN", "META"]
        }
    },

    "notify": {
        "CONSOLE_LOG": True,
        "WECOM_ENABLE": True,
        "TG_ENABLE": False,

        "WECOM_WEBHOOK":WECOM_WEBHOOK,
        "TG_TOKEN": TG_TOKEN,
        "TG_CHAT_ID": TG_CHAT_ID
    }
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DataEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.url = cfg.get('EXNESS_BASE_URL')
        self.authorization_token = cfg.get("AUTHORIZATION_TOKEN")

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str) -> Optional[pd.DataFrame]:
        url = self.url + f"/{symbol}/candles"
        params = {
            "time_frame": "5",
            "from": "9007199254740991",
            "count": "-300",
            "price": "bid"
        }
        headers = {
            "authority": "rtapi-sl.eccweb.mobi",
            "authorization": self.authorization_token,
            "referer": "https://my.exness.com/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "accept": "application/json"
        }
        try:
            async with session.get(url, params=params, headers=headers, timeout=10) as r:
                res = await r.json()
                data = res.get('price_history', [])
                if not data:
                    logger.error("未获取到 enxesss 接口数据，或许token失效")
                    return None
                df = pd.DataFrame(data)
                df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
                df['date'] = pd.to_datetime(df['time'], unit='ms') + pd.Timedelta(hours=8)
                df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
                df.set_index('date', inplace=True)
                return df

        except Exception as e:
            logger.error(f"未获取到 enxesss 接口数据: {e}")
            return None

class IndicatorEngine:
    def __init__(self, st_cfg: dict):
        self.cfg = st_cfg

    @staticmethod
    def tv_linreg(series: pd.Series, length: int):
        if pd.isna(series).any() or len(series) < length:
            return np.nan
        x = np.arange(length)
        y_vals = series.values[-length:]
        A = np.vstack([x, np.ones(length)]).T
        try:
            m, b = np.linalg.lstsq(A, y_vals, rcond=None)[0]
            return m * (length - 1) + b
        except Exception as e:
            logger.error(f"linear regression fitting failed: {e}")
            return np.nan

    @staticmethod
    def true_range(df: pd.DataFrame) -> pd.Series:
        prev_close = df['close'].shift(1)
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    @staticmethod
    def add_squeeze_counter(df: pd.DataFrame) -> pd.DataFrame:
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

    @staticmethod
    def get_squeeze_momentum_histogram_color(val, val_prev):
        if pd.isna(val) or pd.isna(val_prev):
            return "数据不足"
        if val > 0:
            return "亮绿" if val > val_prev else "暗绿"
        elif val < 0:
            return "亮红" if val < val_prev else "暗红"
        else:
            return "中性"

    def squeeze_momentum_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        close, high, low = df['close'], df['high'], df['low']

        basis = close.rolling(self.cfg.get('bb_length')).mean()
        dev = self.cfg.get('kc_mult') * close.rolling(self.cfg.get('bb_length')).std(ddof=0)
        upperBB, lowerBB = basis + dev, basis - dev

        ma = close.rolling(self.cfg.get('kc_length')).mean()
        r = self.true_range(df) if self.cfg.get('use_true_range') else (high - low)
        rangema = r.rolling(self.cfg.get('kc_length')).mean()
        upperKC, lowerKC = ma + rangema * self.cfg.get('kc_mult'), ma - rangema * self.cfg.get('kc_mult')

        sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
        sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)
        df["sqz_status"] = np.select([sqzOn, sqzOff], ["ON", "OFF"], default="NO")

        highest_h = high.rolling(self.cfg.get('kc_length')).max()
        lowest_l = low.rolling(self.cfg.get('kc_length')).min()
        avg_hl = (highest_h + lowest_l) / 2
        sma_close = close.rolling(self.cfg.get('kc_length')).mean()
        mid = (avg_hl + sma_close) / 2
        source_mid = close - mid
        histogram_value = source_mid.rolling(self.cfg.get('kc_length')).apply(lambda x: self.tv_linreg(pd.Series(x), self.cfg.get('kc_length')), raw=False)

        df["sqz_hvalue"] = histogram_value
        df["sqz_pre_hvalue"] = histogram_value.shift(1)
        df = self.add_squeeze_counter(df)

        df["sqz_hcolor"] = df.apply(
            lambda re: self.get_squeeze_momentum_histogram_color(re["sqz_hvalue"], re["sqz_pre_hvalue"]), axis=1)

        df.drop(columns=["sqz_pre_hvalue"], inplace=True)

        return df

    def ema_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        df[f"ema{self.cfg.get('ema_length')}"] = df['close'].ewm(span=self.cfg.get('ema_length'), adjust=False).mean()

        return df

    def support_resistance_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        window = self.cfg.get('srb_left') + self.cfg.get('srb_right') + 1

        df['is_min'] = df['low'].rolling(window, center=True).apply(
            lambda x: 1 if x[self.cfg.get('srb_left')] == np.min(x) else 0, raw=True
        )
        df['is_max'] = df['high'].rolling(window, center=True).apply(
            lambda x: 1 if x[self.cfg.get('srb_left')] == np.max(x) else 0, raw=True
        )

        df['srb_sup'] = np.where(df['is_min'] == 1, df['low'], np.nan)
        df['srb_res'] = np.where(df['is_max'] == 1, df['high'], np.nan)

        df['srb_sup'] = df['srb_sup'].ffill()
        df['srb_res'] = df['srb_res'].ffill()

        df.drop(columns=['is_min', 'is_max'], inplace=True)

        return df

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = self.squeeze_momentum_indicator(df)
        df = self.ema_indicator(df)
        df = self.support_resistance_indicator(df)

        return df

class StrategyEngine:
    def __init__(self, st_cfg: dict):
        self.cfg = st_cfg

    def execute(self, df: pd.DataFrame, symbol: str, interval: str) -> Dict[str, Any]:

        cur = df.iloc[-1]
        prev = df.iloc[-2]
        change = (cur['close'] / prev['close'] - 1) * 100

        signal = "No"
        if cur['sqz_status'] == "OFF" and prev['sqz_status'] == "ON" and prev['sqz_id'] >= self.cfg['min_sqz_bars']:
            if (cur['close'] > cur['ema200']
                    and change > 0
                    and cur['close'] > cur['srb_res']
                    and cur['sqz_hcolor'] == "亮绿"):
                signal = "Long"

            elif (cur['close'] < cur['ema200']
                  and change < 0
                  and cur['close'] < cur['srb_sup']
                  and cur['sqz_hcolor'] == "亮红"):
                signal = "Short"

        energy, tr, ts = [], [], []

        for i in range(6, 0, -1):
            row = df.iloc[-(i + 1)]
            energy.append(f"{row['sqz_hcolor']}[{row['sqz_hvalue']:+.2f}]")
            tr.append("高" if row['close'] > cur['srb_res'] else "低")
            ts.append("高" if row['close'] > cur['srb_sup'] else "低")

        return {
            "date": df.index[-1].strftime("%Y-%m-%d"),
            "time": df.index[-1].strftime("%H:%M:%S"),
            "interval": interval,
            "symbol": symbol,
            "signal": signal,
            "price": round(cur['close'], 4),
            "change": round(change, 2),
            "bars": int(prev['sqz_id']),
            "ema200": round(cur['ema200'], 4),
            "energy": "-".join(energy),
            "support": str(round(cur['srb_sup'], 4)),
            "resistance": str(round(cur['srb_res'], 4)),
            "trend_r": "-".join(tr),
            "trend_s": "-".join(ts)
        }

class NotifyEngine:
    def __init__(self, notify_cfg: dict, time_cfg: dict):
        self.cfg = notify_cfg
        self.time_cfg = time_cfg
        self.running_tasks = []

    def process_results(self, results: list, interval: str):
        results_list = [r for r in results if r is not None]
        if not results_list:
            return

        signals = [r for r in results_list if r.get('signal') != "No"]

        if self.cfg.get('CONSOLE_LOG'):
            logger.info(f"[{interval}] 扫描完成 | 监控品种: {len(results_list)} | 触发信号: {len(signals)}")
            for item in results_list:
                symbol = item.get('symbol', 'Unknown')
                json_str = json.dumps(item, ensure_ascii=False)
                log_prefix = f"[{interval}] {symbol.ljust(20)}"
                if item.get('signal') != "No":
                    logger.info(f"{log_prefix} | Y | {json_str}")
                else:
                    logger.info(f"{log_prefix} | N | {json_str}")

        if self.cfg.get('TG_ENABLE') and signals:
            task = asyncio.create_task(self.tg_broadcast_and_send(signals, interval))
            self.running_tasks.append(task)
            task.add_done_callback(lambda t: self.running_tasks.remove(t) if t in self.running_tasks else None)

        if self.cfg.get('WECOM_ENABLE') and signals:
            task = asyncio.create_task(self.wecom_broadcast_and_send(signals, interval))
            self.running_tasks.append(task)
            task.add_done_callback(lambda t: self.running_tasks.remove(t) if t in self.running_tasks else None)

    def format_single_signal(self, res, interval, tag):
        symbol = res.get('symbol', 'Unknown')
        s_upper = symbol.upper()

        tv_symbol = symbol[:-1] if s_upper.endswith('M') else symbol

        groups = self.time_cfg.get("market_groups", {})

        forex_list = groups.get("forex_gold", [])
        stocks_list = groups.get("us_stocks", [])

        if any(k in s_upper for k in stocks_list):
            exchange = "NASDAQ"
        elif any(k in s_upper for k in forex_list):
            exchange = "FX"
        else:
            logger.error("没有配置对应的跳转链接")
            exchange = ""

        tv_url = f"https://cn.tradingview.com/chart/?symbol={exchange}%3A{tv_symbol}"

        raw_signal = res.get('signal', 'No')
        if raw_signal == "Long":
            signal_text = "🟢 Long"
            trend_str = str(res.get('trend_r', ""))
        elif raw_signal == "Short":
            signal_text = "🔴 Short"
            trend_str = str(res.get('trend_s', ""))
        else:
            signal_text = "No"
            trend_str = str(res.get('trend_r', ""))

        price = res.get('price', 0)
        change = res.get('change', 0)
        change_str = f"({'+' if change >= 0 else ''}{change}%)"

        energy_str = str(res.get('energy', ""))
        energy_items = energy_str.split('-') if energy_str else []
        recent_items = energy_items[-6:]
        mom_icons = "".join(["🟢" if "绿" in item else "🔴" for item in recent_items])

        trend_list = trend_str.split('-') if trend_str else []
        trend_icons = "".join(["⬆️" if "高" in t else "⬇️" for t in trend_list[-6:]]) if trend_list else ""

        if tag == "telegram":
            symbol_link = f'<a href="{tv_url}">{tv_symbol}</a>'
            tg_msg_text = (
                f"💹 <b>代码: </b> <b>{symbol_link}【{interval.upper()}】</b>\n"
                f"💰 <b>价格:</b> <code>{price}{change_str}</code>\n"
                f"💸 <b>信号:</b> <code>{signal_text}</code>\n"
                f"🔄 <b>时间:</b> <code>{res.get('time', '-')}（UTC+8）</code>\n"
                f"🧨 <b>挤压:</b> <code>{res.get('bars', 0)} Bars</code>\n"
                f"📊 <b>动能:</b> {mom_icons if mom_icons else '无'}\n"
                f"🚀 <b>趋势:</b> {trend_icons if trend_icons else '无'}\n"
                f"📅 <b>日期:</b> <code>{res.get('date', '-')}</code>\n"
            )
            return tg_msg_text

        elif tag == "wecom":
            symbol_link = f'[{tv_symbol}]({tv_url})'
            wecom_msg_text = (
                f"💹 代码: {symbol_link}【{interval.upper()}】\n"
                f"💰 价格: {price}{change_str}\n"
                f"💸 信号: {signal_text}\n"
                f"🔄 时间: {res.get('time', '-')}（UTC+8）\n"
                f"🧨 挤压: {res.get('bars', 0)} Bars\n"
                f"📊 动能: {mom_icons if mom_icons else '无'}\n"
                f"🚀 趋势: {trend_icons if trend_icons else '无'}\n"
                f"📅 日期: {res.get('date', '-')}"
            )
            return wecom_msg_text
        else:
            logger.error("没有对应的消息卡片，请检查")
            return None

    async def tg_broadcast_and_send(self, signal_results, interval, tag="telegram"):
        token = self.cfg.get('TG_TOKEN')
        chat_id = self.cfg.get('TG_CHAT_ID')
        url = f"https://api.telegram.org/bot{token}/sendMessage"

        chunk_size = 10

        total_signals = len(signal_results)

        async with aiohttp.ClientSession() as session:
            for i in range(0, len(signal_results), chunk_size):
                chunk = signal_results[i:i + chunk_size]

                header = (
                    f"🚀 <b>【{interval.upper()}】周期信号报告</b>\n"
                    f"⏰ 扫描时间: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"━━━━━━━━━━━━━━\n"
                )

                body_parts = [ self.format_single_signal(res, interval, tag) for res in chunk ]

                final_msg = header + "\n\n".join(body_parts)

                payload = {
                    "chat_id": chat_id,
                    "text": final_msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                    "disable_notification": False
                }

                try:
                    async with session.post(url, data=payload, timeout=10) as resp:
                        if resp.status != 200:
                            logger.error( f"TG 发送失败 [{resp.status}]: {await resp.text()}")
                except Exception as e:
                    logger.error(f"TG 网络异常: {e}")

                await asyncio.sleep(0.5)

        logger.info(f"[{interval}] telegram通知发送完毕 | 总信号数: {total_signals}")

    async def wecom_broadcast_and_send(self, signal_results, interval, tag="wecom"):
        webhook_url = self.cfg.get('WECOM_WEBHOOK')
        if not webhook_url:
            return

        chunk_size = 8

        total_signals = len(signal_results)

        async with aiohttp.ClientSession() as session:
            for i in range(0, len(signal_results), chunk_size):
                chunk = signal_results[i:i + chunk_size]

                header = (
                    f"🚀【{interval.upper()}】周期信号报告\n"
                    f"⏰ 扫描时间: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"━━━━━━━━━━━━━━\n"
                )

                body_parts = []
                for res in chunk:
                    text = self.format_single_signal(res, interval, tag)
                    if text:
                        body_parts.append(text.rstrip())

                final_content = header + "\n\n\n".join(body_parts)

                payload = { "msgtype": "markdown",  "markdown": { "content": final_content } }

                try:
                    async with session.post(webhook_url, json=payload, timeout=10) as resp:
                        if resp.status != 200:
                            logger.error(f"wecom 发送失败 [{resp.status}]: {await resp.text()}")
                except Exception as e:
                    logger.error(f"wecom 网络异常: {e}")

                await asyncio.sleep(0.5)

        logger.info(f"[{interval}] wecom通知发送完毕 | 总信号数: {total_signals}")

    async def send_error_msg(self, error_text: str):
        tasks = []
        if self.cfg.get('WECOM_ENABLE'):
            webhook_url = self.cfg.get('WECOM_WEBHOOK')
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"⚠️ **Exness系统异常报警**\n\n> 详情: {error_text}\n> 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            }
            tasks.append(asyncio.create_task(self._post_request(webhook_url, payload, "wecom_err")))

        if self.cfg.get('TG_ENABLE'):
            token = self.cfg.get('TG_TOKEN')
            chat_id = self.cfg.get('TG_CHAT_ID')
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": f"⚠️ <b>Exness系统异常报警</b>\n\n详情: {error_text}",
                "parse_mode": "HTML"
            }
            tasks.append(asyncio.create_task(self._post_request(url, payload, "tg_err")))

        if tasks:
            await asyncio.gather(*tasks)

    async def send_heartbeat(self):
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = (
            f"💓 **Exness机器人运行中**\n"
            f"━━━━━━━━━━━━━━\n"
            f"状态: 系统心跳正常\n"
            f"时间: {now_str}\n"
            f"提示: 监控任务持续运行中..."
        )

        tasks = []

        if self.cfg.get('WECOM_ENABLE'):
            webhook_url = self.cfg.get('WECOM_WEBHOOK')
            payload = {
                "msgtype": "markdown",
                "markdown": {"content": msg}
            }
            tasks.append(asyncio.create_task(self._post_request(webhook_url, payload, "wecom_hb")))

        if self.cfg.get('TG_ENABLE'):
            token = self.cfg.get('TG_TOKEN')
            chat_id = self.cfg.get('TG_CHAT_ID')
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            tg_msg = msg.replace("**", "<b>").replace("**", "</b>")
            payload = {
                "chat_id": chat_id,
                "text": tg_msg,
                "parse_mode": "HTML"
            }
            tasks.append(asyncio.create_task(self._post_request(url, payload, "tg_hb")))

        if tasks:
            await asyncio.gather(*tasks)
            logger.info("💓 已发送系统存活心跳通知")

    @staticmethod
    async def _post_request(url, payload, tag):
        async with aiohttp.ClientSession() as session:
            try:
                if "msgtype" in payload:  # WeCom
                    await session.post(url, json=payload, timeout=5)
                else:  # Telegram
                    await session.post(url, data=payload, timeout=5)
            except Exception as e:
                logger.error(f"发送报警失败 [{tag}]: {e}")

class TimeEngine:
    def __init__(self, time_cfg: dict):
        self.cfg = time_cfg

    @staticmethod
    def get_wait_seconds(interval: str) -> float:
        now = datetime.now()
        val = int(interval[:-1])
        unit = interval[-1].lower()

        if unit == 'm':
            offset_sec = 3
        elif unit == 'h':
            offset_sec = 5
        elif unit == 'd':
            offset_sec = 5
        else:
            offset_sec = 3

        if unit == 'm':
            target_min = ((now.minute // val) + 1) * val
            if target_min >= 60:
                base_time = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            else:
                base_time = now.replace(minute=target_min, second=0, microsecond=0)

        elif unit == 'h':
            target_hour = ((now.hour // val) + 1) * val
            if target_hour >= 24:
                base_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                base_time = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)

        elif unit == 'd':
            base_time = now.replace(hour=8, minute=0, second=0, microsecond=0)
            if now >= base_time:
                base_time += timedelta(days=1)
        else:
            return 60.0

        next_run = base_time + timedelta(seconds=offset_sec)

        wait_sec = (next_run - now).total_seconds()

        return wait_sec if wait_sec > 0 else 1.0

    def is_symbol_market_open(self, symbol: str) -> bool:
        s = symbol.upper()
        now = datetime.now()
        weekday = now.weekday()
        hour = now.hour
        minute = now.minute

        is_dst = 3 <= now.month <= 11

        groups = self.cfg.get("market_groups", {})
        forex_keywords = groups.get("forex_gold", [])
        stock_keywords = groups.get("us_stocks", [])

        if any(k in s for k in forex_keywords):
            close_h = 5 if is_dst else 6
            open_h = 6 if is_dst else 7
            if (weekday == 5 and hour >= close_h) or weekday == 6:
                return False
            if weekday == 0 and hour < open_h:
                return False
            return True

        elif any(k in s for k in stock_keywords):
            if weekday >= 5: return False

            start_h, start_m = (21, 30) if is_dst else (22, 30)
            end_h = 4 if is_dst else 5

            curr_min = hour * 60 + minute
            start_min = start_h * 60 + start_m
            end_min = end_h * 60

            if curr_min >= start_min or curr_min < end_min:
                return True
            return False

        return True

class ScanEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.is_active = True
        self.data_e = DataEngine(cfg['api'])
        self.ind_e = IndicatorEngine(cfg['strategy'])
        self.strat_e = StrategyEngine(cfg['strategy'])
        self.notify_e = NotifyEngine(cfg['notify'], cfg['time'])
        self.timer_e = TimeEngine(cfg['time'])

    async def _proc_symbol(self, session, symbol, interval, sem):
        async with sem:
            try:
                if not self.timer_e.is_symbol_market_open(symbol):
                    return None

                raw = await self.data_e.fetch_klines(session, symbol)

                if raw is None:
                    logger.error(f"❌ {symbol} 获取数据失败 (API返回空)")
                    return None

                data_len = len(raw)
                if data_len < 200:
                    logger.warning(f"⚠️ {symbol} 数据条数不足: {data_len} (需要至少200条)")
                    return None

                df = self.ind_e.calculate(raw)

                res = self.strat_e.execute(df, symbol, interval)
                return res

            except Exception as e:
                logger.error(f"💥 {symbol} 处理过程中崩溃: {e}", exc_info=True)
                return None

    async def scan_cycle(self, session, symbols, interval):
        sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])
        tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]
        results = list(await asyncio.gather(*tasks))
        self.notify_e.process_results(results, interval)

    async def interval_worker(self, session, interval):
        logger.info(f"🟢 [{interval}] 周期监控任务已启动")

        last_run_slot = None
        self.is_active = True

        while True:
            if not self.is_active:
                logger.critical(f"🛑 [{interval}] 系统已熔断停机。请检查 Token 有效性并手动重启脚本。")
                break

            wait_sec = self.timer_e.get_wait_seconds(interval)
            if wait_sec > 0:
                if wait_sec > 10:
                    target_time = (datetime.now() + timedelta(seconds=wait_sec)).strftime('%H:%M:%S')
                    logger.info(f"💤 [{interval}] 下次对齐点: {target_time} (等待 {int(wait_sec)}s)")
                await asyncio.sleep(wait_sec)

            symbols = self.cfg.get("watch_list", [])
            opened_symbols = [s for s in symbols if self.timer_e.is_symbol_market_open(s)]

            if not opened_symbols:
                await asyncio.sleep(60)
                continue

            current_slot = datetime.now().replace(second=0, microsecond=0)
            if last_run_slot == current_slot:
                await asyncio.sleep(1)
                continue

            try:
                start_time = time.time()
                symbols = self.cfg.get("watch_list", [])

                if not symbols:
                    logger.warning(f"⚠️ [{interval}] 监控列表为空，跳过本次扫描")
                    await asyncio.sleep(10)
                    continue

                sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])
                tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]

                results = await asyncio.gather(*tasks)

                opened_symbols = [s for s in symbols if self.timer_e.is_symbol_market_open(s)]

                valid_results = [r for r in results if r is not None]

                if len(opened_symbols) > 0 and len(valid_results) == 0:
                    self.is_active = False  # 触发熔断开关
                    error_msg = (f"🚨 [{interval}] 关键异常：所有品种接口请求均失败！\n"
                                 f"原因：Token 已失效或 API 被暂时封禁。\n"
                                 f"结果：系统已自动熔断停机，不再请求接口。")

                    logger.critical(error_msg)
                    await self.notify_e.send_error_msg(error_msg)
                    continue

                self.notify_e.process_results(list(results), interval)

                if self.notify_e.running_tasks:
                    await asyncio.gather(*self.notify_e.running_tasks)

                last_run_slot = current_slot
                duration = time.time() - start_time
                logger.info(
                    f"✅ [{interval}] 扫描完成 (有效:{len(valid_results)}/{len(symbols)}), 耗时: {duration:.2f}s")

            except Exception as e:
                logger.error(f"❌ [{interval}] 运行过程中发生未预料异常: {e}", exc_info=True)
                await asyncio.sleep(10)

    async def heartbeat_worker(self):
        logger.info("💗 心跳监控协程已启动 (周期: 4小时)")

        await self.notify_e.send_heartbeat()

        while True:
            try:
                await asyncio.sleep(4 * 3600)

                if self.is_active:
                    await self.notify_e.send_heartbeat()
                else:
                    logger.warning("💓 心跳跳过：系统目前处于熔断停机状态。")

            except Exception as e:
                logger.error(f"❌ 心跳协程异常: {e}")
                await asyncio.sleep(60)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            try:
                logger.info("⚡ 启动即时扫描调试开始...")

                symbols = self.cfg.get("watch_list")

                if symbols and len(symbols) > 0:

                    await self.scan_cycle(session, symbols, "5M")
                else:
                    logger.error("❌ 严重错误：最终 symbols 列表为空，无法扫描！")

            except Exception as e:
                logger.error(f"❌ 初始扫描发生崩溃: {e}", exc_info=True)

            workers = [self.interval_worker(session, i) for i in self.cfg.get('intervals')]

            workers.append(self.heartbeat_worker())

            await asyncio.gather(*workers)

async def handle_health(request):
    return web.Response(text="Bot is running", content_type='text/html')

async def main():
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 7860)
    await site.start()
    scanner = ScanEngine(CONFIG)
    await scanner.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.error("APP stopped manually")