#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import numpy as np
import pandas as pd
import asyncio
from aiohttp import web
import aiohttp
import logging
import os
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional, Any
from cryptography.fernet import Fernet

TZ = os.getenv("TZ") # TZ -> Asia/Shanghai
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

cipher = Fernet(ENCRYPTION_KEY.encode())
WECOM_WEBHOOK = cipher.decrypt(b'gAAAAABpX1lf_OZccl6JYh14FJlLEmJDtV37L1jW5MMRhdA09xypIujad5g1e2axJUwOA_gKCF3kodoYVG9Wrj1TyayLXmSn3t6lnG5xzNXedE01dNq1E-S77oYFLhaS9g3Ay24P2apcvBGkaV61cI76Pk7jNrjRTNjhxwgrvT3FiDHaQk3FULbFwvQJy0BADgv1cli4_vzB').decode()
TG_TOKEN = cipher.decrypt(b'gAAAAABpX1mGV2Aqsf_W0eXjohhjNzWB4pDhsPqRDDei9jfKMkwsCT9Bu0qHzOGDAaapiBGNPwP1hyk46SN78yq2si5RylJTSBmdh6wPJlWpeAZtlEgu7wuxlEi3AMByECDdWnBx1iol').decode()
TG_CHAT_ID = cipher.decrypt(b'gAAAAABpX1maZKmpePVf4ancQG2QpOX7YXk4wPMqPTw8x4DgJN3cKaVO6I0cQp0eCpL1gR4lim2W6k0LWXqH-R28889G2I446Q==').decode()

CONFIG = {
    "watch_list" : [],
    "intervals": ["1H", "4H", "1D"],
    "api": {
        "ACTIVE_EXCHANGE": "OKX",
        "OKX_BASE_URL": "https://www.okx.com",
        "BINANCE_BASE_URL": "https://fapi.binance.com",
        "TOP_N": 100,
        "MAX_CONCURRENT": 8,
        "KLINE_LIMIT": 1000,
        "EXCLUDE_TOKENS": ["USDC", "FDUSD", "DAI", "EUR"]
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
    "notify": {
        "CONSOLE_LOG": True,
        "WECOM_ENABLE": True,
        "TG_ENABLE": False,
        "WECOM_WEBHOOK": WECOM_WEBHOOK,
        "TG_TOKEN": TG_TOKEN,
        "TG_CHAT_ID": TG_CHAT_ID
    }
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DataEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.exchange = cfg.get("ACTIVE_EXCHANGE").upper()
        self.okx_base = cfg.get('OKX_BASE_URL')
        self.binance_base = cfg.get('BINANCE_BASE_URL')

    async def get_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        if self.exchange == "BINANCE":
            return await self._get_binance_active_symbols(session)
        else:
            return await self._get_okx_active_symbols(session)

    async def _get_binance_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        url = f"{self.binance_base}/fapi/v1/ticker/24hr"
        try:
            async with session.get(url, timeout=10) as r:
                data = await r.json()
                if not isinstance(data, list):
                    logger.error(f"❌ 币安 API 响应异常: {data}")
                    return []

                df = pd.DataFrame(data)
                df['vol_usdt'] = pd.to_numeric(df['quoteVolume'], errors='coerce')

                df = df[df['symbol'].str.endswith('USDT')]

                exclude = self.cfg.get('EXCLUDE_TOKENS', [])
                for token in exclude:
                    df = df[~df['symbol'].str.contains(token)]

                df = df.sort_values('vol_usdt', ascending=False)
                top_n = self.cfg.get('TOP_N', 50)
                symbols = df.head(top_n)['symbol'].tolist()

                logger.info(f"🔥 [Binance] 当前成交额前5: {symbols[:5]}")
                return symbols
        except Exception as e:
            logger.error(f"💥 获取币安活跃币种失败: {e}")
            return []

    async def _get_okx_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        url = f"{self.okx_base}/api/v5/market/tickers"
        params = {"instType": "SWAP"}
        try:
            async with session.get(url, params=params, timeout=10) as r:
                res = await r.json()
                data = res.get('data', [])
                if not data: return []

                df = pd.DataFrame(data)
                df['vol_usdt'] = pd.to_numeric(df['volCcy24h'], errors='coerce') * pd.to_numeric(df['last'], errors='coerce')
                df = df[df['instId'].str.endswith('-USDT-SWAP')]

                exclude = self.cfg.get('EXCLUDE_TOKENS', [])
                for token in exclude:
                    df = df[~df['instId'].str.contains(token)]

                df = df.sort_values('vol_usdt', ascending=False)
                top_n = self.cfg.get('TOP_N', 50)
                symbols = df.head(top_n)['instId'].tolist()

                for core in ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]:
                    if core not in symbols: symbols.insert(0, core)

                logger.info(f"🔝 [OKX] 当前成交额前5: {symbols[:5]}")
                return symbols[:top_n]
        except Exception as e:
            logger.error(f"💥 获取 OKX 活跃币种失败: {e}")
            return []

    def format_symbol(self, token: str) -> str:
        clean_token = token.upper().replace("-USDT-SWAP", "").replace("USDT", "")
        if self.exchange == "OKX":
            return f"{clean_token}-USDT-SWAP"
        else:
            return f"{clean_token}USDT"

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        if self.exchange == "BINANCE":
            return await self._fetch_binance_klines(session, symbol, interval)
        else:
            return await self._fetch_okx_klines(session, symbol, interval)

    async def _fetch_okx_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        url = f"{self.okx_base}/api/v5/market/candles"
        okx_interval = interval.upper()
        params = {
            "instId": symbol,
            "bar": okx_interval,
            "limit": self.cfg.get('KLINE_LIMIT', 1000)
        }
        try:
            async with session.get(url, params=params, timeout=10) as r:
                res = await r.json()
                data = res.get('data', [])
                if not data: return None

                df = pd.DataFrame(data, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'volCcy', 'volCcyQuote', 'confirm'])
                df = df.iloc[::-1].reset_index(drop=True)

                df = df[df['confirm'] == '1'].copy()

                df = df[['ts', 'o', 'h', 'l', 'c', 'v']].astype(float)
                df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
                df['date'] = pd.to_datetime(df['ts'], unit='ms') + timedelta(hours=8)
                df.set_index('date', inplace=True)
                return df
        except Exception as e:
            logger.error(f"OKX Fetch Error ({symbol}): {e}")
            return None

    async def _fetch_binance_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        url = f"{self.binance_base}/fapi/v1/klines"
        bn_interval = interval.lower()
        params = {
            "symbol": symbol,
            "interval": bn_interval,
            "limit": self.cfg.get('KLINE_LIMIT', 1000)
        }
        try:
            async with session.get(url, params=params, timeout=10) as r:
                data = await r.json()
                if isinstance(data, dict) or not data: return None

                df = pd.DataFrame(data).iloc[:-1]
                df = df[[0, 1, 2, 3, 4, 5]].astype(float)
                df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
                df['date'] = pd.to_datetime(df['ts'], unit='ms') + timedelta(hours=8)
                df.set_index('date', inplace=True)
                return df
        except Exception as e:
            logger.error(f"Binance Fetch Error ({symbol}): {e}")
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
            if (
                # cur['close'] > cur['ema200']
                change > 0
                # and cur['close'] > cur['srb_res']
                and cur['sqz_hcolor'] == "亮绿"
            ):
                signal = "Long"

            elif (
                # cur['close'] < cur['ema200']
                change < 0
                # and cur['close'] < cur['srb_sup']
                and cur['sqz_hcolor'] == "亮红"
            ):
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
    def __init__(self, notify_cfg: dict):
        self.cfg = notify_cfg
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

    @staticmethod
    def format_single_signal(res, interval, tag):
        symbol = res.get('symbol', 'Unknown')
        active_exchange = CONFIG["api"].get("ACTIVE_EXCHANGE")
        tv_symbol = symbol.replace("-SWAP", "").replace("-", "")

        if active_exchange == "BINANCE":
            tv_url = f"https://cn.tradingview.com/chart/?symbol=BINANCE%3A{tv_symbol}"
        else:
            tv_url = f"https://cn.tradingview.com/chart/?symbol=OKX%3A{tv_symbol}.P"

        price = res.get('price', 0)
        change = res.get('change', 0)
        change_str = f"({'+' if change >= 0 else ''}{change}%)"

        ema200 = res.get('ema200', 0)
        support = res.get('support', 0)
        resistance = res.get('resistance', 0)

        raw_signal = res.get('signal', 'No')
        if raw_signal == "Long":
            signal_text = "🟢 Long"
            trend_str = str(res.get('trend_r', ""))
            e_b = "📈EMA200" if price > ema200 else "📉EMA200"
            r_b = "📈压力位" if price > resistance else "📉压力位"
            judge_text = f"{e_b}|{r_b}"
        elif raw_signal == "Short":
            signal_text = "🔴 Short"
            trend_str = str(res.get('trend_s', ""))
            e_b = "📈EMA200" if price > ema200 else "📉EMA200"
            r_b = "📈支撑位" if price > support else "📉支撑位"
            judge_text = f"{e_b}|{r_b}"
        else:
            signal_text = "No"
            trend_str = str(res.get('trend_r', ""))
            judge_text = ""

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
                f"⚖️ <b>判断:</b> <code>{judge_text}</code>\n"
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
                f"⚖️ 判断: {judge_text}\n"
                f"🔄 时间: {res.get('time', '-')}（UTC+8）\n"
                f"🧨 挤压: {res.get('bars', 0)} Bars\n"
                f"📊 动能: {mom_icons if mom_icons else '无'}\n"
                f"🚀 趋势: {trend_icons if trend_icons else '无'}\n"
                f"📅 日期: {res.get('date', '-')}"
            )
            return wecom_msg_text
        else:
            logger.error("notify configuration error, please check")
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
                    "content": f"⚠️ **Crypto系统异常报警**\n\n> 详情: {error_text}\n> 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            }
            tasks.append(asyncio.create_task(self._post_request(webhook_url, payload, "wecom_err")))

        if self.cfg.get('TG_ENABLE'):
            token = self.cfg.get('TG_TOKEN')
            chat_id = self.cfg.get('TG_CHAT_ID')
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": f"⚠️ <b>Crypto系统异常报警</b>\n\n详情: {error_text}",
                "parse_mode": "HTML"
            }
            tasks.append(asyncio.create_task(self._post_request(url, payload, "tg_err")))

        if tasks:
            await asyncio.gather(*tasks)

    async def send_heartbeat(self):
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = (
            f"💓 **Crypto机器人**\n"
            f"━━━━━━━━━━━━━━\n"
            f"状态: 心跳正常\n"
            f"时间: {now_str}\n"
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
            # TG 使用 HTML 格式
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

class ScanEngine:
    def __init__(self, cfg: dict):
        self.is_active = True
        self.cfg = cfg
        self.data_e = DataEngine(cfg['api'])
        self.ind_e = IndicatorEngine(cfg['strategy'])
        self.strat_e = StrategyEngine(cfg['strategy'])
        self.notify_e = NotifyEngine(cfg['notify'])
        self.timer_e = TimeEngine()

    async def _proc_symbol(self, session, symbol, interval, sem):
        async with sem:
            try:
                raw = await self.data_e.fetch_klines(session, symbol, interval)

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

        while True:
            if not self.is_active:
                logger.critical(f"🛑 [{interval}] 系统已熔断停机")
                break

            wait_sec = self.timer_e.get_wait_seconds(interval)
            if wait_sec > 0:
                if wait_sec > 10:
                    target_time = (datetime.now() + timedelta(seconds=wait_sec)).strftime('%H:%M:%S')
                    logger.info(f"💤 [{interval}] 下次对齐点: {target_time} (等待 {int(wait_sec)}s)")
                await asyncio.sleep(wait_sec)

            current_slot = datetime.now().replace(second=0, microsecond=0)
            if last_run_slot == current_slot:
                await asyncio.sleep(1)
                continue

            try:
                start_time = time.time()
                watch_list = self.cfg.get("watch_list", [])

                if watch_list:
                    symbols = [self.data_e.format_symbol(s) for s in watch_list]
                else:
                    symbols = await self.data_e.get_active_symbols(session)

                if not symbols:
                    reason = "关键异常：无法获取活跃币种列表"
                    await self._trigger_circuit_breaker(interval, reason)
                    continue  # 这里进入 continue 后，下一轮循环会在步骤 A 退出

                sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])
                tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]
                results = await asyncio.gather(*tasks)

                valid_results = [r for r in results if r is not None]

                if len(symbols) > 0 and len(valid_results) == 0:
                    reason = "关键异常：所有币种详情请求均失败"
                    await self._trigger_circuit_breaker(interval, reason)
                    continue

                self.notify_e.process_results(list(results), interval)

                if self.notify_e.running_tasks:
                    await asyncio.gather(*self.notify_e.running_tasks)

                last_run_slot = current_slot
                logger.info(
                    f"✅ [{interval}] 扫描完成 (有效:{len(valid_results)}), 耗时: {time.time() - start_time:.2f}s")

            except Exception as e:
                logger.error(f"❌ [{interval}] 运行时异常: {e}", exc_info=True)
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

    async def _trigger_circuit_breaker(self, interval: str, reason: str):
        self.is_active = False
        error_msg = (
            f"🛑 【系统熔断停机】\n"
            f"触发周期: {interval}\n"
            f"故障原因: {reason}\n"
            f"结果: 扫描任务已终止"
        )
        logger.critical(error_msg)
        await self.notify_e.send_error_msg(error_msg)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            try:
                logger.info("⚡ 启动即时扫描")

                watch_list = self.cfg.get("watch_list", [])

                if watch_list and len(watch_list) > 0:
                    symbols = [self.data_e.format_symbol(s) for s in watch_list]
                    logger.info(f"📋 使用配置列表 (已转换格式): {symbols}")
                else:
                    symbols = await self.data_e.get_active_symbols(session)

                if not symbols or len(symbols) == 0:
                    error_msg = "🚨 程序启动失败：请求数据为空，无法执行初始扫描"
                    logger.critical(f"❌ {error_msg}")
                    await self.notify_e.send_error_msg(error_msg)
                    return

                await self.scan_cycle(session, symbols, "1H")

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