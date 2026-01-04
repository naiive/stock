#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import numpy as np
import pandas as pd
import asyncio
import aiohttp
import logging
import os
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional, Any
from aiohttp import web

# =====================================================
# Hugging Face 配置适配
# =====================================================
# 请在 HF Space 的 Settings -> Variables and Secrets 中添加这两个键值对
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

# 直接在 Hugging Face 的环境变量（Secrets/Variables）里加一个配置：
# Name: TZ
# Value: Asia/Shanghai

# =====================================================
# 0. 配置中心 (CONFIG)
# =====================================================
CONFIG = {
    # 留空则自动获取全市场高成交额品种
    "watch_list": [],
    # "watch_list": ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP"],
    # 监听的时间周期
    "intervals": ["1H", "4H", "1D"],

    "api": {
        "BASE_URL": "https://www.okx.com",
        "TOP_N": 50,  # 自动抓取成交额前50的品种
        "MAX_CONCURRENT": 8,  # 最大并发请求数
        "KLINE_LIMIT": 1000,  # K线数量
        "EXCLUDE_TOKENS": ["USDC", "FDUSD", "DAI", "EUR"]  # 排除稳定币之类的
    },

    "strategy": {
        "bb_length": 20,  # 布林带周期
        "bb_mult": 2.0,  # 布林带标准差倍数
        "kc_length": 20,  # 肯特纳通道周期
        "kc_mult": 1.2,  # 肯特纳通道倍数 (Squeeze核心参数)
        "use_true_range": True,  # True真实波动幅度/简单波动范围

        "ema_length": 200,  # 长期趋势过滤

        "srb_left": 15,  # 支撑压力左侧强度
        "srb_right": 15,  # 支撑压力右侧强度

        "min_sqz_bars": 6  # 至少6根K线才视为有效挤压
    },

    "notify": {
        "CONSOLE_LOG": True,  # 控制台日志输出
        "TG_ENABLE": True,  # telegram bot 发送
        "TG_TOKEN": TG_TOKEN,  # 修改：改为读取变量
        "TG_CHAT_ID": TG_CHAT_ID  # 修改：改为读取变量
    }
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


# =====================================================
# 1. 数据引擎 (DataEngine)
# =====================================================
class DataEngine:
    def __init__(self, api_cfg: dict):
        self.cfg = api_cfg
        self.base_url = self.cfg.get('BASE_URL')

    async def get_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        """获取 OKX 活跃币种 (按 USDT 成交额排序)"""
        url = f"{self.base_url}/api/v5/market/tickers"
        params = {"instType": "SWAP"}
        try:
            async with session.get(url, params=params, timeout=10) as r:
                res = await r.json()
                data = res.get('data', [])

                if not data:
                    logger.error("❌ 获取 Tickers 失败，数据为空")
                    return []

                # 1. 转为 DataFrame
                df = pd.DataFrame(data)

                # 2. 关键步骤：强制将 USDT 成交额字段转为浮点数
                # volCcy24h 是以计价货币（USDT）为单位的成交额
                df['vol_usdt'] = pd.to_numeric(df['volCcy24h'], errors='coerce')

                # 3. 过滤：只保留 USDT 永续合约
                df = df[df['instId'].str.endswith('-USDT-SWAP')]

                # 4. 排除你配置中的特定币种
                exclude_list = self.cfg.get('EXCLUDE_TOKENS', [])
                for token in exclude_list:
                    df = df[~df['instId'].str.contains(token)]

                # 5. 核心排序：按 USDT 成交额从大到小排列 (ascending=False)
                df = df.sort_values('vol_usdt', ascending=False)

                # 打印前 5 名核实
                top_5_check = df.head(5)[['instId', 'vol_usdt']].values.tolist()
                logger.info(f"🔝 当前成交额前5名: {top_5_check}")

                # 6. 提取前 N 个
                top_n = self.cfg.get('TOP_N', 50)
                top_symbols = df.head(top_n)['instId'].tolist()

                # 7. 额外保险：确保 BTC/ETH 无论如何都在列表里
                for core in ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]:
                    if core in df['instId'].values and core not in top_symbols:
                        top_symbols.insert(0, core)

                return top_symbols[:top_n]

        except Exception as e:
            logger.error(f"💥 按成交额排序获取币种失败: {e}")
            return []

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        """抓取 OKX K线数据并自动处理参数格式"""
        url = f"{self.base_url}/api/v5/market/candles"

        # OKX 转换逻辑：将 "1h" 转换为 "1H", "1d" 转换为 "1D"
        okx_interval = interval.upper() if 'h' in interval or 'd' in interval else interval

        params = {
            "instId": symbol,
            "bar": okx_interval,
            "limit": self.cfg.get('KLINE_LIMIT', 100)
        }

        try:
            async with session.get(url, params=params, timeout=10) as r:
                if r.status != 200:
                    err_msg = await r.text()
                    logger.error(f"OKX API 响应异常: {r.status} - {err_msg}")
                    return None

                res = await r.json()
                data = res.get('data', [])

                if not data:
                    return None

                # OKX 返回格式: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                df = pd.DataFrame(data, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'volCcy', 'volCcyQuote', 'confirm'])

                # 重要：OKX 数据是倒序的（最新在前），必须翻转回正序进行技术指标计算
                df = df.iloc[::-1].reset_index(drop=True)

                # 剔除未闭合的 K 线
                if len(df) > 0:
                    df = df.iloc[:-1].copy()

                # 转换数值
                df = df[['ts', 'o', 'h', 'l', 'c', 'v']].astype(float)
                df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']

                # 时间转换为北京时间
                df['date'] = pd.to_datetime(df['ts'], unit='ms') + timedelta(hours=8)
                df.set_index('date', inplace=True)
                return df

        except Exception as e:
            logger.error(f"OKX 抓取K线数据失败 ({symbol}): {e}")
            return None


# =====================================================
# 2. 指标引擎 (IndicatorEngine)
# =====================================================
class IndicatorEngine:
    def __init__(self, st_cfg: dict):
        self.cfg = st_cfg

    @staticmethod
    def tv_linreg(series: pd.Series, length: int):
        """线性回归拟合"""
        if pd.isna(series).any() or len(series) < length:
            return np.nan
        x = np.arange(length)
        y_vals = series.values[-length:]  # 确保只取最新长度
        A = np.vstack([x, np.ones(length)]).T
        try:
            m, b = np.linalg.lstsq(A, y_vals, rcond=None)[0]
            return m * (length - 1) + b
        except Exception as e:
            logger.error(f"线性回归拟合失败: {e}")
            return np.nan

    @staticmethod
    def true_range(df: pd.DataFrame) -> pd.Series:
        """计算真实波幅 TR"""
        prev_close = df['close'].shift(1)
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    @staticmethod
    def add_squeeze_counter(df: pd.DataFrame) -> pd.DataFrame:
        """给每根K线打上一个 连续积压/释放计数"""
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
        """动能柱颜色"""
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

        # 计算Bollinger Bands (BB)
        basis = close.rolling(self.cfg.get('bb_length')).mean()
        dev = self.cfg.get('bb_mult') * close.rolling(self.cfg.get('bb_length')).std(ddof=0)
        upperBB, lowerBB = basis + dev, basis - dev

        # 计算Keltner Channels (KC)
        ma = close.rolling(self.cfg.get('kc_length')).mean()
        r = self.true_range(df) if self.cfg.get('use_true_range') else (high - low)
        rangema = r.rolling(self.cfg.get('kc_length')).mean()
        upperKC, lowerKC = ma + rangema * self.cfg.get('kc_mult'), ma - rangema * self.cfg.get('kc_mult')

        # 判断Squeeze状态 {"ON":"积压", "OFF":"释放", "NO":无}
        sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
        sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)
        df["sqz_status"] = np.select([sqzOn, sqzOff], ["ON", "OFF"], default="NO")

        # 计算Momentum柱的线性趋势
        highest_h = high.rolling(self.cfg.get('kc_length')).max()
        lowest_l = low.rolling(self.cfg.get('kc_length')).min()
        avg_hl = (highest_h + lowest_l) / 2
        sma_close = close.rolling(self.cfg.get('kc_length')).mean()
        mid = (avg_hl + sma_close) / 2
        source_mid = close - mid

        histogram_value = source_mid.rolling(self.cfg.get('kc_length')).apply(
            lambda x: self.tv_linreg(pd.Series(x), self.cfg.get('kc_length')), raw=False)

        # 动能柱数值
        df["sqz_hvalue"] = histogram_value
        # 前一根动能柱数值
        df["sqz_pre_hvalue"] = histogram_value.shift(1)
        # 积压计数
        df = self.add_squeeze_counter(df)

        # 柱状图颜色
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
        """综合调用所有指标方法"""
        df = df.copy()
        df = self.squeeze_momentum_indicator(df)
        df = self.ema_indicator(df)
        df = self.support_resistance_indicator(df)
        return df


# =====================================================
# 3. 策略引擎 (StrategyEngine)
# =====================================================
class StrategyEngine:
    def __init__(self, st_cfg: dict):
        self.cfg = st_cfg

    def execute(self, df: pd.DataFrame, symbol: str, interval: str) -> Dict[str, Any]:
        cur = df.iloc[-1]
        prev = df.iloc[-2]
        change = (cur['close'] / prev['close'] - 1) * 100

        signal = "No"
        if cur['sqz_status'] == "OFF" and prev['sqz_status'] == "ON" and prev['sqz_id'] >= self.cfg['min_sqz_bars']:
            if cur['close'] > cur['ema200'] and change > 0 and cur['sqz_hcolor'] == "亮绿":
                signal = "Long"
            elif cur['close'] < cur['ema200'] and change < 0 and cur['sqz_hcolor'] == "亮红":
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


# =====================================================
# 4. 通知引擎 (NotifyEngine)
# =====================================================
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
            task = asyncio.create_task(self.broadcast_to_tg(signals, interval))
            self.running_tasks.append(task)
            task.add_done_callback(
                lambda t: self.running_tasks.remove(t) if t in self.running_tasks else None
            )

    async def broadcast_to_tg(self, signal_results, interval):
        async with aiohttp.ClientSession() as session:
            chunk_size = 10
            for i in range(0, len(signal_results), chunk_size):
                chunk = signal_results[i:i + chunk_size]
                header = f"🚀 <b>信号报告【{interval.upper()}】</b>\n"
                header += f"⏰ 扫描时间: {datetime.now().strftime('%H:%M:%S')}\n"
                header += f"━━━━━━━━━━━━━━\n"
                body_parts = []
                for res in chunk:
                    body_parts.append(self.format_single_signal(res, interval))
                final_msg = header + "\n\n".join(body_parts)
                await self.send_raw_tg_message(session, final_msg)
                await asyncio.sleep(0.5)

    @staticmethod
    def format_single_signal(res, interval):
        symbol = res.get('symbol', 'Unknown')
        tv_symbol = symbol.replace("-SWAP", "").replace("-", "")
        tv_url = f"https://cn.tradingview.com/chart/pvCjwkIK/?symbol=OKX%3A{tv_symbol}.P"
        symbol_link = f'<a href="{tv_url}">{tv_symbol}</a>'

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

        msg_text = (
            f"⚡ <b>信号【{interval.upper()}】</b> <b>{symbol_link}</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🔄 <b>时间:</b> <code>{res.get('time', '-')}（UTC+8）</code>\n"
            f"💹 <b>信号:</b> <code>{signal_text}</code>\n"
            f"💰 <b>价格:</b> <code>{price}{change_str}</code>\n"
            f"🧨 <b>挤压:</b> <code>{res.get('bars', 0)} Bars</code>\n"
            f"📊 <b>动能:</b> {mom_icons if mom_icons else '无'}\n"
            f"🚀 <b>趋势:</b> {trend_icons if trend_icons else '无'}\n"
            f"📅 <b>日期:</b> <code>{res.get('date', '-')}</code>\n"
        )
        return msg_text

    async def send_raw_tg_message(self, session, msg_text):
        token = self.cfg.get('TG_TOKEN')
        chat_id = self.cfg.get('TG_CHAT_ID')
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id, "text": msg_text, "parse_mode": "HTML",
            "disable_web_page_preview": True, "disable_notification": False
        }
        try:
            async with session.post(url, data=payload, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"TG 发送失败 [{resp.status}]: {await resp.text()}")
        except Exception as e:
            logger.error(f"TG 网络异常: {e}")


# =====================================================
# 5. 定时引擎 (TimeEngine)
# =====================================================
class TimeEngine:
    @staticmethod
    def get_wait_seconds(interval: str) -> float:
        now = datetime.now()
        val = int(interval[:-1])
        unit = interval[-1].lower()

        if unit == 'm':
            offset_sec = 10
        elif unit == 'h':
            offset_sec = 120
        elif unit == 'd':
            offset_sec = 300
        else:
            offset_sec = 5

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


# =====================================================
# 6. 扫描引擎 (ScanEngine)
# =====================================================
class ScanEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.data_e = DataEngine(cfg['api'])
        self.ind_e = IndicatorEngine(cfg['strategy'])
        self.strat_e = StrategyEngine(cfg['strategy'])
        self.notify_e = NotifyEngine(cfg['notify'])
        self.timer = TimeEngine()

    async def _proc_symbol(self, session, symbol, interval, sem):
        async with sem:
            try:
                raw = await self.data_e.fetch_klines(session, symbol, interval)
                if raw is None:
                    return None
                if len(raw) < 200:
                    return None
                df = self.ind_e.calculate(raw)
                res = self.strat_e.execute(df, symbol, interval)
                return res
            except Exception as e:
                logger.error(f"💥 {symbol} 崩溃: {e}", exc_info=True)
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
            wait_sec = self.timer.get_wait_seconds(interval)
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)
            current_slot = datetime.now().replace(second=0, microsecond=0)
            if last_run_slot == current_slot:
                await asyncio.sleep(1)
                continue
            try:
                start_time = time.time()
                symbols = self.cfg.get("watch_list") or await self.data_e.get_active_symbols(session)
                await self.scan_cycle(session, symbols, interval)
                if self.notify_e.running_tasks:
                    await asyncio.gather(*self.notify_e.running_tasks)
                last_run_slot = current_slot
                logger.info(f"✅ [{interval}] 扫描完成，耗时: {time.time() - start_time:.2f}s")
            except Exception as e:
                logger.error(f"❌ [{interval}] 异常: {e}")
                await asyncio.sleep(10)

    @staticmethod
    async def heartbeat_worker():
        while True:
            logger.info("💓 机器人运行中...")
            await asyncio.sleep(8 * 3600)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            try:
                symbols = self.cfg.get("watch_list")
                if not symbols or len(symbols) == 0:
                    symbols = await self.data_e.get_active_symbols(session)
                if symbols:
                    await self.scan_cycle(session, symbols, "1H")
            except Exception as e:
                logger.error(f"❌ 初始扫描发生崩溃: {e}", exc_info=True)

            workers = [self.interval_worker(session, i) for i in self.cfg.get('intervals')]
            workers.append(self.heartbeat_worker())
            await asyncio.gather(*workers)


# =====================================================
# Hugging Face 入口函数
# =====================================================
async def handle_health(request):
    return web.Response(text="Bot is running", content_type='text/html')


async def main():
    # 启动健康检查 Web 服务器（Hugging Face 必要）
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 7860)  # HF 默认端口
    await site.start()

    # 运行原本的机器人逻辑
    scanner = ScanEngine(CONFIG)
    await scanner.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.error("APP 已手动停止")