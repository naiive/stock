#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import numpy as np
import pandas as pd
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional, Any
from conf.config import TELEGRAM_CONFIG

# =====================================================
# 0. 配置中心 (CONFIG)
# =====================================================
CONFIG = {
    "watch_list": [],           # 留空则自动获取全市场高成交额品种
    "intervals": ["1h", "4h", "1d"],        # 监听的时间周期

    "api": {
        "BASE_URL": "https://fapi.binance.com",
        "TOP_N": 50,            # 自动抓取成交额前50的品种
        "MAX_CONCURRENT": 8,    # 最大并发请求数
        "KLINE_LIMIT": 1000,    # K线数量
        "EXCLUDE_TOKENS": ["USDC", "FDUSD", "DAI", "EUR"] # 排除稳定币之类的
    },

    "strategy": {
        "bb_length": 20,        # 布林带周期
        "bb_mult": 2.0,         # 布林带标准差倍数
        "kc_length": 20,        # 肯特纳通道周期
        "kc_mult": 1.2,         # 肯特纳通道倍数 (Squeeze核心参数)
        "use_true_range": True, # True真实波动幅度/简单波动范围

        "ema_length": 200,      # 长期趋势过滤

        "srb_left": 15,         # 支撑压力左侧强度
        "srb_right": 15,        # 支撑压力右侧强度

        "min_sqz_bars": 6       # 至少6根K线才视为有效挤压
    },

    "notify": {
        "CONSOLE_LOG": True,    # 控制台日志输出
        "TG_ENABLE": True,      # telegram bot 发送
        "TG_TOKEN": TELEGRAM_CONFIG.get("BOT_TOKEN"),
        "TG_CHAT_ID": TELEGRAM_CONFIG.get("CHAT_ID")
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

    async def get_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        """获取全市场成交额排名前 N 的合约"""
        url = f"{self.cfg['BASE_URL']}/fapi/v1/ticker/24hr"
        try:
            async with session.get(url, timeout=10) as r:
                if r.status != 200:
                    logger.error(f"币安 API 响应异常: {r.status}")
                    return []
                data = await r.json()

                # 检查 data 是否为列表，如果不是（报错信息通常是字典），则返回空
                if not isinstance(data, list):
                    logger.error(f"API 返回数据格式错误: {data}")
                    return []
                df = pd.DataFrame(data)
                # 过滤 USDT 交易对且排除稳定币
                df = df[df['symbol'].str.endswith('USDT')]
                for token in self.cfg.get('EXCLUDE_TOKENS', []):
                    df = df[~df['symbol'].str.contains(token)]

                df['quoteVolume'] = pd.to_numeric(df['quoteVolume'], errors='coerce')
                # 剔除空值并排序
                df = df.dropna(subset=['quoteVolume'])

                top_symbols = df.sort_values('quoteVolume', ascending=False).head(self.cfg['TOP_N'])['symbol'].tolist()
                logger.info(f"✅ 成功获取活跃币种数量: {len(top_symbols)}")
                return top_symbols
        except Exception as e:
            logger.error(f"获取活跃币种失败: {e}")  # 这里的错误就是日志里看到的那个
            return []

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        """抓取K线数据"""
        url = f"{self.cfg['BASE_URL']}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": self.cfg['KLINE_LIMIT']}
        try:
            async with session.get(url, params=params, timeout=10) as r:
                if r.status != 200: return None
                data = await r.json()
                df = pd.DataFrame(data, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'cts', 'qv', 'tr', 'tb', 'tq', 'i'])

                # ******剔除最新的一条（尚未闭合的 K 线）******
                if len(df) > 0:
                    df = df.iloc[:-1].copy()
                # ******剔除最新的一条（尚未闭合的 K 线）******

                df = df[['ts', 'o', 'h', 'l', 'c', 'v']].astype(float)
                df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
                # 时间转换为北京时间
                df['date'] = pd.to_datetime(df['ts'], unit='ms') + timedelta(hours=8)
                df.set_index('date', inplace=True)
                return df
        except Exception as e:
            logger.error(f"抓取K线数据失败: {e}")
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
        # 通过移动平均+标准差计算BB上下轨
        basis = close.rolling(self.cfg.get('bb_length')).mean()
        dev = self.cfg.get('kc_mult') * close.rolling(self.cfg.get('bb_length')).std(ddof=0)
        upperBB, lowerBB = basis + dev, basis - dev

        # 计算Keltner Channels (KC)
        # 通过ATR或高低差计算KC上下轨
        # 用于判断市场是否处于低波动（挤压）状态
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
        # 柱状图值大小，0轴上为正，0轴下为负
        histogram_value = source_mid.rolling(self.cfg.get('kc_length')).apply(lambda x: self.tv_linreg(pd.Series(x), self.cfg.get('kc_length')), raw=False)

        # 动能柱数值
        df["sqz_hvalue"] = histogram_value
        # 前一根动能柱数值，用于判断动能柱颜色：亮绿色、绿色、亮红色、红色
        df["sqz_pre_hvalue"] = histogram_value.shift(1)
        # 给每根K线打上一个连续积压或释放计数值，用于判断连续积压
        df = self.add_squeeze_counter(df)

        # 柱状图颜色
        df["sqz_hcolor"] = df.apply(
            lambda re: self.get_squeeze_momentum_histogram_color(re["sqz_hvalue"], re["sqz_pre_hvalue"]), axis=1)

        # 删除一些中间结果列
        df.drop(columns=["sqz_pre_hvalue"], inplace=True)

        return df

    def ema_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        df[f"ema{self.cfg.get('ema_length')}"] = df['close'].ewm(span=self.cfg.get('ema_length'), adjust=False).mean()

        return df

    def support_resistance_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        # 总窗口长度
        window = self.cfg.get('srb_left') + self.cfg.get('srb_right') + 1

        # 1. 寻找局部极值点 (Pivot High / Pivot Low)
        # center=True 配合窗口计算，判断中间那根 K 线是否是区间最值
        df['is_min'] = df['low'].rolling(window, center=True).apply(
            lambda x: 1 if x[self.cfg.get('srb_left')] == np.min(x) else 0, raw=True
        )
        df['is_max'] = df['high'].rolling(window, center=True).apply(
            lambda x: 1 if x[self.cfg.get('srb_left')] == np.max(x) else 0, raw=True
        )

        # 2. 标记价格并向前填充 (Forward Fill)
        # 当发现新的分型点时更新价格，否则沿用之前的价格线
        df['srb_sup'] = np.where(df['is_min'] == 1, df['low'], np.nan)
        df['srb_res'] = np.where(df['is_max'] == 1, df['high'], np.nan)

        df['srb_sup'] = df['srb_sup'].ffill()
        df['srb_res'] = df['srb_res'].ffill()

        df.drop(columns=['is_min', 'is_max'], inplace=True)

        return df

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """综合调用所有指标方法"""
        df = df.copy()

        # 1. 计算Squeeze
        df = self.squeeze_momentum_indicator(df)

        # 2. 计算趋势过滤
        df = self.ema_indicator(df)

        # 3. 计算支撑阻力
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

        signal = "No"
        if cur['sqz_status'] == "OFF" and prev['sqz_status'] == "ON" and prev['sqz_id'] >= self.cfg['min_sqz_bars']:
            if cur['close'] > cur['ema200'] and cur['sqz_hvalue'] > 0:
                signal = "Long"
            elif cur['close'] < cur['ema200'] and cur['sqz_hvalue'] < 0:
                signal = "Short"

        energy, tr, ts = [], [], []

        for i in range(6, 0, -1):
            row = df.iloc[-(i + 1)]
            energy.append(f"{row['sqz_hcolor']}[{row['sqz_hvalue']:+.2f}]")
            tr.append("高" if row['close'] > cur['srb_res'] else "低")
            ts.append("高" if row['close'] > cur['srb_sup'] else "低")

        change = (cur['close'] / prev['close'] - 1) * 100
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
        # 过滤 None
        results_list = [r for r in results if r is not None]
        if not results_list:
            return

        # 统计产生信号的数量
        signals = [r for r in results_list if r.get('signal') != "No"]

        # 1. 控制台打印
        if self.cfg.get('CONSOLE_LOG'):
            logger.info(f"[{interval}] 扫描完成 | 监控品种: {len(results_list)} | 触发信号: {len(signals)}")
            for item in results_list:
                symbol = item.get('symbol', 'Unknown')
                json_str = json.dumps(item, ensure_ascii=False)
                log_prefix = f"[{interval}] {symbol.ljust(12)}"
                if item.get('signal') != "No":
                    logger.info(f"{log_prefix} | Y | {json_str}")
                else:
                    logger.info(f"{log_prefix} | N | {json_str}")

        # 2. Telegram：合并发送
        if self.cfg.get('TG_ENABLE') and signals:
            # 修改点：直接把所有有信号的结果传给 broadcast_to_tg
            task = asyncio.create_task(self.broadcast_to_tg(signals, interval))
            self.running_tasks.append(task)
            task.add_done_callback(
                lambda t: self.running_tasks.remove(t) if t in self.running_tasks else None
            )

    async def broadcast_to_tg(self, signal_results, interval):
        """
        合并信号并分段发送（每 10 个信号合并为一条消息）
        """
        async with aiohttp.ClientSession() as session:
            # 设置每条消息合并的数量，防止单条消息过长
            chunk_size = 10
            for i in range(0, len(signal_results), chunk_size):
                chunk = signal_results[i:i + chunk_size]

                # 构建合并后的消息头部
                header = f"🚀 <b>信号报告【{interval.upper()}】</b>\n"
                header += f"⏰ 扫描时间: {datetime.now().strftime('%H:%M:%S')}\n"
                header += f"━━━━━━━━━━━━━━\n"

                body_parts = []
                for res in chunk:
                    body_parts.append(self.format_single_signal(res, interval))

                final_msg = header + "\n\n".join(body_parts)

                # 发送合并后的消息
                await self.send_raw_tg_message(session, final_msg)
                await asyncio.sleep(0.5)

    @staticmethod
    def format_single_signal(res, interval):
        """
        将单个信号格式化为字符串片段（原 send_to_telegram 的逻辑搬迁到这里）
        """
        symbol = res.get('symbol', 'Unknown')
        tv_url = f"https://cn.tradingview.com/chart/pvCjwkIK/?symbol=BINANCE%3A{symbol}"
        symbol_link = f'<a href="{tv_url}">{symbol}</a>'

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

        # 动能图标
        energy_str = str(res.get('energy', ""))
        energy_items = energy_str.split('-') if energy_str else []
        recent_items = energy_items[-6:]
        mom_icons = "".join(["🟢" if "绿" in item else "🔴" for item in recent_items])

        # 趋势图标
        trend_list = trend_str.split('-') if trend_str else []
        trend_icons = "".join(["⬆️" if "高" in t else "⬇️" for t in trend_list[-6:]]) if trend_list else ""

        # 6. 构建消息模板
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
        """
        最终的 TG 发送底层逻辑
        """
        token = self.cfg.get('TG_TOKEN')
        chat_id = self.cfg.get('TG_CHAT_ID')
        url = f"https://api.telegram.org/bot{token}/sendMessage"

        payload = {
            "chat_id": chat_id,
            "text": msg_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": False
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

        # 1. 先确定延迟偏移量 (单位：秒)
        if unit == 'm':
            offset_sec = 10
        elif unit == 'h':
            offset_sec = 120  # 2分钟
        elif unit == 'd':
            offset_sec = 300  # 5分钟
        else:
            offset_sec = 5

        # 2. 计算基础对齐时间点 (不带 offset 的整点)
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

        # 3. 使用 timedelta 加上偏移量，而不是在 replace 里改 second
        next_run = base_time + timedelta(seconds=offset_sec)

        # 4. 计算差值
        wait_sec = (next_run - now).total_seconds()

        # 如果当前就在延迟窗内（wait_sec 为负），则强制返回 1 秒后执行或跳到下一周期
        return wait_sec if wait_sec > 0 else 1.0


# =====================================================
# 5. 扫描引擎 (ScanEngine) - 适配定时器
# =====================================================
class ScanEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        # 数据引擎
        self.data_e = DataEngine(cfg['api'])
        # 指标引擎
        self.ind_e = IndicatorEngine(cfg['strategy'])
        # 策略引擎
        self.strat_e = StrategyEngine(cfg['strategy'])
        # 通知引擎
        self.notify_e = NotifyEngine(cfg['notify'])
        # 定时引擎
        self.timer = TimeEngine()

    async def _proc_symbol(self, session, symbol, interval, sem):
        """单个币种的处理流水线"""
        async with sem:
            try:
                raw = await self.data_e.fetch_klines(session, symbol, interval)
                if raw is None or len(raw) < 300: return None
                df = self.ind_e.calculate(raw)
                return self.strat_e.execute(df, symbol, interval)
            except Exception as e:
                logger.error(f"处理 {symbol} 失败: {e}")
                return None

    async def scan_cycle(self, session, symbols, interval):
        """单次循环调度"""
        sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])
        tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]
        results = list(await asyncio.gather(*tasks))
        # 这里的 process_results 内部会过滤没有信号的数据并发送 TG
        self.notify_e.process_results(results, interval)

    async def interval_worker(self, session, interval):
        logger.info(f"🟢 [{interval}] 周期监控任务已启动")

        # 记录上一次成功执行的“时间槽”，防止在同一个周期内重复触发
        last_run_slot = None

        while True:
            # 1. 计算距离“下一次”对齐点的时间
            wait_sec = self.timer.get_wait_seconds(interval)

            # 2. 只有在需要等待时才休眠
            if wait_sec > 0:
                # 计算目标时间用于日志展示
                target_time = (datetime.now() + timedelta(seconds=wait_sec)).strftime('%H:%M:%S')
                logger.info(f"💤 [{interval}] 下次对齐点: {target_time} (等待 {int(wait_sec)}s)")
                await asyncio.sleep(wait_sec)

            # 3. 确定当前的时间槽（例如 11:00），防止重复扫描
            # 如果 get_wait_seconds 逻辑正确，这里其实是双保险
            current_slot = datetime.now().replace(second=0, microsecond=0)
            if last_run_slot == current_slot:
                # 如果当前分钟已经跑过了，强制休眠一小会儿避开这个槽位
                await asyncio.sleep(1)
                continue

            try:
                start_time = time.time()

                # 执行扫描逻辑
                symbols = self.cfg.get("watch_list") or await self.data_e.get_active_symbols(session)
                await self.scan_cycle(session, symbols, interval)

                # 确保 TG 消息发出
                if self.notify_e.running_tasks:
                    await asyncio.gather(*self.notify_e.running_tasks)

                # 标记本次槽位已完成
                last_run_slot = current_slot

                duration = time.time() - start_time
                logger.info(f"✅ [{interval}] 扫描完成，耗时: {duration:.2f}s")

            except Exception as e:
                logger.error(f"❌ [{interval}] 异常: {e}")
                # 报错后不要立即重试，防止死循环轰炸 API
                await asyncio.sleep(min(wait_sec, 30) if wait_sec > 0 else 10)

    @staticmethod
    async def heartbeat_worker():
        """独立的心跳协程"""
        while True:
            logger.info("💓 机器人运行中，系统心跳正常")
            await asyncio.sleep(8 * 3600)

    async def run(self):
        """总入口：并发启动所有周期的 Worker"""
        async with aiohttp.ClientSession() as session:

            # --- 启动时立即触发一次 1h 扫描 ---
            logger.info("⚡ 正在执行启动即时扫描 (Manual Trigger)...")
            symbols = self.cfg.get("watch_list") or await self.data_e.get_active_symbols(session)
            await self.scan_cycle(session, symbols, "1h")
            # --- 启动时立即触发一次 1h 扫描 ---

            workers = []

            # 1. 为每个周期创建一个独立的 Worker
            for interval in self.cfg.get('intervals'):
                workers.append(self.interval_worker(session, interval))

            # 2. 加入心跳 Worker
            workers.append(self.heartbeat_worker())

            # 3. 并行运行所有死循环协程
            await asyncio.gather(*workers)


if __name__ == "__main__":
    scanner = ScanEngine(CONFIG)
    try:
        asyncio.run(scanner.run())
    except KeyboardInterrupt:
        logger.error("APP启动出错")
