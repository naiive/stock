#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import logging
import asyncio
import aiohttp
from aiohttp import web
from typing import Dict, Optional, Any
from cryptography.fernet import Fernet


# =====================================================
# 0. 配置中心 (CONFIG)
# =====================================================
CONFIG = {
    "watch_list" : ["XAU/USD", "TSLA"],

    # 监听的时间周期
    "intervals": ["5M", "1H"],

    "api": {
        "TWELVE_DATA_URL": None, # Twelve Data API Url
        "TWELVE_DATA_KEY": None, # Twelve Data API Key
        "MAX_CONCURRENT": 1, # 免费版建议设为 1 最大进程数
        "KLINE_LIMIT": 500,  # K线获取数量
        "MIN_INTERVAL": 2    # 串行等待时间 2 秒
    },

    "strategy": {
        "bb_length": 20,        # 布林带周期
        "bb_mult": 2.0,         # 布林带标准差倍数
        "kc_length": 20,        # 肯特纳通道周期
        "kc_mult": 1.2,         # 肯特纳通道倍数 (Squeeze核心参数)
        "use_true_range": True, # True真实波动幅度/简单波动范围
        "min_sqz_bars": 6,      # 至少6根K线才视为有效挤压

        "ema_length": 200,      # EMA

        "srb_left": 15,         # 支撑压力左侧
        "srb_right": 15,        # 支撑压力右侧

        "adx_length": 14,       # ADX长度
        "adx_threshold": 25,    # ADX水平【指标不使用，只是用作判断】
    },

    "time": {
        # 市场开盘逻辑分组
        "market_groups": {
            "forex_gold": ["XAU", "OIL", "USD", "EUR", "GBP"], # 黄金、原油、外汇
            "us_stocks": ["TSLA", "AAPL", "NVDA", "MSFT", "AMZN", "META"] # 美股
            }
    },

    "notify": {
        "CONSOLE_LOG": True,     # 控制台日志输出
        "WECOM_ENABLE": True,    # wecom机器人
        "TG_ENABLE": False,      # telegram bot 发送

        "WECOM_WEBHOOK": None,
        "TG_TOKEN": None,
        "TG_CHAT_ID": None
    }
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


# =====================================================
# 1. 数据引擎 (DataEngine)
# =====================================================
class DataEngine:
    def __init__(self, cfg: dict, market_cfg: dict):
        """
        cfg: CONFIG['api']
        market_cfg: CONFIG['time'] (包含 market_groups)
        """
        self.cfg = cfg
        self.market_cfg = market_cfg
        self.api_url = cfg.get('TWELVE_DATA_URL')
        self.api_key = cfg.get('TWELVE_DATA_KEY')

        # 频率控制：Twelve Data 免费版 8次/分钟
        self._request_lock = asyncio.Lock()
        self._last_request_time = 0
        self._min_interval = cfg.get('MIN_INTERVAL')
        self._kline_limit = cfg.get('KLINE_LIMIT')

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str, interval) -> Optional[pd.DataFrame]:
        """
        通过 Twelve Data 获取 K 线数据
        """
        # 适配 Twelve Data 周期格式
        interval_lower = interval.lower()

        # 2. 严谨的周期转换映射
        if "m" in interval_lower and "min" not in interval_lower:
            # 处理 "5m" -> "5min", "15m" -> "15min"
            td_interval = interval_lower.replace("m", "min")
        elif "h" in interval_lower:
            # Twelve Data 接受 "1h", "4h" 等格式，确保是小写即可
            td_interval = interval_lower
        elif "d" in interval_lower:
            # 处理 "1d" 或 "1D" -> "1day"
            td_interval = "1day"
        else:
            # 备用：如果没有匹配到，尝试原样输出或给个默认值
            td_interval = interval_lower

        params = {
            "symbol": symbol,
            "interval": td_interval,
            "outputsize": self._kline_limit,
            "apikey": self.api_key,
            "timezone": "Asia/Shanghai"
        }

        # 频率保护：使用 Lock 确保 ScanEngine 并发抓取时自动排队
        async with self._request_lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)

            try:
                async with session.get(self.api_url, params=params, timeout=15) as r:
                    self._last_request_time = time.time()

                    if r.status == 429:
                        logger.error("🚨 Twelve Data 触发频率限制，请检查间隔设置")
                        return None

                    res = await r.json()
                    if res.get("status") == "error":
                        logger.error(f"❌ API报错: {res.get('message')}")
                        return None

                    values = res.get('values', [])
                    if not values:
                        return None

                    # 转换为标准 DataFrame
                    df = pd.DataFrame(values)
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df.set_index('datetime', inplace=True)
                    df.index.name = 'date'

                    # 整理列并重排时间（Twelve Data 默认返回最新在前的逆序，需反转）
                    df = df[['open', 'high', 'low', 'close']].astype(float)
                    return df.sort_index()

            except Exception as e:
                logger.error(f"💥 {symbol} 抓取异常: {e}")
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

    @staticmethod
    def wilder_smoothing(series: pd.Series, length: int):
        """
        实现 Pine Script 中 ADX/DI 所使用的 Wilder's Smoothing 逻辑。
        SmoothedValue = Prev_SmoothedValue - (Prev_SmoothedValue / length) + CurrentValue
        """
        # 转换为 numpy 数组以便进行迭代计算
        values = series.values
        smoothed = np.empty_like(values)
        smoothed.fill(np.nan)

        # 初始化第一个值（通常为前 length 个值的 SMA，但Pine Script中是基于累积的逻辑）
        # 在许多技术分析库中，第一个平滑值直接使用前 length 个值的简单平均。
        # 为了简化且不引入复杂的迭代，我们采用技术分析库常用的惯例：
        # 第一个平滑值设置为前 length 个值的 SMA
        smoothed[length - 1] = np.sum(values[:length])

        # 从第 length 个值开始应用 Wilder's Smoothing
        for i in range(length, len(values)):
            smoothed[i] = smoothed[i - 1] - (smoothed[i - 1] / length) + values[i]

        return pd.Series(smoothed, index=series.index)

    def adx_di_indicator(self, df: pd.DataFrame) -> pd.DataFrame:

        length =  self.cfg.get('adx_length')
        threshold =  self.cfg.get('adx_threshold')

        # --- 1. 计算 True Range (TR) ---
        high_low = df['high'] - df['low']
        high_prev_close = np.abs(df['high'] - df['close'].shift(1))
        low_prev_close = np.abs(df['low'] - df['close'].shift(1))

        df['TrueRange'] = high_low.combine(high_prev_close, max).combine(low_prev_close, max)

        # --- 2. 计算 Directional Movement (+DM, -DM) ---
        up_move = df['high'] - df['high'].shift(1)
        down_move = df['low'].shift(1) - df['low']

        # +DM 逻辑: UpMove > DownMove 且 UpMove > 0
        df['adx_plus'] = np.where((up_move > down_move) & (up_move > 0), up_move, 0)

        # -DM 逻辑: DownMove > UpMove 且 DownMove > 0
        df['adx_minus'] = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

        # --- 3. Wilder's Smoothing (TR, +DM, -DM) ---
        df['SmoothedTR'] = self.wilder_smoothing(df['TrueRange'], length)
        df['SmoothedDMPlus'] = self.wilder_smoothing(df['adx_plus'], length)
        df['SmoothedDMMinus'] = self.wilder_smoothing(df['adx_minus'], length)

        # --- 4. 计算 +DI 和 -DI ---
        # 乘以 100
        df['adx_plus'] = (df['SmoothedDMPlus'] / df['SmoothedTR']) * 100
        df['adx_minus'] = (df['SmoothedDMMinus'] / df['SmoothedTR']) * 100

        # --- 5. 计算 DX (Directional Index) ---
        # DX = |+DI - -DI| / (+DI + -DI) * 100
        # 避免除以零
        sum_di = df['adx_plus'] + df['adx_minus']
        df['DX'] = np.where(sum_di != 0, np.abs(df['adx_plus'] - df['adx_minus']) / sum_di * 100, 0)

        # --- 6. 计算 ADX (DX 的 SMA) ---
        # Pine Script 中 ADX = sma(DX, len)。在 ADX/DMI 系统中，这通常也意味着 Wilder's Smoothing
        # 但为严格遵循您的 Pine Script 代码，我们使用标准的 SMA：
        df['adx'] = df['DX'].rolling(window=length).mean()
        df['adx_threshold'] = threshold

        # --- 7. 删除一些中间结果列 ---
        df.drop(columns=['TrueRange', 'SmoothedTR', 'SmoothedDMPlus', 'SmoothedDMMinus', 'DX'], inplace=True)

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

        # 4. 计算ADX
        df = self.adx_di_indicator(df)

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
        # 涨幅
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
            "adx": round(cur['adx'], 4),
            "adx_threshold": int(cur['adx_threshold']),
            "energy": "-".join(energy),
            "support": round(cur['srb_sup'], 4),
            "resistance": round(cur['srb_res'], 4),
            "trend_r": "-".join(tr),
            "trend_s": "-".join(ts)
        }


# =====================================================
# 4. 通知引擎 (NotifyEngine)
# =====================================================
class NotifyEngine:
    def __init__(self, notify_cfg: dict, time_cfg: dict):
        self.cfg = notify_cfg
        self.time_cfg = time_cfg
        self.running_tasks = []

    # 主控流程
    def process_results(self, results: list, interval: str):
        """不同渠道消息通知：控制台、telegram、企微"""
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
                log_prefix = f"[{interval}] {symbol.ljust(20)}"
                if item.get('signal') != "No":
                    logger.info(f"{log_prefix} | Y | {json_str}")
                else:
                    logger.info(f"{log_prefix} | N | {json_str}")

        # 2. Telegram合并发送
        if self.cfg.get('TG_ENABLE') and signals:
            task = asyncio.create_task(self.tg_broadcast_and_send(signals, interval))
            self.running_tasks.append(task)
            task.add_done_callback(lambda t: self.running_tasks.remove(t) if t in self.running_tasks else None)

        # 3. 企业微信通知合并发送
        if self.cfg.get('WECOM_ENABLE') and signals:
            task = asyncio.create_task(self.wecom_broadcast_and_send(signals, interval))
            self.running_tasks.append(task)
            task.add_done_callback(lambda t: self.running_tasks.remove(t) if t in self.running_tasks else None)

    # 共用消息卡片组装
    def format_single_signal(self, res, interval, tag):
        """
        将单个信号格式化为字符串片段
        """
        symbol = res.get('symbol', 'Unknown')

        tv_symbol = symbol.upper().replace("/", "")

        # 2. 从配置中读取分组，动态判断交易所前缀
        # 注意：这里假设 NotifyEngine 实例化时传入了包含 market_groups 的配置
        groups = self.time_cfg.get("market_groups", {})

        forex_list = groups.get("forex_gold", [])
        stocks_list = groups.get("us_stocks", [])

        # 逻辑判断：
        if any(k in tv_symbol for k in stocks_list):
            exchange = "NASDAQ"
        elif any(k in tv_symbol for k in forex_list):
            # 黄金和原油在 TV 上通常用 TVC 前缀更准确
            exchange = "FX"
        else:
            logger.error("没有配置对应的跳转链接")
            exchange = ""

        # 组装跳转链接
        tv_url = f"https://cn.tradingview.com/chart/?symbol={exchange}%3A{tv_symbol}"

        price = res.get('price', 0)
        change = res.get('change', 0)
        change_str = f"({'+' if change >= 0 else ''}{change}%)"

        ema200 = res.get('ema200', 0)
        adx = res.get('adx', 0)
        adx_threshold = res.get('adx_threshold', 0)
        support = res.get('support', 0)
        resistance = res.get('resistance', 0)

        raw_signal = res.get('signal', 'No')
        if raw_signal == "Long":
            signal_text = "🟢 Long"
            trend_str = str(res.get('trend_r', ""))
            e_b = "📈EMA" if price > ema200 else "📉EMA"
            r_b = "📈压力" if price > resistance else "📉压力"
            a_b = "📈ADX" if adx > adx_threshold else "📉ADX"
            judge_text = f"{e_b}{r_b}{a_b}"
        elif raw_signal == "Short":
            signal_text = "🔴 Short"
            trend_str = str(res.get('trend_s', ""))
            e_b = "📈EMA" if price > ema200 else "📉EMA"
            r_b = "📈支撑" if price > support else "📉支撑"
            a_b = "📈ADX" if adx > adx_threshold else "📉ADX"
            judge_text = f"{e_b}{r_b}{a_b}"
        else:
            signal_text = "No"
            trend_str = str(res.get('trend_r', ""))
            e_b = "🟰EMA"
            r_b = "🟰支撑"
            a_b = "🟰ADX"
            judge_text = f"{e_b}{r_b}{a_b}"

        # 动能图标
        energy_str = str(res.get('energy', ""))
        energy_items = energy_str.split('-') if energy_str else []
        recent_items = energy_items[-6:]
        mom_icons = "".join(["🟢" if "绿" in item else "🔴" for item in recent_items])

        # 趋势图标
        trend_list = trend_str.split('-') if trend_str else []
        trend_icons = "".join(["⬆️" if "高" in t else "⬇️" for t in trend_list[-6:]]) if trend_list else ""

        # telegram
        if tag == "telegram":
            # url
            symbol_link = f'<a href="{tv_url}">{tv_symbol}</a>'
            # 消息模板
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

        # wecom
        elif tag == "wecom":
            # url
            symbol_link = f'[{tv_symbol}]({tv_url})'
            # 消息模板
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
            logger.error("没有对应的消息卡片，请检查")
            return None

    # telegram
    async def tg_broadcast_and_send(self, signal_results, interval, tag="telegram"):
        """
        合并信号并分段发送（每 10 个信号合并为一条消息）
        """
        token = self.cfg.get('TG_TOKEN')
        chat_id = self.cfg.get('TG_CHAT_ID')
        url = f"https://api.telegram.org/bot{token}/sendMessage"

        chunk_size = 10

        # 记录发送的消息条数
        total_signals = len(signal_results)

        async with aiohttp.ClientSession() as session:
            for i in range(0, len(signal_results), chunk_size):
                chunk = signal_results[i:i + chunk_size]

                # 消息头
                header = (
                    f"🏛️ <b>外汇【{interval.upper()}】周期</b>\n"
                    f"⏰ 扫描时间 {datetime.now().strftime('%H:%M:%S')}\n"
                    f"━━━━━━━━━━━\n"
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

    # wecom
    async def wecom_broadcast_and_send(self, signal_results, interval, tag="wecom"):
        """
        wecom 合并信号并分段发送（每 8 个信号合并为一条消息）
        """
        webhook_url = self.cfg.get('WECOM_WEBHOOK')
        if not webhook_url:
            return

        chunk_size = 8  # wecom 4096 字节限制

        # 记录发送的消息条数
        total_signals = len(signal_results)

        async with aiohttp.ClientSession() as session:
            for i in range(0, len(signal_results), chunk_size):
                chunk = signal_results[i:i + chunk_size]

                header = (
                    f"🏛️ 外汇【{interval.upper()}】周期\n"
                    f"⏰ 扫描时间 {datetime.now().strftime('%H:%M:%S')}\n"
                    f"━━━━━━━━━━━\n"
                )

                body_parts = []
                for res in chunk:
                    text = self.format_single_signal(res, interval, tag)
                    if text:
                        body_parts.append(text.rstrip())  # 防止尾部空行

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

    # 失效通知
    async def send_error_msg(self, error_text: str):
        """当接口失效或无数据时，根据配置发送报警"""
        tasks = []
        # 1. 发送到企业微信
        if self.cfg.get('WECOM_ENABLE'):
            webhook_url = self.cfg.get('WECOM_WEBHOOK')
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"⚠️ **Twelve系统异常报警**\n\n> 详情: {error_text}\n> 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            }
            tasks.append(asyncio.create_task(self._post_request(webhook_url, payload, "wecom_err")))

        # 2. 发送到 Telegram
        if self.cfg.get('TG_ENABLE'):
            token = self.cfg.get('TG_TOKEN')
            chat_id = self.cfg.get('TG_CHAT_ID')
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": f"⚠️ <b>Twelve系统异常报警</b>\n\n详情: {error_text}",
                "parse_mode": "HTML"
            }
            tasks.append(asyncio.create_task(self._post_request(url, payload, "tg_err")))

        if tasks:
            await asyncio.gather(*tasks)

    # 心跳通知
    async def send_heartbeat(self):
        """发送系统心跳存活通知"""
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = (
            f"💓 **Twelve机器人**\n"
            f"状态: 心跳正常\n"
            f"时间: {now_str}\n"
        )

        tasks = []
        # 按照配置发送到对应渠道
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

    # 异步POST请求
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


# =====================================================
# 5. 定时引擎 (TimeEngine)
# =====================================================
class TimeEngine:
    def __init__(self, time_cfg: dict):
        self.cfg = time_cfg

    @staticmethod
    def get_wait_seconds(interval: str) -> float:
        now = datetime.now()
        val = int(interval[:-1])
        unit = interval[-1].lower()

        # 1. 先确定延迟偏移量 (单位：秒)
        if unit == 'm':
            offset_sec = 3
        elif unit == 'h':
            offset_sec = 30
        elif unit == 'd':
            offset_sec = 60
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

    def is_symbol_market_open(self, symbol: str) -> bool:
        """
        根据配置判断品种是否开盘
        :param symbol: 品种名 (如 XAUUSDm)
        """
        s = symbol.upper()
        now = datetime.now()
        weekday = now.weekday()
        hour = now.hour
        minute = now.minute

        # 自动处理夏令时 (3月-11月)
        is_dst = 3 <= now.month <= 11

        # 获取分组配置
        groups = self.cfg.get("market_groups", {})
        forex_keywords = groups.get("forex_gold", [])
        stock_keywords = groups.get("us_stocks", [])

        # --- A. 匹配外汇/黄金逻辑 ---
        if any(k.upper() in s for k in forex_keywords):
            close_h = 5 if is_dst else 6
            open_h = 6 if is_dst else 7
            if (weekday == 5 and hour >= close_h) or weekday == 6:
                return False  # 周六凌晨关盘后或周日
            if weekday == 0 and hour < open_h:
                return False  # 周一凌晨开盘前
            return True

        # --- B. 匹配美股逻辑 ---
        elif any(k.upper() in s for k in stock_keywords):
            if weekday >= 5: return False  # 周六周日不交易

            # 转换北京时间开盘
            start_h, start_m = (21, 30) if is_dst else (22, 30)
            end_h = 4 if is_dst else 5

            curr_min = hour * 60 + minute
            start_min = start_h * 60 + start_m
            end_min = end_h * 60

            # 跨午夜逻辑：21:30以后 OR 凌晨4:00以前
            if curr_min >= start_min or curr_min < end_min:
                return True
            return False

        # --- C. 默认返回 True (防止遗漏品种) ---
        return True


# =====================================================
# 6. 扫描引擎 (ScanEngine)
# =====================================================
class ScanEngine:
    def __init__(self, cfg: dict):
        # 全局运行状态：True正常，False停机
        self.is_active = True
        # 全配置
        self.cfg = cfg
        # 数据引擎
        self.data_e = DataEngine(cfg['api'], cfg['time'])
        # 指标引擎
        self.ind_e = IndicatorEngine(cfg['strategy'])
        # 策略引擎
        self.strat_e = StrategyEngine(cfg['strategy'])
        # 通知引擎
        self.notify_e = NotifyEngine(cfg['notify'], cfg['time'])
        # 定时引擎
        self.timer_e = TimeEngine(cfg['time'])

    async def _proc_symbol(self, session, symbol, interval, sem):
        """单个币种的处理流水线"""
        async with sem:
            try:
                # 【改动点】：传入 time 节点进行开盘检查
                if not self.timer_e.is_symbol_market_open(symbol):
                    # 如果没开盘，直接安静地返回 None，不浪费 API 次数
                    return None

                raw = await self.data_e.fetch_klines(session, symbol, interval)

                if raw is None:
                    logger.error(f"❌ {symbol} 获取数据失败 (API返回空)")
                    return None

                # 2. 检查数据长度
                data_len = len(raw)
                if data_len < 200:  # 策略计算 EMA200 至少需要 200 条
                    logger.warning(f"⚠️ {symbol} 数据条数不足: {data_len} (需要至少200条)")
                    return None

                # 3. 计算指标
                df = self.ind_e.calculate(raw)

                # 4. 执行策略
                res = self.strat_e.execute(df, symbol, interval)
                return res

            except Exception as e:
                logger.error(f"💥 {symbol} 处理过程中崩溃: {e}", exc_info=True)
                return None

    async def scan_cycle(self, session, symbols, interval):
        """单次循环调度"""
        sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])
        tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]
        results = list(await asyncio.gather(*tasks))
        # 这里的 process_results 内部会过滤没有信号的数据并发送 TG
        self.notify_e.process_results(results, interval)

    async def interval_worker(self, session, interval):
        """
        核心监控工作协程
        :param session: aiohttp 客户端会话
        :param interval: 监控周期，如 '5M', '1H'
        """
        logger.info(f"🟢 [{interval}] 周期监控任务已启动")

        # 1. 状态位初始化
        # last_run_slot: 记录上一次成功执行的时间点（分钟级），防止在同一分钟内重复触发
        last_run_slot = None
        # is_active: 熔断开关。如果接口崩溃，设为 False 以停止后续所有请求
        self.is_active = True

        while True:
            # ==========================================
            # 步骤 A: 熔断检查 (Circuit Breaker)
            # ==========================================
            if not self.is_active:
                logger.critical(f"🛑 [{interval}] 系统已熔断停机。请检查 Token 有效性并手动重启脚本。")
                # 发送停机通知后，退出协程循环，不再占用系统资源
                break

            # ==========================================
            # 步骤 B: 精准定时等待 (Timer)
            # ==========================================
            # 计算距离下一个整点（如 05分, 10分）还剩多少秒
            wait_sec = self.timer_e.get_wait_seconds(interval)
            if wait_sec > 0:
                # 只在长等待时打印日志，避免日志刷屏
                if wait_sec > 10:
                    target_time = (datetime.now() + timedelta(seconds=wait_sec)).strftime('%H:%M:%S')
                    logger.info(f"💤 [{interval}] 下次对齐点: {target_time} (等待 {int(wait_sec)}s)")
                # 无论长短，只要大于0就执行实际的等待
                await asyncio.sleep(wait_sec)

            # ==========================================
            # 步骤 C: 市场开盘状态检查
            # ==========================================
            # 调用之前定义的 is_market_open()，非交易时段不请求接口
            symbols = self.cfg.get("watch_list", [])
            opened_symbols = [s for s in symbols if self.timer_e.is_symbol_market_open(s)]

            if not opened_symbols:
                # 如果当前没有任何一个品种在交易时段（比如周六、周日）
                # 为了省电/省资源，我们每分钟检查一次，并跳过本次循环
                await asyncio.sleep(60)
                continue

            # ==========================================
            # 步骤 D: 重复触发保护
            # ==========================================
            # 确保在同一个 K 线周期内只执行一次扫描
            current_slot = datetime.now().replace(second=0, microsecond=0)
            if last_run_slot == current_slot:
                await asyncio.sleep(1)
                continue

            # ==========================================
            # 步骤 E: 执行核心扫描逻辑
            # ==========================================
            try:
                start_time = time.time()
                symbols = self.cfg.get("watch_list", [])

                if not symbols:
                    logger.warning(f"⚠️ [{interval}] 监控列表为空，跳过本次扫描")
                    await asyncio.sleep(10)
                    continue

                # 1. 并发扫描所有品种
                # 使用信号量控制最大并发数，保护 API 不被封禁
                sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])
                tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]

                # gather 会等待所有任务返回
                results = await asyncio.gather(*tasks)

                # 找出【当前应该处于开盘状态】的品种
                opened_symbols = [s for s in symbols if self.timer_e.is_symbol_market_open(s)]

                # 2. 接口可用性检测 (熔断逻辑核心)
                # 过滤出成功获取到数据的品种
                valid_results = [r for r in results if r is not None]

                # 熔断判定：如果现在有品种该开盘，但我们一个有效结果都没拿到
                if len(opened_symbols) > 0 and len(valid_results) == 0:
                    self.is_active = False  # 触发熔断开关
                    error_msg = (f"🚨 [{interval}] 所有品种接口请求均失败 \n"
                                 f"结果: 系统已自动熔断停机")

                    logger.critical(error_msg)
                    # 发送报警到配置的通知渠道 (TG/WeCom)
                    await self.notify_e.send_error_msg(error_msg)
                    continue

                # 3. 处理并发送信号通知
                # 内部会根据策略结果判断是否需要推送消息
                self.notify_e.process_results(list(results), interval)

                # 4. 确保异步通知任务执行完毕
                if self.notify_e.running_tasks:
                    await asyncio.gather(*self.notify_e.running_tasks)

                # 5. 标记扫描成功
                last_run_slot = current_slot
                duration = time.time() - start_time
                logger.info(
                    f"✅ [{interval}] 扫描完成 (有效:{len(valid_results)}/{len(symbols)}), 耗时: {duration:.2f}s")

            except Exception as e:
                # 捕获循环内的未知异常，防止单个周期报错导致整个脚本崩溃
                logger.error(f"❌ [{interval}] 运行过程中发生未预料异常: {e}", exc_info=True)
                await asyncio.sleep(10)  # 发生异常时等待 10 秒再试

    async def heartbeat_worker(self):
        """独立的心跳协程：每4小时发送一次存活通知"""
        logger.info("💗 心跳监控协程已启动 (周期: 4小时)")

        # 启动时可以先发一条，确认机器人刚启动是好使的
        await self.notify_e.send_heartbeat()

        while True:
            try:
                # 等待 4 小时 (4 * 3600 秒)
                await asyncio.sleep(4 * 3600)

                # 如果系统没有因为故障停机 (is_active 为 True)，则发送心跳
                if self.is_active:
                    await self.notify_e.send_heartbeat()
                else:
                    logger.warning("💓 心跳跳过: 系统目前处于熔断停机状态")

            except Exception as e:
                logger.error(f"❌ 心跳协程异常: {e}")
                await asyncio.sleep(60)  # 异常后等待一分钟重试

    async def run(self):
        async with aiohttp.ClientSession() as session:
            try:
                logger.info("⚡ 启动即时扫描")

                # 1. 获取 symbols
                symbols = self.cfg.get("watch_list")

                # 2. 检查 symbols 是否有效
                if symbols and len(symbols) > 0:
                    # 执行首次即时扫描
                    await self.scan_cycle(session, symbols, "5M")
                else:
                    logger.error("❌ 严重错误: 最终 symbols 列表为空，无法扫描！")

            except Exception as e:
                logger.error(f"❌ 初始扫描发生崩溃: {e}", exc_info=True)

            # 组装所有 worker
            workers = [self.interval_worker(session, i) for i in self.cfg.get('intervals')]

            # 添加心跳 worker
            workers.append(self.heartbeat_worker())

            # 并发运行
            await asyncio.gather(*workers)


# =====================================================
# 7. 启动引擎 (RunEngine)
# =====================================================
class RunEngine:
    def __init__(self, config: Dict):
        # 1. 自动处理时区
        self._setup_timezone()

        # 2. 动态加载配置（处理线上报错问题）
        self.config = config

        self.local_key = self._load_initial_config()

        # 1. 获取 KEY
        self.env_key = os.getenv("ENCRYPTION_KEY")
        self.final_key = self.env_key or self.local_key

        if not self.final_key:
            raise ValueError("CRITICAL: ENCRYPTION_KEY not found!")

        # 2. 初始化加密对象
        self.cipher = Fernet(self.final_key.encode())

        # 3. 解密并更新配置
        self._setup_credentials()

        # 4. 初始化引擎实例
        self.scan_engine = ScanEngine(self.config)

    @staticmethod
    def _setup_timezone():
        os.environ['TZ'] = 'Asia/Shanghai'
        if hasattr(time, 'tzset'):
            time.tzset()

    @staticmethod
    def _load_initial_config():
        try:
            from conf.config import ENCRYPTION_KEY as LOCAL_VAL
            return LOCAL_VAL
        except (ImportError, ModuleNotFoundError):
            logger.warning("⚠️ Local config file not found. Switching to Environment Mode.")
            return None

    def _setup_credentials(self):
        try:
            TWELVE_DATA_URL = b'gAAAAABpX2App_DGAktBZLYAxKvv8WYTZgDagkxRPd_PKauN_VSBSeAIV3NYxEAJIvsSJ1eS76OWY_I-59Kym3TFhuEun39CywUmSm2wPuVjGmHNwgqDUrqYzRhdcoTw_wM2EnCC62k4'
            TWELVE_DATA_KEY = b'gAAAAABpX1jAwrYOW4EGBhuRwrU7Iz8s_tfJssQ0-yzCEOWoAVzG-4enR4wW1lxyBiqFc7N0k8HmdqBkiRj8SVoCmw5khSOq4vRX1hJDuRaYqylrT3NYq7XJ609kGEr11DrMAPXEWbFQ'
            WECOM_WEBHOOK = b'gAAAAABpX1lf_OZccl6JYh14FJlLEmJDtV37L1jW5MMRhdA09xypIujad5g1e2axJUwOA_gKCF3kodoYVG9Wrj1TyayLXmSn3t6lnG5xzNXedE01dNq1E-S77oYFLhaS9g3Ay24P2apcvBGkaV61cI76Pk7jNrjRTNjhxwgrvT3FiDHaQk3FULbFwvQJy0BADgv1cli4_vzB'
            TG_TOKEN = b'gAAAAABpX1mGV2Aqsf_W0eXjohhjNzWB4pDhsPqRDDei9jfKMkwsCT9Bu0qHzOGDAaapiBGNPwP1hyk46SN78yq2si5RylJTSBmdh6wPJlWpeAZtlEgu7wuxlEi3AMByECDdWnBx1iol'
            TG_CHAT_ID = b'gAAAAABpX1maZKmpePVf4ancQG2QpOX7YXk4wPMqPTw8x4DgJN3cKaVO6I0cQp0eCpL1gR4lim2W6k0LWXqH-R28889G2I446Q=='

            self.config["api"]["TWELVE_DATA_URL"] = self.cipher.decrypt(TWELVE_DATA_URL).decode()
            self.config["api"]["TWELVE_DATA_KEY"] = self.cipher.decrypt(TWELVE_DATA_KEY).decode()
            self.config["notify"]["WECOM_WEBHOOK"] = self.cipher.decrypt(WECOM_WEBHOOK).decode()
            self.config["notify"]["TG_TOKEN"] = self.cipher.decrypt(TG_TOKEN).decode()
            self.config["notify"]["TG_CHAT_ID"] = self.cipher.decrypt(TG_CHAT_ID).decode()

        except Exception as e:
            logger.error(f"Failed to decrypt credentials. Verify that ENCRYPTION_KEY is valid: {e}")
            raise

    @staticmethod
    async def _handle_health(_request):
        return web.Response(text="Bot is running", content_type='text/html')

    async def _run_services(self):
        await asyncio.gather(self.scan_engine.run())

    async def run_huggingface(self):
        app = web.Application()
        app.router.add_get('/', self._handle_health)
        run = web.AppRunner(app)
        await run.setup()
        site = web.TCPSite(run, '0.0.0.0', 7860)
        await site.start()
        logger.info("✅ HF Mode: Web Dashboard started on port 7860")
        await self._run_services()

    async def run_local(self):
        logger.info("✅ Local Mode: Starting engines")
        await self._run_services()

    def start(self):
        try:
            if self.env_key:
                asyncio.run(self.run_huggingface())
            else:
                asyncio.run(self.run_local())
        except KeyboardInterrupt:
            logger.warning("Stopped by user")
        except Exception as e:
            logger.error(f"Critical error: {e}")


if __name__ == "__main__":
    runner = RunEngine(CONFIG)
    runner.start()