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
import gradio as gr
from typing import Dict, Optional, Any, List
from cryptography.fernet import Fernet


# =====================================================
# 0. 配置中心 (CONFIG)
# =====================================================
CONFIG = {
    # 留空则自动获取全市场高成交额品种，统一使用 Token 名称，程序会自动转换后缀
    "watch_list" : [],
    # "watch_list": ["BTC", "ETH", "SOL", "DOGE"],

    # 监听的时间周期
    "intervals": ["1H", "4H", "1D"],

    "api": {
        "ACTIVE_EXCHANGE": "OKX", # OKX 或 BINANCE
        "OKX_BASE_URL": "https://www.okx.com",          # OKX合约接口y域名
        "BINANCE_BASE_URL": "https://fapi.binance.com", # binance合约接口y域名
        "TOP_N": 100,             # 自动抓取成交额前50的品种
        "MAX_CONCURRENT": 8,      # 最大并发请求数
        "KLINE_LIMIT": 1000,      # K线数量
        "EXCLUDE_TOKENS": ["USDC", "FDUSD", "DAI", "EUR"] # 排除稳定币之类的
    },

    "ui": {
        "refresh_interval": 5   # UI日志刷新时间 秒
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
        "adx_threshold": 25     # ADX水平【指标不使用，只是用作判断】
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
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.exchange = cfg.get("ACTIVE_EXCHANGE").upper()
        self.okx_base = cfg.get('OKX_BASE_URL')
        self.binance_base = cfg.get('BINANCE_BASE_URL')

    async def get_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        """公用入口：获取当前交易所成交额前 N 的品种"""
        if self.exchange == "BINANCE":
            return await self._get_binance_active_symbols(session)
        else:
            return await self._get_okx_active_symbols(session)

    # =====================================================
    # 币安 活跃币种获取逻辑
    # =====================================================
    async def _get_binance_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        url = f"{self.binance_base}/fapi/v1/ticker/24hr"
        try:
            async with session.get(url, timeout=10) as r:
                data = await r.json()
                if not isinstance(data, list):
                    logger.error(f"❌ 币安 API 响应异常: {data}")
                    return []

                df = pd.DataFrame(data)
                # quoteVolume 是 24h USDT 成交额
                df['vol_usdt'] = pd.to_numeric(df['quoteVolume'], errors='coerce')

                # 过滤：仅限 USDT 合约
                df = df[df['symbol'].str.endswith('USDT')]

                # 排除配置中的 Token
                exclude = self.cfg.get('EXCLUDE_TOKENS', [])
                for token in exclude:
                    df = df[~df['symbol'].str.contains(token)]

                # 排序并取前 N
                df = df.sort_values('vol_usdt', ascending=False)
                top_n = self.cfg.get('TOP_N', 50)
                symbols = df.head(top_n)['symbol'].tolist()

                logger.info(f"🔥 [Binance] 当前成交额前5: {symbols[:5]}")
                return symbols
        except Exception as e:
            logger.error(f"💥 获取币安活跃币种失败: {e}")
            return []

    # =====================================================
    # OKX 活跃币种获取逻辑
    # =====================================================
    async def _get_okx_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        url = f"{self.okx_base}/api/v5/market/tickers"
        params = {"instType": "SWAP"}
        try:
            async with session.get(url, params=params, timeout=10) as r:
                res = await r.json()
                data = res.get('data', [])
                if not data: return []

                df = pd.DataFrame(data)
                # volCcy24h * last  是 OKX 的 24h USDT 成交额
                df['vol_usdt'] = pd.to_numeric(df['volCcy24h'], errors='coerce') * pd.to_numeric(df['last'], errors='coerce')
                df = df[df['instId'].str.endswith('-USDT-SWAP')]

                exclude = self.cfg.get('EXCLUDE_TOKENS', [])
                for token in exclude:
                    df = df[~df['instId'].str.contains(token)]

                df = df.sort_values('vol_usdt', ascending=False)
                top_n = self.cfg.get('TOP_N', 50)
                symbols = df.head(top_n)['instId'].tolist()

                # 确保 BTC/ETH 在列表里
                for core in ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]:
                    if core not in symbols: symbols.insert(0, core)

                logger.info(f"🔝 [OKX] 当前成交额前5: {symbols[:5]}")
                return symbols[:top_n]
        except Exception as e:
            logger.error(f"💥 获取 OKX 活跃币种失败: {e}")
            return []

    def format_symbol(self, token: str) -> str:
        """统一转换币种格式"""
        clean_token = token.upper().replace("-USDT-SWAP", "").replace("USDT", "")
        if self.exchange == "OKX":
            return f"{clean_token}-USDT-SWAP"
        else:
            return f"{clean_token}USDT"

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        """公用入口：根据配置路由到不同的私有抓取方法"""
        if self.exchange == "BINANCE":
            return await self._fetch_binance_klines(session, symbol, interval)
        else:
            return await self._fetch_okx_klines(session, symbol, interval)

    async def _fetch_okx_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        """OKX 专用抓取逻辑"""
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

                # OKX数据处理: 倒序转正序 -> 转换数值 -> 转换时间
                df = pd.DataFrame(data, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'volCcy', 'volCcyQuote', 'confirm'])
                df = df.iloc[::-1].reset_index(drop=True)

                # 剔除未闭合 K 线 (confirm='0' 为未完结)
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
        """Binance 专用抓取逻辑"""
        url = f"{self.binance_base}/fapi/v1/klines"
        bn_interval = interval.lower()  # 币安通常使用小写 1h, 4h
        params = {
            "symbol": symbol,
            "interval": bn_interval,
            "limit": self.cfg.get('KLINE_LIMIT', 1000)
        }
        try:
            async with session.get(url, params=params, timeout=10) as r:
                data = await r.json()
                if isinstance(data, dict) or not data: return None

                # 币安数据处理: 已经是正序 -> 剔除最后一根未闭合 -> 转换数值
                df = pd.DataFrame(data).iloc[:-1]
                df = df[[0, 1, 2, 3, 4, 5]].astype(float)
                df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
                df['date'] = pd.to_datetime(df['ts'], unit='ms') + timedelta(hours=8)
                df.set_index('date', inplace=True)
                return df
        except Exception as e:
            logger.error(f"Binance Fetch Error ({symbol}): {e}")
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
    def __init__(self, notify_cfg: dict):
        self.cfg = notify_cfg
        self.running_tasks = []

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
    @staticmethod
    def format_single_signal(res, interval, tag):
        """
        将单个信号格式化为字符串片段
        """
        # 假设你在通知或主循环逻辑中获取了 symbol
        symbol = res.get('symbol', 'Unknown')
        active_exchange = CONFIG["api"].get("ACTIVE_EXCHANGE")

        # OKX:     ETH-USDT-SWAP -> ETHUSDT
        # Binance: ETHUSDT -> ETHUSDT
        tv_symbol = symbol.replace("-SWAP", "").replace("-", "")

        if active_exchange == "BINANCE":
            # 币安合约 TradingView 格式通常是 BINANCE:ETHUSDT
            tv_url = f"https://cn.tradingview.com/chart/?symbol=BINANCE%3A{tv_symbol}"
        else:
            # OKX TradingView 格式通常是 OKX:ETHUSDT.P
            tv_url = f"https://cn.tradingview.com/chart/?symbol=OKX%3A{tv_symbol}.P"

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
                f"💸 <b>判断:</b> <code>{judge_text}</code>\n"
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
                f"💸 判断: {judge_text}\n"
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
                    f"🟠 <b>币圈【{interval.upper()}】周期</b>\n"
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
                    f"🟠 币圈【{interval.upper()}】周期\n"
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
                    "content": f"⚠️ **Crypto系统异常报警**\n\n> 详情: {error_text}\n> 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            }
            tasks.append(asyncio.create_task(self._post_request(webhook_url, payload, "wecom_err")))

        # 2. 发送到 Telegram
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

    # 心跳通知
    async def send_heartbeat(self):
        """发送系统心跳存活通知"""
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = (
            f"💓 **Crypto机器人**\n"
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
        self.data_e = DataEngine(cfg['api'])
        # 指标引擎
        self.ind_e = IndicatorEngine(cfg['strategy'])
        # 策略引擎
        self.strat_e = StrategyEngine(cfg['strategy'])
        # 通知引擎
        self.notify_e = NotifyEngine(cfg['notify'])
        # 定时引擎
        self.timer_e = TimeEngine()
        # UI引擎
        self.ui_e = UIEngine(self.cfg)

    async def _proc_symbol(self, session, symbol, interval, sem):
        """单个币种的处理流水线"""
        async with sem:
            try:
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

        # UI 投喂点
        valid_results = [r for r in results if r is not None]
        signals = [r for r in valid_results if r.get('signal') != "No"]
        self.ui_e.update_state(valid_results, signals, interval)

        # 这里的 process_results 内部会过滤没有信号的数据并发送 TG
        self.notify_e.process_results(results, interval)

    async def interval_worker(self, session, interval):
        logger.info(f"🟢 [{interval}] 周期监控任务已启动")

        # 记录上一次成功执行的“时间槽”，防止在同一个周期内重复触发
        last_run_slot = None

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
            # 步骤 C: 重复触发保护
            # ==========================================
            # 确保在同一个 K 线周期内只执行一次扫描
            current_slot = datetime.now().replace(second=0, microsecond=0)
            if last_run_slot == current_slot:
                await asyncio.sleep(1)
                continue

            # ==========================================
            # 步骤 D: 执行核心扫描逻辑
            # ==========================================
            try:
                start_time = time.time()
                watch_list = self.cfg.get("watch_list", [])

                # 1. 转换或获取活跃币种列表
                if watch_list:
                    symbols = [self.data_e.format_symbol(s) for s in watch_list]
                else:
                    symbols = await self.data_e.get_active_symbols(session)

                # --- 判定 1：活跃列表没数据，立即停机 ---
                if not symbols:
                    reason = "关键异常：无法获取活跃币种列表（接口返回为空）。"
                    await self._trigger_circuit_breaker(interval, reason)
                    continue  # 这里进入 continue 后，下一轮循环会在步骤 A 退出

                # 2. 执行并发扫描获取 K 线详情
                sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])
                tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]
                results = await asyncio.gather(*tasks)

                # --- 判定 2：币详情数据全部失败，立即停机 ---
                valid_results = [r for r in results if r is not None]

                # 如果配置了监控名单，但一个成功的返回都没有，判定为接口失效
                if len(symbols) > 0 and len(valid_results) == 0:
                    reason = "关键异常：所有币种详情请求均失败"
                    await self._trigger_circuit_breaker(interval, reason)
                    continue

                # 提取信号用于 UI 信号墙统计
                signals = [r for r in valid_results if r.get('signal') != "No"]
                try:
                    self.ui_e.update_state(valid_results, signals, interval)
                except Exception as ui_err:
                    logger.error(f"⚠️ UI 引擎状态更新失败: {ui_err}")

                # ==========================================
                # 成功逻辑: 处理结果并重置（如果有计数器的话）
                # ==========================================
                # 正常处理扫描结果
                self.notify_e.process_results(list(results), interval)

                # 确保异步任务完成
                if self.notify_e.running_tasks:
                    await asyncio.gather(*self.notify_e.running_tasks)

                last_run_slot = current_slot
                logger.info(
                    f"✅ [{interval}] 扫描完成 (有效:{len(valid_results)}), 耗时: {time.time() - start_time:.2f}s")

            except Exception as e:
                # 运行时系统崩溃
                logger.error(f"❌ [{interval}] 运行时异常: {e}", exc_info=True)
                # 如果是网络相关的严重崩溃，也可以选择直接停机
                # await self._trigger_circuit_breaker(interval, f"系统崩溃: {str(e)}")
                await asyncio.sleep(10)

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
                    logger.warning("💓 心跳跳过：系统目前处于熔断停机状态。")

            except Exception as e:
                logger.error(f"❌ 心跳协程异常: {e}")
                await asyncio.sleep(60)  # 异常后等待一分钟重试

    async def _trigger_circuit_breaker(self, interval: str, reason: str):
        """私有方法：触发系统熔断"""
        self.is_active = False
        error_msg = (
            f"🛑 【系统熔断停机】\n"
            f"触发周期: {interval}\n"
            f"故障原因: {reason}\n"
            f"结果: 扫描任务已终止"
        )
        logger.critical(error_msg)
        # 调用通知引擎发送紧急错误消息
        await self.notify_e.send_error_msg(error_msg)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            try:
                logger.info("⚡ 启动即时扫描")

                # 1. 获取并转换 symbols
                watch_list = self.cfg.get("watch_list", [])

                if watch_list and len(watch_list) > 0:
                    # 如果有 watch_list，必须进行格式化转换
                    symbols = [self.data_e.format_symbol(s) for s in watch_list]
                    logger.info(f"📋 使用配置列表 (已转换格式): {symbols}")
                else:
                    # 自动获取（内部已经处理过格式了）
                    symbols = await self.data_e.get_active_symbols(session)

                # 2. 强校验：如果最终 symbols 列表为空，直接熔断并退出程序
                if not symbols or len(symbols) == 0:
                    error_msg = "🚨 程序启动失败：请求数据为空，无法执行初始扫描"
                    logger.critical(f"❌ {error_msg}")
                    # 直接触发熔断通知并返回，不再向下执行启动 worker
                    await self.notify_e.send_error_msg(error_msg)
                    return

                # 3. 执行首次即时扫描（既然过了上面的校验，这里 symbols 一定有效）
                await self.scan_cycle(session, symbols, "1H")

            except Exception as e:
                logger.error(f"❌ 初始扫描发生崩溃: {e}", exc_info=True)

            # 组装所有 worker
            workers = [self.interval_worker(session, i) for i in self.cfg.get('intervals')]

            # 添加心跳 worker
            workers.append(self.heartbeat_worker())

            # 并发运行
            await asyncio.gather(*workers)


# =====================================================
# 7. UI引擎 (UIEngine)
# =====================================================
class UIEngine:
    def __init__(self, ui_cfg: dict):
        self.cfg = ui_cfg
        self.latest_signals = []
        self.market_snapshot = pd.DataFrame()
        self.last_update = "尚未开始"
        self.log_stream = []

        self.theme_css = """
        /* 1. 基础容器：亮色背景 */
        .gradio-container { 
            max-width: 98% !important; 
            background-color: #f7f9fc !important; 
            color: #1a1d21 !important; 
        }

        /* 2. 状态栏与日志卡片：白底深影，增加专业感 */
        .stat-card { 
            background: #ffffff !important; 
            border: 1px solid #e1e4e8 !important; 
            padding: 16px !important;
            border-radius: 12px !important;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05) !important;
            min-width: 380px !important; /* 防止状态栏折行 */
            white-space: nowrap !important;
        }

        /* 3. 监控状态文字：深色更加醒目 */
        .stat-card p { 
            color: #24292e !important; 
            font-size: 15px !important; 
            font-weight: 600 !important;
            margin: 0 !important;
        }

        /* 4. 实时日志：改为“护眼深蓝”或“亮绿”，白底背景 */
        .log-box { 
            background-color: #f0f2f5 !important; 
            color: #0066cc !important; /* 深蓝色文字，亮色下更易读 */
            font-family: 'Fira Code', monospace !important; 
            border: 1px solid #d1d5da !important;
            padding: 12px !important;
            border-radius: 8px;
            font-size: 13px !important;
            min-height: 120px;
        }

        /* 5. 表格美化：亮色模式下的表格 */
        #sig-table { 
            background: white !important; 
            border-radius: 12px !important; 
            overflow: hidden !important; 
        }
        #sig-table table { border-collapse: collapse !important; }
        #sig-table th { background: #f6f8fa !important; color: #586069 !important; }

        /* 6. 强制列宽控制 */
        #sig-table th:nth-child(1) { width: 90px; }
        #sig-table th:nth-child(2) { width: 80px; }
        #sig-table th:nth-child(3) { width: 70px; }
        #sig-table th:nth-child(4) { width: 100px; }
        #sig-table th:nth-child(11) { width: 60px; }
        """

    def update_state(self, all_results, signal_results, interval):
        """
        all_results: 当前扫描周期内所有币种的完整数据列表 (包含指标)
        signal_results: 触发了 Long/Short 信号的币种列表
        interval: 当前扫描的周期 (如 '1H')
        """
        # 1. 更新最后刷新时间
        self.last_update = datetime.now().strftime("%H:%M:%S")

        # 2. 更新全市场概览快照 (用于 📊 标签页)
        # 确保每个 item 都带上周期信息，以便 _refresh_logic 识别
        for item in all_results:
            item['interval'] = interval
        self.market_snapshot = all_results

        # 3. 更新信号墙 (用于 🎯 标签页)
        if signal_results:
            for s in signal_results:
                s['interval'] = interval
            # 将新信号插入列表顶部
            self.latest_signals = (signal_results + self.latest_signals)

        # 4.生成实时扫描日志
        log_msg = f"[{interval}] 扫描完成 | 时间: {self.last_update} | 信号: {len(signal_results)}"

        # 如果有信号，详细记录一下哪个币出了信号
        if signal_results:
            symbols = [s['symbol'].split('-')[0] for s in signal_results]
            log_msg += f" (发现: {', '.join(symbols)})"

        # 存入 log_stream，放在最前面（最新的在上面）
        self.log_stream.insert(0, log_msg)
        # 只保留最近 20 条日志
        self.log_stream = self.log_stream[:20]

    def _refresh_logic(self):
        # --- 内部复用逻辑：将原始数据转为 UI 行 ---
        def transform_to_row(res):
            symbol = res.get('symbol', 'Unknown')
            interval = res.get('interval', '1H')
            price = res.get('price', 0)
            ema200 = res.get('ema200', 0)
            adx = res.get('adx', 0)
            adx_threshold = res.get('adx_threshold', 0)
            support = res.get('support', 0)
            resistance = res.get('resistance', 0)
            raw_signal = res.get('signal', 'No')

            # 1. 信号与判断逻辑 (复刻 format_single_signal)
            if raw_signal == "Long":
                signal_text = "🟢 Long"
                trend_str = str(res.get('trend_r', ""))
                e_b = "📈EMA" if price > ema200 else "📉EMA"
                r_b = "📈压力" if price > resistance else "📉压力"
                a_b = "📈ADX" if adx > adx_threshold else "📉ADX"
                judge_text = f"{e_b} / {r_b} / {a_b}"
            elif raw_signal == "Short":
                signal_text = "🔴 Short"
                trend_str = str(res.get('trend_s', ""))
                e_b = "📈EMA" if price > ema200 else "📉EMA"
                r_b = "📈支撑" if price > support else "📉支撑"
                a_b = "📈ADX" if adx > adx_threshold else "📉ADX"
                judge_text = f"{e_b} / {r_b} / {a_b}"
            else:
                # 全市场概览中没有信号时的默认显示
                signal_text = "⚪ No"
                trend_str = str(res.get('trend_r', ""))
                e_b = "📈EMA" if price > ema200 else "📉EMA"
                r_b = "📈支撑" if price > support else "📉支撑"
                a_b = "📈ADX" if adx > adx_threshold else "📉ADX"
                judge_text = f"{e_b} / {r_b} / {a_b}"

            # 2. 动能图标
            energy_items = str(res.get('energy', "")).split('-')
            mom_icons = "".join(["🟢" if "绿" in i else "🔴" for i in energy_items[-6:]])

            # 3. 趋势图标
            trend_list = trend_str.split('-') if trend_str else []
            trend_icons = "".join(["⬆️" if "高" in t else "⬇️" for t in trend_list[-6:]])

            # 4. TradingView 链接精简 (严格 Markdown 格式)
            tv_sym = symbol.replace("-SWAP", "").replace("-", "")
            tv_url = f"https://cn.tradingview.com/chart/?symbol=OKX%3A{tv_sym}.P"
            tv_link = f"[📊]({tv_url})"

            # 5. 返回行数据
            return [
                res.get('date', '-'),
                res.get('time', '-'),
                symbol,
                f"{interval.upper()}",
                signal_text,
                f"{price}",
                f"{res.get('change', 0)}%",
                judge_text,
                f"{res.get('bars', 0)}bars",
                mom_icons or "—",
                trend_icons or "—",
                tv_link
            ]

        # 1. 信号墙：只显示有信号的
        sig_rows = [transform_to_row(s) for s in self.latest_signals]

        # 2. 全市场概览：显示所有快照数据
        market_rows = []
        # 如果 ScanEngine 传过来的是 DataFrame，可以用 .to_dict('records')
        snapshot_data = self.market_snapshot if isinstance(self.market_snapshot, list) else []
        for item in snapshot_data:
            market_rows.append(transform_to_row(item))

        # 3. 状态栏信息
        status_info = f"🟠【{self.cfg.get('intervals', ['1H'])[0]}】周期 ⏰{self.last_update} 📅{datetime.now().strftime('%m-%d')}"
        log_html = f"<div class='log-box'>{''.join([f'<div>> {m}</div>' for m in self.log_stream])}</div>"

        return sig_rows, market_rows, status_info, log_html

    def create_ui(self):
        """
        核心 UI 构建方法
        """

        with gr.Blocks(css=self.theme_css, theme=gr.themes.Soft()) as ui:
            gr.HTML(f"""
                    <div style="text-align:center; padding: 20px 0; background-color: #ffffff; border-bottom: 1px solid #e1e4e8; margin-bottom: 20px;">
                        <h1 style="color: #e67e22; margin: 0; font-size: 28px; font-weight: 800; letter-spacing: 1px;">
                            BOT监控看板
                        </h1>
                    </div>
                """)

            with gr.Row():
                # 左侧：状态监控面板
                with gr.Column(scale=1):
                    with gr.Group(elem_classes="stat-card"):
                        gr.Markdown("### 🛰️ 监控状态")
                        status_display = gr.Markdown("等待初次扫描...")

                # 右侧：实时日志输出
                with gr.Column(scale=3):
                    with gr.Group(elem_classes="stat-card"):
                        gr.Markdown("### 📜 实时扫描日志")
                        log_display = gr.HTML(value="<div class='log-box'>>> 系统启动中...</div>")

            # 数据展示 Tab 区域
            with gr.Tabs(elem_classes="tabs"):
                with gr.TabItem("🎯 信号墙"):
                    signal_table = gr.DataFrame(
                        headers=["日期", "时间", "代码", "周期", "信号", "现价", "涨幅", "判断", "挤压", "动能", "趋势", "图表"],
                        datatype="markdown",
                        elem_id="sig-table",
                        wrap=False,
                        interactive=False
                    )
                with gr.TabItem("📊 全市场"):
                    market_table = gr.DataFrame(
                        headers=["日期", "时间", "代码", "周期", "信号", "现价", "涨幅", "判断", "挤压", "动能", "趋势", "图表"],
                        datatype="markdown", elem_id="market-table", interactive=False
                    )

            # 设置 5 秒定时刷新
            gr.Timer(5).tick(
                fn=self._refresh_logic,
                outputs=[signal_table, market_table, status_display, log_display]
            )

        return ui


# =====================================================
# 8. 启动引擎 (RunEngine)
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
            WECOM_WEBHOOK = b'gAAAAABpX1lf_OZccl6JYh14FJlLEmJDtV37L1jW5MMRhdA09xypIujad5g1e2axJUwOA_gKCF3kodoYVG9Wrj1TyayLXmSn3t6lnG5xzNXedE01dNq1E-S77oYFLhaS9g3Ay24P2apcvBGkaV61cI76Pk7jNrjRTNjhxwgrvT3FiDHaQk3FULbFwvQJy0BADgv1cli4_vzB'
            TG_TOKEN = b'gAAAAABpX1mGV2Aqsf_W0eXjohhjNzWB4pDhsPqRDDei9jfKMkwsCT9Bu0qHzOGDAaapiBGNPwP1hyk46SN78yq2si5RylJTSBmdh6wPJlWpeAZtlEgu7wuxlEi3AMByECDdWnBx1iol'
            TG_CHAT_ID = b'gAAAAABpX1maZKmpePVf4ancQG2QpOX7YXk4wPMqPTw8x4DgJN3cKaVO6I0cQp0eCpL1gR4lim2W6k0LWXqH-R28889G2I446Q=='

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
        # 1. 实例化 UI
        ui = self.scan_engine.ui_e.create_ui()

        # 2. 启动扫描引擎任务 (非阻塞)
        asyncio.create_task(self.scan_engine.run())

        # 3. 使用 Gradio 6.0 推荐的启动方式
        logger.info("🚀 Starting Gradio Interface on port 7860...")

        # launch 是一个阻塞操作，但在 asyncio 环境下
        # 我们使用 prevent_thread_lock 来允许后台任务运行
        ui.launch(
            server_name="0.0.0.0",
            server_port=7860,
            prevent_thread_lock=True
        )

        # 4. 持续保持异步循环
        while True:
            await asyncio.sleep(3600)

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