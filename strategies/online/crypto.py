import numpy as np
import pandas as pd
import asyncio
import aiohttp
import json
import logging
from datetime import timedelta
from typing import List, Dict, Optional, Any
from conf.config import TELEGRAM_CONFIG

# =====================================================
# 0. 配置中心 (CONFIG)
# =====================================================
CONFIG = {
    "watch_list": [],           # 留空则自动获取全市场高成交额品种
    "intervals": ["1h"],        # 监听的时间周期

    "api": {
        "BASE_URL": "https://fapi.binance.com",
        "TOP_N": 2,             # 自动抓取成交额前10的品种
        "MAX_CONCURRENT": 10,   # 最大并发请求数
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
# 1. 数据引擎 (DataEngine) - 负责行情抓取
# =====================================================
class DataEngine:
    def __init__(self, api_cfg: dict):
        self.cfg = api_cfg

    async def get_active_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        """获取全市场成交额排名前 N 的合约"""
        url = f"{self.cfg['BASE_URL']}/fapi/v1/ticker/24hr"
        try:
            async with session.get(url, timeout=10) as r:
                data = await r.json()
                df = pd.DataFrame(data)
                # 过滤 USDT 交易对且排除稳定币
                df = df[df['symbol'].str.endswith('USDT')]
                for token in self.cfg['EXCLUDE_TOKENS']:
                    df = df[~df['symbol'].str.contains(token)]

                df['quoteVolume'] = df['quoteVolume'].astype(float)
                return df.sort_values('quoteVolume', ascending=False).head(self.cfg['TOP_N'])['symbol'].tolist()
        except Exception as e:
            logger.error(f"获取活跃币种失败: {e}")
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
# 3. 策略引擎 (StrategyEngine) - 信号判定
# =====================================================
class StrategyEngine:
    def __init__(self, st_cfg: dict):
        self.cfg = st_cfg

    def execute(self, df: pd.DataFrame, symbol: str, interval: str) -> Dict[str, Any]:
        if len(df) < 2: return {"signal": "NO"}

        cur = df.iloc[-1]
        prev = df.iloc[-2]

        # 核心策略逻辑：
        # 1. 前一根K线处于 ON (挤压状态) 且持续时间 >= 阈值
        # 2. 当前K线变为 OFF (爆发状态)
        # 3. 配合 EMA200 过滤多空方向
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
# 4. 通知引擎 (NotifyEngine) - log和telegram
# =====================================================
class NotifyEngine:
    def __init__(self, notify_cfg: dict):
        self.cfg = notify_cfg
        self.running_tasks = []  # 用于追踪异步任务

    def process_results(self, results: list, interval: str):

        results_list = [r for r in results if r is not None]

        if not results_list:
            return

        # 统计产生信号的数量
        signals = [r for r in results_list if r.get('signal') != "NO"]

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

        # 2. Telegram 推送任务
        if self.cfg.get('TG_ENABLE'):
            task = asyncio.create_task(self.broadcast_to_tg(results_list, interval))
            self.running_tasks.append(task)
            task.add_done_callback(lambda t: self.running_tasks.remove(t) if t in self.running_tasks else None)

    async def broadcast_to_tg(self, results_list, interval):
        proxy = self.cfg.get('TG_PROXY')
        async with aiohttp.ClientSession() as session:
            for res in results_list:
                await self.send_to_telegram(session, res, interval, proxy)
                await asyncio.sleep(0.1)  # 频率保护

    async def send_to_telegram(self, session, res, interval, proxy):
        # 1. 基础字段处理
        token = self.cfg.get('TG_TOKEN')
        chat_id = self.cfg.get('TG_CHAT_ID')

        # Tradingview链接
        symbol = res.get('symbol', 'Unknown')
        tv_url = f"https://cn.tradingview.com/chart/pvCjwkIK/?symbol=BINANCE%3A{symbol}"

        symbol_link = f'<a href="{tv_url}">{symbol}</a>'

        # 2. 信号转换
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

        # 3. 价格与涨幅
        price = res.get('price', 0)
        change = res.get('change', 0)
        change_str = f"（{'+' if change >= 0 else ''}{change}%）"

        # 4. 动能图标处理
        energy_str = str(res.get('energy', ""))
        energy_items = energy_str.split('-') if energy_str else []
        recent_items = energy_items[-6:]
        mom_icons = "".join(["🟢" if "绿" in item else "🔴" for item in recent_items])

        # 5. 趋势图标映射
        trend_list = trend_str.split('-') if trend_str else []
        trend_icons = "".join(["⬆️" if "高" in t else "⬇️" for t in trend_list[-6:]]) if trend_list else ""


        # 6. 构建消息模板
        msg = (
            f"⚡ <b>信号【{interval.upper()}】</b> <b>{symbol_link}</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🔄 <b>时间:</b> {res.get('time', '-')} (UTC+8)\n"
            f"💹 <b>信号:</b> {signal_text}\n"
            f"💰 <b>价格:</b> <code>{price}</code>{change_str}\n"
            f"🧨 <b>挤压:</b> <code>{res.get('bars', 0)}</code> Bars\n"
            f"📊 <b>动能:</b> {mom_icons if mom_icons else '无'}\n"
            f"🚀 <b>趋势:</b> {trend_icons if trend_icons else '无'}\n"
            f"📅 <b>日期:</b> {res.get('date', '-')}\n"
        )

        url = f"https://api.telegram.org/bot{token}/sendMessage"

        payload = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
            "disable_notification": True if raw_signal == "No" else False
        }

        try:
            async with session.post(url, data=payload, proxy=proxy, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"TG 发送失败 [{resp.status}]: {await resp.text()}")
        except Exception as e:
            logger.error(f"TG 网络异常: {e}")


# =====================================================
# 5. 扫描引擎 (ScanEngine) - 并发调度
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

    async def _proc_symbol(self, session, symbol, interval, sem):
        """单个币种的处理流水线"""
        async with sem:
            try:
                # 1. 获取数据
                raw = await self.data_e.fetch_klines(session, symbol, interval)
                if raw is None or len(raw) < 300:
                    return None

                # 2. 计算指标 (实例方法调用)
                df = self.ind_e.calculate(raw)

                # 3. 执行策略判定
                return self.strat_e.execute(df, symbol, interval)
            except Exception as e:
                logger.error(f"处理 {symbol} 失败: {e}")
                return None

    async def scan_cycle(self, session, symbols, interval):
        """单次循环调度"""
        sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])

        # 创建并发任务
        tasks = [self._proc_symbol(session, s, interval, sem) for s in symbols]

        # 获取所有结果
        results = list(await asyncio.gather(*tasks))

        # 将结果移交给通知引擎进行后续处理
        self.notify_e.process_results(results, interval)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            symbols = self.cfg.get("watch_list") or await self.data_e.get_active_symbols(session)
            tasks = [self.scan_cycle(session, symbols, i) for i in self.cfg['intervals']]
            await asyncio.gather(*tasks)

            # --- 关键修复：等待所有 TG 推送任务完成 ---
            if self.notify_e.running_tasks:
                logger.info(f"等待 TG 推送完成 (共 {len(self.notify_e.running_tasks)} 个任务)...")
                await asyncio.gather(*self.notify_e.running_tasks)
                logger.info("所有推送任务已结束。")


if __name__ == "__main__":
    scanner = ScanEngine(CONFIG)
    try:
        asyncio.run(scanner.run())
    except KeyboardInterrupt:
        logger.error("APP启动出错")
