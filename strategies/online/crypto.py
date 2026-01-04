import numpy as np
import pandas as pd
import asyncio
import aiohttp
import json
import logging
from datetime import timedelta
from typing import List, Dict, Optional, Any

# =====================================================
# 0. 配置中心 (CONFIG)
# =====================================================
CONFIG = {
    "api": {
        "BASE_URL": "https://fapi.binance.com",
        "TOP_N": 30,           # 自动抓取成交额前30的品种
        "MAX_CONCURRENT": 10,  # 最大并发请求数
        "KLINE_LIMIT": 1000,
        "EXCLUDE_TOKENS": ["USDC", "FDUSD", "DAI", "EUR"]
    },

    "watch_list": ["ETHUSDT"],  # 留空则自动获取全市场高成交额品种

    "intervals": ["1h"],        # 监听的时间周期

    "strategy": {
        "bb_length": 20,   # 布林带周期
        "bb_mult": 2.0,    # 布林带标准差倍数
        "kc_length": 20,   # 肯特纳通道周期
        "kc_mult": 1.5,    # 肯特纳通道倍数 (Squeeze核心参数)
        "use_true_range": True,

        "ema_length": 200,  # 长期趋势过滤

        "srb_left": 15,     # 支撑压力左侧强度
        "srb_right": 15,    # 支撑压力右侧强度

        "min_sqz_bars": 6   # 至少6根K线才视为有效挤压
    },

    "notify": {
        "CONSOLE_LOG": True,
        "TG_ENABLE": False
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
        except Exception:
            return None


# =====================================================
# 2. 指标引擎 (IndicatorEngine)
# =====================================================
class IndicatorEngine:
    def __init__(self, st_cfg: dict):
        self.cfg = st_cfg

    @staticmethod
    def calculate_linreg(series: pd.Series, length: int):
        """线性回归拟合 - 优化性能版"""
        # 使用 numpy 向量化计算替代 rolling.apply
        x = np.arange(length)
        x_mean = np.mean(x)

        def get_linreg(y_slice):
            if len(y_slice) < length: return np.nan
            y_mean = np.mean(y_slice)
            # 计算斜率 beta
            ss_xy = np.sum((x - x_mean) * (y_slice - y_mean))
            ss_xx = np.sum((x - x_mean) ** 2)
            slope = ss_xy / ss_xx
            intercept = y_mean - slope * x_mean
            return slope * (length - 1) + intercept

        return series.rolling(window=length).apply(get_linreg, raw=True)

    @staticmethod
    def get_histogram_color(series: pd.Series) -> Any:
        val = series
        val_prev = series.shift(1)

        conditions = [
            (val > 0) & (val > val_prev),   # 亮绿
            (val > 0) & (val <= val_prev),  # 暗绿
            (val < 0) & (val < val_prev),   # 亮红
            (val < 0) & (val >= val_prev)   # 暗红
        ]
        choices = ["亮绿", "暗绿", "亮红", "暗红"]

        # 使用 np.select 处理向量化判定，性能极高
        return np.select(conditions, choices, default="中性")

    def squeeze_momentum_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        c, h, l = df['close'], df['high'], df['low']

        # --- 1. Squeeze Momentum ---
        # BB 轨道
        basis = c.rolling(self.cfg['bb_length']).mean()
        dev = self.cfg['bb_mult'] * c.rolling(self.cfg['bb_length']).std()
        upperBB, lowerBB = basis + dev, basis - dev

        # KC 轨道
        ma = c.rolling(self.cfg['kc_length']).mean()
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        range_ma = tr.rolling(self.cfg['kc_length']).mean()
        upperKC, lowerKC = ma + range_ma * self.cfg['kc_mult'], ma - range_ma * self.cfg['kc_mult']

        # 挤压状态判定
        sqz_on = (lowerBB > lowerKC) & (upperBB < upperKC)
        sqz_off = (lowerBB < lowerKC) & (upperBB > upperKC)
        df["sqz_status"] = np.select([sqz_on, sqz_off], ["ON", "OFF"], default="NO")

        # 状态计数：连续 ON 或 OFF 的次数
        status_groups = (df["sqz_status"] != df["sqz_status"].shift()).cumsum()
        df["sqz_id"] = df.groupby(status_groups).cumcount() + 1

        # 动能柱计算 (Momentum)
        highest_h = h.rolling(self.cfg['kc_length']).max()
        lowest_l = l.rolling(self.cfg['kc_length']).min()
        mid_price = ((highest_h + lowest_l) / 2 + ma) / 2
        df["sqz_hvalue"] = self.calculate_linreg(c - mid_price, self.cfg['kc_length'])

        # 调用封装好的颜色判定静态方法
        df["sqz_hcolor"] = self.get_histogram_color(df["sqz_hvalue"])

        return df

    def ema_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        df[f"ema{self.cfg.get('ema_length')}"] = df['close'].ewm(span=self.cfg.get('ema_length'), adjust=False).mean()

        return df

    def support_resistance_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        # 总窗口长度
        window =  self.cfg.get('srb_left') + self.cfg.get('srb_right') + 1

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

        # 清理中间计算列
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
        signal = "NO"
        if cur['sqz_status'] == "OFF" and prev['sqz_status'] == "ON" and prev['sqz_id'] >= self.cfg['min_sqz_bars']:
            if cur['close'] > cur['ema200'] and cur['sqz_hvalue'] > 0:
                signal = "BUY (Long)"
            elif cur['close'] < cur['ema200'] and cur['sqz_hvalue'] < 0:
                signal = "SELL (Short)"

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
            "bars": int(prev['sqz_id']),
            "ema200": round(cur['ema200'], 4),
            "energy": "-".join(energy),
            "support": str(round(cur['srb_sup'], 4)),
            "resistance": str(round(cur['srb_res'], 4)),
            "trend_r": "-".join(tr),
            "trend_s": "-".join(ts)
        }


# =====================================================
# 4. 扫描引擎 (ScanEngine) - 并发调度
# =====================================================
class ScanEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.data_e = DataEngine(cfg['api'])
        self.ind_e = IndicatorEngine(cfg['strategy'])
        self.strat_e = StrategyEngine(cfg['strategy'])

    async def scan_cycle(self, session, symbols, interval):
        sem = asyncio.Semaphore(self.cfg['api']['MAX_CONCURRENT'])

        async def _proc(s):
            async with sem:
                raw = await self.data_e.fetch_klines(session, s, interval)
                if raw is None or len(raw) < 300: return None
                # 调用实例方法而不是类方法
                df = self.ind_e.calculate(raw)
                return self.strat_e.execute(df, s, interval)

        results = await asyncio.gather(*(_proc(s) for s in symbols))
        # 过滤掉无效数据和无信号数据（若只想看信号，可过滤 signal != "NO"）
        signals = [r for r in results ]

        if self.cfg['notify']['CONSOLE_LOG']:
            print(f"\n>>> [{interval}] 扫描完成 | 总数: {len(symbols)} | 发现信号: {len(signals)}")
            if signals:
                print(json.dumps(signals, indent=4, ensure_ascii=False))

    async def run(self):
        async with aiohttp.ClientSession() as session:
            symbols = self.cfg.get("watch_list")
            if not symbols:
                symbols = await self.data_e.get_active_symbols(session)

            logger.info(f"开始扫描 {len(symbols)} 个品种: {symbols[:5]}...")
            tasks = [self.scan_cycle(session, symbols, i) for i in self.cfg['intervals']]
            await asyncio.gather(*tasks)


# =====================================================
# 运行
# =====================================================
if __name__ == "__main__":
    scanner = ScanEngine(CONFIG)
    try:
        asyncio.run(scanner.run())
    except KeyboardInterrupt:
        pass