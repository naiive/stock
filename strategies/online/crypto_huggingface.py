import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ============================================================
# 1. 核心配置区 (参数根据需求在此调整)
# ============================================================
# [环境变量] 建议在 Hugging Face 的 Settings -> Variables and secrets 中设置
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# [多周期监控清单] 程序会自动根据此列表创建不同的定时任务
MONITOR_INTERVALS = ["1h", "4h", "1d"]

TOP_N = 20              # 每个周期只扫成交额前 20 的活跃币种
MAX_CONCURRENT = 8      # 异步并发数，日线级别建议设小一点，保护 IP 稳定性
HEARTBEAT_INTERVAL = 4  # 机器人每 4 小时发一次心跳，确认在线

# [Squeeze Momentum 策略参数]
STRATEGY_CONFIG = {
    "length": 20,       # 指标计算周期 (BB/KC)
    "multKC": 1.2,      # 肯特纳通道乘数，决定挤压的敏感度
    "ema_filter": 200,  # 200周期均线过滤，确保顺势交易
    "min_sqz_bars": 6,  # 挤压状态至少维持 6 根 K 线才算有效蓄势
    "srb_left": 15,     # 支撑阻力判定：左侧强度
    "srb_right": 15,    # 支撑阻力判定：右侧确认
}

BASE_URL = "https://fapi.binance.com"

# [日志配置]
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ============================================================
# 2. 量化算法引擎 (StrategyEngine)
# ============================================================
class StrategyEngine:
    """负责核心指标计算与信号识别"""

    @staticmethod
    def tv_linreg(y_series, length):
        """模拟 TradingView 的 linreg 线性回归预测函数"""
        if len(y_series) < length: return np.nan
        y = y_series.values[-length:]
        x = np.arange(length)
        A = np.vstack([x, np.ones(length)]).T
        m, b = np.linalg.lstsq(A, y, rcond=None)[0]
        return m * (length - 1) + b

    @classmethod
    def check_signal(cls, df, symbol, interval):
        """计算指标并返回触发结果"""
        if df is None or len(df) < 250: return None
        conf = STRATEGY_CONFIG
        df = df.copy()

        # --- [指标 1: SRB 支撑阻力] ---
        l, r = conf['srb_left'], conf['srb_right'];
        win = l + r + 1
        df['p_low'] = df['low'].rolling(win).apply(lambda x: 1 if x[l] == np.min(x) else 0, raw=True)
        df['p_high'] = df['high'].rolling(win).apply(lambda x: 1 if x[l] == np.max(x) else 0, raw=True)
        # ffill() 确保支撑阻力线在图表上是连续的
        df['srb_sup'] = np.where(df['p_low'] == 1, df['low'].shift(r), np.nan).astype(float)
        df['srb_res'] = np.where(df['p_high'] == 1, df['high'].shift(r), np.nan).astype(float)
        df['srb_sup'] = df['srb_sup'].ffill();
        df['srb_res'] = df['srb_res'].ffill()

        # --- [指标 2: Squeeze Momentum 核心] ---
        # 布林带计算
        df['basis'] = df['close'].rolling(20).mean()
        df['dev'] = 1.2 * df['close'].rolling(20).std(ddof=0)
        df['upBB'], df['loBB'] = df['basis'] + df['dev'], df['basis'] - df['dev']
        # 肯特纳通道 (KC) 计算
        tr = pd.concat(
            [df['high'] - df['low'], (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()],
            axis=1).max(axis=1)
        df['maKC'] = df['close'].rolling(20).mean();
        df['rangeKC'] = tr.rolling(20).mean()
        df['upKC'], df['loKC'] = df['maKC'] + df['rangeKC'] * 1.2, df['maKC'] - df['rangeKC'] * 1.2

        # 判定挤压状态 (Squeeze On)
        df['sqzOn'] = (df['loBB'] > df['loKC']) & (df['upBB'] < df['upKC'])
        df['status'] = np.where(df['sqzOn'], "ON", "OFF")

        # 挤压计时器：计算当前状态持续了几根 K 线
        group = (df['status'] != pd.Series(df['status']).shift()).cumsum()
        df['sqz_id'] = df.groupby(group).cumcount() + 1

        # 动能柱颜色计算
        df['hval'] = (df['close'] - (df['close'].rolling(20).mean())).rolling(20).apply(lambda x: cls.tv_linreg(x, 20),
                                                                                        raw=False)
        df['pre_h'] = df['hval'].shift(1)
        # 200 周期 EMA 趋势过滤
        df['ema'] = df['close'].rolling(conf['ema_filter']).mean()

        curr, prev = df.iloc[-1], df.iloc[-2]

        # --- [信号逻辑判定] ---
        signal = None
        # 核心逻辑：当前挤压释放(OFF)，前一根在挤压(ON)，且满足 EMA 趋势和动能方向
        if curr['status'] == 'OFF' and prev['status'] == 'ON' and prev['sqz_id'] >= conf['min_sqz_bars']:
            # 多头：收盘价在 EMA200 上，且动能柱为正向增强
            if curr['close'] > curr['ema'] and curr['hval'] > curr['pre_h'] and curr['hval'] > 0:
                signal = "做多 (Long) ↑"
            # 空头：收盘价在 EMA200 下，且动能柱为负向增强
            elif curr['close'] < curr['ema'] and curr['hval'] < curr['pre_h'] and curr['hval'] < 0:
                signal = "做空 (Short) ↓"

        if signal:
            return {
                "symbol": symbol, "interval": interval, "signal": signal,
                "price": round(curr['close'], 4), "time": curr['date'].strftime('%H:%M')
            }
        return None


# ============================================================
# 3. 异步任务管理器 (AsyncBotManager)
# ============================================================
class AsyncBotManager:
    """管理多周期抓取、视觉化推送与系统心跳"""

    def __init__(self):
        self.last_sent = {}  # 信号去重字典
        self.start_time = datetime.now()

    async def send_tg(self, session, res):
        """
        根据 1h/4h/1d 周期，执行完全不同的视觉格式化
        让用户一眼就能区分出信号的重要性级别
        """
        # 视觉配置表
        style_cfg = {
            "1h": {"icon": "⚡", "dot": "🟢" if "多" in res['signal'] else "🔴", "label": "1小时级爆发"},
            "4h": {"icon": "💎", "dot": "🔵", "label": "4小时级中线"},
            "1d": {"icon": "👑", "dot": "🟣", "label": "日线级战略突破"}
        }
        cfg = style_cfg.get(res['interval'], {"icon": "🔔", "dot": "⚪", "label": "多周期信号"})

        # 构造 HTML 格式的 Telegram 消息
        msg = (
            f"{cfg['icon']} <b>{cfg['label']} | #{res['symbol']}</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"<b>周 期:</b> {res['interval']}\n"
            f"<b>动 作:</b> {cfg['dot']} {res['signal']}\n"
            f"<b>价 格:</b> <code>{res['price']}</code>\n"
            f"<b>时 间:</b> {res['time']} (UTC+8)\n"
            f"━━━━━━━━━━━━━━"
        )

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        try:
            async with session.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}) as r:
                return await r.json()
        except Exception as e:
            logger.error(f"TG发送异常: {e}")

    async def scan_cycle(self, interval):
        """针对单一周期的扫描任务"""
        logger.info(f"🚀 开始扫描 {interval} 周期...")
        async with aiohttp.ClientSession() as session:
            # 获取全市场成交额前 N 的品种
            try:
                async with session.get(f"{BASE_URL}/fapi/v1/ticker/24hr") as res:
                    data = await res.json()
                    symbols = pd.DataFrame(data).sort_values('quoteVolume', ascending=False).head(TOP_N)[
                        'symbol'].tolist()
            except:
                symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

            semaphore = asyncio.Semaphore(MAX_CONCURRENT)

            async def run(s):
                async with semaphore:
                    # 获取 K 线数据，limit 设为 500 以保证 EMA200 准确
                    params = {"symbol": s, "interval": interval, "limit": 500}
                    async with session.get(f"{BASE_URL}/fapi/v1/klines", params=params) as r:
                        if r.status == 200:
                            data = await r.json()
                            df = pd.DataFrame(data,
                                              columns=['ts', 'o', 'h', 'l', 'c', 'v', 'cts', 'qv', 'tr', 'tb', 'tq',
                                                       'i'])
                            df = df[['ts', 'o', 'h', 'l', 'c', 'v']].astype(float)
                            df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
                            # 转换为北京时间
                            df['date'] = pd.to_datetime(df['ts'], unit='ms') + timedelta(hours=8)

                            res = StrategyEngine.check_signal(df, s, interval)
                            if res:
                                key = f"{s}_{interval}_{res['time']}"  # 唯一标识：品种+周期+时间
                                if key not in self.last_sent:
                                    await self.send_tg(session, res)
                                    self.last_sent[key] = True

            await asyncio.gather(*(run(s) for s in symbols))
            logger.info(f"✅ {interval} 周期扫描完成")

    async def send_heartbeat(self):
        """每隔固定时间发送心跳，告知程序存活"""
        uptime = str(datetime.now() - self.start_time).split('.')[0]
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            text = f"💓 监控运行中\n已运行: {uptime}\n当前监控: {', '.join(MONITOR_INTERVALS)}"
            await session.post(url, data={"chat_id": CHAT_ID, "text": text})


# ============================================================
# 4. 主入口与 HF 适配 (Web Server & Scheduler)
# ============================================================
async def main():
    manager = AsyncBotManager()
    scheduler = AsyncIOScheduler()

    # --- 任务调度逻辑 ---
    for interval in MONITOR_INTERVALS:
        val, unit = int(interval[:-1]), interval[-1]

        # 如果是小时级别：每 val 小时触发一次
        if unit == 'h':
            scheduler.add_job(manager.scan_cycle, 'cron', hour=f'*/{val}', minute='0', second='5', args=[interval])
        # 如果是日线级别：每天早上 8:00 (换线时间) 触发一次
        elif unit == 'd':
            scheduler.add_job(manager.scan_cycle, 'cron', hour='8', minute='0', second='10', args=[interval])

    # 注册心跳任务
    scheduler.add_job(manager.send_heartbeat, 'interval', hours=HEARTBEAT_INTERVAL)
    scheduler.start()

    # 启动时执行一次，确保一切正常
    for interval in MONITOR_INTERVALS:
        await manager.scan_cycle(interval)

    # --- Hugging Face 存活守卫 ---
    # 开启一个极简 Web 服务，让 HF 认为此应用在正常服务
    from aiohttp import web
    async def hf_welcome(req):
        return web.Response(text="Trading Bot Active and Scanning...")

    app = web.Application();
    app.router.add_get('/', hf_welcome)
    runner = web.AppRunner(app);
    await runner.setup()
    # 7860 是 Hugging Face Space 默认监听端口
    await web.TCPSite(runner, '0.0.0.0', 7860).start()

    # 保持协程常驻
    while True: await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 系统安全退出")