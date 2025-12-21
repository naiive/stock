# A 股突破扫描系统

一个基于 **历史日线数据** 的 A 股突破扫描工具，核心策略为：

**Pivot 突破 + SQZMOM 动量 + MA200 趋势过滤**

项目默认不使用实时行情，适合离线分析、批量扫描和策略研究。

---

## 功能说明

- 获取 A 股历史日线数据（基于 akshare）
- 自动计算技术指标并进行条件筛选
- 扫描满足突破条件的股票并输出结果
- 全流程日志记录，便于排错与复盘

---

## 历史数据获取

- 数据源：`akshare`
- 使用日线级别数据
- 支持前复权 / 不复权
- 数据字段统一为：


## 程序全流程图 (Sequence Flow)
整个系统的运作流程如下：

- 数据装载 (Data Loading): main.py 启动 -> DataHandler 建立连接 -> prepare_realtime_data 将全市场快照存入内存缓存。

- 异步分批 (Async Batching): MarketScanner 将 5000 只股票切分为每批 500 只，通过 asyncio.as_completed 启动任务。

- 并发计算 (Concurrent Computing):

- 线程池启动 _worker(symbol)。

- DataHandler: 执行 get_full_data，通过 _append_snapshot 把“历史线”和“实时点”缝合成“完整面”。

- Strategies: breakout_strategy 拿到 DataFrame。

- Indicators: 策略内部依次调用 indicators/ 下的函数计算 ADX -> Squeeze -> ATR。

- 信号筛选 (Filtering): 策略根据指标结果（如 sqz_status == 'OFF' 且 adx > 20）判定是否命中。

- 汇总导出 (Exporting): MarketScanner 收集所有命中的字典，一次性写入 CSV。
