A 股突破扫描系统（Pivot + SQZMOM + MA200）
创建时间：2025-12-06
版本：v1.1

【功能简介】
- 扫描随机一批 A 股股票（可配置 sample）
- 获取过去 N 天行情数据（不需要填写日期）
- 非实时数据
- 自动计算：
    1. MA200 趋势判断
    2. LazyBear SQZMOM
    3. 前方 pivot 阻力位
    4. 上破阻力位 + 今日上涨
- 最终输出信号：
    ✓ 买入
- 自动导出 CSV（每日新建文件夹）

【运行方法】
1. 安装依赖：
    pip install pandas numpy akshare tqdm

2. 运行脚本：
    python3 stock_scan.py

3. 查看输出：
    -> Day_Stocks/YYYY-MM-DD/Buy_Stocks_xxxxxx.csv

【适用人群】
需要本地离线批量扫描 A 股突破信号的量化用户。
