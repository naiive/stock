<div align="center">

# A 股量化突破扫描系统

轻量、可扩展的全市场异步扫描引擎，聚焦“挤压 + 趋势 + 突破”的高胜率交易型信号。

![banner](path.png)

<br/>

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](#许可协议)
[![Status](https://img.shields.io/badge/status-active-success.svg)](#)
[![Made with ❤️](https://img.shields.io/badge/made%20with-%E2%9D%A4-red.svg)](#)

</div>

---


## 概览

本项目是一个针对 A 股市场的全市场因子/信号扫描器：

- 基于本地 MySQL 的历史日线数据（支持前复权/后复权），可选叠加实时快照；
- 使用异步分批并发扫描，全市场 5000+ 只股票依然具备良好吞吐；
- 聚焦“挤压（Squeeze）+ 动量 + 趋势过滤（如 ADX、MA200）”的多维共振；
- 输出命中标的 CSV 并可选邮件/Telegram 推送，支持二次分析与复盘。

---

## 功能亮点

- 异步批处理：按批次切分代码池，避免 I/O 挤压与 API/数据库限流；
- 指标链式计算：`indicators/` 目录下统一封装，策略可自由组合；
- 策略注册表：新增策略只需在 `conf/registry.py` 注册即可被系统识别；

---


## 安装与运行

前置条件：

- Python 3.11+
- MySQL 8.0+（提供历史日线数据表，如 `asian_quant_stock_daily`）

安装建议（示例）：

```bash

~/Desktop $ python3.11 -m venv ./asian-quant/venv311

source ./asian-quant/venv311/bin/activate

pip install -r requirements.txt

deactivate
```

---

## 策略清单与说明（conf/registry.py）

已内置并注册的策略（可在 `STRATEGY_CONFIG.RUN_STRATEGY` 中选择）：

- `squeeze_resistance`：Squeeze + 突破前高；
- `cross`：交叉类买入信号；
- `aroon_oscillator`：Aroon 新低判断；


---

## 贡献 & 许可

欢迎提交 Issue/PR 共同完善策略与指标生态。

本项目采用 MIT 许可协议，详见 LICENSE（若未包含，可按需补充）。

—— 祝交易顺利，风控常伴。📈
