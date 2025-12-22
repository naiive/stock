# -*- coding: utf-8 -*-

# 策略注册表
from strategies import (
    squeeze_resistance_strategy,
    squeeze_adx_strategy,
    cross_strategy
)

STRATEGY_REGISTRY = {
    "squeeze_resistance": squeeze_resistance_strategy.run_strategy,
    "squeeze_adx": squeeze_adx_strategy.run_strategy,
    "cross": cross_strategy.run_strategy,
}
