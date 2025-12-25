# -*- coding: utf-8 -*-

# 策略注册表
from strategies import (
    cross_strategy,
    squeeze_resistance_strategy,
    squeeze_adx_strategy,
    squeeze_maroon_strategy,
    fair_value_gap_strategy
)

STRATEGY_REGISTRY = {
    "cross": cross_strategy.run_strategy,
    "squeeze_resistance": squeeze_resistance_strategy.run_strategy,
    "squeeze_adx": squeeze_adx_strategy.run_strategy,
    "squeeze_maroon": squeeze_maroon_strategy.run_strategy,
    "fvg": fair_value_gap_strategy.run_strategy,
}
