# -*- coding: utf-8 -*-

# 策略注册表
from strategies import (
    squeeze_resistance_strategy,
    squeeze_adx_strategy,
    cross_strategy,
    maroon_squeeze_strategy,
    fair_value_gap_strategy
)

STRATEGY_REGISTRY = {
    "squeeze_resistance": squeeze_resistance_strategy.run_strategy,
    "squeeze_adx": squeeze_adx_strategy.run_strategy,
    "cross": cross_strategy.run_strategy,
    "maroon_squeeze": maroon_squeeze_strategy.run_strategy,
    "fvg": fair_value_gap_strategy.run_strategy,
}
