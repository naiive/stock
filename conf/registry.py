# -*- coding: utf-8 -*-

# 策略注册表
from strategies import (
    aroon_oscillator_strategy,
    cross_strategy,
    squeeze_strategy,
    squeeze_resistance_strategy,
    squeeze_adx_strategy,
    squeeze_red_cover_strategy,
    squeeze_divergence_strategy,
    fair_value_gap_strategy,
    macd_histogram_double_divergence_strategy
)

STRATEGY_REGISTRY = {
    "aroon_oscillator": aroon_oscillator_strategy.run_strategy,
    "cross": cross_strategy.run_strategy,
    "squeeze_resistance": squeeze_resistance_strategy.run_strategy,
    # 原生squeeze策略
    "squeeze": squeeze_strategy.run_strategy,
    "squeeze_adx": squeeze_adx_strategy.run_strategy,
    # 红色覆盖挤压释放策略
    "squeeze_red_cover": squeeze_red_cover_strategy.run_strategy,
    "squeeze_divergence": squeeze_divergence_strategy.run_strategy,
    "fvg": fair_value_gap_strategy.run_strategy,
    "macd_histogram_double_divergence": macd_histogram_double_divergence_strategy.run_strategy
}
