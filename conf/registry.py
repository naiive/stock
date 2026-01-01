# -*- coding: utf-8 -*-

# 策略注册表
from strategies import (
    squeeze_resistance_strategy,
    squeeze_adx_strategy,
    macd_histogram_double_divergence_strategy,
    cross_strategy,
    aroon_oscillator_strategy
)

STRATEGY_REGISTRY = {
    # squeeze + 突破前高 策略【⭐⭐⭐⭐⭐】
    "squeeze_resistance": squeeze_resistance_strategy.run_strategy,
    # squeeze + adx > 25 策略【⭐⭐⭐⭐⭐】
    "squeeze_adx": squeeze_adx_strategy.run_strategy,
    # macd 双峰底背离 策略【⭐⭐⭐⭐】
    "macd_histogram_double_divergence": macd_histogram_double_divergence_strategy.run_strategy,
    # 交叉买入型号 策略【⭐⭐⭐⭐】
    "cross": cross_strategy.run_strategy,
    # 底部是否新低 策略【⭐⭐⭐】
    "aroon_oscillator": aroon_oscillator_strategy.run_strategy
}
