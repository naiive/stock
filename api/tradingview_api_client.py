import requests
import json
import time
import random
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple, Union


# --- 自定义异常类 ---
class TechIndicatorError(Exception):
    """用于表示技术指标获取或处理失败的自定义异常。"""
    pass


# --- TradingView API 配置 ---
BASE_URL = "https://scanner.tradingview.com/symbol"
# 所有请求的指标字段
ALL_FIELDS = "Recommend.Other,Recommend.All,Recommend.MA,RSI,RSI[1],Stoch.K,Stoch.D,Stoch.K[1],Stoch.D[1],CCI20,CCI20[1],ADX,ADX+DI,ADX-DI,ADX+DI[1],ADX-DI[1],AO,AO[1],AO[2],Mom,Mom[1],MACD.macd,MACD.signal,Rec.Stoch.RSI,Stoch.RSI.K,Rec.WR,W.R,Rec.BBPower,BBPower,Rec.UO,UO,EMA10,close,SMA10,EMA20,SMA20,EMA30,SMA30,EMA50,SMA50,EMA100,SMA100,EMA200,SMA200,Rec.Ichimoku,Ichimoku.BLine,Rec.VWMA,VWMA,Rec.HullMA9,HullMA9,Pivot.M.Classic.R3,Pivot.M.Classic.R2,Pivot.M.Classic.R1,Pivot.M.Classic.Middle,Pivot.M.Classic.S1,Pivot.M.Classic.S2,Pivot.M.Classic.S3,Pivot.M.Fibonacci.R3,Pivot.M.Fibonacci.R2,Pivot.M.Fibonacci.R1,Pivot.M.Fibonacci.Middle,Pivot.M.Fibonacci.S1,Pivot.M.Fibonacci.S2,Pivot.M.Fibonacci.S3,Pivot.M.Camarilla.R3,Pivot.M.Camarilla.R2,Pivot.M.Camarilla.R1,Pivot.M.Camarilla.Middle,Pivot.M.Camarilla.S1,Pivot.M.Camarilla.S2,Pivot.M.Camarilla.S3,Pivot.M.Woodie.R3,Pivot.M.Woodie.R2,Pivot.M.Woodie.R1,Pivot.M.Woodie.Middle,Pivot.M.Woodie.S1,Pivot.M.Woodie.S2,Pivot.M.Woodie.S3,Pivot.M.Demark.R1,Pivot.M.Demark.Middle,Pivot.M.Demark.S1"

# --- 指标中文名映射 ---
INDICATOR_NAME_MAP = {
    "Recommend.Other": "震荡指标综合建议", "Recommend.All": "所有指标综合建议", "Recommend.MA": "移动平均线综合建议",
    "RSI": "RSI(14)", "Stoch.K": "Stochastic %K (14, 3, 3)", "CCI20": "CCI指标(20)",
    "ADX": "平均趋向指数ADX(14)", "AO": "动量震荡指标(AO)", "Mom": "动量指标(10)",
    "MACD.macd": "MACD Level (12, 26)", "Stoch.RSI.K": "Stochastic RSI Fast", "W.R": "威廉百分比变动(14)",
    "BBPower": "牛熊力量(BBP)", "UO": "终极震荡指标UO",
    "EMA10": "指数移动平均线(10)", "SMA10": "简单移动平均线(10)", "EMA20": "指数移动平均线(20)",
    "SMA20": "简单移动平均线(20)", "EMA30": "指数移动平均线(30)", "SMA30": "简单移动平均线(30)",
    "EMA50": "指数移动平均线(50)", "SMA50": "简单移动平均线(50)", "EMA100": "指数移动平均线(100)",
    "SMA100": "简单移动平均线(100)", "EMA200": "指数移动平均线(200)", "SMA200": "简单移动平均线(200)",
    "Ichimoku.BLine": "一目均衡表基准线", "VWMA": "成交量加权移动平均线 VWMA (20)",
    "HullMA9": "船体移动平均线 Hull MA (9)",
    "close": "当前收盘价",
}


# --- 核心 API 调用函数 ---

def _fetch_indicators_by_symbol(
        full_symbol: str,
        fields: str = ALL_FIELDS
) -> Dict[str, Any]:
    """
    内部函数：直接调用 TradingView API。
    成功返回指标字典。
    失败时，直接抛出 requests.exceptions.RequestException 或 TechIndicatorError。
    """
    params = {
        'symbol': full_symbol,
        'fields': fields,
        'no_404': 'true',
        'label-product': 'popup-technicals'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Referer': f'https://cn.tradingview.com/symbols/{full_symbol}/technicals/',
        'Connection': 'keep-alive',
    }

    try:
        time.sleep(random.uniform(0.5, 1.5))
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)

        # 1. 检查 HTTP 状态码
        if response.status_code != 200:
            # 对于非 200 状态码，直接抛出 HTTPError
            response.raise_for_status()

            # 2. 解析 JSON
        data = response.json()

        # 3. 检查 API 响应结构
        if isinstance(data, dict):
            if data.get('s') == 'error':
                # API 内部错误，即使状态码为 200
                raise TechIndicatorError(f"API返回状态 'error' for {full_symbol}: {data.get('d', 'N/A')}")

            # 兼容 s:'ok' 结构
            if data.get('s') == 'ok':
                field_names = fields.split(',')
                field_values = data.get('d', [])
                return dict(zip(field_names, field_values))

            # 兼容扁平化 K-V 结构
            if any(key in data for key in fields.split(',')):
                return data

        # 结构异常
        raise TechIndicatorError(f"API返回数据结构异常 for {full_symbol}。")

    except requests.exceptions.RequestException as err:
        # 捕获连接、超时、HTTPError等
        raise err


def get_tech_indicators_robust(
        code: str
) -> pd.DataFrame:
    """
    主接口：接收六位代码，自动判断市场，执行双重查询。
    成功返回 DataFrame，失败则抛出异常。
    """
    if not code or not code.isdigit() or len(code) != 6:
        raise ValueError(f"输入代码 '{code}' 必须是六位数字。")

    # 1. 确定可能的市场顺序
    market_rules = {
        '6': ['SSE', 'SZSE'],  # 6开头：沪市优先
        '0': ['SZSE', 'SSE'],  # 0开头：深市优先 (主板/中小板)
        '3': ['SZSE', 'SSE'],  # 3开头：深市优先 (创业板)
    }
    first_digit = code[0]
    market_order = market_rules.get(first_digit, ['SSE', 'SZSE'])

    raw_data = None

    # 2. 循环尝试查询
    for market in market_order:
        full_symbol = f"{market}:{code}"
        print(f"-> 正在尝试查询符号: {full_symbol}")

        try:
            raw_data = _fetch_indicators_by_symbol(full_symbol)

            # 成功获取数据，检查关键字段
            if 'RSI' in raw_data and 'close' in raw_data:
                break  # 成功，跳出循环
            else:
                # 即使 API 返回成功，但关键字段缺失，视为数据不完整
                raise TechIndicatorError(f"成功获取 {full_symbol} 数据，但缺少关键指标（如RSI/close）无法进行分析。")

        except requests.exceptions.HTTPError as e:
            # 捕获 HTTPError，例如 404 Not Found。如果是 404，继续尝试下一个市场
            if e.response.status_code == 404:
                print(f"-> 符号 {full_symbol} 未找到 (404)，尝试下一个市场。")
                raw_data = None
                continue  # 尝试下一个市场
            else:
                # 其他 HTTP 错误（如 5xx 服务器错误），直接抛出
                raise e
        except Exception as e:
            # 捕获其他连接、解析、或自定义错误，如果是致命错误，停止尝试
            raw_data = None
            raise e  # 抛出连接、解析等错误

    # 3. 最终处理
    if raw_data:
        df = _format_indicators_to_dataframe(raw_data)
        if df is not None:
            return df
        else:
            # 理论上此路径应该被上面的 TechIndicatorError 捕获，以防万一
            raise TechIndicatorError(f"无法为 {code} 生成有效的 DataFrame。")
    else:
        # 两次尝试均失败且没有抛出其他致命错误，说明股票代码不存在
        raise TechIndicatorError(f"股票代码 {code} 在沪深两市中均未找到。")


# --- 分析和格式化函数 (操作建议统一为 '中立') ---

def _analyze_indicator(key: str, value: float, current_close: float) -> str:
    """ 根据指标给出操作建议，非明确信号统一返回 '中立'。"""
    if not isinstance(value, (int, float)): return "中立"

    if key in ["Recommend.All", "Recommend.MA", "Recommend.Other"]:
        if value > 0.5:
            return "强力买入" if key == "Recommend.MA" else "买入"
        elif value < -0.5:
            return "强力卖出" if key == "Recommend.MA" else "卖出"
        elif value >= 0.1:
            return "买入"
        elif value <= -0.1:
            return "卖出"
        else:
            return "中立"
    elif key == "RSI" or key == "Stoch.K" or key == "Stoch.D":
        if value > 70 or value > 80:
            return "卖出"
        elif value < 30 or value < 20:
            return "买入"
        else:
            return "中立"
    elif key == "CCI20":
        if value > 100:
            return "卖出"
        elif value < -100:
            return "买入"
        else:
            return "中立"
    elif key == "W.R":
        if value > -20:
            return "卖出"
        elif value < -80:
            return "买入"
        else:
            return "中立"
    elif key in ["AO", "MACD.macd", "BBPower", "Mom"]:
        if value > 0:
            return "买入"
        elif value < 0:
            return "卖出"
        else:
            return "中立"
    elif key.startswith(("EMA", "SMA", "VWMA", "HullMA9")):
        if current_close > value:
            return "买入"
        elif current_close < value:
            return "卖出"
        else:
            return "中立"
    else:
        return "中立"


def _format_indicators_to_dataframe(indicators: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    根据获取的指标数据，格式化为 Pandas DataFrame。
    """
    data_list = []
    current_close = indicators.get('close', None)

    for key in ALL_FIELDS.split(','):
        if key in indicators and (
                key in INDICATOR_NAME_MAP or key in ["Recommend.All", "Recommend.MA", "Recommend.Other"]):
            value = indicators[key]
            cn_name = INDICATOR_NAME_MAP.get(key, key)
            action = _analyze_indicator(key, value, current_close)

            data_list.append({
                '中文名称': cn_name,
                '当前值': value,
                '操作建议': action
            })

    if not data_list:
        return None

    df = pd.DataFrame(data_list)
    df = df.set_index('中文名称')

    return df


# --- 运行示例 (使用 try...except 捕获错误) ---

if __name__ == '__main__':
    # 示例 1: 成功查询 (600519)
    code_success = "600519"
    print(f"--- 示例 1: 尝试查询 {code_success} (成功) ---")
    try:
        df_indicators = get_tech_indicators_robust(code_success)
        print(f"\n✅ 查询成功！代码: {code_success}")
        print(df_indicators)
    except Exception as e:
        print(f"\n❌ 查询失败！捕获到异常: {type(e).__name__}: {e}")

    print("\n" + "=" * 60 + "\n")

    # 示例 2: 失败查询 (不存在的代码 999999)
    code_failure = "999999"
    print(f"--- 示例 2: 尝试查询 {code_failure} (失败/抛出 TechIndicatorError) ---")
    try:
        df_indicators = get_tech_indicators_robust(code_failure)
    except TechIndicatorError as e:
        print(f"\n❌ 查询失败！捕获到自定义异常: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"\n❌ 查询失败！捕获到其他异常: {type(e).__name__}: {e}")