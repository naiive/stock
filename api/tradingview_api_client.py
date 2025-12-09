import requests
import json
import time
import random
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple, Union

# =========================================================
#  ✅ 查询成功！返回 DataFrame 结构:
#     震荡指标综合建议   所有指标综合建议  ... Pivot.M.Demark.Middle Pivot.M.Demark.S1
#  0  -0.1818 (卖出)  -0.5576 (卖出)  ...        1460.6625 (中立)    1435.2550 (中立)
#
#  ❌ 查询失败！返回 DataFrame 结构:
#     震荡指标综合建议   所有指标综合建议  ... Pivot.M.Demark.Middle Pivot.M.Demark.S1
#  0  N/A (N/A)  N/A (N/A)  ...             N/A (N/A)         N/A (N/A)

# ============================================================
# --- 自定义异常类 (保留但不会在主逻辑中抛出) ---
class TechIndicatorError(Exception):
    """用于表示技术指标获取或处理失败的自定义异常。"""
    pass


# --- TradingView API 配置 ---
BASE_URL = "https://scanner.tradingview.com/symbol"
# 所有请求的指标字段 (用于 API 请求)
ALL_FIELDS_RAW = "Recommend.Other,Recommend.All,Recommend.MA,RSI,RSI[1],Stoch.K,Stoch.D,Stoch.K[1],Stoch.D[1],CCI20,CCI20[1],ADX,ADX+DI,ADX-DI,ADX+DI[1],ADX-DI[1],AO,AO[1],AO[2],Mom,Mom[1],MACD.macd,MACD.signal,Rec.Stoch.RSI,Stoch.RSI.K,Rec.WR,W.R,Rec.BBPower,BBPower,Rec.UO,UO,EMA10,close,SMA10,EMA20,SMA20,EMA30,SMA30,EMA50,SMA50,EMA100,SMA100,EMA200,SMA200,Rec.Ichimoku,Ichimoku.BLine,Rec.VWMA,VWMA,Rec.HullMA9,HullMA9,Pivot.M.Classic.R3,Pivot.M.Classic.R2,Pivot.M.Classic.R1,Pivot.M.Classic.Middle,Pivot.M.Classic.S1,Pivot.M.Classic.S2,Pivot.M.Classic.S3,Pivot.M.Fibonacci.R3,Pivot.M.Fibonacci.R2,Pivot.M.Fibonacci.R1,Pivot.M.Fibonacci.Middle,Pivot.M.Fibonacci.S1,Pivot.M.Fibonacci.S2,Pivot.M.Fibonacci.S3,Pivot.M.Camarilla.R3,Pivot.M.Camarilla.R2,Pivot.M.Camarilla.R1,Pivot.M.Camarilla.Middle,Pivot.M.Camarilla.S1,Pivot.M.Camarilla.S2,Pivot.M.Camarilla.S3,Pivot.M.Woodie.R3,Pivot.M.Woodie.R2,Pivot.M.Woodie.R1,Pivot.M.Woodie.Middle,Pivot.M.Woodie.S1,Pivot.M.Woodie.S2,Pivot.M.Woodie.S3,Pivot.M.Demark.R1,Pivot.M.Demark.Middle,Pivot.M.Demark.S1"

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

# 提取所有中文指标名称，用于创建空 DataFrame
ALL_CN_NAMES = [
    INDICATOR_NAME_MAP.get(key, key)
    for key in ALL_FIELDS_RAW.split(',')
    if INDICATOR_NAME_MAP.get(key, key) is not None  # 过滤掉不在映射中的原始字段（保持兼容性）
]


# --- 辅助函数：创建固定列的空结果 DataFrame ---

def _create_empty_result_df() -> pd.DataFrame:
    """
    创建包含所有固定指标列，且值为 'N/A' 的单行 DataFrame。
    """
    empty_dict = {cn_name: "N/A (N/A)" for cn_name in ALL_CN_NAMES}
    return pd.DataFrame([empty_dict])


# --- 核心 API 调用函数 ---

def _fetch_indicators_by_symbol(
        full_symbol: str,
        fields: str = ALL_FIELDS_RAW
) -> Dict[str, Any]:
    """
    内部函数：直接调用 TradingView API。
    成功返回指标字典。失败时，仍允许抛出异常，以便被 get_tech_indicators_robust 捕获。
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

    # ❗ 注意：此函数内部仍然抛出异常，以便 get_tech_indicators_robust 可以捕获它们
    try:
        time.sleep(random.uniform(0.5, 1.5))
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)

        # 1. 检查 HTTP 状态码
        if response.status_code != 200:
            response.raise_for_status()

        # 2. 解析 JSON
        data = response.json()

        # 3. 检查 API 响应结构
        if isinstance(data, dict):
            if data.get('s') == 'error':
                raise TechIndicatorError(f"API返回状态 'error' for {full_symbol}: {data.get('d', 'N/A')}")

            if data.get('s') == 'ok':
                field_names = fields.split(',')
                field_values = data.get('d', [])
                return dict(zip(field_names, field_values))

            if any(key in data for key in fields.split(',')):
                return data

        # 结构异常
        raise TechIndicatorError(f"API返回数据结构异常 for {full_symbol}。")

    except requests.exceptions.RequestException as err:
        raise err


# --- 主查询函数 (修改为不抛出异常) ---
def get_tech_indicators_robust(
        code: str
) -> pd.DataFrame:
    """
    主接口：接收六位代码，自动判断市场，执行双重查询。
    成功返回包含所有指标的 DataFrame；失败时返回填充了 'N/A' 的 DataFrame，绝不抛出异常。
    """
    # 1. 输入校验
    if not code or not code.isdigit() or len(code) != 6:
        print(f"[警告] 代码格式错误: {code}")
        return _create_empty_result_df()

    # 2. 确定可能的市场顺序
    market_rules = {
        '6': ['SSE', 'SZSE'],
        '0': ['SZSE', 'SSE'],
        '3': ['SZSE', 'SSE'],
    }
    first_digit = code[0]
    market_order = market_rules.get(first_digit, ['SSE', 'SZSE'])

    raw_data = None

    # 3. 循环尝试查询并捕获所有错误
    try:
        for market in market_order:
            full_symbol = f"{market}:{code}"
            print(f"-> 正在尝试查询符号: {full_symbol}")

            try:
                raw_data = _fetch_indicators_by_symbol(full_symbol)

                # 成功获取数据，检查关键字段
                if 'RSI' in raw_data and 'close' in raw_data:
                    break  # 成功，跳出循环
                else:
                    # 即使 API 返回成功，但关键字段缺失，继续尝试下一个市场或认为失败
                    print(f"[警告] {full_symbol} 关键数据缺失，尝试下一个市场或视为失败。")
                    raw_data = None
                    continue  # 尝试下一个市场

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"-> 符号 {full_symbol} 未找到 (404)，尝试下一个市场。")
                    raw_data = None
                    continue
                else:
                    # 其他 HTTP 错误，记录并退出内层 try，继续尝试下一个市场
                    print(f"[警告] {full_symbol} HTTP 错误 ({e.response.status_code})，尝试下一个市场。")
                    raw_data = None
                    continue
            except Exception as e:
                # 捕获其他连接、解析、或自定义错误，记录并继续尝试下一个市场
                print(f"[警告] {full_symbol} 发生异常 ({type(e).__name__}: {e})，尝试下一个市场。")
                raw_data = None
                continue

        # 4. 最终处理
        if raw_data:
            df = _format_indicators_to_dataframe(raw_data)
            # 即使格式化失败，也不抛出异常，而是返回空 DataFrame
            if df is not None:
                return df
            else:
                print(f"[警告] {code} 格式化数据失败。")
                return _create_empty_result_df()
        else:
            # 两次尝试均失败
            print(f"[警告] 股票代码 {code} 两次查询均未成功。")
            return _create_empty_result_df()

    # 5. 捕获所有致命的外部异常（例如网络初始化失败）
    except Exception as e:
        print(f"[致命警告] 股票代码 {code} 发生顶级异常 ({type(e).__name__}: {e})。")
        return _create_empty_result_df()


# --- 分析函数 (保持不变) ---
def _analyze_indicator(key: str, value: float, current_close: float) -> str:
    # ... (此函数内容保持不变，负责计算操作建议)
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


# --- 格式化函数 (确保包含所有指标，缺失的用 'N/A' 代替) ---
def _format_indicators_to_dataframe(indicators: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    根据获取的指标数据，格式化为 Pandas DataFrame。
    将所有指标的中文名称作为列，当前值和操作建议拼接后作为单元格内容。
    对于缺失的指标，使用 'N/A' 代替。
    """
    data_dict = {}
    current_close = indicators.get('close', None)

    for key_raw in ALL_FIELDS_RAW.split(','):
        cn_name = INDICATOR_NAME_MAP.get(key_raw, key_raw)

        # 检查指标是否存在于 API 响应中
        if key_raw in indicators and indicators[key_raw] is not None:
            value = indicators[key_raw]
            action = _analyze_indicator(key_raw, value, current_close)

            # 格式化值
            if isinstance(value, (float, int)):
                formatted_value = f"{value:.4f}"
            else:
                formatted_value = str(value)

            # 拼接： "值 (建议)"
            data_dict[cn_name] = f"{formatted_value} ({action})"
        else:
            # 缺失或值为 None，使用空值代替
            data_dict[cn_name] = "N/A (N/A)"

    if not data_dict:
        # 理论上不会发生，因为 ALL_FIELDS_RAW 列表是固定的
        return None

    # 构造单行 DataFrame，列顺序根据 ALL_CN_NAMES 确定
    df = pd.DataFrame([data_dict], columns=ALL_CN_NAMES)

    return df


# --- 运行示例 (展示成功和失败都会返回 DataFrame) ---

if __name__ == '__main__':
    # 示例 1: 成功查询 (600519)
    code_success = "600519"
    print(f"--- 示例 1: 尝试查询 {code_success} (成功) ---")
    df_indicators_success = get_tech_indicators_robust(code_success)
    print(f"\n✅ 查询成功！返回 DataFrame 结构:")
    print(df_indicators_success.head())
    print(f"列数：{len(df_indicators_success.columns)}")
    print(f"部分列名：{list(df_indicators_success.columns)[:5]}...")

    print("\n" + "=" * 60 + "\n")

    # 示例 2: 失败查询 (不存在的代码 999999)
    code_failure = "999999"
    print(f"--- 示例 2: 尝试查询 {code_failure} (失败/返回空值 DataFrame) ---")
    df_indicators_failure = get_tech_indicators_robust(code_failure)
    print(f"\n❌ 查询失败！返回 DataFrame 结构:")
    print(df_indicators_failure.head())
    print(f"列数：{len(df_indicators_failure.columns)}")
    print(f"第一个单元格内容：{df_indicators_failure.iloc[0, 0]}")