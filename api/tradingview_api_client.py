import requests
import json
import time
import random
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple, Union

# =========================================================
# TradingView API 配置
# =========================================================
BASE_URL = "https://scanner.tradingview.com/symbol"
# 原始指标字段 (包含所有，用于过滤)
ALL_FIELDS_RAW_FULL = "Recommend.Other,Recommend.All,Recommend.MA,RSI,RSI[1],Stoch.K,Stoch.D,Stoch.K[1],Stoch.D[1],CCI20,CCI20[1],ADX,ADX+DI,ADX-DI,ADX+DI[1],ADX-DI[1],AO,AO[1],AO[2],Mom,Mom[1],MACD.macd,MACD.signal,Rec.Stoch.RSI,Stoch.RSI.K,Rec.WR,W.R,Rec.BBPower,BBPower,Rec.UO,UO,EMA10,close,SMA10,EMA20,SMA20,EMA30,SMA30,EMA50,SMA50,EMA100,SMA100,EMA200,SMA200,Rec.Ichimoku,Ichimoku.BLine,Rec.VWMA,VWMA,Rec.HullMA9,HullMA9,Pivot.M.Classic.R3,Pivot.M.Classic.R2,Pivot.M.Classic.R1,Pivot.M.Classic.Middle,Pivot.M.Classic.S1,Pivot.M.Classic.S2,Pivot.M.Classic.S3,Pivot.M.Fibonacci.R3,Pivot.M.Fibonacci.R2,Pivot.M.Fibonacci.R1,Pivot.M.Fibonacci.Middle,Pivot.M.Fibonacci.S1,Pivot.M.Fibonacci.S2,Pivot.M.Fibonacci.S3,Pivot.M.Camarilla.R3,Pivot.M.Camarilla.R2,Pivot.M.Camarilla.R1,Pivot.M.Camarilla.Middle,Pivot.M.Camarilla.S1,Pivot.M.Camarilla.S2,Pivot.M.Camarilla.S3,Pivot.M.Woodie.R3,Pivot.M.Woodie.R2,Pivot.M.Woodie.R1,Pivot.M.Woodie.Middle,Pivot.M.Woodie.S1,Pivot.M.Woodie.S2,Pivot.M.Woodie.S3,Pivot.M.Demark.R1,Pivot.M.Demark.Middle,Pivot.M.Demark.S1"

# 1. 提取非 Pivot 字段
ALL_FIELDS_RAW_LIST_NO_PIVOT = [f for f in ALL_FIELDS_RAW_FULL.split(',') if not f.startswith('Pivot.M.')]

# --- 指标中文名映射 ---
INDICATOR_NAME_MAP = {
    "Recommend.All": "所有指标综合建议",
    "Recommend.Other": "震荡指标综合建议",
    "Recommend.MA": "移动平均线综合建议",
    "RSI": "RSI(14)",
    "Stoch.K": "Stochastic %K (14, 3, 3)",
    "CCI20": "CCI指标(20)",
    "ADX": "平均趋向指数ADX(14)",
    "AO": "动量震荡指标(AO)",
    "Mom": "动量指标(10)",
    "MACD.macd": "MACD Level (12, 26)",
    "Stoch.RSI.K": "Stochastic RSI Fast",
    "W.R": "威廉百分比变动(14)",
    "BBPower": "牛熊力量(BBP)",
    "UO": "终极震荡指标UO",
    "EMA10": "指数移动平均线(10)",
    "SMA10": "简单移动平均线(10)",
    "EMA20": "指数移动平均线(20)",
    "SMA20": "简单移动平均线(20)",
    "EMA30": "指数移动平均线(30)",
    "SMA30": "简单移动平均线(30)",
    "EMA50": "指数移动平均线(50)",
    "SMA50": "简单移动平均线(50)",
    "EMA100": "指数移动平均线(100)",
    "SMA100": "简单移动平均线(100)",
    "EMA200": "指数移动平均线(200)",
    "SMA200": "简单移动平均线(200)",
    "Ichimoku.BLine": "一目均衡表基准线",
    "VWMA": "成交量加权移动平均线 VWMA (20)",
    "HullMA9": "船体移动平均线 Hull MA (9)",
    "close": "当前收盘价",
}

# 2. 定义需要特殊排序的字段和中文名
# ✅ 调整顺序：让 '所有指标综合建议' 在 '震荡指标综合建议' 前面
SPECIAL_FIELDS = ["Recommend.All", "Recommend.Other", "Recommend.MA"]
SPECIAL_CN_NAMES = ["所有指标综合建议", "震荡指标综合建议", "移动平均线综合建议"]

# 3. 提取剩余字段
OTHER_FIELDS = [f for f in ALL_FIELDS_RAW_LIST_NO_PIVOT if f not in SPECIAL_FIELDS]

# 4. 重建 API 请求字段字符串 (SPECIAL_FIELDS 优先)
ALL_FIELDS_RAW = ','.join(SPECIAL_FIELDS + OTHER_FIELDS)

# 5. 重建中文列名列表
OTHER_CN_NAMES = [
    INDICATOR_NAME_MAP.get(key, key)
    for key in OTHER_FIELDS
]

# ✅ 重建 ALL_CN_NAMES_RAW: 确保特殊字段在列表的最前面
ALL_CN_NAMES_RAW = SPECIAL_CN_NAMES + OTHER_CN_NAMES

# 最终输出的列名列表：将 "代码" 放在最前面
ALL_CN_NAMES = ["代码"] + ALL_CN_NAMES_RAW


# --- 自定义异常类 (保留但不会在主逻辑中抛出) ---
class TechIndicatorError(Exception):
    """用于表示技术指标获取或处理失败的自定义异常。"""
    pass


# --- 辅助函数：创建固定列的空结果 DataFrame ---

def _create_empty_result_df(code: str = "N/A") -> pd.DataFrame:
    """
    创建包含所有固定指标列，且值为 'N/A' 的单行 DataFrame。
    """
    # 初始化所有指标列为 'N/A (N/A)'
    empty_dict = {cn_name: "N/A (N/A)" for cn_name in ALL_CN_NAMES_RAW}

    # 增加 "代码" 字段
    empty_dict["代码"] = code

    # 使用 ALL_CN_NAMES 确保列的顺序是正确的
    return pd.DataFrame([empty_dict], columns=ALL_CN_NAMES)


# --- 核心 API 调用函数 ---

def _fetch_indicators_by_symbol(
        full_symbol: str,
        fields: str = ALL_FIELDS_RAW
) -> Dict[str, Any]:
    """
    内部函数：直接调用 TradingView API。
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


# --- 分析函数 (保持不变) ---
def _analyze_indicator(key: str, value: float, current_close: float) -> str:
    """根据指标名称和值计算操作建议。"""
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
        if current_close is not None and current_close > value:
            return "买入"
        elif current_close is not None and current_close < value:
            return "卖出"
        else:
            return "中立"
    else:
        return "中立"


# --- 格式化函数 ---
def _format_indicators_to_dataframe(indicators: Dict[str, Any], code: str) -> Optional[pd.DataFrame]:
    """
    根据获取的指标数据，格式化为 Pandas DataFrame。
    """
    data_dict = {}
    current_close = indicators.get('close', None)

    # 循环时使用不含 Pivot 的 ALL_FIELDS_RAW (已按顺序调整)
    for key_raw in ALL_FIELDS_RAW.split(','):
        cn_name = INDICATOR_NAME_MAP.get(key_raw, key_raw)

        if key_raw in indicators and indicators[key_raw] is not None:
            value = indicators[key_raw]
            action = _analyze_indicator(key_raw, value, current_close)

            # 格式化值
            if isinstance(value, (float, int)):
                if key_raw in ["Recommend.Other", "Recommend.All", "Recommend.MA", "close"]:
                    formatted_value = f"{value:.4f}".rstrip('0').rstrip('.') if value != 0 else "0"
                else:
                    formatted_value = f"{value:.4f}"

            elif isinstance(value, str):
                formatted_value = value
            else:
                formatted_value = str(value)

            data_dict[cn_name] = f"{formatted_value} ({action})"
        else:
            data_dict[cn_name] = "N/A (N/A)"

    # 插入股票代码
    data_dict["代码"] = code

    if not data_dict:
        return None

    # 构造单行 DataFrame，使用 ALL_CN_NAMES 确定列的最终顺序
    df = pd.DataFrame([data_dict], columns=ALL_CN_NAMES)

    return df


# --- 主查询函数 (保持不变) ---
def get_tech_indicators_robust(
        code: str
) -> pd.DataFrame:
    """
    主接口：接收六位代码，自动判断市场，执行双重查询。
    成功返回包含所有指标的 DataFrame；失败时返回填充了 'N/A' 的 DataFrame，绝不抛出异常。
    """
    code = str(code).strip()

    # 1. 输入校验
    if not code or not code.isdigit() or len(code) != 6:
        print(f"[警告] 代码格式错误: {code}")
        return _create_empty_result_df(code=code)

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
            print(f"-> 正在查询tradingview_api: {full_symbol}")

            try:
                raw_data = _fetch_indicators_by_symbol(full_symbol)

                # 成功获取数据，检查关键字段
                if raw_data and 'RSI' in raw_data and 'close' in raw_data:
                    break  # 成功，跳出循环
                else:
                    print(f"[警告] {full_symbol} 关键数据缺失，尝试下一个市场或视为失败。")
                    raw_data = None
                    continue

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"-> 符号 {full_symbol} 未找到 (404)，尝试下一个市场。")
                    raw_data = None
                    continue
                else:
                    print(f"[警告] {full_symbol} HTTP 错误 ({e.response.status_code})，尝试下一个市场。")
                    raw_data = None
                    continue
            except Exception as e:
                print(f"[警告] {full_symbol} 发生异常 ({type(e).__name__}: {e})，尝试下一个市场。")
                raw_data = None
                continue

        # 4. 最终处理
        if raw_data:
            df = _format_indicators_to_dataframe(raw_data, code=code)
            if df is not None:
                return df
            else:
                print(f"[警告] {code} 格式化数据失败。")
                return _create_empty_result_df(code=code)
        else:
            print(f"[警告] 股票代码 {code} 两次查询均未成功。")
            return _create_empty_result_df(code=code)

    # 5. 捕获所有致命的外部异常
    except Exception as e:
        print(f"[致命警告] 股票代码 {code} 发生顶级异常 ({type(e).__name__}: {e})。")
        return _create_empty_result_df(code=code)


# --- 运行示例 ---

if __name__ == '__main__':
    # 示例 1: 成功查询 (600519)
    code_success = "600519"
    print(f"--- 示例 1: 尝试查询 {code_success} (成功) ---")
    df_indicators_success = get_tech_indicators_robust(code_success)
    print(f"\n✅ 查询成功！返回 DataFrame 结构:")

    # 打印前 5 列，验证顺序: 代码, 所有指标, 震荡指标, 移动平均线, RSI
    print(df_indicators_success.iloc[:, :5].head())

    total_cols = len(df_indicators_success.columns)
    print(f"\n📢 总列数：{total_cols}。 (Pivot Points 已移除)")

    # 验证关键列的顺序
    print(f"列 1: {df_indicators_success.columns[0]}")
    print(f"列 2: {df_indicators_success.columns[1]}")  # 预期：所有指标综合建议
    print(f"列 3: {df_indicators_success.columns[2]}")  # 预期：震荡指标综合建议

    print("\n" + "=" * 60 + "\n")

    # 示例 2: 失败查询 (不存在的代码 999999)
    code_failure = "999999"
    print(f"--- 示例 2: 尝试查询 {code_failure} (失败/返回空值 DataFrame) ---")
    df_indicators_failure = get_tech_indicators_robust(code_failure)
    print(f"\n❌ 查询失败！返回 DataFrame 结构:")
    print(df_indicators_failure.iloc[:, :5].head())
    print(f"总列数：{len(df_indicators_failure.columns)}")
    print(f"第一个单元格内容 (代码): {df_indicators_failure.iloc[0, 0]}")