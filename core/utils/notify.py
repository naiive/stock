# -*- coding: utf-8 -*-
"""
通知工具集：邮件与 Telegram 推送（TradingView Scan 风格）
- 自动适配不固定列
- CSV → Scan 卡片风格输出
- Telegram / Email 内容完全一致
- ✅ Telegram 分页显示，总览只在第一页出现
"""

from __future__ import annotations

import os
import ssl
import json
import time
import math
import smtplib
import urllib.request
import urllib.parse
from typing import List, Optional, Tuple

import pandas as pd
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders

from conf.config import SYSTEM_CONFIG, EMAIL_CONFIG, TELEGRAM_CONFIG, PATH_CONFIG, STRATEGY_CONFIG
from core.map.emoji_map import hist_emoji_map, break_emoji_map

# =====================================================
# Email
# =====================================================
def send_email(
    smtp_host: str,
    smtp_port: int,
    use_ssl: bool,
    username: str,
    password: str,
    sender: str,
    to_list: List[str],
    subject: str,
    body: str,
    attachment_path: Optional[str] = None,
) -> bool:
    try:
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = ", ".join([x for x in to_list if x])
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{os.path.basename(attachment_path)}"'
            )
            msg.attach(part)

        if use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
                if username:
                    server.login(username, password)
                server.sendmail(sender, to_list, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.ehlo()
                try:
                    server.starttls()
                except Exception:
                    pass
                if username:
                    server.login(username, password)
                server.sendmail(sender, to_list, msg.as_string())

        print("📧 Email sent")
        return True
    except Exception as e:
        print(f"⚠️ Email failed: {e}")
        return False


# =====================================================
# Telegram HTTP
# =====================================================
def _http_post_form(url: str, data: dict) -> dict:
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=encoded)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def send_telegram(
    bot_token: str,
    chat_id: str,
    text: str,
    disable_web_page_preview: bool = True,
    parse_mode: str = "HTML",
) -> bool:
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
            "parse_mode": parse_mode,
        }
        resp = _http_post_form(url, payload)
        if not resp.get("ok"):
            raise RuntimeError(resp)
        print("🤖 Telegram sent")
        return True
    except Exception as e:
        print(f"⚠️ Telegram failed: {e}")
        return False



def parse_histogram_emoji(val) -> str:
    if not val or pd.isna(val):
        return ""
    parts = str(val).split("-")
    out = []
    for p in parts:
        for k, e in hist_emoji_map.items():
            if p.startswith(k):
                out.append(e)
                break
    return "".join(out)


def parse_break_emoji(val) -> str:
    if not val or pd.isna(val):
        return ""
    return "".join(break_emoji_map.get(x, "") for x in str(val).split("-"))


def fmt_pct(val) -> str:
    if val is None or pd.isna(val):
        return "NA"
    return f"{val:+.2f}%"


# =====================================================
# TradingView Scan 卡片
# =====================================================
def build_tv_card(row: pd.Series) -> str:
    name = row.get("名称", "")
    code = str(row.get("代码", ""))  # 确保是字符串

    price = row.get("收盘价", row.get("现价", ""))
    chg = fmt_pct(row.get("涨幅(%)"))
    ytd = fmt_pct(row.get("年涨幅(%)"))

    turnover = row.get("换手率(%)", "")
    pe = row.get("市盈率(动)", "")

    squeeze_days = row.get("挤压天数", "")

    ath_val = str(row.get("是否ATH", "")).strip()
    ath = "YES ATH" if ath_val == "是" else "No ATH"

    hist = parse_histogram_emoji(row.get("动能情况"))
    brk = parse_break_emoji(row.get("突破趋势"))

    mv = row.get("总市值(亿)", "")
    date = str(row.get("日期", ""))[5:10]

    lines = []

    code_str = ""
    if code:
        if code.startswith("60"):
            tv_prefix = "SSE"
        elif code.startswith("00") or code.startswith("30"):
            tv_prefix = "SZSE"
        else:
            tv_prefix = ""

        if tv_prefix:
            tv_link = f"https://cn.tradingview.com/chart/?symbol={tv_prefix}%3A{code}"
            code_str = f'<a href="{tv_link}">{code}</a>'
        else:
            code_str = code

    if name or code:
        lines.append(f"💹 {name} · {code_str}")

    if price:
        lines.append(f"💰 {price}（{chg}）🗓 年 {ytd}")

    if turnover or pe:
        parts = []
        if turnover:
            parts.append(f"🔄 换手 {turnover}%")
        if pe:
            parts.append(f"📐 PE {pe}")
        lines.append("       ".join(parts))

    if squeeze_days:
        lines.append(f"🧨 挤压 {squeeze_days} 天     📍 {ath}")

    if hist:
        lines.append(f"📊 动能 {hist}")

    if brk:
        lines.append(f"🚀 趋势 {brk}")

    if mv or date:
        parts = []
        if mv:
            parts.append(f"🏛 市值 {mv}亿")
        if date:
            parts.append(f"📅 {date}")
        lines.append("  ".join(parts))

    return "\n".join(lines)


# =====================================================
# Unified message builder（Telegram / Email 共用）
# =====================================================
def build_unified_message(
    df: Optional[pd.DataFrame],
    file_path: Optional[str],
    total_cnt: int = 0,
    page_no: int = 1,
    page_cnt: int = 1,
    is_first_page: bool = True,
) -> Tuple[str, str]:

    if is_first_page:
        title = f"📈 扫描完成：{total_cnt} 条信号"
        lines = [
            f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"文件：{os.path.basename(file_path) if file_path else '<未落盘>'}",
            "",
            f"📄 第 {page_no}/{page_cnt} 页",
            "────────────────",
            "",
        ]
    else:
        title = f"📄 扫描结果 · 第 {page_no}/{page_cnt} 页"
        lines = [
            "────────────────",
            "",
        ]

    if isinstance(df, pd.DataFrame) and not df.empty:
        for _, row in df.iterrows():
            lines.append(build_tv_card(row))
            lines.append("")
    else:
        if is_first_page:
            lines.append("（无信号数据）")

    return title, "\n".join(lines)


def clip_for_telegram(text: str, limit: int = 3800) -> str:
    return text if len(text) <= limit else text[:limit] + "\n...（已截断）"


# =====================================================
# Notify entry
# =====================================================
def post_export_notify(
    file_path: Optional[str],
    df: Optional[pd.DataFrame],
    max_rows_per_msg: int = 10,
) -> None:

    if isinstance(df, pd.DataFrame) and not df.empty and SYSTEM_CONFIG.get("ENABLE_TELEGRAM"):
        # 排序
        if "绿色动能" in df.columns and "挤压天数" in df.columns:
            # 先按绿色动能降序，再按挤压天数降序
            df = df.sort_values(by=["绿色动能", "挤压天数"], ascending=[False, False])
        elif "绿色动能" in df.columns:
            df = df.sort_values(by="绿色动能", ascending=False)
        elif "挤压天数" in df.columns:
            df = df.sort_values(by="挤压天数", ascending=False)

        total_cnt = len(df)
        page_cnt = math.ceil(total_cnt / max_rows_per_msg)

        for idx, start in enumerate(range(0, total_cnt, max_rows_per_msg), start=1):
            sub_df = df.iloc[start:start + max_rows_per_msg]
            title, body = build_unified_message(
                sub_df,
                file_path,
                total_cnt=total_cnt,
                page_no=idx,
                page_cnt=page_cnt,
                is_first_page=(idx == 1),
            )
            send_telegram(
                TELEGRAM_CONFIG["BOT_TOKEN"],
                str(TELEGRAM_CONFIG["CHAT_ID"]).strip(),
                clip_for_telegram(f"{title}\n\n{body}"),
                TELEGRAM_CONFIG.get("DISABLE_WEB_PAGE_PREVIEW", True),
            )
            time.sleep(1)

    if SYSTEM_CONFIG.get("ENABLE_EMAIL"):
        title, body = build_unified_message(df, file_path)
        send_email(
            EMAIL_CONFIG["SMTP_HOST"],
            int(EMAIL_CONFIG.get("SMTP_PORT", 465)),
            EMAIL_CONFIG.get("USE_SSL", True),
            EMAIL_CONFIG["USERNAME"],
            EMAIL_CONFIG["PASSWORD"],
            EMAIL_CONFIG["FROM"],
            EMAIL_CONFIG["TO"],
            title,
            body,
            file_path,
        )


# =====================================================
# CSV export + notify
# =====================================================
def export_and_notify(df: Optional[pd.DataFrame]) -> Optional[str]:
    file_path = None

    if SYSTEM_CONFIG.get("ENABLE_EXPORT", True):
        date_str = time.strftime('%Y%m%d')
        save_dir = os.path.join(PATH_CONFIG["OUTPUT_FOLDER_BASE"], date_str)
        os.makedirs(save_dir, exist_ok=True)
        strategy_name = STRATEGY_CONFIG.get("RUN_STRATEGY", "strategy")
        file_path = os.path.join(save_dir, f"{strategy_name}_{time.strftime('%H%M%S')}.csv")
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"🎉 导出成功：{file_path}")

    post_export_notify(file_path, df)
    return file_path
