# -*- coding: utf-8 -*-
"""
通知工具集：邮件与 Telegram 推送（改进版）
- 自动适配不固定列
- JSON / 字典列自动格式化
- Telegram 消息美化显示
"""

from __future__ import annotations

import os
import ssl
import json
import time
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
) -> bool:
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
        }
        resp = _http_post_form(url, payload)
        if not resp.get("ok"):
            raise RuntimeError(resp)
        print("🤖 Telegram sent")
        return True
    except Exception as e:
        print(f"⚠️ Telegram failed: {e}")
        return False

# =====================================================
# JSON / dict 格式化工具
# =====================================================
def format_dict_or_json(val) -> str:
    if isinstance(val, dict):
        return " | ".join([f"{k}:{v}" for k, v in val.items()])
    elif isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, dict):
                return " | ".join([f"{k}:{v}" for k, v in parsed.items()])
            elif isinstance(parsed, list):
                return ", ".join([str(x) for x in parsed])
            else:
                return str(parsed)
        except Exception:
            return val
    else:
        return str(val)

# =====================================================
# Unified message builder
# =====================================================
def build_unified_message(
    df: Optional[pd.DataFrame],
    file_path: Optional[str],
    max_rows: int = 8,
) -> Tuple[str, str]:
    hit_cnt = int(len(df)) if isinstance(df, pd.DataFrame) else 0
    title = f"📈 扫描完成：{hit_cnt} 条信号"

    lines = [
        f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"命中数量：{hit_cnt}",
        f"文件：{os.path.basename(file_path) if file_path else '<未落盘>'}",
        "",
    ]

    if isinstance(df, pd.DataFrame) and not df.empty:
        for _, row in df.head(max_rows).iterrows():
            # 标题行：名称 + 代码
            name_code = f"🔹 {row.get('名称','')} ({row.get('代码','')})"
            lines.append(name_code)

            for col in row.index:
                if col in ["名称", "代码"]:
                    continue
                val_str = format_dict_or_json(row[col])
                # 关键字段加 Emoji
                if "价" in col or "当前" in col:
                    lines.append(f"💰 {col}: {val_str}")
                elif "涨幅" in col:
                    lines.append(f"📈 {col}: {val_str}%")
                elif "市值" in col:
                    lines.append(f"🏦 {col}: {val_str} 亿")
                elif "换手率" in col:
                    lines.append(f"🔄 {col}: {val_str}%")

                elif "SQZ" in col or "评分" in col:
                    lines.append(f"🎯 {col}: {val_str}")
                elif "止损" in col:
                    lines.append(f"⚠️ {col}: {val_str}")
                elif "ADX" in col:
                    lines.append(f"📊 {col}: {val_str}")

                elif "左波峰日期" in col:
                    lines.append(f"📅 {col}: {val_str}")
                elif "右波峰日期" in col:
                    lines.append(f"📅️ {col}: {val_str}")
                elif "EMA200" in col:
                    lines.append(f"📈 {col}: {val_str}")

                elif "红线" in col:
                    lines.append(f"🔴 {col}: {val_str}")
                elif "绿线" in col:
                    lines.append(f"🟢 {col}: {val_str}")

                else:
                    lines.append(f"{col}: {val_str}")

            lines.append("")  # 每条策略间空行
    else:
        lines.append("（无信号数据）")

    return title, "\n".join(lines)

def clip_for_telegram(text: str, limit: int = 3800) -> str:
    return text if len(text) <= limit else text[:limit] + "\n...（已截断）"

# =====================================================
# Public notify entry
# =====================================================
def post_export_notify(
    file_path: Optional[str],
    df: Optional[pd.DataFrame],
) -> None:
    try:
        title, body = build_unified_message(df, file_path)
        tg_text = clip_for_telegram(f"{title}\n\n{body}")

        # ---------- Email ----------
        if SYSTEM_CONFIG.get("ENABLE_EMAIL"):
            send_email(
                smtp_host=EMAIL_CONFIG.get("SMTP_HOST", ""),
                smtp_port=int(EMAIL_CONFIG.get("SMTP_PORT", 465)),
                use_ssl=bool(EMAIL_CONFIG.get("USE_SSL", True)),
                username=EMAIL_CONFIG.get("USERNAME", ""),
                password=EMAIL_CONFIG.get("PASSWORD", ""),
                sender=EMAIL_CONFIG.get("FROM", ""),
                to_list=[x for x in EMAIL_CONFIG.get("TO", []) if x],
                subject=title,
                body=body,
                attachment_path=file_path,
            )

        # ---------- Telegram ----------
        if SYSTEM_CONFIG.get("ENABLE_TELEGRAM"):
            send_telegram(
                bot_token=TELEGRAM_CONFIG.get("BOT_TOKEN", ""),
                chat_id=str(TELEGRAM_CONFIG.get("CHAT_ID", "")).strip(),
                text=tg_text,
                disable_web_page_preview=bool(
                    TELEGRAM_CONFIG.get("DISABLE_WEB_PAGE_PREVIEW", True)
                ),
            )

    except Exception as e:
        print(f"⚠️ Notify failed: {e}")

# =====================================================
# CSV 导出 + 通知接口
# =====================================================
def export_and_notify(df: Optional[pd.DataFrame]) -> Optional[str]:
    file_path: Optional[str] = None
    try:
        if SYSTEM_CONFIG.get("ENABLE_EXPORT", True):
            import time
            date_str = time.strftime('%Y%m%d')
            save_dir = os.path.join(PATH_CONFIG["OUTPUT_FOLDER_BASE"], date_str)
            os.makedirs(save_dir, exist_ok=True)
            strategy_name = STRATEGY_CONFIG.get("RUN_STRATEGY", "strategy")
            file_path = os.path.join(save_dir, f"{strategy_name}_{time.strftime('%H%M%S')}.csv")
            encoding = SYSTEM_CONFIG.get("EXPORT_ENCODING", "utf-8-sig")
            if isinstance(df, pd.DataFrame):
                df.to_csv(file_path, index=False, encoding=encoding)
            else:
                with open(file_path, "w", encoding=encoding) as f:
                    f.write("")
            print(f"\n🎉 导出成功！文件路径: {file_path}")
        else:
            print("ℹ️ 已关闭导出开关（ENABLE_EXPORT=False），仅发送通知...")
    except Exception as e:
        print(f"⚠️ 导出 CSV 失败：{e}")

    try:
        post_export_notify(file_path=file_path, df=df)
    except Exception as e:
        print(f"⚠️ 导出后通知失败: {e}")

    return file_path
