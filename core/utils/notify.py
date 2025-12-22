# -*- coding: utf-8 -*-
"""
通知工具集：邮件与 Telegram 推送

- send_email: 通过 SMTP 发送文本 + 可选附件（CSV 报告）。
- send_telegram: 通过 Telegram Bot API 发送文本消息；如可用则尝试发送文档。
- post_export_notify: 对外统一接口；封装主题/正文构建与渠道分发逻辑。

使用：在 engine/market_scanner.py 导出 CSV 后只需调用 `post_export_notify(file_path, df)`。
"""

from __future__ import annotations

import os
import smtplib
import ssl
from typing import List, Optional
import pandas as pd
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders

import json
import urllib.request
import urllib.parse

from conf.config import SYSTEM_CONFIG, EMAIL_CONFIG, TELEGRAM_CONFIG, PATH_CONFIG, STRATEGY_CONFIG


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
    """
    发送邮件（带可选附件）。

    返回 True 表示发送成功；False 表示失败（已吞掉异常并打印）。
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ', '.join([x for x in to_list if x])
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            filename = os.path.basename(attachment_path)
            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
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
                    if username:
                        server.login(username, password)
                except Exception:
                    # 部分服务器不需要/不支持 starttls
                    if username:
                        server.login(username, password)
                server.sendmail(sender, to_list, msg.as_string())

        print("📧 邮件发送成功。")
        return True
    except Exception as e:
        print(f"⚠️ 邮件发送失败: {e}")
        return False


def _http_post_json(url: str, data: dict) -> dict:
    req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers={
        'Content-Type': 'application/json'
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _http_post_form(url: str, data: dict) -> dict:
    encoded = urllib.parse.urlencode(data).encode('utf-8')
    req = urllib.request.Request(url, data=encoded)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode('utf-8'))


def send_telegram(
    bot_token: str,
    chat_id: str,
    text: str,
    disable_web_page_preview: bool = True,
) -> bool:
    """
    发送 Telegram 文本消息（使用官方 Bot API）。
    不依赖第三方 requests 库，使用 urllib 实现。
    """
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': disable_web_page_preview,
        }
        resp = _http_post_form(url, payload)
        if not resp.get('ok'):
            raise Exception(resp)
        print("🤖 Telegram 文本消息已发送。")
        return True
    except Exception as e:
        print(f"⚠️ Telegram 发送失败: {e}")
        return False


def _build_summary_message(file_path: Optional[str], df: Optional[pd.DataFrame]) -> (str, str):
    """
    构建通知的标题与正文（含Top5预览）。
    返回 (subject, body)
    """
    import time, os

    hit_cnt = int(len(df)) if isinstance(df, pd.DataFrame) else 0
    subject = f"{EMAIL_CONFIG.get('SUBJECT_PREFIX', '[StockScan]')} 扫描完成：{hit_cnt} 条信号"

    lines = [
        f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"命中数量：{hit_cnt}",
    ]

    if file_path:
        try:
            lines.append(f"文件：{os.path.basename(file_path)}")
        except Exception:
            lines.append("文件：<未落盘>")
    else:
        lines.append("文件：<未落盘>")

    # 追加 Top5 预览
    if isinstance(df, pd.DataFrame) and not df.empty:
        try:
            preview_cols = [c for c in ['代码', '名称', 'score', '涨幅(%)', '当前价'] if c in df.columns]
            if preview_cols:
                head_df = df[preview_cols].head(5)
                lines.append("\nTop5 预览：")
                for _, row in head_df.iterrows():
                    preview = ' | '.join([f"{col}:{row[col]}" for col in preview_cols])
                    lines.append(preview)
        except Exception:
            pass

    body = "\n".join(lines)
    return subject, body


def post_export_notify(file_path: Optional[str], df: Optional[pd.DataFrame]) -> None:
    """
    对外统一通知接口：根据配置发送邮件与 Telegram 消息。
    - file_path: 导出 CSV 文件的绝对路径。
    - df: 导出前的 DataFrame（用于构造摘要与预览），可为 None。
    """
    try:
        subject, body = _build_summary_message(file_path, df)

        # 邮件
        if SYSTEM_CONFIG.get("ENABLE_EMAIL"):
            try:
                send_email(
                    smtp_host=EMAIL_CONFIG.get('SMTP_HOST', ''),
                    smtp_port=int(EMAIL_CONFIG.get('SMTP_PORT', 465)),
                    use_ssl=bool(EMAIL_CONFIG.get('USE_SSL', True)),
                    username=EMAIL_CONFIG.get('USERNAME', ''),
                    password=EMAIL_CONFIG.get('PASSWORD', ''),
                    sender=EMAIL_CONFIG.get('FROM', ''),
                    to_list=[x for x in EMAIL_CONFIG.get('TO', []) if x],
                    subject=subject,
                    body=body,
                    attachment_path=file_path,
                )
            except Exception as e:
                print(f"⚠️ 邮件发送异常: {e}")

        # Telegram
        if SYSTEM_CONFIG.get("ENABLE_TELEGRAM"):
            try:
                import time, os
                hit_cnt = int(len(df)) if isinstance(df, pd.DataFrame) else 0
                tg_text = (
                    f"📈 扫描完成：{hit_cnt} 条信号\n"
                    f"文件：{os.path.basename(file_path) if file_path else '<未落盘>'}\n"
                    f"时间：{time.strftime('%H:%M:%S')}"
                )
                send_telegram(
                    bot_token=TELEGRAM_CONFIG.get('BOT_TOKEN', ''),
                    chat_id=TELEGRAM_CONFIG.get('CHAT_ID', ''),
                    text=tg_text,
                    disable_web_page_preview=bool(TELEGRAM_CONFIG.get('DISABLE_WEB_PAGE_PREVIEW', True)),
                )
            except Exception as e:
                print(f"⚠️ Telegram 推送异常: {e}")
    except Exception as e:
        print(f"⚠️ 通知流程失败: {e}")


def export_and_notify(df: Optional[pd.DataFrame]) -> Optional[str]:
    """
    对外统一导出+通知接口：
    - 根据 SYSTEM_CONFIG['ENABLE_EXPORT'] 决定是否写入 CSV；
    - 始终尝试发送通知（若开启相关开关）。

    Args:
        df: 待导出的 DataFrame。
    Returns:
        file_path: 实际写入的 CSV 路径；若未写入则为 None。
    """
    file_path: Optional[str] = None
    try:
        # 是否需要实际导出 CSV
        if SYSTEM_CONFIG.get("ENABLE_EXPORT", True):
            import time
            date_str = time.strftime('%Y%m%d')
            save_dir = os.path.join(PATH_CONFIG["OUTPUT_FOLDER_BASE"], date_str)
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            # 策略名称前缀
            strategy_name = STRATEGY_CONFIG.get("RUN_STRATEGY")
            file_path = os.path.join(save_dir, f"{strategy_name}_{time.strftime('%H%M%S')}.csv")
            encoding = SYSTEM_CONFIG.get("EXPORT_ENCODING", "utf-8-sig")

            if isinstance(df, pd.DataFrame):
                df.to_csv(file_path, index=False, encoding=encoding)
            else:
                # 兜底：创建空文件占位，便于定位
                with open(file_path, 'w', encoding=encoding) as f:
                    f.write("")

            print(f"\n🎉 导出成功！文件路径: {file_path}")
        else:
            print("ℹ️ [系统] 已关闭导出开关（ENABLE_EXPORT=False），将跳过 CSV 落盘，仅发送通知...")
    except Exception as e:
        print(f"⚠️ 导出 CSV 失败：{e}")
        # 出错也继续走通知流程，方便运维感知

    # 统一通知
    try:
        post_export_notify(file_path=file_path, df=df)
    except Exception as e:
        print(f"⚠️ 导出后通知失败: {e}")

    return file_path
