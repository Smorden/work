#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DolphinScheduler default 工作流状态检查

连接 MySQL 查询名为 'default' 的工作流定时调度状态（release_state）：
  release_state = 1  -> default工作流状态正常
  release_state = 0  -> default工作流下线，请恢复上线
结果可选择推送到钉钉群「finebi同步任务报错」。

依赖安装:
  pip install pymysql requests

用法:
  python check_default_status.py             # 查询并按需推送钉钉（状态正常时不推送）
  python check_default_status.py --no-send   # 只查询打印，不推送
  python check_default_status.py --force     # 强制推送，状态正常也发送（调试用）
"""

import argparse
import base64
import hashlib
import hmac
import os
import sys
import time
import urllib.parse

import pymysql

# ----------------------------- 配置区 -----------------------------

DB_HOST = os.getenv("DS_DB_HOST", "172.16.51.8")
DB_PORT = int(os.getenv("DS_DB_PORT", "53366"))
DB_USER = os.getenv("DS_DB_USER", "developer")
DB_PASSWORD = os.getenv("DS_DB_PASSWORD", "developer123")
DB_NAME = os.getenv("DS_DB_NAME", "dolphinscheduler")
DINGTALK_TOKEN = os.getenv("DINGTALK_WEBHOOK_TOKEN", "d8b3b9fd753638590559ebcfa89ab11f8375b2d2ca1b33f03830ecb94cdc3b51")
DINGTALK_SECRET = os.getenv("DINGTALK_WEBHOOK_SECRET", "SEC88ac7b12e6b90c7032e8c4f64f6cd234a3a946a650f42f1da798f9b333961935")
WORKFLOW_NAME = "default"

# 钉钉群「finebi同步任务报错」的 openConversationId（供 dws 方式发送）
OPEN_CONVERSATION_ID = "cidjj2PDYlnI7WIWPieOs67hA=="

SQL = """
select s.release_state
from t_ds_schedules as s
join t_ds_process_definition as d
  on d.code = s.process_definition_code and d.name = %s
"""

# ------------------------------------------------------------------


def query_release_states() -> list:
    """连接数据库并返回所有匹配记录的 release_state 列表。"""
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        connect_timeout=10,
        read_timeout=15,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(SQL, (WORKFLOW_NAME,))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def is_all_online(states: list) -> bool:
    """所有调度记录 release_state 均为 1（在线）时返回 True。"""
    return bool(states) and all(s == 1 for s in states)


def build_message(states: list) -> str:
    """按查询结果生成告警文案。"""
    if not states:
        return f"未查到工作流 '{WORKFLOW_NAME}' 的调度记录，请确认工作流名称及调度配置"
    if any(s == 0 for s in states):
        return f"{WORKFLOW_NAME}工作流下线，请恢复上线"
    if all(s == 1 for s in states):
        return f"{WORKFLOW_NAME}工作流状态正常"
    return f"{WORKFLOW_NAME}工作流状态异常：release_state={states}，请人工核查"


def build_signed_webhook_url() -> str:
    """构造带加签参数的钉钉 webhook URL。

    加签算法（钉钉官方）：
      stringToSign = timestamp + "\\n" + secret
      sign = urlEncode( Base64( HMAC-SHA256(stringToSign, key=secret) ) )
    """
    timestamp = str(round(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{DINGTALK_SECRET}"
    digest = hmac.new(
        DINGTALK_SECRET.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(digest))
    return (
        f"https://oapi.dingtalk.com/robot/send?access_token={DINGTALK_TOKEN}"
        f"&timestamp={timestamp}&sign={sign}"
    )


def send_dingtalk(text: str) -> str:
    """推送钉钉群（webhook 加签方式）。"""
    import requests

    if DINGTALK_TOKEN:
        r = requests.post(
            build_signed_webhook_url(),
            json={
                "msgtype": "text",
                "text": {"content": text},
                "at": {"isAtAll": True},
            },
            timeout=30,
        )
        result = r.json()
        if result.get("errcode") != 0:
            raise RuntimeError(f"webhook 推送失败: {result}")
        return "webhook"
    print("[警告] 未配置 DINGTALK_WEBHOOK_TOKEN，消息未推送，仅打印如下:")
    print(text)
    return "console"


def main():
    parser = argparse.ArgumentParser(description="DolphinScheduler default 工作流状态检查")
    parser.add_argument("--no-send", action="store_true", help="只查询并打印，不推送钉钉")
    parser.add_argument("--force", action="store_true", help="强制推送，即使工作流在线（release_state=1）也发送")
    args = parser.parse_args()

    print(f"[1/3] 连接 {DB_HOST}:{DB_PORT}/{DB_NAME} ...")
    try:
        states = query_release_states()
    except Exception as e:
        msg = f"default工作流状态检查失败：无法连接数据库或查询出错（{e}）"
        print(f"      {msg}")
        if not args.no_send:
            channel = send_dingtalk(msg)
            print(f"[2/3] 异常已推送钉钉（通道: {channel}）")
        return 1

    msg = build_message(states)
    print(f"      查询到 {len(states)} 条调度记录，release_state={states}")
    print(f"[2/3] 判定结果: {msg}")

    if args.no_send:
        print("[3/3] --no-send 已指定，跳过推送")
        return 0

    if is_all_online(states) and not args.force:
        print("[3/3] 工作流在线（release_state=1），无需推送告警，跳过")
        return 0

    channel = send_dingtalk(msg)
    print(f"[3/3] 推送完成（通道: {channel}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
