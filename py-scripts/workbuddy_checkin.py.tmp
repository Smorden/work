#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WorkBuddy 客户端「Buddy加油站」每日签到脚本

原理：
    WorkBuddy 桌面端把登录会话（含 accessToken / refreshToken）以明文 JSON
    持久化在本地 auth 文件中；签到/状态查询走的是后端 HTTP 接口。本脚本
    直接读取该会话文件、携带 Bearer Token 调用接口完成签到，等价于在客户端
    点击「立即领取」。

接口（逆向自客户端 app.asar）：
    状态查询  POST https://copilot.tencent.com/v2/billing/meter/checkin-activity-status
    执行签到  POST https://copilot.tencent.com/v2/billing/meter/daily-checkin
    刷新令牌  POST https://copilot.tencent.com/v2/auth/token/refresh

依赖：仅 Python 3 标准库，无需 pip 安装任何第三方包。

用法：
    python workbuddy_checkin.py            # 查状态，未签到则签到
    python workbuddy_checkin.py --status   # 仅查询签到状态，不签到
    python workbuddy_checkin.py --dry-run  # 演练：只打印将执行的动作
    python workbuddy_checkin.py --no-persist   # 刷新 token 后不回写会话文件
    python workbuddy_checkin.py --auth <path>   # 指定会话文件路径
"""

import argparse
import glob
import json
import os
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
API_BASE = "https://copilot.tencent.com"
CHECKIN_STATUS_PATH = "/v2/billing/meter/checkin-activity-status"
CHECKIN_PATH = "/v2/billing/meter/daily-checkin"
REFRESH_PATH = "/v2/auth/token/refresh"
TIMEOUT_SECONDS = 20

# 会话文件的相对路径（相对 AppData\Local）
AUTH_REL_PATH = os.path.join("CodeBuddyExtension", "Data", "Public", "auth",
                             "workbuddy-desktop.info")


def _candidate_bases():
    """按优先级生成可能的 AppData\\Local 基目录列表（去重、过滤无效值）。"""
    bases = []

    def add(p):
        if p and os.path.isabs(p) and p not in bases:
            bases.append(p)

    # 1) 用 ~ 推导（依赖 USERPROFILE/HOME，最稳定，不依赖 LOCALAPPDATA）
    home = os.path.expanduser("~")
    if home and home != "~":
        add(os.path.join(home, "AppData", "Local"))

    # 2) LOCALAPPDATA 环境变量
    add(os.environ.get("LOCALAPPDATA", ""))

    # 3) APPDATA 的父目录 + Local
    appdata = os.environ.get("APPDATA", "")
    if appdata:
        add(os.path.join(os.path.dirname(appdata), "Local"))

    return bases


def collect_auth_candidates():
    """收集所有可能的会话文件路径（含通配兜底），去重保序。"""
    cands = []
    for base in _candidate_bases():
        cands.append(os.path.join(base, AUTH_REL_PATH))
    # 通配兜底：扫描所有盘符的 C:\Users\*\AppData\Local\...
    for drive in ("C:", "D:"):
        pattern = os.path.join(drive, os.sep, "Users", "*", "AppData",
                               "Local", AUTH_REL_PATH)
        cands.extend(sorted(glob.glob(pattern)))
    out = []
    for p in cands:
        if p and p not in out:
            out.append(p)
    return out


def _http_post(path, token, uid, domain, body=None, extra_headers=None):
    """发送一个 POST 请求，返回 (status_code, parsed_json_or_raw_text)。"""
    url = API_BASE + path
    data = json.dumps(body if body is not None else {}).encode("utf-8")
    headers = {
        "Authorization": "Bearer " + token,
        "X-User-Id": uid,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "WorkBuddy-Checkin/1.0",
    }
    if domain:
        headers["X-Domain"] = domain
    if extra_headers:
        headers.update(extra_headers)

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, e.read().decode("utf-8", "replace")[:500]
    except urllib.error.URLError as e:
        return -1, {"error": "network", "reason": str(e.reason)}


def load_session(auth_path):
    """读取并解析本地会话文件。"""
    with open(auth_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_session(auth_path, session):
    """原子写回会话文件（临时文件 + os.replace）。"""
    tmp = auth_path + ".tmp-checkin"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)
    os.replace(tmp, auth_path)


def find_auth_path(explicit=None):
    if explicit:
        if os.path.exists(explicit):
            return explicit
        raise SystemExit(f"[错误] 指定的会话文件不存在: {explicit}")
    for p in collect_auth_candidates():
        if os.path.exists(p):
            return p
    print("[错误] 未找到 WorkBuddy 会话文件。已按以下路径探测（均不存在）：")
    for p in collect_auth_candidates():
        print("  - " + p)
    raise SystemExit(
        "请确认已登录客户端，或用 --auth <path> 手动指定路径。\n"
        "会话文件通常位于: %LOCALAPPDATA%\\CodeBuddyExtension\\Data\\Public\\auth\\workbuddy-desktop.info"
    )


def token_expired(session):
    """依据 expiresAt 判断 accessToken 是否已过期（留 5 分钟余量）。"""
    expires_at = session.get("auth", {}).get("expiresAt")
    if not expires_at:
        return False
    return time.time() * 1000 > expires_at - 5 * 60 * 1000


def refresh_token(session, auth_path, persist):
    """用 refreshToken 换新令牌；成功则写回（可选）。返回新的 session。"""
    auth = session.get("auth", {})
    refresh_token = auth.get("refreshToken")
    if not refresh_token:
        raise SystemExit("[错误] accessToken 已过期且缺少 refreshToken，请重新登录客户端。")

    uid = session.get("account", {}).get("uid", "")
    domain = auth.get("domain", "")
    print("accessToken 已过期，尝试刷新令牌 …")
    status, body = _http_post(
        REFRESH_PATH,
        auth.get("accessToken", ""),
        uid,
        domain,
        body={},
        extra_headers={
            "X-Refresh-Token": refresh_token,
            "X-Auth-Refresh-Source": "plugin",
        },
    )
    if status != 200 or not isinstance(body, dict) or body.get("code") != 0 \
            or not body.get("data"):
        print(f"[错误] 令牌刷新失败: HTTP {status} {body}")
        raise SystemExit(2)

    new_auth = body["data"]
    new_auth.setdefault("refreshToken", new_auth.get("refreshToken", refresh_token))
    new_auth.setdefault("accessToken", new_auth.get("accessToken", auth.get("accessToken")))
    new_auth["lastRefreshTime"] = int(time.time() * 1000)
    session["auth"] = new_auth

    if persist:
        save_session(auth_path, session)
        print("新令牌已写回会话文件。")
    return session


def do_checkin(args):
    auth_path = find_auth_path(args.auth)
    session = load_session(auth_path)

    account = session.get("account", {})
    auth = session.get("auth", {})
    token = auth.get("accessToken", "")
    uid = account.get("uid", "")
    domain = auth.get("domain", "")
    nickname = account.get("nickname", "")

    if not token or not uid:
        raise SystemExit("[错误] 会话文件缺少 accessToken/uid，请确认已登录客户端。")

    # 令牌过期先刷新
    if token_expired(session):
        session = refresh_token(session, auth_path, not args.no_persist)
        token = session["auth"].get("accessToken", "")
        domain = session["auth"].get("domain", domain)

    # 1) 查询签到状态
    status, body = _http_post(CHECKIN_STATUS_PATH, token, uid, domain, body={})
    if status == 401:
        # 令牌在服务端被判失效，尝试刷新后重试一次
        session = refresh_token(session, auth_path, not args.no_persist)
        token = session["auth"].get("accessToken", "")
        domain = session["auth"].get("domain", domain)
        status, body = _http_post(CHECKIN_STATUS_PATH, token, uid, domain, body={})

    if status != 200 or not isinstance(body, dict) or body.get("code") != 0:
        print(f"[错误] 状态查询失败: HTTP {status} {body}")
        raise SystemExit(2)

    data = body.get("data", {})
    who = f"用户「{nickname or uid}」"
    theme = data.get("theme_name", "Buddy加油站")
    print(f"== {theme} · {who} ==")
    print(f"   今日积分: {data.get('today_credit', '-')} 分/天")
    print(f"   当前连续: {data.get('streak_days', '-')} 天")
    print(f"   累计积分: {data.get('total_credits', '-')} 分")
    print(f"   活动名称: {data.get('activity_name', '-')} (第 {data.get('season', '-')} 季)")
    print(f"   活动区间: {data.get('start_time', '-')} ~ {data.get('end_time', '-')}")

    checked = bool(data.get("today_checked_in"))
    print(f"   今日状态: {'已签到' if checked else '未签到'}")

    if args.status:
        return 0

    if checked:
        print("结果：今日已签到过，无需重复领取。")
        return 0

    if args.dry_run:
        print(f"[dry-run] 将调用 {CHECKIN_PATH} 领取今日 {data.get('daily_credit', '-')} 积分（未实际执行）。")
        return 0

    # 2) 执行签到
    st2, body2 = _http_post(CHECKIN_PATH, token, uid, domain, body={})
    if st2 != 200 or not isinstance(body2, dict) or body2.get("code") != 0:
        print(f"[错误] 签到失败: HTTP {st2} {body2}")
        raise SystemExit(2)

    d2 = body2.get("data", {})
    print(f"结果：签到成功 ✅  +{d2.get('credit', '-')} 分，连续签到 {d2.get('streak_days', '-')} 天。")
    return 0


def main():
    parser = argparse.ArgumentParser(description="WorkBuddy Buddy加油站 每日签到")
    parser.add_argument("--status", action="store_true", help="仅查询状态，不签到")
    parser.add_argument("--dry-run", action="store_true", help="演练模式，不实际签到")
    parser.add_argument("--no-persist", action="store_true", help="刷新 token 后不回写会话文件")
    parser.add_argument("--auth", metavar="PATH", help="指定会话文件路径")
    args = parser.parse_args()
    sys.exit(do_checkin(args))


if __name__ == "__main__":
    main()
