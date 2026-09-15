#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WorkBuddy 成长计划 - 猫猫旅行自动脚本

根据 button.gs-buddy-travel 文本判断 3 种场景：
  1) 派猫猫旅行    → 点派出 → 弹窗点「确定派出」→ 关闭派出弹窗
  2) 旅行倒计时    → 无需操作
  3) 领取礼物      → 点领取 → 弹窗点「领积分」→ 关闭领积分弹窗
                     → 等按钮文本变回「派猫猫旅行」→ 走场景1

登录态持久化（本脚本的关键设计）：
  workbuddy.cn 的登录态完全依赖 SESSION cookie（AUTH_SESSION_ID / KC_RESTART /
  9c412d6095037d16 / _TDID_CK 等，expires=0）。Chrome 的设计是浏览器关闭即丢弃
  session cookie，所以独立实例一关就要重新登录。
  对策：每次任务结束时，把目标域名的 session cookie 改写为 N 天有效的持久
  cookie 并写回。此后即使浏览器被关闭，下次启动同一 user-data-dir 依然是登录态。

用法：
    python workbuddy_growth.py                # 连接现有实例，连不上则启动独立实例
    python workbuddy_growth.py --show         # 启动时显示窗口（首次登录用，会等你按回车）
    python workbuddy_growth.py --no-spawn     # 只连接，不自动启动实例
    python workbuddy_growth.py --close-browser# 任务结束后关闭本脚本启动的实例（默认保留常驻）
    python workbuddy_growth.py --port 9224    # 指定调试端口

注意：Chrome 136+ 对默认 user-data-dir 会忽略 --remote-debugging-port（Google 安全
      设计），因此独立实例必须使用非默认的 --user-data-dir（见 AUTO_USER_DATA_DIR）。
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# 关键：绕过本机 HTTP_PROXY 对回环地址的劫持
# ---------------------------------------------------------------------------
# 本机环境设置了 HTTP_PROXY/HTTPS_PROXY（沙箱代理）。Playwright 的 CDP 连接和
# urllib 都会尊重这些变量，把访问 127.0.0.1 调试端口的请求送去代理，表现为
# "502 Bad Gateway / upstream connect failed"。CDP 只走回环，这里在进程内清掉。
for _v in ("http_proxy", "https_proxy", "all_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
    os.environ.pop(_v, None)
os.environ["no_proxy"] = os.environ["NO_PROXY"] = "127.0.0.1,localhost,::1"

from playwright.sync_api import sync_playwright

# Windows 下控制台/gocron/任务计划普遍按 GBK 解码子进程输出。
# 直接输出 GBK 保持一致；遇到打不出的字符（如 emoji）降级为 ?，但脚本不会崩。
_OUT_ENCODING = "gbk" if sys.platform == "win32" else "utf-8"
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding=_OUT_ENCODING, errors="replace")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
# 独立于 xianyupc（ChromeAutoPC）、xianyudanji（ChromeAuto）的自动化 Chrome
AUTO_USER_DATA_DIR = r"C:\Users\Mickey.Deng\AppData\Local\ChromeAutoGC"
DEFAULT_PORT = 9224
DEBUG_PORTS = [9224, 9222, 9223, 9333, 9229]

GROWTH_URL = "https://www.workbuddy.cn/profile/growth-center?utm_medium=cpc&utm_id=gwzcw.15291244.15291244.15291244"

# 需要把 session cookie 固化为持久 cookie 的域名
PERSIST_COOKIE_DOMAINS = ("workbuddy.cn",)
COOKIE_KEEP_DAYS = 180
# cookie 独立备份文件：Chrome 对 cookie 是延迟写盘的，进程被强杀会丢；
# 这份 JSON 在脚本结束时立即写盘，下次启动时注入，作为 profile 之外的第二重保险。
COOKIE_STATE_FILE = Path(__file__).with_name("wbgc_cookies.json")

# 状态/动作超时
WAIT_STATE_TIMEOUT = 20  # 等待按钮文本切换的最大秒数
CLICK_TIMEOUT = 10000    # 单次点击默认 10s


# ---------------------------------------------------------------------------
# Chrome 连接
# ---------------------------------------------------------------------------
# 显式禁用代理的 opener：确保探测 127.0.0.1 一定直连
_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def probe_debug_port(port):
    """直连探测调试端口（显式禁用代理）。"""
    try:
        _NO_PROXY_OPENER.open(
            f"http://127.0.0.1:{port}/json/version", timeout=2
        )
        return True
    except Exception:
        return False


def connect_via_cdp(p, port=0):
    """连接现有 Chrome 实例。返回 (browser, port)，失败返回 (None, None)。"""
    ports = [port] if port else DEBUG_PORTS
    for pt in ports:
        if not pt:
            continue
        if not probe_debug_port(pt):
            continue
        try:
            browser = p.chromium.connect_over_cdp(
                f"http://127.0.0.1:{pt}", timeout=10000
            )
            print(f"已连接现有 Chrome（127.0.0.1:{pt}）")
            return browser, pt
        except Exception as e:
            print(f"  [提示] 连接 127.0.0.1:{pt} 失败: {e}")
            continue
    return None, None


def kill_auto_chrome():
    """只杀本脚本专用目录（ChromeAutoGC）的 Chrome 进程。

    注意：这是强杀，会丢弃尚未落盘的 session cookie。仅在启动新实例前用于
    清理僵死进程；正常流程结束时绝不用它。
    """
    ps_cmd = (
        "Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\" | "
        "Where-Object {$_.CommandLine -like '*ChromeAutoGC*'} | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, timeout=15,
        )
        time.sleep(2)
        print("  已清理僵死的自动化 Chrome 实例（ChromeAutoGC）")
    except Exception as e:
        print(f"  [提示] 清理自动化 Chrome 失败（忽略）: {e}")


def spawn_auto_chrome(port, show_window=False):
    """启动独立 Chrome 实例。

    --restore-last-session 让 Chrome 在重启时恢复上次会话（配合 session cookie
    固化双保险）。
    """
    print(f"启动独立 Chrome（{('有窗口' if show_window else '无头')}，目录 {AUTO_USER_DATA_DIR}）…")
    args = [
        CHROME_PATH,
        f"--user-data-dir={AUTO_USER_DATA_DIR}",
        f"--remote-debugging-port={port}",
        "--window-size=1280,900",
    ]
    if not show_window:
        args.insert(1, "--headless=new")
    # 让 Chrome 脱离当前进程组，避免脚本/调度器退出时被连带结束（保证常驻）
    creationflags = 0
    if sys.platform == "win32":
        creationflags = 0x00000008 | 0x00000200  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    subprocess.Popen(args + ["about:blank"], creationflags=creationflags, close_fds=True)
    for _ in range(20):
        time.sleep(1)
        if probe_debug_port(port):
            print(f"  调试端口 127.0.0.1:{port} 已就绪")
            return True
    print("  [错误] Chrome 启动后调试端口未就绪")
    return False


# ---------------------------------------------------------------------------
# 登录页状态修复：清除 fromLoginType 记忆
# ---------------------------------------------------------------------------
def fix_login_type(page):
    """登录页把「上次使用的登录方式」存进 localStorage（fromLoginType=phone）。

    带着这个 key 打开登录页会直接进入手机验证模式，微信扫码二维码
    根本不初始化（实测：wechat-url 请求不会发出）。清掉它并刷新，
    登录页回到默认的微信扫码 tab。

    返回 True 表示执行了清除+刷新。
    """
    try:
        if "/login" not in page.url:
            return False
        val = page.evaluate("() => localStorage.getItem('fromLoginType')")
        if val and val != "wechat":
            page.evaluate("() => localStorage.removeItem('fromLoginType')")
            print(f"  [修复] 检测到 fromLoginType={val}（上次用手机验证登录，")
            print(f"         会导致微信二维码不显示）——已清除，正在刷新页面 …")
            try:
                page.reload(wait_until="load", timeout=60000)
            except Exception as e:
                print(f"  [提示] 刷新异常（继续）: {e}")
            return True
    except Exception as e:
        print(f"  [提示] 清除 fromLoginType 失败: {e}")
    return False


# Keycloak 微信登录 URL 获取端点（固定；返回 JSON {"url": 微信 qrconnect 地址}）
WECHAT_URL_ENDPOINT = (
    "https://www.workbuddy.cn/auth/realms/copilot/console/auth/wechat-url"
    "?client_id=login&response_type=code&self_redirect=true"
    "&kc_idp_hint=weixinwebchat&prompt=login"
    "&redirect_uri=https%3A%2F%2Fwww.workbuddy.cn%2Flogin%2F%3Fplatform%3Dusercenter"
)


def prepare_wechat_qrcode(page, total_timeout=60, reload_after=30):
    """自动把登录页推进到「微信二维码可见」状态。

    workbuddy.cn 登录页的微信二维码链路（同意 → Keycloak iframe →
    wechat-url → open.weixin.qq.com qrconnect）异步且时序敏感，依赖登录页
    JS 竞态，不稳定。策略：

    1. 自动点击「同意」协议按钮（button.agree-btn）
    2. 轮询等待微信二维码 frame 出现（快速路径，15s）
    3. 未出现则走稳定路径：直接 GET Keycloak 的 wechat-url 端点拿
       微信 qrconnect 地址，顶层导航过去（已实测稳定渲染 210x210 二维码）
    """
    def click_agree():
        try:
            page.locator("button.agree-btn").click(timeout=6000)
            print("  已点击「同意」协议按钮")
            return True
        except Exception:
            return False

    def qr_frame_present():
        return any("open.weixin.qq.com" in f.url for f in page.frames)

    # 快速路径：登录页内等 iframe 二维码（最多 15s）
    click_agree()
    start = time.time()
    while time.time() - start < 15:
        time.sleep(3)
        if qr_frame_present():
            print(f"  微信二维码已出现（{time.time() - start:.0f}s）[OK]")
            return True

    # 稳定路径：直取 wechat-url 端点，顶层导航到微信二维码页
    print("  登录页内未出现二维码（页面竞态），改用直达路径 …")
    try:
        page.goto(WECHAT_URL_ENDPOINT, timeout=30000, wait_until="domcontentloaded")
        time.sleep(2)
        import json as _json
        body = page.evaluate("() => document.body.innerText")
        data = _json.loads(body)
        qr_url = data.get("url", "")
        if not qr_url.startswith("https://open.weixin.qq.com"):
            print(f"  [警告] wechat-url 返回异常: {body[:120]}")
            return False
        page.goto(qr_url, timeout=30000, wait_until="domcontentloaded")
        # 轮询确认二维码 img 渲染
        for _ in range(10):
            time.sleep(2)
            try:
                ok = page.evaluate(
                    "() => Array.from(document.querySelectorAll('img'))"
                    ".some(el => { const r = el.getBoundingClientRect();"
                    " return r.width > 80 && el.complete && el.naturalWidth > 0; })"
                )
                if ok:
                    print("  微信二维码已打开（独立页），请扫码登录 [OK]")
                    return True
            except Exception:
                pass
        print("  [提示] 已跳转微信二维码页，请在窗口中确认二维码是否显示")
        return False
    except Exception as e:
        print(f"  [提示] 直达路径失败: {e}；请在窗口中手动切换到微信登录 tab")
        return False


# ---------------------------------------------------------------------------
# 登录态持久化：把 session cookie 固化为长期持久 cookie
# ---------------------------------------------------------------------------
def persist_session_cookies(context, domains=PERSIST_COOKIE_DOMAINS,
                            days=COOKIE_KEEP_DAYS):
    """把目标域名的 session cookie（expires<=0）改写为 days 天有效的持久 cookie。

    这是解决「独立实例关闭后登录态丢失」的核心手段：Chrome 关闭时会丢弃
    session cookie，但不会丢弃带 expires 的持久 cookie。
    """
    try:
        cookies = context.cookies()
    except Exception as e:
        print(f"  [提示] 读取 cookie 失败，跳过固化: {e}")
        return 0

    now = time.time()
    to_add = []
    for c in cookies:
        dom = c.get("domain") or ""
        if not any(d in dom for d in domains):
            continue
        if c.get("expires", -1) not in (-1, 0, None):
            continue  # 已是持久 cookie
        item = {
            "name": c["name"],
            "value": c["value"],
            "domain": c["domain"],
            "path": c.get("path") or "/",
            "expires": now + days * 86400,
            "httpOnly": bool(c.get("httpOnly")),
            "secure": bool(c.get("secure")),
        }
        ss = c.get("sameSite")
        if ss in ("Strict", "Lax", "None"):
            item["sameSite"] = ss
        to_add.append(item)

    if not to_add:
        print("  [提示] 未发现需要固化的 session cookie（可能尚未登录）")
        return 0

    try:
        context.add_cookies(to_add)
    except Exception as e:
        print(f"  [错误] 固化 cookie 失败: {e}")
        return 0

    print(f"  已固化 {len(to_add)} 个 session cookie 为 {days} 天持久 cookie：")
    for item in to_add:
        print(f"    {item['domain']:24s} {item['name']}")
    return len(to_add)


def export_cookie_state(context, domains=PERSIST_COOKIE_DOMAINS,
                        days=COOKIE_KEEP_DAYS):
    """把目标域 cookie 导出为 JSON（立即写盘），session cookie 的 expires 拉长。

    这是「独立实例被关闭」场景的第二重保险：不依赖 Chrome 的延迟落盘。
    """
    try:
        state = context.storage_state()
    except Exception as e:
        print(f"  [提示] 导出 cookie 状态失败: {e}")
        return 0

    now = time.time()
    kept = []
    for c in state.get("cookies", []):
        dom = c.get("domain") or ""
        if not any(d in dom for d in domains):
            continue
        if c.get("expires", -1) in (-1, 0, None):
            c["expires"] = now + days * 86400
        kept.append(c)

    if not kept:
        print("  [提示] 无可导出的 cookie（可能尚未登录）")
        return 0

    try:
        COOKIE_STATE_FILE.write_text(
            json.dumps({"cookies": kept, "origins": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"  [错误] 写入 cookie 备份失败: {e}")
        return 0

    print(f"  已导出 {len(kept)} 个 cookie 到 {COOKIE_STATE_FILE.name}（独立备份，立即落盘）")
    return len(kept)


def import_cookie_state(context):
    """从 JSON 备份注入 cookie（profile 丢登录态时兜底）。"""
    if not COOKIE_STATE_FILE.exists():
        return 0
    try:
        data = json.loads(COOKIE_STATE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  [提示] 读取 cookie 备份失败: {e}")
        return 0

    now = time.time()
    valid = []
    for c in data.get("cookies", []):
        exp = c.get("expires", -1)
        if exp not in (-1, 0, None) and exp <= now:
            continue  # 已过期
        item = {k: v for k, v in c.items()
                if k in ("name", "value", "domain", "path", "expires",
                         "httpOnly", "secure", "sameSite")}
        if item.get("sameSite") not in ("Strict", "Lax", "None"):
            item.pop("sameSite", None)
        valid.append(item)

    if not valid:
        print("  [提示] cookie 备份已全部过期")
        return 0

    try:
        context.add_cookies(valid)
    except Exception as e:
        print(f"  [提示] 注入 cookie 备份失败: {e}")
        return 0

    print(f"  已从备份注入 {len(valid)} 个 cookie")
    return len(valid)


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------
def dismiss_swal2(page):
    """关掉所有可见的 SweetAlert2 弹窗（避免拦截后续点击）。

    注意：Escape 兜底只在真正检测到 swal2 容器时才执行。workbuddy 的猫猫旅行
    弹窗是 gs-travel-* 系列（非 swal2），若无条件按 Escape 会把刚弹出的目标
    弹窗误关掉，导致后续按钮读不到（count=0）。
    """
    had_swal2 = False
    try:
        for _ in range(3):
            closed = page.evaluate(
                "() => {"
                "  const cs = document.querySelectorAll("
                "    '.swal2-container.swal2-backdrop-show, "
                "     .swal2-container:not(.swal2-hide)');"
                "  if (!cs.length) return 0;"
                "  for (const c of cs) {"
                "    const btn = c.querySelector('.swal2-confirm, .swal2-close, .swal2-cancel');"
                "    if (btn) { btn.click(); continue; }"
                "    const r = c.getBoundingClientRect();"
                "    c.dispatchEvent(new MouseEvent('click', {bubbles:true, clientX:r.left+5, clientY:r.top+5}));"
                "  }"
                "  return cs.length;"
                "}"
            )
            if closed == 0:
                break
            had_swal2 = True
            time.sleep(0.5)
    except Exception:
        pass
    if had_swal2:
        try:
            page.keyboard.press("Escape")
            time.sleep(0.3)
        except Exception:
            pass


def click_by_class(page, selector, label, timeout=CLICK_TIMEOUT, dismiss_first=False):
    """按 class 点击按钮。

    dismiss_first=True 时先清掉可能残留的 swal2 弹窗——仅用于点击「主按钮」
    之前。点击弹窗内部按钮（确定派出/领取积分/关闭）时绝不能用 dismiss，
    否则 Escape 会误关目标弹窗，导致按钮读不到。
    """
    loc = page.locator(selector).first
    # locator 是惰性查询：wait_for 会一直等到目标元素真正出现并可见，
    # 即使该元素是点击后才动态渲染出来的「新弹窗」也能等到。
    loc.wait_for(state="visible", timeout=timeout)
    if dismiss_first:
        dismiss_swal2(page)
    try:
        loc.click(timeout=timeout, force=True)
    except Exception:
        # 强制点击失败时降级到 JS 派发（不 dismiss，避免误关弹窗）
        page.evaluate(
            f"() => {{ const e = document.querySelector({selector!r}); if (e) e.click(); }}"
        )
    print(f"  已点击: {label} ({selector})")


# ---------------------------------------------------------------------------
# 场景判定与执行
# ---------------------------------------------------------------------------
def get_travel_state(page):
    """读 button.gs-buddy-travel 文本 + disabled 状态判断场景。

    返回值：
        "send"      派猫猫旅行（可派，按钮 enabled）
        "rested"    猫猫累了（按钮 disabled，今天已派过，等价倒计时）
        "countdown" 旅行倒计时 ...
        "claim"     领取礼物
        None        找不到按钮（页面未加载/未登录）
        "unknown:..."  未知文本（带原始内容供诊断）
    """
    try:
        loc = page.locator('button.gs-buddy-travel')
        if loc.count() == 0:
            return None
        btn = loc.first
        text = (btn.inner_text() or "").strip()
    except Exception:
        return None
    if "旅行倒计时" in text:
        return "countdown"
    if "领取礼物" in text:
        return "claim"
    if "派猫猫旅行" in text:
        # 猫猫累了：按钮 disabled（hover 显示「累啦，明天再来吧」tooltip）
        try:
            if btn.get_attribute("disabled") is not None:
                return "rested"
        except Exception:
            pass
        return "send"
    return f"unknown:{text}"


def wait_state(page, state, timeout=WAIT_STATE_TIMEOUT):
    """轮询等到 button.gs-buddy-travel 文本变为指定状态。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if get_travel_state(page) == state:
            return True
        time.sleep(1)
    return False


def do_send(page):
    """场景1: 派猫猫旅行。"""
    print("== 场景1: 派猫猫旅行 ==")
    click_by_class(page, "button.gs-buddy-travel", "派猫猫旅行", dismiss_first=True)
    time.sleep(0.8)
    click_by_class(page, "button.gs-travel-primary-btn", "确定派出")
    time.sleep(1)
    # 关闭派出弹窗
    try:
        click_by_class(page, "button.gs-travel-hero-close", "关闭派出弹窗")
    except Exception as e:
        print(f"  [提示] 关闭派出弹窗失败（可忽略）: {e}")
    print("  场景1 完成")


def do_claim(page):
    """场景3: 领取礼物 → 等按钮变「派猫猫旅行」→ 走场景1。"""
    print("== 场景3: 领取礼物 ==")
    click_by_class(page, "button.gs-buddy-travel", "领取礼物", dismiss_first=True)
    time.sleep(0.8)
    click_by_class(page, "button.gs-travel-claim-btn", "领取积分")
    time.sleep(1)
    try:
        click_by_class(page, "button.gs-travel-letter-close", "关闭领积分弹窗")
    except Exception as e:
        print(f"  [提示] 关闭领积分弹窗失败（可忽略）: {e}")

    # 关闭弹窗后等按钮文本回到「派猫猫旅行」，然后立即派出（避免又被锁住）
    print("  等待按钮文本变回「派猫猫旅行」 …")
    if not wait_state(page, "send", timeout=WAIT_STATE_TIMEOUT):
        print(f"  [警告] {WAIT_STATE_TIMEOUT}s 内按钮未切到「派猫猫旅行」，跳过场景1")
        return
    print("  按钮已就绪，立即派出")
    do_send(page)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="WorkBuddy 成长计划 猫猫旅行")
    ap.add_argument("--port", type=int, default=0,
                    help=f"指定调试端口（默认自动探测，优先 {DEFAULT_PORT}）")
    ap.add_argument("--show", action="store_true",
                    help="启动实例时显示窗口（首次登录用，会等你按回车）")
    ap.add_argument("--no-spawn", action="store_true",
                    help="只连接现有实例，不自动启动")
    ap.add_argument("--close-browser", action="store_true",
                    help="任务结束后关闭本脚本启动的实例（默认保留常驻，最保登录态）")
    args = ap.parse_args()

    if not Path(CHROME_PATH).exists():
        print(f"[错误] 未找到 Chrome: {CHROME_PATH}")
        return 2

    with sync_playwright() as p:
        browser, port = connect_via_cdp(p, args.port)
        spawned_by_us = False

        # --show 需要可见窗口：即便已连上实例，也重启一个带窗口的
        if args.show and not args.no_spawn and browser is not None:
            print("  --show 指定：先关闭当前实例，改为启动带窗口的实例")
            try:
                browser.close()
            except Exception:
                pass
            browser = None
            time.sleep(2)

        if browser is None and not args.no_spawn:
            print("  启动独立 Chrome …")
            spawn_port = args.port or DEFAULT_PORT
            kill_auto_chrome()
            if spawn_auto_chrome(spawn_port, show_window=args.show):
                browser, port = connect_via_cdp(p, spawn_port)
                spawned_by_us = True

        if browser is None:
            print("[错误] 无法连接任何 Chrome 实例")
            print("       若不想自动启动，请去掉 --no-spawn；"
                  "或先跑一次 `python workbuddy_growth.py --show` 完成首次登录。")
            return 1

        context = browser.contexts[0]
        page = context.new_page()
        time.sleep(1)

        try:
            # 打开页面前先注入本地 cookie 备份，保证登录态（profile 丢时的兜底）
            import_cookie_state(context)

            # 首次登录模式：显示窗口时，自动把登录页准备好，再让用户扫码
            if args.show and spawned_by_us:
                print("== 首次登录模式 ==")
                print(f"已打开: {GROWTH_URL}")
                try:
                    page.goto(GROWTH_URL, timeout=60000, wait_until="load")
                except Exception as e:
                    print(f"  [提示] 页面加载异常: {e}（继续）")
                # 等前端完成未登录跳转（异步，最长 20s）
                deadline = time.time() + 20
                while time.time() < deadline and "/login" not in page.url:
                    time.sleep(1)
                # 跳到登录页：修复登录方式记忆 + 自动点同意 + 等二维码出现
                if "/login" in page.url:
                    fix_login_type(page)
                    prepare_wechat_qrcode(page)
                    print("请在 Chrome 窗口中用微信扫码完成登录。")
                    print("（不要用手机验证登录——那是企业租户通道，会要求创建企业）")
                print("确认页面出现「猫猫旅行」后继续。")
                try:
                    input("按回车继续执行脚本 ...")
                except EOFError:
                    print("  [提示] 非交互环境，跳过等待")

            print(f"打开页面: {GROWTH_URL}")
            try:
                page.goto(GROWTH_URL, timeout=60000, wait_until="load")
            except Exception as e:
                print(f"  [提示] 页面加载异常: {e}（可能在重定向，继续探测）")
            # 定时任务里发现登录态失效跳到登录页时，同样修复登录方式记忆
            fix_login_type(page)

            # 懒加载/异步渲染：轮询等到 button 出现，最多 20s
            print("== 探测猫猫旅行状态 ==")
            deadline = time.time() + 20
            state = None
            while time.time() < deadline:
                state = get_travel_state(page)
                if state is not None:
                    break
                time.sleep(1)
            if state is None:
                print("  [错误] 20s 内未找到 button.gs-buddy-travel")
                print("        可能原因：未登录 / 页面结构变化 / 网络异常")
                try:
                    print(f"        当前 URL: {page.url}")
                    print(f"        当前标题: {page.title()}")
                    body_text = page.evaluate(
                        "() => (document.body ? document.body.innerText || '' : '').substring(0, 200)"
                    )
                    print(f"        页面文本片段: {body_text}")
                except Exception as diag_e:
                    print(f"        诊断输出失败: {diag_e}")
                page.screenshot(path="wbgc_state_error.png")
                return 2

            print(f"  当前状态: {state}")
            if state == "send":
                do_send(page)
            elif state == "countdown":
                print("  猫猫正在旅行，无需操作 [OK]")
            elif state == "rested":
                print("  猫猫累了，今天已派过，无需操作 [OK]")
            elif state == "claim":
                do_claim(page)
            elif state.startswith("unknown:"):
                print(f"  [警告] 按钮文本未识别: {state}")
                print("        可能是页面改版，请检查 DOM 后更新场景匹配规则")
                page.screenshot(path="wbgc_unknown_state.png")
                return 2
            print("== 任务完成 ==")
            return 0
        finally:
            # 1) 固化 session cookie + 导出独立备份（双重保险）
            try:
                n = persist_session_cookies(context)
                export_cookie_state(context)
                if n:
                    time.sleep(2)  # 给 Chrome 一点时间把 cookie 写入磁盘
            except Exception as e:
                print(f"  [提示] cookie 持久化处理异常（忽略）: {e}")

            # 2) 只关闭本次新建的标签页
            try:
                if page is not None and not page.is_closed():
                    page.close()
                    print("已关闭本次操作的标签页")
            except Exception as e:
                print(f"  [提示] 关闭标签页失败（忽略）: {e}")

            # 3) 是否关闭浏览器（默认保留常驻；绝不强杀，避免丢弃未落盘 cookie）
            if spawned_by_us and args.close_browser:
                try:
                    session = browser.new_browser_cdp_session()
                    session.send("Browser.close")
                    print("  已请求优雅关闭，等待会话落盘 …")
                    time.sleep(3)
                    print("已优雅关闭自动化 Chrome（登录态已落盘）")
                except Exception as e:
                    print(f"  [警告] 优雅关闭失败，保留进程（不强杀，避免丢 Cookie）: {e}")
            elif spawned_by_us:
                print("自动化 Chrome 保持常驻（下次直接复用；加 --close-browser 可关闭）")


if __name__ == "__main__":
    sys.exit(main())
