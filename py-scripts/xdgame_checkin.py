#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xdgame.com 每日签到脚本（Playwright + Chrome）

流程：
    1. 连接「正在运行的 Chrome」，在它里面新建标签页（不关你现有的 Chrome）
    2. 先访问签到页探测登录态：
       - 已登录 → 直接签到（不再重复登录）
       - 未登录 → 打开登录页，脚本直接填充账号密码（不依赖 LastPass），
         处理「确认你是真人」验证，点击「立即登录」
    3. 跳转到签到页，点击「立即签到」

依赖（仅需装一次）：
    pip install playwright
    （本脚本用系统 Chrome，无需 playwright install 下载浏览器）

前置条件（一次性设置，二选一）：
    方式 A（推荐）：python xdgame_checkin.py --spawn
    脚本自动用独立目录（C:\\Users\\Mickey.Deng\\AppData\\Local\\ChromeAuto）
    启动无头（后台、不弹窗）Chrome 实例并连接，不影响你日常的 Chrome；
    账号密码由脚本直接填充，无需在独立 profile 装 LastPass。

    方式 B：手动保持一个带调试端口的 Chrome 开着，脚本直接连。

    注意：Chrome 129+ 在默认 User Data 目录下会静默忽略
    --remote-debugging-port（安全限制），所以「改日常 Chrome 快捷方式加参数」无效。

用法：
    python xdgame_checkin.py --spawn          # 自动启动独立 Chrome 并登录+签到（推荐）
    python xdgame_checkin.py                  # 连接已开的调试 Chrome（登录+签到）
    python xdgame_checkin.py --skip-login     # 已登录过，直接签到
    python xdgame_checkin.py --port 9223     # 指定调试端口
    python xdgame_checkin.py --launch        # 回退：Playwright 独立实例（需先关 Chrome）

注意：
    - 默认模式不会关闭你的 Chrome，只连上去新建标签页。
    - 真人验证偶尔会被判定为机器而弹出额外验证，此时脚本会停下等你手动完成。
"""

import argparse
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Windows 下控制台/gocron/任务计划普遍按 GBK 解码子进程输出，
# 若脚本输出 UTF-8 会被按 GBK 解码显示成乱码；直接输出 GBK 保持一致。
# 打不出的字符（如 ✅）降级为 ?，但脚本不会崩。
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
USER_DATA_DIR = r"C:\Users\Mickey.Deng\AppData\Local\Google\Chrome\User Data"
PROFILE = "Default"

# 专用自动化数据目录（Chrome 129+ 在默认目录下会忽略 --remote-debugging-port，
# 因此调试端口只能搭配独立 user-data-dir 使用）
AUTO_USER_DATA_DIR = r"C:\Users\Mickey.Deng\AppData\Local\ChromeAuto"

LOGIN_URL = "https://xdgame.com/user/login.php?gourl=%2Fuser%2Fsignin.php"
SIGNIN_URL = "https://xdgame.com/user/signin.php"

# 登录凭据（脚本直接填充，不再依赖 LastPass）
# 安全提示：密码明文存在脚本里，请勿把本文件提交到公开仓库；
# 也可用环境变量 XDGAME_USER / XDGAME_PASS 覆盖，脚本优先读环境变量。
import os as _os
USERNAME = _os.environ.get("XDGAME_USER", "dmjj88")
PASSWORD = _os.environ.get("XDGAME_PASS", "2223141_Xd")

# 清掉代理环境变量：Playwright 的 CDP 连接会读 *_proxy，本机代理软件
# 劫持 127.0.0.1 回环请求时 connect_over_cdp 会 502 失败
# （Chrome 自身走系统代理设置，不受此影响）
for _v in ("http_proxy", "https_proxy", "all_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
    _os.environ.pop(_v, None)
_os.environ["no_proxy"] = _os.environ["NO_PROXY"] = "127.0.0.1,localhost,::1"

# 各脚本专属端口（只探测自己的，绝不连别人的实例——否则会被对方
# 任务结束时的关闭逻辑杀掉浏览器，表现为 page.goto TargetClosedError）：
#   xdgame        = 9222 (ChromeAuto)
#   xianyudanji   = 9225 (ChromeAutoXY)
#   xianyupc      = 9223 (ChromeAutoPC)
#   workbuddy成长 = 9224 (ChromeAutoGC)
DEBUG_PORTS = [9222]


# ---------------------------------------------------------------------------
# Chrome 连接
# ---------------------------------------------------------------------------
def probe_debug_port(port):
    """探测某调试端口是否可用（返回 True 表示有 Chrome 在监听）。

    用 ProxyHandler({}) 强制直连：本机代理软件会劫持回环请求，
    走系统代理探测会 502 / 误判端口不可用。
    """
    try:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )
        opener.open(f"http://127.0.0.1:{port}/json/version", timeout=2)
        return True
    except Exception:
        return False


def connect_via_cdp(p, port=0):
    """通过 CDP 连接正在运行的 Chrome。返回 (browser, port)，失败返回 (None, None)。"""
    ports = [port] if port else DEBUG_PORTS
    for pt in ports:
        if not pt:
            continue
        if not probe_debug_port(pt):
            continue
        try:
            # timeout 显式收紧：默认 30s，僵死实例会一直挂着
            browser = p.chromium.connect_over_cdp(
                f"http://127.0.0.1:{pt}", timeout=10000
            )
            print(f"已连接现有 Chrome（127.0.0.1:{pt}），将新建标签页 …")
            return browser, pt
        except Exception as e:
            print(f"  [提示] 连接 127.0.0.1:{pt} 失败: {e}")
            continue
    return None, None


def kill_auto_chrome():
    """只杀本脚本专用目录（ChromeAuto）的 Chrome 进程，不碰日常 Chrome。

    精确匹配 "--user-data-dir=…\\ChromeAuto"（目录名后面必须是空格或
    命令行结尾），不会误杀 ChromeAutoXY / ChromeAutoPC / ChromeAutoGC
    等其他签到脚本的实例（旧的 '*ChromeAuto*' 前缀通配会把它们全杀掉）。
    """
    ps_cmd = (
        "Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\" | "
        "Where-Object {$_.CommandLine -match "
        "'--user-data-dir=\\S+\\\\ChromeAuto(\\s|$)'} | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, timeout=15,
        )
        time.sleep(2)
        print("  已清理僵死的自动化 Chrome 实例（ChromeAuto）")
    except Exception as e:
        print(f"  [提示] 清理自动化 Chrome 失败（忽略）: {e}")


def chrome_running():
    """检测是否有 Chrome 主进程在运行。"""
    try:
        out = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq chrome.exe", "/NH"],
            capture_output=True, text=True, timeout=10,
        ).stdout
        return "chrome.exe" in out
    except Exception:
        return False


def kill_chrome():
    subprocess.run(["taskkill", "/F", "/IM", "chrome.exe"],
                   capture_output=True, check=False)
    time.sleep(3)


# ---------------------------------------------------------------------------
# 页面操作
# ---------------------------------------------------------------------------
def fill_credentials(page):
    """脚本直接填充账号与密码（不依赖 LastPass）。返回 (user_ok, pwd_ok)。"""
    user_sel = ('input[name="username"], input[name="user"], '
                'input[name="account"], input[type="text"]:not([readonly])')
    pwd_sel = 'input[type="password"]'

    user_ok = pwd_ok = False
    try:
        u = page.locator(user_sel).first
        u.wait_for(state="visible", timeout=10000)
        u.click()
        u.fill("")           # 清掉可能存在的残留/LastPass 半填充内容
        u.type(USERNAME, delay=60)   # 模拟逐键输入，降低风控特征
        user_ok = True
        print(f"  已填充账号: {USERNAME}")
    except Exception as e:
        print(f"  [错误] 填充账号失败: {e}")

    try:
        p = page.locator(pwd_sel).first
        p.wait_for(state="visible", timeout=5000)
        p.click()
        p.fill("")
        p.type(PASSWORD, delay=60)
        pwd_ok = True
        print("  已填充密码")
    except Exception as e:
        print(f"  [错误] 填充密码失败: {e}")

    return user_ok, pwd_ok


def click_human_verify(page):
    """点击「确认你是真人」验证（Cap.js cap-widget / Cloudflare Turnstile / hCaptcha）。

    返回 True 表示已点击到验证框；是否通过由 verify_passed() 再判断。
    """
    # 0) Cap.js：cap-widget 用 open shadow DOM，Playwright 的 CSS 选择器
    #    可穿透 shadow root，直接点里面的 .captcha-trigger
    try:
        widget = page.locator('cap-widget').first
        if widget.is_visible(timeout=3000):
            state = page.evaluate(
                "() => { const w = document.querySelector('cap-widget');"
                "  if (!w || !w.shadowRoot) return 'none';"
                "  const c = w.shadowRoot.querySelector('.captcha');"
                "  return c ? (c.dataset.state || 'idle') : 'idle'; }"
            )
            print(f"  发现 Cap.js 验证，当前状态: {state}")
            if state != "done":
                trigger = page.locator('cap-widget .captcha-trigger').first
                trigger.scroll_into_view_if_needed()
                trigger.click(timeout=5000)
                print("  已点击 Cap 验证（.captcha-trigger）")
            return True
    except Exception:
        pass

    # 1) 优先在验证 iframe 内找复选框
    for frame in page.frames:
        url = frame.url or ""
        if "challenges.cloudflare.com" in url or "hcaptcha" in url \
                or "recaptcha" in url:
            try:
                box = frame.locator(
                    'input[type="checkbox"], #checkbox, div[role="checkbox"]'
                ).first
                if box.is_visible(timeout=3000):
                    box.scroll_into_view_if_needed()
                    box.click(timeout=5000)
                    print("  已在验证 iframe 内点击复选框")
                    return True
            except Exception:
                continue

    # 2) 回退：主页面内直接找验证控件
    selectors = [
        'div.cf-turnstile',
        'div.g-recaptcha',
        'div.h-captcha',
        'input[type="checkbox"][name*="cf"]',
        'text=确认你是真人',
        'text=Verify you are human',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                el.scroll_into_view_if_needed()
                el.click(timeout=5000)
                print(f"  已点击验证控件: {sel}")
                return True
        except Exception:
            continue

    return False


def verify_passed(page):
    """判断真人验证是否通过。覆盖三种信号（任一命中即视为通过）：

    1) Cap.js：shadow root 内 .captcha 的 data-state="done"，
       或隐藏字段 input[name="cap-token"] 有值（实测成功后的 DOM 就是这样）
    2) Turnstile/hCaptcha：隐藏字段 cf-turnstile-response /
       h-captcha-response / g-recaptcha-response 有值
    3) 通用：iframe 内 checked 复选框 / .mark 成功图标
    """
    # 1) Cap.js
    try:
        cap_ok = page.evaluate(
            "() => {"
            "  const w = document.querySelector('cap-widget');"
            "  if (w && w.shadowRoot) {"
            "    const c = w.shadowRoot.querySelector('.captcha');"
            "    if (c && c.dataset.state === 'done') return true;"
            "  }"
            "  const t = document.querySelector('input[name=\"cap-token\"]');"
            "  return !!(t && t.value && t.value.length > 10);"
            "}"
        )
        if cap_ok:
            return True
    except Exception:
        pass

    # 2) Turnstile / hCaptcha / reCAPTCHA 隐藏 response 字段
    try:
        ok = page.evaluate(
            "() => {"
            "  const names = ['cf-turnstile-response','h-captcha-response',"
            "                'g-recaptcha-response'];"
            "  for (const n of names) {"
            "    const el = document.querySelector(`[name=\"${n}\"]`) || document.getElementById(n);"
            "    if (el && el.value && el.value.length > 10) return true;"
            "  }"
            "  return false;"
            "}"
        )
        if ok:
            return True
    except Exception:
        pass

    # 3) 通用：iframe 内 checked 复选框或 .mark 成功图标
    for frame in page.frames:
        try:
            has_check = frame.locator(
                'input[type="checkbox"].checked, .mark'
            ).count() > 0
            if has_check:
                return True
        except Exception:
            continue
    return False


def do_login(page):
    print("== 登录 ==")
    page.goto(LOGIN_URL, timeout=60000)
    time.sleep(2)

    print("脚本直接填充账号密码 …")
    user_ok, pwd_ok = fill_credentials(page)
    if not (user_ok and pwd_ok):
        print("  [提示] 填充不完整，30 秒内可手动补填，超时自动继续 …")
        js_check = (
            "() => {"
            "  const u = document.querySelector('input[name=\"username\"],"
            "input[name=\"user\"],input[type=\"text\"]:not([readonly])');"
            "  const p = document.querySelector('input[type=\"password\"]');"
            "  return [!!(u && u.value && u.value.trim()), !!(p && p.value)];"
            "}"
        )
        for _ in range(30):
            try:
                user_ok, pwd_ok = page.evaluate(js_check)
            except Exception:
                pass
            if user_ok and pwd_ok:
                print("  已检测到账号密码就绪")
                break
            time.sleep(1)

    print("处理真人验证 …")
    clicked = click_human_verify(page)
    if clicked:
        # 给验证完成留时间，最多等 20s；期间一旦检测到通过立即继续
        for _ in range(20):
            if verify_passed(page):
                print("  真人验证已通过")
                break
            time.sleep(1)
        else:
            print("  [注意] 20s 内未检测到验证通过标记，仍尝试登录（有时检测信号缺失但实际已过）")
    else:
        print("  [警告] 未找到验证控件，可能是自动通过的隐藏验证，继续尝试登录 …")

    # 若仍显示未通过，再等最多 30s 留人工补点（不阻塞死等，超时自动继续）
    if not verify_passed(page):
        print("  [提示] 若浏览器里弹出图片验证，请手动完成；30 秒后自动继续 …")
        for _ in range(30):
            if verify_passed(page):
                print("  真人验证已通过")
                break
            time.sleep(1)

    print("点击「立即登录」…")
    login_btn_selectors = (
        'button:has-text("立即登录"), input[value*="登录"], '
        'a:has-text("立即登录"), button:has-text("登录")'
    )
    clicked_login = False
    for attempt in range(3):
        try:
            btn = page.locator(login_btn_selectors).first
            # 等按钮可见且可用（验证刚通过时按钮可能有短暂 disabled）
            btn.wait_for(state="visible", timeout=10000)
            for _ in range(10):
                if btn.is_enabled():
                    break
                time.sleep(1)
            btn.click(timeout=10000)
            clicked_login = True
            break
        except Exception as e:
            print(f"  第 {attempt + 1} 次点击登录失败: {e}")
            time.sleep(2)
    if not clicked_login:
        print("  [错误] 多次尝试后仍点不到登录按钮")
        page.screenshot(path="login_error.png")
        return False

    # 等跳转到签到页
    try:
        page.wait_for_url("**/signin.php", timeout=30000)
        print("  登录成功，已进入签到页")
        return True
    except PWTimeout:
        # 可能还停在登录页（验证没过/密码错）
        if "login" in page.url:
            print(f"  [错误] 登录失败，仍停留在: {page.url}")
            page.screenshot(path="login_failed.png")
            print("  截图已保存: login_failed.png")
            return False
        return True


def dismiss_favorite_popup(page, wait_seconds=0):
    """关掉用户中心的「收藏本站」引导弹窗。

    跳转到用户中心后站点会弹一个收藏引导层（button.mn-confirm「我已收藏」），
    挡住每日签到标签/按钮。点掉它弹窗才消失，后续点击才能生效。
    没弹窗时静默跳过（幂等）。

    wait_seconds > 0 时轮询等待弹窗出现（弹窗可能延迟渲染，
    两次立即检查会漏掉），期间出现即点击。
    """
    deadline = time.time() + wait_seconds
    while True:
        try:
            btn = page.locator("button.mn-confirm").first
            if btn.is_visible(timeout=2500):
                btn.click(timeout=5000)
                print("  已关闭「收藏本站」引导弹窗（我已收藏）")
                time.sleep(1)
                return True
        except Exception:
            pass
        if time.time() >= deadline:
            return False
        time.sleep(1)


def ensure_logged_in(page, force_login=False):
    """探测登录态：先访问签到页。

    - 已登录（没被踢回登录页）→ 返回 "ok"，直接签到
    - 未登录（跳到 login.php 或页面有密码框）→ 走完整登录流程
    """
    print("== 检查登录状态 ==")
    page.goto(SIGNIN_URL, timeout=60000)
    time.sleep(2)
    # 用户中心可能弹「收藏本站」引导层，挡住签到入口，先关掉
    dismiss_favorite_popup(page)

    if not force_login:
        redirected_to_login = "login" in page.url
        try:
            has_pwd = page.locator('input[type="password"]').count() > 0
        except Exception:
            has_pwd = False
        if not redirected_to_login and not has_pwd:
            print("  已登录（会话有效），跳过登录，直接签到")
            return "ok"
        print(f"  未登录（当前页面: {page.url}），开始登录流程 …")
    else:
        print("  指定强制登录，开始登录流程 …")

    return "ok" if do_login(page) else "fail"


def do_signin(page):
    print("== 签到 ==")
    if "signin" not in page.url:
        page.goto(SIGNIN_URL, timeout=60000)
        time.sleep(2)

    # 用户中心可能弹「收藏本站」引导层（登录跳转后新出现），挡住签到标签；
    # 弹窗可能延迟渲染，轮询等 6s，期间出现即点掉
    dismiss_favorite_popup(page, wait_seconds=6)

    # 先检测是否已签到：已签到时按钮变成 disabled 的「今日已签到」
    try:
        already = page.evaluate(
            "() => {"
            "  const b = document.getElementById('signin-button');"
            "  if (b && b.disabled) return true;"
            "  const text = (b && b.textContent || '').trim();"
            "  return text.includes('已签到');"
            "}"
        )
        if already:
            print("  今日已签到（按钮 disabled: 今日已签到），无需重复签到 ✅")
            return True
    except Exception:
        pass

    print("点击「立即签到」…")
    for attempt in range(3):
        try:
            btn = page.locator('#signin-button').first
            btn.wait_for(state="visible", timeout=10000)
            # 若中途变为 disabled（已签到），直接返回
            if btn.is_disabled():
                print("  今日已签到，无需重复签到 ✅")
                return True
            btn.scroll_into_view_if_needed()
            btn.click(timeout=10000)
            print("  已点击签到按钮")
            break
        except Exception as e:
            # 点击可能被延迟出现的弹窗遮罩拦截，关掉再试
            dismiss_favorite_popup(page)
            if attempt == 2:
                print(f"  [错误] 签到失败: {e}")
                page.screenshot(path="signin_error.png")
                return False
            print(f"  第 {attempt + 1} 次点击失败（{e}），已尝试关弹窗，重试 …")

    time.sleep(3)
    print("签到完成 ✅")
    return True


def _print_cdp_help():
    print()
    print("[无法连接现有 Chrome] 没有发现开了调试端口的 Chrome 实例。")
    print()
    print("注意：Chrome 129+ 在默认 User Data 目录下会静默忽略 --remote-debugging-port，")
    print("所以不能用普通快捷方式加参数的方式。需要用独立的自动化数据目录：")
    print()
    print('  一次性初始化（手动跑一次，在里面装好 LastPass / 登录一次 xdgame）：')
    print()
    print(f'    "{CHROME_PATH}" --user-data-dir="{AUTO_USER_DATA_DIR}" --remote-debugging-port=9222')
    print()
    print("  之后每天直接运行：")
    print()
    print("    python xdgame_checkin.py --spawn")
    print()
    print("（--spawn 会自动用独立目录启动 Chrome 并连接；用完不会关你日常的 Chrome）")
    print()


def spawn_auto_chrome(port):
    """用独立 user-data-dir 启动带调试端口的 Chrome（无头后台运行），返回是否成功。"""
    import subprocess as sp
    print(f"启动自动化 Chrome 实例（无头后台，独立目录 {AUTO_USER_DATA_DIR}）…")
    # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP：脱离父进程组，
    # gocron/终端退出或被强杀时不连带杀掉 Chrome（否则实例半死成僵死状态）
    sp.Popen([
        CHROME_PATH,
        "--headless=new",
        f"--user-data-dir={AUTO_USER_DATA_DIR}",
        f"--remote-debugging-port={port}",
        "--window-size=1280,900",
        "about:blank",
    ], creationflags=0x00000008 | 0x00000200)
    # 等端口就绪
    for _ in range(20):
        time.sleep(1)
        if probe_debug_port(port):
            print(f"  调试端口 127.0.0.1:{port} 已就绪")
            return True
    print("  [错误] Chrome 启动后调试端口未就绪")
    return False


def browser_alive(browser):
    """CDP 连接成功不代表实例可用。

    僵死/正在关闭的实例端口还在监听、connect_over_cdp 也能成功，但
    浏览器随时消失，表现为 page.goto 抛 TargetClosedError。
    建临时页做一次 about:blank 导航，验证通道真的活着。
    """
    try:
        ctx = browser.contexts[0]
        tp = ctx.new_page()
        tp.goto("about:blank", timeout=8000)
        tp.close()
        return True
    except Exception:
        return False


def close_auto_chrome(browser):
    """优雅关闭自动化 Chrome（Browser.close 走正常退出，Cookie 落盘）。

    任务结束后关闭实例，避免残留进程被 gocron 等调度器弄成僵死状态；
    会话数据已持久化在 ChromeAuto 目录，下次启动自动恢复登录态。
    """
    try:
        session = browser.new_browser_cdp_session()
        session.send("Browser.close")
        time.sleep(2)
        print("已优雅关闭自动化 Chrome（会话已落盘）")
        return True
    except Exception:
        pass
    # CDP 关不掉就强杀兜底
    kill_auto_chrome()
    return False


def main():
    ap = argparse.ArgumentParser(description="xdgame.com 每日签到")
    ap.add_argument("--skip-login", action="store_true",
                    help="跳过登录检查，直接签到（已确认登录时用）")
    ap.add_argument("--force-login", action="store_true",
                    help="强制重新登录（默认自动探测：已登录就直接签到）")
    ap.add_argument("--kill-chrome", action="store_true",
                    help="（仅 --launch 模式）自动关闭正在运行的 Chrome")
    ap.add_argument("--launch", action="store_true",
                    help="回退：独立启动 Chrome 实例（需先关闭 Chrome）")
    ap.add_argument("--spawn", action="store_true",
                    help="连不上时，自动用独立数据目录启动带调试端口的 Chrome 再连接")
    ap.add_argument("--port", type=int, default=0,
                    help="指定调试端口（默认自动探测 9222/9223/9333/9229）")
    args = ap.parse_args()

    if not Path(CHROME_PATH).exists():
        print(f"[错误] 未找到 Chrome: {CHROME_PATH}")
        return 2

    with sync_playwright() as p:
        browser = None
        context = None
        spawned_by_us = False

        if args.launch:
            # ---- 旧方式：独立启动 Chrome（独占 profile，需先关现有 Chrome）----
            if chrome_running():
                if args.kill_chrome:
                    print("检测到 Chrome 正在运行，正在关闭 …")
                    kill_chrome()
                else:
                    print("[错误] Chrome 正在运行，--launch 模式无法复用其配置。")
                    print("       请先关闭 Chrome，或加 --kill-chrome 让脚本自动关闭。")
                    return 1
            context = p.chromium.launch_persistent_context(
                user_data_dir=USER_DATA_DIR,
                executable_path=CHROME_PATH,
                headless=False,
                slow_mo=150,
                args=[f"--profile-directory={PROFILE}"],
            )
            page = context.pages[0] if context.pages else context.new_page()
        else:
            # ---- 默认：连接正在运行的 Chrome，新建标签页 ----
            browser, port = connect_via_cdp(p, args.port)
            # 活性验证：连上了但实例僵死/正在关闭 → 忽略它走 spawn 路径
            if browser is not None and not browser_alive(browser):
                print("  [提示] 现有实例无响应（僵死或正在关闭），忽略它重新拉起 …")
                try:
                    browser.close()   # 仅断开连接，不动浏览器本体
                except Exception:
                    pass
                browser = None
            if browser is None and args.spawn:
                # 自动拉起独立目录的 Chrome 实例再连
                spawn_port = args.port or 9222
                # 连不上可能是旧实例僵死（端口在听但 CDP 无响应），先清理再拉新的
                kill_auto_chrome()
                if spawn_auto_chrome(spawn_port):
                    browser, port = connect_via_cdp(p, spawn_port)
                    spawned_by_us = True
            if browser is None:
                _print_cdp_help()
                return 1
            context = browser.contexts[0]
            page = context.new_page()
            time.sleep(1)

        try:
            if args.skip_login:
                result = "ok"
            else:
                result = ensure_logged_in(page, force_login=args.force_login)
            if result != "ok":
                return 2
            if not do_signin(page):
                return 2
            return 0
        finally:
            # 任务结束收尾：
            # - 我们 spawn 的实例 → 优雅关闭（避免残留成僵死进程）
            # - 外部已有实例     → 只断开连接，不动它
            if browser is not None:
                if spawned_by_us:
                    close_auto_chrome(browser)
                else:
                    browser.close()
            elif context is not None:
                context.close()


if __name__ == "__main__":
    sys.exit(main())
