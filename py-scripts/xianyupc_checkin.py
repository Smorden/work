#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
咸鱼PC（xianyupc.com）每日签到脚本（Playwright + Chrome + HTTP 代理）

与 xianyudanji_checkin.py 同源（ripro-v2 主题，页面结构相同），差异：
    1. 域名：https://www.xianyupc.com/
    2. 登录无滑块验证（rsc-captcha），填完账号密码直接点「立即登录」
    3. 直连慢，走 HTTP 代理 127.0.0.1:1080（Chrome 启动参数 --proxy-server）

流程：
    1. 自动启动无头 Chrome（--spawn，带代理，独立目录 ChromeAutoPC / 端口 9223）
    2. 先访问 /user 探测登录态（多信号：user-logout / 头像 / 用户名）
    3. 未登录 → 回首页点右上角「登录」→ 浮层填账号密码 → 点「立即登录」
    4. 在 /user 页点击「每日签到」（已签到幂等）

用法：
    python xianyupc_checkin.py --spawn       # 自动启动带代理的无头 Chrome 并签到（推荐）
    python xianyupc_checkin.py               # 连接已开的调试 Chrome（需自行带代理启动）
    python xianyupc_checkin.py --force-login # 强制重新登录
    python xianyupc_checkin.py --port 9223   # 指定调试端口
"""

import argparse
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

# Windows 下控制台/gocron/任务计划普遍按 GBK 解码子进程输出，
# 直接输出 GBK 保持一致；打不出的字符（如 ✅）降级为 ?，但脚本不会崩。
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
# 独立于 xianyudanji 的 ChromeAuto：两站签到任务可能同时/先后跑，
# 数据目录和端口都分开，互不干扰
AUTO_USER_DATA_DIR = r"C:\Users\Mickey.Deng\AppData\Local\ChromeAutoPC"
DEFAULT_PORT = 9223
# 本脚本专属端口（只探测自己的，绝不连别的脚本的实例——否则会被对方
# 任务结束时的关闭逻辑杀掉浏览器，表现为 page.goto TargetClosedError）：
#   xdgame=9222(ChromeAuto) xianyudanji=9225(ChromeAutoXY)
#   xianyupc=9223(ChromeAutoPC) workbuddy成长=9224(ChromeAutoGC)
DEBUG_PORTS = [9223]

BASE_URL = "https://www.xianyupc.com/"
HOME_URL = BASE_URL
USER_URL = "https://www.xianyupc.com/user"

# HTTP 代理（本站直连慢，Chrome 层代理）
PROXY = "http://127.0.0.1:1080"

import os as _os
USERNAME = _os.environ.get("XYPC_USER", "dmjj88")
PASSWORD = _os.environ.get("XYPC_PASS", "2223141_Xy")

# 清掉代理环境变量：Playwright 的 CDP 连接会读 *_proxy，本机代理软件
# 劫持 127.0.0.1 回环请求时 connect_over_cdp 会 502 失败
# （Chrome 网页流量走下面的 --proxy-server=1080 参数，不受此影响）
for _v in ("http_proxy", "https_proxy", "all_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
    _os.environ.pop(_v, None)
_os.environ["no_proxy"] = _os.environ["NO_PROXY"] = "127.0.0.1,localhost,::1"


# ---------------------------------------------------------------------------
# Chrome 连接
# ---------------------------------------------------------------------------
def probe_debug_port(port):
    """探测调试端口是否可用。

    ProxyHandler({}) 强制直连：实测本机代理软件会劫持回环请求，
    urllib 走系统代理探测 127.0.0.1 会得到 502（urllib 默认不会绕过
    127.0.0.1，必须显式空代理）。
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
    """只杀本脚本专用目录（ChromeAutoPC）的 Chrome 进程。

    精确匹配 ChromeAutoPC，不会误杀 xianyudanji 的 ChromeAuto 实例和日常 Chrome。
    """
    ps_cmd = (
        "Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\" | "
        "Where-Object {$_.CommandLine -like '*ChromeAutoPC*'} | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, timeout=15,
        )
        time.sleep(2)
        print("  已清理僵死的自动化 Chrome 实例（ChromeAutoPC）")
    except Exception as e:
        print(f"  [提示] 清理自动化 Chrome 失败（忽略）: {e}")


def spawn_auto_chrome(port):
    """启动带代理 + 调试端口的无头 Chrome（独立目录 ChromeAutoPC）。"""
    import subprocess as sp
    print(f"启动自动化 Chrome（无头 + 代理 {PROXY}，目录 {AUTO_USER_DATA_DIR}）…")
    # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP：脱离父进程组，
    # gocron/终端退出或被强杀时不连带杀掉 Chrome
    sp.Popen([
        CHROME_PATH,
        "--headless=new",
        f"--user-data-dir={AUTO_USER_DATA_DIR}",
        f"--remote-debugging-port={port}",
        f"--proxy-server={PROXY}",
        # 本地回环不走代理（CDP 调试端口必须直连）
        "--proxy-bypass-list=<-loopback>",
        "--window-size=1280,900",
        "about:blank",
    ], creationflags=0x00000008 | 0x00000200)
    for _ in range(20):
        time.sleep(1)
        if probe_debug_port(port):
            print(f"  调试端口 127.0.0.1:{port} 已就绪")
            return True
    print("  [错误] Chrome 启动后调试端口未就绪")
    return False


def _print_cdp_help():
    print()
    print("[无法连接现有 Chrome] 没有发现开了调试端口的 Chrome 实例。")
    print("直接运行 python xianyupc_checkin.py --spawn 即可自动拉起带代理的无头 Chrome。")
    print()


# ---------------------------------------------------------------------------
# 页面操作
# ---------------------------------------------------------------------------
def is_logged_in(page):
    """判断是否已登录（ripro-v2 主题，多信号）。

    已登录：导航栏渲染头像下拉框（.user-logout / .mx-display-name /
    .menu-avatar-img），只有登录态才有；未登录：导航栏是「登录」按钮。
    不能只认「每日签到」按钮文案：已签到当天文案会变，懒加载也会慢。
    """
    signals = [
        'a.user-logout',            # 退出登录链接（已登录独有）
        '.mx-display-name',         # 导航栏用户名
        '.menu-avatar-img',         # 导航栏头像
        '.dropdown-item-nicon',     # 用户下拉菜单导航
        'text=每日签到',             # 保留原信号
        'text=已签到',               # 当天已签到的情况
    ]
    for sel in signals:
        try:
            if page.locator(sel).count() > 0:
                return True
        except Exception:
            continue
    try:
        if page.locator('text=退出登录').count() > 0:
            return True
    except Exception:
        pass
    return False


def click_first_visible(page, selectors, timeout_ms=3000):
    """遍历选择器列表，点第一个「可见」的匹配元素。

    页面里常藏着不可见的登录浮层（DOM 存在但 display:none），
    locator().first 可能选中不可见元素导致 wait_for(visible) 永远超时——
    所以逐个候选、逐个实例检查可见性再点。
    """
    for sel in selectors:
        try:
            loc = page.locator(sel)
            n = loc.count()
            for i in range(min(n, 20)):
                el = loc.nth(i)
                try:
                    if el.is_visible():
                        el.click(timeout=5000)
                        return el
                except Exception:
                    continue
        except Exception:
            continue
    return None


def fill_login_form(page):
    """在登录浮层内填充账号密码（form.ajax-signup-form，取可见实例）。"""
    user_ok = pwd_ok = False
    try:
        u = click_first_visible(page, ['input[name="username"]'], timeout_ms=0)
        if u is None:
            raise RuntimeError("账号输入框不可见")
        u.fill("")
        u.type(USERNAME, delay=60)
        user_ok = True
        print(f"  已填充账号: {USERNAME}")
    except Exception as e:
        print(f"  [错误] 填充账号失败: {e}")

    try:
        p = click_first_visible(page, ['input[name="password"]'], timeout_ms=0)
        if p is None:
            raise RuntimeError("密码输入框不可见")
        p.fill("")
        p.type(PASSWORD, delay=60)
        pwd_ok = True
        print("  已填充密码")
    except Exception as e:
        print(f"  [错误] 填充密码失败: {e}")

    return user_ok, pwd_ok


def do_login(page):
    """首页点「登录」→ 浮层填表 → 点「立即登录」（本站无滑块验证）。"""
    print("== 登录 ==")
    # 走代理时页面加载更慢，超时放宽
    if not goto_stable(page, HOME_URL):
        return False
    time.sleep(3)

    print("点击右上角「登录」…")
    entry_selectors = [
        '.switch-mod-btn[data-mod="login"]',   # ajax 登录插件的标准入口属性
        'a[data-mod="login"]',
        '[data-mod="login"]',
        'a:has-text("登录")',
        'button:has-text("登录")',
        'a:has-text("登 录")',
        'text=登录',
        '[class*="login"] a',
        '[class*="login"] button',
    ]
    entry = None
    for attempt in range(2):
        entry = click_first_visible(page, entry_selectors)
        if entry is not None:
            break
        time.sleep(3)
    if entry is None:
        # JS 兜底：直接给 [data-mod="login"] 派发点击事件
        try:
            js_clicked = page.evaluate(
                "() => {"
                "  const els = document.querySelectorAll('[data-mod=\"login\"]');"
                "  for (const el of els) { el.click(); return true; }"
                "  const a = Array.from(document.querySelectorAll('a,button'))"
                "    .find(x => x.textContent.trim() === '登录');"
                "  if (a) { a.click(); return true; }"
                "  return false;"
                "}"
            )
            if js_clicked:
                print("  已通过 JS 事件派发触发登录弹窗")
                entry = "js"
        except Exception:
            pass
    if entry is None:
        print("  [错误] 找不到可见的登录入口")
        page.screenshot(path="xypc_login_entry_error.png")
        return False
    print("  已点击登录入口")
    time.sleep(1.5)

    print("填充账号密码 …")
    user_ok, pwd_ok = fill_login_form(page)
    if not (user_ok and pwd_ok):
        print("  [提示] 填充不完整，仍尝试继续 …")

    # 本站无滑块验证，按钮应可直接点击（仍保留轮询等解锁的稳健逻辑，
    # 防止个别情况按钮 disabled）
    print("点击「立即登录」…")
    clicked = False
    for _ in range(15):
        try:
            loc = page.locator('button.go-login')
            for i in range(min(loc.count(), 5)):
                el = loc.nth(i)
                if el.is_visible():
                    if el.is_enabled():
                        el.click(timeout=5000)
                        clicked = True
                        print("  已点击立即登录")
                    break
        except Exception:
            pass
        if clicked:
            break
        time.sleep(1)
    if not clicked:
        print("  [错误] 登录按钮 15s 内未解锁或点击失败")
        page.screenshot(path="xypc_login_error.png")
        return False

    # 登录成功的信号：浮层关闭（密码框不可见）或页面导航
    print("等待登录结果 …")
    login_done = False
    for _ in range(20):
        time.sleep(1)
        try:
            pwd_visible = page.locator('input[type="password"]:visible').count()
        except Exception:
            pwd_visible = 0  # 页面导航中取不到 = 表单已不在
        if pwd_visible == 0:
            login_done = True
            print("  登录浮层已关闭，登录成功")
            break
    if not login_done:
        print("  [警告] 20s 后浮层仍在，登录可能失败（密码错/账号不存在）")
        page.screenshot(path="xypc_login_failed.png")

    # 登录后站点可能自己触发跳转（如刷新首页），等导航彻底稳定，
    # 否则 do_signin 的 goto 会和这次跳转撞车（interrupted by another navigation）
    try:
        page.wait_for_load_state("load", timeout=30000)
    except Exception:
        pass
    return True


def goto_stable(page, url, timeout=90000, attempts=3):
    """带重试的 goto：登录后站点自身跳转可能打断我们的导航。"""
    last_err = None
    for _ in range(attempts):
        try:
            page.goto(url, timeout=timeout)
            return True
        except Exception as e:
            last_err = e
            # 导航被打断（站点自身在跳转）——等它稳定后重试
            try:
                page.wait_for_load_state("load", timeout=15000)
            except Exception:
                pass
            time.sleep(1)
    print(f"  [错误] 打开 {url} 失败: {last_err}")
    return False


def dismiss_swal2(page):
    """关掉页面上所有可见的 SweetAlert2 弹窗（点遮罩 / 关闭按钮 / Escape）。

    这类站经常在签到页弹出「请先阅读条款」「签到成功」之类的弹窗，
    不关掉会拦截后续点击（element intercepts pointer events）。
    """
    try:
        # 关掉所有可见的 swal2 弹窗（最多 3 个）
        for _ in range(3):
            closed = page.evaluate(
                "() => {"
                "  const containers = document.querySelectorAll('.swal2-container.swal2-backdrop-show, .swal2-container:not(.swal2-hide)');"
                "  if (!containers.length) return 0;"
                "  for (const c of containers) {"
                "    // 优先找确认按钮 / 关闭按钮"
                "    const btn = c.querySelector('.swal2-confirm, .swal2-close, .swal2-cancel');"
                "    if (btn) { btn.click(); }"
                "    else {"
                "      // 没按钮就点遮罩的右上角（点容器背景，不点内容区）"
                "      const r = c.getBoundingClientRect();"
                "      const ev = new MouseEvent('click', {bubbles:true, clientX:r.left+5, clientY:r.top+5});"
                "      c.dispatchEvent(ev);"
                "    }"
                "  }"
                "  return containers.length;"
                "}"
            )
            if closed == 0:
                break
            time.sleep(0.5)
    except Exception:
        pass
    # 兜底：Escape 键
    try:
        page.keyboard.press("Escape")
        time.sleep(0.3)
    except Exception:
        pass


def do_signin(page):
    print("== 签到 ==")
    if not goto_stable(page, USER_URL):
        return False
    time.sleep(2)

    # 签到前先关掉所有 swal2 弹窗（公告/条款/广告等），否则会拦截按钮点击
    dismiss_swal2(page)

    # 已签到幂等检测：根据按钮状态判断。
    # 按钮 .go-user-qiandao 的文本/data-original-title 在「未签到」时含「每日签到」，
    # 「已签到」时变成「已签到」之类。直接读按钮属性，避免正文里其他「每日签到」文字干扰。
    try:
        state = page.evaluate(
            "() => {"
            "  const b = document.querySelector('button.go-user-qiandao');"
            "  if (!b) return 'notfound';"
            "  if (b.disabled) return 'disabled';"
            "  const txt = (b.innerText || b.textContent || '').trim();"
            "  const tip = b.getAttribute('data-original-title') || '';"
            "  if (/已签到|已领取|已完成/.test(txt + tip)) return 'already';"
            "  return 'ok';"
            "}"
        )
        if state == "already":
            print("  今日已签到，无需重复签到 [OK]")
            return True
        if state == "disabled":
            print("  签到按钮 disabled（可能今天已签或活动未开始）")
            return True
    except Exception:
        pass

    print("点击「每日签到」…")
    try:
        # 优先用按钮 class 定位，避免 text 匹配到其他「每日签到」文字
        btn = page.locator('button.go-user-qiandao').first
        btn.wait_for(state="visible", timeout=10000)
        btn.scroll_into_view_if_needed()
        # 点击前再清一次弹窗（页面加载完成后又可能弹了新的）
        dismiss_swal2(page)
        try:
            btn.click(timeout=10000, force=True)
        except Exception:
            # 强制点击被拒绝就再清弹窗后用 JS 直接派发
            dismiss_swal2(page)
            page.evaluate("() => { const b = document.querySelector('button.go-user-qiandao'); if (b) b.click(); }")
        print("  已点击签到按钮")
    except Exception as e:
        print(f"  [错误] 签到失败: {e}")
        page.screenshot(path="xypc_signin_error.png")
        return False

    # 签到后可能弹出结果弹窗，先关掉再判定
    time.sleep(2)
    dismiss_swal2(page)
    time.sleep(1)
    print("签到完成 [OK]")
    return True


def ensure_logged_in(page, force_login=False):
    """先访问 /user 探测登录态，未登录才走登录流程。"""
    print("== 检查登录状态 ==")
    if not goto_stable(page, USER_URL):
        return False
    time.sleep(2)

    # 首查未命中可能是懒加载/代理慢未渲染完，重查三轮再下结论
    if not force_login:
        for _ in range(3):
            if is_logged_in(page):
                print("  已登录（会话有效），跳过登录，直接签到")
                return True
            time.sleep(2)
        print("  未登录，开始登录流程 …")
        return do_login(page)
    print("  强制重新登录 …")
    return do_login(page)


def close_auto_chrome(browser):
    """优雅关闭自动化 Chrome（Browser.close 走正常退出，Cookie 落盘）。"""
    try:
        session = browser.new_browser_cdp_session()
        session.send("Browser.close")
        time.sleep(2)
        print("已优雅关闭自动化 Chrome（会话已落盘）")
        return True
    except Exception:
        pass
    kill_auto_chrome()
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


def main():
    ap = argparse.ArgumentParser(description="咸鱼PC（xianyupc.com）每日签到")
    ap.add_argument("--skip-login", action="store_true",
                    help="跳过登录检查，直接签到（已确认登录时用）")
    ap.add_argument("--force-login", action="store_true",
                    help="强制重新登录（默认自动探测：已登录就直接签到）")
    ap.add_argument("--spawn", action="store_true",
                    help="连不上时，自动用独立数据目录启动带代理的无头 Chrome 再连接")
    ap.add_argument("--port", type=int, default=0,
                    help=f"指定调试端口（默认优先 {DEFAULT_PORT}）")
    args = ap.parse_args()

    if not Path(CHROME_PATH).exists():
        print(f"[错误] 未找到 Chrome: {CHROME_PATH}")
        return 2

    with sync_playwright() as p:
        browser = None
        spawned_by_us = False

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
            spawn_port = args.port or DEFAULT_PORT
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
                ok = True
            else:
                ok = ensure_logged_in(page, force_login=args.force_login)
            if not ok:
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


if __name__ == "__main__":
    sys.exit(main())
