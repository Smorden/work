#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
咸鱼单机（xianyudanji.gg）每日签到脚本（Playwright + Chrome）

流程：
    1. 连接「正在运行的 Chrome」或自动启动无头 Chrome（--spawn）
    2. 先访问 /user 探测登录态：
       - 已登录（能看到每日签到按钮）→ 直接签到
       - 未登录 → 回首页点右上角「登录」→ 弹出登录浮层 → 填账号密码
         → 拖滑块验证（请按住滑块，拖动到最右边）→ 点「立即登录」
         → 登录后页面不跳转，手动 goto /user
    3. 在 /user 页点击「每日签到」

依赖（仅需装一次）：
    pip install playwright

用法：
    python xianyudanji_checkin.py --spawn      # 自动启动无头 Chrome 并签到（推荐）
    python xianyudanji_checkin.py              # 连接已开的调试 Chrome
    python xianyudanji_checkin.py --skip-login # 已确认登录，直接签到
    python xianyudanji_checkin.py --force-login# 强制重新登录
    python xianyudanji_checkin.py --port 9223  # 指定调试端口
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
USER_DATA_DIR = r"C:\Users\Mickey.Deng\AppData\Local\Google\Chrome\User Data"
PROFILE = "Default"
AUTO_USER_DATA_DIR = r"C:\Users\Mickey.Deng\AppData\Local\ChromeAuto"

BASE_URL = "https://www.xianyudanji.gg/"
HOME_URL = BASE_URL
USER_URL = "https://www.xianyudanji.gg/user"

import os as _os
USERNAME = _os.environ.get("XYDJ_USER", "dmjj88")
PASSWORD = _os.environ.get("XYDJ_PASS", "2223141_Xy")

DEBUG_PORTS = [9222, 9223, 9333, 9229]


# ---------------------------------------------------------------------------
# Chrome 连接
# ---------------------------------------------------------------------------
def probe_debug_port(port):
    try:
        urllib.request.urlopen(
            f"http://127.0.0.1:{port}/json/version", timeout=2
        )
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
    """只杀自动化专用目录（ChromeAuto）的 Chrome 进程，不碰日常 Chrome。"""
    ps_cmd = (
        "Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\" | "
        "Where-Object {$_.CommandLine -like '*ChromeAuto*'} | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, timeout=15,
        )
        time.sleep(2)
        print("  已清理僵死的自动化 Chrome 实例")
    except Exception as e:
        print(f"  [提示] 清理自动化 Chrome 失败（忽略）: {e}")


def spawn_auto_chrome(port):
    """用独立 user-data-dir 启动带调试端口的 Chrome（无头后台运行）。"""
    import subprocess as sp
    print(f"启动自动化 Chrome 实例（无头后台，独立目录 {AUTO_USER_DATA_DIR}）…")
    sp.Popen([
        CHROME_PATH,
        "--headless=new",
        f"--user-data-dir={AUTO_USER_DATA_DIR}",
        f"--remote-debugging-port={port}",
        "--window-size=1280,900",
        "about:blank",
    ])
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
    print("直接运行 python xianyudanji_checkin.py --spawn 即可自动拉起无头 Chrome。")
    print()


# ---------------------------------------------------------------------------
# 页面操作
# ---------------------------------------------------------------------------
def is_logged_in(page):
    """在 /user 页判断是否已登录：能看到「每日签到」按钮即为已登录。"""
    try:
        cnt = page.locator('text=每日签到').count()
        return cnt > 0
    except Exception:
        return False


def fill_login_form(page):
    """在登录浮层内填充账号密码（form.ajax-signup-form，取可见实例）。返回 (user_ok, pwd_ok)。"""
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


def drag_slider(page):
    """拖动 rsc-captcha 滑块（取可见实例，注册浮层里可能还藏着一份）。

    用多段带抖动的移动模拟真人拖拽。
    返回 True 表示完成了拖拽动作（是否验证通过由 slider_passed() 判断）。
    """
    try:
        handle = None
        hloc = page.locator('.rsc-slider')
        for i in range(min(hloc.count(), 5)):
            el = hloc.nth(i)
            if el.is_visible():
                handle = el
                break
        if handle is None:
            print("  [警告] 没有可见的滑块把手")
            return False

        # 轨道取把手所在的最近 .rsc-container
        container = handle.locator('xpath=ancestor::div[contains(@class,"rsc-container")][1]')
        hbox = handle.bounding_box()
        cbox = container.bounding_box()
        if not hbox or not cbox:
            print("  [警告] 无法获取滑块位置")
            return False

        # 拖到最右边：容器右边 - 把手一半宽（把手右缘贴容器右缘）
        distance = cbox["x"] + cbox["width"] - hbox["width"] / 2 \
            - (hbox["x"] + hbox["width"] / 2)
        start_x = hbox["x"] + hbox["width"] / 2
        start_y = hbox["y"] + hbox["height"] / 2

        print(f"  拖动滑块：距离 {distance:.0f}px")
        page.mouse.move(start_x, start_y)
        page.mouse.down()
        # 分段移动 + 抖动，模拟真人（先快后慢）
        steps = 30
        for i in range(1, steps + 1):
            t = i / steps
            x = start_x + distance * (1 - (1 - t) ** 2)  # ease-out
            y = start_y + (1 if i % 3 == 0 else 0)  # 轻微上下抖动
            page.mouse.move(x, y)
            time.sleep(0.02 + (0.02 if t > 0.8 else 0))
        # 末端小回弹再推到位
        page.mouse.move(start_x + distance - 2, start_y)
        time.sleep(0.1)
        page.mouse.move(start_x + distance, start_y)
        page.mouse.up()
        print("  滑块拖拽完成")
        time.sleep(1)
        return True
    except Exception as e:
        print(f"  [错误] 拖拽滑块失败: {e}")
        return False


def slider_passed(page):
    """判断 rsc-captcha 滑块验证是否通过。

    可靠信号：隐藏字段 input[name="rsc_verified"] 的 value 变为 "1"
    （验证通过时 JS 会写入），rsc_token 同时被填充。
    """
    try:
        return page.evaluate(
            "() => {"
            "  const v = document.querySelector('input[name=\"rsc_verified\"]');"
            "  const t = document.querySelector('input[name=\"rsc_token\"]');"
            "  return !!(v && v.value === '1') || !!(t && t.value && t.value.length > 5);"
            "}"
        )
    except Exception:
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
            for i in range(min(n, 8)):
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


def do_login(page):
    """首页点「登录」→ 浮层填表 → 拖滑块 → 立即登录。"""
    print("== 登录 ==")
    page.goto(HOME_URL, timeout=60000)
    time.sleep(3)

    # 点右上角「登录」按钮，弹出登录浮层。
    # 注意：页面 DOM 里可能藏着不可见的登录浮层（含「登录您的账户」等文字），
    # 所以用 click_first_visible 只点可见的入口。
    print("点击右上角「登录」…")
    entry_selectors = [
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
        # 等页面动态渲染完再试一轮
        time.sleep(3)
    if entry is None:
        print("  [错误] 找不到可见的登录入口")
        page.screenshot(path="xydj_login_entry_error.png")
        return False
    print("  已点击登录入口")
    time.sleep(1.5)

    print("填充账号密码 …")
    user_ok, pwd_ok = fill_login_form(page)
    if not (user_ok and pwd_ok):
        print("  [提示] 填充不完整，仍尝试继续 …")

    print("处理滑块验证 …")
    drag_ok = drag_slider(page)
    if drag_ok:
        # 等验证通过，最多 10s；不过则再拖一次
        for _ in range(10):
            if slider_passed(page):
                print("  滑块验证通过")
                break
            time.sleep(1)
        else:
            print("  [提示] 首次拖拽后未确认通过，再拖一次 …")
            drag_slider(page)
            time.sleep(2)
    else:
        print("  [警告] 滑块拖拽未完成，登录可能失败")

    print("点击「立即登录」…")
    # 滑块验证确认是异步的（rsc_verified=1 到按钮解锁有延迟），轮询等解锁后只点一次
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
                    break  # 只处理第一个可见实例
        except Exception:
            pass
        if clicked:
            break
        time.sleep(1)
    if not clicked:
        print("  [错误] 登录按钮 15s 内未解锁或点击失败")
        page.screenshot(path="xydj_login_error.png")
        return False

    # 登录成功的信号（实测两种都会出现）：
    #   a) 浮层关闭（密码框不可见）
    #   b) 页面发生导航/刷新（表单提交后跳转）
    # 轮询 20s，任一出现即认为登录提交成功
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
        print("  [警告] 20s 后浮层仍在，登录可能失败（滑块未过/密码错）")
        page.screenshot(path="xydj_login_failed.png")
        # 不直接失败，继续去 /user 看真实状态（do_signin 里会再探测）
    return True


def do_signin(page):
    print("== 签到 ==")
    page.goto(USER_URL, timeout=60000)
    time.sleep(2)

    # 已签到幂等：按钮文本变成「已签到」之类
    try:
        already = page.evaluate(
            "() => {"
            "  const t = document.body.innerText;"
            "  return t.includes('已签到') && !t.includes('每日签到');"
            "}"
        )
        if already:
            print("  今日已签到，无需重复签到 [OK]")
            return True
    except Exception:
        pass

    print("点击「每日签到」…")
    try:
        btn = page.locator('text=每日签到').first
        btn.wait_for(state="visible", timeout=10000)
        btn.scroll_into_view_if_needed()
        btn.click(timeout=10000)
        print("  已点击签到按钮")
    except Exception as e:
        print(f"  [错误] 签到失败: {e}")
        page.screenshot(path="xydj_signin_error.png")
        return False

    time.sleep(3)
    print("签到完成 [OK]")
    return True


def ensure_logged_in(page, force_login=False):
    """先访问 /user 探测登录态，未登录才走登录流程。"""
    print("== 检查登录状态 ==")
    page.goto(USER_URL, timeout=60000)
    time.sleep(2)

    if not force_login and is_logged_in(page):
        print("  已登录（会话有效），跳过登录，直接签到")
        return True
    print("  未登录，开始登录流程 …")
    return do_login(page)


def main():
    ap = argparse.ArgumentParser(description="咸鱼单机 每日签到")
    ap.add_argument("--skip-login", action="store_true",
                    help="跳过登录检查，直接签到（已确认登录时用）")
    ap.add_argument("--force-login", action="store_true",
                    help="强制重新登录（默认自动探测：已登录就直接签到）")
    ap.add_argument("--kill-chrome", action="store_true",
                    help="（仅 --launch 模式）自动关闭正在运行的 Chrome")
    ap.add_argument("--launch", action="store_true",
                    help="回退：Playwright 独立实例（需先关闭 Chrome）")
    ap.add_argument("--spawn", action="store_true",
                    help="连不上时，自动用独立数据目录启动无头 Chrome 再连接")
    ap.add_argument("--port", type=int, default=0,
                    help="指定调试端口（默认自动探测 9222/9223/9333/9229）")
    args = ap.parse_args()

    if not Path(CHROME_PATH).exists():
        print(f"[错误] 未找到 Chrome: {CHROME_PATH}")
        return 2

    with sync_playwright() as p:
        browser = None
        context = None

        if args.launch:
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
            browser, port = connect_via_cdp(p, args.port)
            if browser is None and args.spawn:
                spawn_port = args.port or 9222
                # 连不上可能是旧实例僵死（端口在听但 CDP 无响应），先清理再拉新的
                kill_auto_chrome()
                if spawn_auto_chrome(spawn_port):
                    browser, port = connect_via_cdp(p, spawn_port)
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
            if browser is not None:
                browser.close()
            elif context is not None:
                context.close()


def chrome_running():
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


if __name__ == "__main__":
    sys.exit(main())
