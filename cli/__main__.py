# -*- coding: utf-8 -*-
"""
威科夫终端读盘室 — 入口。

用法:
    wyckoff                # 直接启动，在 TUI 内配置模型
    wyckoff update         # 升级到最新版
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

from dotenv import load_dotenv

# 加载 .env（项目根目录）
load_dotenv()

# 抑制 Streamlit 在非 Streamlit 环境下的全部日志
os.environ["STREAMLIT_LOG_LEVEL"] = "error"
import logging as _logging


def _silence_streamlit():
    for name in list(_logging.Logger.manager.loggerDict):
        if name.startswith("streamlit"):
            lg = _logging.getLogger(name)
            lg.handlers.clear()
            lg.setLevel(_logging.CRITICAL)
            lg.propagate = False


try:
    import streamlit  # noqa: F401
except Exception:
    pass
_silence_streamlit()

# CLI 环境：只显示 CRITICAL，不泄漏 traceback 给用户
import warnings as _warnings
_warnings.filterwarnings("ignore", category=DeprecationWarning)
_logging.basicConfig(level=_logging.CRITICAL)


# ---------------------------------------------------------------------------
# Provider 工厂
# ---------------------------------------------------------------------------

def _create_provider(provider_name: str, api_key: str, model: str = "", base_url: str = ""):
    from cli.providers import PROVIDERS
    import inspect

    cls = PROVIDERS.get(provider_name)
    if cls is None:
        install_hints = {
            "gemini": "pip install google-genai",
            "claude": "pip install anthropic",
            "openai": "pip install openai",
        }
        hint = install_hints.get(provider_name, "")
        return None, f"Provider '{provider_name}' 不可用，请先安装依赖：{hint}"

    kwargs = {"api_key": api_key}
    if model:
        kwargs["model"] = model
    if base_url:
        kwargs["base_url"] = base_url

    sig = inspect.signature(cls.__init__)
    kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}

    return cls(**kwargs), None


def _do_update():
    import shutil
    print("正在升级 youngcan-wyckoff-analysis ...")
    pkg = "youngcan-wyckoff-analysis"
    uv = shutil.which("uv")
    if uv:
        cmd = [uv, "pip", "install", "--python", sys.executable, "--upgrade", pkg]
    else:
        cmd = [sys.executable, "-m", "pip", "install", "--upgrade", pkg]
    try:
        subprocess.check_call(cmd)
        print("\n升级完成！请重新运行 wyckoff。")
    except subprocess.CalledProcessError as e:
        print(f"\n升级失败: {e}")
    sys.exit(0)


def _get_version() -> str:
    try:
        from importlib.metadata import version
        return version("youngcan-wyckoff-analysis")
    except Exception:
        return "dev"


def main():
    parser = argparse.ArgumentParser(
        prog="wyckoff",
        description="威科夫终端读盘室 — Wyckoff 量价分析 Agent",
    )
    parser.add_argument("-v", "--version", action="version", version=f"wyckoff {_get_version()}")
    parser.add_argument("-q", "--quiet", action="store_true", help="静默模式，不显示 banner")
    parser.add_argument("--no-color", action="store_true", help="禁用颜色输出")
    parser.add_argument(
        "command", nargs="?", default=None,
        help="子命令: update（升级到最新版）",
    )
    args = parser.parse_args()

    if args.no_color:
        os.environ["NO_COLOR"] = "1"

    if args.command == "update":
        _do_update()
    elif args.command is not None:
        print(f"未知命令: {args.command}")
        print("可用命令: wyckoff update")
        sys.exit(1)

    # UI
    from cli import ui

    # --- Auth + Tools：统一 session state ---
    from cli.tools import ToolRegistry
    tools = ToolRegistry()

    try:
        from cli.auth import restore_session
        session = restore_session()
        if session:
            tools.state.update({
                "user_id": session["user_id"],
                "email": session["email"],
                "access_token": session.get("access_token", ""),
                "refresh_token": session.get("refresh_token", ""),
            })
    except Exception:
        pass

    # 加载系统提示词
    from core.prompts import CHAT_AGENT_SYSTEM_PROMPT
    system_prompt = CHAT_AGENT_SYSTEM_PROMPT

    # Provider 状态
    state = {
        "provider": None,
        "provider_name": "",
        "model": "",
        "api_key": "",
        "base_url": "",
    }

    # --- 恢复模型配置 ---
    try:
        from cli.auth import load_model_config
        saved_config = load_model_config()
        if saved_config and saved_config.get("provider_name") and saved_config.get("api_key"):
            env_key = {"gemini": "GEMINI_API_KEY", "claude": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}.get(saved_config["provider_name"])
            if env_key:
                os.environ[env_key] = saved_config["api_key"]
            provider, err = _create_provider(
                saved_config["provider_name"], saved_config["api_key"],
                saved_config.get("model", ""), saved_config.get("base_url", ""),
            )
            if not err:
                state.update(saved_config)
                state["provider"] = provider
    except Exception:
        pass

    def _ensure_provider() -> bool:
        if state["provider"] is not None:
            return True
        ui.print_info("尚未配置模型，请先运行 /model 设置。")
        return False

    def _do_login():
        """执行登录流程，失败后引导重新输入。"""
        from cli.auth import login
        while True:
            creds = ui.login_prompt()
            if not creds:
                return
            email, password = creds
            try:
                session = login(email, password)
                tools.state.update({
                    "user_id": session["user_id"],
                    "email": session["email"],
                    "access_token": session.get("access_token", ""),
                    "refresh_token": session.get("refresh_token", ""),
                })
                ui.print_info(f"✓ 登录成功 ({session['email']})")
                return
            except Exception as e:
                err_msg = str(e)
                if "Invalid login" in err_msg or "invalid" in err_msg.lower():
                    ui.print_error("邮箱或密码错误，请重新输入。")
                else:
                    ui.print_error(f"登录失败: {err_msg}")
                    return

    def _do_logout():
        """执行登出。"""
        from cli.auth import logout
        logout()
        tools.state.update({
            "user_id": "",
            "email": "",
            "access_token": "",
            "refresh_token": "",
        })
        ui.print_info("已退出登录。")

    # Banner
    if not args.quiet:
        model_hint = f"{state['provider_name']}:{state['model']}" if state["provider"] else ""
        ui.print_banner(email=tools.state.get("email", ""), model=model_hint)

    # 对话历史 & 累计 token 统计
    messages: list[dict] = []
    session_tokens = {"input": 0, "output": 0, "rounds": 0}

    # REPL
    while True:
        user_input = ui.get_input()

        if not user_input:
            continue

        if user_input.startswith("/"):
            cmd = user_input.lower().split()[0]
            if cmd in ("/quit", "/exit", "/q"):
                ui.print_info("再见。")
                break
            elif cmd == "/clear":
                os.system("clear" if os.name != "nt" else "cls")
                model_hint = f"{state['provider_name']}:{state['model']}" if state["provider"] else ""
                ui.print_banner(email=tools.state.get("email", ""), model=model_hint)
            elif cmd == "/new":
                messages.clear()
                ui.print_info("新对话已开始。")
                continue
            elif cmd == "/help":
                ui.print_help()
                continue
            elif cmd == "/login":
                _do_login()
                continue
            elif cmd == "/logout":
                _do_logout()
                continue
            elif cmd == "/token":
                ui.print_token_summary(session_tokens, state.get("model", ""))
                continue
            elif cmd == "/model":
                result = ui.configure_model(state)
                if result:
                    provider, err = _create_provider(
                        result["provider_name"], result["api_key"],
                        result["model"], result["base_url"],
                    )
                    if err:
                        ui.print_error(err)
                    else:
                        state.update(result)
                        state["provider"] = provider
                        ui.print_info(f"已切换到: {provider.name}")
                        # 持久化模型配置
                        from cli.auth import save_model_config
                        save_model_config({
                            "provider_name": result["provider_name"],
                            "api_key": result["api_key"],
                            "model": result["model"],
                            "base_url": result["base_url"],
                        })
                continue
            else:
                ui.print_error(f"未知命令: {user_input}，输入 /help 查看可用命令。")
                continue

        if not _ensure_provider():
            continue

        messages.append({"role": "user", "content": user_input})

        def on_tool_call(name, call_args):
            ui.print_tool_call(name, tools.display_name(name), call_args)

        def on_tool_result(name, result):
            ui.print_tool_result(name, tools.display_name(name), result)

        try:
            from cli.agent import run
            result = run(
                provider=state["provider"],
                tools=tools,
                messages=messages,
                system_prompt=system_prompt,
                on_tool_call=on_tool_call,
                on_tool_result=on_tool_result,
                console=ui.console,
            )
            if not result.get("streamed"):
                ui.print_response(result["text"])
            else:
                ui.console.print()  # 流式后补一个空行
            usage = result.get("usage", {})
            inp, out = usage.get("input_tokens", 0), usage.get("output_tokens", 0)
            session_tokens["input"] += inp
            session_tokens["output"] += out
            session_tokens["rounds"] += 1
            ui.print_usage(inp, out, result.get("elapsed", 0), state.get("model", ""))
        except KeyboardInterrupt:
            ui.print_info("\n已中断。")
            if messages and messages[-1]["role"] == "user":
                messages.pop()
        except Exception as e:
            ui.print_error(f"Agent 错误: {e}")
            # 回滚本轮消息（包括 user + assistant/tool）
            while messages and messages[-1].get("role") != "user":
                messages.pop()
            messages.pop() if messages else None


if __name__ == "__main__":
    main()
