# -*- coding: utf-8 -*-
"""
威科夫终端读盘室 — Textual TUI。

全屏布局：上方可滚动聊天区 + 下方固定输入框。
"""
from __future__ import annotations

import json
import time
from collections import deque
from typing import Any

from rich.markdown import Markdown
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Input, OptionList, RichLog, Static
from textual.widgets.option_list import Option


# ---------------------------------------------------------------------------
# Widget
# ---------------------------------------------------------------------------

def _estimate_tokens(messages: list[dict]) -> int:
    """粗略估算消息列表的 token 数（中文~2字/token，英文~4字符/token）。"""
    total = 0
    for m in messages:
        content = m.get("content", "")
        if isinstance(content, str):
            total += max(len(content) // 2, len(content.encode("utf-8")) // 3)
        # tool_calls 参数也占 token
        for tc in m.get("tool_calls", []):
            args_str = json.dumps(tc.get("args", {}), ensure_ascii=False)
            total += len(args_str) // 3
    return total


_COMPACTION_PROMPT = """请将以下对话历史总结为简洁的上下文摘要，保留关键信息：
1. 用户的目标和意图
2. 已完成的操作和结果
3. 重要的数据发现（股票代码、价格、信号等）
4. 未完成的任务

用中文输出，控制在 500 字以内。只输出摘要，不要其他内容。"""


def _pop_lines(log_widget, n: int) -> None:
    """从 RichLog 底部移除 n 行 strips。"""
    from textual.geometry import Size
    if n > 0 and len(log_widget.lines) >= n:
        del log_widget.lines[-n:]
        log_widget.virtual_size = Size(
            log_widget._widest_line_width, len(log_widget.lines)
        )
        log_widget.refresh()


class ChatLog(RichLog):
    DEFAULT_CSS = """
    ChatLog {
        background: $surface;
        scrollbar-size: 1 1;
    }
    """


class StatusBar(Static):
    DEFAULT_CSS = """
    StatusBar {
        dock: top;
        height: 1;
        background: $primary-background;
        color: $text-muted;
        padding: 0 1;
    }
    """


class FloatingSelector(OptionList):
    """临时浮层选择器 — 上下键选择，Enter 确认，Esc 取消。"""
    DEFAULT_CSS = """
    FloatingSelector {
        dock: bottom;
        height: auto;
        max-height: 8;
        margin: 0 1;
        border: round $accent;
        background: $surface;
    }
    """

    def __init__(self, options: list[tuple[str, str]], callback_id: str, **kwargs):
        """options: [(value, label), ...], callback_id: 回调标识。"""
        self._values = [v for v, _ in options]
        self._callback_id = callback_id
        super().__init__(*[Option(label, id=val) for val, label in options], **kwargs)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        value = self._values[event.option_index]
        self.app._on_selector_choice(self._callback_id, value)

    def on_key(self, event) -> None:
        if event.key == "escape":
            self.app._on_selector_choice(self._callback_id, None)


_SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


# ---------------------------------------------------------------------------
# 交互式输入状态机（/login, /model）
# ---------------------------------------------------------------------------

class _InputState:
    """管理多步交互式输入流程。"""
    NONE = "none"
    LOGIN_EMAIL = "login_email"
    LOGIN_PASSWORD = "login_password"
    MODEL_ID = "model_id"
    MODEL_PROVIDER = "model_provider"
    MODEL_KEY = "model_key"
    MODEL_NAME = "model_name"
    MODEL_URL = "model_url"


# ---------------------------------------------------------------------------
# 主应用
# ---------------------------------------------------------------------------

class WyckoffTUI(App):
    """威科夫终端读盘室。"""

    TITLE = "Wyckoff 读盘室"
    CSS = """
    Screen {
        layout: vertical;
    }
    #chat-log {
        height: 1fr;
        border: round $primary;
        margin: 0 1;
    }
    #chat-input {
        dock: bottom;
        margin: 0 1 0 1;
    }
    """

    ENABLE_COMMAND_PALETTE = True
    COMMAND_PALETTE_BINDING = "ctrl+p"
    COMMANDS = set()  # will be populated below after class definition

    BINDINGS = [
        Binding("ctrl+c", "smart_copy", show=False, priority=True),
        Binding("ctrl+q", "quit", "退出", show=False),
        Binding("ctrl+n", "new_chat", "新对话"),
        Binding("ctrl+l", "clear_chat", "清屏"),
    ]

    def __init__(
        self,
        provider: Any = None,
        tools: Any = None,
        state: dict | None = None,
        system_prompt: str = "",
        session_expired: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._provider = provider
        self._tools = tools
        self._state = state or {}
        self._system_prompt = system_prompt
        self._session_expired = session_expired
        self._messages: list[dict] = []
        self._session_tokens = {"input": 0, "output": 0, "rounds": 0}
        self._busy = False
        self._queue: deque[str] = deque()
        # 后台任务管理
        from cli.background import BackgroundTaskManager
        self._bg_manager = BackgroundTaskManager()
        if self._tools:
            self._tools.set_background_manager(self._bg_manager, self._on_bg_complete)
        # 交互式输入状态
        self._input_mode = _InputState.NONE
        self._input_buf: dict[str, str] = {}

    def compose(self) -> ComposeResult:
        yield StatusBar(self._build_status_text(), id="status-bar")
        yield ChatLog(id="chat-log", highlight=True, markup=True, wrap=True)
        yield Input(placeholder="问我关于股票的任何问题... (/help 查看命令)", id="chat-input")

    def on_mount(self) -> None:
        # 加载保存的主题
        try:
            from cli.auth import load_config
            saved_theme = load_config().get("theme", "")
            if saved_theme and saved_theme in self.available_themes:
                self.theme = saved_theme
        except Exception:
            pass

        log = self.query_one("#chat-log", ChatLog)
        log.write(Text.from_markup(
            "[bold]Wyckoff 读盘室[/bold]\n"
            "[dim]直接输入问题开始对话  ·  /help 查看命令  ·  Ctrl+P 命令面板  ·  Ctrl+C 复制/退出[/dim]\n"
        ))
        if not self._provider:
            log.write(Text.from_markup(
                "[yellow]⚠ 未配置模型，请输入 /model add 添加[/yellow]\n"
            ))
        if self._session_expired:
            log.write(Text.from_markup(
                "[yellow]⚠ 登录已过期，请输入 /login 重新登录[/yellow]\n"
            ))
        self.query_one("#chat-input", Input).focus()

    def _build_status_text(self) -> str:
        parts = ["Wyckoff CLI"]
        prov = self._state.get("provider_name", "")
        model = self._state.get("model", "")
        if prov and model:
            parts.append(f"{prov}:{model}")
        email = self._tools.state.get("email", "") if self._tools else ""
        parts.append(email or "未登录")
        t = self._session_tokens
        if t["rounds"] > 0:
            parts.append(f"Token: {t['input']+t['output']:,}")
        return " · ".join(parts)

    def _update_status(self) -> None:
        self.query_one("#status-bar", StatusBar).update(self._build_status_text())

    # ----- 快捷键动作 -----

    def action_smart_copy(self) -> None:
        """Ctrl+C: 有选中文本 → 复制；无选中 → 退出。"""
        text = self.screen.get_selected_text()
        if text:
            self.copy_to_clipboard(text)
            self.screen.clear_selection()
            self.notify("已复制", timeout=1)
        else:
            self.exit()

    def action_switch_model(self) -> None:
        self._switch_model_selector()

    def action_list_models(self) -> None:
        self._list_models()

    def action_add_model(self) -> None:
        self._start_model_add()

    def action_start_login(self) -> None:
        self._start_login()

    def action_do_logout(self) -> None:
        self._do_logout()

    def action_show_token(self) -> None:
        log = self.query_one("#chat-log", ChatLog)
        t = self._session_tokens
        if t["rounds"] == 0:
            log.write(Text.from_markup("[dim]本次会话尚无 Token 记录[/dim]"))
        else:
            log.write(Text.from_markup(
                f"\n[bold]Token 用量[/bold]  "
                f"输入: {t['input']:,}  输出: {t['output']:,}  "
                f"合计: {t['input']+t['output']:,}  轮次: {t['rounds']}"
            ))

    def action_switch_theme(self) -> None:
        """打开主题切换器并保存选择。"""
        self.action_change_theme()

    def watch_theme(self, new_theme: str) -> None:
        """主题变化时自动保存。"""
        try:
            from cli.auth import save_config_key
            save_config_key("theme", new_theme)
        except Exception:
            pass

    # ----- Spinner（ChatLog 底部边框） -----

    def _start_spinner(self, label: str = "thinking") -> None:
        self._spinner_label = label
        self._spinner_idx = 0
        log = self.query_one("#chat-log", ChatLog)
        log.border_subtitle = f"{_SPINNER[0]} {label}"
        if not hasattr(self, "_spinner_timer") or self._spinner_timer is None:
            self._spinner_timer = self.set_interval(0.08, self._tick_spinner)

    def _stop_spinner(self) -> None:
        if hasattr(self, "_spinner_timer") and self._spinner_timer is not None:
            self._spinner_timer.stop()
            self._spinner_timer = None
        self.query_one("#chat-log", ChatLog).border_subtitle = ""

    def _tick_spinner(self) -> None:
        self._spinner_idx = (self._spinner_idx + 1) % len(_SPINNER)
        self.query_one("#chat-log", ChatLog).border_subtitle = (
            f"{_SPINNER[self._spinner_idx]} {self._spinner_label}"
        )

    # ----- 输入处理 -----

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        inp = event.input
        inp.clear()

        # 交互式多步输入
        if self._input_mode != _InputState.NONE:
            self._handle_interactive_input(text)
            return

        if not text:
            return

        log = self.query_one("#chat-log", ChatLog)

        # 斜杠命令
        if text.startswith("/"):
            self._handle_command(text)
            return

        if not self._provider:
            log.write(Text.from_markup("[yellow]⚠ 未配置模型，请先输入 /model add[/yellow]"))
            return

        if self._busy:
            self._queue.append(text)
            return

        # 用户消息
        self._send_message(text)

    def _send_message(self, text: str) -> None:
        log = self.query_one("#chat-log", ChatLog)
        log.write(Text(""))
        log.write(Text.from_markup(f"[bold cyan]❯[/bold cyan] {text}"))
        self._messages.append({"role": "user", "content": text})
        self._start_spinner("thinking")
        self._run_agent()

    # ----- 斜杠命令 -----

    def _handle_command(self, raw: str) -> None:
        cmd = raw.lower().split()[0]
        log = self.query_one("#chat-log", ChatLog)

        if cmd in ("/quit", "/exit", "/q"):
            self.exit()
        elif cmd == "/clear":
            self.action_clear_chat()
        elif cmd == "/new":
            self.action_new_chat()
        elif cmd == "/help":
            log.write(Text.from_markup(
                "\n[bold]可用命令[/bold]\n"
                "  /model   — 切换模型（list/add/rm/default）\n"
                "  /login   — 登录\n"
                "  /logout  — 退出登录\n"
                "  /token   — Token 用量\n"
                "  /new     — 新对话 (Ctrl+N)\n"
                "  /clear   — 清屏 (Ctrl+L)\n"
                "  /quit    — 退出 (Ctrl+Q)\n"
                "\n[bold]快捷键[/bold]\n"
                "  Ctrl+P   — 命令面板\n"
                "  Ctrl+C   — 复制选中文本 / 退出\n"
                "  Ctrl+N   — 新对话\n"
                "  Ctrl+L   — 清屏\n"
                "  鼠标拖选  — 选择文本\n"
            ))
        elif cmd == "/token":
            t = self._session_tokens
            if t["rounds"] == 0:
                log.write(Text.from_markup("[dim]本次会话尚无 Token 记录[/dim]"))
            else:
                log.write(Text.from_markup(
                    f"\n[bold]Token 用量[/bold]  "
                    f"输入: {t['input']:,}  输出: {t['output']:,}  "
                    f"合计: {t['input']+t['output']:,}  轮次: {t['rounds']}"
                ))
        elif cmd == "/login":
            self._start_login()
        elif cmd == "/logout":
            self._do_logout()
        elif cmd == "/model":
            parts = raw.strip().split()
            if len(parts) == 1:
                self._switch_model_selector()
            elif parts[1] == "list":
                self._list_models()
            elif parts[1] == "add":
                self._start_model_add()
            elif parts[1] == "rm" and len(parts) >= 3:
                self._remove_model(parts[2])
            elif parts[1] == "default" and len(parts) >= 3:
                self._set_default_model(parts[2])
            else:
                log.write(Text.from_markup(
                    "[dim]/model 用法: /model (切换) | /model list | /model add | /model rm <id> | /model default <id>[/dim]"
                ))
        else:
            log.write(Text.from_markup(f"[red]未知命令: {raw}[/red]，/help 查看"))

    # ----- /login 交互 -----

    def _start_login(self) -> None:
        log = self.query_one("#chat-log", ChatLog)
        inp = self.query_one("#chat-input", Input)
        log.write(Text.from_markup("\n[bold]登录[/bold]"))
        log.write(Text.from_markup("  输入邮箱（留空取消）："))
        inp.placeholder = "邮箱..."
        self._input_mode = _InputState.LOGIN_EMAIL
        self._input_buf = {}

    def _do_logout(self) -> None:
        log = self.query_one("#chat-log", ChatLog)
        if self._tools:
            try:
                from cli.auth import logout
                logout()
            except Exception:
                pass
            self._tools.state.update({"user_id": "", "email": "", "access_token": "", "refresh_token": ""})
        log.write(Text.from_markup("[green]已退出登录[/green]"))
        self._update_status()

    # ----- /model 交互 -----

    def _list_models(self) -> None:
        log = self.query_one("#chat-log", ChatLog)
        from cli.auth import load_model_configs, load_default_model_id
        configs = load_model_configs()
        default_id = load_default_model_id()
        if not configs:
            log.write(Text.from_markup("[dim]尚无模型配置，使用 /model add 添加[/dim]"))
            return
        log.write(Text.from_markup("\n[bold]已配置模型[/bold]"))
        for c in configs:
            mark = " [green]⭐ 默认[/green]" if c["id"] == default_id else ""
            log.write(Text.from_markup(
                f"  [bold]{c['id']}[/bold] — {c['provider_name']}/{c.get('model', '?')}{mark}"
            ))
        log.write(Text.from_markup(
            "[dim]  /model add | /model rm <id> | /model default <id>[/dim]"
        ))

    def _remove_model(self, model_id: str) -> None:
        log = self.query_one("#chat-log", ChatLog)
        from cli.auth import remove_model_entry
        if remove_model_entry(model_id):
            log.write(Text.from_markup(f"  [green]✓ 已删除 {model_id}[/green]"))
            self._rebuild_provider()
        else:
            log.write(Text.from_markup(f"  [red]无法删除（至少保留一个模型）[/red]"))

    def _set_default_model(self, model_id: str) -> None:
        log = self.query_one("#chat-log", ChatLog)
        from cli.auth import load_model_configs, set_default_model
        configs = load_model_configs()
        if not any(c["id"] == model_id for c in configs):
            log.write(Text.from_markup(f"  [red]未找到: {model_id}[/red]"))
            return
        set_default_model(model_id)
        log.write(Text.from_markup(f"  [green]✓ 默认模型已设为 {model_id}[/green]"))
        self._rebuild_provider()

    def _rebuild_provider(self) -> None:
        from cli.auth import load_model_configs, load_default_model_id
        configs = load_model_configs()
        default_id = load_default_model_id()
        if not configs:
            self._provider = None
            return
        default_cfg = next((c for c in configs if c["id"] == default_id), configs[0])
        if len(configs) == 1:
            from cli.__main__ import _create_provider
            provider, err = _create_provider(
                default_cfg["provider_name"], default_cfg["api_key"],
                default_cfg.get("model", ""), default_cfg.get("base_url", ""),
            )
            if not err:
                self._provider = provider
        else:
            from cli.providers.fallback import FallbackProvider
            self._provider = FallbackProvider(configs, default_id)
        self._state.update(default_cfg)
        self._update_status()

    def _show_selector(self, options: list[tuple[str, str]], callback_id: str) -> None:
        """显示浮层选择器。options: [(value, label), ...]"""
        # 移除已有的 selector
        try:
            old = self.query_one("#floating-selector", FloatingSelector)
            old.remove()
        except Exception:
            pass
        selector = FloatingSelector(options, callback_id, id="floating-selector")
        self.mount(selector, before="#chat-input")
        selector.focus()

    def _dismiss_selector(self) -> None:
        try:
            self.query_one("#floating-selector", FloatingSelector).remove()
        except Exception:
            pass
        self.query_one("#chat-input", Input).focus()

    def _on_selector_choice(self, callback_id: str, value: str | None) -> None:
        """选择器回调。"""
        self._dismiss_selector()
        log = self.query_one("#chat-log", ChatLog)
        inp = self.query_one("#chat-input", Input)

        if value is None:
            log.write(Text.from_markup("[dim]已取消[/dim]"))
            self._input_mode = _InputState.NONE
            inp.placeholder = "问我关于股票的任何问题... (/help 查看命令)"
            return

        if callback_id == "model_switch":
            self._set_default_model(value)

        elif callback_id == "model_provider":
            self._input_buf["provider"] = value
            log.write(Text.from_markup(f"  供应商: {value}"))
            log.write(Text.from_markup("  输入 API Key："))
            inp.placeholder = "API Key..."
            inp.password = True
            self._input_mode = _InputState.MODEL_KEY

    def _switch_model_selector(self) -> None:
        """弹出浮层选择器切换当前模型。"""
        from cli.auth import load_model_configs, load_default_model_id
        configs = load_model_configs()
        if not configs:
            log = self.query_one("#chat-log", ChatLog)
            log.write(Text.from_markup("[dim]尚无模型配置，使用 /model add 添加[/dim]"))
            return
        default_id = load_default_model_id()
        options = []
        for c in configs:
            mark = " ⭐" if c["id"] == default_id else ""
            label = f"{c['id']} ({c.get('model', '?')}){mark}"
            options.append((c["id"], label))
        self._show_selector(options, "model_switch")

    def _start_model_add(self) -> None:
        log = self.query_one("#chat-log", ChatLog)
        inp = self.query_one("#chat-input", Input)
        log.write(Text.from_markup(
            "\n[bold]添加模型[/bold]\n"
            "  输入别名（如 gemini, longcat, deepseek）："
        ))
        inp.placeholder = "模型别名..."
        self._input_mode = _InputState.MODEL_ID
        self._input_buf = {}

    # ----- 交互式输入状态机 -----

    def _handle_interactive_input(self, text: str) -> None:
        log = self.query_one("#chat-log", ChatLog)
        inp = self.query_one("#chat-input", Input)
        mode = self._input_mode

        # MODEL_NAME 和 MODEL_URL 留空表示用默认值，不取消
        if not text and mode not in (_InputState.MODEL_NAME, _InputState.MODEL_URL, _InputState.MODEL_ID):
            log.write(Text.from_markup("[dim]已取消[/dim]"))
            self._input_mode = _InputState.NONE
            inp.placeholder = "问我关于股票的任何问题... (/help 查看命令)"
            return

        if mode == _InputState.LOGIN_EMAIL:
            self._input_buf["email"] = text
            log.write(Text.from_markup(f"  邮箱: {text}"))
            log.write(Text.from_markup("  输入密码："))
            inp.placeholder = "密码..."
            inp.password = True
            self._input_mode = _InputState.LOGIN_PASSWORD

        elif mode == _InputState.LOGIN_PASSWORD:
            inp.password = False
            log.write(Text.from_markup("  密码: ****"))
            self._input_mode = _InputState.NONE
            inp.placeholder = "问我关于股票的任何问题... (/help 查看命令)"
            # 执行登录
            try:
                from cli.auth import login
                session = login(self._input_buf["email"], text)
                self._tools.state.update({
                    "user_id": session["user_id"],
                    "email": session["email"],
                    "access_token": session.get("access_token", ""),
                    "refresh_token": session.get("refresh_token", ""),
                })
                from core.stock_cache import set_cli_tokens
                set_cli_tokens(session.get("access_token", ""), session.get("refresh_token", ""))
                log.write(Text.from_markup(f"  [green]✓ 登录成功 ({session['email']})[/green]"))
                self._update_status()
            except Exception as e:
                err = str(e)
                if "Invalid login" in err or "invalid" in err.lower():
                    log.write(Text.from_markup("  [red]邮箱或密码错误，请重新输入[/red]"))
                else:
                    log.write(Text.from_markup(f"  [red]登录失败: {err}，请重新输入[/red]"))
                self._start_login()

        elif mode == _InputState.MODEL_ID:
            model_id = text.strip().lower() if text.strip() else ""
            if not model_id:
                log.write(Text.from_markup("[dim]已取消[/dim]"))
                self._input_mode = _InputState.NONE
                inp.placeholder = "问我关于股票的任何问题... (/help 查看命令)"
                return
            self._input_buf["id"] = model_id
            log.write(Text.from_markup(f"  别名: {model_id}"))
            log.write(Text.from_markup("  选择供应商（↑↓ 选择，Enter 确认，Esc 取消）："))
            self._input_mode = _InputState.MODEL_PROVIDER
            self._show_selector([
                ("gemini", "Gemini (Google)"),
                ("openai", "OpenAI / 兼容接口 (LongCat, DeepSeek, Qwen...)"),
                ("claude", "Claude (Anthropic)"),
            ], "model_provider")
            return  # 等 selector 回调

        elif mode == _InputState.MODEL_PROVIDER:
            # 文本输入兜底（selector 取消后手动输入）
            prov = text.strip().lower()
            if prov not in ("gemini", "openai", "claude"):
                log.write(Text.from_markup(f"  [red]不支持: {prov}[/red]"))
                self._input_mode = _InputState.NONE
                inp.placeholder = "问我关于股票的任何问题... (/help 查看命令)"
                return
            self._input_buf["provider"] = prov
            log.write(Text.from_markup(f"  供应商: {prov}"))
            log.write(Text.from_markup("  输入 API Key："))
            inp.placeholder = "API Key..."
            inp.password = True
            self._input_mode = _InputState.MODEL_KEY

        elif mode == _InputState.MODEL_KEY:
            inp.password = False
            self._input_buf["api_key"] = text
            log.write(Text.from_markup("  API Key: ****"))
            default_models = {"gemini": "gemini-2.0-flash", "openai": "gpt-4o", "claude": "claude-sonnet-4-20250514"}
            default = default_models.get(self._input_buf["provider"], "")
            log.write(Text.from_markup(f"  输入模型名（留空使用 {default}）："))
            inp.placeholder = f"模型名，默认 {default}"
            self._input_mode = _InputState.MODEL_NAME

        elif mode == _InputState.MODEL_NAME:
            default_models = {"gemini": "gemini-2.0-flash", "openai": "gpt-4o", "claude": "claude-sonnet-4-20250514"}
            model = text or default_models.get(self._input_buf["provider"], "")
            self._input_buf["model"] = model
            log.write(Text.from_markup(f"  模型: {model}"))
            log.write(Text.from_markup("  输入 Base URL（留空使用默认）："))
            inp.placeholder = "Base URL（可选）"
            self._input_mode = _InputState.MODEL_URL

        elif mode == _InputState.MODEL_URL:
            self._input_buf["base_url"] = text
            self._input_mode = _InputState.NONE
            inp.placeholder = "问我关于股票的任何问题... (/help 查看命令)"
            # 创建 provider
            self._apply_model_config()

    def _apply_model_config(self) -> None:
        log = self.query_one("#chat-log", ChatLog)
        buf = self._input_buf
        try:
            entry = {
                "id": buf.get("id", buf["provider"]),
                "provider_name": buf["provider"],
                "api_key": buf["api_key"],
                "model": buf.get("model", ""),
                "base_url": buf.get("base_url", ""),
            }
            from cli.auth import save_model_entry, load_model_configs, set_default_model
            save_model_entry(entry)
            # 首条模型或新添加的设为默认
            if len(load_model_configs()) == 1:
                set_default_model(entry["id"])
            import os
            env_key = {"gemini": "GEMINI_API_KEY", "claude": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}.get(buf["provider"])
            if env_key:
                os.environ[env_key] = buf["api_key"]
            self._rebuild_provider()
            log.write(Text.from_markup(f"  [green]✓ 已添加 {entry['id']} ({self._provider.name if self._provider else '?'})[/green]"))
        except Exception as e:
            log.write(Text.from_markup(f"  [red]配置失败: {e}[/red]"))

    # ----- Agent 执行（后台 Worker）-----

    @work(thread=True, exclusive=True)
    def _run_agent(self) -> None:
        self._busy = True
        log = self.query_one("#chat-log", ChatLog)

        def _write(renderable):
            self.call_from_thread(log.write, renderable)

        def _scroll():
            self.call_from_thread(log.scroll_end, animate=False)

        def _spinner_start(label="思考中"):
            self.call_from_thread(self._start_spinner, label)

        def _spinner_stop():
            self.call_from_thread(self._stop_spinner)

        total_input = 0
        total_output = 0
        t_start = time.monotonic()
        _recent_calls: list[tuple[str, str]] = []  # doom-loop: (name, args_hash)

        try:
            from cli.agent import MAX_TOOL_ROUNDS

            _COMPACT_THRESHOLD = 12000  # ~12K tokens 触发压缩
            _TAIL_KEEP = 4  # 保留最近 4 条消息原文

            for round_idx in range(MAX_TOOL_ROUNDS):
                # ── Context compaction ──
                if len(self._messages) > _TAIL_KEEP + 2 and _estimate_tokens(self._messages) > _COMPACT_THRESHOLD:
                    _spinner_start("压缩上下文")
                    head = self._messages[:-_TAIL_KEEP]
                    tail = self._messages[-_TAIL_KEEP:]
                    head_text = "\n".join(
                        f"[{m['role']}] {m.get('content', '') or json.dumps(m.get('tool_calls', []), ensure_ascii=False)[:200]}"
                        for m in head
                    )
                    try:
                        summary_chunks = list(self._provider.chat_stream(
                            [{"role": "user", "content": head_text}],
                            [],
                            _COMPACTION_PROMPT,
                        ))
                        summary = "".join(c.get("text", "") for c in summary_chunks if c["type"] == "text_delta")
                        if summary:
                            self._messages = [{"role": "user", "content": f"[对话摘要]\n{summary}"},
                                              {"role": "assistant", "content": "好的，我已了解之前的对话上下文，请继续。"}] + tail
                            _write(Text.from_markup(
                                f"  [dim]📦 上下文已压缩（{len(head)}条→摘要，保留最近{_TAIL_KEEP}条）[/dim]"
                            ))
                    except Exception:
                        pass  # 压缩失败不影响主流程
                    _spinner_stop()

                text_buf = ""
                thinking_buf = ""
                tool_calls = None
                round_usage = {}
                _stream_write_count = 0
                _streaming_started = False
                _stream_line_buf = ""

                if round_idx > 0:
                    _spinner_start()

                # ── 带重试的 streaming ──
                _MAX_STREAM_RETRIES = 3
                _stream = None
                for _retry in range(_MAX_STREAM_RETRIES):
                    try:
                        _stream = self._provider.chat_stream(
                            self._messages, self._tools.schemas(), self._system_prompt
                        )
                        break
                    except Exception as _stream_err:
                        from cli.providers.fallback import _is_retriable
                        if not _is_retriable(_stream_err) or _retry == _MAX_STREAM_RETRIES - 1:
                            raise
                        _delay = min(2 ** (_retry + 1), 30)
                        _write(Text.from_markup(
                            f"  [yellow]⚡ 连接失败，{_delay}s 后重试（{_retry+1}/{_MAX_STREAM_RETRIES}）[/yellow]"
                        ))
                        _scroll()
                        time.sleep(_delay)

                for chunk in _stream:
                    if chunk["type"] == "thinking_delta":
                        thinking_buf += chunk["text"]

                    elif chunk["type"] == "text_delta":
                        text_buf += chunk["text"]
                        _stream_line_buf += chunk["text"]
                        if not _streaming_started:
                            _spinner_stop()
                            _write(Text.from_markup("  [dim]───[/dim]"))
                            _streaming_started = True
                        while "\n" in _stream_line_buf:
                            line, _stream_line_buf = _stream_line_buf.split("\n", 1)
                            _write(Text(line))
                            _stream_write_count += 1
                            _scroll()

                    elif chunk["type"] == "tool_calls":
                        tool_calls = chunk["tool_calls"]
                        partial = chunk.get("text", "")
                        if partial and not text_buf:
                            text_buf = partial

                    elif chunk["type"] == "usage":
                        round_usage = chunk

                _spinner_stop()

                # 刷出流式行缓冲剩余
                if _stream_line_buf:
                    _write(Text(_stream_line_buf))
                    _stream_write_count += 1
                    _stream_line_buf = ""
                    _scroll()

                total_input += round_usage.get("input_tokens", 0)
                total_output += round_usage.get("output_tokens", 0)

                # ── Fallback 通知 ──
                fb_msg = getattr(self._provider, "last_fallback_msg", None)
                if fb_msg:
                    _write(Text.from_markup(f"  [yellow]⚡ {fb_msg}[/yellow]"))
                    self._provider.last_fallback_msg = None

                # ── Thinking 摘要（折叠为一行） ──
                if thinking_buf:
                    preview = thinking_buf.strip().replace("\n", " ")
                    if len(preview) > 80:
                        preview = preview[:80] + "…"
                    _write(Text.from_markup(
                        f"  [italic magenta]💭 {preview}[/italic magenta]  [dim]({len(thinking_buf)} 字)[/dim]"
                    ))

                # ── 工具调用 ──
                if tool_calls:
                    # 清掉流式阶段的部分文本（LLM 先输出文字后又调工具的情况）
                    if _streaming_started and _stream_write_count > 0:
                        self.call_from_thread(_pop_lines, log, _stream_write_count + 1)  # +1 for separator
                        _stream_write_count = 0
                        _streaming_started = False

                    assistant_msg: dict[str, Any] = {"role": "assistant", "tool_calls": tool_calls}
                    if text_buf:
                        assistant_msg["content"] = text_buf
                    self._messages.append(assistant_msg)

                    for call in tool_calls:
                        name = call["name"]
                        args = call["args"]
                        call_id = call["id"]
                        display = self._tools.display_name(name)

                        # ── Doom-loop 检测 ──
                        args_hash = hash(json.dumps(args, sort_keys=True, ensure_ascii=False))
                        _recent_calls.append((name, args_hash))
                        if len(_recent_calls) > 6:
                            _recent_calls.pop(0)
                        if _recent_calls.count((name, args_hash)) >= 3:
                            _write(Text.from_markup(
                                f"  [yellow]⚠ 检测到重复调用 {display}，已中止循环[/yellow]"
                            ))
                            self._messages.append({
                                "role": "tool", "tool_call_id": call_id, "name": name,
                                "content": json.dumps({"error": "doom-loop: 同参数重复调用3次，已中止"}, ensure_ascii=False),
                            })
                            tool_calls = None  # 跳出 tool loop, 不 continue 到下一轮
                            break

                        _spinner_start(display)

                        t_tool = time.monotonic()
                        result = self._tools.execute(name, args)
                        elapsed_tool = time.monotonic() - t_tool

                        _spinner_stop()

                        if isinstance(result, dict) and result.get("error"):
                            _write(Text.from_markup(
                                f"  [red]✗ {display}[/red] [dim]{elapsed_tool:.1f}s {str(result['error'])[:80]}[/dim]"
                            ))
                        elif isinstance(result, dict) and result.get("status") == "background":
                            _write(Text.from_markup(
                                f"  [cyan]↗ {display}[/cyan] [dim]已提交后台[/dim]"
                            ))
                        else:
                            _write(Text.from_markup(
                                f"  [green]✓ {display}[/green] [dim]{elapsed_tool:.1f}s[/dim]"
                            ))
                        _scroll()

                        self._messages.append({
                            "role": "tool",
                            "tool_call_id": call_id,
                            "name": name,
                            "content": json.dumps(result, ensure_ascii=False, default=str),
                        })
                    if tool_calls is not None:
                        continue
                    # doom-loop 中止：不再继续下一轮，走到最终输出

                # ── 最终输出 ──
                self._messages.append({"role": "assistant", "content": text_buf})
                if text_buf:
                    if _streaming_started and _stream_write_count > 0:
                        # pop 流式纯文本，替换为 Markdown 格式
                        self.call_from_thread(_pop_lines, log, _stream_write_count)
                    else:
                        _write(Text.from_markup("  [dim]───[/dim]"))
                    _write(Markdown(text_buf))
                    _scroll()

                elapsed = time.monotonic() - t_start
                self._session_tokens["input"] += total_input
                self._session_tokens["output"] += total_output
                self._session_tokens["rounds"] += 1

                usage_parts = []
                if total_input or total_output:
                    usage_parts.append(f"↑{total_input:,} ↓{total_output:,}")
                usage_parts.append(f"{elapsed:.1f}s")
                _write(Text.from_markup(f"  [dim]{' · '.join(usage_parts)}[/dim]"))
                _scroll()
                self.call_from_thread(self._update_status)
                break
            else:
                _write(Text.from_markup("[yellow](工具调用轮次超限)[/yellow]"))

        except Exception as e:
            _spinner_stop()
            err = str(e)
            # 清理 HTML 错误响应，只保留关键信息
            if "<html" in err.lower():
                import re
                title = re.search(r"<title>(.*?)</title>", err, re.IGNORECASE)
                err = title.group(1) if title else "服务端返回 HTML 错误"
            if len(err) > 200:
                err = err[:200] + "..."
            _write(Text.from_markup(f"[red]错误: {err}[/red]"))
            while self._messages and self._messages[-1].get("role") != "user":
                self._messages.pop()
            if self._messages:
                self._messages.pop()

        finally:
            self._busy = False
            if self._queue:
                next_msg = self._queue.popleft()
                self.call_from_thread(self._send_message, next_msg)

    # ----- 后台任务回调 -----

    def _on_bg_complete(self, task_id: str, tool_name: str, result) -> None:
        """后台任务完成，注入结果到消息队列。"""
        from cli.tools import TOOL_DISPLAY_NAMES
        display = TOOL_DISPLAY_NAMES.get(tool_name, tool_name)
        is_error = isinstance(result, dict) and result.get("error")

        log = self.query_one("#chat-log", ChatLog)
        if is_error:
            self.call_from_thread(
                log.write,
                Text.from_markup(f"  [red]✗ 后台任务失败：{display}[/red] [dim]{str(result['error'])[:80]}[/dim]"),
            )
        else:
            self.call_from_thread(
                log.write,
                Text.from_markup(f"  [green]✅ 后台任务完成：{display}[/green]"),
            )

        summary = json.dumps(result, ensure_ascii=False, default=str)
        if len(summary) > 3000:
            summary = summary[:3000] + "..."
        self._queue.append(f"[后台任务完成] {tool_name}: {summary}")
        # 空闲时自动触发
        if not self._busy:
            self.call_from_thread(self._send_message, self._queue.popleft())

    # ----- Actions -----

    def action_clear_chat(self) -> None:
        self.query_one("#chat-log", ChatLog).clear()

    def action_new_chat(self) -> None:
        self._messages.clear()
        self._queue.clear()
        self._session_tokens = {"input": 0, "output": 0, "rounds": 0}
        log = self.query_one("#chat-log", ChatLog)
        log.clear()
        log.write(Text.from_markup("[green]新对话已开始[/green]\n"))
        self._update_status()


# 注册命令面板（class 定义完成后）
try:
    from cli.commands import WyckoffCommands
    WyckoffTUI.COMMANDS = {WyckoffCommands}
except ImportError:
    pass


def _brief_args(args: dict) -> str:
    if not args:
        return ""
    s = ", ".join(f"{k}={v}" for k, v in args.items())
    return s[:60] + ("..." if len(s) > 60 else "")
