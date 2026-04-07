from __future__ import annotations

from typing import Any

import streamlit as st

from app.agent_jobs import (
    agent_mode_enabled,
    agent_mode_ready_for_current_user,
    submit_agent_job,
    sync_agent_job_state,
)
from integrations.github_actions import (
    background_jobs_allowed_for_user,
    clear_github_actions_caches,
    find_run_by_request_id,
    github_actions_ready,
    load_latest_result,
    load_result_json_for_run,
    trigger_web_job,
)


def current_user_id() -> str:
    user = st.session_state.get("user") or {}
    if isinstance(user, dict):
        return str(user.get("id", "") or "").strip()
    return ""


def background_jobs_ready_for_current_user() -> tuple[bool, str]:
    # ── AGENT_MODE: 进程内执行，无需 GH Actions Token ──
    if agent_mode_enabled():
        return agent_mode_ready_for_current_user()
    # ── 原有 GH Actions 路径 ──
    ready, msg = github_actions_ready()
    if not ready:
        return (False, msg)
    user_id = current_user_id()
    if not user_id:
        return (False, "当前未登录")
    if not background_jobs_allowed_for_user(user_id):
        return (False, "当前账号未被授权触发后台任务")
    return (True, "")


def submit_background_job(job_kind: str, payload: dict[str, Any], *, state_key: str) -> str:
    user_id = current_user_id()
    merged_payload = {"user_id": user_id, **payload}
    # ── full_pipeline 始终走进程内执行（GitHub Actions 不支持此 job_kind） ──
    if agent_mode_enabled() or job_kind == "full_pipeline":
        return submit_agent_job(job_kind, merged_payload, state_key=state_key)
    # ── 原有 GH Actions 路径 ──
    request_id = trigger_web_job(job_kind, merged_payload)
    st.session_state[state_key] = {
        "job_kind": job_kind,
        "request_id": request_id,
        "run": None,
        "result": None,
    }
    return request_id


def sync_background_job_state(*, state_key: str) -> dict[str, Any] | None:
    state = st.session_state.get(state_key)
    if not isinstance(state, dict):
        return None
    # ── AGENT_MODE: 从进程内存读取状态 ──
    if state.get("_agent_mode"):
        return sync_agent_job_state(state_key=state_key)
    # ── 原有 GH Actions 路径 ──
    request_id = str(state.get("request_id", "") or "").strip()
    if not request_id:
        return state
    run = find_run_by_request_id(request_id)
    state["run"] = run
    if run and run.status == "completed":
        state["result"] = load_result_json_for_run(run.run_id)
    st.session_state[state_key] = state
    return state


def load_latest_job_result(job_kind: str, *, per_page: int = 10) -> tuple[Any, dict[str, Any] | None]:
    user_id = current_user_id()
    return load_latest_result(job_kind, requested_by_user_id=user_id, per_page=per_page)


def refresh_background_job_data() -> None:
    clear_github_actions_caches()


def render_background_job_status(state: dict | None, *, noun: str = "任务") -> dict | None:
    """
    渲染后台任务状态（WyckoffScreeners / AIAnalysis 共用）。

    Returns:
        result dict（如果有），否则 None。
    """
    if not isinstance(state, dict):
        return None
    run = state.get("run")
    result = state.get("result")
    request_id = str(state.get("request_id", "") or "").strip()
    if request_id:
        st.caption(f"请求 ID: `{request_id}`")
    if run is None:
        st.info(f"后台{noun}已提交，运行实例还在排队创建。")
        return result if isinstance(result, dict) else None

    status = f"{getattr(run, 'status', '') or '--'}"
    conclusion = f"{getattr(run, 'conclusion', '') or '--'}"
    html_url = str(getattr(run, "html_url", "") or "").strip()
    if status == "completed":
        if conclusion == "success":
            st.success(f"后台{noun}完成。")
        else:
            st.error(f"后台{noun}已结束，但结论为 `{conclusion}`。")
    else:
        st.info(f"后台{noun}进行中：`{status}`")
    if html_url:
        st.markdown(f"[打开 GitHub Actions 运行详情]({html_url})")
    if isinstance(result, dict) and str(result.get("status", "") or "") == "error":
        st.error(str(result.get("error", f"后台{noun}失败")))
    return result if isinstance(result, dict) else None
