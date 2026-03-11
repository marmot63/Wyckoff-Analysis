# -*- coding: utf-8 -*-
"""AI 分析页：单股本地，批量后台。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st

from app.background_jobs import (
    background_jobs_ready_for_current_user,
    load_latest_job_result,
    refresh_background_job_data,
    submit_background_job,
    sync_background_job_state,
)
from app.layout import setup_page
from app.navigation import show_right_nav
from core.single_stock_logic import render_single_stock_page
from integrations.llm_client import SUPPORTED_PROVIDERS, GEMINI_MODELS
from utils import extract_symbols_from_text

setup_page(page_title="AI 分析", page_icon="🤖")

STATE_KEY = "batch_ai_background_job"


def _parse_manual_codes(text: str) -> list[dict]:
    raw_codes = extract_symbols_from_text(str(text or ""))
    rows: list[dict] = []
    seen: set[str] = set()
    for code in raw_codes:
        code_s = str(code or "").strip()
        if not code_s or code_s in seen:
            continue
        seen.add(code_s)
        rows.append({"code": code_s, "name": code_s, "tag": ""})
    return rows[:6]


def _load_find_gold_source() -> tuple[list[dict], dict]:
    session_rows = st.session_state.get("ai_find_gold_background_symbols") or []
    if isinstance(session_rows, list) and session_rows:
        return (session_rows, {})
    _, latest_result = load_latest_job_result("funnel_screen")
    if latest_result:
        return (
            latest_result.get("symbols_for_report", []) or [],
            latest_result.get("benchmark_context", {}) or {},
        )
    return ([], {})


def _render_ai_status(state: dict | None) -> dict | None:
    if not isinstance(state, dict):
        return None
    run = state.get("run")
    result = state.get("result")
    request_id = str(state.get("request_id", "") or "").strip()
    if request_id:
        st.caption(f"请求 ID: `{request_id}`")
    if run is None:
        st.info("后台 AI 任务已提交，正在等待 GitHub Actions 创建运行实例。")
        return result if isinstance(result, dict) else None
    status = str(getattr(run, "status", "") or "")
    conclusion = str(getattr(run, "conclusion", "") or "")
    if status == "completed":
        if conclusion == "success":
            st.success("后台 AI 任务已完成。")
        else:
            st.error(f"后台 AI 任务结束，但结论为 `{conclusion or '--'}`。")
    else:
        st.info(f"后台 AI 任务进行中：`{status}`")
    html_url = str(getattr(run, "html_url", "") or "").strip()
    if html_url:
        st.markdown(f"[打开 GitHub Actions 运行详情]({html_url})")
    if isinstance(result, dict) and str(result.get("status", "") or "") == "error":
        st.error(str(result.get("error", "后台 AI 任务失败")))
    return result if isinstance(result, dict) else None


content_col = show_right_nav()
with content_col:
    st.title("🤖 AI 分析")
    st.markdown("单股维持本地分析；批量研报和漏斗联动已经迁到 GitHub Actions 后台。")

    analysis_type = st.radio(
        "分析类型",
        options=["single_stock", "stock_list", "find_gold"],
        format_func=lambda x: {
            "single_stock": "单股分析 (本地)",
            "stock_list": "指定股票代码 (后台批量研报)",
            "find_gold": "使用后台漏斗候选 (后台批量研报)",
        }.get(x, x),
        horizontal=True,
        key="ai_analysis_type",
    )

    if analysis_type == "single_stock":
        provider = st.selectbox(
            "API 供应商",
            options=list(SUPPORTED_PROVIDERS),
            format_func=lambda x: "Gemini" if x == "gemini" else x,
            key="ai_provider_single",
        )
        default_model = (
            st.session_state.get("ai_model")
            or st.session_state.get("gemini_model")
            or "gemini-3.1-flash-lite-preview"
        )
        model = st.text_input(
            "模型",
            value=str(default_model),
            key="ai_model_single",
            help="单股模式继续走本地轻量分析，不经过后台任务。",
        ).strip()
        api_key = (st.session_state.get("gemini_api_key") or "").strip()
        if not api_key:
            st.warning("单股模式需要本地 API Key，请先在设置页录入。")
            st.page_link("pages/Settings.py", label="前往设置", icon="⚙️")
            st.stop()
        st.caption("常用模型示例：" + "、".join(GEMINI_MODELS[:6]))
        render_single_stock_page(provider, model or str(default_model), api_key)
        st.stop()

    ready, ready_msg = background_jobs_ready_for_current_user()
    if not ready:
        st.error(ready_msg)
        st.stop()

    st.info(
        "批量模式已改成后台任务。页面只提交参数并读取结果，不再在 Streamlit 进程里拉全量 OHLCV 或等待长时间模型调用。"
    )
    model_override = st.text_input(
        "后台模型覆盖（可留空）",
        value=str(st.session_state.get("gemini_model") or "gemini-3.1-flash-lite-preview"),
        help="留空则优先使用你在设置页保存的 Gemini 模型。",
    ).strip()
    preview_only = st.checkbox("仅生成输入预演，不真正调用模型", value=False)

    selected_symbols_info: list[dict] = []
    benchmark_context: dict = {}

    if analysis_type == "stock_list":
        stock_input = st.text_area(
            "股票代码（最多 6 个）",
            placeholder="例如：000001；600519；300364",
            height=110,
            key="ai_stock_list_input_bg",
        )
        selected_symbols_info = _parse_manual_codes(stock_input)
        if not selected_symbols_info:
            st.caption("请至少输入 1 个股票代码。")
        else:
            st.dataframe(
                pd.DataFrame(
                    [{"代码": x["code"], "名称": x["name"]} for x in selected_symbols_info]
                ),
                use_container_width=True,
                hide_index=True,
            )
    else:
        source_rows, benchmark_context = _load_find_gold_source()
        if not source_rows:
            st.warning("当前没有可用的后台漏斗候选。")
            st.page_link("pages/WyckoffScreeners.py", label="前往后台漏斗页", icon="🔬")
        else:
            options = {
                f"{row.get('code', '')} {row.get('name', '')} | {row.get('track', '')} | {row.get('stage', '')}": row
                for row in source_rows
            }
            default_labels = list(options.keys())[: min(6, len(options))]
            picked = st.multiselect(
                "选择要送去后台 AI 的候选",
                options=list(options.keys()),
                default=default_labels,
                help="默认预选前 6 个后台漏斗候选；你也可以自行删减后再提交后台研报。",
            )
            selected_symbols_info = [options[label] for label in picked][:6]
            if selected_symbols_info:
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "代码": row.get("code", ""),
                                "名称": row.get("name", ""),
                                "行业": row.get("industry", ""),
                                "轨道": row.get("track", ""),
                                "阶段": row.get("stage", ""),
                                "标签": row.get("tag", ""),
                            }
                            for row in selected_symbols_info
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

    run_btn = st.button("提交后台 AI 研报", type="primary", disabled=not bool(selected_symbols_info))
    refresh_btn = st.button("刷新后台状态")

    if run_btn and selected_symbols_info:
        payload = {
            "symbols_info": selected_symbols_info,
            "benchmark_context": benchmark_context,
            "model": model_override,
            "preview_only": preview_only,
        }
        request_id = submit_background_job("batch_ai_report", payload, state_key=STATE_KEY)
        st.success(f"后台 AI 任务已提交：`{request_id}`")

    state = sync_background_job_state(state_key=STATE_KEY)
    active_result = _render_ai_status(state)
    if refresh_btn:
        refresh_background_job_data()
        st.rerun()

    if not active_result:
        latest_run, latest_result = load_latest_job_result("batch_ai_report")
        if latest_result:
            st.divider()
            st.caption(
                "以下展示当前账号最近一次成功的后台批量研报。"
                + (f" Run #{latest_run.run_number}" if latest_run else "")
            )
            active_result = latest_result

    if active_result:
        st.subheader("📄 深度研报")
        
        ok_status = active_result.get("ok", True)
        if not ok_status:
            err_msg = active_result.get("error") or active_result.get("reason") or "未知错误"
            st.error(f"后台研报生成失败：\n\n{err_msg}")
        
        if active_result.get("preview_only"):
            st.caption("当前结果来自输入预演模式。")
            
        report_text = str(active_result.get("report_text", "") or "")
        if report_text:
            st.markdown(report_text)
