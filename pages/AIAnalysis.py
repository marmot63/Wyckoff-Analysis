# -*- coding: utf-8 -*-
"""AI 分析页：单股本地，批量后台，完整管线（AGENT_MODE）。"""
import os
import time

import pandas as pd
import streamlit as st

from app.background_jobs import (
    background_jobs_ready_for_current_user,
    current_user_id,
    load_latest_job_result,
    refresh_background_job_data,
    render_background_job_status,
    submit_background_job,
    sync_background_job_state,
)
from app.layout import setup_page
from app.navigation import show_right_nav
from app.pipeline_renderers import render_pipeline_progress, render_pipeline_summary
from app.single_stock_logic import render_single_stock_page
from integrations.llm_client import (
    DEFAULT_GEMINI_MODEL,
    GEMINI_MODELS,
    OPENAI_COMPATIBLE_BASE_URLS,
    PROVIDER_LABELS,
    SUPPORTED_PROVIDERS,
    get_provider_credentials,
)
from utils import extract_symbols_from_text

AI_ANALYSIS_DEFAULT_FEISHU_WEBHOOK = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/4ef56ec3-fb84-4eb4-b4d9-775ae7de69ff"
)


def _resolve_ai_analysis_feishu_webhook() -> str:
    """
    AI 分析页专用 webhook 口径：
    1) 优先用户在设置页保存的 feishu_webhook
    2) 用户未设置时，回退到 AI 分析页专用兜底地址
    """
    user_webhook = str(st.session_state.get("feishu_webhook") or "").strip()
    return user_webhook or AI_ANALYSIS_DEFAULT_FEISHU_WEBHOOK


_get_provider_credentials = get_provider_credentials


def _render_single_stock_page_compat(
    provider: str,
    model: str,
    api_key: str,
    base_url: str,
    feishu_webhook: str,
) -> None:
    """
    兼容旧版本 single_stock_logic.render_single_stock_page(provider, model, api_key)
    与新版本 render_single_stock_page(..., base_url=...)。
    """
    try:
        render_single_stock_page(
            provider,
            model,
            api_key,
            base_url=base_url,
            feishu_webhook=feishu_webhook,
        )
    except TypeError as e:
        err = str(e)
        if "unexpected keyword argument 'feishu_webhook'" in err:
            try:
                render_single_stock_page(
                    provider,
                    model,
                    api_key,
                    base_url=base_url,
                )
                return
            except TypeError as e2:
                if "unexpected keyword argument 'base_url'" not in str(e2):
                    raise
                render_single_stock_page(provider, model, api_key)
                return
        if "unexpected keyword argument 'base_url'" in err:
            render_single_stock_page(provider, model, api_key)
            return
        raise

setup_page(page_title="AI 分析", page_icon="🤖")

STATE_KEY = "batch_ai_background_job"
PIPELINE_STATE_KEY = "full_pipeline_job"


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


def _pipeline_is_running(state: dict | None) -> bool:
    if not isinstance(state, dict):
        return False
    run = state.get("run")
    if run is None:
        return bool(state.get("request_id", ""))
    return getattr(run, "status", "") in ("queued", "in_progress")


def _pipeline_is_completed(state: dict | None) -> bool:
    if not isinstance(state, dict):
        return False
    run = state.get("run")
    if run is None:
        return False
    return getattr(run, "status", "") == "completed"


def _render_ai_status(state: dict | None) -> dict | None:
    return render_background_job_status(state, noun="AI 任务")


content_col = show_right_nav()
with content_col:
    st.title("🤖 AI 分析")
    st.markdown("单股本地分析 · 批量后台研报 · 完整管线一键运行")

    analysis_type = st.radio(
        "分析类型",
        options=["single_stock", "stock_list", "find_gold", "full_pipeline"],
        format_func=lambda x: {
            "single_stock": "单股分析 (本地)",
            "stock_list": "指定股票代码 (后台批量研报)",
            "find_gold": "使用后台漏斗候选 (后台批量研报)",
            "full_pipeline": "🚀 完整管线",
        }.get(x, x),
        horizontal=True,
        key="ai_analysis_type",
    )

    if analysis_type == "single_stock":
        effective_feishu_webhook = _resolve_ai_analysis_feishu_webhook()
        provider = st.selectbox(
            "API 供应商",
            options=list(SUPPORTED_PROVIDERS),
            format_func=lambda x: PROVIDER_LABELS.get(x, x),
            key="ai_provider_single",
        )
        api_key, default_model, base_url = _get_provider_credentials(provider)
        model = st.text_input(
            "模型",
            value=default_model or (GEMINI_MODELS[0] if provider == "gemini" else ""),
            key="ai_model_single",
            help="单股模式继续走本地轻量分析，不经过后台任务。",
        ).strip()
        effective_single_base_url = base_url
        if provider in OPENAI_COMPATIBLE_BASE_URLS:
            single_base_url_input = st.text_input(
                "Base URL（可选）",
                value=base_url,
                key=f"ai_single_base_url_{provider}",
                help="留空时自动使用该供应商默认 Base URL。",
            ).strip()
            effective_single_base_url = single_base_url_input or OPENAI_COMPATIBLE_BASE_URLS.get(provider, "")
        if not api_key:
            st.warning(
                f"单股模式需要 {PROVIDER_LABELS.get(provider, provider)} API Key，请先在设置页录入或配置环境变量。"
            )
            st.page_link("pages/Settings.py", label="前往设置", icon="⚙️")
            st.stop()
        if provider == "gemini":
            st.caption("常用模型示例：" + "、".join(GEMINI_MODELS[:6]))
        _render_single_stock_page_compat(
            provider,
            model or default_model or (GEMINI_MODELS[0] if provider == "gemini" else ""),
            api_key,
            effective_single_base_url,
            effective_feishu_webhook,
        )
        st.stop()

    # ── 完整管线 (进程内 Agent Pipeline) ──
    if analysis_type == "full_pipeline":
        _uid = current_user_id()
        if not _uid:
            st.warning("请先登录后使用完整管线。")
            st.stop()

        from integrations.supabase_job_usage import (
            check_daily_limit, get_user_tier_info, increment_daily_usage,
        )

        _tier_info = get_user_tier_info(_uid)
        _allowed, _used, _limit = check_daily_limit(_uid, "full_pipeline")

        st.info("完整管线将在进程内依次执行：**漏斗筛选 → 大盘环境 → AI 研报 → 持仓策略 → 通知汇总**。")
        if _limit < 0:
            st.caption(f"等级：**{_tier_info['label']}** · 不限次数")
        elif _limit > 0:
            _remaining = max(_limit - _used, 0)
            st.caption(f"等级：**{_tier_info['label']}** · 今日剩余额度：**{_remaining}/{_limit}** 次")

        # 模型配置
        with st.expander("模型配置", expanded=False):
            col_prov, col_mdl = st.columns(2)
            with col_prov:
                p_provider = st.selectbox(
                    "API 供应商",
                    options=list(SUPPORTED_PROVIDERS),
                    format_func=lambda x: PROVIDER_LABELS.get(x, x),
                    key="pipeline_provider",
                )
            with col_mdl:
                if p_provider == "gemini":
                    p_model = st.selectbox(
                        "Gemini 模型",
                        options=GEMINI_MODELS,
                        index=GEMINI_MODELS.index(DEFAULT_GEMINI_MODEL) if DEFAULT_GEMINI_MODEL in GEMINI_MODELS else 0,
                        key="pipeline_model_gemini",
                    )
                else:
                    p_model = st.text_input(
                        "模型名称",
                        value=get_provider_credentials(p_provider)[1],
                        key="pipeline_model_other",
                    )
            p_api_key, p_default_model, p_base_url = get_provider_credentials(p_provider)
            if not p_model:
                p_model = p_default_model
            p_api_key_input = st.text_input(
                f"{PROVIDER_LABELS.get(p_provider, p_provider)} API Key",
                value=p_api_key,
                type="password",
                key="pipeline_api_key",
            )
            if p_api_key_input:
                p_api_key = p_api_key_input

        # 高级配置
        with st.expander("高级配置"):
            col_a, col_b = st.columns(2)
            with col_a:
                p_webhook = st.text_input(
                    "飞书 Webhook (可选)",
                    value=st.session_state.get("feishu_webhook", "") or "",
                    key="pipeline_feishu_webhook",
                )
            with col_b:
                p_skip_step4 = st.checkbox(
                    "跳过持仓策略 (Step4)",
                    value=False,
                    key="pipeline_skip_step4",
                )

        # 提交 / 状态
        p_state = st.session_state.get(PIPELINE_STATE_KEY)
        p_running = _pipeline_is_running(p_state)

        if p_running:
            st.button("🔄 管线运行中...", disabled=True, use_container_width=True)
        else:
            if not p_api_key:
                st.warning(f"请配置 {PROVIDER_LABELS.get(p_provider, p_provider)} API Key。")
            btn_disabled = (not p_api_key) or (not _allowed)
            if not _allowed:
                st.warning(f"今日管线额度已用完（{_used}/{_limit}），明天再来。")
            if st.button("🚀 一键运行完整管线", type="primary", disabled=btn_disabled, use_container_width=True):
                # 提交前再次校验（防并发竞态）
                ok2, used2, lim2 = check_daily_limit(_uid, "full_pipeline")
                if not ok2:
                    st.error(f"额度已用完（{used2}/{lim2}），请明天再试。")
                else:
                    increment_daily_usage(_uid, "full_pipeline")
                    payload = {
                        "user_id": _uid,
                        "provider": p_provider,
                        "model": p_model,
                        "api_key": p_api_key,
                        "base_url": p_base_url,
                        "webhook_url": (p_webhook or "").strip(),
                        "skip_step4": p_skip_step4,
                    }
                    submit_background_job("full_pipeline", payload, state_key=PIPELINE_STATE_KEY)
                    st.rerun()

        p_state = sync_background_job_state(state_key=PIPELINE_STATE_KEY)
        if isinstance(p_state, dict) and p_state.get("request_id"):
            st.divider()
            st.subheader("管线进度")
            st.caption(f"任务 ID: `{p_state.get('request_id', '')}`")

            render_pipeline_progress(
                stages=p_state.get("stages", []),
                current_stage=p_state.get("current_stage", ""),
                current_stage_status=p_state.get("current_stage_status", ""),
                is_running=p_running,
            )

            if _pipeline_is_completed(p_state):
                result = p_state.get("result")
                if isinstance(result, dict):
                    conclusion = getattr(p_state.get("run"), "conclusion", "")
                    if conclusion == "success" and result.get("ok"):
                        st.success("管线运行完成!")
                    elif conclusion == "success":
                        st.warning("管线部分完成，部分阶段失败。")
                    else:
                        st.error("管线运行失败。")
                        if result.get("error"):
                            st.error(result["error"])

                    st.divider()
                    st.subheader("运行结果")
                    render_pipeline_summary(result)

            if p_running:
                time.sleep(2)
                st.rerun()

        st.stop()

    # ── 批量模式 (stock_list / find_gold) ──
    ready, ready_msg = background_jobs_ready_for_current_user()
    if not ready:
        st.error(ready_msg)
        st.stop()

    st.info(
        "批量模式已改成后台任务。页面只提交参数并读取结果，不再在 Streamlit 进程里拉全量 OHLCV 或等待长时间模型调用。"
    )
    batch_provider = st.selectbox(
        "后台 API 供应商",
        options=list(SUPPORTED_PROVIDERS),
        format_func=lambda x: PROVIDER_LABELS.get(x, x),
        key="ai_provider_batch",
    )
    batch_api_key, batch_default_model, batch_base_url = _get_provider_credentials(batch_provider)
    model_override = st.text_input(
        "后台模型覆盖（可留空）",
        value=batch_default_model or (GEMINI_MODELS[0] if batch_provider == "gemini" else ""),
        key=f"ai_model_batch_{batch_provider}",
        help="留空则优先使用你在设置页保存的对应供应商模型。",
    ).strip()
    effective_batch_base_url = batch_base_url
    if batch_provider in OPENAI_COMPATIBLE_BASE_URLS:
        batch_base_url_input = st.text_input(
            "后台 Base URL（可选）",
            value=batch_base_url,
            key=f"ai_batch_base_url_{batch_provider}",
            help="留空时自动使用该供应商默认 Base URL。",
        ).strip()
        effective_batch_base_url = batch_base_url_input or OPENAI_COMPATIBLE_BASE_URLS.get(batch_provider, "")
    if not batch_api_key:
        st.warning(
            f"后台批量模式需要 {PROVIDER_LABELS.get(batch_provider, batch_provider)} API Key，请先在设置页录入或配置环境变量。"
        )
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

    run_btn = st.button(
        "提交后台 AI 研报",
        type="primary",
        disabled=(not bool(selected_symbols_info)) or (not bool(batch_api_key)),
    )
    refresh_btn = st.button("刷新后台状态")

    if run_btn and selected_symbols_info:
        effective_feishu_webhook = _resolve_ai_analysis_feishu_webhook()
        payload = {
            "symbols_info": selected_symbols_info,
            "benchmark_context": benchmark_context,
            "provider": batch_provider,
            "model": model_override,
            "base_url": effective_batch_base_url,
            "webhook_url": effective_feishu_webhook,
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
