# -*- coding: utf-8 -*-
"""AI 分析页：Alpha 虚拟投委会研报。"""
import os
import random
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st
import akshare as ak

from app.layout import setup_page
from app.navigation import show_right_nav
from app.ui_helpers import show_page_loading
from integrations.ai_prompts import ALPHA_CIO_SYSTEM_PROMPT
from integrations.llm_client import call_llm, SUPPORTED_PROVIDERS, GEMINI_MODELS
from integrations.fetch_a_share_csv import (
    _resolve_trading_window,
    _fetch_hist,
    _build_export,
    get_all_stocks,
    get_stocks_by_board,
    _normalize_symbols,
    _stock_name_from_code,
)
from utils import extract_symbols_from_text, stock_sector_em
from core.wyckoff_engine import (
    FunnelConfig,
    normalize_hist_from_fetch,
    run_funnel,
)
from integrations.data_source import fetch_index_hist, fetch_sector_map, fetch_market_cap_map
from core.single_stock_logic import render_single_stock_page
from scripts.step3_batch_report import generate_stock_payload
from integrations.ai_prompts import WYCKOFF_FUNNEL_SYSTEM_PROMPT

# 等待时随机展示的股市名人名言（本地列表）
STOCK_QUOTES = [
    "市场永远是对的。——杰西·利弗莫尔",
    "不要与市场争辩，最小阻力线才是方向。——杰西·利弗莫尔",
    "买你懂的，不懂不买。——彼得·林奇",
    "当你持有好公司时，时间站在你这一边。——彼得·林奇",
    "别人恐惧时我贪婪，别人贪婪时我恐惧。——沃伦·巴菲特",
    "价格是你付出的，价值是你得到的。——沃伦·巴菲特",
    "趋势是你的朋友。——华尔街谚语",
    "截断亏损，让利润奔跑。——威廉·埃克哈特",
    "计划你的交易，交易你的计划。——佚名",
    "本金安全第一，先求不败再求胜。——杰西·利弗莫尔",
    "没有纪律，再好的策略也是空谈。——理查德·丹尼斯",
    "市场会奖励耐心与纪律。——查理·芒格",
    "只在关键点出手。——杰西·利弗莫尔",
    "量在价先，资金不会骗人。——威科夫",
    "买在分歧，卖在一致。——情绪流龙头战法",
]

TRIGGER_LABELS = {
    "spring": "Spring（终极震仓）",
    "lps": "LPS（缩量回踩）",
    "evr": "Effort vs Result（放量不跌）",
}

TRADING_DAYS_OHLCV = 60
ADJUST = "qfq"
MAX_SYMBOLS = 6

setup_page(page_title="AI 分析", page_icon="🤖")

content_col = show_right_nav()
with content_col:
    st.title("🤖 AI 分析")
    st.markdown("选定股票或筛出候选后，一键生成多维度深度研报，供你决策参考。")

    # 1) 供应商与模型（首期仅 Gemini）
    st.subheader("API 与模型")
    provider = st.selectbox(
        "API 供应商",
        options=list(SUPPORTED_PROVIDERS),
        format_func=lambda x: "Gemini" if x == "gemini" else x,
        key="ai_provider",
    )
    model = st.selectbox(
        "模型",
        options=list(GEMINI_MODELS),
        key="ai_model",
    )

    # 2) API Key 校验
    api_key = (st.session_state.get("gemini_api_key") or "").strip()
    if not api_key:
        st.toast("请先在设置页录入 API Key", icon="⚠️")
        st.warning("未检测到 API Key，请先在设置页录入后再使用 AI 分析。")
        st.page_link("pages/Settings.py", label="前往设置", icon="⚙️")
        st.stop()

    # 3) 分析类型与标的
    st.subheader("分析内容")
    analysis_type = st.radio(
        "分析类型",
        options=["single_stock", "stock_list", "find_gold"],
        format_func=lambda x: {
            "single_stock": "单股分析 (威科夫大师模式)",
            "stock_list": "指定股票代码 (批量研报)",
            "find_gold": "Wyckoff Funnel (批量研报)"
        }.get(x, x),
        horizontal=True,
        key="ai_analysis_type",
    )

    if analysis_type == "single_stock":
        render_single_stock_page(provider, model, api_key)
        st.stop()  # 单股模式独占页面下方

    symbols: list[str] = []
    if analysis_type == "stock_list":
        stock_input = st.text_area(
            "股票代码（最多 6 个）",
            placeholder="例如：000001；600519；300364（分号或空格分隔）",
            height=100,
            key="ai_stock_list_input",
        )
        candidates = extract_symbols_from_text(stock_input or "")
        symbols = _normalize_symbols(candidates)[:MAX_SYMBOLS]
        if not symbols:
            st.info("请至少输入 1 个、最多 6 个股票代码。")
        elif len(_normalize_symbols(candidates)) > MAX_SYMBOLS:
            st.caption(f"已自动截取前 {MAX_SYMBOLS} 个代码：{', '.join(symbols)}")
    else:
        # Wyckoff Funnel：本页直接执行，无需跳转
        find_gold_result: list[tuple[str, float]] = st.session_state.get("ai_find_gold_result") or []
        if find_gold_result:
            symbols = [s for s, _ in find_gold_result[:MAX_SYMBOLS]]
            st.caption(f"将使用漏斗结果中的 {len(symbols)} 只股票：{', '.join(symbols)}")
            if st.button("重新漏斗筛选", key="ai_reset_find_gold"):
                del st.session_state["ai_find_gold_result"]
                st.rerun()
        else:
            with st.container(border=True):
                st.markdown("**先通过 Wyckoff Funnel 筛选值得关注的股票**")
                pool_mode = st.radio("股票池", options=["板块", "手动输入"], horizontal=True, key="ai_pool_mode")
                symbols_input_fg = ""
                board_fg = "all"
                limit_fg = 500
                if pool_mode == "手动输入":
                    symbols_input_fg = st.text_area(
                        "股票代码",
                        placeholder="例如：600519, 000001",
                        height=80,
                        key="ai_find_gold_symbols_text",
                    )
                else:
                    board_fg = st.selectbox(
                        "板块",
                        options=["all", "main", "chinext"],
                        format_func=lambda v: {"all": "全部主板+创业板", "main": "主板", "chinext": "创业板"}.get(v, v),
                        key="ai_find_gold_board",
                    )
                    limit_fg = int(st.number_input("股票数量上限", min_value=50, max_value=5000, value=500, step=100, key="ai_find_gold_limit"))

                run_find_gold = st.button("执行 Wyckoff Funnel", type="primary", key="ai_run_find_gold")
                if run_find_gold:
                    if pool_mode == "手动输入":
                        candidates_fg = extract_symbols_from_text(symbols_input_fg or "")
                        pool_symbols = _normalize_symbols(candidates_fg)
                    else:
                        pool_symbols = [s.get("code") for s in get_stocks_by_board(board_fg) if s.get("code")][:limit_fg]
                    if not pool_symbols:
                        st.warning("请先输入股票代码或选择板块。")
                        st.stop()
                    end_cal = date.today() - timedelta(days=1)
                    try:
                        window_fg = _resolve_trading_window(end_cal, 500)
                    except Exception as e:
                        st.error(f"交易日窗口解析失败：{e}")
                        st.stop()
                    start_s = window_fg.start_trade_date.strftime("%Y%m%d")
                    end_s = window_fg.end_trade_date.strftime("%Y%m%d")

                    with st.spinner("加载行业 & 市值数据..."):
                        sector_map_fg = fetch_sector_map()
                        market_cap_fg = fetch_market_cap_map()
                    name_map_fg = {s.get("code", ""): s.get("name", "") for s in get_all_stocks() if isinstance(s, dict) and s.get("code")}

                    progress_ph = st.empty()
                    progress_bar = progress_ph.progress(0)
                    data_map_fg: dict[str, pd.DataFrame] = {}
                    for idx, sym in enumerate(pool_symbols):
                        try:
                            df_h = _fetch_hist(sym, window_fg, "qfq")
                            data_map_fg[sym] = normalize_hist_from_fetch(df_h)
                        except Exception:
                            pass
                        progress_bar.progress((idx + 1) / len(pool_symbols))
                    progress_ph.empty()

                    bench_df_fg = None
                    try:
                        bench_df_fg = fetch_index_hist("000001", start_s, end_s)
                    except Exception:
                        pass

                    funnel_result = run_funnel(
                        all_symbols=list(data_map_fg.keys()),
                        df_map=data_map_fg,
                        bench_df=bench_df_fg,
                        name_map=name_map_fg,
                        market_cap_map=market_cap_fg,
                        sector_map=sector_map_fg,
                    )
                    result_list: list[tuple[str, float]] = []
                    seen: set[str] = set()
                    for trig_key in ("spring", "lps", "evr"):
                        for code, score in funnel_result.triggers.get(trig_key, []):
                            if code not in seen:
                                seen.add(code)
                                result_list.append((code, score))
                    st.session_state["ai_find_gold_result"] = result_list
                    st.toast(f"漏斗筛选完成，共 {len(result_list)} 只", icon="✅")
                    st.rerun()
                st.stop()

    if not symbols:
        st.stop()

    run_btn = st.button("开始分析", type="primary", key="ai_run_btn")

    if run_btn:
        # 时间窗口：近 60 个交易日，前复权
        end_calendar = date.today() - timedelta(days=1)
        try:
            window = _resolve_trading_window(end_calendar, TRADING_DAYS_OHLCV)
        except Exception as e:
            st.error(f"无法解析交易日窗口：{e}")
            st.stop()

        loading = show_page_loading(
            title="正在拉取 OHLCV 与生成研报…",
            subtitle="请稍候",
            quote=random.choice(STOCK_QUOTES),
        )
        failed: list[str] = []
        parts: list[str] = []

        try:
            if analysis_type == "find_gold":
                # 使用和 step3_batch_report.py 完全一致的格式
                # funnel_result 已经存在了 session 里 (code, score)
                # find_gold 已经提取出了 symbols 列表
                # 取出 tag 和 score
                # 由于之前的 funnel 没有把完整的 row 存到 session，我们在这动态生成。
                fg_list = st.session_state.get("ai_find_gold_result") or []
                score_map = {code: score for code, score in fg_list}
                
                parts: list[str] = []
                for symbol in symbols:
                    try:
                        df_hist = _fetch_hist(symbol, window, ADJUST)
                        df = normalize_hist_from_fetch(df_hist)
                        sector = stock_sector_em(symbol, timeout=30)
                        try:
                            name = _stock_name_from_code(symbol)
                        except Exception:
                            name = symbol

                        score = score_map.get(symbol)
                        # 生成统一的高密度盘口切片文本
                        payload = generate_stock_payload(
                            stock_code=symbol,
                            stock_name=name,
                            wyckoff_tag="AI分析", # 网页端简化
                            df=df,
                            industry=sector,
                            quant_score=score,
                        )
                        parts.append(payload)
                    except Exception as e:
                        failed.append(f"{symbol}（{e}）")
                        continue

                system_prompt_to_use = WYCKOFF_FUNNEL_SYSTEM_PROMPT
                user_message = (
                    "请按最新的《Alpha 投委会机密电报》流程，使用综合人视角，对以下票池进行诊断与分流。\n\n"
                    + "\n\n".join(parts)
                )

            else:
                # 兼容旧单股 / stock_list 模式
                for symbol in symbols:
                    try:
                        df_hist = _fetch_hist(symbol, window, ADJUST)
                        sector = stock_sector_em(symbol, timeout=30)
                        df_export = _build_export(df_hist, sector)
                        try:
                            name = _stock_name_from_code(symbol)
                        except Exception:
                            name = symbol
                        csv_text = df_export.to_csv(index=False, encoding="utf-8-sig")
                        parts.append(f"## {symbol} {name}\n\n```csv\n{csv_text}\n```")
                    except Exception as e:
                        failed.append(f"{symbol}（{e}）")
                        continue

                system_prompt_to_use = ALPHA_CIO_SYSTEM_PROMPT
                user_message = (
                    "请按 Alpha 投委会流程分析以下 OHLCV 数据（CSV 格式）。\n\n"
                    + "\n\n".join(parts)
                )

            if not parts:
                st.error("所有标的拉取失败，无法进行分析。失败详情：" + "; ".join(failed))
                loading.empty()
                st.stop()

            if failed:
                st.caption("以下标的拉取失败，已跳过：" + "; ".join(failed))

            report_text = call_llm(
                provider=provider,
                model=model,
                api_key=api_key,
                system_prompt=system_prompt_to_use,
                user_message=user_message,
                timeout=120,
            )
        except ValueError as e:
            loading.empty()
            st.error(str(e))
            st.stop()
        except RuntimeError as e:
            loading.empty()
            st.error(f"模型调用失败：{e}。请检查 Key、网络或稍后重试。")
            st.stop()
        except Exception as e:
            loading.empty()
            st.error(f"发生错误：{e}")
            st.stop()
        finally:
            loading.empty()

        st.subheader("📄 深度研报")
        st.markdown(report_text)
