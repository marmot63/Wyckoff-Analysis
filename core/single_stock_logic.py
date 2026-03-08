# -*- coding: utf-8 -*-
import ast
import re
import traceback
from datetime import date, datetime, timedelta
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import platform
import os

from integrations.fetch_a_share_csv import _fetch_hist, _resolve_trading_window, _stock_name_from_code
from utils import extract_symbols_from_text, stock_sector_em
from integrations.llm_client import call_llm
from core.wyckoff_single_prompt import WYCKOFF_SINGLE_SYSTEM_PROMPT
from app.layout import is_data_source_failure_message
from app.ui_helpers import show_page_loading

TRADING_DAYS_OHLCV = 500  # 威科夫分析需要较长周期
ADJUST = "qfq"
ALLOW_LLM_PLOT_EXEC = os.getenv("ALLOW_LLM_PLOT_EXEC", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

SAFE_EXEC_BUILTINS = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "range": range,
    "reversed": reversed,
    "round": round,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
}

DISALLOWED_NAMES = {
    "__import__",
    "compile",
    "delattr",
    "eval",
    "exec",
    "getattr",
    "globals",
    "help",
    "input",
    "locals",
    "open",
    "setattr",
    "vars",
    "breakpoint",
    "os",
    "sys",
    "subprocess",
    "shutil",
    "pathlib",
    "socket",
    "requests",
    "http",
    "urllib",
    "importlib",
    "builtins",
}

DISALLOWED_ATTRS = {
    "__bases__",
    "__class__",
    "__closure__",
    "__code__",
    "__dict__",
    "__delattr__",
    "__getattribute__",
    "__getattr__",
    "__globals__",
    "__mro__",
    "__setattr__",
    "__subclasses__",
}

DISALLOWED_AST_NODES = (
    ast.Import,
    ast.ImportFrom,
    ast.Global,
    ast.Nonlocal,
    ast.Try,
    ast.With,
    ast.AsyncWith,
    ast.Raise,
    ast.ClassDef,
    ast.AsyncFunctionDef,
)

def get_chinese_font_path():
    """获取系统中文字体路径"""
    system = platform.system()
    if system == "Darwin":
        paths = [
            "/System/Library/Fonts/PingFang.ttc",
            "/System/Library/Fonts/STHeiti Light.ttc",
            "/System/Library/Fonts/STHeiti Medium.ttc",
        ]
        for p in paths:
            if os.path.exists(p):
                return p
    elif system == "Linux":
        # 常见 Linux/Docker 字体
        paths = [
            "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"
        ]
        for p in paths:
            if os.path.exists(p):
                return p
    return None

def extract_python_code(text: str) -> str | None:
    """从 LLM 回复中提取 Python 代码块"""
    # 匹配 ```python ... ``` 或 ``` ... ```
    pattern = re.compile(r"```(?:python)?\s*(.*?)```", re.DOTALL)
    matches = pattern.findall(text)
    if matches:
        # 返回最长的一段，通常是完整代码
        return max(matches, key=len)
    return None


def _validate_plot_code(code_block: str) -> tuple[bool, str]:
    try:
        tree = ast.parse(code_block)
    except Exception as e:
        return (False, f"代码语法错误: {e}")

    if not any(
        isinstance(node, ast.FunctionDef) and node.name == "create_plot"
        for node in tree.body
    ):
        return (False, "缺少 create_plot(df) 函数")

    allowed_top_level = (ast.FunctionDef, ast.Assign, ast.AnnAssign, ast.Expr)
    for node in tree.body:
        if not isinstance(node, allowed_top_level):
            return (False, f"不允许的顶层语句: {type(node).__name__}")
        if isinstance(node, ast.Expr) and not isinstance(node.value, ast.Constant):
            return (False, "仅允许文档字符串作为顶层表达式")

    for node in ast.walk(tree):
        if isinstance(node, DISALLOWED_AST_NODES):
            return (False, f"不允许的语句: {type(node).__name__}")
        if isinstance(node, ast.Name) and node.id in DISALLOWED_NAMES:
            return (False, f"不允许的标识符: {node.id}")
        if isinstance(node, ast.Attribute):
            if node.attr in DISALLOWED_ATTRS or node.attr.startswith("__"):
                return (False, f"不允许的属性访问: {node.attr}")
        if isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Name) and fn.id in DISALLOWED_NAMES:
                return (False, f"不允许的函数调用: {fn.id}")
            if isinstance(fn, ast.Attribute) and (
                fn.attr in DISALLOWED_NAMES or fn.attr in DISALLOWED_ATTRS
            ):
                return (False, f"不允许的方法调用: {fn.attr}")
    return (True, "")


def _run_plot_code_safely(code_block: str, df_hist: pd.DataFrame):
    ok, reason = _validate_plot_code(code_block)
    if not ok:
        raise ValueError(f"安全策略已拦截生成代码: {reason}")

    exec_globals = {
        "__builtins__": SAFE_EXEC_BUILTINS,
        "pd": pd,
        "plt": plt,
        "fm": fm,
        "datetime": datetime,
        "date": date,
    }
    # ⚠️  SECURITY WARNING
    # exec() 无法提供强隔离，AST 黑名单可被绕过（通过异常链、字符串拼接等）。
    # 仅限私人单用户本地部署使用；公网/多人可触发场景必须保持 ALLOW_LLM_PLOT_EXEC=0。
    exec(code_block, exec_globals)
    create_plot = exec_globals.get("create_plot")
    if not callable(create_plot):
        raise ValueError("未找到可调用的 create_plot(df) 函数")

    df_plot = df_hist.copy()
    if "date" in df_plot.columns:
        df_plot["date"] = pd.to_datetime(df_plot["date"], errors="coerce")

    fig = create_plot(df_plot)
    if fig is None:
        fig = plt.gcf()
    if fig is None or not hasattr(fig, "savefig"):
        raise ValueError("create_plot(df) 未返回有效图表对象")
    return fig

def render_single_stock_page(provider, model, api_key):
    """渲染单股分析页面"""
    st.markdown("### 🔍 威科夫单股分析 (大师模式)")
    st.caption("上传 K 线/分时图（可选），配合 500 天历史数据，生成大师级威科夫分析与标注图表。")

    col1, col2 = st.columns([1, 1])
    with col1:
        stock_input = st.text_input(
            "股票代码",
            placeholder="例如：600519",
            help="请输入单个 A 股代码",
            key="single_stock_code"
        )
    with col2:
        uploaded_file = st.file_uploader(
            "上传今日盘面截图 (可选)",
            type=["png", "jpg", "jpeg"],
            help="上传分时图或 K 线图，辅助判断当日微观结构",
            key="single_stock_image"
        )

    # 提取代码
    symbol = ""
    if stock_input:
        candidates = extract_symbols_from_text(stock_input)
        if candidates:
            symbol = candidates[0]

    run_btn = st.button("开始大师分析", type="primary", disabled=not symbol, key="run_single_stock")

    if run_btn and symbol:
        _run_analysis(symbol, uploaded_file, provider, model, api_key)

def _run_analysis(symbol, image_file, provider, model, api_key):
    """执行分析流程"""
    # 1. 准备数据
    end_calendar = date.today() - timedelta(days=1)
    try:
        window = _resolve_trading_window(end_calendar, TRADING_DAYS_OHLCV)
    except Exception as e:
        st.error(f"无法解析交易日窗口：{e}")
        return

    loading = show_page_loading(
        title="威科夫大师正在读图...",
        subtitle=f"正在拉取 {symbol} 近 {TRADING_DAYS_OHLCV} 天数据并进行结构分析",
    )

    try:
        # 获取 CSV 数据
        df_hist = _fetch_hist(symbol, window, ADJUST)
        sector = stock_sector_em(symbol, timeout=30)
        try:
            name = _stock_name_from_code(symbol)
        except Exception:
            name = symbol

        # 新增：计算该股票的威科夫阶段信息
        from core.wyckoff_engine import (
            FunnelConfig,
            detect_markup_stage,
            detect_accum_stage,
            layer5_exit_signals,
            normalize_hist_from_fetch,
            _sorted_if_needed,
        )

        df_normalized = normalize_hist_from_fetch(df_hist)
        cfg = FunnelConfig()

        # 检测阶段
        stage_info = ""
        markup_list = detect_markup_stage([symbol], {symbol: df_normalized}, cfg)
        accum_map = detect_accum_stage([symbol], {symbol: df_normalized}, cfg)
        exit_signals = layer5_exit_signals([symbol], {symbol: df_normalized}, accum_map, cfg)

        if symbol in markup_list:
            stage_info = "✓ **当前阶段**: Markup（上升期）- 已从积累期成功进入上升趋势\n"
        elif symbol in accum_map:
            stage = accum_map.get(symbol, "")
            stage_cn = {"Accum_A": "积累A（下跌停止）", "Accum_B": "积累B（底部振荡）", "Accum_C": "积累C（最后洗盘）"}.get(stage, stage)
            stage_info = f"✓ **当前阶段**: {stage_cn} - {stage}阶段\n"

        # Exit 信号
        exit_info = ""
        if symbol in exit_signals:
            sig = exit_signals[symbol]
            if sig.get("signal") == "profit_target":
                exit_info = f"⚠ **Exit提醒**: 已达止盈价位 {sig.get('price', 0):.2f} - {sig.get('reason', '')}\n"
            elif sig.get("signal") == "stop_loss":
                exit_info = f"🔴 **Exit提醒**: 触发止损价位 {sig.get('price', 0):.2f} - {sig.get('reason', '')}\n"
            elif sig.get("signal") == "distribution_warning":
                exit_info = f"⚠ **Exit提醒**: {sig.get('reason', '检测到Distribution阶段迹象')}\n"

        # 转换为 CSV 文本
        csv_text = df_hist.to_csv(index=False, encoding="utf-8-sig")

        # 准备 Prompt
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        font_path = get_chinese_font_path()
        font_hint = f"\n【系统检测】当前环境建议中文字体路径：'{font_path}'" if font_path else "\n【系统检测】未检测到常见中文字体，请尝试自动查找。"

        final_system_prompt = WYCKOFF_SINGLE_SYSTEM_PROMPT + font_hint

        user_msg = (
            f"当前北京时间：{current_time}\n"
            f"分析标的：{symbol} {name} ({sector})\n"
            f"数据长度：{len(df_hist)} 交易日\n\n"
            f"{stage_info}"
            f"{exit_info}"
            f"\n以下是 CSV 数据：\n```csv\n{csv_text}\n```\n\n"
            "请开始分析，并生成绘图代码。"
        )

        # 准备图片
        images = []
        if image_file:
            # 读取图片 bytes
            from PIL import Image
            img = Image.open(image_file)
            images.append(img)
            user_msg += "\n\n【用户已上传今日盘面截图，请结合分析】"

        # 2. 调用 LLM
        response_text = call_llm(
            provider=provider,
            model=model,
            api_key=api_key,
            system_prompt=final_system_prompt,
            user_message=user_msg,
            images=images,
            timeout=180, # 增加超时时间，因为任务复杂
        )

        # 3. 展示分析结果
        loading.empty()

        # 分离代码和文本
        code_block = extract_python_code(response_text)

        # 展示文本部分（去除代码块后，或者直接展示全部）
        # 为了美观，我们可以尝试把代码块折叠，或者只展示非代码部分
        # 这里简单起见，直接展示 Markdown
        st.markdown("### 📝 威科夫大师研报")
        st.markdown(response_text)

        # --- 新增飞书推送 ---
        if st.session_state.feishu_webhook:
            try:
                from utils.feishu import send_feishu_notification
                send_feishu_notification(st.session_state.feishu_webhook, f"AI 深度研报 (单股 - {symbol})", response_text)
            except Exception as e:
                traceback.print_exc()
                st.toast(f"飞书推送失败: {e}", icon="⚠️")

        # 4. 执行绘图代码
        if code_block:
            st.markdown("### 📊 结构标注图")
            if not ALLOW_LLM_PLOT_EXEC:
                st.warning(
                    "已禁用自动执行模型生成代码。"
                    "如需启用，请设置环境变量 ALLOW_LLM_PLOT_EXEC=1。"
                )
                st.expander("查看生成代码").code(code_block, language="python")
                return
            with st.spinner("正在绘制图表..."):
                try:
                    fig = _run_plot_code_safely(code_block, df_hist)
                    st.pyplot(fig)
                except Exception as e:
                    st.error(f"绘图代码执行失败：{e}")
                    st.expander("查看生成代码").code(code_block, language="python")
                    st.expander("错误详情").text(traceback.format_exc())

    except Exception as e:
        loading.empty()
        msg = str(e)
        if is_data_source_failure_message(msg):
            st.error(msg)
        else:
            st.error(f"分析过程中发生错误：{e}")
        st.expander("错误详情").text(traceback.format_exc())
