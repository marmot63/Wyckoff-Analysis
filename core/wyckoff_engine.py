# -*- coding: utf-8 -*-
# Copyright (c) 2024 youngcan. All Rights Reserved.
# 本代码仅供个人学习研究使用，未经授权不得用于商业目的。
# 商业授权请联系作者支付授权费用。

"""
Wyckoff Funnel 4 层漏斗筛选引擎

Layer 1: 剥离垃圾 (ST / 北交所 / 科创板 / 市值 / 成交额)
Layer 2: 强弱甄别 (MA50>MA200 多头排列, 或大盘连跌时守住 MA20)
Layer 3: 板块共振 (行业分布 Top-N)
Layer 4: 威科夫狙击 (Spring / LPS / Effort vs Result)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

import numpy as np
import pandas as pd


def normalize_hist_from_fetch(df: pd.DataFrame) -> pd.DataFrame:
    """将 fetch_a_share_csv._fetch_hist 返回的 DataFrame 转为筛选器所需格式。"""
    col_map = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "pct_chg",
    }
    out = df.rename(columns=col_map)
    keep = [
        c
        for c in ["date", "open", "high", "low", "close", "volume", "amount", "pct_chg"]
        if c in out.columns
    ]
    out = out[keep].copy()
    if "pct_chg" not in out.columns and "close" in out.columns:
        out["pct_chg"] = out["close"].astype(float).pct_change() * 100
    for col in ["open", "high", "low", "close", "volume", "amount", "pct_chg"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _sorted_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return df
    try:
        if df["date"].is_monotonic_increasing:
            return df
    except Exception:
        pass
    return df.sort_values("date")


def _latest_trade_date(df: pd.DataFrame) -> object | None:
    if df is None or df.empty or "date" not in df.columns:
        return None
    s = pd.to_datetime(df["date"], errors="coerce").dropna()
    if s.empty:
        return None
    return s.iloc[-1].date()




# Config


@dataclass
class FunnelConfig:
    trading_days: int = 500

    # Layer 1
    min_market_cap_yi: float = 20.0
    min_avg_amount_wan: float = 5000.0
    amount_avg_window: int = 20

    # Layer 2
    ma_short: int = 50
    ma_long: int = 200
    ma_hold: int = 20
    bench_drop_days: int = 3
    bench_drop_threshold: float = -2.0
    rs_window_long: int = 10
    rs_window_short: int = 3
    rs_min_long: float = 0.0
    rs_min_short: float = 0.0
    enable_rs_filter: bool = True
    enable_rps_filter: bool = True
    rps_window_fast: int = 50
    rps_window_slow: int = 120
    rps_fast_min: float = 75.0
    rps_slow_min: float = 70.0
    require_bench_latest_alignment: bool = False
    # Layer 2 潜伏通道（长强短弱）
    enable_ambush_channel: bool = True
    ambush_rps_fast_max: float = 45.0
    ambush_rps_slow_min: float = 70.0
    ambush_rs_long_min: float = -2.0
    ambush_rs_short_min: float = -8.0
    ambush_bias_200_abs_max: float = 0.08
    ambush_ret20_max: float = -3.0

    # Layer 2 低位吸筹通道（Wyckoff Accumulation Channel）
    # 不依赖 RPS 强势排名，专门捕捉”已止跌横盘蓄势”的 Phase A/B/C 股票。
    # 触发条件：低位区间 + 横盘振幅小 + 量能萎缩 + 均线胶着（尚未多头排列）。
    # 这类股票应与 L4 Spring/LPS 配合使用，单独出现时仅进观察池。
    enable_accumulation_channel: bool = True
    accum_lookback_days: int = 250          # 年内低点计算窗口（交易日）
    accum_price_from_low_max: float = 0.35  # 现价不超过年内低点 +35%
    accum_range_window: int = 60            # 横盘振幅计算窗口（交易日）
    accum_range_max_pct: float = 30.0       # 窗口内 (high_max-low_min)/low_min 不超过 30%
    accum_vol_dry_window: int = 20          # 量能萎缩统计近 N 日
    accum_vol_dry_ref_window: int = 120     # 量能萎缩对比参考窗口
    accum_vol_dry_ratio: float = 0.65       # 近 N 日均量 / 参考均量 < 此值（量能萎缩）
    accum_ma_gap_max: float = 0.06          # |MA50 - MA200| / MA200 < 此值（均线胶着）

    # Layer 3
    # 行业共振过滤：按”行业样本数分位阈值 + 最小样本数”动态过滤，避免固定 TopN 误杀。
    top_n_sectors: int = 5
    sector_min_count: int = 3
    sector_count_quantile: float = 0.70
    sector_super_strength_quantile: float = 0.90  # 小而强板块免死阈值（强度分位）

    # Layer 4 - Spring
    spring_support_window: int = 60
    spring_vol_ratio: float = 1.0
    spring_tr_max_range_pct: float = 30.0
    spring_tr_max_drift_pct: float = 12.0

    # Layer 4 - LPS
    lps_lookback: int = 3
    lps_ma: int = 20
    lps_ma_tolerance: float = 0.02
    lps_vol_dry_ratio: float = 0.35
    lps_vol_ref_window: int = 60

    # Layer 4 - Effort vs Result
    # 默认关闭：2025-09~2026-03 快照回测中，开启 EVR 会显著拉低胜率与收益质量。
    enable_evr_trigger: bool = False
    evr_lookback: int = 3
    evr_vol_ratio: float = 1.6
    evr_vol_window: int = 20
    evr_max_drop: float = 2.0
    evr_max_bias_200: float = 40.0
    evr_confirm_days: int = 1
    evr_confirm_allow_break_pct: float = 0.0

    # Layer 4 - SOS / JAC (Sign of Strength / Jump Across the Creek)
    sos_pct_min: float = 4.5  # 点火当日最小涨幅（%）
    sos_vol_ratio: float = 2.0  # 点火当日相比近期均量的最小倍数（爆量）
    sos_vol_window: int = 20  # 计算点火爆量时的参考窗口
    sos_breakout_window: int = 20  # 要求突破或接近近 N 日的高点
    sos_max_bias_200: float = 40.0  # 防止在极高位将 Buying Climax 误判为 SOS

    # Markup 阶段识别（Layer 2.5）
    enable_markup_detection: bool = True
    markup_ma_crossover_confirm_days: int = 5  # MA50 穿过 MA200 后，需要连续 N 日在上方
    markup_ma_angle_min: float = 2.0  # MA50 的角度（% per 5 days），用于确认上升趋势强度
    markup_rs_positive_min: float = 0.5  # RS_short 需要保持正值且持续增强

    # Accumulation ABC 细化（Layer 2 增强）
    enable_accum_abc_detail: bool = True
    accum_b_test_count: int = 3  # B 阶段需要测试底部至少 N 次
    accum_c_max_drop_ratio: float = 0.03  # C 阶段下跌不超过 A 低的 3%

    # Exit 策略（Layer 5）
    enable_exit_signals: bool = True
    # 获利目标：从 Accumulation 低点计算
    exit_profit_target_pct: float = 50.0  # 目标涨幅（%）
    exit_stop_loss_pct: float = -8.0  # 风险控制止损幅度（%）
    # Distribution 识别：高位缩量警告
    dist_high_threshold_pct: float = 30.0  # 相对 MA200 的高度（%）
    dist_vol_dry_ratio: float = 0.5  # 高位缩量比
    dist_confirm_days: int = 3  # 需要连续确认 N 日


class FunnelResult(NamedTuple):
    layer1_symbols: list[str]
    layer2_symbols: list[str]
    layer3_symbols: list[str]
    top_sectors: list[str]
    triggers: dict[str, list[tuple[str, float]]]
    # 新增：威科夫阶段细节
    stage_map: dict[str, str]  # code -> stage_name（如 "Accumulation A"、"Markup"、"Distribution"）
    markup_symbols: list[str]  # 已进入 Markup 的股票
    exit_signals: dict[str, dict]  # code -> {"signal": "profit_target|stop_loss", "price": xxx, "reason": xxx}




# Layer 1: 剥离垃圾


def _is_main_or_chinext(code: str) -> bool:
    return code.startswith(
        ("600", "601", "603", "605", "000", "001", "002", "003", "300", "301")
    )


def layer1_filter(
    symbols: list[str],
    name_map: dict[str, str],
    market_cap_map: dict[str, float],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
) -> list[str]:
    """
    硬过滤：剔除 ST、北交所/科创板、市值<阈值、近期均成交额<阈值。
    market_cap_map 单位：亿元。若 market_cap_map 为空则跳过市值过滤。
    """
    cap_available = bool(market_cap_map)
    passed: list[str] = []
    for sym in symbols:
        if not _is_main_or_chinext(sym):
            continue
        name = name_map.get(sym, "")
        if "ST" in name.upper():
            continue
        if cap_available:
            cap = market_cap_map.get(sym, 0.0)
            if cap < cfg.min_market_cap_yi:
                continue
        df = df_map.get(sym)
        if df is None or df.empty:
            continue
        df_sorted = _sorted_if_needed(df)
        if "amount" in df_sorted.columns:
            avg_amt = df_sorted["amount"].tail(cfg.amount_avg_window).mean()
            if pd.notna(avg_amt) and avg_amt < cfg.min_avg_amount_wan * 10000:
                continue
        passed.append(sym)
    return passed




# Layer 2: 强弱甄别


def layer2_strength_detailed(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame | None,
    cfg: FunnelConfig,
) -> tuple[list[str], dict[str, str]]:
    """
    Layer2 双通道：
    1) 主升通道：MA50>MA200（或大盘连跌时守住MA20）+ RS/RPS 强势过滤
    2) 潜伏通道：长强短弱（RPS120高、RPS50低）且回到年线附近

    返回：
    - passed: 通过 Layer2 的股票
    - channel_map: code -> 主升通道/潜伏通道/双通道
    """

    def _cum_return_pct_from_series(pct_series: pd.Series) -> float | None:
        s = pd.to_numeric(pct_series, errors="coerce").dropna()
        if s.empty:
            return None
        return float(((s / 100.0 + 1.0).prod() - 1.0) * 100.0)

    def _close_return_pct(close_series: pd.Series, lookback: int) -> float | None:
        s = pd.to_numeric(close_series, errors="coerce").dropna()
        lb = max(int(lookback), 1)
        if len(s) <= lb:
            return None
        start = float(s.iloc[-lb - 1])
        end = float(s.iloc[-1])
        if start == 0:
            return None
        return (end - start) / start * 100.0

    def _calc_rs(
        stock_df: pd.DataFrame, bench_sorted_df: pd.DataFrame
    ) -> tuple[float | None, float | None]:
        stock_p = stock_df[["date", "pct_chg"]].copy()
        bench_p = bench_sorted_df[["date", "pct_chg"]].copy()
        merged = stock_p.merge(bench_p, on="date", how="inner", suffixes=("_s", "_b"))
        if merged.empty:
            return (None, None)
        w_long = max(int(cfg.rs_window_long), 1)
        w_short = max(int(cfg.rs_window_short), 1)
        if len(merged) < max(w_long, w_short):
            return (None, None)
        s_long = _cum_return_pct_from_series(merged["pct_chg_s"].tail(w_long))
        b_long = _cum_return_pct_from_series(merged["pct_chg_b"].tail(w_long))
        s_short = _cum_return_pct_from_series(merged["pct_chg_s"].tail(w_short))
        b_short = _cum_return_pct_from_series(merged["pct_chg_b"].tail(w_short))
        if s_long is None or b_long is None or s_short is None or b_short is None:
            return (None, None)
        return (s_long - b_long, s_short - b_short)

    bench_dropping = False
    bench_sorted: pd.DataFrame | None = None
    bench_latest_date = None
    if bench_df is not None and not bench_df.empty:
        bench_sorted = _sorted_if_needed(bench_df)
        bench_latest_date = _latest_trade_date(bench_sorted)
        if len(bench_sorted) >= cfg.bench_drop_days:
            recent_bench = bench_sorted.tail(cfg.bench_drop_days)
            bench_cum = (recent_bench["pct_chg"].dropna() / 100.0 + 1).prod() - 1
            bench_dropping = bench_cum * 100 <= cfg.bench_drop_threshold

    # 截面强弱：RPS50 / RPS120（欧奈尔思路）
    rps_fast_map: dict[str, float] = {}
    rps_slow_map: dict[str, float] = {}
    rps_filter_active = False
    if cfg.enable_rps_filter and symbols:
        rows: list[tuple[str, float, float]] = []
        for sym in symbols:
            df = df_map.get(sym)
            if df is None or df.empty:
                continue
            df_sorted = _sorted_if_needed(df)
            close = pd.to_numeric(df_sorted.get("close"), errors="coerce")
            ret_fast = _close_return_pct(close, cfg.rps_window_fast)
            ret_slow = _close_return_pct(close, cfg.rps_window_slow)
            if ret_fast is None or ret_slow is None:
                continue
            rows.append((sym, ret_fast, ret_slow))
        if rows:
            rps_df = pd.DataFrame(rows, columns=["sym", "ret_fast", "ret_slow"])
            rps_df["rps_fast"] = (
                rps_df["ret_fast"].rank(pct=True, ascending=True, method="average")
                * 100.0
            )
            rps_df["rps_slow"] = (
                rps_df["ret_slow"].rank(pct=True, ascending=True, method="average")
                * 100.0
            )
            rps_fast_map = (
                rps_df.set_index("sym")["rps_fast"].astype(float).to_dict()
            )
            rps_slow_map = (
                rps_df.set_index("sym")["rps_slow"].astype(float).to_dict()
            )
            rps_filter_active = True

    passed: list[str] = []
    channel_map: dict[str, str] = {}
    for sym in symbols:
        df = df_map.get(sym)
        if df is None or len(df) < cfg.ma_long:
            continue
        df_sorted = _sorted_if_needed(df)
        if (
            cfg.require_bench_latest_alignment
            and bench_latest_date is not None
            and _latest_trade_date(df_sorted) != bench_latest_date
        ):
            continue
        close = df_sorted["close"].astype(float)
        ma_short = close.rolling(cfg.ma_short).mean()
        ma_long = close.rolling(cfg.ma_long).mean()
        last_ma_short = ma_short.iloc[-1]
        last_ma_long = ma_long.iloc[-1]
        last_close = close.iloc[-1]

        bullish_alignment = (
            pd.notna(last_ma_short)
            and pd.notna(last_ma_long)
            and last_ma_short > last_ma_long
        )

        holding_ma20 = False
        if bench_dropping:
            ma_hold = close.rolling(cfg.ma_hold).mean()
            last_ma_hold = ma_hold.iloc[-1]
            if pd.notna(last_ma_hold) and last_close >= last_ma_hold:
                holding_ma20 = True

        momentum_rs_ok = True
        ambush_rs_ok = True
        rs_long = None
        rs_short = None
        if cfg.enable_rs_filter and bench_sorted is not None and not bench_sorted.empty:
            rs_long, rs_short = _calc_rs(df_sorted, bench_sorted)
            if rs_long is None or rs_short is None:
                momentum_rs_ok = False
                ambush_rs_ok = False
            else:
                momentum_rs_ok = (
                    rs_long >= cfg.rs_min_long and rs_short >= cfg.rs_min_short
                )
                ambush_rs_ok = (
                    rs_long >= cfg.ambush_rs_long_min
                    and rs_short >= cfg.ambush_rs_short_min
                )

        rps_fast = rps_fast_map.get(sym)
        rps_slow = rps_slow_map.get(sym)
        momentum_rps_ok = True
        ambush_rps_ok = True
        if cfg.enable_rps_filter and rps_filter_active:
            momentum_rps_ok = (
                rps_fast is not None
                and rps_slow is not None
                and rps_fast >= cfg.rps_fast_min
                and rps_slow >= cfg.rps_slow_min
            )
            ambush_rps_ok = (
                rps_fast is not None
                and rps_slow is not None
                and rps_fast <= cfg.ambush_rps_fast_max
                and rps_slow >= cfg.ambush_rps_slow_min
            )

        momentum_ok = (bullish_alignment or holding_ma20) and momentum_rs_ok and momentum_rps_ok

        ambush_shape_ok = False
        if (
            cfg.enable_ambush_channel
            and pd.notna(last_ma_long)
            and float(last_ma_long) > 0
            and pd.notna(last_close)
        ):
            bias_200 = (float(last_close) - float(last_ma_long)) / float(last_ma_long)
            ret20 = _close_return_pct(close, 20)
            ambush_shape_ok = (
                abs(bias_200) <= cfg.ambush_bias_200_abs_max
                and ret20 is not None
                and ret20 <= cfg.ambush_ret20_max
            )
        ambush_ok = (
            cfg.enable_ambush_channel
            and ambush_shape_ok
            and ambush_rs_ok
            and ambush_rps_ok
        )

        # 低位吸筹通道（Wyckoff Accumulation Channel）
        # 四个条件逐一检测，全通才标记。不依赖 RPS 排名。
        accum_ok = False
        if cfg.enable_accumulation_channel and len(df_sorted) >= max(
            cfg.accum_lookback_days, cfg.accum_vol_dry_ref_window
        ):
            _c = close  # alias，避免遮蔽外层

            # 条件 1：低位区——现价在年内低点 +X% 以内
            lookback_w = max(int(cfg.accum_lookback_days), 2)
            period_low = float(_c.tail(lookback_w).min())
            accum_low_ok = (
                period_low > 0
                and float(last_close) <= period_low * (1.0 + cfg.accum_price_from_low_max)
            )

            # 条件 2：横盘振幅——近 N 日 high/low 振幅不超过阈值
            accum_range_ok = False
            if accum_low_ok:
                rw = max(int(cfg.accum_range_window), 5)
                zone = df_sorted.tail(rw)
                _high = pd.to_numeric(zone.get("high"), errors="coerce")
                _low = pd.to_numeric(zone.get("low"), errors="coerce")
                if not _high.dropna().empty and not _low.dropna().empty:
                    h_max = float(_high.max())
                    l_min = float(_low.min())
                    if l_min > 0:
                        range_pct = (h_max - l_min) / l_min * 100.0
                        accum_range_ok = range_pct <= cfg.accum_range_max_pct

            # 条件 3：量能萎缩——近 N 日均量 / 参考均量 < 阈值
            accum_vol_ok = False
            if accum_range_ok:
                vol = pd.to_numeric(df_sorted.get("volume"), errors="coerce")
                dw = max(int(cfg.accum_vol_dry_window), 2)
                rfw = max(int(cfg.accum_vol_dry_ref_window), dw + 1)
                recent_vol_mean = float(vol.tail(dw).mean()) if len(vol) >= dw else None
                ref_vol_mean = float(vol.tail(rfw).iloc[:-dw].mean()) if len(vol) >= rfw else None
                if (
                    recent_vol_mean is not None
                    and ref_vol_mean is not None
                    and ref_vol_mean > 0
                ):
                    accum_vol_ok = (recent_vol_mean / ref_vol_mean) < cfg.accum_vol_dry_ratio

            # 条件 4：均线胶着——MA50 和 MA200 差距不超过阈值（尚未多头排列）
            accum_ma_ok = False
            if accum_vol_ok:
                if (
                    pd.notna(last_ma_short)
                    and pd.notna(last_ma_long)
                    and float(last_ma_long) > 0
                ):
                    ma_gap = abs(float(last_ma_short) - float(last_ma_long)) / float(last_ma_long)
                    accum_ma_ok = ma_gap <= cfg.accum_ma_gap_max

            accum_ok = accum_low_ok and accum_range_ok and accum_vol_ok and accum_ma_ok

        if momentum_ok or ambush_ok or accum_ok:
            passed.append(sym)
            labels: list[str] = []
            if momentum_ok:
                labels.append("主升通道")
            if ambush_ok:
                labels.append("潜伏通道")
            if accum_ok:
                labels.append("吸筹通道")
            channel_map[sym] = "+".join(labels)
    return passed, channel_map



def layer2_strength(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame | None,
    cfg: FunnelConfig,
) -> list[str]:
    passed, _ = layer2_strength_detailed(symbols, df_map, bench_df, cfg)
    return passed




# Layer 3: 板块共振


def layer3_sector_resonance(
    symbols: list[str],
    sector_map: dict[str, str],
    cfg: FunnelConfig,
    base_symbols: list[str] | None = None,
    df_map: dict[str, pd.DataFrame] | None = None,
) -> tuple[list[str], list[str]]:
    """
    统计行业分布，做“行业通过率 + 行业强度中位数”分析：
    - 行业通过率 = L2行业样本数 / 基准池(L1)行业样本数
    - 行业强度 = 行业内个股短中期动量分数中位数
    注：为了避免错杀刚刚启动的威科夫吸筹/潜伏标的，
    本层不再对股票做硬性剔除，仅进行 Top 行业计算以供后续打分和标识使用。
    返回 (原始输入 symbols 列表, top_sectors 强势行业列表)。
    """
    if base_symbols is None:
        base_symbols = symbols

    counts: dict[str, int] = {}
    for sym in symbols:
        sector = sector_map.get(sym, "")
        if sector:
            counts[sector] = counts.get(sector, 0) + 1

    if not counts:
        return symbols, []

    base_counts: dict[str, int] = {}
    for sym in base_symbols:
        sector = sector_map.get(sym, "")
        if sector:
            base_counts[sector] = base_counts.get(sector, 0) + 1

    # 个股强度：20日收益(70%) + 5日收益(30%) 的截面百分位分数
    strength_map: dict[str, float] = {}
    if df_map:
        rows: list[tuple[str, float, float]] = []
        for sym in symbols:
            df = df_map.get(sym)
            if df is None or df.empty:
                continue
            s = _sorted_if_needed(df)
            close = pd.to_numeric(s.get("close"), errors="coerce").dropna()
            if len(close) <= 20:
                continue
            ret20 = (float(close.iloc[-1]) - float(close.iloc[-21])) / float(close.iloc[-21]) * 100.0
            ret5 = (float(close.iloc[-1]) - float(close.iloc[-6])) / float(close.iloc[-6]) * 100.0 if len(close) > 5 else ret20
            rows.append((sym, ret20, ret5))
        if rows:
            st_df = pd.DataFrame(rows, columns=["sym", "ret20", "ret5"])
            st_df["q20"] = st_df["ret20"].rank(pct=True, ascending=True, method="average")
            st_df["q5"] = st_df["ret5"].rank(pct=True, ascending=True, method="average")
            st_df["strength"] = 0.7 * st_df["q20"] + 0.3 * st_df["q5"]
            strength_map = st_df.set_index("sym")["strength"].astype(float).to_dict()

    ranked = sorted(counts.items(), key=lambda x: -x[1])
    min_count = max(int(cfg.sector_min_count), 1)
    q = float(cfg.sector_count_quantile)
    q = min(max(q, 0.0), 1.0)
    size_arr = np.array(list(counts.values()), dtype=float)
    q_count = int(np.ceil(np.quantile(size_arr, q))) if size_arr.size > 0 else min_count
    threshold = max(min_count, q_count)

    # 行业通过率阈值（动态）：按行业通过率分位数（默认与 sector_count_quantile 同步）
    pass_ratios: list[float] = []
    pass_ratio_map: dict[str, float] = {}
    for sec, cnt in ranked:
        base_cnt = max(int(base_counts.get(sec, 0)), 1)
        ratio = float(cnt) / float(base_cnt)
        pass_ratio_map[sec] = ratio
        pass_ratios.append(ratio)
    pass_threshold = float(np.quantile(np.array(pass_ratios, dtype=float), q)) if pass_ratios else 0.0

    # 行业强度阈值（动态）：行业内强度中位数分位阈值
    sector_strength_map: dict[str, float] = {}
    for sec, _ in ranked:
        vals = [strength_map.get(sym) for sym in symbols if sector_map.get(sym, "") == sec and sym in strength_map]
        vals = [float(v) for v in vals if v is not None]
        sector_strength_map[sec] = float(np.median(vals)) if vals else 0.0
    strength_vals = list(sector_strength_map.values())
    strength_threshold = float(np.quantile(np.array(strength_vals, dtype=float), q)) if strength_vals else 0.0

    # 小而强板块免死阈值：强度进 Top X% 时放宽数量门槛，防止概念主线被大行业吞没。
    super_q = float(getattr(cfg, "sector_super_strength_quantile", 0.90))
    super_q = min(max(super_q, 0.0), 1.0)
    super_strength_threshold = (
        float(np.quantile(np.array(strength_vals, dtype=float), super_q))
        if strength_vals
        else 0.0
    )

    keep_sectors: list[str] = []
    for s, c in ranked:
        pass_r = pass_ratio_map.get(s, 0.0)
        str_val = sector_strength_map.get(s, 0.0)
        normal_pass = (
            c >= threshold
            and pass_r >= pass_threshold
            and str_val >= strength_threshold
        )
        super_pass = (c >= min_count and str_val >= super_strength_threshold)
        if normal_pass or super_pass:
            keep_sectors.append(s)
    if not keep_sectors:
        # 极端场景兜底：至少保留样本最多的行业，避免空集。
        max_count = int(size_arr.max()) if size_arr.size > 0 else 0
        keep_sectors = [s for s, c in ranked if c == max_count]

    # Top 行业按强度排序展示，而非按数量排序，提升“主线识别”灵敏度。
    keep_sectors_sorted = sorted(
        keep_sectors,
        key=lambda s: (
            -sector_strength_map.get(s, 0.0),
            -pass_ratio_map.get(s, 0.0),
            -counts.get(s, 0),
            s,
        ),
    )
    top_n = max(int(cfg.top_n_sectors), 0)
    top_sectors = (
        keep_sectors_sorted[:top_n] if top_n > 0 else keep_sectors_sorted
    )
    
    # 威科夫重个股量价、轻板块。为了避免把底部尚未形成板块效应的吸筹股、潜伏股错杀，
    # L3 板块共振不再做硬性拦截（不剔除股票），只作特征打标签和 Top 行业计算输出。
    filtered = symbols
    return filtered, top_sectors




# Layer 4: 威科夫狙击


def _is_trading_range_context(zone: pd.DataFrame, cfg: FunnelConfig) -> bool:
    """
    Spring 必须先发生在可接受的交易区间（TR）内，避免单边下跌中的假 Spring。
    """
    if zone is None or zone.empty:
        return False
    high = pd.to_numeric(zone.get("high"), errors="coerce")
    low = pd.to_numeric(zone.get("low"), errors="coerce")
    close = pd.to_numeric(zone.get("close"), errors="coerce")
    if high.isna().all() or low.isna().all() or close.isna().all():
        return False

    high_max = float(high.max())
    low_min = float(low.min())
    if low_min <= 0:
        return False
    range_pct = (high_max - low_min) / low_min * 100.0
    if range_pct > cfg.spring_tr_max_range_pct:
        return False

    c_start = float(close.iloc[0])
    c_end = float(close.iloc[-1])
    if c_start <= 0:
        return False
    drift_pct = abs((c_end - c_start) / c_start * 100.0)
    if drift_pct > cfg.spring_tr_max_drift_pct:
        return False
    return True


def _detect_spring(df: pd.DataFrame, cfg: FunnelConfig) -> float | None:
    """
    Spring（终极震仓）：允许“前一日或当日盘中”跌破近 N 日支撑位，且当日收盘收回并放量。
    返回 score（收回幅度%）或 None。
    """
    if len(df) < cfg.spring_support_window + 2:
        return None
    df_s = _sorted_if_needed(df)
    support_zone = df_s.iloc[-(cfg.spring_support_window + 1) : -1]
    if not _is_trading_range_context(support_zone, cfg):
        return None
    support_level = support_zone["close"].min()
    prev = df_s.iloc[-2]
    last = df_s.iloc[-1]

    # 允许单日盘中洗盘（长下影锤子线）：只要 prev/last 至少一日跌破即可。
    if (prev["low"] >= support_level) and (last["low"] >= support_level):
        return None
    if last["close"] <= support_level:
        return None
    vol_avg = df_s["volume"].tail(5).iloc[:-1].mean()
    if vol_avg <= 0 or last["volume"] < vol_avg * cfg.spring_vol_ratio:
        return None
    recovery = (last["close"] - support_level) / support_level * 100
    return float(recovery)


def _detect_lps(df: pd.DataFrame, cfg: FunnelConfig) -> float | None:
    """
    LPS（最后支撑点缩量）：近 N 日回踩 MA20 且缩量。
    返回 score（缩量比）或 None。
    """
    if len(df) < max(cfg.lps_vol_ref_window, cfg.lps_ma) + cfg.lps_lookback:
        return None
    df_s = _sorted_if_needed(df)
    close = df_s["close"].astype(float)
    ma = close.rolling(cfg.lps_ma).mean()
    last_ma = ma.iloc[-1]
    if pd.isna(last_ma) or last_ma <= 0:
        return None

    recent = df_s.tail(cfg.lps_lookback)
    last_close = close.iloc[-1]
    if last_close < last_ma:
        return None

    low_near_ma = recent["low"].min()
    if abs(low_near_ma - last_ma) / last_ma > cfg.lps_ma_tolerance:
        return None

    recent_max_vol = recent["volume"].max()
    ref_max_vol = df_s["volume"].tail(cfg.lps_vol_ref_window).max()
    if ref_max_vol <= 0:
        return None
    vol_ratio = recent_max_vol / ref_max_vol
    if vol_ratio > cfg.lps_vol_dry_ratio:
        return None
    return float(vol_ratio)


def _detect_evr(df: pd.DataFrame, cfg: FunnelConfig) -> float | None:
    """
    Effort vs Result（努力无结果）：
    仅识别“相对低位的巨量滞涨/抗跌”，排除高位派发。
    返回 score（量比）或 None。
    """
    min_required = cfg.evr_vol_window + 2 + max(int(cfg.evr_confirm_days), 0)
    if len(df) < min_required:
        return None
    df_s = _sorted_if_needed(df)

    close = pd.to_numeric(df_s["close"], errors="coerce")
    low = pd.to_numeric(df_s["low"], errors="coerce")
    volume = pd.to_numeric(df_s["volume"], errors="coerce")
    pct_chg = pd.to_numeric(df_s["pct_chg"], errors="coerce")
    if close.isna().all() or low.isna().all() or volume.isna().all() or pct_chg.isna().all():
        return None

    # 位阶保护：高位放量优先按派发处理，避免 EVR 误判
    ma200 = close.rolling(200).mean()
    ma200_last = ma200.iloc[-1]
    close_last = close.iloc[-1]
    if pd.notna(ma200_last) and pd.notna(close_last) and float(ma200_last) > 0:
        bias_200 = (float(close_last) - float(ma200_last)) / float(ma200_last) * 100.0
        if bias_200 > float(cfg.evr_max_bias_200):
            return None

    # 基准量能取“最近窗口但剔除最后两天”，避免当前异动污染基线
    vol_ref = volume.tail(cfg.evr_vol_window).iloc[:-2]
    vol_ref_avg = float(vol_ref.mean()) if not vol_ref.empty else 0.0
    if vol_ref_avg <= 0:
        return None

    confirm_days = max(int(cfg.evr_confirm_days), 0)
    candidate_idx = (-2,) if confirm_days > 0 else (-1, -2)

    # 默认要求“放量滞涨”后至少 1 天确认，不再当日立即上报。
    for idx in candidate_idx:
        vol_ratio = float(volume.iloc[idx] / vol_ref_avg) if vol_ref_avg > 0 else 0.0
        if vol_ratio < cfg.evr_vol_ratio:
            continue

        day_pct = pct_chg.iloc[idx]
        if pd.isna(day_pct):
            continue

        # 结果约束：剔除大阴线/大阳线，保留“努力无结果”的滞涨/抗跌
        if float(day_pct) < -cfg.evr_max_drop or float(day_pct) > 3.0:
            continue

        # 结构约束：最新收盘不能明显弱于三天前（防止下跌中继）
        if len(close) >= 4:
            close_3d_ago = close.iloc[-4]
            if (
                pd.notna(close_3d_ago)
                and float(close_last) < float(close_3d_ago) * 0.98
            ):
                continue

        if confirm_days > 0:
            event_pos = len(df_s) + idx
            confirm_start = event_pos + 1
            confirm_end = confirm_start + confirm_days
            if confirm_end > len(df_s):
                continue
            event_low = low.iloc[idx]
            confirm_close = close.iloc[confirm_start:confirm_end]
            if pd.isna(event_low) or confirm_close.empty or confirm_close.isna().all():
                continue
            min_confirm_close = float(confirm_close.min())
            allow_break = max(float(cfg.evr_confirm_allow_break_pct), 0.0) / 100.0
            if min_confirm_close < float(event_low) * (1.0 - allow_break):
                continue
        return vol_ratio

    return None


def _detect_sos(df: pd.DataFrame, cfg: FunnelConfig) -> float | None:
    """
    Sign of Strength (SOS) / Jump Across the Creek (JAC):
    点火标志。特征为低位脱盘、放量大阳线，破除重要阻力或近期高点。
    返回 score（量比）或 None。
    """
    if len(df) < max(cfg.sos_vol_window, cfg.sos_breakout_window, 200) + 2:
        # Fallback to a smaller necessary length if 200 is too strict, but MA200 needs 200 days
        # We handle MA200 dynamically inside
        pass
        
    if len(df) < max(cfg.sos_vol_window, cfg.sos_breakout_window) + 2:
        return None

    df_s = _sorted_if_needed(df)
    
    close = pd.to_numeric(df_s["close"], errors="coerce")
    volume = pd.to_numeric(df_s["volume"], errors="coerce")
    pct_chg = pd.to_numeric(df_s["pct_chg"], errors="coerce")
    high = pd.to_numeric(df_s["high"], errors="coerce")
    
    if close.isna().all() or volume.isna().all() or pct_chg.isna().all():
        return None
        
    # 位阶保护：高位爆量很大可能是 Buying Climax（派发），排除极大乖离
    close_last = close.iloc[-1]
    if len(close) >= 200:
        ma200 = close.rolling(200).mean()
        ma200_last = ma200.iloc[-1]
        if pd.notna(ma200_last) and pd.notna(close_last) and float(ma200_last) > 0:
            bias_200 = (float(close_last) - float(ma200_last)) / float(ma200_last) * 100.0
            if bias_200 > float(cfg.sos_max_bias_200):
                return None
            
    # 只看当天（威科夫点火通常是当天的明显大阳线）
    day_pct = float(pct_chg.iloc[-1])
    if pd.isna(day_pct) or day_pct < cfg.sos_pct_min:
        return None
        
    # 量能要求：暴击量
    vol_ref = volume.tail(cfg.sos_vol_window + 1).iloc[:-1]
    vol_ref_avg = float(vol_ref.mean()) if not vol_ref.empty else 0.0
    if vol_ref_avg <= 0:
        return None
    vol_ratio = float(volume.iloc[-1]) / vol_ref_avg
    if vol_ratio < cfg.sos_vol_ratio:
        return None
        
    # 结构突破要求：创N日新高，或强势穿透季线/半年线
    ma50 = close.rolling(50).mean()
    ma50_last = ma50.iloc[-1] if not ma50.empty else None
    
    recent_highs = high.tail(cfg.sos_breakout_window + 1).iloc[:-1]
    max_recent_high = float(recent_highs.max()) if not recent_highs.empty else float("inf")
    
    # 接近或突破近期高点 (容差2%)
    is_breakout = float(close_last) >= max_recent_high * 0.98  
    
    is_ma_crossover = False
    if ma50_last is not None and pd.notna(ma50_last):
        prev_close = float(close.iloc[-2])
        if prev_close <= float(ma50_last) and float(close_last) > float(ma50_last):
            is_ma_crossover = True
            
    if not (is_breakout or is_ma_crossover):
        return None
        
    return vol_ratio


def layer4_triggers(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
) -> dict[str, list[tuple[str, float]]]:
    """
    在最终候选集上运行 Spring / LPS / EffortVsResult 检测。
    """
    results: dict[str, list[tuple[str, float]]] = {
        "sos": [],
        "spring": [],
        "lps": [],
        "evr": [],
    }
    for sym in symbols:
        df = df_map.get(sym)
        if df is None or df.empty:
            continue
        score = _detect_spring(df, cfg)
        if score is not None:
            results["spring"].append((sym, score))
        score = _detect_lps(df, cfg)
        if score is not None:
            results["lps"].append((sym, score))
        if getattr(cfg, "enable_evr_trigger", False):
            score = _detect_evr(df, cfg)
            if score is not None:
                results["evr"].append((sym, score))
        score = _detect_sos(df, cfg)
        if score is not None:
            results["sos"].append((sym, score))
    return results




# Layer 2.5: Markup 阶段识别


def _detect_markup_entry(df: pd.DataFrame, cfg: FunnelConfig) -> float | None:
    """
    Markup 阶段：MA50 从下穿上 MA200，且在上方保持 N 日，确认进入上升趋势。
    返回 score（确认天数占比）或 None。
    """
    if len(df) < max(cfg.ma_long, cfg.markup_ma_crossover_confirm_days) + 5:
        return None

    df_s = _sorted_if_needed(df)
    close = df_s["close"].astype(float)
    ma_short = close.rolling(cfg.ma_short).mean()
    ma_long = close.rolling(cfg.ma_long).mean()

    if (
        ma_short.isna().any() or ma_long.isna().any()
        or ma_short.iloc[-1] <= ma_long.iloc[-1]
    ):
        return None

    # 检查过去 N 日内 MA50 是否从下穿上 MA200
    lookback = max(int(cfg.markup_ma_crossover_confirm_days * 2), 10)
    if len(ma_short) < lookback:
        return None

    recent_ma_short = ma_short.tail(lookback).values
    recent_ma_long = ma_long.tail(lookback).values

    # 寻找穿过点
    crossover_found = False
    for i in range(1, len(recent_ma_short)):
        if (
            recent_ma_short[i - 1] <= recent_ma_long[i - 1]
            and recent_ma_short[i] > recent_ma_long[i]
        ):
            crossover_found = True
            break

    if not crossover_found:
        return None

    # 确认最近 N 日持续在 MA200 上方
    confirm_days = max(int(cfg.markup_ma_crossover_confirm_days), 1)
    recent_above = sum(
        1 for j in range(-confirm_days, 0)
        if ma_short.iloc[j] > ma_long.iloc[j]
    )

    if recent_above < confirm_days:
        return None

    # 计算 MA50 的角度（过去 5 日的变化率）
    ma_short_recent = ma_short.tail(6).values
    if len(ma_short_recent) < 2:
        return None

    angle = (ma_short_recent[-1] - ma_short_recent[0]) / ma_short_recent[0] * 100.0
    if angle < cfg.markup_ma_angle_min:
        return None

    return float(recent_above / confirm_days)


def detect_markup_stage(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
) -> list[str]:
    """
    返回已进入 Markup 阶段的股票。
    """
    if not cfg.enable_markup_detection:
        return []

    markup: list[str] = []
    for sym in symbols:
        df = df_map.get(sym)
        if df is None or df.empty:
            continue
        score = _detect_markup_entry(df, cfg)
        if score is not None:
            markup.append(sym)

    return markup




# Layer 2 增强: Accumulation ABC 细化


def _analyze_accum_stage(df: pd.DataFrame, cfg: FunnelConfig) -> str | None:
    """
    分析 Accumulation 内部的三个子阶段：
    - A: 下跌停止，量能萎缩
    - B: 底部区间反复测试
    - C: 小幅下跌不破 A 低，量能再度萎缩

    返回 "Accum_A"、"Accum_B"、"Accum_C" 或 None。
    """
    if len(df) < max(
        cfg.accum_lookback_days, cfg.accum_vol_dry_ref_window, cfg.accum_range_window
    ):
        return None

    df_s = _sorted_if_needed(df)
    close = pd.to_numeric(df_s["close"], errors="coerce")
    low = pd.to_numeric(df_s["low"], errors="coerce")
    high = pd.to_numeric(df_s["high"], errors="coerce")
    volume = pd.to_numeric(df_s["volume"], errors="coerce")

    last_close = close.iloc[-1]

    # 条件 1: 低位区——现价在年内低点 +35% 以内
    lookback_w = max(int(cfg.accum_lookback_days), 2)
    period_low = float(low.tail(lookback_w).min())
    if period_low <= 0 or last_close > period_low * (1.0 + cfg.accum_price_from_low_max):
        return None

    accum_base_low = period_low

    # 条件 2: 均线胶着（尚未多头排列）
    ma_short = close.rolling(cfg.ma_short).mean()
    ma_long = close.rolling(cfg.ma_long).mean()
    last_ma_short = ma_short.iloc[-1]
    last_ma_long = ma_long.iloc[-1]

    if (
        pd.isna(last_ma_short)
        or pd.isna(last_ma_long)
        or float(last_ma_long) <= 0
    ):
        return None

    ma_gap = abs(float(last_ma_short) - float(last_ma_long)) / float(last_ma_long)
    if ma_gap > cfg.accum_ma_gap_max:
        return None

    # 条件 3: 量能萎缩
    dw = max(int(cfg.accum_vol_dry_window), 2)
    rfw = max(int(cfg.accum_vol_dry_ref_window), dw + 1)
    recent_vol_mean = float(volume.tail(dw).mean()) if len(volume) >= dw else 0.0
    ref_vol_mean = float(volume.tail(rfw).iloc[:-dw].mean()) if len(volume) >= rfw else 0.0

    if ref_vol_mean <= 0 or recent_vol_mean / ref_vol_mean >= cfg.accum_vol_dry_ratio:
        return None

    # 现在确定是 A、B 还是 C
    # B 阶段特征：近期有多次测试底部（高低点逐渐走高）
    rw = max(int(cfg.accum_range_window), 5)
    zone = df_s.tail(rw)
    zone_high = pd.to_numeric(zone.get("high"), errors="coerce")
    zone_low = pd.to_numeric(zone.get("low"), errors="coerce")

    if zone_high.empty or zone_low.empty:
        return "Accum_A"

    # 分割测试：最近 N 日内，有多少天的低点接近底部（±5%）
    test_count = sum(
        1
        for l in zone_low.dropna()
        if abs(l - accum_base_low) / accum_base_low <= 0.05
    )

    if test_count >= cfg.accum_b_test_count:
        return "Accum_B"

    # C 阶段：最近有小幅下跌但不破底，且量能再度萎缩
    recent_lookback = min(20, len(df_s))
    recent = df_s.tail(recent_lookback)
    recent_low = pd.to_numeric(recent.get("low"), errors="coerce").min()

    c_stage_ok = (
        recent_low >= accum_base_low * (1.0 - cfg.accum_c_max_drop_ratio)
    )

    if c_stage_ok and recent_vol_mean < ref_vol_mean * cfg.accum_vol_dry_ratio:
        return "Accum_C"

    return "Accum_A"


def detect_accum_stage(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    cfg: FunnelConfig,
) -> dict[str, str]:
    """
    返回 symbol -> stage 的映射。
    """
    if not cfg.enable_accum_abc_detail:
        return {}

    result: dict[str, str] = {}
    for sym in symbols:
        df = df_map.get(sym)
        if df is None or df.empty:
            continue
        stage = _analyze_accum_stage(df, cfg)
        if stage is not None:
            result[sym] = stage

    return result




# Layer 5: Exit 策略


def _detect_profit_target(
    df: pd.DataFrame, accum_base_low: float, cfg: FunnelConfig
) -> float | None:
    """
    从 Accumulation 底部向上计算目标位。
    返回建议止盈价格或 None。
    """
    target_pct = cfg.exit_profit_target_pct / 100.0
    return accum_base_low * (1.0 + target_pct)


def _detect_distribution_start(df: pd.DataFrame, cfg: FunnelConfig) -> bool:
    """
    Distribution 阶段识别：高位缩量警告。
    触发条件：
    1. 价格相对 MA200 处于高位（>30%）
    2. 连续 N 日的成交量 < 参考均量的 50%
    """
    if len(df) < max(cfg.ma_long, cfg.dist_confirm_days) + 20:
        return False

    df_s = _sorted_if_needed(df)
    close = df_s["close"].astype(float)
    volume = df_s["volume"].astype(float)

    ma_long = close.rolling(cfg.ma_long).mean()
    last_ma_long = ma_long.iloc[-1]
    last_close = close.iloc[-1]

    if pd.isna(last_ma_long) or pd.isna(last_close) or last_ma_long <= 0:
        return False

    bias = (last_close - last_ma_long) / last_ma_long * 100.0
    if bias < cfg.dist_high_threshold_pct:
        return False

    # 检查近 N 日的缩量
    ref_vol = volume.tail(60).mean()
    recent_vol = volume.tail(cfg.dist_confirm_days).mean()

    if ref_vol <= 0:
        return False

    if recent_vol / ref_vol > cfg.dist_vol_dry_ratio:
        return False

    return True


def layer5_exit_signals(
    symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    accum_stage_map: dict[str, str],
    cfg: FunnelConfig,
) -> dict[str, dict]:
    """
    为 Accumulation 和 Markup 阶段股票生成静态参考 Exit 信号（实际实盘止损在 OMS 中独立维护）。
    返回 {symbol: {signal: ..., price: ..., reason: ...}}
    """
    if not cfg.enable_exit_signals:
        return {}

    signals: dict[str, dict] = {}

    for sym in symbols:
        df = df_map.get(sym)
        if df is None or df.empty:
            continue

        df_s = _sorted_if_needed(df)
        close = pd.to_numeric(df_s["close"], errors="coerce")
        low = pd.to_numeric(df_s["low"], errors="coerce")
        high = pd.to_numeric(df_s["high"], errors="coerce")

        if close.empty or low.empty or high.empty:
            continue

        last_close = float(close.iloc[-1])
        stage = accum_stage_map.get(sym, "Markup") # 默认按主升处理

        profit_target = None
        stop_loss_price = None

        if stage.startswith("Accum_"):
            # 对于吸筹股，锚定“年内最低点”
            lookback_w = max(int(cfg.accum_lookback_days), 2)
            accum_low = float(low.tail(lookback_w).min())
            
            # 止盈目标
            profit_target = _detect_profit_target(df_s, accum_low, cfg)
            # 止损：底部跌破 8%
            stop_loss_price = accum_low * (1.0 + cfg.exit_stop_loss_pct / 100.0)
        else:
            # 对于 Markup 强势主升股，锚定“近期高点”向下计算跟踪止损 或 跌破 MA50
            # 避免使用一年前的水下最低价
            ma_short = close.rolling(cfg.ma_short).mean().iloc[-1]
            recent_high = float(high.tail(60).max())
            if not pd.isna(ma_short):
                # 以 MA50 和 近期高点回撤较大者作为跟踪止损
                stop_loss_price = max(float(ma_short) * 0.98, recent_high * (1.0 + cfg.exit_stop_loss_pct / 100.0))
            else:
                stop_loss_price = recent_high * (1.0 + cfg.exit_stop_loss_pct / 100.0)
            
            # 主升股通常不设硬止盈目标（让利润奔跑直到跌破跟踪止损），此处设为 None 或很高

        if profit_target is not None and last_close >= profit_target:
            signals[sym] = {
                "signal": "profit_target",
                "price": profit_target,
                "current": last_close,
                "reason": f"已达估测底部利润目标（+{cfg.exit_profit_target_pct:.0f}%）",
            }
            continue

        if stop_loss_price is not None and last_close <= stop_loss_price:
            signals[sym] = {
                "signal": "stop_loss",
                "price": stop_loss_price,
                "current": last_close,
                "reason": f"破位防守（{stage}）",
            }
            continue

        # Distribution 警告 (主要针对高位股)
        if _detect_distribution_start(df_s, cfg):
            signals[sym] = {
                "signal": "distribution_warning",
                "reason": "检测到高位 Distribution 阶段迹象（放量不涨/高位缩量）",
            }

    return signals




# run_funnel: 串联 4 层


def run_funnel(
    all_symbols: list[str],
    df_map: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame | None,
    name_map: dict[str, str],
    market_cap_map: dict[str, float],
    sector_map: dict[str, str],
    cfg: FunnelConfig | None = None,
) -> FunnelResult:
    if cfg is None:
        cfg = FunnelConfig()

    # 预先整理时序，避免各层重复 sort/copy 产生大量临时对象。
    prepared_df_map: dict[str, pd.DataFrame] = {
        sym: _sorted_if_needed(df)
        for sym, df in df_map.items()
        if df is not None and not df.empty
    }

    l1 = layer1_filter(all_symbols, name_map, market_cap_map, prepared_df_map, cfg)
    l2 = layer2_strength(l1, prepared_df_map, bench_df, cfg)
    l3, top_sectors = layer3_sector_resonance(
        l2,
        sector_map,
        cfg,
        base_symbols=l1,
        df_map=prepared_df_map,
    )
    triggers = layer4_triggers(l3, prepared_df_map, cfg)

    # 新增：阶段识别和退出信号
    markup_symbols = detect_markup_stage(l3, prepared_df_map, cfg)
    accum_stage_map = detect_accum_stage(l2, prepared_df_map, cfg)  # 对 L2 做细化分析

    # 构建完整的 stage_map（包括 Markup）
    stage_map: dict[str, str] = accum_stage_map.copy()
    for sym in markup_symbols:
        stage_map[sym] = "Markup"

    # 退出信号针对 L2 和 Markup 股票
    exit_signals = layer5_exit_signals(
        l2 + markup_symbols, prepared_df_map, accum_stage_map, cfg
    )

    return FunnelResult(
        layer1_symbols=l1,
        layer2_symbols=l2,
        layer3_symbols=l3,
        top_sectors=top_sectors,
        triggers=triggers,
        stage_map=stage_map,
        markup_symbols=markup_symbols,
        exit_signals=exit_signals,
    )
