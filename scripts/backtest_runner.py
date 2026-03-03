# -*- coding: utf-8 -*-
"""
日线级轻量回测器（低成本数据版）

目标：
1) 复用当前 Wyckoff Funnel 规则，不依赖分钟级或付费 Level-2 数据。
2) 在给定历史区间内，统计信号后 N 交易日收益分布与胜率。
3) 输出 summary markdown + trades csv，便于后续参数复盘。
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.wyckoff_engine import FunnelConfig, normalize_hist_from_fetch, run_funnel
from integrations.data_source import fetch_index_hist, fetch_market_cap_map, fetch_sector_map, fetch_stock_hist
from integrations.fetch_a_share_csv import get_stocks_by_board, _normalize_symbols


@dataclass
class TradeRecord:
    signal_date: date
    exit_date: date
    code: str
    name: str
    trigger: str
    score: float
    entry_close: float
    exit_close: float
    ret_pct: float


def _parse_date(v: str) -> date:
    s = str(v).strip().replace("/", "-")
    if "-" in s:
        return datetime.strptime(s, "%Y-%m-%d").date()
    return datetime.strptime(s, "%Y%m%d").date()


def _build_universe(board: str, sample_size: int) -> tuple[list[str], dict[str, str]]:
    if board == "main":
        items = get_stocks_by_board("main")
    elif board == "chinext":
        items = get_stocks_by_board("chinext")
    else:
        merged: dict[str, str] = {}
        for item in get_stocks_by_board("main") + get_stocks_by_board("chinext"):
            code = str(item.get("code", "")).strip()
            if not code:
                continue
            if code not in merged:
                merged[code] = str(item.get("name", "")).strip()
        items = [{"code": c, "name": n} for c, n in merged.items()]

    name_map = {
        str(x.get("code", "")).strip(): str(x.get("name", "")).strip()
        for x in items
        if str(x.get("code", "")).strip()
    }
    # 先过滤 ST，再采样（可复现）
    symbols = [
        s
        for s in _normalize_symbols(list(name_map.keys()))
        if "ST" not in name_map.get(s, "").upper()
    ]
    symbols = sorted(set(symbols))
    if sample_size > 0:
        symbols = symbols[:sample_size]
    return symbols, name_map


def _fetch_hist_norm(
    symbol: str,
    start_dt: date,
    end_dt: date,
) -> tuple[str, pd.DataFrame | None, str | None]:
    try:
        raw = fetch_stock_hist(symbol, start_dt, end_dt, adjust="qfq")
        df = normalize_hist_from_fetch(raw)
        if df is None or df.empty:
            return symbol, None, "empty"
        out = df.sort_values("date").copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
        out = out.dropna(subset=["date"]).reset_index(drop=True)
        if out.empty:
            return symbol, None, "empty_after_date_parse"
        return symbol, out, None
    except Exception as exc:  # pragma: no cover - runtime path
        return symbol, None, str(exc)


def _combine_trigger_scores(triggers: dict[str, list[tuple[str, float]]]) -> dict[str, tuple[float, str]]:
    """
    合并 spring/lps/evr 触发结果：
    返回 code -> (best_score, joined_trigger_name)
    """
    reason_map: dict[str, list[str]] = {}
    score_map: dict[str, float] = {}
    for key, pairs in triggers.items():
        for code, score in pairs:
            if code not in reason_map:
                reason_map[code] = []
                score_map[code] = float(score)
            reason_map[code].append(key)
            score_map[code] = max(score_map.get(code, 0.0), float(score))
    out: dict[str, tuple[float, str]] = {}
    for code, reasons in reason_map.items():
        out[code] = (score_map.get(code, 0.0), "、".join(reasons))
    return out


def _close_on_date(df: pd.DataFrame, d: date) -> float | None:
    row = df[df["date"] == d]
    if row.empty:
        return None
    v = pd.to_numeric(row["close"], errors="coerce").dropna()
    if v.empty:
        return None
    return float(v.iloc[-1])


def _close_on_or_after(df: pd.DataFrame, d: date) -> tuple[float | None, date | None]:
    row = df[df["date"] >= d].head(1)
    if row.empty:
        return None, None
    v = pd.to_numeric(row["close"], errors="coerce").dropna()
    if v.empty:
        return None, None
    hit_date = row.iloc[0]["date"]
    return float(v.iloc[0]), hit_date


def run_backtest(
    start_dt: date,
    end_dt: date,
    hold_days: int,
    top_n: int,
    board: str,
    sample_size: int,
    trading_days: int,
    max_workers: int,
) -> tuple[pd.DataFrame, dict]:
    if end_dt <= start_dt:
        raise ValueError("end 必须晚于 start")
    if hold_days < 1:
        raise ValueError("hold_days 必须 >= 1")

    symbols, name_map = _build_universe(board=board, sample_size=sample_size)
    if not symbols:
        raise RuntimeError("股票池为空")
    print(f"[backtest] 股票池={len(symbols)} (board={board}, sample_size={sample_size})")

    prefetch_start = start_dt - timedelta(days=trading_days * 3)
    prefetch_end = end_dt + timedelta(days=hold_days * 3 + 30)

    try:
        bench_raw = fetch_index_hist("000001", prefetch_start, prefetch_end)
    except Exception as exc:
        raise RuntimeError(
            "回测需要大盘交易日历与基准收益，请先配置可用的 TUSHARE_TOKEN。"
        ) from exc
    bench_df = normalize_hist_from_fetch(bench_raw).sort_values("date").copy()
    bench_df["date"] = pd.to_datetime(bench_df["date"], errors="coerce").dt.date
    bench_df = bench_df.dropna(subset=["date"]).reset_index(drop=True)
    trade_dates = [d for d in bench_df["date"].tolist() if start_dt <= d <= end_dt]
    if len(trade_dates) <= hold_days:
        raise RuntimeError("回测区间交易日过少，无法计算 forward return")

    # 全量历史一次拉取，后续只做日期切片
    all_df_map: dict[str, pd.DataFrame] = {}
    failures: list[str] = []
    print(f"[backtest] 开始拉取历史日线: symbols={len(symbols)}, workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max(int(max_workers), 1)) as ex:
        futures = {
            ex.submit(_fetch_hist_norm, sym, prefetch_start, prefetch_end): sym for sym in symbols
        }
        done = 0
        for ft in as_completed(futures):
            done += 1
            sym = futures[ft]
            code, df, err = ft.result()
            if df is not None and not df.empty:
                all_df_map[code] = df
            else:
                failures.append(f"{sym}:{err or 'unknown'}")
            if done % 200 == 0 or done == len(futures):
                print(f"[backtest] 拉取进度 {done}/{len(futures)}")
    print(f"[backtest] 历史拉取完成: ok={len(all_df_map)}, fail={len(failures)}")

    market_cap_map = fetch_market_cap_map()
    sector_map = fetch_sector_map()
    cfg = FunnelConfig(trading_days=trading_days)

    records: list[TradeRecord] = []
    signal_days = 0
    eval_days = 0

    max_idx = len(trade_dates) - hold_days
    for idx in range(max_idx):
        signal_date = trade_dates[idx]
        exit_anchor_date = trade_dates[idx + hold_days]

        # 各票截止到 signal_date 的切片（滚动窗口）
        day_df_map: dict[str, pd.DataFrame] = {}
        for code, df in all_df_map.items():
            s = df[df["date"] <= signal_date]
            if s.empty:
                continue
            tail = s.tail(trading_days)
            if len(tail) < cfg.ma_long:
                continue
            day_df_map[code] = tail
        if not day_df_map:
            continue

        bench_slice = bench_df[bench_df["date"] <= signal_date].tail(trading_days)
        if len(bench_slice) < cfg.ma_long:
            continue

        eval_days += 1
        result = run_funnel(
            all_symbols=list(day_df_map.keys()),
            df_map=day_df_map,
            bench_df=bench_slice,
            name_map=name_map,
            market_cap_map=market_cap_map,
            sector_map=sector_map,
            cfg=cfg,
        )
        score_map = _combine_trigger_scores(result.triggers)
        if not score_map:
            continue

        ranked_codes = sorted(score_map.keys(), key=lambda c: -score_map[c][0])[:top_n]
        signal_days += 1
        for code in ranked_codes:
            full_df = all_df_map.get(code)
            if full_df is None or full_df.empty:
                continue
            entry_close = _close_on_date(full_df, signal_date)
            if entry_close is None or entry_close <= 0:
                continue
            exit_close, exit_date = _close_on_or_after(full_df, exit_anchor_date)
            if exit_close is None or exit_date is None:
                continue
            ret_pct = (exit_close - entry_close) / entry_close * 100.0
            score, trigger_name = score_map[code]
            records.append(
                TradeRecord(
                    signal_date=signal_date,
                    exit_date=exit_date,
                    code=code,
                    name=name_map.get(code, code),
                    trigger=trigger_name,
                    score=score,
                    entry_close=entry_close,
                    exit_close=exit_close,
                    ret_pct=ret_pct,
                )
            )

        if (idx + 1) % 20 == 0 or (idx + 1) == max_idx:
            print(f"[backtest] 回放进度 {idx + 1}/{max_idx}, trades={len(records)}")

    trades_df = pd.DataFrame([r.__dict__ for r in records])
    summary = {
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "hold_days": hold_days,
        "top_n": top_n,
        "board": board,
        "sample_size": sample_size,
        "trading_days": trading_days,
        "universe_ok": len(all_df_map),
        "universe_fail": len(failures),
        "eval_days": eval_days,
        "signal_days": signal_days,
        "trades": len(trades_df),
    }
    if not trades_df.empty:
        ret = pd.to_numeric(trades_df["ret_pct"], errors="coerce").dropna()
        summary.update(
            {
                "win_rate_pct": float((ret > 0).mean() * 100.0),
                "avg_ret_pct": float(ret.mean()),
                "median_ret_pct": float(ret.median()),
                "q25_ret_pct": float(ret.quantile(0.25)),
                "q75_ret_pct": float(ret.quantile(0.75)),
            }
        )
    else:
        summary.update(
            {
                "win_rate_pct": None,
                "avg_ret_pct": None,
                "median_ret_pct": None,
                "q25_ret_pct": None,
                "q75_ret_pct": None,
            }
        )
    return trades_df, summary


def _fmt_metric(v: float | int | str | None, ndigits: int = 3) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.{ndigits}f}"
    return str(v)


def _build_summary_md(summary: dict) -> str:
    return "\n".join(
        [
            "# Wyckoff Funnel Daily Backtest",
            "",
            f"- 区间: {summary.get('start')} ~ {summary.get('end')}",
            f"- 持有周期: {summary.get('hold_days')} 交易日",
            f"- 每日选股: Top {summary.get('top_n')}",
            f"- 股票池: {summary.get('board')} (sample={summary.get('sample_size')})",
            f"- 评估交易日: {summary.get('eval_days')}",
            f"- 触发交易日: {summary.get('signal_days')}",
            f"- 成交样本: {summary.get('trades')}",
            "",
            "## 收益统计",
            f"- 胜率: {_fmt_metric(summary.get('win_rate_pct'), 2)}%",
            f"- 平均收益: {_fmt_metric(summary.get('avg_ret_pct'), 3)}%",
            f"- 中位收益: {_fmt_metric(summary.get('median_ret_pct'), 3)}%",
            f"- 25%分位: {_fmt_metric(summary.get('q25_ret_pct'), 3)}%",
            f"- 75%分位: {_fmt_metric(summary.get('q75_ret_pct'), 3)}%",
            "",
            "## 说明",
            "- 该回测仅使用日线数据（qfq），不含盘口、滑点、涨跌停成交约束。",
            "- 市值/行业映射采用当前快照，历史回放属于近似估计（用于参数方向验证）。",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Wyckoff Funnel 日线轻量回测器")
    parser.add_argument("--start", required=True, help="起始日期: YYYY-MM-DD 或 YYYYMMDD")
    parser.add_argument("--end", required=True, help="结束日期: YYYY-MM-DD 或 YYYYMMDD")
    parser.add_argument("--hold-days", type=int, default=5, help="持有交易日数 (default: 5)")
    parser.add_argument("--top-n", type=int, default=6, help="每日最多纳入交易样本的股票数 (default: 6)")
    parser.add_argument("--board", choices=["all", "main", "chinext"], default="all")
    parser.add_argument("--sample-size", type=int, default=300, help="股票池采样数量，0 表示不采样")
    parser.add_argument("--trading-days", type=int, default=500, help="单次筛选回看交易日数")
    parser.add_argument("--workers", type=int, default=8, help="历史拉取并发数")
    parser.add_argument(
        "--output-dir",
        default="analysis/backtest",
        help="输出目录（会写 summary.md 与 trades.csv）",
    )
    args = parser.parse_args()

    start_dt = _parse_date(args.start)
    end_dt = _parse_date(args.end)
    trades_df, summary = run_backtest(
        start_dt=start_dt,
        end_dt=end_dt,
        hold_days=args.hold_days,
        top_n=args.top_n,
        board=args.board,
        sample_size=args.sample_size,
        trading_days=args.trading_days,
        max_workers=args.workers,
    )

    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = f"{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}_h{args.hold_days}_n{args.top_n}"
    summary_path = out_dir / f"summary_{stamp}.md"
    trades_path = out_dir / f"trades_{stamp}.csv"

    summary_md = _build_summary_md(summary)
    summary_path.write_text(summary_md + "\n", encoding="utf-8")
    trades_df.to_csv(trades_path, index=False, encoding="utf-8-sig")

    print(summary_md)
    print("")
    print(f"[backtest] summary -> {summary_path}")
    print(f"[backtest] trades  -> {trades_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
