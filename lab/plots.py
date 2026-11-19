"""Deterministic, zero-dependency SVG plots generated directly from raw M3 run evidence."""

from __future__ import annotations

from html import escape
import math
from typing import Any


BG_COLOR = "#ffffff"
PANEL_BG = "#f8fafc"
BORDER_COLOR = "#cbd5e1"
GRID_COLOR = "#e2e8f0"
TEXT_COLOR = "#0f172a"
MUTED_COLOR = "#475569"
BASELINE_COLOR = "#2563eb"
REPACK_COLOR = "#d97706"
PLAIN_COLOR = "#dc2626"
SHADE_COLOR = "#fef3c7"
PLAIN_SHADE_COLOR = "#fee2e2"


def _nice_upper(val: float, *, min_upper: float = 10.0) -> float:
    if val <= 0:
        return min_upper
    target = max(val * 1.15, min_upper)
    exp = math.floor(math.log10(target))
    base = 10.0 ** exp
    for mult in (1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0):
        cand = mult * base
        if cand >= target:
            return cand
    return 10.0 * base


def _svg_open(width: int, height: int, title: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">',
        f"<title>{escape(title)}</title>",
        "<style>",
        "text { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }",
        "</style>",
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="{BG_COLOR}" />',
    ]


def render_latency_comparison_svg(agg: dict[str, Any]) -> str:
    """Render matched-interval and full-window latency comparison across pairs (median + per-pair points)."""
    width = 960
    control = agg.get("plain_repack_control")
    height = 560 if control else 480
    lines = _svg_open(width, height, "Host Writer Latency: Baseline vs Concurrent REPACK")

    lines.append(
        f'<text x="24" y="30" font-size="18" font-weight="700" fill="{TEXT_COLOR}">'
        "Writer Latency Across Matched Pairs (This Host Only)</text>"
    )
    lines.append(
        f'<text x="24" y="49" font-size="12" fill="{MUTED_COLOR}">'
        "Bars show median across matched pairs; circles show individual pair values. "
        "Scheduled-to-completed (S2C) and execution (Exec) latency in ms.</text>"
    )

    # Legend
    lx = 675
    lines.append(f'<rect x="{lx}" y="18" width="12" height="12" rx="2" fill="{BASELINE_COLOR}" />')
    lines.append(f'<text x="{lx + 18}" y="28" font-size="12" fill="{TEXT_COLOR}">Baseline</text>')
    lines.append(f'<rect x="{lx + 92}" y="18" width="12" height="12" rx="2" fill="{REPACK_COLOR}" />')
    lines.append(
        f'<text x="{lx + 110}" y="28" font-size="12" fill="{TEXT_COLOR}">Concurrent REPACK</text>'
    )

    host_agg = agg["host_performance_summary"]
    panels = [
        (
            "Matched REPACK Interval (scheduled in interval)",
            [
                ("Exec p50", host_agg["matched_interval"]["exec_p50_ms"]),
                ("S2C p50", host_agg["matched_interval"]["sched_to_complete_p50_ms"]),
                ("Exec p99", host_agg["matched_interval"]["exec_p99_ms"]),
                ("S2C p99", host_agg["matched_interval"]["sched_to_complete_p99_ms"]),
                ("S2C max", host_agg["matched_interval"]["sched_to_complete_max_ms"]),
            ],
            24,
            70,
            444,
            370,
        ),
        (
            "Full Measurement Window (180 s)",
            [
                ("Exec p50", host_agg["full_window"]["exec_p50_ms"]),
                ("S2C p50", host_agg["full_window"]["sched_to_complete_p50_ms"]),
                ("Exec p99", host_agg["full_window"]["exec_p99_ms"]),
                ("S2C p99", host_agg["full_window"]["sched_to_complete_p99_ms"]),
                ("S2C max", host_agg["full_window"]["sched_to_complete_max_ms"]),
            ],
            492,
            70,
            444,
            370,
        ),
    ]

    for panel_title, metrics, px, py, pw, ph in panels:
        lines.append(
            f'<rect x="{px}" y="{py}" width="{pw}" height="{ph}" rx="6" '
            f'fill="{PANEL_BG}" stroke="{BORDER_COLOR}" />'
        )
        lines.append(
            f'<text x="{px + 14}" y="{py + 22}" font-size="13" font-weight="600" fill="{TEXT_COLOR}">'
            f"{escape(panel_title)}</text>"
        )

        # Compute y-axis upper bound from all baseline & repack individual values.
        all_vals: list[float] = []
        for _, m in metrics:
            all_vals.extend(float(v) for v in (m["baseline"].get("values") or []))
            all_vals.extend(float(v) for v in (m["repack"].get("values") or []))
        y_max = _nice_upper(max(all_vals) if all_vals else 100.0, min_upper=50.0)

        plot_x = px + 54
        plot_y = py + 42
        plot_w = pw - 70
        plot_h = ph - 84

        # Horizontal grid lines (4 intervals)
        for tick_i in range(5):
            val = y_max * tick_i / 4.0
            yy = plot_y + plot_h - (plot_h * tick_i / 4.0)
            lines.append(
                f'<line x1="{plot_x}" y1="{yy:.1f}" x2="{plot_x + plot_w}" y2="{yy:.1f}" '
                f'stroke="{GRID_COLOR}" stroke-width="1" />'
            )
            lines.append(
                f'<text x="{plot_x - 8}" y="{yy + 4:.1f}" font-size="10" text-anchor="end" '
                f'fill="{MUTED_COLOR}">{val:.0f}</text>'
            )

        # Baseline axis line
        lines.append(
            f'<line x1="{plot_x}" y1="{plot_y + plot_h}" x2="{plot_x + plot_w}" y2="{plot_y + plot_h}" '
            f'stroke="{MUTED_COLOR}" stroke-width="1" />'
        )

        group_w = plot_w / max(1, len(metrics))
        bar_w = min(22.0, group_w * 0.28)
        gap = 5.0

        for idx, (label, m) in enumerate(metrics):
            cx = plot_x + group_w * (idx + 0.5)
            lines.append(
                f'<text x="{cx:.1f}" y="{plot_y + plot_h + 18}" font-size="10.5" text-anchor="middle" '
                f'fill="{TEXT_COLOR}">{escape(label)}</text>'
            )

            for series_idx, (series_key, color) in enumerate(
                (("baseline", BASELINE_COLOR), ("repack", REPACK_COLOR))
            ):
                stats = m[series_key]
                med = stats.get("median")
                vals = stats.get("values") or []
                bx = cx - bar_w - gap / 2.0 if series_idx == 0 else cx + gap / 2.0
                bcx = bx + bar_w / 2.0

                if med is not None:
                    bh = (float(med) / y_max) * plot_h
                    by = plot_y + plot_h - bh
                    lines.append(
                        f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" '
                        f'fill="{color}" fill-opacity="0.78" rx="2" />'
                    )
                    label_y = max(plot_y + 10, by - 5)
                    lines.append(
                        f'<text x="{bcx:.1f}" y="{label_y:.1f}" font-size="9" text-anchor="middle" '
                        f'fill="{TEXT_COLOR}">{float(med):.1f}</text>'
                    )

                # Range whisker + individual pair points
                if vals:
                    ymin_v = plot_y + plot_h - (min(vals) / y_max) * plot_h
                    ymax_v = plot_y + plot_h - (max(vals) / y_max) * plot_h
                    lines.append(
                        f'<line x1="{bcx:.1f}" y1="{ymax_v:.1f}" x2="{bcx:.1f}" y2="{ymin_v:.1f}" '
                        f'stroke="{TEXT_COLOR}" stroke-width="1.1" stroke-opacity="0.65" />'
                    )
                    for v_i, v in enumerate(vals):
                        vy = plot_y + plot_h - (float(v) / y_max) * plot_h
                        jitter = (v_i - (len(vals) - 1) / 2.0) * 3.0
                        lines.append(
                            f'<circle cx="{bcx + jitter:.1f}" cy="{vy:.1f}" r="2.7" '
                            f'fill="#ffffff" stroke="{color}" stroke-width="1.6" />'
                        )

        lines.append(
            f'<text x="{px + 14}" y="{py + ph - 8}" font-size="10" fill="{MUTED_COLOR}">'
            "Y-axis: milliseconds (ms); bar = median, line = [min, max], dots = individual pairs.</text>"
        )

    if control:
        cy = 456
        lines.append(
            f'<rect x="24" y="{cy}" width="{width - 48}" height="86" rx="6" '
            f'fill="#fef2f2" stroke="#fca5a5" />'
        )
        lines.append(
            f'<text x="38" y="{cy + 22}" font-size="13" font-weight="700" fill="{PLAIN_COLOR}">'
            "Plain REPACK Control (REPACK public.orders, same 250k fixture &amp; Pair 1 seed)</text>"
        )
        c_int = control["matched_interval"]
        c_exec = control["repack_execution"]
        c_s2c = c_int["latency_sched_to_complete_ms"]
        c_ex = c_int["latency_exec_ms"]
        lines.append(
            f'<text x="38" y="{cy + 44}" font-size="11.5" fill="{TEXT_COLOR}">'
            f"Duration: {c_exec['duration_ms']:.1f} ms | "
            f"AccessExclusiveLock observed: {c_exec['exclusive_lock_observed']} | "
            f"Blocked waiter samples: {c_exec['waiting_locks_observed_count']} | "
            f"Outcome: {escape(str(control['outcome']))}</text>"
        )
        lines.append(
            f'<text x="38" y="{cy + 65}" font-size="11.5" fill="{TEXT_COLOR}">'
            f"Matched interval S2C: p50={c_s2c['p50_ms']} ms, p99={c_s2c['p99_ms']} ms, "
            f"max={c_s2c['max_ms']} ms | "
            f"Exec max={c_ex['max_ms']} ms | "
            f"Backpressure-delayed ops (full window): {control['backpressure_delayed_count']}</text>"
        )

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def render_repack_window_timeseries_svg(
    pair_series: list[dict[str, Any]],
    *,
    control_series: dict[str, Any] | None = None,
    t_min_s: float = 20.0,
    t_max_s: float = 45.0,
) -> str:
    """Render per-op sched-to-complete latency around the REPACK window for each pair (+ control)."""
    width = 960
    subplot_h = 165
    gap_y = 16
    top_margin = 64
    bottom_margin = 34

    n_panels = len(pair_series) + (1 if control_series else 0)
    height = top_margin + n_panels * subplot_h + max(0, n_panels - 1) * gap_y + bottom_margin
    lines = _svg_open(width, height, "Per-Operation Latency Around the REPACK Window")

    lines.append(
        f'<text x="24" y="30" font-size="18" font-weight="700" fill="{TEXT_COLOR}">'
        "Per-Operation Scheduled-to-Completed Latency Around REPACK (t = 20 s to 45 s)</text>"
    )
    lines.append(
        f'<text x="24" y="48" font-size="12" fill="{MUTED_COLOR}">'
        "Shaded band marks the REPACK execution interval. Concurrent pairs share a common Y scale; "
        "plain REPACK control uses its own Y scale.</text>"
    )

    # Legend
    lx = 625
    lines.append(f'<line x1="{lx}" y1="24" x2="{lx + 16}" y2="24" stroke="{BASELINE_COLOR}" stroke-width="2" />')
    lines.append(f'<text x="{lx + 21}" y="28" font-size="11.5" fill="{TEXT_COLOR}">Baseline</text>')
    lines.append(f'<line x1="{lx + 84}" y1="24" x2="{lx + 100}" y2="24" stroke="{REPACK_COLOR}" stroke-width="2" />')
    lines.append(f'<text x="{lx + 105}" y="28" font-size="11.5" fill="{TEXT_COLOR}">Concurrent REPACK</text>')
    if control_series:
        lines.append(f'<line x1="{lx + 228}" y1="24" x2="{lx + 244}" y2="24" stroke="{PLAIN_COLOR}" stroke-width="2" />')
        lines.append(f'<text x="{lx + 249}" y="28" font-size="11.5" fill="{TEXT_COLOR}">Plain REPACK</text>')

    # Common y_max across concurrent pairs
    conc_max = 0.0
    for ps in pair_series:
        for row in ps["baseline_ops"]:
            t_s = float(row["scheduled_offset_ms"]) / 1000.0
            if t_min_s <= t_s <= t_max_s and row.get("outcome") == "ok":
                conc_max = max(conc_max, float(row["sched_to_complete_ms"]))
        for row in ps["repack_ops"]:
            t_s = float(row["scheduled_offset_ms"]) / 1000.0
            if t_min_s <= t_s <= t_max_s and row.get("outcome") == "ok":
                conc_max = max(conc_max, float(row["sched_to_complete_ms"]))
    conc_y_max = _nice_upper(conc_max, min_upper=100.0)

    def _draw_panel(
        panel_idx: int,
        title: str,
        subtitle: str,
        repack_start_s: float,
        repack_end_s: float,
        base_ops: list[dict[str, Any]],
        rep_ops: list[dict[str, Any]],
        y_max: float,
        rep_color: str,
        shade_color: str,
    ) -> None:
        px = 24
        py = top_margin + panel_idx * (subplot_h + gap_y)
        pw = width - 48
        ph = subplot_h

        lines.append(
            f'<rect x="{px}" y="{py}" width="{pw}" height="{ph}" rx="6" '
            f'fill="{PANEL_BG}" stroke="{BORDER_COLOR}" />'
        )
        lines.append(
            f'<text x="{px + 12}" y="{py + 19}" font-size="12.5" font-weight="600" fill="{TEXT_COLOR}">'
            f"{escape(title)}</text>"
        )
        lines.append(
            f'<text x="{px + pw - 12}" y="{py + 19}" font-size="10.5" text-anchor="end" fill="{MUTED_COLOR}">'
            f"{escape(subtitle)}</text>"
        )

        plot_x = px + 56
        plot_y = py + 30
        plot_w = pw - 72
        plot_h = ph - 54

        def _tx(t_s: float) -> float:
            return plot_x + ((t_s - t_min_s) / (t_max_s - t_min_s)) * plot_w

        def _ty(ms: float) -> float:
            clamped = max(0.0, min(ms, y_max))
            return plot_y + plot_h - (clamped / y_max) * plot_h

        # Shaded REPACK interval
        sx1 = max(plot_x, min(plot_x + plot_w, _tx(repack_start_s)))
        sx2 = max(plot_x, min(plot_x + plot_w, _tx(repack_end_s)))
        if sx2 > sx1:
            lines.append(
                f'<rect x="{sx1:.1f}" y="{plot_y}" width="{sx2 - sx1:.1f}" height="{plot_h}" '
                f'fill="{shade_color}" />'
            )

        # Y grid lines (3 intervals)
        for ti in range(4):
            val = y_max * ti / 3.0
            yy = plot_y + plot_h - (plot_h * ti / 3.0)
            lines.append(
                f'<line x1="{plot_x}" y1="{yy:.1f}" x2="{plot_x + plot_w}" y2="{yy:.1f}" '
                f'stroke="{GRID_COLOR}" stroke-width="1" />'
            )
            lines.append(
                f'<text x="{plot_x - 8}" y="{yy + 3.5:.1f}" font-size="9.5" text-anchor="end" '
                f'fill="{MUTED_COLOR}">{val:.0f}</text>'
            )

        # X ticks every 5 seconds
        t_tick = t_min_s
        while t_tick <= t_max_s + 1e-6:
            xx = _tx(t_tick)
            lines.append(
                f'<line x1="{xx:.1f}" y1="{plot_y}" x2="{xx:.1f}" y2="{plot_y + plot_h}" '
                f'stroke="{GRID_COLOR}" stroke-width="0.8" />'
            )
            lines.append(
                f'<text x="{xx:.1f}" y="{plot_y + plot_h + 14}" font-size="9.5" text-anchor="middle" '
                f'fill="{MUTED_COLOR}">{t_tick:.0f}s</text>'
            )
            t_tick += 5.0

        def _polyline(ops: list[dict[str, Any]], color: str, opacity: float) -> None:
            pts: list[str] = []
            for row in ops:
                if row.get("outcome") != "ok":
                    continue
                t_s = float(row["scheduled_offset_ms"]) / 1000.0
                if t_min_s <= t_s <= t_max_s:
                    pts.append(f"{_tx(t_s):.1f},{_ty(float(row['sched_to_complete_ms'])):.1f}")
            if pts:
                lines.append(
                    f'<polyline fill="none" stroke="{color}" stroke-width="1.4" '
                    f'stroke-opacity="{opacity}" points="{" ".join(pts)}" />'
                )

        if base_ops:
            _polyline(base_ops, BASELINE_COLOR, 0.72)
        _polyline(rep_ops, rep_color, 0.92)

        # Border box around plot area
        lines.append(
            f'<rect x="{plot_x}" y="{plot_y}" width="{plot_w}" height="{plot_h}" '
            f'fill="none" stroke="{BORDER_COLOR}" />'
        )

    for idx, ps in enumerate(pair_series):
        _draw_panel(
            idx,
            f"Pair {ps['pair_index']} ({ps['order']}, seed={ps['rng_seed']})",
            (
                f"REPACK {ps['repack_start_s']:.2f}s-{ps['repack_end_s']:.2f}s "
                f"({ps['repack_duration_ms']:.0f} ms) | "
                f"{ps['committed_during_repack']} ops committed | "
                f"waiters={ps['waiting_locks_observed_count']}"
            ),
            ps["repack_start_s"],
            ps["repack_end_s"],
            ps["baseline_ops"],
            ps["repack_ops"],
            conc_y_max,
            REPACK_COLOR,
            SHADE_COLOR,
        )

    if control_series:
        c_max = 0.0
        for row in control_series["repack_ops"]:
            t_s = float(row["scheduled_offset_ms"]) / 1000.0
            if t_min_s <= t_s <= t_max_s and row.get("outcome") == "ok":
                c_max = max(c_max, float(row["sched_to_complete_ms"]))
        c_y_max = _nice_upper(c_max, min_upper=500.0)
        _draw_panel(
            len(pair_series),
            f"Control: Plain REPACK public.orders (seed={control_series['rng_seed']}, vs Pair 1 baseline)",
            (
                f"Plain REPACK {control_series['repack_start_s']:.2f}s-{control_series['repack_end_s']:.2f}s "
                f"({control_series['repack_duration_ms']:.0f} ms) | "
                f"max S2C={c_max:.0f} ms | "
                f"blocked waiter samples={control_series['waiting_locks_observed_count']}"
            ),
            control_series["repack_start_s"],
            control_series["repack_end_s"],
            control_series.get("baseline_ops") or [],
            control_series["repack_ops"],
            c_y_max,
            PLAIN_COLOR,
            PLAIN_SHADE_COLOR,
        )

    lines.append(
        f'<text x="{width / 2:.0f}" y="{height - 10}" font-size="11" text-anchor="middle" '
        f'fill="{MUTED_COLOR}">Measurement window offset (seconds); Y-axis = scheduled-to-completed latency (ms)</text>'
    )
    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def render_reader_lag_svg(agg: dict[str, Any]) -> str:
    """Render steady-state reader probe lag and final-marker convergence timing across pairs."""
    width = 960
    height = 380
    lines = _svg_open(width, height, "Reader-Visible Lag and Final Marker Timing")

    lines.append(
        f'<text x="24" y="30" font-size="18" font-weight="700" fill="{TEXT_COLOR}">'
        "DuckDB Reader-Visible Lag Across Repeated Pairs (This Host Only)</text>"
    )
    lines.append(
        f'<text x="24" y="49" font-size="12" fill="{MUTED_COLOR}">'
        "Left: steady-state probe lag (circle = p50, whisker = [min, max]). "
        "Right: final marker commit-to-visibility (solid) and commit-to-verified-match (outlined).</text>"
    )

    lx = 620
    lines.append(f'<rect x="{lx}" y="18" width="12" height="12" rx="2" fill="{BASELINE_COLOR}" />')
    lines.append(f'<text x="{lx + 17}" y="28" font-size="11.5" fill="{TEXT_COLOR}">Baseline</text>')
    lines.append(f'<rect x="{lx + 80}" y="18" width="12" height="12" rx="2" fill="{REPACK_COLOR}" />')
    lines.append(f'<text x="{lx + 97}" y="28" font-size="11.5" fill="{TEXT_COLOR}">Concurrent REPACK</text>')
    if agg.get("plain_repack_control"):
        lines.append(f'<rect x="{lx + 216}" y="18" width="12" height="12" rx="2" fill="{PLAIN_COLOR}" />')
        lines.append(f'<text x="{lx + 233}" y="28" font-size="11.5" fill="{TEXT_COLOR}">Plain REPACK</text>')

    pairs = agg["pairs"]
    control = agg.get("plain_repack_control")

    # Build categories: Pair 1, Pair 2, Pair 3 (+ Control if present)
    cats: list[dict[str, Any]] = []
    for p in pairs:
        cats.append(
            {
                "label": f"Pair {p['pair_index']}",
                "series": [
                    ("baseline", BASELINE_COLOR, p["baseline"]),
                    ("repack", REPACK_COLOR, p["repack"]),
                ],
            }
        )
    if control:
        cats.append(
            {
                "label": "Plain ctrl",
                "series": [("plain_repack", PLAIN_COLOR, control["run_metrics"])],
            }
        )

    # Panel 1: Steady-state probe lag (seconds)
    p1x, p1y, p1w, p1h = 24, 68, 444, 290
    lines.append(
        f'<rect x="{p1x}" y="{p1y}" width="{p1w}" height="{p1h}" rx="6" '
        f'fill="{PANEL_BG}" stroke="{BORDER_COLOR}" />'
    )
    lines.append(
        f'<text x="{p1x + 14}" y="{p1y + 22}" font-size="13" font-weight="600" fill="{TEXT_COLOR}">'
        "Steady-State Probe Visibility Lag (s)</text>"
    )

    ss_max = 0.0
    for c in cats:
        for _, _, m in c["series"]:
            ss_max = max(ss_max, float(m.get("probe_max_ms") or 0.0) / 1000.0)
    y1_max = _nice_upper(ss_max, min_upper=15.0)

    px1, py1, pw1, ph1 = p1x + 48, p1y + 38, p1w - 64, p1h - 76
    for ti in range(5):
        val = y1_max * ti / 4.0
        yy = py1 + ph1 - (ph1 * ti / 4.0)
        lines.append(f'<line x1="{px1}" y1="{yy:.1f}" x2="{px1 + pw1}" y2="{yy:.1f}" stroke="{GRID_COLOR}" />')
        lines.append(
            f'<text x="{px1 - 7}" y="{yy + 3.5:.1f}" font-size="10" text-anchor="end" fill="{MUTED_COLOR}">'
            f"{val:.0f}s</text>"
        )
    lines.append(f'<line x1="{px1}" y1="{py1 + ph1}" x2="{px1 + pw1}" y2="{py1 + ph1}" stroke="{MUTED_COLOR}" />')

    gw1 = pw1 / max(1, len(cats))
    for ci, c in enumerate(cats):
        cx = px1 + gw1 * (ci + 0.5)
        lines.append(
            f'<text x="{cx:.1f}" y="{py1 + ph1 + 18}" font-size="10.5" text-anchor="middle" fill="{TEXT_COLOR}">'
            f"{escape(c['label'])}</text>"
        )
        n_ser = len(c["series"])
        for si, (_, color, m) in enumerate(c["series"]):
            dx = (si - (n_ser - 1) / 2.0) * 26.0
            sx = cx + dx
            vmin = float(m.get("probe_min_ms") or 0.0) / 1000.0
            vp50 = float(m.get("probe_p50_ms") or 0.0) / 1000.0
            vmax = float(m.get("probe_max_ms") or 0.0) / 1000.0
            y_lo = py1 + ph1 - (vmin / y1_max) * ph1
            y_mid = py1 + ph1 - (vp50 / y1_max) * ph1
            y_hi = py1 + ph1 - (vmax / y1_max) * ph1
            lines.append(f'<line x1="{sx:.1f}" y1="{y_hi:.1f}" x2="{sx:.1f}" y2="{y_lo:.1f}" stroke="{color}" stroke-width="2.2" />')
            lines.append(f'<line x1="{sx - 5:.1f}" y1="{y_hi:.1f}" x2="{sx + 5:.1f}" y2="{y_hi:.1f}" stroke="{color}" stroke-width="1.6" />')
            lines.append(f'<line x1="{sx - 5:.1f}" y1="{y_lo:.1f}" x2="{sx + 5:.1f}" y2="{y_lo:.1f}" stroke="{color}" stroke-width="1.6" />')
            lines.append(f'<circle cx="{sx:.1f}" cy="{y_mid:.1f}" r="4.5" fill="{color}" stroke="#ffffff" stroke-width="1.4" />')
            lines.append(
                f'<text x="{sx:.1f}" y="{max(py1 + 10, y_hi - 5):.1f}" font-size="9" text-anchor="middle" fill="{TEXT_COLOR}">'
                f"{vp50:.1f}s</text>"
            )

    # Panel 2: Final marker visibility & verified match (seconds)
    p2x, p2y, p2w, p2h = 492, 68, 444, 290
    lines.append(
        f'<rect x="{p2x}" y="{p2y}" width="{p2w}" height="{p2h}" rx="6" '
        f'fill="{PANEL_BG}" stroke="{BORDER_COLOR}" />'
    )
    lines.append(
        f'<text x="{p2x + 14}" y="{p2y + 22}" font-size="13" font-weight="600" fill="{TEXT_COLOR}">'
        "Final Marker Visibility &amp; Verified-Match Time (s)</text>"
    )

    fm_max = 0.0
    for c in cats:
        for _, _, m in c["series"]:
            fm_max = max(
                fm_max,
                float(m.get("final_marker_to_verified_match_ms") or 0.0) / 1000.0,
                float(m.get("final_marker_visibility_ms") or 0.0) / 1000.0,
            )
    y2_max = _nice_upper(fm_max, min_upper=25.0)

    px2, py2, pw2, ph2 = p2x + 48, p2y + 38, p2w - 64, p2h - 76
    for ti in range(5):
        val = y2_max * ti / 4.0
        yy = py2 + ph2 - (ph2 * ti / 4.0)
        lines.append(f'<line x1="{px2}" y1="{yy:.1f}" x2="{px2 + pw2}" y2="{yy:.1f}" stroke="{GRID_COLOR}" />')
        lines.append(
            f'<text x="{px2 - 7}" y="{yy + 3.5:.1f}" font-size="10" text-anchor="end" fill="{MUTED_COLOR}">'
            f"{val:.0f}s</text>"
        )
    lines.append(f'<line x1="{px2}" y1="{py2 + ph2}" x2="{px2 + pw2}" y2="{py2 + ph2}" stroke="{MUTED_COLOR}" />')

    gw2 = pw2 / max(1, len(cats))
    bar_w2 = 22.0
    for ci, c in enumerate(cats):
        cx = px2 + gw2 * (ci + 0.5)
        lines.append(
            f'<text x="{cx:.1f}" y="{py2 + ph2 + 18}" font-size="10.5" text-anchor="middle" fill="{TEXT_COLOR}">'
            f"{escape(c['label'])}</text>"
        )
        n_ser = len(c["series"])
        for si, (_, color, m) in enumerate(c["series"]):
            dx = (si - (n_ser - 1) / 2.0) * (bar_w2 + 8.0)
            bx = cx + dx - bar_w2 / 2.0
            vis_s = float(m.get("final_marker_visibility_ms") or 0.0) / 1000.0
            ver_s = float(m.get("final_marker_to_verified_match_ms") or 0.0) / 1000.0
            h_ver = (ver_s / y2_max) * ph2
            y_ver = py2 + ph2 - h_ver
            h_vis = (vis_s / y2_max) * ph2
            y_vis = py2 + ph2 - h_vis
            # Total commit-to-verified-match (light fill + border)
            lines.append(
                f'<rect x="{bx:.1f}" y="{y_ver:.1f}" width="{bar_w2:.1f}" height="{h_ver:.1f}" '
                f'fill="{color}" fill-opacity="0.22" stroke="{color}" stroke-width="1.3" rx="2" />'
            )
            # Commit-to-visibility (solid)
            lines.append(
                f'<rect x="{bx:.1f}" y="{y_vis:.1f}" width="{bar_w2:.1f}" height="{h_vis:.1f}" '
                f'fill="{color}" fill-opacity="0.82" rx="2" />'
            )
            lines.append(
                f'<text x="{bx + bar_w2 / 2.0:.1f}" y="{max(py2 + 10, y_ver - 4):.1f}" '
                f'font-size="8.8" text-anchor="middle" fill="{TEXT_COLOR}">{ver_s:.1f}s</text>'
            )

    lines.append("</svg>")
    return "\n".join(lines) + "\n"
