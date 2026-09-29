"""charts.py — tiny server-side SVG charts for the dashboard (no JS, no CDN)."""

from html import escape

PALETTE = {"model": "#20e486", "pool": "#5cc8ff", "random": "#7f8ca3", "plain": "#f6b93b"}


def _scale(v, lo, hi, a, b):
    return a if hi == lo else a + (v - lo) * (b - a) / (hi - lo)


def line_chart(series, width=880, height=260, y_label="cumulative % (sum of trade returns)"):
    """series: list of (name, color, [(x_label, y), ...]) sharing the same x axis."""
    series = [s for s in series if s[2]]
    if not series:
        return ""
    pad_l, pad_r, pad_t, pad_b = 56, 16, 14, 30
    n = max(len(s[2]) for s in series)
    ys = [y for s in series for _, y in s[2]]
    lo, hi = min(ys + [0.0]), max(ys + [0.0])
    span = (hi - lo) or 1.0
    lo, hi = lo - span * 0.05, hi + span * 0.05

    def X(i):
        return _scale(i, 0, max(n - 1, 1), pad_l, width - pad_r)

    def Y(v):
        return _scale(v, lo, hi, height - pad_b, pad_t)

    parts = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" '
             f'aria-label="{escape(y_label)}">']
    for k in range(5):
        v = lo + (hi - lo) * k / 4
        y = Y(v)
        parts.append(f'<line x1="{pad_l}" x2="{width - pad_r}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{pad_l - 8}" y="{y + 4:.1f}" class="axis" text-anchor="end">{v:+.0f}</text>')
    parts.append(f'<line x1="{pad_l}" x2="{width - pad_r}" y1="{Y(0):.1f}" y2="{Y(0):.1f}" class="zero"/>')

    labels = series[0][2]
    step = max(1, -(-len(labels) // 8))
    for i in range(0, len(labels), step):
        parts.append(f'<text x="{X(i):.1f}" y="{height - 8}" class="axis" text-anchor="middle">'
                     f'{escape(str(labels[i][0]))}</text>')

    for name, color, pts in series:
        path = " ".join(f"{'M' if i == 0 else 'L'}{X(i):.1f},{Y(y):.1f}" for i, (_, y) in enumerate(pts))
        parts.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2"/>')
    parts.append("</svg>")

    legend = "".join(f'<span class="legend-item"><i style="background:{c}"></i>{escape(nm)}</span>'
                     for nm, c, _ in series)
    return f'<div class="chart-wrap">{"".join(parts)}<div class="legend">{legend}</div></div>'


def bar_chart(bars, width=880, height=220, fmt="{:+.1f}%"):
    """bars: [(label, value)] — green up, red down."""
    if not bars:
        return ""
    pad_l, pad_r, pad_t, pad_b = 56, 16, 14, 30
    vals = [v for _, v in bars]
    lo, hi = min(vals + [0.0]), max(vals + [0.0])
    span = (hi - lo) or 1.0
    lo, hi = lo - span * 0.08, hi + span * 0.08
    bw = (width - pad_l - pad_r) / len(bars)

    def Y(v):
        return _scale(v, lo, hi, height - pad_b, pad_t)

    parts = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" aria-label="monthly P&L">']
    parts.append(f'<line x1="{pad_l}" x2="{width - pad_r}" y1="{Y(0):.1f}" y2="{Y(0):.1f}" class="zero"/>')
    step = max(1, -(-len(bars) // 8))            # at most ~8 labels
    for i, (label, v) in enumerate(bars):
        x = pad_l + i * bw + bw * 0.15
        y0, y1 = Y(0), Y(v)
        cls = "bar-up" if v >= 0 else "bar-down"
        parts.append(f'<rect x="{x:.1f}" y="{min(y0, y1):.1f}" width="{bw * 0.7:.1f}" '
                     f'height="{abs(y1 - y0):.1f}" class="{cls}"><title>{escape(str(label))}: '
                     f'{fmt.format(v)}</title></rect>')
        if i % step == 0:
            parts.append(f'<text x="{x + bw * 0.35:.1f}" y="{height - 8}" class="axis" '
                         f'text-anchor="middle">{escape(str(label))}</text>')
    for k in (lo, 0.0, hi):
        parts.append(f'<text x="{pad_l - 8}" y="{Y(k) + 4:.1f}" class="axis" text-anchor="end">'
                     f'{k:+.0f}</text>')
    parts.append("</svg>")
    return f'<div class="chart-wrap">{"".join(parts)}</div>'
