"""Draw the weight-lookup figures of the med unit conversion user guide.

    .venv/bin/python dev/docs_figures/med_unit_conversion.py

writes `weight-strategies.png` (Figure 1) and `staleness-window.png`
(Figure 2) to `docs/images/med-unit-conversion/`.

MADE-UP NUMBERS ONLY; no data file is read. Figure 1 draws the five stays of
the guide's "Fallback strategies" example (the `scenario_data` fixture of
tests/utils/med_unit_converter/test_weight_fallbacks.py); Figure 2 draws the
guide's staleness example. Every served weight, its strategy and its age come
from `attach_weight` on frames built from the literals below, so a figure
cannot disagree with the code; `served_weights()` returns those results and
tests/utils/med_unit_converter/test_docs_figures.py pins them to the values
the guide states. Stay END dates are invented for drawing (the converter
reads only `admission_dttm`).

Background: the mkdocs-material slate page colour (#1e2029), so the PNG blends
into the guide and stays readable on its own. Saved at 2x (200 dpi).
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import logging

import matplotlib

matplotlib.use("Agg")
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)  # Roboto is optional
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.offsetbox import AnnotationBbox, HPacker, TextArea, VPacker  # noqa: E402
from matplotlib.patches import ConnectionPatch, Patch, Rectangle  # noqa: E402
from matplotlib.text import Text  # noqa: E402
from matplotlib.ticker import FixedLocator, MultipleLocator  # noqa: E402
import polars as pl  # noqa: E402

from clifpy.utils.unit_converter import WEIGHT_FALLBACKS, attach_weight  # noqa: E402

OUT_DIR = Path(__file__).resolve().parents[2] / "docs" / "images" / "med-unit-conversion"
DPI = 200            # 2x for retina
TRANSPARENT = False  # True: transparent background instead of the slate fill

# ---------------------------------------------------------------- fonts (pt)
FONT_SANS = ["Roboto", "Arial", "Helvetica", "DejaVu Sans"]
FONT_MONO = ["Roboto Mono", "Menlo", "DejaVu Sans Mono"]
FONT_BASE = 9
FONT_TICK = 8.5
FONT_LANE = 9       # lane IDs (mono)
FONT_SUB = 8.5      # lane sublabels, stay IDs, notes
FONT_KG = 8.5       # labels above weight circles
FONT_PICK = 8.5     # connector / served-by labels
FONT_LEGEND = 8.5
FONT_PANEL = 10.5   # bold panel letters
MONO_SCALE = 0.92   # Menlo reads larger than Arial at the same size

# ---------------------------------------------------------------- geometry
FIG_W = 8.0          # in; shown at ~720-800 px in the guide
LANE_PITCH = 0.66    # in per lane (Fig 1A, Fig 2)
LANE_PITCH_B = 0.52  # in per lane (Fig 1B: labels live in a column, not under the lane)
BAR_H = 0.11         # stay bar height, lane units
MARKER_W = 7.5       # weight circle diameter, pt
MARKER_D = 7.5       # dose diamond size, pt
LABEL_DROP = 9       # pt from the lane centre down to a connector label
LINE_W = 1.6         # connector width, pt (about 2 px at guide width)

# Tick spacing per axis
YTICK_INTERVAL = {}  # no quantitative y axis: lanes only
XTICK_INTERVAL = {"fig1a_hours": 1.0, "fig1b_hours": 2.0, "fig1b_days": 2.0}

# ---------------------------------------------------------------- palette
# dataviz reference palette, DARK steps, validated against BG:
# adjacent pairs pass (worst CVD dE 8.4, normal 19.8, all >= 3:1). Under all
# pairs, slot 2 (orange) vs slot 4 (yellow) fails (CVD 4.8): they never share
# a lane and every connector is named in text, so colour is never alone.
BG = "#1e2029"       # mkdocs-material slate --md-default-bg-color, hsl(232 15% 14%)
INK = "#f0efec"      # primary ink, 14.1:1 on BG
INK2 = "#c3c2b7"     # secondary ink, 9.1:1
MUTED = "#898781"    # muted ink / rejected picks, 4.5:1
GRID = "#2c2f3b"     # hairline grid, one step off BG
AXIS = "#454857"     # baseline / axis
STAY = "#3b3f50"     # stay bars (context, not a series)
WINDOW = (0.94, 0.94, 0.92, 0.10)   # cap-window wash (INK at 10%)
WINDOW_ON_BG = "#35373f"             # the wash composited on BG (label masks inside it)
STRATEGY = {         # fixed categorical order = the chain order
    "asof": "#3987e5",                 # slot 1 blue
    "same_hosp_earliest": "#d95926",   # slot 2 orange
    "prior_hosp_latest": "#199e70",    # slot 3 aqua
    "later_hosp_earliest": "#c98500",  # slot 4 yellow
}


def apply_theme() -> None:
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": FONT_SANS,
        "font.monospace": FONT_MONO,
        "font.size": FONT_BASE,
        "text.color": INK,
        "axes.facecolor": "none",
        "figure.facecolor": BG,
        "savefig.facecolor": "none" if TRANSPARENT else BG,
        "axes.edgecolor": AXIS,
        "axes.labelcolor": INK2,
        "axes.labelsize": FONT_TICK,
        "xtick.color": AXIS,
        "xtick.labelcolor": INK2,
        "xtick.labelsize": FONT_TICK,
        "ytick.left": False,
        "ytick.labelleft": False,
        "legend.fontsize": FONT_LEGEND,
        "legend.labelcolor": INK,
    })


# ================================================================ made-up data
DOSE_1 = dt.datetime(2024, 3, 10, 10, 0)   # every Figure 1 dose
H = dt.timedelta(hours=1)
D = dt.timedelta(days=1)


def _t(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s)


# Figure 1, within the stay: lane -> [(recorded, kg)]
WITHIN = {
    "H_ASOF": [(_t("2024-03-10 08:00"), 70.0), (_t("2024-03-10 09:00"), 72.0),
               (_t("2024-03-10 11:00"), 80.0)],
    "H_AFTER": [(_t("2024-03-10 11:00"), 75.0), (_t("2024-03-10 13:00"), 77.0)],
    "H_NONE": [],
}
# Figure 1, across stays: dose stay -> patient, stays [(id, admit, end*, weights)]
# *end dates invented for drawing
ACROSS = {
    "H_PRIOR": ("P3", [
        ("H_P3_OLD", _t("2024-01-01"), _t("2024-01-05"),
         [(_t("2024-01-02 09:00"), 68.0), (_t("2024-01-03 09:00"), 69.0)]),
        ("H_PRIOR", _t("2024-03-09"), _t("2024-03-14"), []),
    ]),
    "H_LATER": ("P4", [
        ("H_LATER", _t("2024-03-09"), _t("2024-03-14"), []),
        ("H_P4_NEW", _t("2024-05-01"), _t("2024-05-08"),
         [(_t("2024-05-02 09:00"), 90.0), (_t("2024-05-05 09:00"), 91.0)]),
    ]),
}
# Figure 2: a made-up stay; "day N, HH:00" = N days + HH hours
DOSE_2 = 10 * D + 10 * H
FIG2_DAY0 = dt.datetime(2024, 2, 29)   # so "day 10, 10:00" is 2024-03-10 10:00, as in the guide
FIG2_KG = (60.0, 62.0, 64.0)
FIG2_ROWS = [  # (row key, cap days, weights)
    ("none", None, [1 * D + 10 * H, 3 * D + 10 * H, 10 * D + 11 * H]),
    ("cap", 7, [1 * D + 10 * H, 3 * D + 10 * H, 10 * D + 11 * H]),
    ("cap_moved", 7, [1 * D + 10 * H, 2 * D + 10 * H, 10 * D + 11 * H]),
]
# served_weights()["staleness"]: the guide's four cases, by the second weight's day
FIG2_SCENARIOS = {"on the boundary": 3 * D + 10 * H, "a day past it": 2 * D + 10 * H}
FIG2_CHAIN = ("asof", "same_hosp_earliest")


def _fig1_frames():
    """The five stays as converter inputs: doses, weights, hospitalizations."""
    lanes = ["H_ASOF", "H_AFTER", "H_PRIOR", "H_LATER", "H_NONE"]
    doses = pl.DataFrame({"hospitalization_id": lanes, "admin_dttm": [DOSE_1] * len(lanes)})
    weights = [(stay, t, kg) for stay, ws in WITHIN.items() for t, kg in ws]
    stays = [("H_ASOF", "P1", _t("2024-03-09")), ("H_AFTER", "P2", _t("2024-03-09")),
             ("H_NONE", "P5", _t("2024-03-09"))]
    for patient, patient_stays in ACROSS.values():
        for sid, admit, _end, ws in patient_stays:
            stays.append((sid, patient, admit))
            weights += [(sid, t, kg) for t, kg in ws]
    vitals = pl.DataFrame(weights, schema=["hospitalization_id", "recorded_dttm", "vital_value"],
                          orient="row").with_columns(vital_category=pl.lit("weight_kg"))
    hosp = pl.DataFrame(stays, schema=["hospitalization_id", "patient_id", "admission_dttm"],
                        orient="row")
    return doses, vitals, hosp


def _fig1_served():
    """`attach_weight` with the full chain: stay -> (strategy, recorded, kg, age h) or None."""
    doses, vitals, hosp = _fig1_frames()
    out = attach_weight(doses, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    return out, {
        r["hospitalization_id"]: (None if r["_weight_source"] is None else
                                  (r["_weight_source"], r["_weight_recorded_dttm"],
                                   r["weight_kg"], r["_weight_age_hours"]))
        for r in out.iter_rows(named=True)
    }


def _fig2_attach(times, cap, fallbacks=FIG2_CHAIN):
    dose_frame = pl.DataFrame({"hospitalization_id": ["S"], "admin_dttm": [FIG2_DAY0 + DOSE_2]})
    vitals = pl.DataFrame({
        "hospitalization_id": ["S"] * len(times),
        "recorded_dttm": [FIG2_DAY0 + t for t in times],
        "vital_value": list(FIG2_KG),
        "vital_category": ["weight_kg"] * len(times),
    })
    return attach_weight(dose_frame, vitals, fallbacks=fallbacks, max_staleness_days=cap)


def serve_same_stay(times, cap):
    """Figure 2's pick, from `attach_weight`: (served index | None, strategy | None,
    [(strategy, index)] of a pick the cap rejected)."""
    got = _fig2_attach(times, cap).row(0, named=True)
    i = None if got["_weight_source"] is None else times.index(got["_weight_recorded_dttm"] - FIG2_DAY0)
    rejected = []
    if cap is not None and got["_weight_source"] != "asof":
        uncapped = _fig2_attach(times, None, fallbacks=("asof",)).row(0, named=True)
        if uncapped["_weight_source"] == "asof":
            rejected.append(("asof", times.index(uncapped["_weight_recorded_dttm"] - FIG2_DAY0)))
    return i, got["_weight_source"], rejected


def served_weights():
    """What the figures draw, as `attach_weight` serves it."""
    out, _ = _fig1_served()
    rows = []
    for scenario, second in FIG2_SCENARIOS.items():
        times = [1 * D + 10 * H, second, 10 * D + 11 * H]
        for cap in (None, 7):
            got = _fig2_attach(times, cap).row(0, named=True)
            rows.append((scenario, cap, got["_weight_source"], got["weight_kg"], got["_weight_age_hours"]))
    return {
        "strategies": out.select("hospitalization_id", "_weight_source", "weight_kg", "_weight_age_hours"),
        "staleness": pl.DataFrame(
            rows, orient="row",
            schema={"scenario": pl.Utf8, "cap": pl.Int64, "_weight_source": pl.Utf8,
                    "weight_kg": pl.Float64, "_weight_age_hours": pl.Float64},
        ),
    }


def age_label(delta: dt.timedelta) -> str:
    """Signed age as `_weight_age_hours`: + when charted before the dose."""
    hours = delta / H
    sign = "+" if hours >= 0 else "−"
    if abs(hours) < 48:
        return f"{sign}{abs(hours):.0f} h"
    return f"{sign}{abs(hours) / 24:.0f} d"


def hours_of(t: dt.datetime) -> float:
    return (t - DOSE_1.replace(hour=0)) / H


# ================================================================ drawing helpers
REGISTRY: list = []   # labels and markers the overlap check reads


def text_box(parts, size=FONT_PICK, color=INK, sep=0):
    """One line of mixed sans/mono text: parts = [(text, 'sans'|'mono'[, colour[, size]])]."""
    areas = []
    for part in parts:
        text, fam = part[0], part[1]
        col = part[2] if len(part) > 2 else color
        sz = part[3] if len(part) > 3 else size
        areas.append(TextArea(text, textprops=dict(
            fontsize=sz * (MONO_SCALE if fam == "mono" else 1), color=col,
            fontfamily=FONT_MONO if fam == "mono" else FONT_SANS)))
    return HPacker(children=areas, align="baseline", pad=0, sep=sep)


def stack(lines, align="left", sep=1.5):
    return VPacker(children=lines, align=align, pad=0, sep=sep)


def put(ax, box, xy, offset=(0, 0), align=(0.5, 0.5), xycoords="data", mask=True, z=6,
        mask_fc=BG):
    """Place a text box; `mask` hides grid / dose lines under it with the local background."""
    ab = AnnotationBbox(box, xy, xycoords=xycoords, xybox=offset, boxcoords="offset points",
                        box_alignment=align, frameon=mask, pad=0.15,
                        bboxprops=dict(fc=mask_fc, ec="none"), annotation_clip=False, zorder=z)
    ax.add_artist(ab)
    REGISTRY.append(("label", ab))
    return ab


def label(ax, x, y, text, offset=(0, 0), ha="center", va="center", size=FONT_KG,
          color=INK2, mono=False, transform=None, **kw):
    t = ax.text(x, y, text, ha=ha, va=va, fontsize=size * (MONO_SCALE if mono else 1), color=color,
                fontfamily=FONT_MONO if mono else FONT_SANS,
                transform=(transform or ax.transData), zorder=6, **kw)
    if offset != (0, 0):
        t.set_transform(t.get_transform() + matplotlib.transforms.ScaledTranslation(
            offset[0] / 72, offset[1] / 72, ax.figure.dpi_scale_trans))
    REGISTRY.append(("label", t))
    return t


def stay_bar(ax, x0, x1, y):
    ax.add_patch(Rectangle((x0, y - BAR_H / 2), x1 - x0, BAR_H, fc=STAY, ec="none", zorder=1))


def weight(ax, x, y, colour=None, dim=False, crossed=False):
    """Hollow circle = charted weight; filled in the strategy colour = served."""
    if colour:
        ax.plot(x, y, "o", ms=MARKER_W + 1, mfc=colour, mec=BG, mew=1.5, zorder=4)
    else:
        ax.plot(x, y, "o", ms=MARKER_W, mfc=BG, mec=MUTED if dim else INK2, mew=1.3, zorder=4)
    if crossed:
        ax.plot(x, y, "x", ms=MARKER_W * 0.55, mec=MUTED, mew=1.2, zorder=5)
    REGISTRY.append(("marker", (ax, x, y, MARKER_W + 1)))


def dose(ax, x, y):
    ax.plot(x, y, "D", ms=MARKER_D, mfc=INK, mec=BG, mew=1.5, zorder=5)
    REGISTRY.append(("marker", (ax, x, y, MARKER_D + 1)))


def connect(fig, ax_a, xy_a, ax_b, xy_b, colour, rejected=False):
    """Dose -> weight arrow along the lane; may cross an axis cut (two axes)."""
    con = ConnectionPatch(
        xy_a, xy_b, coordsA="data", coordsB="data", axesA=ax_a, axesB=ax_b,
        arrowstyle="-|>", mutation_scale=8 if rejected else 10,
        connectionstyle="arc3,rad=0", shrinkA=5.5, shrinkB=6,
        color=colour, lw=1.2 if rejected else LINE_W,
        linestyle=(0, (3, 2)) if rejected else "-", zorder=3, clip_on=False)
    fig.add_artist(con)
    return con


def lane_label(ax, y, main, sub=None, x=-0.015, main_mono=True):
    tr = matplotlib.transforms.blended_transform_factory(ax.transAxes, ax.transData)
    if sub is None:
        label(ax, x, y, main, ha="right", size=FONT_LANE, color=INK, mono=main_mono, transform=tr)
        return
    lines = [text_box([(main, "mono" if main_mono else "sans")], size=FONT_LANE)]
    lines += [text_box([(s, "sans")], size=FONT_SUB, color=INK2) for s in sub.split("\n")]
    put(ax, stack(lines, align="right", sep=1), (x, y), align=(1, 0.5), xycoords=tr, mask=False)


def style_lanes(ax, n_lanes, grid_x=True):
    ax.set_ylim(-0.55, n_lanes - 0.45)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.tick_params(axis="x", length=3, width=0.8, pad=2)
    if grid_x:
        ax.grid(axis="x", color=GRID, lw=0.8, zorder=0)
        ax.set_axisbelow(True)


def break_marks(ax_left, ax_right):
    """Two short slashes where the time axis is cut."""
    for ax, x in ((ax_left, 1.0), (ax_right, 0.0)):
        ax.plot(x, 0, ls="none", marker=[(-1, -1.8), (1, 1.8)], ms=8, mec=INK2, mew=1.0,
                transform=ax.transAxes, clip_on=False, zorder=7)


def panel_header(fig, x_in, y_in, letter, text):
    box = text_box([(letter + "  ", "sans"), (text, "sans")], size=FONT_BASE)
    box.get_children()[0]._text.set_fontweight("bold")
    box.get_children()[0]._text.set_fontsize(FONT_PANEL)
    ab = AnnotationBbox(box, (x_in / fig.get_figwidth(), y_in / fig.get_figheight()),
                        xycoords="figure fraction", box_alignment=(0, 0.5), frameon=False,
                        annotation_clip=False)
    fig.add_artist(ab)
    REGISTRY.append(("label", ab))


def add_axes_in(fig, left, bottom, width, height):
    fw, fh = fig.get_figwidth(), fig.get_figheight()
    return fig.add_axes((left / fw, bottom / fh, width / fw, height / fh))


def strategy_handles(names):
    return [Line2D([0], [0], color=STRATEGY[n], lw=LINE_W, marker="o", ms=MARKER_W,
                   mfc=STRATEGY[n], mec=BG, label=n) for n in names]


MARK_HANDLES = {
    "weight": Line2D([0], [0], ls="none", marker="o", ms=MARKER_W, mfc=BG, mec=INK2, mew=1.3,
                     label="charted weight (kg)"),
    "weight_noun": Line2D([0], [0], ls="none", marker="o", ms=MARKER_W, mfc=BG, mec=INK2,
                          mew=1.3, label="charted weight"),
    "dose": Line2D([0], [0], ls="none", marker="D", ms=MARKER_D, mfc=INK, mec=BG, label="dose"),
    "stay": Patch(fc=STAY, ec="none", label="stay"),
    "window": Patch(fc=WINDOW, ec="none", label="cap window"),
    "rejected": Line2D([0], [0], color=INK2, lw=1.2, ls=(0, (3, 2)), marker="x", ms=4,
                       mec=MUTED, label="pick rejected by the cap"),
}


def legend(fig, handles, ncol, y_top, mono_from=None):
    """One legend row block at the top; strategy names set in mono."""
    rows = -(-len(handles) // ncol)
    grid = [handles[r * ncol:(r + 1) * ncol] for r in range(rows)]
    handles = [grid[r][c] for c in range(ncol) for r in range(rows) if c < len(grid[r])]
    leg = fig.legend(handles=handles, loc="upper center",
                     bbox_to_anchor=(0.5, y_top / fig.get_figheight()), ncol=ncol,
                     frameon=False, handlelength=1.8, handletextpad=0.5, columnspacing=1.6,
                     borderaxespad=0)
    for t in leg.get_texts():
        if t.get_text() in STRATEGY:
            t.set_fontfamily(FONT_MONO)
    REGISTRY.append(("legend", leg))
    return leg


def footnote(fig, y_in, parts, x_in=0.05):
    ab = AnnotationBbox(text_box(parts, size=FONT_SUB, color=INK2),
                        (x_in / fig.get_figwidth(), y_in / fig.get_figheight()),
                        xycoords="figure fraction", box_alignment=(0, 0.5), frameon=False,
                        annotation_clip=False)
    fig.add_artist(ab)
    REGISTRY.append(("label", ab))


AGE_NOTE = [("Age = dose time − weight time, as in ", "sans"), ("_weight_age_hours", "mono"),
            (": + charted before the dose, − after.", "sans")]


# ================================================================ Figure 1, layout A
def fig1_broken_axis():
    """One panel, five lanes in chain order, time axis cut into three segments."""
    REGISTRY.clear()
    lanes = ["H_ASOF", "H_AFTER", "H_PRIOR", "H_LATER", "H_NONE"]
    n = len(lanes)
    h = n * LANE_PITCH_B
    fig_h = 0.55 + 0.5 + h + 0.45 + 0.42
    fig = plt.figure(figsize=(FIG_W, fig_h))
    bottom = 0.42 + 0.45
    # columns (in): lane labels | Jan | dose day | May | served-by
    x_l, w_l = 0.95, 1.1
    x_c, w_c = x_l + w_l + 0.12, 2.5
    x_r, w_r = x_c + w_c + 0.12, 1.1
    x_col = x_r + w_r + 0.18
    ax_l = add_axes_in(fig, x_l, bottom, w_l, h)
    ax_c = add_axes_in(fig, x_c, bottom, w_c, h)
    ax_r = add_axes_in(fig, x_r, bottom, w_r, h)

    legend(fig, strategy_handles(list(STRATEGY)) + [MARK_HANDLES[k] for k in ("weight", "dose", "stay")],
           ncol=4, y_top=fig_h - 0.02)

    lane_y = {s: n - 1 - k for k, s in enumerate(lanes)}
    for ax in (ax_l, ax_c, ax_r):
        style_lanes(ax, n)
    # Jan segment
    ax_l.set_xlim(mdates.date2num(_t("2023-12-31 12:00")), mdates.date2num(_t("2024-01-06 00:00")))
    ax_l.xaxis.set_major_locator(FixedLocator([mdates.date2num(_t(f"2024-01-0{d}")) for d in (1, 3, 5)]))
    ax_l.xaxis.set_major_formatter(lambda v, k: mdates.num2date(v).strftime("%b %-d" if k == 0 else "%-d"))
    # dose-day segment, hours
    ax_c.set_xlim(7, 14)
    ax_c.xaxis.set_major_locator(FixedLocator([8, 10, 12]))   # 14:00 would touch "May 1"
    ax_c.xaxis.set_minor_locator(MultipleLocator(1))
    ax_c.xaxis.set_major_formatter(lambda v, _: f"{v:02.0f}:00")
    ax_c.grid(axis="x", which="minor", color=GRID, lw=0.8, zorder=0)
    # May segment
    ax_r.set_xlim(mdates.date2num(_t("2024-04-30 12:00")), mdates.date2num(_t("2024-05-06 18:00")))
    ax_r.xaxis.set_major_locator(FixedLocator([mdates.date2num(_t(f"2024-05-0{d}")) for d in (1, 3, 5)]))
    ax_r.xaxis.set_major_formatter(lambda v, k: mdates.num2date(v).strftime("%b %-d" if k == 0 else "%-d"))
    break_marks(ax_l, ax_c)
    break_marks(ax_c, ax_r)

    # segment headers
    top = matplotlib.transforms.blended_transform_factory
    for ax, lines in ((ax_l, ["earlier stay", "H_P3_OLD"]), (ax_c, ["the dose's stay", "2024-03-10"]),
                      (ax_r, ["later stay", "H_P4_NEW"])):
        box = stack([text_box([(lines[0], "sans")], size=FONT_SUB, color=INK2),
                     text_box([(lines[1], "mono" if lines[1].startswith("H_") else "sans")],
                              size=FONT_SUB, color=INK2)], align="center", sep=1)
        put(ax, box, (0.5, 1.0), offset=(0, 14), align=(0.5, 0), xycoords="axes fraction", mask=False)
    ax_c.axvline(10, color=INK2, lw=0.8, alpha=0.45, zorder=1)
    label(ax_c, 10, 1.0, "dose 10:00", offset=(0, 3), va="bottom", size=FONT_SUB,
          transform=top(ax_c.transData, ax_c.transAxes))

    served_by = {}
    _, picks = _fig1_served()
    for stay in lanes:
        y = lane_y[stay]
        stay_bar(ax_c, 7, 14, y)
        if stay in ACROSS:
            patient, stays = ACROSS[stay]
            lane_label(ax_l, y, stay, f"patient {patient}", x=-0.04)
            name, served_t, _, _ = picks[stay]
            ax_far = ax_l if name == "prior_hosp_latest" else ax_r
            for sid, admit, end, ws in stays:
                if sid == stay:
                    continue
                stay_bar(ax_far, mdates.date2num(admit), mdates.date2num(end), y)
                for k, (t, kg) in enumerate(ws):
                    served = sid != stay and t == served_t
                    weight(ax_far, mdates.date2num(t), y, STRATEGY[name] if served else None)
                    label(ax_far, mdates.date2num(t), y, f"{kg:.0f}", offset=(0, 7), va="bottom")
                    if served:
                        connect(fig, ax_c, (10, y), ax_far, (mdates.date2num(t), y), STRATEGY[name])
                        served_by[stay] = (name, kg, DOSE_1 - t)
        else:
            lane_label(ax_l, y, stay, x=-0.04)
            ws = WITHIN[stay]
            pick = picks[stay]
            name = None if pick is None else pick[0]
            i = None if pick is None else [t for t, _ in ws].index(pick[1])
            for k, (t, kg) in enumerate(ws):
                weight(ax_c, hours_of(t), y, STRATEGY[name] if k == i else None)
                label(ax_c, hours_of(t), y, f"{kg:.0f}", offset=(0, 7), va="bottom")
            if i is not None:
                connect(fig, ax_c, (10, y), ax_c, (hours_of(ws[i][0]), y), STRATEGY[name])
                served_by[stay] = (name, ws[i][1], DOSE_1 - ws[i][0])
        dose(ax_c, 10, y)

    # served-by column
    col_tr = top(fig.dpi_scale_trans, ax_c.transData)
    put(ax_c, text_box([("served by", "sans")], size=FONT_SUB, color=INK2),
        (x_col, n - 0.45), offset=(0, 14), align=(0, 0), xycoords=col_tr, mask=False)
    for stay in lanes:
        y = lane_y[stay]
        if stay in served_by:
            name, kg, age = served_by[stay]
            sw = ("● ", "sans", STRATEGY[name], 12)  # swatch: the served-weight mark
            lines = [text_box([sw, (name, "mono")]),
                     text_box([("    ", "sans"), (f"{kg:.0f} kg · age {age_label(age)}", "sans")],
                              color=INK2, size=FONT_SUB)]
        else:
            lines = [text_box([("○ ", "sans", INK2, 12), ("nothing: ", "sans"),
                               ("_weight_source", "mono"), (" NULL", "sans")]),
                     text_box([("    ", "sans"), ("…: weight_kg is missing", "sans")], color=INK2,
                              size=FONT_SUB)]
        put(ax_c, stack(lines, sep=1), (x_col, y), align=(0, 0.5), xycoords=col_tr, mask=False)

    footnote(fig, 0.3, [("Cut time axis: the weeks between stays are not drawn; the ages give them.",
                         "sans")])
    footnote(fig, 0.12, AGE_NOTE)
    return fig


# ================================================================ Figure 2
def fig2_staleness():
    REGISTRY.clear()
    n_rows = len(FIG2_ROWS)
    row_y = {"none": 3.45, "cap": 1.6, "cap_moved": 0.45}
    ylim = (-0.5, 3.95)
    h = (ylim[1] - ylim[0]) * 0.56
    fig_h = 0.6 + 0.22 + h + 0.42 + 0.48
    fig = plt.figure(figsize=(FIG_W, fig_h))
    bottom = 0.48 + 0.42
    x_m, w_m = 1.45, 4.75
    x_z, w_z = x_m + w_m + 0.08, FIG_W - (x_m + w_m + 0.08) - 0.12
    ax_m = add_axes_in(fig, x_m, bottom, w_m, h)
    ax_z = add_axes_in(fig, x_z, bottom, w_z, h)
    days = lambda td: td / D  # noqa: E731
    split = 10 * D + 9 * H     # main axis ends at day 10, 09:00; the zoom starts there

    legend(fig, strategy_handles(list(FIG2_CHAIN))
           + [MARK_HANDLES[k] for k in ("weight_noun", "dose", "window", "rejected")],
           ncol=3, y_top=fig_h - 0.02)

    for ax in (ax_m, ax_z):
        style_lanes(ax, 1)
        ax.set_ylim(*ylim)
    ax_m.set_xlim(0.75, days(split))
    ax_m.set_xticks([d + 10 / 24 for d in range(1, 10)])
    ax_m.set_xticklabels([f"day {d}" for d in range(1, 10)])
    ax_m.set_xlabel("Day of stay (ticks at 10:00, the dose's clock time)", labelpad=3)
    ax_z.set_xlim(days(split), days(10 * D + 12 * H))
    ax_z.set_xticks([days(10 * D + 10 * H), days(10 * D + 11 * H)])
    ax_z.set_xticklabels(["10:00", "11:00"])
    ax_z.set_xlabel("Day 10, hour", labelpad=3)
    break_marks(ax_m, ax_z)
    xd = days(DOSE_2)
    top = matplotlib.transforms.blended_transform_factory
    ax_z.axvline(xd, color=INK2, lw=0.8, alpha=0.45, zorder=1)
    label(ax_z, xd, 1.0, "dose day 10, 10:00", offset=(0, 3), va="bottom", size=FONT_SUB,
          transform=top(ax_z.transData, ax_z.transAxes))

    # cap window: shaded behind the capped rows only
    y0, y1 = -0.45, 2.1
    lo = days(DOSE_2 - 7 * D)
    ax_m.add_patch(Rectangle((lo, y0), days(split) - lo, y1 - y0, fc=WINDOW, ec="none", zorder=0))
    ax_z.add_patch(Rectangle((days(split), y0), xd - days(split), y1 - y0, fc=WINDOW, ec="none",
                             zorder=0))
    put(ax_m, stack([
        text_box([("cap window [dose − 7 d, dose]: both ends included", "sans")],
                 size=FONT_SUB, color=INK2),
        text_box([("a weight charted after the dose always passes", "sans")],
                 size=FONT_SUB, color=INK2)], sep=1.5),
        (lo, y1), offset=(0, 3), align=(0, 0), mask=False)

    # row labels: max_staleness_days value + a plain-words line
    put(ax_m, text_box([("max_staleness_days", "mono")], size=FONT_SUB, color=INK2),
        (-0.02, ylim[1]), offset=(0, 2), align=(1, 0),
        xycoords=top(ax_m.transAxes, ax_m.transData), mask=False)
    row_text = {"none": ("None", "no cap"), "cap": ("7", "7-day cap"),
                "cap_moved": ("7", "7-day cap; 2nd weight\nmoved to day 2")}

    for key, cap, times in FIG2_ROWS:
        y = row_y[key]
        lane_label(ax_m, y, row_text[key][0], row_text[key][1], x=-0.02)
        stay_bar(ax_m, 0.75, days(split), y)
        stay_bar(ax_z, days(split), days(10 * D + 12 * H), y)
        i, name, rejected = serve_same_stay(times, cap)
        rej = {k for _, k in rejected}
        for k, t in enumerate(times):
            ax = ax_m if t < split else ax_z
            outside = cap is not None and DOSE_2 - t > cap * D
            weight(ax, days(t), y, STRATEGY[name] if k == i else None, dim=outside,
                   crossed=k in rej)
            words = (f"{age_label(DOSE_2 - t)[1:]} old" if t <= DOSE_2
                     else f"{age_label(DOSE_2 - t)[1:]} after")
            label(ax, days(t), y, words, offset=(0, 7), va="bottom",
                  color=MUTED if outside else INK2)
        dose(ax_z, xd, y)
        for strat, k in rejected:
            t = times[k]
            connect(fig, ax_z, (xd, y), ax_m, (days(t), y), INK2, rejected=True)
            put(ax_m, text_box([(strat, "mono"), (f"  {age_label(DOSE_2 - t)}: older than the cap,"
                                                  " falls through", "sans")], color=INK2),
                ((days(t) + days(split)) / 2, y), offset=(0, -LABEL_DROP), align=(0.5, 1),
                mask_fc=WINDOW_ON_BG)
        if i is None:
            continue
        t = times[i]
        ax_w = ax_m if t < split else ax_z
        connect(fig, ax_z, (xd, y), ax_w, (days(t), y), STRATEGY[name])
        note = {"none": "", "cap": "  exactly 7 d: on the boundary, kept"}.get(key, "")
        if ax_w is ax_m:
            put(ax_m, text_box([(name, "mono"), (f"  {age_label(DOSE_2 - t)}{note}", "sans")]),
                ((days(t) + days(split)) / 2, y), offset=(0, -LABEL_DROP), align=(0.5, 1),
                mask_fc=BG if cap is None else WINDOW_ON_BG)
        else:
            put(ax_z, stack([text_box([(name, "mono")]),
                             text_box([(f"{age_label(DOSE_2 - t)}", "sans")],  # "1 h after" sits above
                                      size=FONT_SUB, color=INK2)], align="right", sep=1),
                (1.0, y), offset=(0, -LABEL_DROP), align=(1, 1),
                xycoords=top(ax_z.transAxes, ax_z.transData), mask=False)

    footnote(fig, 0.3, [("later_hosp_earliest", "mono"), (" is never gated by the cap.", "sans")])
    footnote(fig, 0.12, AGE_NOTE)
    return fig


# ================================================================ checks
def check_layout(fig, name):
    """Report overlapping labels / markers in display space (geometry only)."""
    fig.canvas.draw()
    r = fig.canvas.get_renderer()
    boxes = []
    for kind, obj in REGISTRY:
        if kind == "label":
            boxes.append((obj, obj.get_window_extent(r)))
        elif kind == "legend":
            for t in obj.get_texts():
                boxes.append((t, t.get_window_extent(r)))
        else:
            ax, x, y, size_pt = obj
            px, py = ax.transData.transform((x, y))
            rad = size_pt / 2 * fig.dpi / 72
            boxes.append((("marker", ax, x, y),
                          matplotlib.transforms.Bbox([[px - rad, py - rad], [px + rad, py + rad]])))
    for ax in fig.axes:
        lo, hi = sorted(ax.get_xlim())
        for t in ax.get_xticklabels():
            if t.get_text() and lo <= t.get_position()[0] <= hi:
                boxes.append((t, t.get_window_extent(r)))
    issues = []
    for a in range(len(boxes)):
        for b in range(a + 1, len(boxes)):
            (oa, ba), (ob, bb) = boxes[a], boxes[b]
            both_markers = isinstance(oa, tuple) and isinstance(ob, tuple)
            ia = ba.padded(-0.5)
            ib = bb.padded(-0.5)
            if ia.overlaps(ib):
                w = min(ia.x1, ib.x1) - max(ia.x0, ib.x0)
                hh = min(ia.y1, ib.y1) - max(ia.y0, ib.y0)
                if w > 0.5 and hh > 0.5:
                    issues.append(("marker-marker" if both_markers else "OVERLAP",
                                   _desc(oa), _desc(ob), round(w), round(hh)))
    fw, fh = fig.get_figwidth() * fig.dpi, fig.get_figheight() * fig.dpi
    for o, bb in boxes:
        if bb.x0 < 0 or bb.y0 < 0 or bb.x1 > fw or bb.y1 > fh:
            issues.append(("OFF-CANVAS", _desc(o), "", round(bb.x0), round(bb.x1)))
    print(f"[{name}] {len(boxes)} items, {len(issues)} issues")
    for i in issues:
        print("   ", i)
    return issues


def _desc(o):
    if isinstance(o, tuple):
        return f"marker@{o[2]:.2f},{o[3]:.2f}"
    if isinstance(o, Text):
        return f"text:{o.get_text()[:30]!r}"
    texts = [t.get_text() for t in o.findobj(Text) if t.get_text()]
    return f"box:{' '.join(texts)[:40]!r}"


FIGURES = (("weight-strategies", fig1_broken_axis), ("staleness-window", fig2_staleness))


def main(out_dir=None):
    """Write both PNGs to `out_dir` (default: docs/images/med-unit-conversion)."""
    out_dir = Path(out_dir) if out_dir is not None else OUT_DIR
    apply_theme()
    out_dir.mkdir(parents=True, exist_ok=True)
    for stem, build in FIGURES:
        fig = build()
        check_layout(fig, stem)
        # no Software / date metadata, so a rerun writes identical bytes
        fig.savefig(out_dir / f"{stem}.png", dpi=DPI, bbox_inches="tight", pad_inches=0.03,
                    transparent=TRANSPARENT, metadata={"Software": None})
        plt.close(fig)
        print("wrote", out_dir / f"{stem}.png")


if __name__ == "__main__":
    main()
