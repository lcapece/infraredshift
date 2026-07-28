"""Workload Triage: the default screen.

One ranked list of parent query patterns (recurring SQL shapes), each expandable
into the individual query IDs that belong to it, with a verdict — fix the query,
fix the tables it hits, or both — plus evidence and a recommended fix.
"""
from __future__ import annotations

import math
import os
import re
from pathlib import Path

import pandas as pd
from PySide6.QtCore import QObject, QPoint, QPointF, QRectF, Qt, QThread, QTimer, QUrl, Signal
from PySide6.QtGui import (
    QDesktopServices,
    QColor,
    QFont,
    QPainter,
    QPainterPath,
    QPen,
    QSyntaxHighlighter,
    QTextCharFormat,
    QTextCursor,
)
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QCompleter,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QStackedWidget,
    QStyledItemDelegate,
    QTabWidget,
    QAbstractItemView,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QToolTip,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..join_size_highlight import (
    LARGE_TABLE_MB_DEFAULT,
    alias_map,
    annotate_line,
    build_table_meta,
)
from ..md_render import md_inline as _md_inline
from ..md_render import render_markdown_card as _render_markdown_card
from ..sproc_focus import executable_statement_spans
# Imported rather than restated so the export button tooltip cannot drift
# from the limit the export actually applies.
from ..association_export import MAX_QUERY_IDS_PER_CLUSTER as _EXPORT_ID_LIMIT
from ..theme import PALETTE, is_light_theme

_MAX_CHILD_ROWS = 200
_MAX_CHART_BUBBLES = 200
# The tree renders synchronously on the GUI thread and each row does per-cell
# numeric coercion, so an uncapped multi-thousand-group capture froze the UI
# for minutes at "Rendering ...". Cap to the top groups by priority (they are
# already sorted); the count label reports the total.
_MAX_TREE_GROUPS = 500


class _ProcedureFocusHighlighter(QSyntaxHighlighter):
    """Dims procedure scaffolding so the executable statements stay in focus.

    With no spans set, the document is left untouched, so plain SQL patterns
    render exactly as before.
    """

    def __init__(self, document):
        super().__init__(document)
        self._spans: list[tuple[int, int]] = []
        self._aliases: dict[str, str] = {}
        self._table_meta: dict[str, dict] = {}
        self._large_mb = LARGE_TABLE_MB_DEFAULT

    def set_spans(self, spans: list[tuple[int, int]]) -> None:
        self._spans = sorted(spans or [])
        self.rehighlight()

    def set_size_context(self, sql: str, table_meta: dict[str, dict]) -> None:
        """Enable size-aware coloring of `=` conditions and table references.
        Pass an empty dict to disable (no metadata -> no coloring)."""
        self._table_meta = table_meta or {}
        self._aliases = alias_map(sql) if self._table_meta else {}
        self.rehighlight()

    def highlightBlock(self, text: str) -> None:
        if self._spans:
            block_start = self.currentBlock().position()
            block_end = block_start + len(text)
            dim = QTextCharFormat()
            dim.setForeground(QColor(PALETTE.text_3))
            self.setFormat(0, len(text), dim)
            focus = QTextCharFormat()
            focus.setForeground(QColor(PALETTE.text_0))
            for span_start, span_end in self._spans:
                if span_end <= block_start or span_start >= block_end:
                    continue
                local_start = max(span_start - block_start, 0)
                local_end = min(span_end - block_start, len(text))
                self.setFormat(local_start, local_end - local_start, focus)
        if not self._table_meta:
            return
        severity_colors = {"red": PALETTE.crit, "yellow": PALETTE.warn, "green": PALETTE.ok}
        for start, end, kind in annotate_line(text, self._aliases, self._table_meta, self._large_mb):
            fmt = QTextCharFormat()
            if kind == "small":
                fmt.setForeground(QColor(PALETTE.text_3))
            elif kind == "large":
                tint = QColor(PALETTE.crit)
                tint.setAlpha(46)
                fmt.setBackground(tint)
                fmt.setFontWeight(QFont.Bold)
            else:
                fmt.setForeground(QColor(severity_colors[kind]))
                fmt.setFontWeight(QFont.Bold)
            self.setFormat(start, end - start, fmt)

# Severity roles stay fixed; colors must be resolved at paint/render time so a
# theme toggle after import does not leave light-mode text on dark tokens (or
# the reverse). Color is never the only signal — emoji + severity text remain.
_VERDICT_SEVERITY = {
    "FIX BOTH": "crit",
    "FIX QUERY": "warn",
    "FIX TABLES": "warn",
    "MONITOR": "ok",
}


def _verdict_colors() -> dict[str, str]:
    return {
        "FIX BOTH": PALETTE.crit,
        "FIX QUERY": PALETTE.warn,
        "FIX TABLES": PALETTE.accent,
        "MONITOR": PALETTE.ok,
    }


def _spectrum_bubble_color() -> str:
    # Dark amber on light theme keeps Spectrum markers readable on white cards.
    return "#B45309" if is_light_theme() else "#FFD166"

_IMPACT_ROLE = Qt.UserRole + 1
_COLOR_ROLE = Qt.UserRole + 2
_GROUP_ROLE = Qt.UserRole + 3
_COPY_FLASH_ROLE = Qt.UserRole + 4
_SORT_ROLE = Qt.UserRole + 5


class _SortableTreeItem(QTreeWidgetItem):
    """Tree row that sorts columns by an underlying numeric value when one is
    set (via _SORT_ROLE), so formatted cells like '1.2K', '3.4m', or '82%' sort
    by magnitude instead of alphabetically. Columns with no numeric role fall
    back to case-insensitive text. Child rows always sort after parents are
    reordered, but member children keep their own ordering."""

    def __lt__(self, other: "QTreeWidgetItem") -> bool:
        column = 0
        tree = self.treeWidget()
        if tree is not None:
            column = tree.sortColumn()
        left = self.data(column, _SORT_ROLE)
        right = other.data(column, _SORT_ROLE)
        if left is not None and right is not None:
            try:
                return float(left) < float(right)
            except (TypeError, ValueError):
                pass
        return self.text(column).strip().lower() < other.text(column).strip().lower()


def _collapse_label(captured: int, patterns: int) -> str:
    """"13,232 → 68" — both sides, because the ratio is the point.

    A bare pattern count means nothing without the number it came from: 68
    patterns is unremarkable until you know it accounts for 13,232 queries.
    """
    captured = max(0, int(captured))
    patterns = max(0, int(patterns))
    if not patterns:
        return "-"
    return f"{captured:,} → {patterns:,}"


def _collapse_tooltip(captured: int, patterns: int) -> str:
    captured = max(0, int(captured))
    patterns = max(0, int(patterns))
    if not patterns:
        return "No repeat patterns have been grouped yet."
    lines = [
        f"{captured:,} captured slow queries grouped into {patterns:,} "
        f"repeating pattern(s).",
    ]
    if captured > patterns:
        # State the reduction as a plain fraction. A "99.5% reduction" reads as
        # a marketing number; "1 pattern per 195 queries" is checkable.
        lines.append(
            f"Roughly 1 pattern per {captured / patterns:,.0f} captured queries."
        )
        lines.append(
            "Fixing one pattern addresses every query that shares its shape."
        )
    return "\n".join(lines)


def _fmt_duration(seconds: object) -> str:
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        return "-"
    if math.isnan(value) or value <= 0:
        return "-"
    if value < 90:
        return f"{value:.1f}s"
    if value < 5400:
        return f"{value / 60:.1f}m"
    return f"{value / 3600:.1f}h"


def _fmt_count(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if math.isnan(number):
        return "-"
    if number >= 1e9:
        return f"{number / 1e9:.1f}B"
    if number >= 1e6:
        return f"{number / 1e6:.1f}M"
    if number >= 1e3:
        return f"{number / 1e3:.1f}K"
    return f"{number:,.0f}"


def _text(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value)


_SPECTRUM_LABEL = "Spect: Fix Query"
# Legacy constant kept for any external import; prefer _spectrum_bubble_color().
_SPECTRUM_BUBBLE_COLOR = "#FFD166"
_SPECTRUM_NUMERIC_COLUMNS = (
    "external_steps",
    "avg_external_steps",
    "s3_steps",
    "avg_s3_steps",
    "external_input_bytes",
    "avg_external_input_bytes",
    "external_input_rows",
    "avg_external_input_rows",
    "external_duration_s",
    "avg_external_duration_s",
    "external_duration_pct",
    "avg_external_duration_pct",
    "external_tables_touched",
    "avg_external_tables_touched",
    "s3_scan_cnt",
    "avg_s3_scan_cnt",
)
_SPECTRUM_TEXT_COLUMNS = (
    "dominant_issue",
    "triage_query_flags",
    "triage_recommendation",
    "shared_tables",
    "sql_tables",
    "sql_tables_full",
    "representative_sql",
    "sample_sql",
    "sql_shape",
)
_SPECTRUM_TEXT_MARKERS = (
    "spectrum",
    "external/s3",
    "external scan",
    "external table",
    "s3 scan",
    "s3_scan",
    "s3://",
    "svl_s3",
)
_QUERY_ID_COLUMNS = (
    "query_ids",
    "bridge_query_ids",
    "example_query_ids",
    "representative_query_id",
    "example_query_id_1",
    "example_query_id_2",
    "example_query_id_3",
)


def _to_number(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(number):
        return 0.0
    return number


_SPECTRUM_DURATION_SHARE = 0.25


def _spectrum_s3_scan_count(group: object) -> float:
    """Actual S3/Spectrum scan steps for the group. This is the ONLY external
    signal computed with the correct positive `source = 's3'` test, so it is the
    trustworthy anchor even in snapshots captured before the external-metric fix."""
    return max(
        _to_number(group.get("s3_scan_cnt")),  # type: ignore[attr-defined]
        _to_number(group.get("avg_s3_scan_cnt")),  # type: ignore[attr-defined]
        _to_number(group.get("s3_steps")),  # type: ignore[attr-defined]
        _to_number(group.get("avg_s3_steps")),  # type: ignore[attr-defined]
    )


def _is_spectrum_group(group: object) -> bool:
    """True only when the group has REAL Spectrum/S3 scan steps AND that external
    work is a meaningful share of runtime.

    Requiring an actual S3 scan count (the one external metric derived from the
    correct `source = 's3'` test) is what prevents ordinary local queries from
    being mislabeled Spectrum - the historical bug that made ~all slow queries
    look external. `external_duration_pct` alone is NOT trusted: in older
    snapshots it was inflated by a faulty `source <> 'internal'` filter. Text
    markers like 's3://' are ignored here because COPY/UNLOAD statements and
    table/schema names containing 'spectrum' produced false positives."""
    if group is None or not hasattr(group, "get"):
        return False
    # Hard gate: no real S3 scan step -> not Spectrum, full stop.
    if _spectrum_s3_scan_count(group) < 1:
        return False
    # There IS genuine S3 work; require it to be a meaningful share of runtime so
    # a single incidental external scan does not dominate the pattern's verdict.
    share = max(
        _to_number(group.get("external_duration_pct")),  # type: ignore[attr-defined]
        _to_number(group.get("avg_external_duration_pct")),  # type: ignore[attr-defined]
    )
    if share > 0:
        return share >= _SPECTRUM_DURATION_SHARE
    # S3 scans present but no usable duration share: treat as Spectrum only when
    # the external step count is itself non-trivial.
    return _spectrum_s3_scan_count(group) >= 1


_UTILITY_QUERY_TYPES = {"utility", "vacuum", "analyze", "ddl"}

CHART_FILTERS: tuple[tuple[str, str], ...] = (
    ("all", "All patterns"),
    ("sproc", "Stored procedures"),
    ("spectrum", "Spectrum / external"),
    ("plain", "Plain SQL (non-Spectrum)"),
    ("utility", "Utilities (VACUUM / DDL)"),
)

# Independent, multi-selectable category toggles (icon + label). Any combination
# may be checked at once; unchecking all shows everything.
CHART_CATEGORY_TOGGLES: tuple[tuple[str, str, str], ...] = (
    ("sproc", "\U0001F9EE", "Sprocs"),          # abacus
    ("spectrum", "\U0001F7E1", "Spectrum"),      # yellow circle (matches bubble)
    ("mixed", "\U0001F500", "Mixed"),            # twisted arrows: local + external
    ("view", "\U0001F441", "Uses View"),         # eye: references a view
    ("plain", "\U0001F4C4", "Plain SQL"),        # page
    ("utility", "\U0001F527", "Utilities"),      # wrench
)

# Categories that classify by a query's table mix rather than its query type.
# They are matched independently of _group_category so a mixed sproc still
# counts as mixed.
_TABLE_MIX_CATEGORIES = {"mixed", "view"}


def _is_mixed_query_group(group: object) -> bool:
    """True when the group touches BOTH external and regular Redshift tables."""
    if group is None or not hasattr(group, "get"):
        return False
    return str(group.get("mixed_query_class") or "").strip().lower() == "mixed"


def _uses_view_group(group: object) -> bool:
    """True when the group references at least one known view."""
    if group is None or not hasattr(group, "get"):
        return False
    return bool(group.get("uses_view"))


class _CategoryToggle(QPushButton):
    """One checkable icon+label chip for a chart category."""

    def __init__(self, key: str, icon: str, label: str, parent=None):
        super().__init__(f"{icon}  {label}", parent)
        self.category_key = key
        self.setObjectName("Ghost")
        self.setCheckable(True)
        self.setCursor(Qt.PointingHandCursor)
        self.setToolTip(f"Show {label} patterns. Toggle any combination; none checked = show all.")


class _CategoryFilterBar(QWidget):
    """A row of independent category toggles. Emits `changed` on any toggle."""

    changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        lay = QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(4)
        self._toggles: list[_CategoryToggle] = []
        for key, icon, label in CHART_CATEGORY_TOGGLES:
            chip = _CategoryToggle(key, icon, label)
            chip.toggled.connect(lambda _checked: self.changed.emit())
            lay.addWidget(chip)
            self._toggles.append(chip)

    def selected_categories(self) -> set[str]:
        """Checked category keys. Empty set means 'show all' (no filter)."""
        return {t.category_key for t in self._toggles if t.isChecked()}

    def clear(self, *, emit: bool = True) -> None:
        """Clear every category so the chart includes all query types."""
        for toggle in self._toggles:
            prior = toggle.blockSignals(True)
            toggle.setChecked(False)
            toggle.blockSignals(prior)
        if emit:
            self.changed.emit()


def _group_category(group: object) -> str:
    """Classify a pattern for chart filtering. Stored procedures win over the
    Spectrum heuristic so a sproc whose body mentions s3:// still filters as a
    procedure; utilities are keyed off sys_query_history's query_type."""
    if group is None or not hasattr(group, "get"):
        return "plain"
    if _text(group.get("repeat_kind")) == "stored_procedure":  # type: ignore[attr-defined]
        return "sproc"
    query_type = _text(group.get("query_type")).strip().lower()  # type: ignore[attr-defined]
    if query_type in _UTILITY_QUERY_TYPES:
        return "utility"
    if _is_spectrum_group(group):
        return "spectrum"
    return "plain"


def _display_verdict(group: object) -> str:
    if _is_spectrum_group(group):
        return _SPECTRUM_LABEL
    if hasattr(group, "get"):
        return _text(group.get("triage_verdict")) or "MONITOR"  # type: ignore[attr-defined]
    return "MONITOR"


def _split_query_ids(value: object) -> list[str]:
    text = _text(value).strip()
    if not text:
        return []
    out: list[str] = []
    for part in text.replace(";", ",").split(","):
        query_id = part.strip()
        if query_id and query_id.lower() not in {"none", "nan", "<na>"}:
            out.append(query_id)
    return out


_VERDICT_EMOJI = {
    "FIX BOTH": "\U0001F534",   # red circle
    "FIX TABLES": "\U0001F535", # blue circle (matches the accent bubble color)
    "FIX QUERY": "\U0001F7E1",  # yellow circle
    "MONITOR": "\U0001F7E2",    # green circle
}


def _verdict_html(group: dict, spectrum: bool, display_verdict: str) -> str:
    """Colored, emoji-tagged verdict header for the recommendation card.

    Reads the live PALETTE at render time (not import time) so the color is
    correct after a theme toggle."""
    verdict = _text(group.get("triage_verdict")) or "MONITOR"
    if spectrum:
        color = PALETTE.warn
        emoji = "\U0001F7E1"
    else:
        color = {
            "FIX BOTH": PALETTE.crit,
            "FIX QUERY": PALETTE.warn,
            "FIX TABLES": PALETTE.accent,
            "MONITOR": PALETTE.ok,
        }.get(verdict, PALETTE.accent)
        emoji = _VERDICT_EMOJI.get(verdict, "\U0001F4CC")
    label = str(display_verdict or verdict).upper()
    return (
        f"<div style='margin:0 0 4px 0;'>"
        f"<span style='color:{color}; font-weight:800; font-size:13px;'>{emoji} {label}</span>"
        f"</div>"
    )


def _query_id_sort_key(value: str):
    """Numeric-ascending when the id parses as an int, else lexical. Query IDs
    are numeric in Redshift, so this yields the natural ascending order a DBA
    expects when the copied list is pasted into a SQL IN(...) or a spreadsheet."""
    try:
        return (0, int(str(value).strip()))
    except (TypeError, ValueError):
        return (1, str(value))


def _query_ids_for_group(group: dict | pd.Series | None, members: pd.DataFrame | None) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()

    def add(value: object) -> None:
        for query_id in _split_query_ids(value):
            if query_id in seen:
                continue
            seen.add(query_id)
            ids.append(query_id)

    if group is None:
        return ids
    group_id = _text(group.get("repeat_group_id")) if hasattr(group, "get") else ""
    if group_id and members is not None and not members.empty and "repeat_group_id" in members.columns and "query_id" in members.columns:
        rows = members[members["repeat_group_id"].astype(str) == group_id]
        for value in rows["query_id"]:
            add(value)
    for column in _QUERY_ID_COLUMNS:
        if hasattr(group, "get"):
            add(group.get(column))  # type: ignore[attr-defined]
    # Return the ids sorted ascending by query id so Copy One / Copy All and the
    # summary all present a stable numeric order regardless of capture order.
    return sorted(ids, key=_query_id_sort_key)


def _query_id_summary(ids: list[str]) -> str:
    if not ids:
        return "Query IDs: -"
    shown = ", ".join(ids[:30])
    if len(ids) > 30:
        shown += f", ... (+{len(ids) - 30} more)"
    return f"Query IDs ({len(ids):,}): {shown}"


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame is None or frame.empty or column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)


class _ImpactBarDelegate(QStyledItemDelegate):
    """Paints the Impact column as a proportional pain bar."""

    def paint(self, painter: QPainter, option, index) -> None:
        fraction = index.data(_IMPACT_ROLE)
        if fraction is None:
            super().paint(painter, option, index)
            return
        color_name = index.data(_COLOR_ROLE) or PALETTE.accent
        rect = option.rect.adjusted(6, 7, -6, -7)
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, True)
        track = QColor(PALETTE.bg_3)
        painter.setPen(Qt.NoPen)
        painter.setBrush(track)
        painter.drawRoundedRect(QRectF(rect), 3.0, 3.0)
        width = max(3.0, rect.width() * max(0.02, min(1.0, float(fraction))))
        fill = QColor(color_name)
        painter.setBrush(fill)
        bar = QRectF(rect.x(), rect.y(), width, rect.height())
        painter.drawRoundedRect(bar, 3.0, 3.0)
        painter.restore()


class _Tile(QFrame):
    """One summary metric, laid out horizontally to preserve vertical space.

    These five metrics are context, not the point of the page — the bubble chart
    is. The original stacked-card form cost roughly 58px of height (22px value
    over an 11px caption, plus card padding and border), which was enough to
    push the chart off-screen and force a scrollbar on shorter displays.

    Value and label now sit side by side in a single line, so the whole row
    costs about 24px. No card chrome: five bordered boxes drew the eye away from
    the chart they were supposed to support.
    """

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self.setObjectName("TileInline")
        lay = QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 14, 0)
        lay.setSpacing(5)
        self._value = QLabel("-")
        self._value.setObjectName("TileValue")
        self._label = QLabel(label.upper())
        self._label.setObjectName("Caption")
        lay.addWidget(self._value)
        lay.addWidget(self._label)
        lay.addStretch(1)
        # Full text stays reachable on hover even when the row is tight.
        self.setToolTip(label)

    def set_value(self, text: str) -> None:
        self._value.setText(text)
        self.setToolTip(f"{self._label.text().title()}: {text}")


CHART_METRICS: tuple[tuple[str, str, str], ...] = (
    # (key, combo label, source total column)
    ("rows", "Rows read per run", "total_input_rows"),
    ("bytes", "Bytes read per run", "total_input_bytes"),
    ("spill", "Spill blocks per run", "total_spill_blocks"),
    ("queue", "Queue seconds per run", "total_queue_s"),
)

CHART_RUNTIME_FILTERS: tuple[tuple[str, float], ...] = (
    ("Any average runtime", 0.0),
    ("At least 1 second/run", 1.0),
    ("At least 5 seconds/run", 5.0),
    ("At least 10 seconds/run", 10.0),
    ("At least 30 seconds/run", 30.0),
    ("At least 1 minute/run", 60.0),
    ("At least 5 minutes/run", 300.0),
    # The Producer capture floor is already 300s, so every Producer group
    # clears the 5-minute rung and the filter stops discriminating exactly
    # where the expensive patterns live. These rungs go past the floor.
    ("At least 15 minutes/run", 900.0),
    ("At least 30 minutes/run", 1_800.0),
    ("At least 1 hour/run", 3_600.0),
    ("At least 2 hours/run", 7_200.0),
    ("At least 4 hours/run", 14_400.0),
    ("At least 8 hours/run", 28_800.0),
)

CHART_SCENARIOS: tuple[tuple[str, str], ...] = (
    ("Overview - slowest per run", "overview"),
    ("Slow but light on resources", "slow_light"),
    ("Heavy spill + long runtime", "spill_slow"),
    ("High I/O + shuffle, little output", "io_shuffle"),
    ("Queue-bound / waiting", "queue_bound"),
    ("Skewed or remote scan", "skew_remote"),
)


def _chart_numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(0.0, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)


def _filter_chart_groups(
    groups: pd.DataFrame,
    metric_key: str,
    *,
    categories: set[str] | None = None,
    minimum_avg_runtime_s: float = 0.0,
    positive_metric_only: bool = True,
    scenario: str = "overview",
    limit: int = _MAX_CHART_BUBBLES,
) -> tuple[pd.DataFrame, int]:
    """Apply chart-only filters and enforce the hard bubble-count ceiling."""
    if groups is None or groups.empty:
        return pd.DataFrame(), 0
    filtered = groups.copy()
    if categories:
        type_cats = {c for c in categories if c not in _TABLE_MIX_CATEGORIES}
        mix_cats = categories & _TABLE_MIX_CATEGORIES

        def _row_matches(row) -> bool:
            # OR across all checked toggles (matches existing semantics).
            if type_cats and _group_category(row) in type_cats:
                return True
            if "mixed" in mix_cats and _is_mixed_query_group(row):
                return True
            if "view" in mix_cats and _uses_view_group(row):
                return True
            return False

        mask = filtered.apply(_row_matches, axis=1)
        filtered = filtered[mask].copy()
    if filtered.empty:
        return filtered, 0

    runs = _chart_numeric(filtered, "query_count").clip(lower=1)
    runtime = _chart_numeric(filtered, "total_runtime_s")
    filtered["__chart_avg_runtime"] = runtime / runs
    metric_column = next(
        (column for key, _label, column in CHART_METRICS if key == metric_key),
        "total_input_rows",
    )
    metric_total = _chart_numeric(filtered, metric_column)
    filtered["__chart_metric_per_run"] = metric_total / runs
    filtered["__chart_runs"] = runs

    input_rows = _chart_numeric(filtered, "total_input_rows") / runs
    output_rows = _chart_numeric(filtered, "total_output_rows") / runs
    input_bytes = _chart_numeric(filtered, "total_input_bytes") / runs
    spill = _chart_numeric(filtered, "total_spill_blocks") / runs
    queue = _chart_numeric(filtered, "total_queue_s") / runs
    movement = _chart_numeric(filtered, "avg_dist_both_cnt") + _chart_numeric(filtered, "avg_bcast_cnt")
    skew = pd.concat(
        [
            _chart_numeric(filtered, "avg_max_data_skewness"),
            _chart_numeric(filtered, "avg_max_time_skewness"),
        ],
        axis=1,
    ).max(axis=1)
    remote = _chart_numeric(filtered, "avg_remote_io_ratio")
    s3_scans = _chart_numeric(filtered, "avg_s3_scan_cnt")
    output_ratio = output_rows / input_rows.where(input_rows > 0, 1.0)

    score = filtered["__chart_avg_runtime"].copy()
    if scenario == "slow_light":
        positive_rows = input_rows[input_rows > 0]
        light_ceiling = float(positive_rows.median()) if not positive_rows.empty else 1_000_000.0
        mask = (filtered["__chart_avg_runtime"] >= 10.0) & (input_rows <= light_ceiling) & (spill <= 0)
        filtered = filtered[mask]
        score = filtered["__chart_avg_runtime"] / (
            1.0 + input_rows[mask].map(lambda value: math.log10(1.0 + max(float(value), 0.0)))
        )
    elif scenario == "spill_slow":
        mask = spill > 0
        filtered = filtered[mask]
        score = filtered["__chart_avg_runtime"] * (
            1.0 + spill[mask].map(lambda value: math.log10(1.0 + max(float(value), 0.0)))
        )
    elif scenario == "io_shuffle":
        mask = (movement > 0) & (input_bytes > 0) & (output_ratio <= 0.25)
        filtered = filtered[mask]
        score = (
            filtered["__chart_avg_runtime"]
            * (1.0 + movement[mask])
            * (1.0 + input_bytes[mask].map(lambda value: math.log10(1.0 + max(float(value), 0.0))))
            * (2.0 - output_ratio[mask].clip(lower=0, upper=1))
        )
    elif scenario == "queue_bound":
        mask = queue > 0
        filtered = filtered[mask]
        score = queue[mask] * (1.0 + queue[mask] / filtered["__chart_avg_runtime"].clip(lower=0.001))
    elif scenario == "skew_remote":
        mask = (skew >= 4.0) | (remote >= 0.30) | (s3_scans > 0)
        filtered = filtered[mask]
        score = filtered["__chart_avg_runtime"] * (
            1.0 + skew[mask].clip(lower=0) + remote[mask].clip(lower=0) * 10.0 + s3_scans[mask]
        )

    filtered["__chart_scenario_score"] = score.reindex(filtered.index).fillna(0.0)
    if minimum_avg_runtime_s > 0:
        filtered = filtered[filtered["__chart_avg_runtime"] >= float(minimum_avg_runtime_s)]
    if positive_metric_only:
        # Zero and missing telemetry cannot be placed truthfully on a log axis.
        # Hiding it by default prevents a false "efficient" baseline.
        filtered = filtered[filtered["__chart_metric_per_run"] > 0]
    matching_count = len(filtered)

    filtered = filtered.sort_values(
        ["__chart_scenario_score", "__chart_avg_runtime", "total_runtime_s"],
        ascending=[False, False, False],
        kind="stable",
    ).head(max(1, min(int(limit), _MAX_CHART_BUBBLES)))
    return filtered.drop(
        columns=["__chart_avg_runtime", "__chart_metric_per_run", "__chart_runs", "__chart_scenario_score"],
        errors="ignore",
    ), matching_count


def _fmt_metric(key: str, value: float) -> str:
    if key == "queue":
        return _fmt_duration(value)
    if key == "bytes":
        for unit, size in (("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
            if value >= size:
                return f"{value / size:.1f} {unit}"
        return f"{value:,.0f} B"
    return _fmt_count(value)


class _QuadrantChart(QFrame):
    """Bubble quadrant: X = avg runtime/run (log), Y = resource/run (log),
    bubble area = run count, color = verdict status (never color-alone: the
    legend carries text labels and incomplete evidence adds a dashed ring)."""

    groupClicked = Signal(str)
    groupDoubleClicked = Signal(str)
    # Right-click on a bubble: (group id, global position) for the assign menu.
    groupContextRequested = Signal(str, QPoint)

    _MARGIN_L, _MARGIN_R, _MARGIN_T, _MARGIN_B = 70, 18, 46, 44
    _MIN_R, _MAX_R = 7.0, 30.0

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("Card")
        self.setMouseTracking(True)
        self.setMinimumHeight(320)
        self._points: list[dict] = []
        self._metric_key = CHART_METRICS[0][0]
        self._metric_label = CHART_METRICS[0][1]
        self._selected_gid = ""
        self._hover_gid = ""
        self._shuffle_anchor_gid = ""
        self._shuffle_level = -1
        self._empty_message = "Load triage analysis to plot patterns"
        self._shuffle_btn = QPushButton("Shuffle Forward", self)
        self._shuffle_btn.setObjectName("Ghost")
        self._shuffle_btn.setToolTip(
            "Select a bubble, then bring it forward first. Each additional click brings the next whole overlap layer forward."
        )
        self._shuffle_btn.setCursor(Qt.PointingHandCursor)
        self._shuffle_btn.setEnabled(False)
        self._shuffle_btn.clicked.connect(self.shuffle_forward)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        hint = self._shuffle_btn.sizeHint()
        self._shuffle_btn.resize(hint)
        self._shuffle_btn.move(self.width() - hint.width() - self._MARGIN_R, 4)

    def set_groups(
        self,
        groups: pd.DataFrame,
        metric_key: str,
        *,
        empty_message: str = "Load triage analysis to plot patterns",
    ) -> None:
        self._empty_message = empty_message
        self._metric_key = metric_key
        self._metric_label = next(
            (label for key, label, _ in CHART_METRICS if key == metric_key), metric_key
        )
        column = next((col for key, _, col in CHART_METRICS if key == metric_key), "total_input_rows")
        self._points = []
        if groups is not None and not groups.empty:
            for _, group in groups.iterrows():
                runs = max(float(group.get("query_count") or 0.0), 1.0)
                x = float(group.get("total_runtime_s") or 0.0) / runs
                y = float(group.get(column) or 0.0) / runs
                self._points.append(
                    {
                        "gid": str(group.get("repeat_group_id") or ""),
                        "x": x,
                        "y": y,
                        "runs": runs,
                        "distinct_sql": int(float(group.get("distinct_sql_count") or 0.0)),
                        "verdict": _display_verdict(group),
                        "base_verdict": str(group.get("triage_verdict") or "MONITOR"),
                        "spectrum": _is_spectrum_group(group),
                        "mixed": _is_mixed_query_group(group),
                        "coverage": str(group.get("triage_stats_coverage") or "complete"),
                        "users": str(group.get("users") or ""),
                        "tables": str(group.get("shared_tables") or ""),
                        "total_runtime": float(group.get("total_runtime_s") or 0.0),
                    }
                )
        point_ids = {point["gid"] for point in self._points}
        if self._selected_gid not in point_ids:
            self._shuffle_anchor_gid = ""
            self._shuffle_level = -1
        self._shuffle_btn.setEnabled(bool(self._points and self._selected_gid in point_ids))
        self.update()

    def set_selected(self, gid: str) -> None:
        selected = gid or ""
        if selected != self._selected_gid:
            self._shuffle_anchor_gid = selected
            self._shuffle_level = -1
        self._selected_gid = selected
        self._shuffle_btn.setEnabled(
            bool(selected and any(point["gid"] == selected for point in self._points))
        )
        self.update()

    def shuffle_forward(self) -> None:
        if not self._points or not self._selected_gid:
            return
        if self._shuffle_anchor_gid != self._selected_gid:
            self._shuffle_anchor_gid = self._selected_gid
            self._shuffle_level = -1
        laid = self._layout_points()
        layers = self._overlap_layers(laid, self._selected_gid)
        max_layer = max(layers.values(), default=0)
        self._shuffle_level = (self._shuffle_level + 1) % (max_layer + 1)
        self.update()

    # ------------------------------------------------------------- geometry

    def _plot_rect(self) -> QRectF:
        return QRectF(
            self._MARGIN_L,
            self._MARGIN_T,
            max(self.width() - self._MARGIN_L - self._MARGIN_R, 10),
            max(self.height() - self._MARGIN_T - self._MARGIN_B, 10),
        )

    @staticmethod
    def _log_domain(values: list[float]) -> tuple[float, float]:
        positive = [v for v in values if v > 0]
        if not positive:
            return 0.1, 10.0
        lo, hi = min(positive), max(positive)
        if lo == hi:
            lo, hi = lo / 3.0, hi * 3.0
        return lo * 0.7, hi * 1.4

    @staticmethod
    def _log_frac(value: float, lo: float, hi: float) -> float:
        value = max(value, lo)
        span = math.log10(hi) - math.log10(lo)
        if span <= 0:
            return 0.5
        return (math.log10(value) - math.log10(lo)) / span

    def _layout_points(self) -> list[dict]:
        if not self._points:
            return []
        rect = self._plot_rect()
        xlo, xhi = self._log_domain([p["x"] for p in self._points])
        ylo, yhi = self._log_domain([p["y"] for p in self._points])
        max_runs = max(p["runs"] for p in self._points)
        laid = []
        for p in self._points:
            fx = self._log_frac(p["x"], xlo, xhi)
            fy = self._log_frac(p["y"], ylo, yhi)
            radius = self._MIN_R + (self._MAX_R - self._MIN_R) * math.sqrt(p["runs"] / max_runs)
            laid.append(
                {
                    **p,
                    "cx": rect.x() + fx * rect.width(),
                    "cy": rect.y() + (1.0 - fy) * rect.height(),
                    "r": radius,
                }
            )
        self._domains = (xlo, xhi, ylo, yhi)
        return laid

    def _z_ordered_points(self, laid: list[dict]) -> list[dict]:
        ordered = sorted(laid, key=lambda item: -item["r"])
        if not ordered or self._shuffle_level < 0 or not self._shuffle_anchor_gid:
            return ordered
        layers = self._overlap_layers(ordered, self._shuffle_anchor_gid)
        promoted = [item for item in ordered if layers.get(item["gid"]) == self._shuffle_level]
        if not promoted:
            return ordered
        promoted_ids = {item["gid"] for item in promoted}
        return [item for item in ordered if item["gid"] not in promoted_ids] + promoted

    @staticmethod
    def _overlap_layers(laid: list[dict], anchor_gid: str) -> dict[str, int]:
        """Return geometric overlap layers outward from the selected bubble."""
        by_gid = {item["gid"]: item for item in laid}
        if anchor_gid not in by_gid:
            return {}
        neighbors: dict[str, set[str]] = {gid: set() for gid in by_gid}
        for index, left in enumerate(laid):
            for right in laid[index + 1 :]:
                dx = left["cx"] - right["cx"]
                dy = left["cy"] - right["cy"]
                if dx * dx + dy * dy <= (left["r"] + right["r"] + 2.0) ** 2:
                    neighbors[left["gid"]].add(right["gid"])
                    neighbors[right["gid"]].add(left["gid"])
        layers = {anchor_gid: 0}
        frontier = [anchor_gid]
        while frontier:
            current = frontier.pop(0)
            next_layer = layers[current] + 1
            for gid in sorted(neighbors[current]):
                if gid not in layers:
                    layers[gid] = next_layer
                    frontier.append(gid)
        return layers

    # -------------------------------------------------------------- painting

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        rect = self._plot_rect()
        laid = self._layout_points()

        painter.setPen(QColor(PALETTE.text_3))
        painter.drawText(
            QRectF(0, 4, self.width(), 18),
            Qt.AlignHCenter,
            "PATTERN QUADRANTS  -  bubble size = number of runs",
        )
        self._draw_legend(painter)

        if not laid:
            painter.setPen(QColor(PALETTE.text_2))
            painter.drawText(rect, Qt.AlignCenter, self._empty_message)
            return

        xlo, xhi, ylo, yhi = self._domains
        self._draw_axes(painter, rect, xlo, xhi, ylo, yhi)

        xs = sorted(p["cx"] for p in laid)
        ys = sorted(p["cy"] for p in laid)
        mx = xs[len(xs) // 2]
        my = ys[len(ys) // 2]
        median_pen = QPen(QColor(PALETTE.text_3))
        median_pen.setStyle(Qt.DashLine)
        painter.setPen(median_pen)
        painter.drawLine(QPointF(mx, rect.top()), QPointF(mx, rect.bottom()))
        painter.drawLine(QPointF(rect.left(), my), QPointF(rect.right(), my))
        painter.setPen(QColor(PALETTE.text_3))
        small = painter.font()
        small.setPointSizeF(max(small.pointSizeF() - 1.5, 6.5))
        painter.setFont(small)
        pad = 6
        painter.drawText(QRectF(mx + pad, rect.top() + 2, rect.right() - mx - pad * 2, 14), Qt.AlignRight, "HEAVY & SLOW - FIX FIRST")
        painter.drawText(QRectF(rect.left() + pad, rect.top() + 2, mx - rect.left() - pad * 2, 14), Qt.AlignLeft, "FAST BUT WASTEFUL")
        painter.drawText(QRectF(mx + pad, rect.bottom() - 16, rect.right() - mx - pad * 2, 14), Qt.AlignRight, "SLOW BUT LIGHT")
        painter.drawText(QRectF(rect.left() + pad, rect.bottom() - 16, mx - rect.left() - pad * 2, 14), Qt.AlignLeft, "HEALTHY")

        draw_points = self._z_ordered_points(laid)
        for p in draw_points:
            colors = _verdict_colors()
            color = QColor(
                _spectrum_bubble_color()
                if p.get("spectrum")
                else colors.get(p["verdict"], PALETTE.accent)
            )
            fill = QColor(color)
            fill.setAlpha(150)
            ring = QPen(QColor(PALETTE.bg_1))
            ring.setWidthF(2.0)
            painter.setPen(ring)
            painter.setBrush(fill)
            painter.drawEllipse(QPointF(p["cx"], p["cy"]), p["r"] + 1.0, p["r"] + 1.0)
            outline = QPen(color)
            outline.setWidthF(1.6)
            if p["coverage"] not in ("", "complete"):
                outline.setStyle(Qt.DashLine)
            painter.setPen(outline)
            painter.drawEllipse(QPointF(p["cx"], p["cy"]), p["r"], p["r"])
            if p.get("mixed"):
                # Distinct marker for mixed queries (both external + local
                # tables): a bold blue outer ring, unmistakable against the
                # verdict-colored fill.
                mixed_ring = QPen(QColor("#2B8CBE"))
                mixed_ring.setWidthF(2.4)
                painter.setPen(mixed_ring)
                painter.setBrush(Qt.NoBrush)
                painter.drawEllipse(QPointF(p["cx"], p["cy"]), p["r"] + 2.2, p["r"] + 2.2)
            if p["gid"] == self._selected_gid:
                halo = QPen(QColor(PALETTE.text_0))
                halo.setWidthF(2.0)
                painter.setPen(halo)
                painter.setBrush(Qt.NoBrush)
                painter.drawEllipse(QPointF(p["cx"], p["cy"]), p["r"] + 3.5, p["r"] + 3.5)
            if p["r"] >= 14:
                painter.setPen(QColor(PALETTE.text_0))
                painter.drawText(
                    QRectF(p["cx"] - p["r"], p["cy"] - 8, p["r"] * 2, 16),
                    Qt.AlignCenter,
                    p["gid"],
                )
        self._laid_cache = draw_points

    def _draw_legend(self, painter: QPainter) -> None:
        font = painter.font()
        font.setPointSizeF(max(font.pointSizeF() - 1.5, 6.5))
        painter.setFont(font)
        x = float(self._MARGIN_L)
        y = 26.0
        for verdict in ("FIX BOTH", _SPECTRUM_LABEL, "FIX QUERY", "FIX TABLES", "MONITOR"):
            colors = _verdict_colors()
            color = QColor(
                _spectrum_bubble_color()
                if verdict == _SPECTRUM_LABEL
                else colors[verdict]
            )
            painter.setPen(Qt.NoPen)
            painter.setBrush(color)
            painter.drawEllipse(QPointF(x + 4, y + 4), 4, 4)
            painter.setPen(QColor(PALETTE.text_2))
            width = painter.fontMetrics().horizontalAdvance(verdict)
            painter.drawText(QPointF(x + 12, y + 8), verdict)
            x += 12 + width + 14
        painter.setPen(QColor(PALETTE.text_3))
        painter.drawText(QPointF(x + 4, y + 8), "dashed ring = table evidence incomplete")

    def _draw_axes(self, painter: QPainter, rect: QRectF, xlo, xhi, ylo, yhi) -> None:
        grid = QPen(QColor(PALETTE.bg_3))
        text = QColor(PALETTE.text_3)
        font = painter.font()
        font.setPointSizeF(max(font.pointSizeF() - 1.5, 6.5))
        painter.setFont(font)
        for exponent in range(math.floor(math.log10(xlo)), math.ceil(math.log10(xhi)) + 1):
            value = 10.0 ** exponent
            if value < xlo or value > xhi:
                continue
            px = rect.x() + self._log_frac(value, xlo, xhi) * rect.width()
            painter.setPen(grid)
            painter.drawLine(QPointF(px, rect.top()), QPointF(px, rect.bottom()))
            painter.setPen(text)
            painter.drawText(QRectF(px - 32, rect.bottom() + 4, 64, 14), Qt.AlignHCenter, _fmt_duration(value))
        for exponent in range(math.floor(math.log10(ylo)), math.ceil(math.log10(yhi)) + 1):
            value = 10.0 ** exponent
            if value < ylo or value > yhi:
                continue
            py = rect.y() + (1.0 - self._log_frac(value, ylo, yhi)) * rect.height()
            painter.setPen(grid)
            painter.drawLine(QPointF(rect.left(), py), QPointF(rect.right(), py))
            painter.setPen(text)
            painter.drawText(QRectF(2, py - 7, self._MARGIN_L - 8, 14), Qt.AlignRight | Qt.AlignVCenter, _fmt_metric(self._metric_key, value))
        painter.setPen(text)
        painter.drawText(
            QRectF(rect.left(), self.height() - 18, rect.width(), 14),
            Qt.AlignHCenter,
            "AVG RUNTIME PER RUN (log scale)",
        )
        painter.save()
        painter.translate(12, rect.center().y())
        painter.rotate(-90)
        painter.drawText(QRectF(-120, -7, 240, 14), Qt.AlignHCenter, f"{self._metric_label.upper()} (log scale)")
        painter.restore()

    # ------------------------------------------------------------ interaction

    def _hit(self, pos) -> dict | None:
        laid = getattr(self, "_laid_cache", None) or []
        for p in reversed(laid):
            dx = pos.x() - p["cx"]
            dy = pos.y() - p["cy"]
            if dx * dx + dy * dy <= (p["r"] + 2.0) ** 2:
                return p
        return None

    def mouseMoveEvent(self, event) -> None:
        p = self._hit(event.position())
        if p is not None:
            if p["gid"] != self._hover_gid:
                self._hover_gid = p["gid"]
            QToolTip.showText(
                event.globalPosition().toPoint(),
                (
                    f"{p['gid']} - {p['verdict']}\n"
                    f"{int(p['runs']):,} runs"
                    + (f", {int(p['distinct_sql']):,} distinct SQL" if p.get("distinct_sql") else "")
                    + f", {_fmt_duration(p['total_runtime'])} total\n"
                    f"avg {_fmt_duration(p['x'])}/run, {_fmt_metric(self._metric_key, p['y'])}\n"
                    f"tables: {p['tables'][:80]}\nusers: {p['users'][:60]}"
                    + ("\nSPECTRUM/EXTERNAL: fix SQL or stage locally, not Spectrum table DDL" if p.get("spectrum") else "")
                    + ("\nTABLE EVIDENCE INCOMPLETE" if p["coverage"] not in ("", "complete") else "")
                ),
                self,
            )
            self.setCursor(Qt.PointingHandCursor)
        else:
            self._hover_gid = ""
            self.unsetCursor()
        super().mouseMoveEvent(event)

    def mousePressEvent(self, event) -> None:
        p = self._hit(event.position())
        if p is not None:
            self._selected_gid = p["gid"]
            self.update()
            self.groupClicked.emit(p["gid"])
            if event.button() == Qt.RightButton:
                self.groupContextRequested.emit(
                    p["gid"], event.globalPosition().toPoint()
                )
        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:
        p = self._hit(event.position())
        if p is not None:
            self._selected_gid = p["gid"]
            self.update()
            self.groupClicked.emit(p["gid"])
            self.groupDoubleClicked.emit(p["gid"])
        super().mouseDoubleClickEvent(event)


class _AssignEngineerDialog(QDialog):
    """Searchable pick-list of roster users (ingested from SVV_USER_INFO).

    Shared by two actions with different meanings: assigning the engineer who
    owns the fix, and associating the business user the query belongs to. Only
    the wording differs, so the picker is parameterized rather than duplicated.
    """

    def __init__(
        self,
        group_label: str,
        choices: list[dict],
        current_display: str = "",
        parent=None,
        *,
        title: str = "Assign Query Group",
        prompt: str = "",
    ):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumWidth(420)
        self._choices = choices
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)
        caption = QLabel(
            (f"{prompt} " if prompt else
             f"Assign pattern {group_label} to an engineer for review/fix. ")
            + "Type to search the roster (Lastname, Firstname)."
        )
        caption.setObjectName("Caption")
        caption.setWordWrap(True)
        root.addWidget(caption)
        self._combo = QComboBox()
        self._combo.setEditable(True)
        self._combo.setInsertPolicy(QComboBox.NoInsert)
        self._combo.setAccessibleName("Engineer pick list")
        for choice in choices:
            self._combo.addItem(choice["display"], choice["user_name"])
        completer = QCompleter([choice["display"] for choice in choices], self._combo)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        self._combo.setCompleter(completer)
        index = self._combo.findText(current_display) if current_display else -1
        self._combo.setCurrentIndex(index if index >= 0 else -1)
        if index < 0:
            self._combo.setCurrentText("")
        root.addWidget(self._combo)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

    def selected(self) -> tuple[str, str]:
        """(user_name, display) for the chosen engineer, or ('', '') if none."""
        text = self._combo.currentText().strip()
        if not text:
            return "", ""
        index = self._combo.findText(text)
        if index >= 0:
            return str(self._combo.itemData(index) or ""), self._combo.itemText(index)
        # Free text that matches no roster entry is not an assignment.
        return "", ""


class _SpectrumViewScanWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)
    progress = Signal(int, int)

    def __init__(
        self,
        db_path: str,
        snapshot_id: str | None,
        view_definitions: pd.DataFrame,
    ):
        super().__init__()
        self._db_path = db_path
        self._snapshot_id = snapshot_id
        self._view_definitions = view_definitions.copy()

    def run(self) -> None:
        try:
            from ..spectrum_view_cache import identify_possible_spectrum_views

            thread = QThread.currentThread()
            result = identify_possible_spectrum_views(
                self._db_path,
                self._snapshot_id,
                self._view_definitions,
                progress=lambda complete, total: self.progress.emit(
                    complete, total
                ),
                cancelled=thread.isInterruptionRequested,
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.finished.emit(result)


class _AnalysisProcessFlow(QWidget):
    """Display-only overview of the ten-stage triage analysis process."""

    STEPS = (
        (1, "VERIFY", "Enabled sources"),
        (2, "CAPTURE", "Clusters in parallel"),
        (3, "NORMALIZE", "Data identities"),
        (4, "PRUNE", "Query candidates"),
        (5, "HYDRATE", "SQL + evidence"),
        (6, "VIEWS", "Identify + explode"),
        (7, "TABLES", "Resolve table health"),
        (8, "EXTERNAL", "Match metadata"),
        (9, "RANK", "Total impact"),
        (10, "RECOMMEND", "Action plans"),
    )
    PARALLEL_STEPS = (6, 7, 8)
    _PHASES = (
        frozenset((1,)),
        frozenset((2,)),
        frozenset((3,)),
        frozenset((4,)),
        frozenset((5,)),
        frozenset(PARALLEL_STEPS),
        frozenset((9,)),
        frozenset((10,)),
    )
    _EDGES = (
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 6),
        (5, 7),
        (5, 8),
        (6, 9),
        (7, 9),
        (8, 9),
        (9, 10),
    )

    def __init__(self, parent=None, *, scale: float = 1.0):
        super().__init__(parent)
        self.setObjectName("AnalysisProcessFlow")
        self.setAccessibleName("Ten-step analysis process")
        self.setAccessibleDescription(
            "Display-only animated overview. Steps one through five run in "
            "sequence, steps six through eight run in parallel, and steps nine "
            "and ten complete the process."
        )
        self.setToolTip(
            "Display-only process overview. It does not start queries, load "
            "data, or change staging or live tables."
        )
        self._diagram_scale = max(0.50, min(float(scale), 1.0))
        self.setFixedSize(
            round(1080 * self._diagram_scale),
            round(214 * self._diagram_scale),
        )
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self._phase = 0
        self._phase_progress = 0.0
        self._reduce_motion = str(
            os.environ.get("INFRAREDSHIFT_REDUCE_MOTION") or ""
        ).strip().lower() in {"1", "true", "yes", "on"}
        self._timer = QTimer(self)
        self._timer.setInterval(80)
        self._timer.timeout.connect(self._tick)

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if not self._reduce_motion:
            self._timer.start()

    def hideEvent(self, event) -> None:
        self._timer.stop()
        super().hideEvent(event)

    def _tick(self) -> None:
        self._phase_progress += 0.055
        if self._phase_progress >= 1.0:
            self._phase_progress = 0.0
            self._phase = (self._phase + 1) % len(self._PHASES)
        self.update()

    def _active_steps(self) -> frozenset[int]:
        if self._reduce_motion:
            return frozenset()
        return self._PHASES[self._phase]

    def _completed_steps(self) -> frozenset[int]:
        if self._reduce_motion:
            return frozenset()
        complete: set[int] = set()
        for phase in self._PHASES[: self._phase]:
            complete.update(phase)
        return frozenset(complete)

    @staticmethod
    def _with_alpha(color_value: str, alpha: int) -> QColor:
        color = QColor(color_value)
        color.setAlpha(alpha)
        return color

    def _node_rects(self, canvas: QRectF) -> dict[int, QRectF]:
        left = canvas.left() + 16.0
        usable = canvas.width() - 32.0
        center_y = canvas.top() + 112.0
        fractions = {
            1: 0.055,
            2: 0.175,
            3: 0.295,
            4: 0.415,
            5: 0.535,
            6: 0.680,
            7: 0.680,
            8: 0.680,
            9: 0.840,
            10: 0.958,
        }
        centers_y = {
            6: canvas.top() + 63.0,
            7: center_y,
            8: canvas.top() + 161.0,
        }
        rects: dict[int, QRectF] = {}
        for step, fraction in fractions.items():
            width = 112.0
            height = 52.0
            if step in self.PARALLEL_STEPS:
                width = 148.0
                height = 38.0
            center_x = left + usable * fraction
            node_y = centers_y.get(step, center_y)
            rects[step] = QRectF(
                center_x - width / 2.0,
                node_y - height / 2.0,
                width,
                height,
            )
        return rects

    @staticmethod
    def _edge_path(source: QRectF, target: QRectF) -> QPainterPath:
        start = QPointF(source.right(), source.center().y())
        end = QPointF(target.left(), target.center().y())
        path = QPainterPath(start)
        horizontal = max(18.0, (end.x() - start.x()) * 0.48)
        path.cubicTo(
            QPointF(start.x() + horizontal, start.y()),
            QPointF(end.x() - horizontal, end.y()),
            end,
        )
        return path

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.scale(self._diagram_scale, self._diagram_scale)
        canvas = QRectF(
            0.0,
            0.0,
            self.width() / self._diagram_scale,
            self.height() / self._diagram_scale,
        ).adjusted(1.0, 1.0, -1.0, -1.0)

        painter.setPen(QPen(QColor(PALETTE.border), 1.0))
        painter.setBrush(QColor(PALETTE.bg_1))
        painter.drawRoundedRect(canvas, 10.0, 10.0)

        painter.setPen(QColor(PALETTE.text_0))
        painter.setFont(QFont("Segoe UI", 9, QFont.Bold))
        painter.drawText(
            QRectF(canvas.left() + 16, canvas.top() + 8, 230, 20),
            Qt.AlignLeft | Qt.AlignVCenter,
            "ANALYSIS PROCESS",
        )
        painter.setPen(QColor(PALETTE.text_2))
        painter.setFont(QFont("Segoe UI", 8))
        painter.drawText(
            QRectF(canvas.left() + 154, canvas.top() + 8, 420, 20),
            Qt.AlignLeft | Qt.AlignVCenter,
            "10 stages  •  steps 6–8 run in parallel",
        )

        rects = self._node_rects(canvas)
        active = self._active_steps()
        completed = self._completed_steps()

        for source_step, target_step in self._EDGES:
            path = self._edge_path(rects[source_step], rects[target_step])
            source_complete = source_step in completed
            target_complete = target_step in completed
            if source_complete and target_complete:
                edge_color = QColor(PALETTE.ok)
                edge_width = 1.5
            elif source_step in active:
                edge_color = QColor(PALETTE.accent)
                edge_width = 2.0
            else:
                edge_color = QColor(PALETTE.border_strong)
                edge_width = 1.2
            painter.setPen(QPen(edge_color, edge_width))
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(path)

            if source_step in active and not self._reduce_motion:
                packet = path.pointAtPercent(self._phase_progress)
                painter.setPen(Qt.NoPen)
                painter.setBrush(QColor(PALETTE.accent_bright))
                painter.drawEllipse(packet, 4.0, 4.0)

        step_lookup = {step: (title, detail) for step, title, detail in self.STEPS}
        for step, rect in rects.items():
            title, detail = step_lookup[step]
            is_active = step in active
            is_complete = step in completed
            if is_active:
                border = QColor(PALETTE.accent)
                fill = self._with_alpha(PALETTE.accent_dim, 76)
                width = 2.2
            elif is_complete:
                border = QColor(PALETTE.ok)
                fill = self._with_alpha(PALETTE.ok, 20)
                width = 1.4
            else:
                border = QColor(PALETTE.border_strong)
                fill = QColor(PALETTE.bg_2)
                width = 1.0
            painter.setPen(QPen(border, width))
            painter.setBrush(fill)
            painter.drawRoundedRect(rect, 7.0, 7.0)

            number_rect = QRectF(rect.left() + 5, rect.top() + 5, 22, 18)
            painter.setPen(Qt.NoPen)
            painter.setBrush(border)
            painter.drawRoundedRect(number_rect, 5.0, 5.0)
            painter.setPen(QColor(PALETTE.bg_1))
            painter.setFont(QFont("Segoe UI", 7, QFont.Bold))
            painter.drawText(number_rect, Qt.AlignCenter, str(step))

            painter.setPen(
                QColor(PALETTE.accent_bright)
                if is_active
                else QColor(PALETTE.text_0)
            )
            painter.setFont(QFont("Segoe UI", 7, QFont.Bold))
            title_rect = QRectF(
                rect.left() + 29,
                rect.top() + 4,
                rect.width() - 34,
                18,
            )
            painter.drawText(
                title_rect, Qt.AlignLeft | Qt.AlignVCenter, title
            )
            painter.setPen(QColor(PALETTE.text_2))
            painter.setFont(QFont("Segoe UI", 7))
            detail_rect = QRectF(
                rect.left() + 7,
                rect.top() + 22,
                rect.width() - 14,
                rect.height() - 25,
            )
            painter.drawText(
                detail_rect,
                Qt.AlignCenter | Qt.TextWordWrap,
                detail,
            )

        branch_rect = rects[7]
        painter.setPen(QColor(PALETTE.warn))
        painter.setFont(QFont("Segoe UI", 7, QFont.Bold))
        painter.drawText(
            QRectF(
                branch_rect.left(),
                rects[8].bottom() + 4,
                branch_rect.width(),
                14,
            ),
            Qt.AlignCenter,
            "PARALLEL",
        )


class TriagePage(QWidget):
    """Parent-pattern → child-query triage explorer."""

    loadRequested = Signal(str)
    queryDiagramRequested = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._groups = pd.DataFrame()
        self._members = pd.DataFrame()
        self._group_tables = pd.DataFrame()
        self._action_queue = pd.DataFrame()
        self._table_review = pd.DataFrame()
        self._view_definitions = pd.DataFrame()
        self._slow_queries = pd.DataFrame()
        self._snapshot_id: str | None = None
        self._db_path = ""
        self._spectrum_scan_result = None
        self._spectrum_scan_thread: QThread | None = None
        self._spectrum_scan_worker: _SpectrumViewScanWorker | None = None
        self._copy_flash_serial = 0
        self._query_copy_flash_serial = 0

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)
        self._page_scroll = QScrollArea()
        self._page_scroll.setWidgetResizable(True)
        self._page_scroll.setFrameShape(QFrame.NoFrame)
        self._page_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._page_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        outer.addWidget(self._page_scroll, 1)

        content = QWidget()
        content.setObjectName("TriagePageContent")
        self._page_scroll.setWidget(content)

        root = QVBoxLayout(content)
        root.setContentsMargins(14, 12, 14, 12)
        root.setSpacing(10)

        header = QHBoxLayout()
        title_box = QVBoxLayout()
        title = QLabel("WORKLOAD TRIAGE")
        title.setObjectName("SectionHeader")
        title.setAccessibleName("Workload Triage")
        title_box.addWidget(title)
        header.addLayout(title_box, 1)
        self._load_btn = QPushButton("Load Triage Analysis")
        self._load_btn.setObjectName("Primary")
        self._load_btn.clicked.connect(
            lambda: self.loadRequested.emit("repeat_queries")
        )
        header.addWidget(self._load_btn, 0, Qt.AlignTop)
        root.addLayout(header)

        display_controls = QHBoxLayout()
        display_controls.setSpacing(6)
        self._chart_btn = QPushButton("Chart")
        self._list_btn = QPushButton("List")
        for btn in (self._chart_btn, self._list_btn):
            btn.setObjectName("Ghost")
            btn.setCheckable(True)
        self._chart_btn.setAccessibleName("Chart view")
        self._chart_btn.setAccessibleDescription(
            "Show recurring query patterns as a chart. Colors also have text labels."
        )
        self._list_btn.setAccessibleName("List view")
        self._list_btn.setAccessibleDescription(
            "Show recurring query patterns as an expandable list with verdicts."
        )
        self._chart_btn.setChecked(True)
        self._chart_btn.clicked.connect(lambda: self._set_view(0))
        self._list_btn.clicked.connect(lambda: self._set_view(1))
        self._metric_combo = QComboBox()
        for key, label, _col in CHART_METRICS:
            self._metric_combo.addItem(label, key)
        self._metric_combo.currentIndexChanged.connect(self._refresh_chart)
        self._filter_bar = _CategoryFilterBar()
        self._filter_bar.changed.connect(self._refresh_chart)
        display_controls.addWidget(self._chart_btn)
        display_controls.addWidget(self._list_btn)
        display_controls.addWidget(self._filter_bar, 1)
        root.addLayout(display_controls)

        utility_controls = QHBoxLayout()
        utility_controls.setSpacing(6)
        self._spectrum_scan_btn = QPushButton(
            "Identify Possible Spectrum Views"
        )
        self._spectrum_scan_btn.setObjectName("Ghost")
        self._spectrum_scan_btn.setToolTip(
            "Optional cached background scan of captured view definitions "
            "against Producer SVV_EXTERNAL_COLUMNS metadata. It does not query "
            "Redshift and cannot change staged or live tables."
        )
        self._spectrum_scan_btn.clicked.connect(
            self._identify_possible_spectrum_views
        )
        utility_controls.addWidget(self._spectrum_scan_btn)
        self._script_btn = QPushButton("Generate Structural Script")
        self._script_btn.setObjectName("Ghost")
        self._script_btn.setToolTip("Generate SORTKEY/DISTKEY recommendations from captured SQL and table telemetry.")
        self._script_btn.clicked.connect(self._open_fix_script)
        utility_controls.addWidget(self._script_btn)
        utility_controls.addStretch(1)
        root.addLayout(utility_controls)
        self._spectrum_scan_status = QLabel("")
        self._spectrum_scan_status.setObjectName("Caption")
        self._spectrum_scan_status.setWordWrap(True)
        self._spectrum_scan_status.setVisible(False)
        root.addWidget(self._spectrum_scan_status)

        self._chart_controls = QWidget()
        chart_filters = QGridLayout(self._chart_controls)
        chart_filters.setContentsMargins(0, 0, 0, 0)
        chart_filters.setHorizontalSpacing(8)
        chart_filters.setVerticalSpacing(5)
        scenario_label = QLabel("Scenario")
        scenario_label.setObjectName("Caption")
        chart_filters.addWidget(scenario_label, 0, 0)
        self._scenario_combo = QComboBox()
        for label, key in CHART_SCENARIOS:
            self._scenario_combo.addItem(label, key)
        self._scenario_combo.setToolTip(
            "Choose a diagnostic workload scenario. Results are ranked for that scenario and capped at 200 bubbles."
        )
        self._scenario_combo.currentIndexChanged.connect(self._refresh_chart)
        chart_filters.addWidget(self._scenario_combo, 0, 1)
        self._runtime_combo = QComboBox()
        for label, seconds in CHART_RUNTIME_FILTERS:
            self._runtime_combo.addItem(label, seconds)
        self._runtime_combo.setToolTip("Minimum average elapsed time for each repeated-query pattern.")
        self._runtime_combo.currentIndexChanged.connect(self._refresh_chart)
        chart_filters.addWidget(self._runtime_combo, 0, 2)
        chart_filters.addWidget(self._metric_combo, 0, 3)
        self._metric_coverage_combo = QComboBox()
        self._metric_coverage_combo.addItem("Include zero/missing vertical metric", False)
        self._metric_coverage_combo.addItem("Hide zero/missing vertical metric", True)
        self._metric_coverage_combo.setToolTip(
            "Include missing execution telemetry so repeat patterns still appear. "
            "Choose Hide only when you want a strict logarithmic metric view."
        )
        self._metric_coverage_combo.currentIndexChanged.connect(self._refresh_chart)
        chart_filters.addWidget(self._metric_coverage_combo, 1, 0, 1, 2)
        self._show_all_bubbles_btn = QPushButton("Show All Bubbles")
        self._show_all_bubbles_btn.setObjectName("Ghost")
        self._show_all_bubbles_btn.setToolTip(
            "Reset chart filters. If triage data is not loaded yet, load and "
            "group the captured repeat queries."
        )
        self._show_all_bubbles_btn.clicked.connect(self._show_all_bubbles)
        chart_filters.addWidget(self._show_all_bubbles_btn, 1, 2)
        self._export_assoc_btn = QPushButton("Export User Associations")
        self._export_assoc_btn.setObjectName("Ghost")
        self._export_assoc_btn.setToolTip(
            "Write a Markdown handoff of every pattern with an assigned engineer "
            "or associated user: the grouped SQL, and up to "
            f"{_EXPORT_ID_LIMIT} example query IDs per cluster."
        )
        self._export_assoc_btn.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        self._export_assoc_btn.clicked.connect(self._export_user_associations)
        chart_filters.addWidget(self._export_assoc_btn, 1, 3)
        self._chart_count = QLabel("Showing 0 patterns")
        self._chart_count.setObjectName("Caption")
        self._chart_count.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        chart_filters.addWidget(self._chart_count, 1, 4)
        chart_filters.setColumnStretch(1, 1)
        chart_filters.setColumnStretch(4, 1)
        root.addWidget(self._chart_controls)

        # Single compact strip rather than five cards. Height is pinned so this
        # row can never grow and squeeze the chart below it.
        tile_strip = QWidget()
        tile_strip.setObjectName("TileStrip")
        tiles = QHBoxLayout(tile_strip)
        tiles.setContentsMargins(2, 2, 2, 4)
        tiles.setSpacing(0)
        # The headline number: thousands of "distinct" slow queries collapse to
        # a few dozen shapes. That ratio is the whole argument for the tool, so
        # it goes first and states both sides rather than a bare count.
        self._tile_collapse = _Tile("Slow queries → patterns")
        self._tile_patterns = _Tile("Parent patterns")
        self._tile_runs = _Tile("Repeated runs")
        self._tile_runtime = _Tile("Repeat runtime")
        self._tile_share = _Tile("Share of slow runtime")
        self._tile_tables = _Tile("Tables flagged")
        for tile in (
            self._tile_collapse,
            self._tile_patterns,
            self._tile_runs,
            self._tile_runtime,
            self._tile_share,
            self._tile_tables,
        ):
            tiles.addWidget(tile)
        tiles.addStretch(1)
        tile_strip.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        tile_strip.setMaximumHeight(28)
        root.addWidget(tile_strip)

        body = QSplitter(Qt.Horizontal)
        body.setChildrenCollapsible(False)
        body.setHandleWidth(1)

        self._tree = QTreeWidget()
        self._tree.setAccessibleName("Workload pattern list")
        self._tree.setAccessibleDescription(
            "Expandable list of recurring query patterns. Columns include Impact, "
            "Pattern, Verdict, Runs, Total Time, Avg Time, Rows Read, Users, and "
            "Assigned To. Verdict text is present in addition to color. "
            "Right-click a pattern to assign it to an engineer."
        )
        self._tree.setColumnCount(9)
        self._tree.setHeaderLabels(
            ["Impact", "Pattern", "Verdict", "Runs", "Total Time", "Avg Time", "Rows Read", "Users", "Assigned To"]
        )
        self._tree.setRootIsDecorated(True)
        self._tree.setAlternatingRowColors(True)
        self._tree.setUniformRowHeights(True)
        self._tree.setFocusPolicy(Qt.StrongFocus)
        self._tree.setItemDelegateForColumn(0, _ImpactBarDelegate(self._tree))
        self._tree.setSortingEnabled(True)
        self._tree.sortByColumn(0, Qt.DescendingOrder)
        tree_header = self._tree.header()
        tree_header.setSectionResizeMode(QHeaderView.Interactive)
        tree_header.setSectionsClickable(True)
        tree_header.setSortIndicatorShown(True)
        tree_header.resizeSection(0, 110)
        tree_header.resizeSection(1, 260)
        tree_header.resizeSection(2, 92)
        for col in (3, 4, 5, 6):
            tree_header.resizeSection(col, 84)
        self._tree.itemSelectionChanged.connect(self._on_selection)
        self._tree.itemClicked.connect(self._on_item_click)
        self._tree.itemDoubleClicked.connect(self._on_double_click)
        self._tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self._tree.customContextMenuRequested.connect(self._on_tree_context_menu)

        self._chart = _QuadrantChart()
        self._chart.groupClicked.connect(self._select_group_by_id)
        self._chart.groupDoubleClicked.connect(self._open_group_query_history)
        self._chart.groupContextRequested.connect(self._show_group_context_menu)
        self._views = QStackedWidget()
        self._views.addWidget(self._chart)
        self._views.addWidget(self._tree)
        self._views.setCurrentIndex(0)
        body.addWidget(self._views)

        detail_scroll = QScrollArea()
        detail_scroll.setWidgetResizable(True)
        detail_scroll.setFrameShape(QFrame.NoFrame)
        detail_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        detail = QWidget()
        detail_lay = QVBoxLayout(detail)
        detail_lay.setContentsMargins(10, 0, 4, 0)
        detail_lay.setSpacing(8)

        self._verdict_chip = QLabel("SELECT A PATTERN")
        self._verdict_chip.setProperty("chip", True)
        self._verdict_chip.setProperty("severity", "info")
        self._verdict_chip.setAlignment(Qt.AlignCenter)
        detail_lay.addWidget(self._verdict_chip, 0, Qt.AlignLeft)

        query_id_row = QHBoxLayout()
        query_id_row.setSpacing(6)
        self._query_ids_label = QLabel("Composite queries: -")
        self._query_ids_label.setObjectName("Caption")
        self._query_ids_label.setWordWrap(True)
        query_id_row.addWidget(self._query_ids_label, 1)
        self._query_copy_status = QLabel("")
        self._query_copy_status.setMinimumWidth(86)
        query_id_row.addWidget(self._query_copy_status, 0)
        self._copy_one_btn = QPushButton("Copy One ID")
        self._copy_one_btn.setObjectName("Ghost")
        self._copy_one_btn.setEnabled(False)
        self._copy_one_btn.clicked.connect(self._copy_selected_query_id)
        query_id_row.addWidget(self._copy_one_btn, 0)
        self._copy_all_btn = QPushButton("Copy All IDs")
        self._copy_all_btn.setObjectName("Ghost")
        self._copy_all_btn.setEnabled(False)
        self._copy_all_btn.clicked.connect(self._copy_selected_query_ids)
        query_id_row.addWidget(self._copy_all_btn, 0)
        detail_lay.addLayout(query_id_row)

        self._query_table = QTableWidget(0, 2)
        self._query_table.setObjectName("CompositeQueries")
        self._query_table.setHorizontalHeaderLabels(["Query ID", "SQL"])
        self._query_table.verticalHeader().setVisible(False)
        self._query_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._query_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._query_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._query_table.setAlternatingRowColors(True)
        self._query_table.setWordWrap(False)
        self._query_table.setSortingEnabled(False)
        self._query_table.setMinimumHeight(150)
        self._query_table.setMaximumHeight(220)
        header = self._query_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        self._query_table.setTextElideMode(Qt.ElideRight)
        self._query_table.doubleClicked.connect(self._on_query_table_double_click)
        detail_lay.addWidget(self._query_table)

        self._recommendation = self._detail_card(detail_lay, "RECOMMENDED FIX")
        self._query_evidence = self._detail_card(detail_lay, "QUERY EVIDENCE")
        self._table_evidence = self._detail_card(detail_lay, "TABLE EVIDENCE")
        self._grouping_basis = self._detail_card(detail_lay, "WHY THESE QUERIES ARE ONE PATTERN")

        sql_header = QLabel("REPRESENTATIVE SQL")
        sql_header.setObjectName("Caption")
        detail_lay.addWidget(sql_header)
        size_legend = QLabel(
            f"<span style='color:{PALETTE.crit}; font-weight:700;'>= red</span> large table off its sort key"
            f" &nbsp;<span style='color:{PALETTE.warn}; font-weight:700;'>= yellow</span> large table, key aligned"
            f" &nbsp;<span style='color:{PALETTE.ok}; font-weight:700;'>= green</span> small tables only"
            f" &nbsp;<span style='color:{PALETTE.text_3};'>gray</span> table below the large threshold"
            f" (~{LARGE_TABLE_MB_DEFAULT / 1024:.0f} GB)"
        )
        size_legend.setObjectName("Caption")
        size_legend.setTextFormat(Qt.RichText)
        size_legend.setWordWrap(True)
        detail_lay.addWidget(size_legend)
        sql_actions = QHBoxLayout()
        sql_actions.addStretch(1)
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Ghost")
        format_btn.clicked.connect(self._format_representative_sql)
        lineage_btn = QPushButton("Show Lineage")
        lineage_btn.setObjectName("Ghost")
        lineage_btn.clicked.connect(self._open_representative_lineage)
        subqueries_btn = QPushButton("Extract Subqueries")
        subqueries_btn.setObjectName("Ghost")
        subqueries_btn.clicked.connect(self._open_representative_subqueries)
        sql_actions.addWidget(format_btn)
        sql_actions.addWidget(lineage_btn)
        sql_actions.addWidget(subqueries_btn)
        detail_lay.addLayout(sql_actions)
        self._sql_view = QPlainTextEdit()
        self._sql_view.setObjectName("Mono")
        self._sql_view.setReadOnly(True)
        self._sql_view.setMinimumHeight(180)
        mono = QFont("Consolas")
        mono.setStyleHint(QFont.Monospace)
        self._sql_view.setFont(mono)
        self._sql_focus = _ProcedureFocusHighlighter(self._sql_view.document())
        detail_lay.addWidget(self._sql_view, 1)
        detail_scroll.setWidget(detail)
        body.addWidget(detail_scroll)

        body.setStretchFactor(0, 3)
        body.setStretchFactor(1, 2)
        body.setSizes([760, 460])
        root.addWidget(body, 1)

        self._empty = QLabel(
            "No triage data loaded yet. Click \"Load Triage Analysis\" to group the captured "
            "workload into recurring parent patterns."
        )
        self._empty.setObjectName("Caption")
        self._empty.setAlignment(Qt.AlignCenter)
        self._empty.setWordWrap(True)
        self._empty.setMinimumWidth(0)
        self._empty.setSizePolicy(
            QSizePolicy.Ignored,
            QSizePolicy.Preferred,
        )
        root.addWidget(self._empty)
        self._empty.setVisible(True)

    def _detail_card(self, layout: QVBoxLayout, caption: str) -> QLabel:
        card = QFrame()
        card.setObjectName("Card")
        card_lay = QVBoxLayout(card)
        card_lay.setContentsMargins(12, 8, 12, 10)
        card_lay.setSpacing(4)
        head = QLabel(caption)
        head.setObjectName("Caption")
        body = QLabel("-")
        body.setWordWrap(True)
        body.setTextFormat(Qt.RichText)
        body.setTextInteractionFlags(Qt.TextSelectableByMouse)
        card_lay.addWidget(head)
        card_lay.addWidget(body)
        layout.addWidget(card)
        return body

    def _set_card_markdown(self, label: QLabel, text: str) -> None:
        label.setText(_render_markdown_card(text))

    # ------------------------------------------------------------------ data

    def set_report(self, report) -> None:
        prior_snapshot_id = self._snapshot_id
        groups = getattr(report, "repeat_groups", pd.DataFrame())
        members = getattr(report, "repeat_members", pd.DataFrame())
        group_tables = getattr(report, "repeat_group_tables", pd.DataFrame())
        self._action_queue = getattr(report, "action_queue", pd.DataFrame())
        self._table_review = getattr(report, "table_review", pd.DataFrame())
        self._view_definitions = getattr(report, "view_definitions", pd.DataFrame())
        self._slow_queries = getattr(report, "slow_queries", pd.DataFrame())
        summary = getattr(report, "summary", {}) or {}
        self._snapshot_id = getattr(report, "snapshot_id", None)
        self._db_path = str(getattr(report, "db_path", "") or "")
        if self._snapshot_id != prior_snapshot_id:
            self._spectrum_scan_result = None
            if self._spectrum_scan_thread is None:
                self._spectrum_scan_btn.setText(
                    "Identify Possible Spectrum Views"
                )
            self._spectrum_scan_status.setVisible(False)
        # Merge persisted engineer assignments (durable repeat_group_key) so
        # ownership survives reloads and display-id reshuffles.
        if groups is not None and not groups.empty and self._db_path:
            from ..assignments import attach_assignments, load_assignments

            try:
                groups = attach_assignments(groups, load_assignments(self._db_path))
            except Exception:
                pass
        self.set_dataframes(groups, members, group_tables, summary)

    def _identify_possible_spectrum_views(self) -> None:
        if (
            self._spectrum_scan_thread is not None
            and self._spectrum_scan_thread.isRunning()
        ):
            return
        if (
            self._spectrum_scan_result is not None
            and self._spectrum_scan_result.snapshot_id
            == str(self._snapshot_id or "unversioned")
        ):
            self._show_spectrum_view_results()
            return
        if not self._db_path:
            QMessageBox.information(
                self,
                "Possible Spectrum Views",
                "Load triage data before starting the optional view scan.",
            )
            return
        if self._view_definitions is None or self._view_definitions.empty:
            QMessageBox.information(
                self,
                "Possible Spectrum Views",
                "No captured view definitions are available for this snapshot.",
            )
            return

        thread = QThread()
        worker = _SpectrumViewScanWorker(
            self._db_path,
            self._snapshot_id,
            self._view_definitions,
        )
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.progress.connect(self._on_spectrum_scan_progress)
        worker.finished.connect(self._on_spectrum_scan_finished)
        worker.failed.connect(self._on_spectrum_scan_failed)
        worker.finished.connect(thread.quit)
        worker.failed.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.failed.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._on_spectrum_thread_finished)
        self._spectrum_scan_thread = thread
        self._spectrum_scan_worker = worker
        self._spectrum_scan_btn.setEnabled(False)
        self._spectrum_scan_btn.setText("Spectrum view scan running…")
        self._spectrum_scan_status.setText(
            "Read-only background scan started. Existing triage and loader "
            "results remain available."
        )
        self._spectrum_scan_status.setVisible(True)
        thread.start()

    def _on_spectrum_scan_progress(self, completed: int, total: int) -> None:
        self._spectrum_scan_status.setText(
            f"Checking captured view definitions in the background: "
            f"{completed:,} of {total:,}. Existing results are unchanged."
        )

    def _on_spectrum_scan_finished(self, result) -> None:
        if result.snapshot_id != str(self._snapshot_id or "unversioned"):
            self._spectrum_scan_status.setText(
                "Spectrum-view results were cached for the prior snapshot. "
                "Click the button to scan the current snapshot."
            )
            return
        self._spectrum_scan_result = result
        candidate_count = len(result.candidates)
        self._spectrum_scan_btn.setText(
            f"Possible Spectrum Views ({candidate_count:,})"
        )
        if result.external_table_count == 0:
            self._spectrum_scan_status.setText(
                "Scan finished, but Producer external-table metadata was not "
                "available. No existing triage result was changed."
            )
        else:
            self._spectrum_scan_status.setText(
                f"Scan complete: {candidate_count:,} possible Spectrum-backed "
                f"view(s) among {result.view_count:,}; {result.cache_hits:,} "
                "view result(s) came from cache. Click the button to review."
            )

    def _on_spectrum_scan_failed(self, error: str) -> None:
        self._spectrum_scan_status.setText(
            "Optional Spectrum-view scan stopped without changing triage: "
            + (error or "unknown error")
        )
        self._spectrum_scan_btn.setText(
            "Identify Possible Spectrum Views"
        )

    def _on_spectrum_thread_finished(self) -> None:
        self._spectrum_scan_thread = None
        self._spectrum_scan_worker = None
        self._spectrum_scan_btn.setEnabled(True)

    def _show_spectrum_view_results(self) -> None:
        result = self._spectrum_scan_result
        if result is None:
            return
        candidates = result.candidates
        if candidates is None or candidates.empty:
            detail = (
                "No captured view definition matched the Producer external-table "
                "metadata. This is a cached advisory scan; existing triage was "
                "not changed."
            )
        else:
            lines = []
            for _, row in candidates.head(25).iterrows():
                lines.append(
                    f"• {row.get('view_key') or row.get('view_name')} → "
                    f"{row.get('matched_external_objects') or 'possible external object'}"
                )
            remaining = len(candidates) - len(lines)
            if remaining > 0:
                lines.append(f"• …and {remaining:,} more cached candidate(s)")
            detail = "\n".join(lines)
        QMessageBox.information(
            self,
            "Possible Spectrum Views",
            detail,
        )

    def set_dataframes(
        self,
        groups: pd.DataFrame,
        members: pd.DataFrame,
        group_tables: pd.DataFrame,
        summary: dict,
    ) -> None:
        self._groups = groups if groups is not None else pd.DataFrame()
        self._members = members if members is not None else pd.DataFrame()
        self._group_tables = group_tables if group_tables is not None else pd.DataFrame()
        self._set_tiles(summary)
        self._populate_tree()
        self._refresh_chart()
        has_rows = not self._groups.empty
        self._empty.setVisible(not has_rows)
        if not has_rows:
            note = _text(summary.get("repeat_diagnostic_note"))
            action_count = 0 if self._action_queue is None else len(self._action_queue)
            if action_count:
                self._empty.setText(
                    f"No repeat-query patterns met the grouping rules. {note}\n\n"
                    f"{action_count:,} prioritized fix-queue action(s) are loaded; use Generate Structural Script "
                    "or open the Fix Queue tab for DBA-ready work."
                )
            elif note:
                self._empty.setText(f"No repeat-query patterns met the grouping rules.\n\n{note}")
            else:
                self._empty.setText(
                    "No triage data loaded yet. Click \"Load Triage Analysis\" to group the captured "
                    "workload into recurring parent patterns."
                )
        if has_rows:
            self._tree.setCurrentItem(self._tree.topLevelItem(0))
        else:
            self._set_query_ids([])

    def _set_view(self, index: int) -> None:
        self._views.setCurrentIndex(index)
        self._chart_btn.setChecked(index == 0)
        self._list_btn.setChecked(index == 1)
        self._filter_bar.setVisible(index == 0)
        self._chart_controls.setVisible(index == 0)

    def _refresh_chart(self) -> None:
        key = self._metric_combo.currentData() or CHART_METRICS[0][0]
        selected = self._filter_bar.selected_categories()  # empty set = show all
        scenario = self._scenario_combo.currentData() or "overview"
        minimum_runtime = float(self._runtime_combo.currentData() or 0.0)
        positive_metric_only = bool(self._metric_coverage_combo.currentData())
        empty_message = "Load triage analysis to plot patterns"
        groups, matching_count = _filter_chart_groups(
            self._groups,
            str(key),
            categories=selected,
            minimum_avg_runtime_s=minimum_runtime,
            positive_metric_only=positive_metric_only,
            scenario=str(scenario),
        )
        shown_count = len(groups)
        if matching_count > _MAX_CHART_BUBBLES:
            self._chart_count.setText(
                f"Showing top {_MAX_CHART_BUBBLES} of {matching_count:,} matching patterns"
            )
        else:
            self._chart_count.setText(f"Showing {shown_count:,} matching pattern(s)")
        if self._groups is not None and not self._groups.empty and groups.empty:
            scenario_label = self._scenario_combo.currentText()
            empty_message = f"No patterns match the current filters: {scenario_label}"
        self._chart.set_groups(groups, str(key), empty_message=empty_message)

    def _show_all_bubbles(self) -> None:
        """One-click recovery path for an empty or over-filtered chart."""
        combos = (
            self._scenario_combo,
            self._runtime_combo,
            self._metric_combo,
            self._metric_coverage_combo,
        )
        for combo in combos:
            prior = combo.blockSignals(True)
            combo.setCurrentIndex(0)
            combo.blockSignals(prior)
        self._filter_bar.clear(emit=False)
        self._refresh_chart()
        if self._groups is None or self._groups.empty:
            self.loadRequested.emit("repeat_queries")

    def _select_group_by_id(self, group_id: str) -> None:
        for row in range(self._tree.topLevelItemCount()):
            item = self._tree.topLevelItem(row)
            data = item.data(0, _GROUP_ROLE)
            if isinstance(data, dict) and str(data.get("repeat_group_id")) == group_id:
                self._tree.setCurrentItem(item)
                return

    def _group_by_id(self, group_id: str) -> dict | None:
        groups = self._groups
        if groups is None or groups.empty or "repeat_group_id" not in groups.columns:
            return None
        rows = groups[groups["repeat_group_id"].astype(str) == str(group_id)]
        return rows.iloc[0].to_dict() if not rows.empty else None

    def _on_tree_context_menu(self, pos) -> None:
        item = self._tree.itemAt(pos)
        if item is None:
            return
        data = item.data(0, _GROUP_ROLE)
        # Member rows delegate to their parent pattern.
        if isinstance(data, dict) and data.get("_is_member") and item.parent() is not None:
            data = item.parent().data(0, _GROUP_ROLE)
        if not isinstance(data, dict) or data.get("_is_member"):
            return
        group_id = _text(data.get("repeat_group_id"))
        if group_id:
            self._show_group_context_menu(group_id, self._tree.viewport().mapToGlobal(pos))

    def _show_group_context_menu(self, group_id: str, global_pos) -> None:
        group = self._group_by_id(group_id)
        if group is None:
            return
        current = _text(group.get("assigned_engineer"))
        associated = _text(group.get("associated_user"))
        menu = QMenu(self)
        assign_text = (
            f"Assign to Engineer… (current: {current})"
            if current
            else "Assign to Engineer…"
        )
        assign_action = menu.addAction(assign_text)
        clear_action = menu.addAction("Clear engineer assignment") if current else None
        menu.addSeparator()
        # The engineer who owns the FIX and the user the query BELONGS to are
        # different people, so these are two independent actions on one group.
        associate_text = (
            f"Associate Query to User… (current: {associated})"
            if associated
            else "Associate Query to User…"
        )
        associate_action = menu.addAction(associate_text)
        clear_assoc_action = (
            menu.addAction("Clear user association") if associated else None
        )
        menu.addSeparator()
        email_action = menu.addAction("Email User…")
        chosen = menu.exec(global_pos)
        if chosen is None:
            return
        if chosen is email_action:
            self._email_group_user(group)
            return
        if chosen is assign_action:
            self._assign_group(group)
        elif clear_action is not None and chosen is clear_action:
            self._write_assignment(group, "", "")
        elif chosen is associate_action:
            self._associate_group(group)
        elif clear_assoc_action is not None and chosen is clear_assoc_action:
            self._write_association(group, "", "")

    def _assign_group(self, group: dict) -> None:
        from ..assignments import load_roster_choices

        choices = load_roster_choices(self._db_path) if self._db_path else []
        if not choices:
            QMessageBox.information(
                self,
                "Assign Query Group",
                "The user roster has not been loaded into this warehouse yet. "
                "Open the Data Loader and refresh the User Roster (parsed from "
                "PG_USER), then assign again.",
            )
            return
        dialog = _AssignEngineerDialog(
            _text(group.get("repeat_group_id")),
            choices,
            _text(group.get("assigned_engineer")),
            self,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        user_name, display = dialog.selected()
        if not user_name and not display:
            return
        self._write_assignment(group, user_name, display)

    def _write_assignment(self, group: dict, user_name: str, display: str) -> None:
        from ..assignments import set_assignment

        key = _text(group.get("repeat_group_key"))
        if not key or not self._db_path:
            QMessageBox.warning(
                self,
                "Assign Query Group",
                "This pattern has no durable group key or no warehouse is loaded; "
                "reload the triage analysis and try again.",
            )
            return
        try:
            set_assignment(self._db_path, key, user_name, display)
        except Exception as exc:
            QMessageBox.warning(self, "Assign Query Group", f"Could not save the assignment: {exc}")
            return
        # Update the in-memory frame and the visible row without a full reload.
        if self._groups is not None and not self._groups.empty and "repeat_group_key" in self._groups.columns:
            mask = self._groups["repeat_group_key"].astype(str) == key
            if "assigned_engineer" not in self._groups.columns:
                self._groups["assigned_engineer"] = ""
                self._groups["assigned_user_name"] = ""
            self._groups.loc[mask, "assigned_engineer"] = display
            self._groups.loc[mask, "assigned_user_name"] = user_name
        group_id = _text(group.get("repeat_group_id"))
        for index in range(self._tree.topLevelItemCount()):
            item = self._tree.topLevelItem(index)
            data = item.data(0, _GROUP_ROLE)
            if isinstance(data, dict) and _text(data.get("repeat_group_id")) == group_id:
                item.setText(8, display)
                data["assigned_engineer"] = display
                data["assigned_user_name"] = user_name
                item.setData(0, _GROUP_ROLE, data)
                break

    def _associate_group(self, group: dict) -> None:
        """Record which business user a query pattern belongs to.

        Uses the same roster as engineer assignment - the difference is meaning,
        not source: this is the person whose workload the query IS, not the
        person who owns fixing it.
        """
        from ..assignments import load_roster_choices

        choices = load_roster_choices(self._db_path) if self._db_path else []
        if not choices:
            QMessageBox.information(
                self,
                "Associate Query to User",
                "The user roster has not been loaded into this warehouse yet. "
                "Open the Data Loader and refresh the User Roster (parsed from "
                "SVV_USER_INFO), then associate again.",
            )
            return
        dialog = _AssignEngineerDialog(
            _text(group.get("repeat_group_id")),
            choices,
            _text(group.get("associated_user")),
            self,
            title="Associate Query to User",
            prompt="Business user this query pattern belongs to:",
        )
        if dialog.exec() != QDialog.Accepted:
            return
        user_name, display = dialog.selected()
        if not user_name and not display:
            return
        self._write_association(group, user_name, display)

    def _write_association(self, group: dict, user_name: str, display: str) -> None:
        from ..assignments import set_association

        key = _text(group.get("repeat_group_key"))
        if not key or not self._db_path:
            QMessageBox.warning(
                self,
                "Associate Query to User",
                "This pattern has no durable group key or no warehouse is loaded; "
                "reload the triage analysis and try again.",
            )
            return
        try:
            set_association(self._db_path, key, user_name, display)
        except Exception as exc:
            QMessageBox.warning(
                self, "Associate Query to User", f"Could not save the association: {exc}"
            )
            return
        if (
            self._groups is not None
            and not self._groups.empty
            and "repeat_group_key" in self._groups.columns
        ):
            mask = self._groups["repeat_group_key"].astype(str) == key
            if "associated_user" not in self._groups.columns:
                self._groups["associated_user"] = ""
                self._groups["associated_user_name"] = ""
            self._groups.loc[mask, "associated_user"] = display
            self._groups.loc[mask, "associated_user_name"] = user_name
        group_id = _text(group.get("repeat_group_id"))
        for index in range(self._tree.topLevelItemCount()):
            item = self._tree.topLevelItem(index)
            data = item.data(0, _GROUP_ROLE)
            if isinstance(data, dict) and _text(data.get("repeat_group_id")) == group_id:
                data["associated_user"] = display
                data["associated_user_name"] = user_name
                item.setData(0, _GROUP_ROLE, data)
                break

    def _group_user_candidates(self, group: dict) -> list[str]:
        """Users seen running this pattern, most frequent first.

        Members carry the per-query user; the group's own ``users`` column is
        a pre-joined summary and may read "Multiple Users", so it is only a
        fallback.
        """
        key = _text(group.get("repeat_group_key"))
        group_id = _text(group.get("repeat_group_id"))
        members = self._members
        if members is not None and not members.empty and "user_name" in members.columns:
            mine = members
            if "repeat_group_key" in members.columns and key:
                mine = members[members["repeat_group_key"].astype(str) == key]
            if mine.empty and "repeat_group_id" in members.columns and group_id:
                mine = members[members["repeat_group_id"].astype(str) == group_id]
            if not mine.empty:
                counts = mine["user_name"].dropna().astype(str).str.strip()
                counts = counts[counts != ""]
                if not counts.empty:
                    return list(counts.value_counts().index)
        raw = _text(group.get("users"))
        if raw and raw.lower() != "multiple users":
            return [item.strip() for item in raw.split(",") if item.strip()]
        return []

    def _email_group_user(self, group: dict) -> None:
        """Open a mail client addressed to the user behind this pattern."""
        from ..user_email import (
            build_body,
            build_mailto,
            build_subject,
            resolve_recipient,
        )

        candidates = self._group_user_candidates(group)
        if not candidates:
            QMessageBox.information(
                self,
                "Email User",
                "No user was captured for this query pattern, so there is "
                "nobody to write to.",
            )
            return

        roster = self._load_roster_frame()
        resolved = [
            (name, resolve_recipient(name, roster)[0]) for name in candidates
        ]
        addressable = [(name, email) for name, email in resolved if email]

        if not addressable:
            # The normal case for service accounts, so explain rather than
            # guessing a domain - a wrong address emails a real stranger.
            shown = ", ".join(name for name, _ in resolved[:5])
            QMessageBox.information(
                self,
                "Email User",
                f"This pattern runs as {shown}.\n\n"
                "That is not an email address, and the captured user roster "
                "does not map it to one - it is most likely a service or "
                "application account rather than a person.\n\n"
                "Find out who owns that account, then use "
                "“Associate Query to User…” to record it.",
            )
            return

        name, email = addressable[0]
        sql = (
            _text(group.get("sample_sql"))
            or _text(group.get("representative_sql"))
            or _text(group.get("sql_shape"))
        )
        subject = build_subject(group)
        body = build_body(
            group,
            display_name=name,
            sender=_text(getattr(self, "_sender_name", "")),
            sql=sql,
        )
        url = build_mailto(email, subject, body)

        if not QDesktopServices.openUrl(QUrl(url)):
            QMessageBox.warning(
                self,
                "Email User",
                "Could not open your mail client. The message is on the "
                "clipboard instead.",
            )
            QApplication.clipboard().setText(f"To: {email}\nSubject: {subject}\n\n{body}")

    def _load_roster_frame(self):
        """Captured user roster, or an empty frame when none is loaded."""
        import pandas as _pd

        if not self._db_path:
            return _pd.DataFrame()
        try:
            import duckdb

            con = duckdb.connect(str(self._db_path), read_only=True)
        except Exception:
            return _pd.DataFrame()
        try:
            return con.execute(
                "SELECT user_name, email FROM user_roster"
            ).df()
        except Exception:
            return _pd.DataFrame()
        finally:
            con.close()

    def _export_user_associations(self) -> None:
        """Write the ownership handoff document."""
        from ..association_export import export_markdown

        groups = self._groups
        if groups is None or groups.empty:
            QMessageBox.information(
                self,
                "Export User Associations",
                "Load the triage analysis first - there are no query patterns to export.",
            )
            return

        cluster_names: dict[str, str] = {}
        members = self._members if self._members is not None else pd.DataFrame()
        for frame in (members, groups):
            if frame is None or frame.empty:
                continue
            if "namespace_id" in frame.columns and "cluster_name" in frame.columns:
                for _pos, row in frame[["namespace_id", "cluster_name"]].drop_duplicates().iterrows():
                    namespace = _text(row.get("namespace_id"))
                    name = _text(row.get("cluster_name"))
                    if namespace and name:
                        cluster_names.setdefault(namespace, name)

        markdown = export_markdown(
            groups,
            members,
            cluster_names=cluster_names,
            source=str(self._db_path or ""),
        )
        default = str(Path.home() / "query-ownership.md")
        target, _filter = QFileDialog.getSaveFileName(
            self, "Export User Associations", default, "Markdown (*.md);;All files (*)"
        )
        if not target:
            return
        try:
            Path(target).write_text(markdown, encoding="utf-8")
        except OSError as exc:
            QMessageBox.warning(
                self, "Export User Associations", f"Could not write the file: {exc}"
            )
            return
        QMessageBox.information(
            self, "Export User Associations", f"Written to:\n{target}"
        )

    def _open_group_query_history(self, group_id: str) -> None:
        """Double-click a bubble -> per-query history detail for that pattern,
        with the ability to format the SQL of any selected query."""
        group = self._group_by_id(group_id)
        if group is None:
            return
        members = self._group_member_rows(group)
        dialog = _GroupQueryHistoryDialog(
            group,
            members,
            self,
            table_review=self._table_review,
            view_definitions=self._view_definitions,
        )
        dialog.exec()

    def _set_tiles(self, summary: dict) -> None:
        groups = self._groups
        if groups is None or groups.empty:
            for tile in (
                self._tile_collapse,
                self._tile_patterns,
                self._tile_runs,
                self._tile_runtime,
                self._tile_share,
                self._tile_tables,
            ):
                tile.set_value("-")
            return
        runs = _numeric_column(groups, "query_count").sum()
        runtime = _numeric_column(groups, "total_runtime_s").sum()
        flagged = _numeric_column(groups, "triage_tables_flagged").sum()
        # Prefer the captured slow-query count from the load summary. Fall back
        # to the summed run count, which is the number of queries these
        # patterns actually account for - never invent a denominator.
        captured = int(summary.get("slow_query_count") or 0)
        if captured <= 0:
            captured = int(runs)
        self._tile_collapse.set_value(_collapse_label(captured, len(groups)))
        self._tile_collapse.setToolTip(_collapse_tooltip(captured, len(groups)))
        self._tile_patterns.set_value(f"{len(groups):,}")
        self._tile_runs.set_value(f"{int(runs):,}")
        self._tile_runtime.set_value(_fmt_duration(runtime))
        total_slow = float(summary.get("total_runtime_s") or 0.0)
        if total_slow > 0:
            self._tile_share.set_value(f"{min(runtime / total_slow, 1.0) * 100:.0f}%")
        else:
            self._tile_share.set_value("-")
        self._tile_tables.set_value(f"{int(flagged):,}")

    def _populate_tree(self) -> None:
        # Disable live sorting while filling: otherwise Qt re-sorts on every
        # insert (slow) and would reorder rows before children are attached.
        self._tree.setSortingEnabled(False)
        self._tree.clear()
        groups = self._groups
        if groups is None or groups.empty:
            self._tree.setSortingEnabled(True)
            return
        order_col = "triage_priority_score" if "triage_priority_score" in groups.columns else "total_runtime_s"
        ordered = groups.sort_values(order_col, ascending=False)
        max_priority = max(pd.to_numeric(ordered[order_col], errors="coerce").fillna(0.0).max(), 1.0)
        total_groups = len(ordered)
        if total_groups > _MAX_TREE_GROUPS:
            ordered = ordered.head(_MAX_TREE_GROUPS)
        member_lookup = None
        if self._members is not None and not self._members.empty:
            member_lookup = dict(tuple(self._members.groupby("repeat_group_id", sort=False)))
        bold = QFont()
        bold.setBold(True)
        for _, group in ordered.iterrows():
            verdict = _text(group.get("triage_verdict")) or "MONITOR"
            spectrum = _is_spectrum_group(group)
            display_verdict = _display_verdict(group)
            color = (
                _spectrum_bubble_color()
                if spectrum
                else _verdict_colors().get(verdict, PALETTE.accent)
            )
            users = _text(group.get("users"))
            tables = _text(group.get("shared_tables"))
            pattern_label = _text(group.get("repeat_group_id"))
            if tables:
                pattern_label += "  " + tables.split(",")[0].strip()
            coverage = _text(group.get("triage_stats_coverage"))
            verdict_label = display_verdict if coverage in ("", "complete") else f"{display_verdict} ?"
            runs_value = float(pd.to_numeric(pd.Series([group.get("query_count")]), errors="coerce").fillna(0.0).iloc[0])
            total_runtime_value = float(group.get("total_runtime_s") or 0.0)
            avg_runtime_value = total_runtime_value / max(runs_value, 1.0)
            rows_value = float(pd.to_numeric(pd.Series([group.get("total_input_rows")]), errors="coerce").fillna(0.0).iloc[0])
            item = _SortableTreeItem(
                [
                    "",
                    pattern_label,
                    verdict_label,
                    _fmt_count(group.get("query_count")),
                    _fmt_duration(group.get("total_runtime_s")),
                    _fmt_duration(avg_runtime_value),
                    _fmt_count(group.get("total_input_rows")),
                    users,
                    _text(group.get("assigned_engineer")),
                ]
            )
            item.setFont(1, bold)
            item.setForeground(2, QColor(PALETTE.warn if spectrum else color))
            priority = float(pd.to_numeric(pd.Series([group.get(order_col)]), errors="coerce").fillna(0.0).iloc[0])
            item.setData(0, _IMPACT_ROLE, priority / max_priority)
            item.setData(0, _COLOR_ROLE, color)
            # Numeric sort keys so formatted cells sort by magnitude, not text.
            item.setData(0, _SORT_ROLE, priority)
            item.setData(3, _SORT_ROLE, runs_value)
            item.setData(4, _SORT_ROLE, total_runtime_value)
            item.setData(5, _SORT_ROLE, avg_runtime_value)
            item.setData(6, _SORT_ROLE, rows_value)
            payload = group.to_dict()
            payload["triage_is_spectrum"] = spectrum
            item.setData(0, _GROUP_ROLE, payload)
            item.setToolTip(1, _text(group.get("sql_shape")))
            distinct_sql = int(float(group.get("distinct_sql_count") or 0.0))
            if distinct_sql:
                item.setToolTip(
                    3,
                    f"{int(float(group.get('query_count') or 0.0)):,} captured runs, "
                    f"{distinct_sql:,} distinct SQL statements in the system logs",
                )
            coverage_note = _text(group.get("triage_coverage_note"))
            if spectrum:
                item.setToolTip(2, "Spectrum/external scan detected. Fix query shape or stage locally; Spectrum table DDL is not available here.")
            elif coverage_note:
                item.setToolTip(2, coverage_note)
            self._tree.addTopLevelItem(item)
            self._add_children(item, group, member_lookup)
        if total_groups > _MAX_TREE_GROUPS:
            hidden = total_groups - _MAX_TREE_GROUPS
            note = _SortableTreeItem(
                ["", f"... {hidden:,} lower-priority pattern(s) not shown (top {_MAX_TREE_GROUPS:,} listed)",
                 "", "", "", "", "", "", ""]
            )
            note.setData(0, _SORT_ROLE, -1.0)
            self._tree.addTopLevelItem(note)
        # Re-enable sorting and restore the Impact-descending default. Rows keep
        # the priority order until the user clicks a header to re-sort.
        self._tree.setSortingEnabled(True)
        self._tree.sortByColumn(0, Qt.DescendingOrder)

    def _add_children(self, parent: QTreeWidgetItem, group: pd.Series, member_lookup) -> None:
        group_id = group.get("repeat_group_id")
        if not member_lookup or group_id not in member_lookup:
            return
        # Show the associated query IDs in ascending numeric id order (matches
        # the copied list); sort roles let a header click re-sort by id or time.
        members = member_lookup[group_id].copy()
        members["_qid_sort"] = pd.to_numeric(members.get("query_id"), errors="coerce")
        members = members.sort_values(
            ["_qid_sort", "query_id"], ascending=True, na_position="last"
        )
        shown = 0
        for _, member in members.iterrows():
            if shown >= _MAX_CHILD_ROWS:
                rest = len(members) - shown
                parent.addChild(_SortableTreeItem(["", f"... {rest:,} more run(s)", "", "", "", "", "", ""]))
                break
            query_id_text = _text(member.get("query_id"))
            child = _SortableTreeItem(
                [
                    "",
                    f"query {query_id_text}",
                    _text(member.get("dominant_issue")),
                    "",
                    _fmt_duration(member.get("elapsed_s")),
                    "",
                    _text(member.get("start_time"))[:19],
                    _text(member.get("user_name")),
                ]
            )
            id_rank, id_value = _query_id_sort_key(query_id_text)
            if id_rank == 0:  # numeric query id
                child.setData(1, _SORT_ROLE, id_value)
            elapsed_value = float(pd.to_numeric(pd.Series([member.get("elapsed_s")]), errors="coerce").fillna(0.0).iloc[0])
            child.setData(4, _SORT_ROLE, elapsed_value)
            payload = member.to_dict()
            payload["query_id"] = member.get("query_id")
            payload["_is_member"] = True
            child.setData(0, _GROUP_ROLE, payload)
            parent.addChild(child)
            shown += 1

    # ------------------------------------------------------------- selection

    def _selected_group(self) -> dict | None:
        items = self._tree.selectedItems()
        if not items:
            return None
        data = items[0].data(0, _GROUP_ROLE)
        if isinstance(data, dict) and not data.get("_is_member"):
            return data
        parent = items[0].parent()
        if parent is not None:
            data = parent.data(0, _GROUP_ROLE)
            if isinstance(data, dict):
                return data
        return None

    def _open_representative_lineage(self) -> None:
        from .cluster_dashboard import _open_sql_lineage_dialog

        group = self._selected_group() or {}
        _open_sql_lineage_dialog(
            self._sql_view.toPlainText(),
            pd.Series({
                "query_id": _text(group.get("repeat_group_id")) or "representative SQL",
                "sql_text": self._sql_view.toPlainText(),
            }),
            self._table_review,
            self._view_definitions,
            self,
        )

    def _format_representative_sql(self) -> None:
        from .cluster_dashboard import _apply_format_sql

        _apply_format_sql(self._sql_view, self)

    def _open_representative_subqueries(self) -> None:
        from .cluster_dashboard import _open_sql_subqueries_dialog

        group = self._selected_group() or {}
        _open_sql_subqueries_dialog(
            self._sql_view.toPlainText(),
            pd.Series({
                "query_id": _text(group.get("repeat_group_id")) or "representative SQL",
                "sql_text": self._sql_view.toPlainText(),
            }),
            self._table_review,
            self._view_definitions,
            self,
            source_editor=self._sql_view,
        )

    def _selected_query_ids(self) -> list[str]:
        return _query_ids_for_group(self._selected_group(), self._members)

    def _set_query_ids(self, ids: list[str], group: dict | None = None) -> None:
        self._query_ids_label.setText(_query_id_summary(ids))
        has_ids = bool(ids)
        self._copy_one_btn.setEnabled(has_ids)
        self._copy_all_btn.setEnabled(has_ids)
        self._populate_query_table(group, ids)

    def _group_member_rows(self, group: dict | None) -> pd.DataFrame:
        if not group or self._members is None or self._members.empty:
            return pd.DataFrame()
        if "repeat_group_id" not in self._members.columns:
            return pd.DataFrame()
        group_id = _text(group.get("repeat_group_id"))
        if not group_id:
            return pd.DataFrame()
        return self._members[self._members["repeat_group_id"].astype(str) == group_id].copy()

    def _populate_query_table(self, group: dict | None, ids: list[str]) -> None:
        table = self._query_table
        table.setRowCount(0)
        members = self._group_member_rows(group)
        # Map query_id -> single-line SQL preview from the captured member rows.
        sql_by_id: dict[str, str] = {}
        if not members.empty and "query_id" in members.columns:
            sql_col = "sql_text_full" if "sql_text_full" in members.columns else "sql_text"
            for _, row in members.iterrows():
                qid = _text(row.get("query_id"))
                if qid and qid not in sql_by_id:
                    sql_by_id[qid] = " ".join(str(row.get(sql_col) or "").split())
        # Sort by query ID (numeric when possible), as requested.
        ordered_ids = sorted({i for i in ids if i}, key=_query_id_sort_key)
        table.setRowCount(len(ordered_ids))
        for r, qid in enumerate(ordered_ids):
            id_item = QTableWidgetItem(qid)
            id_item.setData(Qt.UserRole, sql_by_id.get(qid, ""))
            sql_preview = sql_by_id.get(qid, "")
            sql_item = QTableWidgetItem(sql_preview or "(SQL text not captured)")
            if sql_preview:
                sql_item.setToolTip(sql_preview[:2000])
            table.setItem(r, 0, id_item)
            table.setItem(r, 1, sql_item)

    def _on_query_table_double_click(self, index) -> None:
        if index is None or not index.isValid():
            return
        row = index.row()
        id_item = self._query_table.item(row, 0)
        if id_item is None:
            return
        qid = id_item.text()
        sql = id_item.data(Qt.UserRole) or ""
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Query {qid} - full SQL")
        lay = QVBoxLayout(dialog)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(8)
        heading = QLabel(f"Query {qid}")
        heading.setObjectName("Mono")
        hfont = heading.font()
        hfont.setBold(True)
        heading.setFont(hfont)
        lay.addWidget(heading)
        viewer = QPlainTextEdit()
        viewer.setReadOnly(True)
        viewer.setObjectName("Mono")
        viewer.setLineWrapMode(QPlainTextEdit.NoWrap)
        viewer.setPlainText(str(sql) or "SQL text was not captured for this run.")
        lay.addWidget(viewer, 1)
        actions = QHBoxLayout()
        format_btn = QPushButton("Format SQL")
        format_btn.setObjectName("Primary")
        actions.addWidget(format_btn)
        from .cluster_dashboard import _add_sql_structure_buttons, _apply_format_sql
        _add_sql_structure_buttons(
            actions,
            viewer,
            dialog,
            pd.Series({"query_id": qid, "sql_text": sql}),
            self._table_review,
            self._view_definitions,
        )
        actions.addStretch(1)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dialog.close)
        actions.addWidget(close_btn)
        lay.addLayout(actions)
        format_btn.clicked.connect(lambda: _apply_format_sql(viewer, dialog))
        dialog.resize(900, 560)
        dialog.exec()

    def _flash_query_copy_status(self, message: str) -> None:
        self._query_copy_flash_serial += 1
        serial = self._query_copy_flash_serial
        self._query_copy_status.setText(message)
        self._query_copy_status.setStyleSheet(f"color:{PALETTE.crit}; font-weight:700;")

        def restore() -> None:
            if serial != self._query_copy_flash_serial:
                return
            self._query_copy_status.setText("")
            self._query_copy_status.setStyleSheet("")

        QTimer.singleShot(700, restore)

    def _copy_selected_query_id(self) -> None:
        ids = self._selected_query_ids()
        if not ids:
            return
        QApplication.clipboard().setText(ids[0])
        self._flash_query_copy_status("COPIED ONE")

    def _copy_selected_query_ids(self) -> None:
        ids = self._selected_query_ids()
        if not ids:
            return
        QApplication.clipboard().setText(",".join(ids))
        self._flash_query_copy_status("COPIED ALL")

    def _on_selection(self) -> None:
        group = self._selected_group()
        if not group:
            return
        self._chart.set_selected(_text(group.get("repeat_group_id")))
        verdict = _text(group.get("triage_verdict")) or "MONITOR"
        spectrum = _is_spectrum_group(group)
        display_verdict = _display_verdict(group)
        coverage = _text(group.get("triage_stats_coverage"))
        chip_text = f"{display_verdict}  -  {_text(group.get('repeat_group_id'))}"
        if coverage not in ("", "complete"):
            chip_text += "  (TABLE EVIDENCE INCOMPLETE)"
        self._verdict_chip.setText(chip_text)
        self._verdict_chip.setProperty("severity", "warn" if spectrum else _VERDICT_SEVERITY.get(verdict, "info"))
        self._verdict_chip.style().unpolish(self._verdict_chip)
        self._verdict_chip.style().polish(self._verdict_chip)
        ids = _query_ids_for_group(group, self._members)
        self._set_query_ids(ids, group)
        recommendation = _text(group.get("triage_recommendation")) or "-"
        query_evidence = _text(group.get("triage_query_flags")) or "no query-side defects flagged"
        if spectrum:
            spectrum_note = (
                "Spectrum/external/S3 scan detected. Treat this as query or staging work; "
                "local Redshift SORTKEY/DISTKEY changes cannot be applied to Spectrum tables."
            )
            recommendation = f"{spectrum_note}\n\n{recommendation}"
            query_evidence = f"{spectrum_note}\n\n{query_evidence}"
        self._recommendation.setText(
            _verdict_html(group, spectrum, display_verdict) + _render_markdown_card(recommendation)
        )
        self._set_card_markdown(self._query_evidence, query_evidence)
        self._set_card_markdown(self._table_evidence, self._table_evidence_text(group))
        basis = _text(group.get("repeat_match_basis"))
        method = _text(group.get("fingerprint_method"))
        similarity = group.get("min_similarity")
        extra = f"\nFingerprint: {method}." if method else ""
        try:
            if similarity is not None and float(similarity) < 1.0:
                extra += f" Lowest member similarity {float(similarity):.0%}."
        except (TypeError, ValueError):
            pass
        runs_count = int(float(group.get("query_count") or 0.0))
        distinct_sql = int(float(group.get("distinct_sql_count") or 0.0))
        counts_line = f"**{runs_count:,} captured runs**"
        if distinct_sql:
            counts_line += f", **{distinct_sql:,} distinct SQL** statements as logged"
        self._set_card_markdown(self._grouping_basis, counts_line + "\n\n" + (basis or "-") + extra)
        procedure_sql = _text(group.get("procedure_definition"))
        sql = procedure_sql or _text(group.get("sample_sql"))
        self._sql_focus.set_spans([])
        meta_rows: list[dict] = []
        frames = self._group_tables
        if frames is not None and not frames.empty and "repeat_group_id" in frames.columns:
            rows = frames[frames["repeat_group_id"] == group.get("repeat_group_id")]
            meta_rows = rows.to_dict("records")
        self._sql_focus.set_size_context(sql or "", build_table_meta(meta_rows))
        self._sql_view.setPlainText(sql or "")
        if procedure_sql or _text(group.get("repeat_kind")) == "stored_procedure":
            self._sql_focus.set_spans(executable_statement_spans(sql or ""))

    def _table_evidence_text(self, group: dict) -> str:
        parts: list[str] = []
        if _is_spectrum_group(group):
            parts.append(
                "Spectrum/external scan detected. This is not a table-DDL recommendation target; "
                "fix the SQL shape, partition pruning, or materialize/stage the data locally first."
            )
        note = _text(group.get("triage_coverage_note"))
        if note:
            parts.append(f"!! {note}")
        parts.append(self._matched_table_lines(group))
        return "\n\n".join(part for part in parts if part)

    def _matched_table_lines(self, group: dict) -> str:
        group_id = group.get("repeat_group_id")
        frames = self._group_tables
        if frames is None or frames.empty or "repeat_group_id" not in frames.columns:
            flags = _text(group.get("triage_table_flags"))
            if flags:
                return flags
            missing = _text(group.get("triage_missing_tables"))
            if missing:
                return (
                    f"Table metadata unavailable for: {missing}. "
                    "Load SVV_TABLE_INFO for the source database if these are local tables."
                )
            return "no table-design issues flagged"
        rows = frames[frames["repeat_group_id"] == group_id]
        if rows.empty:
            flags = _text(group.get("triage_table_flags"))
            if flags:
                return flags
            missing = _text(group.get("triage_missing_tables"))
            if missing:
                return (
                    f"Table metadata unavailable for: {missing}. "
                    "Load SVV_TABLE_INFO for the source database if these are local tables."
                )
            return "no table-design issues flagged"
        lines: list[str] = []
        for _, row in rows.iterrows():
            name = _text(row.get("table_name"))
            size = float(row.get("size_mb") or 0.0)
            desc = (
                f"{name}  [{_text(row.get('diststyle')) or '-'} | "
                f"sortkey: {_text(row.get('sortkey1')) or 'none'} | {size / 1024:.1f} GB]"
            )
            flags = _text(row.get("table_flags"))
            if flags:
                desc += f"\n    issues: {flags}"
            rec = _text(row.get("table_recommendation"))
            if rec:
                desc += f"\n    fix: {rec}"
            lines.append(desc)
        return "\n".join(lines) if lines else "no table-design issues flagged"

    def _on_double_click(self, item: QTreeWidgetItem, column: int) -> None:
        data = item.data(0, _GROUP_ROLE)
        if isinstance(data, dict) and data.get("_is_member"):
            payload = self._member_payload(data)
            if _text(payload.get("query_id")):
                self.queryDiagramRequested.emit(payload)

    def _on_item_click(self, item: QTreeWidgetItem, column: int) -> None:
        # Single-click only selects. Copying on bare click silently overwrites the
        # user's clipboard (accessibility and trust issue). Copy via Ctrl+C /
        # explicit Copy buttons / the flash path when modifiers are held.
        data = item.data(0, _GROUP_ROLE)
        if not isinstance(data, dict) or not data.get("_is_member"):
            return
        modifiers = QApplication.keyboardModifiers()
        if not (modifiers & (Qt.ControlModifier | Qt.MetaModifier)):
            return
        payload = self._member_payload(data)
        query_id = _text(payload.get("query_id"))
        if not query_id:
            return
        self._copy_query_id_with_flash(item, query_id)

    def _copy_query_id_with_flash(self, item: QTreeWidgetItem, query_id: str) -> None:
        QApplication.clipboard().setText(query_id)
        self._copy_flash_serial += 1
        serial = self._copy_flash_serial
        item.setData(1, _COPY_FLASH_ROLE, serial)
        original_text = f"query {query_id}"
        original_font = item.font(1)
        original_brush = item.foreground(1)
        flash_font = QFont(original_font)
        flash_font.setBold(True)
        item.setText(1, f"COPIED {original_text}")
        item.setFont(1, flash_font)
        item.setForeground(1, QColor(PALETTE.crit))

        def restore() -> None:
            if item.data(1, _COPY_FLASH_ROLE) != serial:
                return
            item.setText(1, original_text)
            item.setFont(1, original_font)
            item.setForeground(1, original_brush)
            item.setData(1, _COPY_FLASH_ROLE, None)

        QTimer.singleShot(500, restore)

    def _member_payload(self, data: dict) -> dict:
        query_id = _text(data.get("query_id"))
        payload = dict(data)
        if query_id and self._members is not None and not self._members.empty:
            try:
                rows = self._members[self._members["query_id"].astype(str).str.strip() == query_id]
            except Exception:
                rows = pd.DataFrame()
            if not rows.empty:
                row_payload = rows.iloc[0].to_dict()
                if _text(row_payload.get("sql_text_full")) or not _text(payload.get("sql_text")):
                    payload.update(row_payload)
        full_sql = _text(payload.get("sql_text_full"))
        if full_sql:
            payload["sql_text"] = full_sql
        payload["_is_member"] = True
        return payload

    def _open_fix_script(self) -> None:
        from ..fix_script import build_fix_script
        from ..structural_recommendations import (
            build_structural_recommendation_script,
            build_structural_recommendations,
        )

        if self._slow_queries is not None and not self._slow_queries.empty and self._table_review is not None and not self._table_review.empty:
            recs = build_structural_recommendations(self._slow_queries, self._table_review)
            script = build_structural_recommendation_script(recs, snapshot_id=self._snapshot_id)
            dialog = _FixScriptDialog(
                script,
                self,
                title="Structural Recommendations - review before running",
                note=(
                    "This output is structural only: SORTKEY and DISTKEY candidates ranked by workload effect. "
                    "Every ALTER statement is commented out on purpose; verify each candidate column and maintenance window before running."
                ),
                default_filename="structural_recommendations.sql",
            )
            dialog.exec()
            return

        script = build_fix_script(
            self._groups,
            self._group_tables,
            action_queue=self._action_queue,
            table_review=self._table_review,
            snapshot_id=self._snapshot_id,
        )
        dialog = _FixScriptDialog(script, self)
        dialog.exec()


class _GroupQueryHistoryDialog(QDialog):
    """Per-pattern query-history detail: the individual captured runs behind a
    quadrant bubble, each with its query id, timing, and SQL - plus a Format SQL
    button for the selected query, plus reversible recursive view expansion."""

    def __init__(
        self,
        group: dict,
        members: pd.DataFrame,
        parent=None,
        *,
        table_review: pd.DataFrame | None = None,
        view_definitions: pd.DataFrame | None = None,
    ):
        super().__init__(parent)
        self._table_review = table_review if table_review is not None else pd.DataFrame()
        self._view_definitions = view_definitions if view_definitions is not None else pd.DataFrame()
        self._original_sql: str | None = None
        gid = _text(group.get("repeat_group_id"))
        self.setWindowTitle(f"Query History - pattern {gid}")
        self.resize(1040, 640)
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        runs = int(float(group.get("query_count") or 0.0)) if group.get("query_count") is not None else len(members)
        assigned = str(group.get("assigned_engineer") or "").strip()
        header = QLabel(
            f"Pattern {gid} - {runs:,} captured run(s). "
            + (f"Assigned to {assigned}. " if assigned else "")
            + "Select a query to see its SQL; use Format SQL to pretty-print it."
        )
        header.setObjectName("Caption")
        header.setWordWrap(True)
        root.addWidget(header)

        split = QSplitter(Qt.Horizontal)
        split.setChildrenCollapsible(False)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["Query ID", "Start Time", "Elapsed", "User"])
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._table.setAlternatingRowColors(True)
        self._table.setSortingEnabled(True)
        thead = self._table.horizontalHeader()
        thead.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        thead.setSectionResizeMode(3, QHeaderView.Stretch)
        self._table.itemSelectionChanged.connect(self._on_select)
        split.addWidget(self._table)

        sql_box = QWidget()
        sql_lay = QVBoxLayout(sql_box)
        sql_lay.setContentsMargins(0, 0, 0, 0)
        sql_lay.setSpacing(6)
        btn_row = QHBoxLayout()
        btn_row.addWidget(QLabel("SQL"))
        btn_row.addStretch(1)
        self._format_btn = QPushButton("Format SQL")
        self._format_btn.setObjectName("Ghost")
        self._format_btn.clicked.connect(self._format_sql)
        self._explode_btn = QPushButton("Explode Views")
        self._explode_btn.setObjectName("Ghost")
        self._explode_btn.setToolTip(
            "Inline every captured view recursively. Expanded view SQL is highlighted yellow; "
            "nested views use progressively deeper amber shades."
        )
        self._explode_btn.clicked.connect(self._toggle_view_explosion)
        self._vitals_btn = QPushButton("Show Vitals")
        self._vitals_btn.setObjectName("Ghost")
        self._vitals_btn.setToolTip(
            "Review every join side and every predicate against captured distribution and sort-key metadata."
        )
        self._vitals_btn.clicked.connect(self._show_vitals)
        lineage_btn = QPushButton("Show Lineage")
        lineage_btn.setObjectName("Ghost")
        lineage_btn.clicked.connect(self._show_lineage)
        subqueries_btn = QPushButton("Extract Subqueries")
        subqueries_btn.setObjectName("Ghost")
        subqueries_btn.clicked.connect(self._identify_subqueries)
        copy_btn = QPushButton("Copy SQL")
        copy_btn.setObjectName("Ghost")
        copy_btn.clicked.connect(self._copy_sql)
        btn_row.addWidget(copy_btn)
        btn_row.addWidget(self._format_btn)
        btn_row.addWidget(self._explode_btn)
        btn_row.addWidget(self._vitals_btn)
        btn_row.addWidget(lineage_btn)
        btn_row.addWidget(subqueries_btn)
        sql_lay.addLayout(btn_row)
        self._sql_view = QPlainTextEdit()
        self._sql_view.setObjectName("Mono")
        self._sql_view.setReadOnly(True)
        self._sql_view.setLineWrapMode(QPlainTextEdit.NoWrap)
        mono = QFont("Consolas")
        mono.setStyleHint(QFont.Monospace)
        self._sql_view.setFont(mono)
        sql_lay.addWidget(self._sql_view, 1)
        split.addWidget(sql_box)
        split.setSizes([420, 620])
        root.addWidget(split, 1)

        buttons = QPushButton("Close")
        buttons.clicked.connect(self.accept)
        foot = QHBoxLayout()
        foot.addStretch(1)
        foot.addWidget(buttons)
        root.addLayout(foot)

        self._sql_by_id: dict[str, str] = {}
        self._populate(members)

    def _populate(self, members: pd.DataFrame) -> None:
        if members is None or members.empty:
            self._sql_view.setPlainText("No captured member rows for this pattern.")
            return
        sql_col = "sql_text_full" if "sql_text_full" in members.columns else "sql_text"
        rows = members
        if "elapsed_s" in rows.columns:
            rows = rows.sort_values("elapsed_s", ascending=False)
        self._table.setRowCount(0)
        self._table.setSortingEnabled(False)
        for _, m in rows.iterrows():
            qid = _text(m.get("query_id"))
            sql = str(m.get(sql_col) or "")
            if qid and qid not in self._sql_by_id:
                self._sql_by_id[qid] = sql
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setItem(r, 0, QTableWidgetItem(qid))
            self._table.setItem(r, 1, QTableWidgetItem(_text(m.get("start_time"))[:19]))
            self._table.setItem(r, 2, QTableWidgetItem(_fmt_duration(m.get("elapsed_s"))))
            self._table.setItem(r, 3, QTableWidgetItem(_text(m.get("user_name"))))
        self._table.setSortingEnabled(True)
        if self._table.rowCount():
            self._table.selectRow(0)

    def _selected_qid(self) -> str:
        items = self._table.selectedItems()
        if not items:
            return ""
        return self._table.item(items[0].row(), 0).text()

    def _on_select(self) -> None:
        self._original_sql = None
        self._explode_btn.setText("Explode Views")
        self._format_btn.setEnabled(True)
        self._sql_view.setExtraSelections([])
        qid = self._selected_qid()
        self._sql_view.setPlainText(self._sql_by_id.get(qid, "") or "SQL text was not captured for this run.")

    def _current_sql(self) -> str:
        return self._sql_view.toPlainText()

    def _format_sql(self) -> None:
        from .cluster_dashboard import _format_sql_text

        sql = self._current_sql().strip()
        if not sql or sql.startswith("SQL text was not captured"):
            return
        formatted = _format_sql_text(sql)
        if formatted:
            self._sql_view.setPlainText(formatted)

    def _toggle_view_explosion(self) -> None:
        """Inline captured views for the selected run, or restore it exactly."""
        if self._original_sql is not None:
            original = self._original_sql
            self._original_sql = None
            self._sql_view.setExtraSelections([])
            self._sql_view.setPlainText(original)
            self._explode_btn.setText("Explode Views")
            self._format_btn.setEnabled(True)
            return

        sql = self._current_sql()
        if not sql.strip() or sql.startswith("SQL text was not captured"):
            QMessageBox.information(self, "Explode Views", "No SQL text is available for this query.")
            return

        from ..sql_xray import build_view_map, explode_views_recursive_with_spans

        rows = (
            self._view_definitions.to_dict("records")
            if isinstance(self._view_definitions, pd.DataFrame)
            else self._view_definitions
        )
        view_map = build_view_map(rows)
        if not view_map:
            QMessageBox.information(
                self,
                "Explode Views",
                "No view definitions are loaded for this snapshot.",
            )
            return
        expanded, exploded, spans = explode_views_recursive_with_spans(sql, view_map)
        if not exploded or expanded == sql:
            QMessageBox.information(
                self,
                "Explode Views",
                "No captured views were found in this query's FROM or JOIN clauses.",
            )
            return

        self._original_sql = sql
        self._sql_view.setPlainText(expanded)
        self._apply_view_explosion_highlights(spans)
        self._explode_btn.setText("Unexplode Views")
        # Formatting changes character offsets, so preserve the requested color
        # ranges until the user restores the original SQL.
        self._format_btn.setEnabled(False)

    def _apply_view_explosion_highlights(self, spans: list[dict]) -> None:
        colors = ("#FFF59D", "#FFD180", "#FFB74D", "#FFA726", "#FB8C00", "#EF6C00")
        selections: list[QTextEdit.ExtraSelection] = []
        text_length = len(self._sql_view.toPlainText())
        for span in sorted(spans or [], key=lambda item: (int(item.get("depth", 0)), int(item.get("start", 0)))):
            start = max(0, min(text_length, int(span.get("start", 0))))
            end = max(start, min(text_length, int(span.get("end", start))))
            if end <= start:
                continue
            depth = max(0, int(span.get("depth", 0)))
            cursor = QTextCursor(self._sql_view.document())
            cursor.setPosition(start)
            cursor.setPosition(end, QTextCursor.KeepAnchor)
            char_format = QTextCharFormat()
            char_format.setBackground(QColor(colors[min(depth, len(colors) - 1)]))
            char_format.setForeground(QColor("#2B2118"))
            selection = QTextEdit.ExtraSelection()
            selection.cursor = cursor
            selection.format = char_format
            selections.append(selection)
        self._sql_view.setExtraSelections(selections)

    def _copy_sql(self) -> None:
        sql = self._current_sql().strip()
        if sql and not sql.startswith("SQL text was not captured"):
            QApplication.clipboard().setText(sql)

    def _source_row(self) -> pd.Series:
        return pd.Series({"query_id": self._selected_qid() or "pattern query", "sql_text": self._current_sql()})

    def _show_lineage(self) -> None:
        from .cluster_dashboard import _open_sql_lineage_dialog

        _open_sql_lineage_dialog(
            self._current_sql(), self._source_row(), self._table_review, self._view_definitions, self
        )

    def _show_vitals(self) -> None:
        from ..sql_lens import analyze_console_sql

        sql = self._current_sql().strip()
        if not sql or sql.startswith("SQL text was not captured"):
            QMessageBox.information(self, "Show Vitals", "No SQL text is available for this query.")
            return
        analysis = analyze_console_sql(sql, self._table_review, pd.DataFrame(), self._view_definitions)
        if not analysis.parse_ok:
            QMessageBox.warning(
                self,
                "Show Vitals",
                f"The query could not be parsed for join and predicate analysis.\n\n{analysis.parse_error}",
            )
            return
        dialog = _QueryVitalsDialog(self._selected_qid(), analysis, self)
        dialog.exec()

    def _identify_subqueries(self) -> None:
        from .cluster_dashboard import _open_sql_subqueries_dialog

        _open_sql_subqueries_dialog(
            self._current_sql(),
            self._source_row(),
            self._table_review,
            self._view_definitions,
            self,
            source_editor=self._sql_view,
        )


def _vital_assessment(row: pd.Series) -> tuple[str, str]:
    visual = str(row.get("visual_status") or "").lower()
    severity = str(row.get("severity") or "").lower()
    if visual == "green" and severity == "ok":
        return "OPTIMAL", PALETTE.ok
    if visual == "red" or severity == "crit":
        return "POOR", PALETTE.crit
    return "CANNOT DETERMINE", PALETTE.warn


def _join_vital_sides(row: pd.Series) -> tuple[str, str]:
    pairs = [part.strip() for part in str(row.get("column_pairs") or "").split(";") if part.strip()]
    left: list[str] = []
    right: list[str] = []
    for pair in pairs:
        if "=" not in pair:
            continue
        lhs, rhs = pair.split("=", 1)
        left.append(lhs.strip())
        right.append(rhs.strip())
    if left or right:
        return "\n".join(left) or "-", "\n".join(right) or "-"
    involved = [part.strip() for part in str(row.get("involved_tables") or "").split(",") if part.strip()]
    target = str(row.get("target_table") or "").strip()
    left_side = ", ".join(part for part in involved if part.lower() != target.lower())
    return left_side or "Not resolved", target or "Not resolved"


class _QueryVitalsDialog(QDialog):
    """Evidence-focused join and predicate health for one selected query."""

    def __init__(self, query_id: str, analysis, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Query Vitals - {query_id or 'selected query'}")
        self.resize(1180, 720)
        root = QVBoxLayout(self)
        heading = QLabel(f"QUERY VITALS  |  Query {query_id or '-'}")
        heading.setObjectName("SectionHeader")
        root.addWidget(heading)
        note = QLabel(
            "Optimal and Poor are reported only when the SQL plus captured table metadata support the conclusion. "
            "Cannot Determine means the Redshift plan or table metadata must be checked before making a claim."
        )
        note.setObjectName("Caption")
        note.setWordWrap(True)
        root.addWidget(note)

        tabs = QTabWidget()
        self._joins = self._build_join_table(analysis.joins)
        self._predicates = self._build_predicate_table(analysis.predicates, analysis.tables)
        tabs.addTab(self._joins, f"Joins ({len(analysis.joins):,})")
        tabs.addTab(self._predicates, f"Predicates ({len(analysis.predicates):,})")
        root.addWidget(tabs, 1)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        foot = QHBoxLayout()
        foot.addStretch(1)
        foot.addWidget(close_btn)
        root.addLayout(foot)

    @staticmethod
    def _base_table(headers: list[str]) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setAlternatingRowColors(True)
        table.setWordWrap(True)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        return table

    def _build_join_table(self, joins: pd.DataFrame) -> QTableWidget:
        table = self._base_table(["#", "Left Side", "Right Side", "Join Type", "Assessment", "Commentary"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        if joins is None or joins.empty:
            return table
        for _, row in joins.iterrows():
            left, right = _join_vital_sides(row)
            assessment, color = _vital_assessment(row)
            commentary = " — ".join(
                part
                for part in (
                    str(row.get("distribution_alignment") or "").strip(),
                    str(row.get("recommendation") or "").strip(),
                )
                if part
            )
            values = [row.get("join_no"), left, right, row.get("join_type") or "JOIN", assessment, commentary]
            out_row = table.rowCount()
            table.insertRow(out_row)
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value or "-"))
                if column == 4:
                    item.setForeground(QColor(color))
                    item.setFont(QFont(item.font().family(), item.font().pointSize(), QFont.Bold))
                table.setItem(out_row, column, item)
        table.resizeRowsToContents()
        return table

    def _build_predicate_table(self, predicates: pd.DataFrame, tables: pd.DataFrame) -> QTableWidget:
        table = self._base_table(
            ["#", "Table / Alias", "Predicate", "Columns", "Captured Sort Key", "Intelligence", "Recommendation"]
        )
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        if predicates is None or predicates.empty:
            return table
        alias_meta: dict[str, pd.Series] = {}
        if tables is not None and not tables.empty and "alias" in tables.columns:
            alias_meta = {
                str(row.get("alias") or "").strip().lower(): row
                for _, row in tables.iterrows()
                if str(row.get("alias") or "").strip()
            }
        for _, row in predicates.iterrows():
            aliases = [part.strip().lower() for part in str(row.get("aliases") or "").split(",") if part.strip()]
            sortkeys: list[str] = []
            for alias in aliases:
                meta = alias_meta.get(alias)
                key = str(meta.get("sortkey1") or "").strip() if meta is not None else ""
                sortkeys.append(f"{alias}: {key or 'not captured / not set'}")
            assessment, color = _vital_assessment(row)
            intelligence = f"{assessment}: {row.get('sortkey_alignment') or 'No alignment evidence'}"
            values = [
                row.get("predicate_no"),
                row.get("involved_tables") or row.get("aliases") or "Not resolved",
                row.get("condition") or "-",
                row.get("columns") or "-",
                "\n".join(sortkeys) or "Not captured / not set",
                intelligence,
                row.get("recommendation") or "-",
            ]
            out_row = table.rowCount()
            table.insertRow(out_row)
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value or "-"))
                if column == 5:
                    item.setForeground(QColor(color))
                    item.setFont(QFont(item.font().family(), item.font().pointSize(), QFont.Bold))
                table.setItem(out_row, column, item)
        table.resizeRowsToContents()
        return table


class _FixScriptDialog(QDialog):
    """Review, edit, copy, or save the generated fix script."""

    def __init__(
        self,
        script: str,
        parent=None,
        *,
        title: str = "Fix Script - review before running",
        note: str | None = None,
        default_filename: str = "fix_script.sql",
    ):
        super().__init__(parent)
        self.setWindowTitle(title)
        self._default_filename = default_filename
        self.resize(900, 640)
        lay = QVBoxLayout(self)
        note = QLabel(
            note
            or "Section 1 (ANALYZE / VACUUM) is runnable. Section 2 design changes are commented out "
            "on purpose - verify each candidate column, then uncomment what you approve."
        )
        note.setObjectName("Caption")
        note.setWordWrap(True)
        lay.addWidget(note)
        self._editor = QPlainTextEdit()
        self._editor.setPlainText(script)
        mono = QFont("Consolas")
        mono.setStyleHint(QFont.Monospace)
        self._editor.setFont(mono)
        lay.addWidget(self._editor, 1)
        buttons = QHBoxLayout()
        buttons.addStretch(1)
        copy_btn = QPushButton("Copy to Clipboard")
        copy_btn.clicked.connect(self._copy)
        save_btn = QPushButton("Save As ...")
        save_btn.setObjectName("Primary")
        save_btn.clicked.connect(self._save)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        buttons.addWidget(copy_btn)
        buttons.addWidget(save_btn)
        buttons.addWidget(close_btn)
        lay.addLayout(buttons)

    def _copy(self) -> None:
        QApplication.clipboard().setText(self._editor.toPlainText())

    def _save(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Save fix script", self._default_filename, "SQL files (*.sql)")
        if path:
            Path(path).write_text(self._editor.toPlainText(), encoding="utf-8")
