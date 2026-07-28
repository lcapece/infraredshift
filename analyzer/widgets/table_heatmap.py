"""Top-level graphical table-health heat map."""
from __future__ import annotations

import math
from pathlib import Path
import re

import pandas as pd
from PySide6.QtCore import QEvent, QRect, QSize, Qt, QTimer, Signal
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSlider,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from ..theme import PALETTE


# External (Spectrum) partition colours. Deliberately NOT the red/amber health
# ramp used by the physical heat map: an unpartitioned external table is a
# design fact to notice, not a severity score, and reusing the health colours
# read as "this table is failing".
_EXTERNAL_PARTITIONED = "#2E86DE"
_EXTERNAL_UNPARTITIONED = "#F5822A"


METRICS = (
    ("combined_health", "Composite View — All Attributes"),
    ("distribution", "Distribution Only"),
    ("sort_key", "Sort Only"),
)


def _number(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
        return default if math.isnan(number) else number
    except (TypeError, ValueError):
        return default


def _missing_sort_key(value: object) -> bool:
    text = "".join(str(value or "").upper().split())
    return text in {
        "", "-", "NONE", "NULL", "NAN", "AUTO", "AUTO(SORTKEY)", "SORTKEY", "(SORTKEY)"
    }


def _distribution_key(row: pd.Series) -> str:
    explicit = str(row.get("distkey") or "").strip()
    if explicit and explicit.upper() not in {"NONE", "NULL", "NAN", "AUTO", "-"}:
        return explicit
    style = "".join(str(row.get("diststyle") or "").upper().split())
    match = re.search(r"KEY\(([^)]+)\)", style)
    if not match:
        return ""
    candidate = match.group(1).strip().strip('"')
    return "" if candidate in {"", "AUTO", "NONE", "NULL", "NAN"} else candidate


def _distribution_health(row: pd.Series) -> int:
    """0 green, 1 yellow, 2 red."""
    if not _distribution_key(row):
        return 2
    return 0 if _number(row.get("skew_rows"), float("inf")) <= 2.0 else 1


def _sort_health(row: pd.Series) -> int:
    """0 green, 1 yellow, 2 red."""
    if _missing_sort_key(row.get("sortkey1")):
        return 2
    sorted_pct = _number(row.get("sorted_pct"), -1.0)
    if sorted_pct > 90.0:
        return 0
    if sorted_pct >= 50.0:
        return 1
    return 2


def _health_color(severity: int) -> QColor:
    return QColor("#2EAD68" if severity == 0 else "#F5A623" if severity == 1 else "#D94B4B")


def _health_label(severity: int) -> str:
    return "Healthy" if severity == 0 else "Review" if severity == 1 else "Problem"


def _statistics_fresh_pct(row: pd.Series) -> float:
    for column in ("statistics_pct", "stats_pct", "statistics_percent"):
        if column in row.index:
            value = _number(row.get(column), float("nan"))
            if not math.isnan(value):
                return max(0.0, min(100.0, value))
    stats_off = _number(row.get("stats_off"), float("nan"))
    if math.isnan(stats_off):
        return float("nan")
    return max(0.0, min(100.0, 100.0 - stats_off))


def _statistics_alert(row: pd.Series) -> bool:
    freshness = _statistics_fresh_pct(row)
    return not math.isnan(freshness) and freshness <= 60.0


def _tile_healths(row: pd.Series, mode: str) -> tuple[int, int]:
    if mode == "distribution":
        health = _distribution_health(row)
        return health, health
    if mode == "sort_key":
        health = _sort_health(row)
        return health, health
    return _distribution_health(row), _sort_health(row)


def _metric_severity(row: pd.Series, metric: str) -> int:
    if metric == "combined_health":
        return max(_distribution_health(row), _sort_health(row))
    if metric == "sort_key":
        return _sort_health(row)
    if metric == "sorted_pct":
        return _sort_health(row)
    if metric == "distribution":
        return _distribution_health(row)
    if metric == "row_skew":
        value = _number(row.get("skew_rows"), 1.0)
        return 2 if value > 2.0 else 1 if value > 1.2 else 0
    value = _number(row.get("stats_off"), 0.0)
    return 2 if value > 20 else 1 if value > 5 else 0


def _blend(left: QColor, right: QColor, ratio: float) -> QColor:
    ratio = max(0.0, min(1.0, ratio))
    return QColor(
        round(left.red() + (right.red() - left.red()) * ratio),
        round(left.green() + (right.green() - left.green()) * ratio),
        round(left.blue() + (right.blue() - left.blue()) * ratio),
    )


def _metric_color(row: pd.Series, metric: str) -> QColor:
    green, amber, red, gray = QColor("#2EAD68"), QColor("#F5A623"), QColor("#D94B4B"), QColor("#7A8494")
    if metric == "sorted_pct":
        value = _number(row.get("sorted_pct"), -1)
        if value < 0:
            return gray
        return _blend(red, amber, value / 80.0) if value < 80 else _blend(amber, green, (value - 80) / 20.0)
    if metric == "row_skew":
        value = _number(row.get("skew_rows"), -1)
        if value < 0:
            return gray
        return _blend(green, amber, (value - 1.0) / 1.0) if value <= 2 else _blend(amber, red, (value - 2.0) / 6.0)
    if metric == "stats_stale":
        value = _number(row.get("stats_off"), -1)
        if value < 0:
            return gray
        return _blend(green, amber, value / 20.0) if value <= 20 else _blend(amber, red, (value - 20) / 80.0)
    if metric == "distribution":
        value = str(row.get("diststyle") or "").upper()
        if "KEY" in value:
            return green
        if "ALL" in value:
            return QColor("#4A90E2")
        if "AUTO" in value:
            return amber
        if "EVEN" in value:
            return red
        return gray
    severity = _metric_severity(row, metric)
    return green if severity == 0 else amber if severity == 1 else red


def _legend_text(metric: str) -> str:
    return (
        "TOP — DISTRIBUTION: Green DISTKEY + skew ≤2.0  •  Yellow DISTKEY + skew >2.0  •  Red no concrete DISTKEY"
        "     BOTTOM — SORT: Green SORTKEY + >90% sorted  •  Yellow SORTKEY + 50–90% sorted"
        "  •  Red no SORTKEY or <50% sorted"
    )


def _filter_heatmap_rows(
    frame: pd.DataFrame,
    *,
    metric: str,
    min_size_mb: float,
    min_rows: float,
    problems_only: bool,
    cluster: str = "",
    database: str = "",
    schema: str = "",
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if cluster:
        cluster_values = _cluster_values(out)
        out = out[cluster_values.str.casefold() == cluster.casefold()]
    if database and not out.empty:
        database_values = _database_values(out)
        out = out[database_values.str.casefold() == database.casefold()]
    if schema and not out.empty:
        schema_values = _schema_values(out)
        out = out[schema_values.str.casefold() == schema.casefold()]
    for column in ("size_mb", "tbl_rows", "sorted_pct", "skew_rows", "stats_off"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out[out.get("size_mb", pd.Series(0, index=out.index)).fillna(0) >= min_size_mb]
    out = out[out.get("tbl_rows", pd.Series(0, index=out.index)).fillna(0) >= min_rows]
    if problems_only and not out.empty:
        out = out[out.apply(lambda row: _metric_severity(row, metric) > 0, axis=1)]
    return out.sort_values(["size_mb", "tbl_rows"], ascending=[False, False], na_position="last").reset_index(drop=True)


def _text_values(frame: pd.DataFrame, primary: str, fallback: str = "") -> pd.Series:
    values = pd.Series("", index=frame.index, dtype="object")
    if primary in frame.columns:
        values = frame[primary].fillna("").astype(str).str.strip()
    if fallback and fallback in frame.columns:
        fallback_values = frame[fallback].fillna("").astype(str).str.strip()
        values = values.where(values.ne(""), fallback_values)
    return values


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "<na>"} else text


def _cluster_values(frame: pd.DataFrame) -> pd.Series:
    values = _text_values(frame, "namespace_id", "cluster_name")
    return values.where(values.ne(""), "producer")


def _database_values(frame: pd.DataFrame) -> pd.Series:
    values = _text_values(frame, "source_db")
    for fallback in ("redshift_database_name", "database_name"):
        if fallback in frame.columns:
            fallback_values = frame[fallback].fillna("").astype(str).str.strip()
            values = values.where(values.ne(""), fallback_values)
    return values


def _schema_values(frame: pd.DataFrame) -> pd.Series:
    return _text_values(frame, "schema_name")


def _scope_rows(
    frame: pd.DataFrame,
    *,
    cluster: str = "",
    database: str = "",
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if cluster:
        out = out[_cluster_values(out).str.casefold() == cluster.casefold()]
    if database and not out.empty:
        out = out[_database_values(out).str.casefold() == database.casefold()]
    return out


def _aggregate_external_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    """Collapse Producer SVV_EXTERNAL_COLUMNS rows into one heat-map tile per table."""
    if frame is None or frame.empty:
        return pd.DataFrame()
    work = frame.copy()
    work["_namespace"] = _cluster_values(work)
    work["_database"] = _text_values(
        work, "redshift_database_name", "source_db"
    )
    if "database_name" in work.columns:
        fallback = work["database_name"].fillna("").astype(str).str.strip()
        work["_database"] = work["_database"].where(
            work["_database"].ne(""), fallback
        )
    work["_schema"] = _text_values(work, "schema_name")
    work["_table"] = _text_values(work, "table_name")
    work = work[
        work["_database"].ne("")
        & work["_schema"].ne("")
        & work["_table"].ne("")
    ].copy()
    if work.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    identity = ["_namespace", "_database", "_schema", "_table"]
    for (namespace, database, schema, table), columns in work.groupby(
        identity, dropna=False, sort=False
    ):
        ordered = columns.copy()
        ordered["_column_number"] = pd.to_numeric(
            ordered.get("column_number", pd.Series(0, index=ordered.index)),
            errors="coerce",
        ).fillna(0)
        ordered["_partition_ordinal"] = pd.to_numeric(
            ordered.get(
                "partition_key_ordinal",
                pd.Series(0, index=ordered.index),
            ),
            errors="coerce",
        ).fillna(0)
        ordered = ordered.sort_values(
            ["_column_number", "_partition_ordinal"],
            kind="stable",
        )

        column_rows: list[tuple[str, str, int]] = []
        for _, column in ordered.iterrows():
            name = _clean_text(column.get("column_name"))
            data_type = _clean_text(column.get("data_type")) or "unknown"
            ordinal = int(_number(column.get("_partition_ordinal"), 0))
            if name:
                column_rows.append((name, data_type, ordinal))

        partition_columns = sorted(
            (
                (ordinal, name)
                for name, _data_type, ordinal in column_rows
                if ordinal > 0
            ),
            key=lambda item: (item[0], item[1].casefold()),
        )
        if not partition_columns:
            legacy_sortkey = next(
                (
                    _clean_text(value)
                    for value in ordered.get(
                        "sortkey", pd.Series(dtype="object")
                    ).tolist()
                    if _clean_text(value)
                ),
                "",
            )
            if legacy_sortkey:
                partition_columns = [
                    (index + 1, name.strip())
                    for index, name in enumerate(legacy_sortkey.split(","))
                    if name.strip()
                ]

        cluster_name = next(
            (
                _clean_text(value)
                for value in ordered.get(
                    "cluster_name", pd.Series(dtype="object")
                ).tolist()
                if _clean_text(value)
            ),
            _clean_text(namespace) or "Producer",
        )
        external_key = next(
            (
                _clean_text(value)
                for value in ordered.get(
                    "external_table_key", pd.Series(dtype="object")
                ).tolist()
                if _clean_text(value)
            ),
            f"{database}.{schema}.{table}".casefold(),
        )
        nullable_count = 0
        if "is_nullable" in ordered.columns:
            nullable_count = int(
                ordered["is_nullable"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.casefold()
                .isin({"true", "t", "yes", "y", "1"})
                .sum()
            )
        definitions = [
            f"{name}: {data_type}"
            + (f" [partition {ordinal}]" if ordinal > 0 else "")
            for name, data_type, ordinal in column_rows
        ]
        preview_limit = 12
        column_preview = "; ".join(definitions[:preview_limit])
        if len(definitions) > preview_limit:
            column_preview += f"; … +{len(definitions) - preview_limit} more"
        distinct_types = sorted(
            {data_type for _name, data_type, _ordinal in column_rows},
            key=str.casefold,
        )
        partition_names = [name for _ordinal, name in partition_columns]
        partition_details = ", ".join(
            f"{name} ({ordinal})" for ordinal, name in partition_columns
        )
        rows.append(
            {
                "namespace_id": _clean_text(namespace) or "producer",
                "cluster_name": cluster_name,
                "source_db": _clean_text(database),
                "redshift_database_name": _clean_text(database),
                "schema_name": _clean_text(schema),
                "table_name": _clean_text(table),
                "external_table_key": external_key,
                "column_count": len(column_rows) or len(ordered),
                "nullable_column_count": nullable_count,
                "partition_present": bool(partition_columns),
                "partition_key_count": len(partition_columns),
                "partition_key_columns": ", ".join(partition_names),
                "partition_key_details": partition_details,
                # Redshift exposes a Spectrum partition key in the same
                # ordinal role the UI describes as the external sort key.
                "sortkey": ", ".join(partition_names),
                "data_types": ", ".join(distinct_types),
                "column_preview": column_preview or "No column details captured",
                "metadata_source": "Producer SVV_EXTERNAL_COLUMNS",
                # Reuse the canvas' existing ordering/layout columns.
                "size_mb": 0.0,
                "tbl_rows": len(column_rows) or len(ordered),
            }
        )
    return pd.DataFrame(rows).sort_values(
        [
            "partition_present",
            "redshift_database_name",
            "schema_name",
            "table_name",
        ],
        ascending=[True, True, True, True],
        kind="stable",
    ).reset_index(drop=True)


def _filter_external_heatmap_rows(
    frame: pd.DataFrame,
    *,
    cluster: str = "",
    database: str = "",
    schema: str = "",
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if cluster:
        out = out[
            _cluster_values(out).str.casefold() == cluster.casefold()
        ]
    if database and not out.empty:
        out = out[
            _database_values(out).str.casefold() == database.casefold()
        ]
    if schema and not out.empty:
        out = out[
            _schema_values(out).str.casefold() == schema.casefold()
        ]
    return out.reset_index(drop=True)


class _HeatMapCanvas(QWidget):
    SQUARE = 24
    GAP = 3
    LABEL_WIDTH = 190

    def __init__(self, parent=None):
        super().__init__(parent)
        self._frame = pd.DataFrame()
        self._metric = "sort_key"
        self._width_hint = 900
        self._items: list[tuple[QRect, pd.Series]] = []
        self._bands: dict[int, list[tuple[QRect, pd.Series]]] = {}
        self._database_rects: list[QRect] = []
        self.setMouseTracking(True)
        self.setMinimumWidth(0)

    def set_data(self, frame: pd.DataFrame, metric: str, available_width: int) -> None:
        self._frame = frame.copy() if frame is not None else pd.DataFrame()
        self._metric = metric
        self._width_hint = max(360, int(available_width or 900))
        self._layout_items()
        self.update()

    def _layout_items(self) -> None:
        self._items = []
        self._bands = {}
        self._database_rects = []
        width = self._width_hint
        pitch = self.SQUARE + self.GAP
        columns = max(1, (width - self.LABEL_WIDTH - 34) // pitch)
        y = 12
        if self._frame.empty:
            self.resize(width, 160)
            return
        work = self._frame.copy()
        database_col = "source_db" if "source_db" in work.columns else "database_name"
        work[database_col] = work.get(database_col, "Unknown").fillna("Unknown").astype(str)
        if "namespace_id" in work.columns:
            work[database_col] = (
                work.get("cluster_name", work["namespace_id"]).fillna("producer").astype(str)
                + " • " + work[database_col]
            )
        work["schema_name"] = work.get("schema_name", "Unknown").fillna("Unknown").astype(str)
        db_order = (
            work.groupby(database_col, dropna=False)["size_mb"].max().sort_values(ascending=False).index.tolist()
        )
        self._group_labels: list[tuple[int, str, bool]] = []
        for database in db_order:
            db_rows = work[work[database_col] == database]
            db_top = y
            self._group_labels.append((y + 17, str(database), True))
            y += 29
            schema_order = (
                db_rows.groupby("schema_name", dropna=False)["size_mb"].max().sort_values(ascending=False).index.tolist()
            )
            for schema in schema_order:
                schema_rows = db_rows[db_rows["schema_name"] == schema].sort_values(
                    ["size_mb", "tbl_rows"], ascending=[False, False], na_position="last"
                )
                count = len(schema_rows)
                visual_rows = max(1, math.ceil(count / columns))
                block_height = visual_rows * pitch
                self._group_labels.append((y + 10, str(schema), False))
                for offset, (_, row) in enumerate(schema_rows.iterrows()):
                    col = offset % columns
                    line = offset // columns
                    rect = QRect(self.LABEL_WIDTH + col * pitch, y + line * pitch, self.SQUARE, self.SQUARE)
                    self._items.append((rect, row))
                    for band in range(rect.top() // pitch, rect.bottom() // pitch + 1):
                        self._bands.setdefault(band, []).append((rect, row))
                y += block_height + 9
            self._database_rects.append(QRect(7, db_top, width - 18, y - db_top + 2))
            y += 12
        self.resize(width, max(160, y + 12))

    def sizeHint(self) -> QSize:
        return QSize(self._width_hint, max(160, self.height()))

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.fillRect(self.rect(), self.palette().base())
        painter.setRenderHint(QPainter.Antialiasing, False)
        for rect in self._database_rects:
            painter.setPen(QPen(QColor(PALETTE.border_strong), 1))
            painter.drawRoundedRect(rect, 5, 5)
        for y, label, is_database in getattr(self, "_group_labels", []):
            painter.setPen(QColor(PALETTE.text_0 if is_database else PALETTE.text_2))
            font = painter.font()
            font.setBold(is_database)
            painter.setFont(font)
            painter.drawText(16 if is_database else 28, y, label)
        painter.setPen(QPen(QColor(PALETTE.bg_0), 1))
        for rect, row in self._items:
            if self._metric == "external_partition":
                color = (
                    QColor(_EXTERNAL_PARTITIONED)
                    if bool(row.get("partition_present"))
                    else QColor(_EXTERNAL_UNPARTITIONED)
                )
                painter.fillRect(rect, color)
                painter.drawRect(rect)
                continue
            top_health, bottom_health = _tile_healths(row, self._metric)
            if self._metric == "distribution":
                painter.fillRect(rect, _health_color(top_health))
            elif self._metric == "sort_key":
                painter.fillRect(rect, _health_color(top_health))
            else:
                half = rect.height() // 2
                top = QRect(rect.left(), rect.top(), rect.width(), half)
                bottom = QRect(rect.left(), rect.top() + half, rect.width(), rect.height() - half)
                painter.fillRect(top, _health_color(top_health))
                painter.fillRect(bottom, _health_color(bottom_health))
            painter.drawRect(rect)
            if self._metric == "combined_health":
                painter.drawLine(rect.left(), rect.center().y(), rect.right(), rect.center().y())
            if _statistics_alert(row):
                font = painter.font()
                font.setBold(True)
                font.setPixelSize(17)
                painter.setFont(font)
                painter.setPen(QColor("#252B36"))
                painter.drawText(rect.translated(1, 1), Qt.AlignCenter, "!")
                painter.setPen(QColor("#FFFFFF"))
                painter.drawText(rect, Qt.AlignCenter, "!")
                painter.setFont(self.font())
                painter.setPen(QPen(QColor(PALETTE.bg_0), 1))

    def mouseMoveEvent(self, event) -> None:
        pos = event.position().toPoint()
        pitch = self.SQUARE + self.GAP
        candidates = self._bands.get(pos.y() // pitch, [])
        for rect, row in candidates:
            if rect.contains(pos):
                QToolTip.showText(event.globalPosition().toPoint(), self._tooltip(row), self)
                return
        QToolTip.hideText()

    def leaveEvent(self, event) -> None:
        QToolTip.hideText()
        super().leaveEvent(event)

    @staticmethod
    def _tooltip(row: pd.Series) -> str:
        if "partition_present" in row.index:
            partitioned = bool(row.get("partition_present"))
            partition_columns = (
                _clean_text(row.get("partition_key_details"))
                or _clean_text(row.get("partition_key_columns"))
                or "none"
            )
            database = (
                _clean_text(row.get("redshift_database_name"))
                or _clean_text(row.get("source_db"))
                or "?"
            )
            schema = _clean_text(row.get("schema_name")) or "?"
            table = _clean_text(row.get("table_name")) or "?"
            return "\n".join(
                [
                    f"External table: {database}.{schema}.{table}",
                    (
                        "Partition status: PARTITIONED (blue)"
                        if partitioned
                        else "Partition status: NOT PARTITIONED (orange)"
                    ),
                    f"Producer: {_clean_text(row.get('cluster_name')) or 'Producer'}",
                    f"Namespace: {_clean_text(row.get('namespace_id')) or 'producer'}",
                    f"Database: {database}",
                    f"Schema: {schema}",
                    f"Table: {table}",
                    f"Partition key(s): {partition_columns}",
                    (
                        "Sort-key equivalent: "
                        f"{_clean_text(row.get('sortkey')) or 'none'}"
                    ),
                    f"Columns: {int(_number(row.get('column_count'), 0)):,}",
                    (
                        "Nullable columns: "
                        f"{int(_number(row.get('nullable_column_count'), 0)):,}"
                    ),
                    f"Data types: {_clean_text(row.get('data_types')) or 'unknown'}",
                    f"Column details: {_clean_text(row.get('column_preview')) or 'unavailable'}",
                    f"Metadata source: {_clean_text(row.get('metadata_source')) or 'Producer SVV_EXTERNAL_COLUMNS'}",
                ]
            )
        database = row.get("source_db") or row.get("database_name") or "?"
        schema = row.get("schema_name") or "?"
        table = row.get("table_name") or "?"
        return "\n".join(
            [
                f"Cluster: {row.get('cluster_name') or row.get('namespace_id') or 'producer'}",
                f"Namespace: {row.get('namespace_id') or 'producer'}",
                f"{database}.{schema}.{table}",
                f"Size: {_number(row.get('size_mb')) / 1024.0:,.2f} GB",
                f"Rows: {_number(row.get('tbl_rows')):,.0f}",
                f"Sort key: {row.get('sortkey1') or 'missing'}",
                f"Sorted: {_number(row.get('sorted_pct')):,.1f}%",
                f"Distribution: {row.get('diststyle') or 'unknown'}",
                f"Distribution key: {_distribution_key(row) or 'missing'}",
                f"Row skew: {_number(row.get('skew_rows'), 1.0):,.2f}",
                f"Distribution health: {_health_label(_distribution_health(row))}",
                f"Sort health: {_health_label(_sort_health(row))}",
                f"Statistics fresh: {_statistics_fresh_pct(row):,.1f}%",
                f"Statistics stale: {_number(row.get('stats_off')):,.1f}%",
                "Statistics alert: ! (freshness ≤ 60%)" if _statistics_alert(row) else "Statistics alert: none",
            ]
        )


def _legend_swatch(color: str, text: str) -> QWidget:
    widget = QWidget()
    layout = QHBoxLayout(widget)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(5)
    swatch = QLabel()
    swatch.setFixedSize(15, 15)
    swatch.setStyleSheet(f"background:{color}; border:1px solid {PALETTE.bg_0}; border-radius:2px;")
    label = QLabel(text)
    label.setObjectName("Caption")
    layout.addWidget(swatch)
    layout.addWidget(label)
    return widget


class _HeatMapLegend(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        root = QGridLayout(self)
        root.setContentsMargins(12, 8, 12, 8)
        root.setHorizontalSpacing(20)
        root.setVerticalSpacing(6)

        title = QLabel("HOW TO READ EACH TABLE SQUARE")
        title.setStyleSheet("font-weight:800;")
        note = QLabel(
            "Every square is one physical table. The horizontal split reports two independent conditions. "
            "Distribution Only and Sort Only color the entire square. A centered ! remains visible in every view "
            "when statistics freshness is 60% or less."
        )
        note.setObjectName("Caption")
        note.setWordWrap(True)
        note.setMinimumWidth(0)
        note.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        root.addWidget(title, 0, 0)
        root.addWidget(note, 0, 1)

        distribution = QVBoxLayout()
        distribution_title = QLabel("TOP HALF — DISTRIBUTION HEALTH")
        distribution_title.setStyleSheet("font-weight:800;")
        distribution.addWidget(distribution_title)
        distribution.addWidget(_legend_swatch("#2EAD68", "DISTKEY exists; skew ≤ 2.0"))
        distribution.addWidget(_legend_swatch("#F5A623", "DISTKEY exists; skew > 2.0"))
        distribution.addWidget(_legend_swatch("#D94B4B", "No concrete DISTKEY / unresolved AUTO"))
        root.addLayout(distribution, 1, 0)

        sort = QVBoxLayout()
        sort_title = QLabel("BOTTOM HALF — SORT HEALTH")
        sort_title.setStyleSheet("font-weight:800;")
        sort.addWidget(sort_title)
        sort.addWidget(_legend_swatch("#2EAD68", "SORTKEY exists; sorted > 90%"))
        sort.addWidget(_legend_swatch("#F5A623", "SORTKEY exists; sorted 50–90%"))
        sort.addWidget(_legend_swatch("#D94B4B", "No SORTKEY or sorted < 50%"))
        root.addLayout(sort, 1, 1)

        alert = QHBoxLayout()
        alert_title = QLabel("CENTER MARKER")
        alert_title.setStyleSheet("font-weight:800;")
        alert.addWidget(alert_title)
        marker = QLabel("!")
        marker.setAlignment(Qt.AlignCenter)
        marker.setFixedSize(28, 28)
        marker.setStyleSheet(
            "background:#566173; color:white; border:1px solid #252B36; border-radius:3px; "
            "font-size:18px; font-weight:900;"
        )
        alert.addWidget(marker)
        marker_text = QLabel("Statistics freshness ≤ 60% (stats_off ≥ 40%)")
        marker_text.setObjectName("Caption")
        alert.addWidget(marker_text)
        alert.addStretch(1)
        root.addLayout(alert, 2, 0, 1, 2)
        root.setColumnStretch(0, 1)
        root.setColumnStretch(1, 1)


class _ExternalHeatMapLegend(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("CardSubtle")
        root = QGridLayout(self)
        root.setContentsMargins(12, 8, 12, 8)
        root.setHorizontalSpacing(18)
        root.setVerticalSpacing(5)
        title = QLabel("EXTERNAL TABLE METADATA")
        title.setStyleSheet("font-weight:800;")
        note = QLabel(
            "One square per external table, derived only from Producer "
            "SVV_EXTERNAL_COLUMNS. Hover a square for keys, columns, data "
            "types, nullability, database, schema, and table identity."
        )
        note.setObjectName("Caption")
        note.setWordWrap(True)
        note.setMinimumWidth(0)
        note.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        root.addWidget(title, 0, 0)
        root.addWidget(note, 0, 1, 1, 2)
        root.addWidget(
            _legend_swatch(
                _EXTERNAL_UNPARTITIONED,
                "Orange — partition key not present",
            ),
            1,
            0,
        )
        root.addWidget(
            _legend_swatch(
                _EXTERNAL_PARTITIONED,
                "Blue — partition key present (sort-key equivalent)",
            ),
            1,
            1,
        )
        root.setColumnStretch(2, 1)


class TableHeatMap(QWidget):
    loadRequested = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._frame = pd.DataFrame()
        self._external_metadata = pd.DataFrame()
        self._external_frame = pd.DataFrame()
        self._db_path = Path()
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 10)
        root.setSpacing(8)

        header = QHBoxLayout()
        self._title = QLabel("TABLE HEALTH HEAT MAP")
        self._title.setObjectName("SectionHeader")
        header.addWidget(self._title)
        header.addStretch(1)
        load_btn = QPushButton("Reload Heat Map")
        load_btn.setObjectName("Primary")
        load_btn.clicked.connect(lambda: self.loadRequested.emit("table_heatmap"))
        header.addWidget(load_btn)
        root.addLayout(header)
        self._status = QLabel("Open this tab to load table health from local DuckDB.")
        self._status.setObjectName("Caption")
        self._status.setWordWrap(True)
        self._status.setMinimumWidth(0)
        self._status.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        root.addWidget(self._status)

        scope = QFrame()
        scope.setObjectName("CardSubtle")
        scope_bar = QGridLayout(scope)
        scope_bar.setContentsMargins(12, 8, 12, 8)
        scope_bar.setHorizontalSpacing(8)
        scope_bar.setVerticalSpacing(6)
        scope_title = QLabel("SCOPE")
        scope_title.setStyleSheet("font-weight:800;")
        scope_bar.addWidget(scope_title, 0, 0)
        scope_bar.addWidget(QLabel("Map"), 0, 1)
        self._view_mode = QComboBox()
        self._view_mode.addItem("Physical table health", "physical")
        self._view_mode.addItem(
            "External table metadata — Producer",
            "external",
        )
        self._view_mode.setMinimumWidth(220)
        scope_bar.addWidget(self._view_mode, 0, 2, 1, 3)
        clear_scope = QPushButton("Clear filters")
        clear_scope.setToolTip("Show all clusters, databases, and schemas")
        clear_scope.clicked.connect(self._clear_scope_filters)
        scope_bar.addWidget(clear_scope, 0, 5)
        scope_bar.addWidget(QLabel("Cluster"), 1, 0)
        self._cluster_filter = QComboBox()
        self._cluster_filter.setMinimumWidth(140)
        self._cluster_filter.addItem("All clusters", "")
        scope_bar.addWidget(self._cluster_filter, 1, 1)
        scope_bar.addWidget(QLabel("Database"), 1, 2)
        self._database_filter = QComboBox()
        self._database_filter.setMinimumWidth(135)
        self._database_filter.addItem("All databases", "")
        scope_bar.addWidget(self._database_filter, 1, 3)
        scope_bar.addWidget(QLabel("Schema"), 1, 4)
        self._schema_filter = QComboBox()
        self._schema_filter.setMinimumWidth(125)
        self._schema_filter.addItem("All schemas", "")
        scope_bar.addWidget(self._schema_filter, 1, 5)
        for column in (1, 3, 5):
            scope_bar.setColumnStretch(column, 1)
        root.addWidget(scope)

        self._legend = _HeatMapLegend()
        root.addWidget(self._legend)
        self._external_legend = _ExternalHeatMapLegend()
        self._external_legend.setVisible(False)
        root.addWidget(self._external_legend)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(False)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._scroll.viewport().installEventFilter(self)
        self._canvas = _HeatMapCanvas()
        self._scroll.setWidget(self._canvas)
        root.addWidget(self._scroll, 1)

        controls = QFrame()
        controls.setObjectName("CardSubtle")
        bar = QGridLayout(controls)
        bar.setContentsMargins(10, 7, 10, 7)
        bar.setHorizontalSpacing(8)
        bar.setVerticalSpacing(5)
        bar.addWidget(QLabel("Focused View"), 0, 0)
        self._metric = QComboBox()
        for key, label in METRICS:
            self._metric.addItem(label, key)
        bar.addWidget(self._metric, 0, 1, 1, 3)
        self._problems_only = QCheckBox("Problems Only")
        bar.addWidget(self._problems_only, 0, 4, 1, 2)
        bar.addWidget(QLabel("Min Size"), 1, 0)
        self._size_slider = QSlider(Qt.Horizontal)
        self._size_slider.setRange(0, 100)
        self._size_slider.setMinimumWidth(80)
        bar.addWidget(self._size_slider, 1, 1)
        self._size_value = QDoubleSpinBox()
        self._size_value.setDecimals(0)
        self._size_value.setRange(0, 1_000_000_000)
        self._size_value.setSuffix(" MB")
        self._size_value.setGroupSeparatorShown(True)
        self._size_value.setMinimumWidth(135)
        self._size_value.setValue(10)
        bar.addWidget(self._size_value, 1, 2)
        bar.addWidget(QLabel("Min Rows"), 1, 3)
        self._rows_slider = QSlider(Qt.Horizontal)
        self._rows_slider.setRange(0, 100)
        self._rows_slider.setMinimumWidth(80)
        bar.addWidget(self._rows_slider, 1, 4)
        self._rows_value = QDoubleSpinBox()
        self._rows_value.setDecimals(0)
        self._rows_value.setRange(0, 1_000_000_000_000)
        self._rows_value.setSuffix(" rows")
        self._rows_value.setGroupSeparatorShown(True)
        self._rows_value.setMinimumWidth(165)
        self._rows_value.setValue(1_000_000)
        bar.addWidget(self._rows_value, 1, 5)
        bar.setColumnStretch(1, 1)
        bar.setColumnStretch(4, 1)
        self._physical_controls = controls
        root.addWidget(controls)

        self._view_mode.currentIndexChanged.connect(self._view_mode_changed)
        self._metric.currentIndexChanged.connect(self._refresh)
        self._size_slider.valueChanged.connect(self._size_slider_changed)
        self._size_value.valueChanged.connect(self._size_exact_changed)
        self._rows_slider.valueChanged.connect(self._rows_slider_changed)
        self._rows_value.valueChanged.connect(self._rows_exact_changed)
        self._problems_only.toggled.connect(self._refresh)
        self._cluster_filter.currentIndexChanged.connect(self._cluster_filter_changed)
        self._database_filter.currentIndexChanged.connect(self._database_filter_changed)
        self._schema_filter.currentIndexChanged.connect(self._refresh)
        self._size_slider.setValue(self._size_to_slider(10))
        self._rows_slider.setValue(self._rows_to_slider(1_000_000))

    def set_report(self, report) -> None:
        self._db_path = Path(report.db_path)
        self._frame = report.table_heatmap.copy() if report.table_heatmap is not None else pd.DataFrame()
        external_metadata = getattr(
            report, "external_table_metadata", pd.DataFrame()
        )
        self._external_metadata = (
            external_metadata.copy()
            if external_metadata is not None
            else pd.DataFrame()
        )
        self._external_frame = _aggregate_external_metadata(
            self._external_metadata
        )
        self._populate_cluster_filter()
        self._populate_database_filter()
        self._populate_schema_filter()
        self._refresh()

    def has_data(self) -> bool:
        return not self._frame.empty or not self._external_frame.empty

    def show_loading(self) -> None:
        self._status.setText(
            "Loading physical table health and Producer external table "
            "metadata from local DuckDB …"
        )

    @staticmethod
    def _size_from_slider(position: int) -> float:
        if position <= 0:
            return 0.0
        return 10 ** (-1 + (position / 100.0) * 10)

    @staticmethod
    def _size_to_slider(value: float) -> int:
        if value <= 0:
            return 0
        return round(max(0, min(1, (math.log10(value) + 1) / 10)) * 100)

    @staticmethod
    def _rows_from_slider(position: int) -> float:
        if position <= 0:
            return 0.0
        return 10 ** ((position / 100.0) * 12)

    @staticmethod
    def _rows_to_slider(value: float) -> int:
        if value <= 0:
            return 0
        return round(max(0, min(1, math.log10(value) / 12)) * 100)

    def _size_slider_changed(self, position: int) -> None:
        self._size_value.blockSignals(True)
        self._size_value.setValue(self._size_from_slider(position))
        self._size_value.blockSignals(False)
        self._refresh()

    def _size_exact_changed(self, value: float) -> None:
        self._size_slider.blockSignals(True)
        self._size_slider.setValue(self._size_to_slider(value))
        self._size_slider.blockSignals(False)
        self._refresh()

    def _rows_slider_changed(self, position: int) -> None:
        self._rows_value.blockSignals(True)
        self._rows_value.setValue(self._rows_from_slider(position))
        self._rows_value.blockSignals(False)
        self._refresh()

    def _rows_exact_changed(self, value: float) -> None:
        self._rows_slider.blockSignals(True)
        self._rows_slider.setValue(self._rows_to_slider(value))
        self._rows_slider.blockSignals(False)
        self._refresh()

    @staticmethod
    def _selected_value(combo: QComboBox) -> str:
        return str(combo.currentData() or "").strip()

    def _external_mode(self) -> bool:
        return self._selected_value(self._view_mode) == "external"

    def _active_frame(self) -> pd.DataFrame:
        return self._external_frame if self._external_mode() else self._frame

    def _view_mode_changed(self) -> None:
        external = self._external_mode()
        self._title.setText(
            "EXTERNAL TABLE METADATA HEAT MAP"
            if external
            else "TABLE HEALTH HEAT MAP"
        )
        self._legend.setVisible(not external)
        self._external_legend.setVisible(external)
        self._physical_controls.setVisible(not external)
        self._populate_cluster_filter()
        self._populate_database_filter()
        self._populate_schema_filter()
        self._refresh()

    @staticmethod
    def _replace_options(
        combo: QComboBox,
        all_label: str,
        options: list[tuple[str, str]],
    ) -> None:
        previous = str(combo.currentData() or "").strip()
        combo.blockSignals(True)
        combo.clear()
        combo.addItem(all_label, "")
        for label, value in options:
            combo.addItem(label, value)
        previous_index = combo.findData(previous)
        combo.setCurrentIndex(previous_index if previous_index >= 0 else 0)
        combo.blockSignals(False)

    def _populate_cluster_filter(self) -> None:
        options: list[tuple[str, str]] = []
        frame = self._active_frame()
        if not frame.empty:
            work = frame.copy()
            work["_cluster_value"] = _cluster_values(work)
            work["_cluster_label"] = _text_values(work, "cluster_name")
            for value, rows in work.groupby("_cluster_value", sort=False):
                friendly = next(
                    (name for name in rows["_cluster_label"].tolist() if name),
                    str(value),
                )
                label = friendly if friendly.casefold() == str(value).casefold() else f"{friendly} — {value}"
                options.append((label, str(value)))
        options.sort(key=lambda option: option[0].casefold())
        self._replace_options(self._cluster_filter, "All clusters", options)

    def _populate_database_filter(self) -> None:
        scoped = _scope_rows(
            self._active_frame(),
            cluster=self._selected_value(self._cluster_filter),
        )
        values = sorted(
            {value for value in _database_values(scoped).tolist() if value},
            key=str.casefold,
        )
        self._replace_options(
            self._database_filter,
            "All databases",
            [(value, value) for value in values],
        )

    def _populate_schema_filter(self) -> None:
        scoped = _scope_rows(
            self._active_frame(),
            cluster=self._selected_value(self._cluster_filter),
            database=self._selected_value(self._database_filter),
        )
        values = sorted(
            {value for value in _schema_values(scoped).tolist() if value},
            key=str.casefold,
        )
        self._replace_options(
            self._schema_filter,
            "All schemas",
            [(value, value) for value in values],
        )

    def _cluster_filter_changed(self) -> None:
        self._populate_database_filter()
        self._populate_schema_filter()
        self._refresh()

    def _database_filter_changed(self) -> None:
        self._populate_schema_filter()
        self._refresh()

    def _clear_scope_filters(self) -> None:
        self._cluster_filter.blockSignals(True)
        self._database_filter.blockSignals(True)
        self._schema_filter.blockSignals(True)
        self._cluster_filter.setCurrentIndex(0)
        self._database_filter.setCurrentIndex(0)
        self._schema_filter.setCurrentIndex(0)
        self._cluster_filter.blockSignals(False)
        self._database_filter.blockSignals(False)
        self._schema_filter.blockSignals(False)
        self._populate_database_filter()
        self._populate_schema_filter()
        self._refresh()

    def _refresh(self) -> None:
        if self._external_mode():
            filtered = _filter_external_heatmap_rows(
                self._external_frame,
                cluster=self._selected_value(self._cluster_filter),
                database=self._selected_value(self._database_filter),
                schema=self._selected_value(self._schema_filter),
            )
            width = max(360, self._scroll.viewport().width() - 2)
            self._canvas.set_data(
                filtered,
                "external_partition",
                width,
            )
            partitioned = int(
                filtered.get(
                    "partition_present",
                    pd.Series(False, index=filtered.index),
                ).fillna(False).astype(bool).sum()
            )
            self._status.setText(
                f"{len(filtered):,} of {len(self._external_frame):,} external "
                f"tables shown; {partitioned:,} partitioned (blue), "
                f"{len(filtered) - partitioned:,} not partitioned (orange). "
                "Producer SVV_EXTERNAL_COLUMNS only."
            )
            return
        metric = str(self._metric.currentData() or "combined_health")
        filtered = _filter_heatmap_rows(
            self._frame,
            metric=metric,
            min_size_mb=float(self._size_value.value()),
            min_rows=float(self._rows_value.value()),
            problems_only=self._problems_only.isChecked(),
            cluster=self._selected_value(self._cluster_filter),
            database=self._selected_value(self._database_filter),
            schema=self._selected_value(self._schema_filter),
        )
        width = max(360, self._scroll.viewport().width() - 2)
        self._canvas.set_data(filtered, metric, width)
        self._status.setText(
            f"{len(filtered):,} of {len(self._frame):,} tables shown; "
            f"{self._metric.currentText()}; largest tables first."
        )

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._refresh()

    def eventFilter(self, watched, event) -> bool:
        if watched is self._scroll.viewport() and event.type() == QEvent.Resize:
            QTimer.singleShot(0, self._refresh)
        return super().eventFilter(watched, event)
