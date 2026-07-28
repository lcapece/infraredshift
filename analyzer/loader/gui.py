"""Standalone Infraredshift Loader — a purpose-built window, not the analyzer.

This is deliberately NOT the analytics application. It imports nothing from
``analyzer.widgets`` and carries its own compact visual identity, so the
loader stays usable, launchable, and understandable on its own:

- One primary action that adapts: Start Safe Load / Resume Safe Load /
  Cancel Load.
- Live per-namespace, per-table progress fed by the loader process's
  ``INFRAREDSHIFT_EVENT`` lines.
- Promote to Live is enabled only when a completed staged snapshot exists.
- The capture itself always runs in a separate process (the same command
  Windows Task Scheduler uses), so this window can never freeze or corrupt
  a load by closing.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
from typing import Callable

from PySide6.QtCore import QProcess, Qt, QTimer
from PySide6.QtGui import QBrush, QColor, QFont, QGuiApplication
from PySide6.QtWidgets import (
    QCheckBox,
    QDoubleSpinBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..brand import PRODUCT_NAME, PRODUCT_NAME_UPPER
from .engine import (
    LoaderRequest,
    build_loader_command,
    build_promote_command,
    printable_loader_command,
    roster_command,
)

_EVENT_PREFIX = "INFRAREDSHIFT_EVENT "
_STATE_TABLE = "_tmp_refresh_state"
_NO_QUALIFYING_QUERY_STATUS = "Complete — no qualifying queries"
_CONSUMER_CATALOG_DATABASE_NOT_PRESENT_STATUS = (
    "Complete — enterprise_datawarehouse not present on this consumer"
)
_COMPLETE_DATASET_STATUSES = {
    "Staged in DuckDB",
    "Recovered checkpoint",
    _NO_QUALIFYING_QUERY_STATUS,
    _CONSUMER_CATALOG_DATABASE_NOT_PRESENT_STATUS,
}
_EMPTY_SUCCESS_STATUSES = {
    _NO_QUALIFYING_QUERY_STATUS,
    _CONSUMER_CATALOG_DATABASE_NOT_PRESENT_STATUS,
}
_EMPTY_SUCCESS_BACKGROUND = QColor("#FFF2B2")
_EMPTY_SUCCESS_FOREGROUND = QColor("#765400")

# Self-contained look: calm, light, one green accent. No dependency on the
# analyzer's application stylesheet keeps the delineation honest.
_STYLE = """
QWidget#LoaderWindow { background: #F2F7F5; }
QLabel#LoaderTitle { color: #163D30; font-size: 20px; font-weight: 750; letter-spacing: 0.5px; }
QLabel#LoaderSubtitle { color: #557168; font-size: 11px; }
QFrame.LoaderCard { background: #FFFFFF; border: 1px solid #D5E5DE; border-radius: 10px; }
QLabel.LoaderCardTitle { color: #008555; font-size: 10px; font-weight: 750; letter-spacing: 1px; }
QLabel#LoaderStatus { color: #163D30; font-size: 14px; font-weight: 650; }
QLabel#LoaderStatusDetail { color: #557168; font-size: 11px; }
QPushButton#LoaderPrimary {
    background: #008555; color: #FAFCFB; border: 1px solid #008555;
    border-radius: 7px; padding: 10px 22px; font-size: 13px; font-weight: 700; min-width: 170px;
}
QPushButton#LoaderPrimary:hover { background: #00965E; }
QPushButton#LoaderPrimary:disabled { background: #9DBFB2; border-color: #9DBFB2; }
QPushButton#LoaderPrimary[cancelMode="true"] { background: #B33A3A; border-color: #B33A3A; }
QPushButton#LoaderPrimary[cancelMode="true"]:hover { background: #C24747; }
QPushButton#LoaderPromote {
    background: #FFFFFF; color: #008555; border: 1px solid #80BFA7;
    border-radius: 7px; padding: 10px 18px; font-weight: 650;
}
QPushButton#LoaderPromote:hover { background: #E7F1ED; }
QPushButton#LoaderPromote:disabled { color: #9DBFB2; border-color: #D5E5DE; }
QPushButton.LoaderQuiet { background: transparent; color: #557168; border: none; padding: 8px 10px; }
QPushButton.LoaderQuiet:hover { color: #163D30; background: #E7F1ED; border-radius: 6px; }
QLineEdit, QSpinBox, QDoubleSpinBox {
    background: #FFFFFF; color: #183E31; border: 1px solid #C6DAD1; border-radius: 6px; padding: 5px 8px;
}
QCheckBox { color: #315B4C; font-size: 12px; }
QProgressBar {
    background: #E3EEE9; border: none; border-radius: 6px; height: 12px; text-align: center;
    color: #163D30; font-size: 9px; font-weight: 650;
}
QProgressBar::chunk { background: #008555; border-radius: 6px; }
QProgressBar[emptySuccess="true"]::chunk { background: #D6A21B; }
QTreeWidget {
    background: #FFFFFF; color: #183E31; border: 1px solid #D5E5DE; border-radius: 8px;
    alternate-background-color: #F6FAF8; font-size: 11px;
}
QHeaderView::section {
    background: #EDF4F1; color: #557168; border: none; border-bottom: 1px solid #D5E5DE;
    padding: 5px 8px; font-size: 10px; font-weight: 700;
}
QPlainTextEdit#LoaderLog {
    background: #10241C; color: #BFE3D4; border: 1px solid #0D1F18; border-radius: 8px;
    font-family: Consolas, monospace; font-size: 10px;
}
"""


def _default_duckdb_path(value: str | None) -> str:
    if value:
        return str(Path(value).expanduser())
    configured = str(os.environ.get("REDSHIFT_DUCKDB_PATH") or "").strip()
    if configured:
        return configured
    from ..duckdb_store import default_duckdb_path

    return str(default_duckdb_path())


def read_staging_state(duckdb_path: str | Path) -> dict[str, str]:
    """Read the loader's durable checkpoint state without taking any lock."""
    target = Path(duckdb_path).expanduser()
    if not target.is_file():
        return {}
    import duckdb

    try:
        con = duckdb.connect(str(target), read_only=True)
    except Exception:
        return {}
    try:
        rows = con.execute(
            f"SELECT state_key, state_value FROM {_STATE_TABLE}"
        ).fetchall()
        return {str(key): str(value) for key, value in rows}
    except Exception:
        return {}
    finally:
        con.close()


def read_staging_checkpoint_progress(
    duckdb_path: str | Path,
    state: dict[str, str],
) -> tuple[int, int]:
    """Return durable completed/required checkpoint counts for display."""
    completed, required, _gaps = read_staging_checkpoint_details(
        duckdb_path, state,
    )
    return completed, required


def read_staging_checkpoint_details(
    duckdb_path: str | Path,
    state: dict[str, str],
) -> tuple[int, int, list[str]]:
    """Return completed/required counts plus every missing checkpoint."""
    planned = tuple(
        value.strip()
        for value in str(state.get("selected_tables") or "").split(",")
        if value.strip()
    )
    target = Path(duckdb_path).expanduser()
    if not planned or not target.is_file():
        return 0, 0, []
    import duckdb
    import runner

    try:
        con = duckdb.connect(str(target), read_only=True)
    except Exception:
        return 0, 0, []
    try:
        completed, required, gaps = runner.staging_checkpoint_progress(
            con, state, planned
        )
        return completed, required, list(gaps)
    except Exception:
        return 0, 0, []
    finally:
        con.close()


def read_load_failures(
    duckdb_path: str | Path,
    state: dict[str, str],
) -> list[dict[str, str]]:
    """Read the exact sanitized failures for the current staged snapshot."""
    target = Path(duckdb_path).expanduser()
    report_value = str(state.get("load_report") or "").strip()
    candidates: list[Path] = []
    if report_value:
        report_path = Path(report_value).expanduser()
        candidates.append(report_path.with_suffix(".json"))
    candidates.append(target.resolve(strict=False).parent / "load_report.json")
    snapshot_id = str(state.get("snapshot_id") or "").strip()
    seen: set[str] = set()
    for candidate in candidates:
        key = os.path.normcase(str(candidate.resolve(strict=False)))
        if key in seen or not candidate.is_file():
            continue
        seen.add(key)
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            continue
        report_snapshot = str(payload.get("snapshot_id") or "").strip()
        if snapshot_id and report_snapshot and report_snapshot != snapshot_id:
            continue
        failures = payload.get("failures")
        if not isinstance(failures, list):
            continue
        return [
            {
                "namespace_id": str(item.get("namespace_id") or ""),
                "table": str(item.get("table") or ""),
                "scope": str(item.get("scope") or ""),
                "error_type": str(item.get("error_type") or ""),
                "error": str(item.get("error") or ""),
            }
            for item in failures
            if isinstance(item, dict)
        ]
    return []


def _failure_summary(failure: dict[str, str]) -> str:
    table = failure.get("table") or "unknown dataset"
    namespace = failure.get("namespace_id") or "unknown source"
    error = failure.get("error") or failure.get("error_type") or "unknown error"
    return f"{table} @ {namespace}: {error}"


def read_no_qualifying_query_namespaces(
    duckdb_path: str | Path,
    state: dict[str, str],
) -> set[str]:
    """Return namespaces whose zero-row query-history checkpoint is success."""
    snapshot_id = str(state.get("snapshot_id") or "").strip()
    target = Path(duckdb_path).expanduser()
    if not snapshot_id or not target.is_file():
        return set()
    import duckdb

    try:
        con = duckdb.connect(str(target), read_only=True)
    except Exception:
        return set()
    try:
        return {
            str(namespace_id).strip()
            for namespace_id, in con.execute(
                "SELECT namespace_id FROM _tmp_namespace_refresh_state "
                "WHERE snapshot_id = ? AND table_name = 'query_history' "
                "AND status = 'complete' AND COALESCE(source_rows, 0) = 0",
                [snapshot_id],
            ).fetchall()
            if str(namespace_id or "").strip()
        }
    except Exception:
        return set()
    finally:
        con.close()


class LoaderWindow(QWidget):
    """The Infraredshift Loader operator window."""

    def __init__(
        self,
        duckdb_path: str,
        parent=None,
        *,
        embedded: bool = False,
        credentials_callback: Callable[[], None] | None = None,
    ):
        super().__init__(parent)
        self._embedded = bool(embedded)
        self._credentials_callback = credentials_callback
        self.setObjectName("LoaderWindow")
        self.setWindowTitle(f"{PRODUCT_NAME} Loader")
        self.resize(940, 660)
        self.setStyleSheet(_STYLE)

        self._process: QProcess | None = None
        self._stdout_buffer = ""
        self._stderr_tail = ""
        self._cancel_requested = False
        self._operation = ""
        self._namespace_items: dict[str, QTreeWidgetItem] = {}
        self._table_items: dict[tuple[str, str], QTreeWidgetItem] = {}

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 14, 18, 14)
        root.setSpacing(10)

        title = QLabel("DATA LOADER" if self._embedded else f"{PRODUCT_NAME_UPPER} LOADER")
        title.setObjectName("LoaderTitle")
        subtitle = QLabel(
            "Safe, recoverable data collection — runs separately from the analyzer. "
            "Cancelling or closing never loses completed checkpoints."
        )
        subtitle.setObjectName("LoaderSubtitle")
        subtitle.setWordWrap(True)
        root.addWidget(title)
        root.addWidget(subtitle)

        # -- target row -----------------------------------------------------
        target_card = QFrame()
        target_card.setProperty("class", "LoaderCard")
        target_row = QHBoxLayout(target_card)
        target_row.setContentsMargins(12, 10, 12, 10)
        target_label = QLabel("WAREHOUSE FILE")
        target_label.setProperty("class", "LoaderCardTitle")
        self._path = QLineEdit(duckdb_path)
        browse = QPushButton("Browse…")
        browse.setProperty("class", "LoaderQuiet")
        browse.clicked.connect(self._browse)
        target_row.addWidget(target_label)
        target_row.addWidget(self._path, 1)
        target_row.addWidget(browse)
        root.addWidget(target_card)
        target_card.setVisible(not self._embedded)

        # -- settings card ---------------------------------------------------
        settings_card = QFrame()
        settings_card.setProperty("class", "LoaderCard")
        settings_box = QVBoxLayout(settings_card)
        settings_box.setContentsMargins(12, 10, 12, 10)
        settings_box.setSpacing(6)
        settings_row = QHBoxLayout()
        settings_row.setSpacing(14)
        settings_label = QLabel("LOAD WINDOW")
        settings_label.setProperty("class", "LoaderCardTitle")
        settings_row.addWidget(settings_label)
        settings_row.addWidget(QLabel("Days:"))
        self._days = QSpinBox()
        self._days.setRange(1, 35)
        self._days.setValue(7)
        settings_row.addWidget(self._days)
        floor_note = QLabel("Min query seconds: per cluster (Producer 300, consumers 30)")
        floor_note.setToolTip(
            "The minimum query cutoff is set per cluster by the administrator via "
            "FLOOR_SECONDS in the cluster profiles JSON. Defaults: Producer 300s, "
            "consumers 30s."
        )
        settings_row.addWidget(floor_note)
        settings_row.addStretch(1)
        settings_box.addLayout(settings_row)
        options_row = QHBoxLayout()
        options_row.setSpacing(18)
        self._auto_promote = QCheckBox("Promote automatically when complete")
        options_row.addWidget(self._auto_promote)
        self._fresh = QCheckBox("Start over (discard checkpoints)")
        options_row.addWidget(self._fresh)
        options_row.addStretch(1)
        settings_box.addLayout(options_row)
        root.addWidget(settings_card)
        settings_card.setVisible(not self._embedded)

        # -- status + progress ---------------------------------------------
        status_card = QFrame()
        status_card.setProperty("class", "LoaderCard")
        status_box = QVBoxLayout(status_card)
        status_box.setContentsMargins(12, 10, 12, 12)
        self._status = QLabel("Checking the warehouse…")
        self._status.setObjectName("LoaderStatus")
        self._status_detail = QLabel("")
        self._status_detail.setObjectName("LoaderStatusDetail")
        self._status_detail.setWordWrap(True)
        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        self._progress.setValue(0)
        self._progress.setFormat("%p%")
        status_box.addWidget(self._status)
        status_box.addWidget(self._status_detail)
        status_box.addWidget(self._progress)
        root.addWidget(status_card)

        # -- namespace/table progress tree ---------------------------------
        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["Source", "Rows", "Status"])
        self._tree.setAlternatingRowColors(True)
        self._tree.setRootIsDecorated(True)
        self._tree.setColumnWidth(0, 420)
        self._tree.setColumnWidth(1, 120)
        root.addWidget(self._tree, 1)

        # -- log ------------------------------------------------------------
        self._log = QPlainTextEdit()
        self._log.setObjectName("LoaderLog")
        self._log.setReadOnly(True)
        self._log.setMaximumHeight(110)
        self._log.setVisible(False)
        root.addWidget(self._log)

        # -- actions --------------------------------------------------------
        actions = QVBoxLayout()
        primary_actions = QHBoxLayout()
        secondary_actions = QHBoxLayout()
        self._primary = QPushButton("Start Safe Load")
        self._primary.setObjectName("LoaderPrimary")
        self._primary.clicked.connect(self._primary_clicked)
        self._promote = QPushButton("Promote to Live")
        self._promote.setObjectName("LoaderPromote")
        self._promote.setEnabled(False)
        self._promote.clicked.connect(self._promote_clicked)
        self._credentials = QPushButton("Set Usernames & Passwords")
        self._credentials.setObjectName("LoaderPromote")
        self._credentials.setToolTip(
            "Open the encrypted Redshift username and password editor directly. "
            "Saved credentials remain protected for this Windows account."
        )
        self._credentials.clicked.connect(self._edit_credentials)
        technical = QPushButton("Technical Details")
        technical.setProperty("class", "LoaderQuiet")
        technical.clicked.connect(
            lambda: self._log.setVisible(not self._log.isVisible())
        )
        self._roster = QPushButton("Load User Roster")
        self._roster.setProperty("class", "LoaderQuiet")
        self._roster.setToolTip(
            "Read SVV_USER_INFO from the producer and store the parsed user "
            "roster. Small and quick - it is a user list, not workload data. "
            "The roster is what lets Assign to Engineer and Email User resolve "
            "real names and addresses."
        )
        self._roster.clicked.connect(self._load_user_roster)
        schedule = QPushButton("Copy nightly schedule command")
        schedule.setProperty("class", "LoaderQuiet")
        schedule.clicked.connect(self._copy_schedule_command)
        close = QPushButton("Close")
        close.setProperty("class", "LoaderQuiet")
        close.clicked.connect(self.close)
        primary_actions.addWidget(self._primary)
        primary_actions.addWidget(self._credentials)
        primary_actions.addWidget(self._promote)
        primary_actions.addStretch(1)
        secondary_actions.addWidget(technical)
        secondary_actions.addWidget(self._roster)
        secondary_actions.addStretch(1)
        secondary_actions.addWidget(schedule)
        secondary_actions.addWidget(close)
        actions.addLayout(primary_actions)
        actions.addLayout(secondary_actions)
        schedule.setVisible(not self._embedded)
        close.setVisible(not self._embedded)
        root.addLayout(actions)

        QTimer.singleShot(0, self.refresh_idle_state)

    # ---- state probing ----------------------------------------------------

    def _running(self) -> bool:
        return self._process is not None and self._process.state() != QProcess.NotRunning

    def set_db_path(self, path: str) -> None:
        """Point the loader at the operator-selected warehouse (embedded use)."""
        path = str(path or "").strip()
        if path and path != self._path.text().strip():
            self._path.setText(path)
            self.refresh_idle_state()

    def refresh_idle_state(self) -> None:
        """Reflect the durable staging state while no load is running."""
        if self._running():
            return
        self._populate_sources()
        state = read_staging_state(self._path.text().strip())
        status = state.get("status", "")
        empty_query_namespaces = read_no_qualifying_query_namespaces(
            self._path.text().strip(), state
        )
        self._set_empty_success(bool(empty_query_namespaces))
        for namespace in empty_query_namespaces:
            self._mark_namespace_empty_success(namespace)
        (
            completed_checkpoints,
            required_checkpoints,
            checkpoint_gaps,
        ) = read_staging_checkpoint_details(
            self._path.text().strip(), state
        )
        failures = read_load_failures(self._path.text().strip(), state)
        for failure in failures:
            namespace = failure.get("namespace_id", "")
            table = failure.get("table", "")
            if namespace and table and not table.startswith("("):
                self._update_tree(
                    namespace,
                    table,
                    0,
                    "Retry required — "
                    + (
                        failure.get("error")
                        or failure.get("error_type")
                        or "unknown Redshift error"
                    ),
                )
        empty_success_note = (
            f"{len(empty_query_namespaces)} cluster(s) had no queries meeting "
            "their threshold; yellow means a successful empty result. "
            if empty_query_namespaces
            else ""
        )
        checkpoint_note = (
            f"{completed_checkpoints} of {required_checkpoints} required dataset "
            "checkpoints are complete. "
            if required_checkpoints
            else ""
        )
        if required_checkpoints:
            self._progress.setValue(
                round(100 * completed_checkpoints / required_checkpoints)
            )
        # Resume must use the original capture window. This matters most in the
        # embedded main app, where the compact loader intentionally hides the
        # advanced settings card and the operator cannot manually restore a
        # non-default value after restarting the application.
        if status in {"loading", "loaded"}:
            try:
                staged_days = round(float(state.get("days", "")))
            except (TypeError, ValueError):
                staged_days = 0
            if self._days.minimum() <= staged_days <= self._days.maximum():
                self._days.setValue(staged_days)
        try:
            recorded_failure_count = int(
                state.get("failure_count", "0") or 0
            )
        except ValueError:
            recorded_failure_count = 0
        unresolved_count = max(
            recorded_failure_count,
            len(checkpoint_gaps),
        )
        self._promote.setText("Promote to Live")
        if status == "loaded":
            self._status.setText("Staged snapshot is complete and ready to promote.")
            self._status_detail.setText(
                checkpoint_note
                + empty_success_note
                + "Review the staged data, then click Promote to Live. Live tables "
                "are untouched until promotion."
            )
            self._primary.setText("Start Safe Load")
            self._promote.setEnabled(True)
        elif status == "loading":
            if unresolved_count:
                self._status.setText(
                    f"Resume required: {unresolved_count} checkpoint(s) incomplete."
                )
                report_path = state.get("load_report", "")
                missing_note = (
                    "Resume will retry: "
                    + "; ".join(checkpoint_gaps[:3])
                    + (
                        f"; and {len(checkpoint_gaps) - 3} more. "
                        if len(checkpoint_gaps) > 3
                        else ". "
                    )
                    if checkpoint_gaps
                    else ""
                )
                error_note = (
                    "Exact error: " + _failure_summary(failures[0]) + ". "
                    if failures
                    else ""
                )
                self._status_detail.setText(
                    checkpoint_note
                    + empty_success_note
                    + missing_note
                    + error_note
                    + "Completed datasets remain staged and will not be reloaded."
                    + (f" Report: {report_path}" if report_path else "")
                )
                self._promote.setText("Promotion Blocked — Review")
            else:
                self._status.setText(
                    "Staging needs final validation before promotion."
                )
                self._status_detail.setText(
                    checkpoint_note
                    + empty_success_note
                    + "Validate & Promote checks every selected staging table, "
                    "snapshot ID, and namespace checkpoint before changing live data."
                )
                self._promote.setText("Validate & Promote")
            self._primary.setText("Resume Safe Load")
            self._promote.setEnabled(True)
        else:
            self._status.setText("Ready to collect cluster data.")
            self._status_detail.setText(
                "Loads are staged beside the live tables and promoted only after "
                "they complete and validate."
            )
            self._primary.setText("Start Safe Load")
            self._promote.setEnabled(False)
        if not self._configuration_ready:
            self._status.setText("Cluster credentials are required before loading.")
            self._status_detail.setText(
                "Set the server address, username, and password for every enabled "
                "cluster. Credentials stay encrypted for this Windows account."
            )
            self._primary.setEnabled(False)
            # Promotion uses only the already-staged local DuckDB snapshot; it
            # remains valid even when Redshift credentials are unavailable.
            if status not in {"loaded", "loading"}:
                self._promote.setEnabled(False)
            self._credentials.setText("Set Usernames & Passwords — Required")
        else:
            self._primary.setEnabled(True)
            self._credentials.setText("Set Usernames & Passwords")
        self._set_cancel_mode(False)

    def _source_profiles(self) -> list[dict[str, object]]:
        """Return display-only cluster readiness without exposing credentials."""
        runner_module = None
        try:
            import runner

            runner._load_dotenv_if_present()
            runner_module = runner
        except Exception:
            pass
        try:
            from ..secrets_store import session_secret, session_secrets

            protected_keys = set(session_secrets())
        except Exception:
            session_secret = lambda _name, default=None: default
            protected_keys = set()

        keys = set(os.environ) | protected_keys
        active_prefixes = (
            runner_module._active_profile_prefixes()
            if runner_module is not None
            else None
        )
        if active_prefixes is None:
            ordinals = sorted({
                int(match.group(1))
                for key in keys
                if (
                    match := re.match(
                        r"^REDSHIFT_CONSUMER_(\d+)_",
                        str(key).upper(),
                    )
                )
            })
            include_producer = True
        else:
            ordinals = sorted(
                int(match.group(1))
                for prefix in active_prefixes
                if (
                    match := re.fullmatch(
                        r"REDSHIFT_CONSUMER_(\d+)", prefix
                    )
                )
            )
            include_producer = "REDSHIFT_PRODUCER" in active_prefixes
        profiles: list[tuple[str, str, int]] = []
        if include_producer:
            profiles.append(("REDSHIFT_PRODUCER", "Producer", 0))
        profiles.extend(
            (f"REDSHIFT_CONSUMER_{number}", "Consumer", number)
            for number in ordinals
        )
        result: list[dict[str, object]] = []
        for prefix, role, ordinal in profiles:
            legacy = role == "Producer"
            configured_keys = {
                key for key in keys
                if key == prefix or str(key).upper().startswith(prefix + "_")
            }
            if legacy:
                configured_keys |= {
                    key for key in keys
                    if str(key).upper() in {
                        "REDSHIFT_ENABLED", "REDSHIFT_HOST", "REDSHIFT_USER",
                        "REDSHIFT_PASSWORD", "REDSHIFT_NAMESPACE",
                        "REDSHIFT_FRIENDLY",
                    }
                }
            if not configured_keys:
                continue
            enabled_key = (
                "REDSHIFT_ENABLED"
                if legacy and os.environ.get("REDSHIFT_ENABLED") is not None
                else f"{prefix}_ENABLED"
            )
            enabled_raw = str(os.environ.get(enabled_key) or "").strip().lower()
            enabled = enabled_raw not in {"0", "false", "no", "off", "unchecked"}
            namespace = str(
                (
                    os.environ.get("REDSHIFT_NAMESPACE")
                    if legacy else None
                )
                or os.environ.get(f"{prefix}_NAMESPACE_ID")
                or ""
            ).strip()
            friendly = str(
                os.environ.get(f"{prefix}_DISPLAY_NAME")
                or os.environ.get(f"{prefix}_FRIENDLY")
                or (
                    os.environ.get("REDSHIFT_FRIENDLY")
                    if legacy else None
                )
                or (role if legacy else f"{role} {ordinal}")
            ).strip()

            def credential(name: str, legacy_name: str) -> bool:
                return bool(
                    str(session_secret(f"{prefix}_{name}") or "").strip()
                    or (
                        legacy
                        and str(session_secret(legacy_name) or "").strip()
                    )
                    or str(os.environ.get(f"{prefix}_{name}") or "").strip()
                    or (
                        legacy
                        and str(os.environ.get(legacy_name) or "").strip()
                    )
                )

            ready = (
                bool(namespace)
                and credential("HOST", "REDSHIFT_HOST")
                and credential("USER", "REDSHIFT_USER")
                and credential("PASSWORD", "REDSHIFT_PASSWORD")
            )
            status = (
                "Not selected"
                if not enabled
                else "Ready"
                if ready
                else "Credentials needed"
                if namespace
                else "Setup needed"
            )
            result.append({
                "prefix": prefix,
                "role": role,
                "ordinal": ordinal,
                "friendly": friendly,
                "namespace": namespace,
                "enabled": enabled,
                "ready": ready,
                "status": status,
            })
        return result

    def _populate_sources(self) -> None:
        """Show Producer, every configured Consumer, and external information together."""
        self._tree.clear()
        self._namespace_items.clear()
        self._table_items.clear()
        profiles = self._source_profiles()
        enabled_profiles = [profile for profile in profiles if profile["enabled"]]
        self._configuration_ready = bool(enabled_profiles) and all(
            bool(profile["ready"]) for profile in enabled_profiles
        )
        for profile in enabled_profiles:
            role = str(profile["role"])
            ordinal = int(profile["ordinal"])
            friendly = str(profile["friendly"])
            namespace = str(profile["namespace"])
            status = str(profile["status"])
            identity = role if role == "Producer" else f"Consumer {ordinal}"
            source_label = (
                identity
                if friendly.casefold() == identity.casefold()
                else f"{identity} — {friendly}"
            )
            item = QTreeWidgetItem([source_label, "", status])
            item.setToolTip(
                0,
                f"{profile['prefix']} | friendly_name={friendly} | "
                f"namespace_id={namespace or 'not configured'}",
            )
            self._tree.addTopLevelItem(item)
            if namespace:
                self._namespace_items[namespace] = item
            if role == "Producer":
                metadata_status = (
                    "Included in safe load"
                    if bool(profile["enabled"])
                    else "Producer not selected"
                )
                metadata_item = QTreeWidgetItem([
                    "External table metadata", "", metadata_status,
                ])
                metadata_item.setToolTip(
                    0,
                    "SVV_EXTERNAL_COLUMNS from the Producer's explicit "
                    "catalog database list. Unavailable/data-share entries "
                    "are skipped while the remaining databases continue. "
                    "It is staged and promoted with the other selected datasets.",
                )
                item.addChild(metadata_item)
                if namespace:
                    self._table_items[
                        (namespace, "external_table_metadata")
                    ] = metadata_item
        if not enabled_profiles:
            empty = QTreeWidgetItem([
                "Enabled Producer and Consumers", "", "Open credentials settings",
            ])
            self._tree.addTopLevelItem(empty)
        self._tree.expandAll()

    def _set_empty_success(self, enabled: bool) -> None:
        self._progress.setProperty("emptySuccess", "true" if enabled else "false")
        style = self._progress.style()
        style.unpolish(self._progress)
        style.polish(self._progress)

    def _mark_namespace_empty_success(self, namespace: str) -> None:
        namespace_item = self._namespace_items.get(namespace)
        if namespace_item is None:
            return
        namespace_item.setText(2, "Success — no qualifying queries")
        for column in range(namespace_item.columnCount()):
            namespace_item.setBackground(column, QBrush(_EMPTY_SUCCESS_BACKGROUND))
            namespace_item.setForeground(column, QBrush(_EMPTY_SUCCESS_FOREGROUND))

    def _edit_credentials(self) -> None:
        if self._credentials_callback is None:
            QMessageBox.information(
                self,
                "Local Credentials",
                "Open the main Infraredshift application, then use "
                "Settings → Data Sources → Edit Local Credentials.",
            )
            return
        self._credentials_callback()
        self._populate_sources()
        self.refresh_idle_state()

    def _set_cancel_mode(self, cancelling: bool) -> None:
        self._primary.setProperty("cancelMode", "true" if cancelling else "false")
        style = self._primary.style()
        style.unpolish(self._primary)
        style.polish(self._primary)

    # ---- actions ----------------------------------------------------------

    def _browse(self) -> None:
        chosen, _filter = QFileDialog.getSaveFileName(
            self, "Choose warehouse file", self._path.text().strip(),
            "DuckDB warehouse (*.duckdb)",
        )
        if chosen:
            self._path.setText(chosen)
            self.refresh_idle_state()

    def _request(self) -> LoaderRequest:
        return LoaderRequest(
            duckdb_path=self._path.text().strip(),
            days=float(self._days.value()),
            floor_seconds=None,
            resume=not self._fresh.isChecked(),
            promote=self._auto_promote.isChecked(),
            # Legacy external performance telemetry is excluded. Producer
            # SVV_EXTERNAL_COLUMNS metadata remains required by runner's plan.
            include_external=False,
            external_timeout_action="ask",
        )

    def _primary_clicked(self) -> None:
        if self._running():
            self._cancel()
            return
        if self._fresh.isChecked():
            answer = QMessageBox.question(
                self, "Start Over",
                "Discard the saved checkpoints and stage everything again from Redshift?",
            )
            if answer != QMessageBox.Yes:
                return
        self._start_process(
            build_loader_command(self._request(), json_events=True),
            operation="refresh",
        )

    def _promote_clicked(self) -> None:
        state = read_staging_state(self._path.text().strip())
        _done, _required, checkpoint_gaps = read_staging_checkpoint_details(
            self._path.text().strip(), state,
        )
        failures = read_load_failures(self._path.text().strip(), state)
        try:
            failure_count = int(state.get("failure_count", "0") or 0)
        except ValueError:
            failure_count = 0
        unresolved_count = max(failure_count, len(checkpoint_gaps))
        if state.get("status") == "loading" and unresolved_count:
            report_path = state.get("load_report", "")
            unresolved = "\n".join(
                f"• {gap}" for gap in checkpoint_gaps[:8]
            )
            error_detail = "\n".join(
                f"• {_failure_summary(failure)}"
                for failure in failures[:4]
            )
            QMessageBox.information(
                self,
                "Promotion Is Not Ready",
                "Live tables were not changed.\n\n"
                f"Incomplete checkpoints: {unresolved_count}\n"
                + (f"\nResume will retry:\n{unresolved}\n" if unresolved else "")
                + (f"\nExact Redshift errors:\n{error_detail}\n" if error_detail else "")
                + "\nClick Resume Safe Load. Completed checkpoints will be skipped."
                + (f"\n\nDetailed report:\n{report_path}" if report_path else ""),
            )
            return
        if state.get("status") not in {"loaded", "loading"}:
            QMessageBox.information(
                self,
                "No Staged Snapshot",
                "No staged snapshot is available to validate or promote.",
            )
            return
        answer = QMessageBox.question(
            self, "Promote to Live",
            "Replace the live analyzer tables with the reviewed staged snapshot? "
            "A verified backup is taken first.",
        )
        if answer != QMessageBox.Yes:
            return
        self._start_process(
            build_promote_command(self._path.text().strip(), json_events=True),
            operation="promote",
        )

    def _load_user_roster(self) -> None:
        """Load only the SVV_USER_INFO roster.

        Separate from a workload load on purpose: the roster is a small user
        list that changes rarely, and needing it should not mean waiting for a
        full capture. It is also the only way to populate the roster from the
        UI - previously the app told users to "refresh the User Roster in the
        Data Loader" when the Data Loader had no such control.
        """
        if self._running():
            QMessageBox.information(
                self,
                "Load User Roster",
                "A load is already running. Wait for it to finish, then try again.",
            )
            return
        path = self._path.text().strip()
        if not path:
            QMessageBox.information(
                self, "Load User Roster", "Choose a DuckDB file first."
            )
            return
        command = roster_command(path)
        self._append_log("Loading the user roster from SVV_USER_INFO (producer).")
        self._start_process(command, operation="roster")

    def _copy_schedule_command(self) -> None:
        request = LoaderRequest(
            duckdb_path=self._path.text().strip(),
            days=float(self._days.value()),
            floor_seconds=None,
            resume=True,
            promote=True,
            # This flag does not disable external_table_metadata.
            include_external=False,
            external_timeout_action="skip",
        )
        command = printable_loader_command(request)
        QGuiApplication.clipboard().setText(command)
        self._status_detail.setText(
            "Command copied. Paste it into Windows Task Scheduler (run as this "
            "Windows user) for unattended nightly loads."
        )

    # ---- process management ------------------------------------------------

    def _start_process(self, command: list[str], *, operation: str) -> None:
        if self._running():
            return
        process = QProcess(self)
        process.setProcessChannelMode(QProcess.SeparateChannels)
        process.setProgram(command[0])
        process.setArguments(command[1:])
        process.readyReadStandardOutput.connect(self._read_stdout)
        process.readyReadStandardError.connect(self._read_stderr)
        process.finished.connect(self._process_finished)
        process.errorOccurred.connect(self._process_error)
        self._process = process
        self._stdout_buffer = ""
        self._stderr_tail = ""
        self._cancel_requested = False
        self._operation = operation
        self._populate_sources()
        self._set_empty_success(False)
        self._progress.setValue(0)
        self._promote.setEnabled(False)
        self._primary.setText("Cancel Load")
        self._set_cancel_mode(True)
        self._status.setText(
            {
                "refresh": "Collecting cluster data…",
                "promote": "Promoting staged snapshot…",
                "roster": "Reading the user roster from SVV_USER_INFO…",
            }.get(operation, "Working…")
        )
        self._status_detail.setText("The load runs in its own process; this window stays responsive.")
        self._append_log("$ " + " ".join(command))
        process.start()

    def _cancel(self) -> None:
        process = self._process
        if process is None or process.state() == QProcess.NotRunning:
            return
        answer = QMessageBox.question(
            self, "Cancel Load",
            "Stop the running load? Completed dataset checkpoints are kept, and "
            "Resume Safe Load will continue from them.",
        )
        if answer != QMessageBox.Yes:
            return
        self._cancel_requested = True
        self._status.setText("Stopping safely…")
        self._status_detail.setText("Completed checkpoints are preserved.")
        process.terminate()
        QTimer.singleShot(3000, lambda: process.kill() if process.state() != QProcess.NotRunning else None)

    def _process_error(self, _error) -> None:
        process = self._process
        if process is None or process.state() != QProcess.NotRunning:
            return
        self._process = None
        process.deleteLater()
        self._status.setText("The loader process could not be started.")
        self._status_detail.setText(
            "Verify the application files and the Python installation, then try again."
        )
        self.refresh_idle_state()

    def _process_finished(self, exit_code: int, _status) -> None:
        process = self._process
        if process is not None:
            self._stdout_buffer += bytes(process.readAllStandardOutput()).decode("utf-8", errors="replace")
            self._stderr_tail += bytes(process.readAllStandardError()).decode("utf-8", errors="replace")
            self._consume_stdout(final=True)
            process.deleteLater()
        self._process = None
        cancelled = self._cancel_requested
        operation = self._operation
        self._operation = ""
        self.refresh_idle_state()
        if cancelled:
            self._status.setText("Load stopped safely — checkpoints preserved.")
            self._status_detail.setText("Resume Safe Load continues from the last completed dataset.")
            return
        if exit_code != 0:
            detail = self._stderr_tail.strip().splitlines()
            self._status.setText("The load did not finish.")
            self._status_detail.setText(detail[-1] if detail else f"Loader exited with code {exit_code}.")
            for line in detail[-6:]:
                self._append_log(line)
            return
        if operation == "roster":
            self._status.setText("User roster loaded.")
            self._status_detail.setText(
                "Assign to Engineer and Email User can now resolve names and "
                "addresses from the roster."
            )
            self._progress.setValue(100)
        elif operation == "promote":
            self._status.setText("Promotion complete — the staged snapshot is now live.")
            self._status_detail.setText("Open the analyzer to explore the refreshed data.")
        elif self._auto_promote.isChecked():
            self._status.setText("Load and promotion complete.")
            self._status_detail.setText("The analyzer now sees the fresh snapshot.")
        else:
            self._progress.setValue(100)

    # ---- event stream -------------------------------------------------------

    def _read_stdout(self) -> None:
        process = self._process
        if process is None:
            return
        self._stdout_buffer += bytes(process.readAllStandardOutput()).decode("utf-8", errors="replace")
        self._consume_stdout()

    def _read_stderr(self) -> None:
        process = self._process
        if process is None:
            return
        self._stderr_tail += bytes(process.readAllStandardError()).decode("utf-8", errors="replace")
        if len(self._stderr_tail) > 20_000:
            self._stderr_tail = self._stderr_tail[-20_000:]

    def _consume_stdout(self, *, final: bool = False) -> None:
        lines = self._stdout_buffer.splitlines(keepends=True)
        if not final and lines and not lines[-1].endswith(("\n", "\r")):
            self._stdout_buffer = lines.pop()
        else:
            self._stdout_buffer = ""
        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            if line.startswith(_EVENT_PREFIX):
                try:
                    payload = json.loads(line[len(_EVENT_PREFIX):])
                except (TypeError, ValueError):
                    continue
                self.handle_event(payload)
            else:
                self._append_log(line)

    def handle_event(self, payload: dict) -> None:
        """Apply one loader event to the window. Public for tests."""
        event = str(payload.get("event") or "")
        message = str(payload.get("message") or "")
        if event == "progress":
            namespace = str(payload.get("namespace_id") or "")
            table = str(payload.get("table_name") or "")
            completed = int(payload.get("completed") or 0)
            total = int(payload.get("total") or 0)
            rows = int(payload.get("duckdb_rows") or 0)
            if total > 0:
                self._progress.setValue(min(100, round(100 * completed / total)))
            if namespace and table:
                self._update_tree(namespace, table, rows, message)
            if message in _EMPTY_SUCCESS_STATUSES:
                self._set_empty_success(True)
                if (
                    message == _NO_QUALIFYING_QUERY_STATUS
                    and table == "query_history"
                ):
                    self._mark_namespace_empty_success(namespace)
            if message:
                self._status_detail.setText(f"{table or 'dataset'} — {message}")
        elif event == "external_timeout":
            self._ask_external_timeout(str(payload.get("table_name") or "external metadata"), message)
        elif event in {
            "started", "staged", "partial", "promoting", "completed", "failed",
        }:
            if message:
                self._status.setText(message)
            self._append_log(f"[{event}] {message}")
            if event in {"partial", "failed"}:
                self._log.setVisible(True)

    def _update_tree(self, namespace: str, table: str, rows: int, status: str) -> None:
        namespace_item = self._namespace_items.get(namespace)
        if namespace_item is None:
            namespace_item = QTreeWidgetItem([namespace, "", "Loading"])
            font = namespace_item.font(0)
            font.setBold(True)
            namespace_item.setFont(0, font)
            self._tree.addTopLevelItem(namespace_item)
            namespace_item.setExpanded(True)
            self._namespace_items[namespace] = namespace_item
        key = (namespace, table)
        table_item = self._table_items.get(key)
        if table_item is None:
            display_table = {
                "external_table_metadata": "External table metadata",
            }.get(table, table)
            table_item = QTreeWidgetItem([display_table, "", ""])
            namespace_item.addChild(table_item)
            self._table_items[key] = table_item
        complete = status in _COMPLETE_DATASET_STATUSES
        table_item.setText(1, f"{rows:,}" if rows or complete else "")
        table_item.setText(2, status)
        table_item.setTextAlignment(1, Qt.AlignRight | Qt.AlignVCenter)
        if status in _EMPTY_SUCCESS_STATUSES:
            for column in range(table_item.columnCount()):
                table_item.setBackground(column, QBrush(_EMPTY_SUCCESS_BACKGROUND))
                table_item.setForeground(column, QBrush(_EMPTY_SUCCESS_FOREGROUND))

    def _ask_external_timeout(self, stage: str, error: str) -> None:
        box = QMessageBox(self)
        box.setWindowTitle("External Metadata Timeout")
        box.setText(
            f"The optional external-table step timed out at {stage}.\n\n"
            "Retry it, or skip it and keep everything already collected?"
        )
        if error:
            box.setInformativeText(error)
        retry = box.addButton("Retry", QMessageBox.AcceptRole)
        box.addButton("Skip", QMessageBox.RejectRole)
        box.exec()
        process = self._process
        if process is not None and process.state() != QProcess.NotRunning:
            answer = b"retry\n" if box.clickedButton() is retry else b"skip\n"
            process.write(answer)

    # ---- misc ---------------------------------------------------------------

    def _append_log(self, line: str) -> None:
        self._log.appendPlainText(line)

    def closeEvent(self, event) -> None:
        if self._running():
            answer = QMessageBox.question(
                self, "Loader Running",
                "A load is still running in its own process. Closing this window "
                "does NOT stop it. Close anyway?",
            )
            if answer != QMessageBox.Yes:
                event.ignore()
                return
        super().closeEvent(event)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"{PRODUCT_NAME} standalone data loader window.")
    parser.add_argument("--duckdb-path", default=None, help="Target local DuckDB file.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Open the loader window without the analytics application."""
    args = _parser().parse_args(argv)
    from PySide6.QtWidgets import QApplication

    from ..bootstrap import bootstrap_application

    bootstrap_application()
    app = QApplication.instance() or QApplication([])
    app.setApplicationName(f"{PRODUCT_NAME} Loader")
    app.setStyle("Fusion")
    app.setFont(QFont("Segoe UI", 9))
    window = LoaderWindow(_default_duckdb_path(args.duckdb_path))
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
