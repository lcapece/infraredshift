from __future__ import annotations

import duckdb
import pandas as pd
from PySide6.QtWidgets import QApplication, QPushButton

from analyzer.spectrum_view_cache import identify_possible_spectrum_views
from analyzer.widgets.triage_home import TriagePage


_APP = QApplication.instance() or QApplication([])


def test_spectrum_view_scan_uses_producer_metadata_and_reuses_sidecar_cache(
    tmp_path,
) -> None:
    warehouse = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(warehouse))
    try:
        con.execute(
            "CREATE TABLE external_table_metadata "
            "(source_db VARCHAR, schema_name VARCHAR, table_name VARCHAR)"
        )
        con.execute(
            "INSERT INTO external_table_metadata VALUES "
            "('dev', 'spectrum', 'events')"
        )
    finally:
        con.close()
    views = pd.DataFrame([
        {
            "source_db": "dev",
            "schema_name": "public",
            "view_name": "external_events_v",
            "source_definition": (
                "SELECT * FROM spectrum.events WHERE event_date >= current_date - 7"
            ),
        },
        {
            "source_db": "dev",
            "schema_name": "public",
            "view_name": "local_orders_v",
            "source_definition": "SELECT * FROM sales.orders",
        },
    ])

    first = identify_possible_spectrum_views(
        warehouse, "snapshot-1", views
    )
    second = identify_possible_spectrum_views(
        warehouse, "snapshot-1", views
    )

    assert first.view_count == 2
    assert first.external_table_count == 1
    assert first.analyzed_count == 2
    assert first.cache_hits == 0
    assert first.candidates["view_name"].tolist() == ["external_events_v"]
    assert "dev.spectrum.events" in first.candidates.iloc[0][
        "matched_external_objects"
    ]
    assert second.cache_hits == 2
    assert second.analyzed_count == 0
    assert second.cache_file.is_file()


def test_spectrum_view_button_is_opt_in_and_cannot_change_loader_tables() -> None:
    page = TriagePage()
    buttons = {
        button.text(): button
        for button in page.findChildren(QPushButton)
    }

    button = buttons["Identify Possible Spectrum Views"]
    assert "does not query Redshift" in button.toolTip()
    assert "cannot change staged or live tables" in button.toolTip()
    assert page._spectrum_scan_thread is None
    page.deleteLater()
