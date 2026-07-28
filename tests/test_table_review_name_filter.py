from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pandas as pd
from PySide6.QtWidgets import QApplication

from analyzer.widgets.cluster_dashboard import _TableReviewPage


_APP = QApplication.instance() or QApplication([])


def test_table_name_filter_is_case_insensitive_and_local() -> None:
    page = _TableReviewPage()
    page._hide_without_intersection.setChecked(False)
    page.set_dataframe(
        pd.DataFrame(
            [
                {"source_db": "dev", "schema_name": "sales", "table_name": "Fact_Orders", "tbl_rows": 20},
                {"source_db": "dev", "schema_name": "sales", "table_name": "dim_customer", "tbl_rows": 10},
            ]
        )
    )

    page._table_name_filter.setText("orders")

    assert page._model is not None
    assert page._model.rowCount() == 1
    assert page._model.row_at(0)["table_name"] == "Fact_Orders"
