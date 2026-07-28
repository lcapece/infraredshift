import os

import pandas as pd

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from analyzer.widgets.cluster_dashboard import TABLE_IMPACT_COLS, _TablePage


def test_table_blast_radius_keeps_external_bottom_horizontal_scrollbar_visible():
    app = QApplication.instance() or QApplication([])
    _ = app
    page = _TablePage("TABLE BLAST RADIUS", TABLE_IMPACT_COLS)
    page.resize(720, 240)
    row = {column: index for index, column in enumerate(TABLE_IMPACT_COLS)}
    row["query_ids"] = ",".join(str(100000 + i) for i in range(120))
    frame = pd.DataFrame([row.copy() for _ in range(8)])

    page.set_dataframe(frame)
    page.show()
    app.processEvents()

    assert page._bottom_scroll.isVisible()
    assert page._bottom_scroll.maximum() > 0
    assert page._bottom_scroll.geometry().bottom() <= page.rect().bottom()

    target = min(80, page._bottom_scroll.maximum())
    page._bottom_scroll.setValue(target)
    app.processEvents()

    assert page._table.horizontalScrollBar().value() == target
