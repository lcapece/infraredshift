"""Clipboard-paste implementation of DataProvider."""
from __future__ import annotations

from ..normalize import parse_query_details, parse_table_info, ParseError
from .base import DataProvider, ProviderError, Snapshot


class PasteProvider:
    name = "paste"

    def __init__(self) -> None:
        self._query_details_text: str = ""
        self._table_info_text: str = ""

    def set_query_details(self, text: str) -> None:
        self._query_details_text = text or ""

    def set_table_info(self, text: str) -> None:
        self._table_info_text = text or ""

    def is_ready(self) -> bool:
        return bool(self._query_details_text.strip() and self._table_info_text.strip())

    def load(self) -> Snapshot:
        if not self.is_ready():
            raise ProviderError("paste both sys_query_details and sys_table_info first")
        try:
            qd = parse_query_details(self._query_details_text)
            ti = parse_table_info(self._table_info_text)
        except ParseError as e:
            raise ProviderError(str(e)) from e
        qid = None
        if "query_id" in qd.columns and not qd["query_id"].isna().all():
            qid = int(qd["query_id"].dropna().iloc[0])
        notes: list[str] = []
        for label, df in (("query details", qd), ("table info", ti)):
            for n in df.attrs.get("parse_notes", []) or []:
                notes.append(f"{label}: {n}")
        return Snapshot(
            query_details=qd,
            table_info=ti,
            query_id=qid,
            source_label="DBeaver paste",
            parse_notes=tuple(notes),
        )
