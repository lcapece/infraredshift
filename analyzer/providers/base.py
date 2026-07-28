"""Provider protocol. Any source (paste, live DB, file dump) must conform."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol

import pandas as pd


class ProviderError(RuntimeError):
    pass


@dataclass(frozen=True)
class Snapshot:
    """Canonical payload consumed by diagnose and all viz widgets."""
    query_details: pd.DataFrame
    table_info: pd.DataFrame
    query_id: int | None = None
    source_label: str = "unknown"
    parse_notes: tuple[str, ...] = ()


class DataProvider(Protocol):
    name: str

    def load(self) -> Snapshot: ...
    def is_ready(self) -> bool: ...
