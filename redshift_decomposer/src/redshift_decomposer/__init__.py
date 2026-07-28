"""Redshift Query Decomposer: staged, key-tuned decomposition of Redshift SQL.

PyPI distribution: ``redshift-query-decomposer``; import name stays
``redshift_decomposer``.
"""

from .catalog import Catalog, TableStats, ViewDef
from .decompose import decompose
from .discover import ObjectRef, discover_object_refs
from .fetch import (
    catalog_from_databasix_frames,
    catalog_from_rows,
    catalog_relation,
    fetch_catalog_for_refs,
    fetch_catalog_for_sql,
    normalize_metadata_schema,
)
from .models import DecompositionPlan, Finding, Stage
from .repository import (
    BuildReport,
    TableRepository,
    build_table_repository,
    fetch_catalog_with_repository,
    list_local_databases,
)
from .triage import TriageReport, TriageSignal, assess_decomposability

__all__ = [
    "Catalog",
    "TableStats",
    "ViewDef",
    "Stage",
    "Finding",
    "DecompositionPlan",
    "ObjectRef",
    "discover_object_refs",
    "fetch_catalog_for_sql",
    "fetch_catalog_for_refs",
    "catalog_from_rows",
    "catalog_from_databasix_frames",
    "catalog_relation",
    "normalize_metadata_schema",
    "TableRepository",
    "BuildReport",
    "build_table_repository",
    "fetch_catalog_with_repository",
    "list_local_databases",
    "decompose",
    "assess_decomposability",
    "TriageReport",
    "TriageSignal",
]

__version__ = "0.2.3"
