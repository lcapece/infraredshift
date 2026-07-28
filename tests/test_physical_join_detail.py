import pandas as pd

from analyzer.widgets.cluster_dashboard import _join_detail_text


def test_join_detail_uses_physical_table_metadata_when_alias_is_a_cte() -> None:
    join = pd.Series({
        "aliases": "recent, customer",
        "join_columns": "recent.customer_id, customer.customer_id",
        "left_physical_sources": "dev.raw.authorization_fact.customer_id",
        "right_physical_sources": "dev.raw.customer.customer_id",
        "physical_column_pairs": "dev.raw.authorization_fact.customer_id = dev.raw.customer.customer_id",
        "condition": "recent.customer_id = customer.customer_id",
    })
    tables = pd.DataFrame([
        {
            "alias": "a", "source_db": "dev", "schema_name": "raw",
            "table_name": "authorization_fact", "object_type": "table",
            "diststyle": "KEY(customer_id)", "sortkey1": "transaction_date",
        },
        {
            "alias": "c", "source_db": "dev", "schema_name": "raw",
            "table_name": "customer", "object_type": "table",
            "diststyle": "KEY(customer_id)", "sortkey1": "customer_id",
        },
    ])

    text = _join_detail_text(join, tables, pd.Series(dtype=object))

    assert "dev.raw.authorization_fact" in text
    assert "dev.raw.customer" in text
    assert "Join columns: customer_id" in text
    assert "No physical table metadata" not in text
