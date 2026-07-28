from redshift_decomposer import Catalog, TableStats, ViewDef


def test_resolve_suffix():
    cat = Catalog(
        tables={
            "analytics.public.orders": TableStats(columns={"id": "INT"}, rows=10),
        }
    )
    hit = cat.resolve_table("public.orders")
    assert hit is not None
    assert hit[0] == "analytics.public.orders"


def test_view_and_table_keys_normalized():
    cat = Catalog(
        tables={"Analytics.Public.T": TableStats(columns={"A": "INT"})},
        views={"Analytics.Reporting.V": ViewDef(sql="SELECT 1 AS a")},
    )
    assert "analytics.public.t" in cat.tables
    assert cat.resolve_view("reporting.v") is not None
