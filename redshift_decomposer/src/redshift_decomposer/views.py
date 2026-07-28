"""View explosion: replace catalog views with their SELECT bodies."""

from __future__ import annotations

from sqlglot import exp, parse_one

from .catalog import Catalog, normalize_ident
from .identity import table_alias, table_identity
from .models import Finding


class ViewExplosionError(Exception):
    pass


def explode_views(
    tree: exp.Expression,
    catalog: Catalog,
    *,
    max_depth: int = 8,
) -> tuple[exp.Expression, list[Finding]]:
    """Inline every catalog-known view reference as a subquery.

    Late-binding views are still inlined when a definition is provided; a review
    finding is recorded. Unresolved view names that appear only as tables are left
    alone (caller may stage physical tables only).
    """
    findings: list[Finding] = []
    working = tree.copy()
    exploded: list[str] = []

    for depth in range(max_depth):
        replacements: list[tuple[exp.Table, exp.Expression, str]] = []
        for table in list(working.find_all(exp.Table)):
            identity = table_identity(table)
            if not identity:
                continue
            resolved = catalog.resolve_view(identity)
            if resolved is None:
                continue
            view_key, view_def = resolved
            if not str(view_def.sql or "").strip():
                findings.append(
                    Finding(
                        "warning",
                        f"View has empty definition: {view_key}",
                        "Cannot explode this view; leaving the reference unchanged.",
                    )
                )
                continue
            try:
                body = parse_one(view_def.sql, read="redshift")
            except Exception as exc:
                raise ViewExplosionError(
                    f"Failed to parse view {view_key}: {str(exc).splitlines()[0][:300]}"
                ) from exc
            if not isinstance(body, (exp.Select, exp.Union, exp.Subquery)):
                # Allow WITH ... SELECT
                if isinstance(body, exp.Query) or body.find(exp.Select):
                    pass
                else:
                    findings.append(
                        Finding(
                            "warning",
                            f"View body is not a SELECT: {view_key}",
                            f"Parsed as {type(body).__name__}; left unexpanded.",
                        )
                    )
                    continue
            alias = table_alias(table)
            subquery = exp.Subquery(
                this=body.copy() if not isinstance(body, exp.Subquery) else body.this.copy(),
                alias=exp.TableAlias(this=exp.to_identifier(alias or view_key.split(".")[-1])),
            )
            # If body already has an outer WITH, keep it inside the subquery select.
            replacements.append((table, subquery, view_key))
            if view_def.is_late_binding:
                findings.append(
                    Finding(
                        "review",
                        f"Late-binding view exploded: {view_key}",
                        "Late-binding views can change shape; confirm column types after expansion.",
                    )
                )

        if not replacements:
            break

        # Replace deepest-first by walking copies; use transform on identity match
        replace_map = {id(table): (subq, key) for table, subq, key in replacements}

        def _transform(node: exp.Expression) -> exp.Expression:
            hit = replace_map.get(id(node))
            if hit is None:
                return node
            subq, key = hit
            exploded.append(key)
            return subq.copy()

        # id() of nodes in the tree after copy is wrong — rebuild by identity+alias
        target_keys = {(table_identity(t), table_alias(t)): (subq, key) for t, subq, key in replacements}

        def _transform_key(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Table):
                return node
            key = (table_identity(node), table_alias(node))
            hit = target_keys.get(key)
            if hit is None:
                # also try bare identity
                for (ident, alias), value in target_keys.items():
                    if table_identity(node) == ident or (
                        catalog.resolve_view(table_identity(node))
                        and catalog.resolve_view(table_identity(node))[0] == value[1]
                    ):
                        # only if alias matches or table has no special alias
                        if alias == table_alias(node) or normalize_ident(node.name) == alias:
                            hit = value
                            break
            if hit is None:
                return node
            subq, view_key = hit
            exploded.append(view_key)
            return subq.copy()

        working = working.transform(_transform_key)
    else:
        findings.append(
            Finding(
                "warning",
                "View explosion depth limit reached",
                f"Stopped after {max_depth} expansion passes. Nested views may remain.",
            )
        )

    if exploded:
        unique = list(dict.fromkeys(exploded))
        findings.append(
            Finding(
                "info",
                "Views exploded",
                "Inlined: " + ", ".join(unique),
            )
        )
    return working, findings
