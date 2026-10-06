"""Compile catalog-grounded logical read plans into parameterized PostgreSQL SQL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CompiledFederatedQuery:
    sql: str
    params: list[Any]
    source_key: str
    limit: int


def _quote(identifier: str) -> str:
    value = str(identifier or "").strip()
    if not value:
        raise ValueError("SQL identifier must not be empty")
    return '"' + value.replace('"', '""') + '"'


def _columns(table: dict[str, Any]) -> dict[str, str]:
    metadata = table.get("metadata") if isinstance(table.get("metadata"), dict) else {}
    return {
        str(column.get("column_name") or "").strip().lower(): str(column.get("column_name") or "").strip()
        for column in metadata.get("columns") or []
        if isinstance(column, dict) and str(column.get("column_name") or "").strip()
    }


def _table_by_logical_name(catalog: list[dict[str, Any]], logical_name: str) -> dict[str, Any]:
    requested = str(logical_name or "").strip().lower()
    matches = []
    for table in catalog:
        metadata = table.get("metadata") if isinstance(table.get("metadata"), dict) else {}
        categories = {str(item).strip().lower() for item in metadata.get("query_categories") or []}
        table_name = str(table.get("table_name") or "").strip().lower()
        if requested == table_name or requested in categories:
            matches.append(table)
    if len(matches) != 1:
        raise ValueError(f"Logical table '{logical_name}' is not uniquely resolved by the source catalog")
    return matches[0]


def _resolve_column(table: dict[str, Any], logical_name: str) -> str:
    available = _columns(table)
    requested = str(logical_name or "").strip().lower()
    if requested not in available:
        raise ValueError(f"Column '{logical_name}' is not present in catalogued table '{table.get('table_name')}'")
    return available[requested]


def _foreign_key_join(left: dict[str, Any], right: dict[str, Any]) -> tuple[str, str]:
    left_table = str(left.get("table_name") or "").strip().lower()
    right_table = str(right.get("table_name") or "").strip().lower()
    left_metadata = left.get("metadata") if isinstance(left.get("metadata"), dict) else {}
    right_metadata = right.get("metadata") if isinstance(right.get("metadata"), dict) else {}

    for fk in left_metadata.get("foreign_keys") or []:
        if isinstance(fk, dict) and str(fk.get("foreign_table_name") or "").strip().lower() == right_table:
            return str(fk.get("column_name") or ""), str(fk.get("foreign_column_name") or "")
    for fk in right_metadata.get("foreign_keys") or []:
        if isinstance(fk, dict) and str(fk.get("foreign_table_name") or "").strip().lower() == left_table:
            return str(fk.get("foreign_column_name") or ""), str(fk.get("column_name") or "")
    raise ValueError(f"No catalogued foreign key connects '{left_table}' and '{right_table}'")


def compile_read_only_query(plan: dict[str, Any], catalog: list[dict[str, Any]]) -> CompiledFederatedQuery:
    """Compile a logical plan after resolving every physical identifier from catalog metadata.

    Supported plan keys: source_key, root, joins, select, filters, aggregation,
    order_by, and limit. Values remain parameters; only catalogued identifiers enter SQL.
    """
    if not isinstance(plan, dict):
        raise ValueError("Logical query plan must be an object")

    source_key = str(plan.get("source_key") or "").strip()
    root = _table_by_logical_name(catalog, str(plan.get("root") or ""))
    if source_key and str(root.get("source_key") or "").strip() != source_key:
        raise ValueError("Root table does not belong to the requested source")
    source_key = str(root.get("source_key") or "").strip()

    aliases: dict[str, tuple[dict[str, Any], str]] = {str(plan.get("root") or "").strip().lower(): (root, "t0")}
    joins_sql: list[str] = []
    for index, join_name in enumerate(plan.get("joins") or [], start=1):
        join_key = str(join_name or "").strip().lower()
        joined = _table_by_logical_name(catalog, join_key)
        if str(joined.get("source_key") or "").strip() != source_key:
            raise ValueError("Cross-source joins are not supported")
        root_left_col, right_col = _foreign_key_join(root, joined)
        joined_alias = f"t{index}"
        aliases[join_key] = (joined, joined_alias)
        table_sql = f"{_quote(str(joined.get('schema_name')))}.{_quote(str(joined.get('table_name')))}"
        joins_sql.append(
            f"JOIN {table_sql} {joined_alias} ON t0.{_quote(root_left_col)} = {joined_alias}.{_quote(right_col)}"
        )

    def field_sql(field: str) -> str:
        source_name, _, column_name = str(field or "").strip().lower().partition(".")
        if not column_name:
            source_name, column_name = str(plan.get("root") or "").strip().lower(), source_name
        table, alias = aliases.get(source_name, (None, ""))
        if table is None:
            raise ValueError(f"Field '{field}' references a table not included in the plan")
        return f"{alias}.{_quote(_resolve_column(table, column_name))}"

    select_fields = [str(item).strip() for item in plan.get("select") or [] if str(item).strip()]
    if not select_fields:
        raise ValueError("Logical query plan requires at least one select field")
    select_sql = [field_sql(field) for field in select_fields]

    aggregation = plan.get("aggregation") if isinstance(plan.get("aggregation"), dict) else {}
    aggregation_function = str(aggregation.get("function") or "").strip().upper()
    if aggregation_function:
        if aggregation_function not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError("Unsupported aggregation function")
        aggregation_field = str(aggregation.get("field") or "").strip()
        if not aggregation_field:
            raise ValueError("Aggregation requires a field")
        select_sql.append(f"{aggregation_function}({field_sql(aggregation_field)}) AS aggregate_value")

    where_sql: list[str] = []
    params: list[Any] = []
    allowed_operators = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "contains": "ILIKE"}
    for filter_spec in plan.get("filters") or []:
        if not isinstance(filter_spec, dict):
            raise ValueError("Filter must be an object")
        operator = allowed_operators.get(str(filter_spec.get("operator") or "").strip().lower())
        if not operator:
            raise ValueError("Unsupported filter operator")
        value = filter_spec.get("value")
        if value is None:
            raise ValueError("Filter value is required")
        where_sql.append(f"{field_sql(str(filter_spec.get('field') or ''))} {operator} %s")
        params.append(f"%{value}%" if operator == "ILIKE" else value)

    root_sql = f"{_quote(str(root.get('schema_name')))}.{_quote(str(root.get('table_name')))} t0"
    sql = f"SELECT {', '.join(select_sql)} FROM {root_sql} {' '.join(joins_sql)}"
    if where_sql:
        sql += " WHERE " + " AND ".join(where_sql)

    group_by = [field_sql(field) for field in aggregation.get("group_by") or []] if aggregation_function else []
    if group_by:
        sql += " GROUP BY " + ", ".join(group_by)
    order_by = str(plan.get("order_by") or "").strip()
    if order_by:
        sql += " ORDER BY " + field_sql(order_by)
    limit = max(1, min(int(plan.get("limit") or 200), 500))
    sql += " LIMIT %s"
    params.append(limit)
    return CompiledFederatedQuery(sql=sql, params=params, source_key=source_key, limit=limit)
