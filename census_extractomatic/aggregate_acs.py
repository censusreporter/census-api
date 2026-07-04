"""Database-independent orchestration for aggregating ACS tables across a set of
component geographies that overlap a user's arbitrary geometry.

The spatial selection and data fetching live in the Flask API layer; this module
takes already-fetched component data plus table metadata and produces aggregated
estimates and margins of error, refusing to aggregate statistics that are not
additive across geographies (medians, means, per-capita, index values).
"""
from census_extractomatic.moe import aggregate_count

# Keywords in a table or column title that mark a statistic as NOT additive
# across geographies. "Aggregate" is deliberately excluded: aggregate totals
# (e.g. Aggregate Household Income) are plain sums and CAN be combined.
_NON_ADDITIVE_KEYWORDS = (
    ("median", "Medians cannot be aggregated across geographies."),
    ("mean", "Means cannot be aggregated across geographies."),
    ("per capita", "Per-capita values cannot be aggregated across geographies."),
    ("gini", "Index values cannot be aggregated across geographies."),
)


def suppression_reason(table_title, column_title):
    """Return a human-readable reason string if the given column is NOT additive
    across geographies (and therefore must not be summed), or None if it is a
    plain count that can be aggregated."""
    haystack = "{} {}".format(table_title or "", column_title or "").lower()
    for keyword, reason in _NON_ADDITIVE_KEYWORDS:
        if keyword in haystack:
            return reason
    return None


def aggregate_tables(components, metadata):
    """Aggregate ACS tables across a list of component geographies.

    ``components`` is a list (one entry per component geography) shaped like the
    per-geography portion of the ``/1.0/data/show`` response::

        {table_id: {"estimate": {col: value}, "error": {col: value}}}

    ``metadata`` maps table_id -> {"title", "denominator_column_id", "columns":
    {col: {"name": column_title}}}.

    Returns, per table::

        {table_id: {"title", "estimate": {col: sum},
                    "error": {col: moe}, "suppressed": [{"column_id", "reason"}]}}

    Count columns are summed with the Census zero-estimate rule. Non-additive
    columns (medians, means, per-capita, index) are never summed; they are listed
    in ``suppressed`` instead.
    """
    result = {}
    for table_id, table_meta in metadata.items():
        table_title = table_meta.get("title")
        columns = table_meta.get("columns", {})

        estimates = {}
        errors = {}
        suppressed = []

        for column_id, column_meta in columns.items():
            column_title = (column_meta or {}).get("name")
            reason = suppression_reason(table_title, column_title)
            if reason is not None:
                suppressed.append({"column_id": column_id, "reason": reason})
                continue

            col_estimates = []
            col_moes = []
            for component in components:
                table_data = component.get(table_id)
                if not table_data:
                    continue
                col_estimates.append(table_data["estimate"][column_id])
                col_moes.append(table_data["error"][column_id])

            est, moe = aggregate_count(col_estimates, col_moes)
            estimates[column_id] = est
            errors[column_id] = moe

        result[table_id] = {
            "title": table_title,
            "estimate": estimates,
            "error": errors,
            "suppressed": suppressed,
        }
    return result
