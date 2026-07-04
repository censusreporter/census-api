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


def select_components(rows, threshold=0.0):
    """Filter spatial-intersection rows down to the component geographies that
    should be included in the aggregate.

    ``rows`` are mappings with ``full_geoid``, ``display_name`` and
    ``area_frac`` (the fraction of the geography's own area inside the shape).
    A geography is included when ``area_frac >= threshold``. With the default
    threshold of 0.0, every geography the shape intersects is kept.

    Returns a list of {"geoid", "name", "area_frac"} dicts, order preserved.
    """
    components = []
    for row in rows:
        if row["area_frac"] >= threshold:
            components.append({
                "geoid": row["full_geoid"],
                "name": row["display_name"],
                "area_frac": row["area_frac"],
            })
    return components


def aggregate_tables(components, metadata):
    """Aggregate ACS tables across a list of component geographies.

    Each component carries its own optional ``weight`` alongside its data, so the
    weight can never drift out of alignment with the geography it belongs to::

        {"weight": 0.37,  # optional; defaults to 1 (whole-geography)
         "data": {table_id: {"estimate": {col: value}, "error": {col: value}}}}

    The ``data`` portion is shaped like the per-geography portion of the
    ``/1.0/data/show`` response. When a weight is given, that geography's
    contribution is apportioned by it (overlap fraction); a component skipped for
    missing data naturally drops its weight along with it.

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
            col_weights = []
            for component in components:
                table_data = component["data"].get(table_id)
                if not table_data:
                    continue
                est_value = table_data["estimate"][column_id]
                moe_value = table_data["error"][column_id]
                # A release may have no value for a column in a given geography;
                # leave that component out of this column rather than crash.
                if est_value is None or moe_value is None:
                    continue
                col_estimates.append(est_value)
                col_moes.append(moe_value)
                col_weights.append(component.get("weight", 1))

            est, moe = aggregate_count(col_estimates, col_moes, weights=col_weights)
            estimates[column_id] = est
            errors[column_id] = moe

        result[table_id] = {
            "title": table_title,
            "estimate": estimates,
            "error": errors,
            "suppressed": suppressed,
        }
    return result
