"""Unit tests for the DB-independent ACS aggregation orchestration
(census_extractomatic.aggregate_acs): column suppression and table aggregation
over a set of component geographies."""
import math

from census_extractomatic.aggregate_acs import suppression_reason, aggregate_tables


def test_median_column_is_suppressed():
    reason = suppression_reason(
        table_title="Median Household Income in the Past 12 Months",
        column_title="Median household income in the past 12 months",
    )
    assert reason is not None
    assert "median" in reason.lower()


def test_mean_and_per_capita_and_gini_are_suppressed():
    assert suppression_reason("Mean Travel Time to Work", "Mean travel time") is not None
    assert suppression_reason("Per Capita Income", "Per capita income") is not None
    assert suppression_reason("Gini Index of Income Inequality", "Gini index") is not None


def test_plain_count_column_is_not_suppressed():
    assert suppression_reason("Sex by Age", "Male: 5 to 9 years") is None


def test_aggregate_word_is_not_suppressed():
    """'Aggregate' totals (e.g. Aggregate Household Income) ARE summable."""
    assert suppression_reason("Aggregate Household Income", "Aggregate household income") is None


def _two_components():
    return [
        {"B01001": {"estimate": {"B01001001": 100, "B01001002": 40},
                    "error": {"B01001001": 20, "B01001002": 10}}},
        {"B01001": {"estimate": {"B01001001": 200, "B01001002": 0},
                    "error": {"B01001001": 30, "B01001002": 8}}},
    ]


_COUNT_META = {
    "B01001": {
        "title": "Sex by Age",
        "denominator_column_id": "B01001001",
        "columns": {
            "B01001001": {"name": "Total"},
            "B01001002": {"name": "Male"},
        },
    }
}


def test_aggregate_tables_sums_counts_and_propagates_moe():
    result = aggregate_tables(_two_components(), _COUNT_META)
    b = result["B01001"]
    assert b["estimate"]["B01001001"] == 300
    assert math.isclose(b["error"]["B01001001"], math.sqrt(20**2 + 30**2))


def test_aggregate_tables_applies_zero_estimate_rule_per_column():
    """B01001002 has estimates [40, 0]; the zero component keeps only its own
    (single) MoE, giving sqrt(10^2 + 8^2)."""
    result = aggregate_tables(_two_components(), _COUNT_META)
    b = result["B01001"]
    assert b["estimate"]["B01001002"] == 40
    assert math.isclose(b["error"]["B01001002"], math.sqrt(10**2 + 8**2))


def test_aggregate_tables_suppresses_median_column():
    components = [
        {"B19013": {"estimate": {"B19013001": 55000}, "error": {"B19013001": 2500}}},
        {"B19013": {"estimate": {"B19013001": 61000}, "error": {"B19013001": 3100}}},
    ]
    metadata = {
        "B19013": {
            "title": "Median Household Income in the Past 12 Months",
            "denominator_column_id": None,
            "columns": {"B19013001": {"name": "Median household income"}},
        }
    }
    result = aggregate_tables(components, metadata)
    b = result["B19013"]
    # The median column must NOT appear in the aggregated estimates...
    assert "B19013001" not in b["estimate"]
    # ...and must be reported as suppressed with a reason.
    suppressed_ids = [s["column_id"] for s in b["suppressed"]]
    assert "B19013001" in suppressed_ids
