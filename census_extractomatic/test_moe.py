"""Unit tests for the MoE aggregation math (census_extractomatic.moe).

These are pure-function tests with no database dependency. Expected values come
from published U.S. Census Bureau worked examples in "Understanding and Using
American Community Survey Data: What All Data Users Need to Know" (Appendix on
calculating measures of error for derived estimates).
"""
import math

from census_extractomatic.moe import (
    aggregate_count,
    derived_proportion,
    derived_ratio,
)


def test_aggregate_count_canonical_handbook_example():
    """Census handbook: aggregating people 60 to 66 years old.

        60 and 61 years: estimate 1,626, MoE 254
        62 to 64 years:  estimate 1,244, MoE 209
        65 and 66 years:  estimate   796, MoE 187

    Aggregate estimate = 3,666; aggregate MoE = sqrt(254^2+209^2+187^2) ~= 378.
    """
    est, moe = aggregate_count(
        estimates=[1626, 1244, 796],
        moes=[254, 209, 187],
    )
    assert est == 3666
    assert round(moe) == 378
    assert math.isclose(moe, math.sqrt(254**2 + 209**2 + 187**2))


def test_aggregate_count_zero_estimate_rule():
    """Census rule: when summing counts where one or more components are zero,
    include only the SINGLE LARGEST MoE among the zero-estimate components (drop
    the rest) to avoid overstating the aggregate MoE.

        estimates [0, 0, 25], MoEs [8, 12, 40]
        -> keep the nonzero MoE (40) plus only the largest zero MoE (12)
        -> MoE = sqrt(12^2 + 40^2)
    """
    est, moe = aggregate_count(estimates=[0, 0, 25], moes=[8, 12, 40])
    assert est == 25
    assert math.isclose(moe, math.sqrt(12**2 + 40**2))


def test_aggregate_count_all_zero_estimates():
    """All components zero: only the single largest MoE survives."""
    est, moe = aggregate_count(estimates=[0, 0, 0], moes=[8, 12, 10])
    assert est == 0
    assert moe == 12


def test_derived_proportion():
    """Proportion p = X/Y where the numerator X is a subset of denominator Y.

        MoE_p = sqrt(MoE_X^2 - p^2 * MoE_Y^2) / Y

    X=100 (MoE 20), Y=500 (MoE 30) -> p=0.2,
    MoE_p = sqrt(400 - 0.04*900)/500 = sqrt(364)/500 ~= 0.03815757
    """
    p, moe = derived_proportion(num=100, num_moe=20, den=500, den_moe=30)
    assert math.isclose(p, 0.2)
    assert math.isclose(moe, 0.038157568056677825)


def test_derived_proportion_negative_radicand_falls_back_to_ratio():
    """When MoE_X^2 - p^2*MoE_Y^2 < 0, fall back to the ratio formula
    (use + instead of -) to avoid taking sqrt of a negative number.

    X=100 (MoE 5), Y=500 (MoE 100) -> radicand 25 - 400 = -375 < 0,
    MoE = sqrt(25 + 0.04*10000)/500 = sqrt(425)/500 ~= 0.04123106
    """
    p, moe = derived_proportion(num=100, num_moe=5, den=500, den_moe=100)
    assert math.isclose(p, 0.2)
    assert math.isclose(moe, 0.04123105625617661)


def test_derived_ratio():
    """Ratio R = X/Y where X is NOT a subset of Y.

        MoE_R = sqrt(MoE_X^2 + R^2 * MoE_Y^2) / Y

    X=100 (MoE 20), Y=50 (MoE 10) -> R=2.0,
    MoE_R = sqrt(400 + 4*100)/50 = sqrt(800)/50 ~= 0.56568542
    """
    r, moe = derived_ratio(num=100, num_moe=20, den=50, den_moe=10)
    assert math.isclose(r, 2.0)
    assert math.isclose(moe, 0.5656854249492381)
