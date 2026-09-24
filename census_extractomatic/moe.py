"""Margin-of-error (MoE) aggregation math for combining ACS estimates across
multiple census geographies.

Pure functions, no database or Flask dependency, so they can be unit-tested in
isolation. Formulas follow the U.S. Census Bureau guidance in "Understanding and
Using American Community Survey Data: What All Data Users Need to Know".
"""
import math


def aggregate_count(estimates, moes, weights=None):
    """Aggregate (sum) a set of count estimates and propagate their MoE.

    Unweighted (whole-geography):
        estimate = sum(estimates)
        moe      = sqrt(sum(moe_i ** 2))

    Weighted (overlap apportionment), when ``weights`` is given:
        estimate = sum(w_i * est_i)
        moe      = sqrt(sum((w_i * moe_i) ** 2))
    Scaling an estimate by a constant scales its MoE by the same constant, so the
    MoE math stays valid; the modelling caveat (does the weight really reflect
    how the variable is distributed?) lives with the caller.

    Census zero-estimate rule: when one or more components have an estimate of
    zero, only the single largest (scaled) MoE among those zero-estimate
    components is kept so the aggregate MoE is not overstated.

    Returns a (estimate, moe) tuple.
    """
    if weights is None:
        weights = [1] * len(estimates)

    total = sum(w * e for w, e in zip(weights, estimates))

    scaled_moes = [w * m for w, m in zip(weights, moes)]
    nonzero_moes = [sm for e, sm in zip(estimates, scaled_moes) if e != 0]
    zero_moes = [sm for e, sm in zip(estimates, scaled_moes) if e == 0]
    kept_moes = list(nonzero_moes)
    if zero_moes:
        kept_moes.append(max(zero_moes))

    moe = math.sqrt(sum(m ** 2 for m in kept_moes))
    return total, moe


def derived_ratio(num, num_moe, den, den_moe):
    """MoE of a ratio R = num / den where num is NOT a subset of den.

        MoE_R = sqrt(num_moe^2 + R^2 * den_moe^2) / den

    Returns a (ratio, moe) tuple.
    """
    ratio = num / den
    moe = math.sqrt(num_moe ** 2 + ratio ** 2 * den_moe ** 2) / den
    return ratio, moe


def derived_proportion(num, num_moe, den, den_moe):
    """MoE of a proportion p = num / den where num IS a subset of den.

        MoE_p = sqrt(num_moe^2 - p^2 * den_moe^2) / den

    If the radicand is negative, the Census Bureau instructs falling back to the
    ratio formula (use + instead of -) to avoid a sqrt of a negative number.

    Returns a (proportion, moe) tuple.
    """
    p = num / den
    radicand = num_moe ** 2 - p ** 2 * den_moe ** 2
    if radicand < 0:
        return derived_ratio(num, num_moe, den, den_moe)
    moe = math.sqrt(radicand) / den
    return p, moe
