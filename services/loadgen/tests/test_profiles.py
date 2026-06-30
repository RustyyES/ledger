"""Tests for the behavioural model.

These matter more than they look. `profiles.py` is what makes this a data
*generator* rather than a data *faker* -- and if its shape silently flattens,
every downstream test that depends on the shape becomes vacuous. It still
passes; it just stops testing anything.

Concretely: with no weekend dip, no cohort decay and no late refunds, the
seasonality models, the retention model and the entire incremental lookback
design are all exercised against uniform noise that cannot break them.
"""

from __future__ import annotations

import datetime as dt

import profiles as P
import pytest

# --------------------------------------------------------------------------- #
# Daily and weekly shape
# --------------------------------------------------------------------------- #


def test_hour_curve_is_a_normalised_distribution():
    assert len(P.HOUR_OF_DAY_CURVE) == 24
    assert pytest.approx(sum(P.HOUR_OF_DAY_CURVE), abs=1e-9) == 1.0
    assert all(w > 0 for w in P.HOUR_OF_DAY_CURVE), "a zero hour would never emit an order"


def test_hour_curve_is_bimodal_with_lunch_and_evening_peaks():
    """A flat curve makes freshness dashboards lie: nothing is ever quiet, so
    the pipeline is never observed at the edges of the diurnal cycle."""
    peak = max(range(24), key=P.hour_weight)
    trough = min(range(24), key=P.hour_weight)

    assert 17 <= peak <= 21, f"evening peak expected, got hour {peak}"
    assert 1 <= trough <= 6, f"overnight trough expected, got hour {trough}"

    # A genuine lunch bump, distinct from the evening peak.
    lunch = max(range(11, 15), key=P.hour_weight)
    afternoon_dip = min(range(14, 18), key=P.hour_weight)
    assert P.hour_weight(lunch) > P.hour_weight(afternoon_dip), "no lunch peak"

    assert P.hour_weight(peak) / P.hour_weight(trough) > 10, "curve is too flat to matter"


def test_hour_weight_is_relative_to_a_flat_day():
    """1.0 means "the daily mean", which is what the live generator scales by."""
    assert pytest.approx(sum(P.hour_weight(h) for h in range(24)), abs=1e-6) == 24.0


def test_weekends_are_quieter_than_weekdays():
    weekday = sum(P.DAY_OF_WEEK_MULTIPLIER[:5]) / 5
    weekend = sum(P.DAY_OF_WEEK_MULTIPLIER[5:]) / 2
    assert weekend < weekday * 0.75, "no weekend dip -- seasonality models test nothing"


def test_friday_is_the_busiest_weekday():
    assert P.DAY_OF_WEEK_MULTIPLIER[4] == max(P.DAY_OF_WEEK_MULTIPLIER[:5])


# --------------------------------------------------------------------------- #
# Calendar effects
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "year,expected",
    [
        (2024, dt.date(2024, 11, 29)),
        (2025, dt.date(2025, 11, 28)),
        (2026, dt.date(2026, 11, 27)),
    ],
)
def test_black_friday_is_the_day_after_the_fourth_thursday(year, expected):
    assert P._black_friday(year) == expected
    assert P._black_friday(year).weekday() == 4  # Friday


def test_black_friday_carries_the_configured_spike():
    assert P.calendar_multiplier(P._black_friday(2026)) == P.BLACK_FRIDAY_SPIKE
    assert P.BLACK_FRIDAY_SPIKE > 3.0


def test_cyber_monday_follows_black_friday():
    cyber = P._black_friday(2026) + dt.timedelta(days=3)
    assert cyber.weekday() == 0  # Monday
    assert P.calendar_multiplier(cyber) > 2.0


def test_the_christmas_dead_week_is_quiet():
    assert P.calendar_multiplier(dt.date(2026, 12, 28)) < 0.6
    assert P.calendar_multiplier(dt.date(2026, 12, 22)) > 1.4


def test_an_ordinary_day_is_unmodified():
    assert P.calendar_multiplier(dt.date(2026, 3, 17)) == 1.0


# --------------------------------------------------------------------------- #
# Churn -- the shape cohort retention depends on
# --------------------------------------------------------------------------- #


def test_churn_decays_with_tenure():
    """Survivors get stickier. Without this the retention curve is a straight
    line and the cohort model demonstrates nothing."""
    hazards = [P.churn_probability(t, cohort_index=0) for t in range(12)]
    assert hazards == sorted(hazards, reverse=True), "churn does not decay with tenure"
    assert hazards[0] > hazards[-1] * 3, "decay is too shallow to be visible"


def test_newer_cohorts_churn_faster():
    """The cross-cohort signature the retention heatmap is supposed to show."""
    old = P.churn_probability(3, cohort_index=0)
    new = P.churn_probability(3, cohort_index=17)
    assert new > old, "all cohorts churn identically -- the heatmap is flat"


def test_churn_is_bounded():
    """An unbounded hazard would empty the newest cohorts instantly."""
    for tenure in range(0, 40):
        for cohort in range(0, 40):
            p = P.churn_probability(tenure, cohort)
            assert 0.0 < p <= 0.45


def test_churn_past_the_table_uses_the_floor():
    assert P.churn_probability(99, 0) == pytest.approx(P.CHURN_FLOOR, abs=1e-9)


# --------------------------------------------------------------------------- #
# The deliberate mess -- each constant maps to one messiness requirement
# --------------------------------------------------------------------------- #


def test_refund_delay_can_exceed_a_week():
    """The constraint the entire incremental lookback design exists for."""
    lo, hi = P.REFUND_DELAY_DAYS
    assert lo >= 1
    assert hi >= 14, "refunds arrive too promptly to break a naive incremental"


def test_refund_delay_stays_inside_the_configured_lookback():
    """Guards the pipeline's central assumption from the generator's side.

    The warehouse's lookback is 21 days. `date_diff` counts calendar boundaries
    rather than elapsed 24-hour periods, so the worst observable lag is the
    nominal maximum plus two. If someone raises REFUND_DELAY_DAYS past that,
    the warehouse silently starts losing refunds -- so the check lives here
    too, where the change would be made.
    """
    worst_observable = P.REFUND_DELAY_DAYS[1] + 2
    warehouse_lookback_days = 21
    assert worst_observable <= warehouse_lookback_days, (
        f"max refund delay {P.REFUND_DELAY_DAYS[1]}d implies a worst-case "
        f"{worst_observable}d calendar lag, beyond the warehouse's "
        f"{warehouse_lookback_days}d lookback. Widen "
        f"`incremental_lookback_days` in transform/dbt_project.yml first."
    )


def test_a_meaningful_share_of_orders_use_naive_local_time():
    assert 0.05 <= P.NAIVE_TIMESTAMP_SHARE <= 0.30


def test_a_meaningful_share_of_customers_are_non_usd():
    assert 0.10 <= P.NON_USD_CUSTOMER_SHARE <= 0.40


def test_every_legacy_status_variant_normalises_to_completed():
    """If a variant were added here without adding it to
    `normalise_order_status`, it would fall through to 'unknown' and silently
    leave the completed population -- making revenue DROP rather than error."""
    known = {"paid", "complete", "completed", "pending", "cancelled", "canceled", "refunded"}
    for variant in P.LEGACY_STATUS_VARIANTS:
        assert (
            variant.lower().strip() in known
        ), f"{variant!r} has no mapping in normalise_order_status"


def test_legacy_status_weights_form_a_distribution():
    assert pytest.approx(sum(P.LEGACY_STATUS_VARIANTS.values()), abs=1e-9) == 1.0


def test_relocation_rate_is_non_zero():
    """Without relocations every customer has one version and SCD2 proves nothing."""
    assert P.RELOCATION_RATE_ANNUAL > 0


def test_payment_failures_and_pending_are_both_possible():
    assert P.PAYMENT_FAILURE_RATE > 0, "no failed payments -- MRR/cash gap is untested"
    assert P.PAYMENT_PENDING_RATE > 0, "no pending payments -- null processed_at is untested"


# --------------------------------------------------------------------------- #
# Weight tables
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "name,table",
    [
        ("PLAN_WEIGHTS", P.PLAN_WEIGHTS),
        ("PAYMENT_METHOD_WEIGHTS", P.PAYMENT_METHOD_WEIGHTS),
        ("COUNTRY_WEIGHTS", P.COUNTRY_WEIGHTS),
    ],
)
def test_weight_tables_sum_to_one(name, table):
    assert pytest.approx(sum(table.values()), abs=1e-6) == 1.0, f"{name} is not normalised"
    assert all(v > 0 for v in table.values())


def test_every_country_has_a_timezone():
    """A missing timezone means naive local timestamps resolve as UTC -- wrong
    by up to 14 hours, and invisible except as future-dated orders."""
    missing = set(P.COUNTRY_WEIGHTS) - set(P.COUNTRY_TIMEZONE)
    assert not missing, f"no timezone for {sorted(missing)}"


def test_every_timezone_is_a_real_iana_name():
    from zoneinfo import ZoneInfo

    for country, tz in P.COUNTRY_TIMEZONE.items():
        try:
            ZoneInfo(tz)
        except Exception as exc:
            pytest.fail(f"{country} maps to unknown timezone {tz!r}: {exc}")


def test_currency_countries_are_countries_we_generate():
    missing = set(P.COUNTRY_CURRENCY) - set(P.COUNTRY_WEIGHTS)
    assert not missing, f"currency mapped for countries never generated: {sorted(missing)}"


# --------------------------------------------------------------------------- #
# Live mode profile
# --------------------------------------------------------------------------- #


def test_live_profile_reads_the_environment_at_construction(monkeypatch):
    """Not at import. A `default_factory` defers the read, so a test (or a
    container) that sets the variable actually gets its value."""
    monkeypatch.setenv("LOADGEN_ORDERS_PER_MINUTE", "77")
    assert P.LiveModeProfile().orders_per_minute == 77.0


def test_live_profile_defaults_are_sane():
    profile = P.LiveModeProfile()
    assert profile.orders_per_minute > 0
    assert profile.concurrency >= 1
    assert profile.max_retries >= 1
    assert profile.request_timeout_s > 0
