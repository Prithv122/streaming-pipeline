from __future__ import annotations

import time
from pathlib import Path

import pytest

from streampipe.events import (
    STATUSES,
    ZONES,
    GenConfig,
    OrderEvent,
    generate,
    read_log,
    to_cents,
    write_log,
)

SMALL = GenConfig(seed=3, duration_s=600.0)


def test_same_seed_is_identical_and_other_seed_differs() -> None:
    assert generate(SMALL) == generate(SMALL)
    assert generate(SMALL) != generate(GenConfig(seed=4, duration_s=600.0))


def test_log_order_invariant() -> None:
    arrivals = generate(SMALL)
    keys = [(a.arrival_time, a.event.order_id) for a in arrivals]
    assert keys == sorted(keys)


def test_event_fields_are_valid() -> None:
    arrivals = generate(SMALL)
    assert len(arrivals) > 0
    ids = [a.event.order_id for a in arrivals]
    assert len(set(ids)) == len(ids)
    by_event_time = sorted(arrivals, key=lambda a: (a.event.event_time, a.event.order_id))
    assert [a.event.order_id for a in by_event_time] == [
        f"ord-{n:06d}" for n in range(1, len(ids) + 1)
    ]
    for a in arrivals:
        e = a.event
        assert a.arrival_time >= e.event_time
        assert 0.0 <= e.event_time < SMALL.duration_s
        assert e.zone in ZONES
        assert e.status in STATUSES
        assert e.amount >= 50.0
        assert round(e.amount, 2) == e.amount
        assert round(e.event_time, 3) == e.event_time


def test_late_share_matches_config() -> None:
    cfg = GenConfig(seed=11, late_share=0.2, max_delay_s=30.0, ontime_jitter_s=2.0)
    arrivals = generate(cfg)
    late = sum(a.arrival_time - a.event.event_time > cfg.ontime_jitter_s + 1e-3 for a in arrivals)
    assert abs(late / len(arrivals) - 0.2) < 0.01


def test_no_late_events_stay_within_jitter() -> None:
    cfg = GenConfig(seed=5, late_share=0.0, duration_s=900.0)
    for a in generate(cfg):
        assert a.arrival_time - a.event.event_time <= cfg.ontime_jitter_s + 1e-9


def test_all_late_delays_in_late_range() -> None:
    cfg = GenConfig(seed=5, late_share=1.0, duration_s=300.0, ontime_jitter_s=2.0, max_delay_s=9.0)
    for a in generate(cfg):
        delay = a.arrival_time - a.event.event_time
        assert 2.0 - 1e-3 <= delay <= 9.0 + 1e-3


def test_burst_density() -> None:
    cfg = GenConfig(seed=2, duration_s=6000.0)
    burst = calm = 0
    for a in generate(cfg):
        if a.event.event_time % cfg.burst_every_s < cfg.burst_len_s:
            burst += 1
        else:
            calm += 1
    burst_time = (cfg.duration_s / cfg.burst_every_s) * cfg.burst_len_s
    calm_time = cfg.duration_s - burst_time
    ratio = (burst / burst_time) / (calm / calm_time)
    assert ratio == pytest.approx(cfg.burst_mult, rel=0.1)
    assert calm / calm_time == pytest.approx(cfg.rate_per_s, rel=0.05)


def test_no_bursts_when_mult_is_one() -> None:
    cfg = GenConfig(seed=2, duration_s=3000.0, burst_mult=1.0)
    assert len(generate(cfg)) / cfg.duration_s == pytest.approx(cfg.rate_per_s, rel=0.05)


def test_zone_and_status_roughly_uniform() -> None:
    arrivals = generate(GenConfig(seed=9, duration_s=1800.0))
    for zone in ZONES:
        share = sum(a.event.zone == zone for a in arrivals) / len(arrivals)
        assert share == pytest.approx(1 / len(ZONES), abs=0.02)
    for status in STATUSES:
        share = sum(a.event.status == status for a in arrivals) / len(arrivals)
        assert share == pytest.approx(1 / len(STATUSES), abs=0.02)


def test_custom_zones() -> None:
    arrivals = generate(GenConfig(seed=1, duration_s=120.0, zones=("a", "b")))
    assert {a.event.zone for a in arrivals} == {"a", "b"}


def test_default_config_is_fast_and_sized() -> None:
    start = time.perf_counter()
    arrivals = generate(GenConfig())
    elapsed = time.perf_counter() - start
    assert 19_000 < len(arrivals) < 23_000
    assert elapsed < 2.0  # gross-regression guard; ~0.3-0.4 s on an idle machine


@pytest.mark.parametrize(
    "kwargs",
    [
        {"duration_s": 0.0},
        {"rate_per_s": 0.0},
        {"burst_every_s": 0.0},
        {"burst_len_s": 700.0},
        {"burst_len_s": -1.0},
        {"burst_mult": 0.0},
        {"late_share": 1.5},
        {"late_share": -0.1},
        {"ontime_jitter_s": -1.0},
        {"max_delay_s": 1.0, "ontime_jitter_s": 2.0, "late_share": 0.1},
        {"zones": ()},
        {"zones": ("a", "a")},
    ],
)
def test_invalid_config_rejected(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        GenConfig(**kwargs)


def test_small_max_delay_ok_without_late_events() -> None:
    GenConfig(late_share=0.0, max_delay_s=1.0, ontime_jitter_s=2.0)


def test_log_round_trip(tmp_path: Path) -> None:
    arrivals = generate(GenConfig(seed=7, duration_s=120.0))
    path = tmp_path / "sub" / "log.jsonl"
    write_log(path, arrivals)
    assert read_log(path) == arrivals


def test_to_cents_and_event_round_trip() -> None:
    assert to_cents(0.1 + 0.2) == 30
    e = OrderEvent("ord-000001", 1.5, "north", 99.99, "placed")
    assert OrderEvent.from_dict(e.to_dict()) == e
