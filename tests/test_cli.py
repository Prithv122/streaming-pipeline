import importlib
import json
from pathlib import Path

import pytest

from streampipe import cli


def test_parser_defaults_and_required_args() -> None:
    p = cli._build_parser()
    args = p.parse_args(["gen", "--out", "x.jsonl"])
    assert (args.seed, args.late_share, args.max_delay, args.duration) == (0, 0.05, 30.0, 3600.0)
    args = p.parse_args(["replay", "--log", "x.jsonl", "--mode", "revise", "--lateness", "15"])
    assert (args.mode, args.lateness, args.out) == ("revise", 15.0, None)
    args = p.parse_args(["sweep", "--small"])
    assert args.small and args.out is None
    args = p.parse_args(["consume"])
    assert args.bootstrap == "localhost:19092"
    assert (args.topic_in, args.topic_out, args.group_id) == (
        "orders.v1",
        "windows.v1",
        "streampipe",
    )
    args = p.parse_args(["bridge"])
    assert args.url == "http://127.0.0.1:8000/publish/windows"
    args = p.parse_args(["produce", "--log", "x.jsonl", "--pace", "60"])
    assert (args.topic, args.pace) == ("orders.v1", 60.0)
    with pytest.raises(SystemExit):
        p.parse_args(["gen"])
    with pytest.raises(SystemExit):
        p.parse_args([])
    with pytest.raises(SystemExit):
        p.parse_args(["replay", "--log", "x", "--mode", "nope"])


def test_gen_then_replay(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    log = tmp_path / "log.jsonl"
    assert cli.main(["gen", "--seed", "3", "--duration", "300", "--out", str(log)]) == 0
    lines = log.read_text().splitlines()
    assert lines and "arrival_time" in json.loads(lines[0])
    capsys.readouterr()

    out = tmp_path / "records.jsonl"
    argv = ["replay", "--log", str(log), "--mode", "revise", "--lateness", "30", "--out", str(out)]
    assert cli.main(argv) == 0
    text = capsys.readouterr().out
    assert "exact_rate_final" in text
    records = [json.loads(line) for line in out.read_text().splitlines()]
    assert records and {"window_start", "revision", "final"} <= set(records[0])


def test_gen_is_deterministic(tmp_path: Path) -> None:
    for name in ("a", "b"):
        cli.main(["gen", "--seed", "1", "--duration", "120", "--out", str(tmp_path / name)])
    assert (tmp_path / "a").read_bytes() == (tmp_path / "b").read_bytes()


def test_sweep_small(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "res" / "sweep.csv"
    assert cli.main(["sweep", "--small", "--out", str(out)]) == 0
    assert out.exists()
    assert (tmp_path / "res" / "sweep_pooled.csv").exists()
    assert (tmp_path / "res" / "sweep_claim.json").exists()
    text = capsys.readouterr().out
    assert "smoke run, not the registered result" in text
    assert "claim passed" not in text


def test_sweep_small_defaults_to_scratch_never_the_registered_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert cli.main(["sweep", "--small"]) == 0
    scratch = tmp_path / "results" / "scratch"
    assert (scratch / "sweep_small.csv").exists()
    assert (scratch / "sweep_small_pooled.csv").exists()
    assert (scratch / "sweep_small_claim.json").exists()
    assert not (tmp_path / "results" / "sweep.csv").exists()
    assert list((tmp_path / "results").glob("sweep*")) == []
    claim = json.loads((scratch / "sweep_small_claim.json").read_text())
    assert (claim["grid"], claim["registered_grid"]) == ("small", False)
    assert "smoke run, not the registered result" in capsys.readouterr().out


def test_sweep_full_grid_defaults_to_results_and_reports_the_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from streampipe import evalrun

    # A tiny stand-in for the registered grid, so the test stays fast.
    tiny = evalrun.Grid((0.05,), (30.0,), (30.0,), ("wait",), (0,), duration_s=120.0)
    monkeypatch.setattr(evalrun, "FULL_GRID", tiny)
    monkeypatch.chdir(tmp_path)
    assert cli.main(["sweep"]) == 0
    assert (tmp_path / "results" / "sweep.csv").exists()
    claim = json.loads((tmp_path / "results" / "sweep_claim.json").read_text())
    assert (claim["grid"], claim["registered_grid"]) == ("full", True)
    assert "pre-registered claim passed" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv",
    [
        ["produce", "--log", "x.jsonl"],
        ["consume"],
        ["bridge"],
    ],
)
def test_broker_commands_explain_the_missing_extra(
    argv: list[str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    real_import = importlib.import_module

    def fake_import(name: str, package: str | None = None):
        if name in ("streampipe.kafka_io", "streampipe.bridge"):
            raise ModuleNotFoundError("No module named 'confluent_kafka'", name="confluent_kafka")
        return real_import(name, package)

    monkeypatch.setattr(cli.importlib, "import_module", fake_import)
    assert cli.main(argv) == 2
    assert "uv sync --extra kafka" in capsys.readouterr().err


def test_bridge_passes_token_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    class FakeBridge:
        @staticmethod
        def run_bridge(bootstrap, url, topic="windows.v1", token=None, idle_timeout_s=10.0):
            calls.update(bootstrap=bootstrap, url=url, topic=topic, token=token)
            return 7

    monkeypatch.setenv("PUBLISH_TOKEN", "secret")
    monkeypatch.setattr(cli, "_import_edge", lambda name: FakeBridge)
    assert cli.main(["bridge"]) == 0
    assert calls["token"] == "secret"
    assert calls["bootstrap"] == "localhost:19092"
