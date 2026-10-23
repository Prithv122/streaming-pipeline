"""Console entry point: `streampipe gen | replay | predict | sweep | produce | consume | bridge`."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import time
from pathlib import Path
from types import ModuleType

FULL_SWEEP_OUT = Path("results/sweep.csv")
SMALL_SWEEP_OUT = Path("results/scratch/sweep_small.csv")
PREDICTIONS_PATH = Path("results/predictions.json")
MAX_FAILED_SHOWN = 10

MISSING_EXTRA = (
    "streampipe: the Kafka client is not installed. Run `uv sync --extra kafka` and try again."
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="streampipe", description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("gen", help="generate a seeded event log")
    g.add_argument("--seed", type=int, default=0)
    g.add_argument("--late-share", type=float, default=0.05)
    g.add_argument("--max-delay", type=float, default=30.0)
    g.add_argument("--duration", type=float, default=3600.0)
    g.add_argument("--out", type=Path, required=True)

    r = sub.add_parser("replay", help="run a log through the window core and score it")
    r.add_argument("--log", type=Path, required=True)
    r.add_argument("--mode", choices=("wait", "revise"), default="wait")
    r.add_argument("--lateness", type=float, default=30.0)
    r.add_argument("--out", type=Path, help="write the emitted records as JSON lines")

    s = sub.add_parser("sweep", help="run the evaluation grid")
    s.add_argument("--small", action="store_true", help="short smoke grid, not a finding")
    s.add_argument(
        "--out",
        type=Path,
        default=None,
        help=f"output CSV (default {FULL_SWEEP_OUT.as_posix()}; "
        f"with --small {SMALL_SWEEP_OUT.as_posix()})",
    )
    s.add_argument("--workers", type=int, default=1)
    s.add_argument(
        "--predictions",
        type=Path,
        default=PREDICTIONS_PATH,
        help=f"analytic predictions (default {PREDICTIONS_PATH.as_posix()}); required for the "
        "full grid, optional with --small",
    )

    pd = sub.add_parser("predict", help="compute the analytic predictions for the sweep")
    pd.add_argument(
        "--out", type=Path, default=PREDICTIONS_PATH, help=f"default {PREDICTIONS_PATH.as_posix()}"
    )
    pd.add_argument(
        "--pin",
        action="store_true",
        help="also write predictions.sha256 next to the output; the registered sweep requires it",
    )

    pr = sub.add_parser("produce", help="publish a log to the orders topic")
    pr.add_argument("--log", type=Path, required=True)
    pr.add_argument("--bootstrap", default="localhost:19092")
    pr.add_argument("--topic", default="orders.v1")
    pr.add_argument("--pace", type=float, default=None, help="log seconds per wall second")

    c = sub.add_parser("consume", help="window the orders topic into the windows topic")
    c.add_argument("--mode", choices=("wait", "revise"), default="wait")
    c.add_argument("--lateness", type=float, default=30.0)
    c.add_argument("--bootstrap", default="localhost:19092")
    c.add_argument("--topic-in", default="orders.v1")
    c.add_argument("--topic-out", default="windows.v1")
    c.add_argument("--group-id", default="streampipe")
    c.add_argument("--idle-timeout", type=float, default=10.0)
    c.add_argument("--max-events", type=int, default=None)

    b = sub.add_parser("bridge", help="forward the windows topic to the dashboard hub")
    b.add_argument("--bootstrap", default="localhost:19092")
    b.add_argument("--url", default="http://127.0.0.1:8000/publish/windows")
    b.add_argument("--topic", default="windows.v1")
    b.add_argument("--idle-timeout", type=float, default=10.0)
    return p


def _cmd_gen(args: argparse.Namespace) -> int:
    from streampipe.events import GenConfig, generate, write_log

    cfg = GenConfig(
        seed=args.seed,
        duration_s=args.duration,
        late_share=args.late_share,
        max_delay_s=args.max_delay,
    )
    arrivals = generate(cfg)
    write_log(args.out, arrivals)
    print(f"wrote {len(arrivals)} events to {args.out}")
    return 0


def _cmd_replay(args: argparse.Namespace) -> int:
    from streampipe.batch import batch_windows
    from streampipe.events import read_log
    from streampipe.metrics import METRIC_KEYS, score
    from streampipe.sim import simulate

    arrivals = read_log(args.log)
    truth = batch_windows(a.event for a in arrivals)
    result = simulate(arrivals, args.mode, args.lateness)
    metrics = score(result, truth)
    print(f"mode={args.mode} lateness={args.lateness:g}s events={len(arrivals)}")
    for key in METRIC_KEYS:
        value = metrics[key]
        print(f"  {key:26s} {value:.4f}" if value % 1 else f"  {key:26s} {value:.0f}")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8", newline="\n") as f:
            for rec in result.records:
                f.write(json.dumps(rec.to_dict(), separators=(",", ":")) + "\n")
        print(f"wrote {len(result.records)} records to {args.out}")
    return 0


def _format_failed(check: dict) -> str:
    def fmt(x: float | None) -> str:
        return "n/a" if x is None else f"{x:.6g}"

    where = (
        f"share={check['late_share']:g} delay={check['max_delay_s']:g}s L={check['lateness_s']:g}s"
    )
    detail = f"observed={fmt(check['observed'])} predicted={fmt(check['predicted'])}"
    if check["accept_lo"] is not None or check["accept_hi"] is not None:
        detail += f" region=[{fmt(check['accept_lo'])}, {fmt(check['accept_hi'])}]"
    else:
        detail += f" tolerance={fmt(check['tolerance'])}"
    note = f" ({check['note']})" if check["note"] else ""
    return f"  {check['family']} {where} {detail}{note}"


def _cmd_predict(args: argparse.Namespace) -> int:
    try:
        analytic = importlib.import_module("streampipe.analytic")
    except ImportError as e:
        print(f"streampipe: cannot load the analytic model ({e})", file=sys.stderr)
        return 2
    args.out.parent.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    analytic.write_predictions(args.out)
    print(f"wrote predictions to {args.out} in {time.perf_counter() - start:.1f}s")
    if args.pin:
        from streampipe.evalrun import pin_path, write_pin

        digest = write_pin(args.out)
        print(f"pinned sha256 {digest} in {pin_path(args.out)}")
    return 0


def _cmd_sweep(args: argparse.Namespace) -> int:
    from streampipe.evalrun import FULL_GRID, SMALL_GRID, PredictionsPinError, run_grid

    grid = SMALL_GRID if args.small else FULL_GRID
    target = args.out or (SMALL_SWEEP_OUT if args.small else FULL_SWEEP_OUT)
    predictions: Path | None = args.predictions
    if args.small and predictions == PREDICTIONS_PATH and not predictions.is_file():
        predictions = None  # a smoke run needs no predictions unless asked for
    try:
        out = run_grid(grid, target, workers=args.workers, predictions_path=predictions)
    except (FileNotFoundError, PredictionsPinError) as e:
        print(f"streampipe: {e}", file=sys.stderr)
        return 2
    claim_path = out.with_name(f"{out.stem}_claim.json")
    claim = json.loads(claim_path.read_text(encoding="utf-8"))
    written = f"{out}, {out.stem}_pooled.csv and {claim_path.name}"
    if claim["primary"] is not None:
        written += f" and {out.stem}_checks.csv"
    print(f"wrote {written} ({grid.n_runs} runs)")
    primary = claim["primary"]
    if not claim["registered_grid"]:
        print("smoke run, not the registered result")
        if primary is not None:
            print(f"predictions checks: {primary['n_passed']}/{primary['n_checks']} passed")
        return 0
    print(f"C0 construction check passed: {claim['c0_construction_check']['passed']}")
    print(
        f"primary claim passed: {primary['primary_passed']} "
        f"({primary['n_passed']}/{primary['n_checks']} checks)"
    )
    failed = [c for c in primary["checks"] if not c["passed"]]
    if failed:
        print(f"failed checks (showing {min(len(failed), MAX_FAILED_SHOWN)} of {len(failed)}):")
        for c in failed[:MAX_FAILED_SHOWN]:
            print(_format_failed(c))
    return 0


def _import_edge(name: str) -> ModuleType | None:
    """Import a broker-facing module, or explain how to get the optional extra."""
    try:
        return importlib.import_module(f"streampipe.{name}")
    except ImportError as e:
        print(f"{MISSING_EXTRA} ({e})", file=sys.stderr)
        return None


def _cmd_produce(args: argparse.Namespace) -> int:
    mod = _import_edge("kafka_io")
    if mod is None:
        return 2
    from streampipe.events import read_log

    n = mod.produce(read_log(args.log), args.bootstrap, args.topic, args.pace)
    print(f"produced {n} events to {args.topic}")
    return 0


def _cmd_consume(args: argparse.Namespace) -> int:
    mod = _import_edge("kafka_io")
    if mod is None:
        return 2
    stats = mod.run_consumer(
        args.bootstrap,
        args.mode,
        args.lateness,
        topic_in=args.topic_in,
        topic_out=args.topic_out,
        group_id=args.group_id,
        idle_timeout_s=args.idle_timeout,
        max_events=args.max_events,
    )
    print(json.dumps(stats, sort_keys=True, default=str))
    return 0


def _cmd_bridge(args: argparse.Namespace) -> int:
    mod = _import_edge("bridge")
    if mod is None:
        return 2
    n = mod.run_bridge(
        args.bootstrap,
        args.url,
        topic=args.topic,
        token=os.environ.get("PUBLISH_TOKEN"),
        idle_timeout_s=args.idle_timeout,
    )
    print(f"forwarded {n} records to {args.url}")
    return 0


_COMMANDS = {
    "gen": _cmd_gen,
    "replay": _cmd_replay,
    "sweep": _cmd_sweep,
    "predict": _cmd_predict,
    "produce": _cmd_produce,
    "consume": _cmd_consume,
    "bridge": _cmd_bridge,
}


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return _COMMANDS[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
