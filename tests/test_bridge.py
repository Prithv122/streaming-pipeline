"""Bridge tests: envelope shape, a real local HTTP round trip, and the loop with a fake consumer."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from streampipe.bridge import PublishError, publish_http, run_bridge, to_envelope
from streampipe.windows import WindowResult


def make_result(**kw) -> WindowResult:
    base = {
        "window_start": 60.0,
        "window_end": 120.0,
        "zone": "airport",
        "orders": 3,
        "revenue_cents": 4250,
        "revenue": 42.5,
        "late_dropped": 1,
        "revision": 2,
        "final": True,
        "emitted_at": 131.5,
    }
    return WindowResult(**{**base, **kw})


def test_envelope_shape():
    r = make_result()
    env = to_envelope(r)
    assert env == {"type": "event", "data": r.to_dict()}
    assert set(env) == {"type", "data"}
    json.dumps(env)  # serialisable as is


class HubStub:
    """A tiny hub: records each request and answers with a configured status."""

    def __init__(self, status: int = 202, body: bytes = b'{"seq": 1}') -> None:
        self.requests: list[dict] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                stub.requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "auth": self.headers.get("Authorization"),
                        "ctype": self.headers.get("Content-Type"),
                        "body": json.loads(self.rfile.read(length)),
                    }
                )
                self.send_response(stub.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(stub.body)))
                self.end_headers()
                self.wfile.write(stub.body)

            def log_message(self, *args):
                pass

        self.status, self.body = status, body
        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}/publish/windows"

    def __enter__(self) -> HubStub:
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def test_publish_round_trip_without_token():
    env = to_envelope(make_result())
    with HubStub() as hub:
        assert publish_http(hub.url, env) == 202
    (req,) = hub.requests
    assert req["method"] == "POST"
    assert req["path"] == "/publish/windows"
    assert req["body"] == env
    assert req["ctype"] == "application/json"
    assert req["auth"] is None


def test_publish_sends_bearer_token_when_given():
    with HubStub() as hub:
        publish_http(hub.url, to_envelope(make_result()), token="s3cret")
    assert hub.requests[0]["auth"] == "Bearer s3cret"


def test_publish_401_raises_with_status_and_hint():
    with (
        HubStub(status=401, body=b'{"detail":"bad token"}') as hub,
        pytest.raises(PublishError, match="401") as exc,
    ):
        publish_http(hub.url, to_envelope(make_result()), token="wrong")
    assert exc.value.status == 401
    assert "PUBLISH_TOKEN" in str(exc.value)


@pytest.mark.parametrize("status", [400, 404, 500, 503])
def test_publish_other_errors_raise(status):
    with HubStub(status=status, body=b"nope") as hub, pytest.raises(PublishError) as exc:
        publish_http(hub.url, to_envelope(make_result()))
    assert exc.value.status == status


def test_publish_unreachable_hub_raises_clear_error():
    # Port 9 (discard) on loopback is closed on a normal machine: connection refused.
    with pytest.raises(PublishError, match="could not reach hub"):
        publish_http("http://127.0.0.1:9/publish/windows", {"type": "event", "data": {}}, None, 1)


# --- run_bridge loop ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class FakeConsumer:
    def __init__(self, values, clock: FakeClock) -> None:
        self.queue = [
            type("M", (), {"value": lambda self, v=v: v, "error": lambda self: None})()
            for v in values
        ]
        self.clock = clock
        self.closed = False

    def poll(self, timeout):
        if self.queue:
            return self.queue.pop(0)
        self.clock.t += timeout
        return None

    def close(self):
        self.closed = True


def test_run_bridge_publishes_every_record_in_order():
    clock = FakeClock()
    results = [make_result(revision=i, final=i == 2) for i in range(3)]
    consumer = FakeConsumer([json.dumps(r.to_dict()).encode() for r in results], clock)
    calls: list[tuple] = []

    def fake_publish(url, envelope, token=None):
        calls.append((url, envelope, token))
        return 202

    n = run_bridge(
        "unused:9092",
        "http://hub/publish/windows",
        token="tok",
        idle_timeout_s=1.0,
        publish=fake_publish,
        consumer=consumer,
        clock=clock,
    )
    assert n == 3
    assert [c[1] for c in calls] == [to_envelope(r) for r in results]
    assert {c[0] for c in calls} == {"http://hub/publish/windows"}
    assert {c[2] for c in calls} == {"tok"}


def test_run_bridge_stops_on_publish_error():
    clock = FakeClock()
    consumer = FakeConsumer([json.dumps(make_result().to_dict()).encode()] * 3, clock)

    def failing(url, envelope, token=None):
        raise PublishError("hub down")

    with pytest.raises(PublishError):
        run_bridge("x:1", "http://hub/p", publish=failing, consumer=consumer, clock=clock)
    assert len(consumer.queue) == 2  # stopped at the first failure
    assert not consumer.closed  # an injected consumer is the caller's to close


def test_run_bridge_empty_topic_returns_zero():
    clock = FakeClock()
    n = run_bridge(
        "x:1",
        "http://hub/p",
        idle_timeout_s=0.5,
        publish=lambda *a: 202,
        consumer=FakeConsumer([], clock),
        clock=clock,
    )
    assert n == 0
