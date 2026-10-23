"""Bridge windows.v1 to the live dashboard hub (29-websocket-live) over HTTP.

Hub contract: `POST /publish/{channel}` with body `{"type": "event", "data": {}}` answers 202
`{"seq": n}`; when the hub has PUBLISH_TOKEN set, requests carry `Authorization: Bearer <token>`.
The hub accepts only `type: "event"` (other types are its own control messages); the `windows`
channel is what marks the payload as a window record.
Standard library only on the HTTP side; the Kafka client is imported lazily by `kafka_io`.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from streampipe.kafka_io import WINDOWS_TOPIC, make_consumer, poll_messages
from streampipe.windows import WindowResult


class PublishError(RuntimeError):
    """The hub refused or could not be reached. `status` is the HTTP status, or None."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def to_envelope(result: WindowResult) -> dict[str, Any]:
    return {"type": "event", "data": result.to_dict()}


def publish_http(
    url: str, envelope: dict[str, Any], token: str | None = None, timeout_s: float = 5.0
) -> int:
    """POST one envelope as JSON; return the HTTP status. Raises PublishError on any failure."""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = resp.status
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:200]
        hint = " (set PUBLISH_TOKEN to the hub's token)" if exc.code == 401 else ""
        raise PublishError(f"hub answered {exc.code} for {url}{hint}: {detail}", exc.code) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PublishError(f"could not reach hub at {url}: {exc}") from exc
    if status >= 300:
        raise PublishError(f"hub answered {status} for {url}", status)
    return status


def run_bridge(
    bootstrap: str,
    url: str,
    topic: str = WINDOWS_TOPIC,
    token: str | None = None,
    idle_timeout_s: float = 10.0,
    publish: Callable[..., int] = publish_http,
    *,
    consumer: Any | None = None,
    group_id: str = "streampipe-bridge",
    clock: Callable[[], float] = time.monotonic,
) -> int:
    """Forward every record on `topic` to the hub; return how many were published.

    Stops after `idle_timeout_s` with no message. `consumer` and `clock` are injectable for tests.
    """
    own = consumer is None
    if consumer is None:
        consumer = make_consumer(bootstrap, group_id, topic)
    count = 0
    try:
        for msg in poll_messages(consumer, idle_timeout_s, clock=clock):
            result = WindowResult(**json.loads(msg.value()))
            publish(url, to_envelope(result), token)
            count += 1
    finally:
        if own:
            consumer.close()
    return count
