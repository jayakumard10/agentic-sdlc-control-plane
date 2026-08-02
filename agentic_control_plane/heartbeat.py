"""A liveness signal the worker writes, and a check the container runs against it.

A process being up says nothing about the worker inside it still turning. This
platform has already been bitten by exactly that gap in its sibling repository: a
background thread died on construction, and the container reported healthy for as
long as it ran, because nothing had ever asked the thread whether it was there.

So the signal is written by the loop whose death matters, not by the process:
`Worker.run_forever` touches the file on every pass, idle or not, and the container
healthcheck fails when the file stops moving.

**Unconfigured is a no-op, deliberately.** `HEARTBEAT_PATH` is set in Compose and
nowhere else, so unit tests and local runs neither write the file nor need a writable
path for it. A heartbeat that has to be stubbed out to run the suite is a heartbeat
people delete.

What staleness means here is worth being precise about, because runs execute
serially on one worker: a single work item that takes longer than
`HEARTBEAT_MAX_AGE_SECONDS` also reads as unhealthy. That is the intended reading
rather than a false positive. A run holding the only worker for five minutes is
blocking every other run behind it, which is the condition worth surfacing whether
the cause is a wedged thread or a genuinely stuck one.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_MAX_AGE_SECONDS = 300


def heartbeat_path() -> Path | None:
    """The configured path, or None when the heartbeat is switched off."""
    raw = os.environ.get("HEARTBEAT_PATH", "").strip()
    return Path(raw) if raw else None


def max_age_seconds() -> float:
    raw = os.environ.get("HEARTBEAT_MAX_AGE_SECONDS", "").strip()
    if not raw:
        return float(DEFAULT_MAX_AGE_SECONDS)
    try:
        return float(raw)
    except ValueError:
        logger.error(
            "HEARTBEAT_MAX_AGE_SECONDS=%r is not a number; using %ss",
            raw,
            DEFAULT_MAX_AGE_SECONDS,
        )
        return float(DEFAULT_MAX_AGE_SECONDS)


def touch(path: Path | None = None) -> None:
    """Record that the worker loop is still turning.

    Never raises. This is called on every pass of the worker loop, and a heartbeat
    that could take the worker down with it would be worse than no heartbeat at all -
    the file is a report about the loop, not a dependency of it.
    """
    target = path if path is not None else heartbeat_path()
    if target is None:
        return
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(time.time()), encoding="utf-8")
    except OSError:
        logger.warning("could not write the heartbeat to %s", target, exc_info=True)


def check(path: Path | None = None, max_age: float | None = None) -> tuple[bool, str]:
    """Report whether the heartbeat is fresh, and why not when it is not."""
    target = path if path is not None else heartbeat_path()
    if target is None:
        return False, "HEARTBEAT_PATH is not set, so there is nothing to check"
    if not target.exists():
        return False, f"{target} does not exist; the worker has not completed a pass"

    limit = max_age if max_age is not None else max_age_seconds()
    age = time.time() - target.stat().st_mtime
    if age > limit:
        return False, f"heartbeat is {age:.0f}s old, over the {limit:.0f}s limit"
    return True, f"heartbeat is {age:.0f}s old"


def main() -> int:
    ok, reason = check()
    print(reason)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
