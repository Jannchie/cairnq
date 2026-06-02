"""ULID-style id generation. Format pinned by PROTOCOL.md and asserted by
conformance: `<prefix>_` + 26-char Crockford base32 (48-bit ms timestamp +
80-bit randomness), lexicographically time-sortable. Must match the TS SDK."""

from __future__ import annotations

import os
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_ulid(ts_ms: int | None = None) -> str:
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    value = (ts_ms << 80) | int.from_bytes(os.urandom(10), "big")
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def new_id(prefix: str = "task") -> str:
    return f"{prefix}_{new_ulid()}"


def now_ms() -> int:
    return int(time.time() * 1000)
