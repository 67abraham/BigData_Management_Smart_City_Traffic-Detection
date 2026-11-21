"""Time alignment between UCI hours and Open-Meteo values (spec §4: join only where defensible). Pure.

A UCI record labels one hour. If the label is the hour START U, the record covers [U, U+1h).
Open-Meteo values have a "valid time":
  instant             value at time T              -> use T = U       (start of the UCI hour)
  preceding_hour_sum  sum over [T-1h, T)            -> use T = U + 1h  (covers exactly [U, U+1h))
If the label were the hour END E (covers [E-1h, E)): instant -> T = E-1h, sum -> T = E.
"""
from __future__ import annotations

from typing import Mapping

LABELS = ("start", "end")
VALID_TIMES = ("instant", "preceding_hour_sum")


def offset_hours(valid_time: str, uci_hour_label: str) -> int:
    """Hours to add to the UCI label to get the Open-Meteo timestamp that describes that hour."""
    if uci_hour_label not in LABELS:
        raise ValueError(f"uci_hour_label must be one of {LABELS}, got {uci_hour_label!r}")
    if valid_time not in VALID_TIMES:
        raise ValueError(f"valid_time must be one of {VALID_TIMES}, got {valid_time!r}")
    if uci_hour_label == "start":
        return 0 if valid_time == "instant" else 1
    return -1 if valid_time == "instant" else 0


def offsets_by_variable(valid_time: Mapping[str, str], uci_hour_label: str) -> dict[int, list[str]]:
    """Group Open-Meteo variables by the offset to join them on, e.g. {0: [...instant], 1: [...sums]}."""
    out: dict[int, list[str]] = {}
    for var, vt in valid_time.items():
        out.setdefault(offset_hours(vt, uci_hour_label), []).append(var)
    return {k: sorted(v) for k, v in sorted(out.items())}
