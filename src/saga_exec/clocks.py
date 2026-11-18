"""Best-effort clock records for run metadata (measurement only; docs/formats.md, Clocks).

The task programs time everything with each machine's own clock, so any
interval between two machines includes their clock offset. Nothing here
corrects timestamps; it records what the machines' time sync said, so the
offset's likely size can be judged next to the results.

Slurm: driver.py runs a probe step on every machine at job start and end
(chronyc / timedatectl; parse_clock_probe there, stdlib only). Wayline:
run.py calls local_clock() on the machine it runs on (cse-lambda1) before and
after each run; the other machine must be read separately (docs/formats.md).
"""
from __future__ import annotations

import shutil
import socket
import subprocess
import time

CHRONY_FIELDS = ("reference_id", "reference_name", "stratum", "reference_time", "system_time_offset",
                 "last_offset", "rms_offset", "frequency_ppm", "residual_frequency_ppm", "skew_ppm",
                 "root_delay", "root_dispersion", "update_interval", "leap_status")
TEXT_FIELDS = ("reference_id", "reference_name", "leap_status")


def parse_chrony_csv(line: str) -> dict | None:
    """One line of `chronyc -c tracking` -> named fields (seconds / ppm, as chrony prints them)."""
    values = line.strip().split(",")
    if len(values) < len(CHRONY_FIELDS):
        return None
    out = {}
    for k, v in zip(CHRONY_FIELDS, values):
        try:
            out[k] = v if k in TEXT_FIELDS else float(v)
        except ValueError:
            out[k] = v
    return out


def local_clock(timeout: float = 5.0) -> dict:
    """This machine's time and NTP state now. Never raises."""
    rec: dict = {"host": socket.gethostname(), "unix": time.time()}
    try:
        if shutil.which("chronyc"):
            out = subprocess.run(["chronyc", "-c", "tracking"], capture_output=True, text=True, timeout=timeout)
            parsed = parse_chrony_csv(out.stdout.splitlines()[0]) if out.stdout.strip() else None
            rec.update({"source": "chronyc", "chrony": parsed} if parsed else
                       {"source": "chronyc", "raw": (out.stdout + out.stderr).strip()})
        elif shutil.which("timedatectl"):
            out = subprocess.run(["timedatectl", "timesync-status"], capture_output=True, text=True, timeout=timeout)
            rec.update({"source": "timedatectl", "raw": (out.stdout + out.stderr).strip()})
        else:
            rec["source"] = None
    except Exception as e:                    # measurement must never fail a run
        rec["error"] = str(e)
    rec["unix_after"] = time.time()
    return rec
