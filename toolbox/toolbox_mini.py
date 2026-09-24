'''Small toolbox for metadata_file.py.'''

# --------- IMPORTS ---------
import os
from datetime import datetime
from pathlib import Path
from fractions import Fraction

import FlowAPI
from dotenv import load_dotenv


# --------- API ---------
def tb_link_api(env_path: Path, api: str) -> FlowAPI.Metadata | None:
    '''Create and return the Flow metadata API gateway.'''
    if api.casefold() != "metadata":
        return None

    load_dotenv(env_path)
    return FlowAPI.Metadata.create_gateway_instance(
        os.environ.get("FLOW_USER"),
        os.environ.get("FLOW_PASSWORD"),
        os.environ.get("FLOW_HOST"),
    )


# --------- LOGGING ---------
def tb_write_log(log_path: Path, message: str) -> None:
    '''Create the log file if needed and append a timestamped message.'''
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp}: {message}\n")


# --------- TIMECODE ---------
def tb_get_duration_hours_from_tc(tc_start: str, tc_end: str) -> str | None:
    """Return elapsed hours from two timecodes or None for an invalid pair.

    Accept hh:mm:ss:ff/fps and hh:mm:ss:ff:rate_n/rate_d. An optional nd
    suffix is ignored; HH:MM:SS represent clock time, without drop-frame
    or 24-hour rollover correction.
    """
    if not isinstance(tc_start, str) or not isinstance(tc_end, str):
        return None

    def parse_tc(tc_value: str) -> tuple[Fraction, Fraction] | None:
        tokens = tc_value.strip().split()
        if len(tokens) not in (1, 2) or (len(tokens) == 2 and tokens[1].casefold() != "nd"):
            return None
        body, separator, rate_tail = tokens[0].partition("/")
        parts = body.split(":")
        if not separator or len(parts) not in (4, 5) or not all(
            part.isascii() and part.isdecimal() for part in (*parts, rate_tail)
        ):
            return None

        hours, minutes, seconds, frames = map(int, parts[:4])
        numerator = int(parts[4] if len(parts) == 5 else rate_tail)
        denominator = int(rate_tail) if len(parts) == 5 else 1
        if numerator < 1 or denominator < 1 or minutes >= 60 or seconds >= 60:
            return None
        fps = Fraction(numerator, denominator)
        if frames >= round(fps):
            return None
        time_seconds = Fraction(hours * 3600 + minutes * 60 + seconds) + frames / fps
        return time_seconds, fps

    start = parse_tc(tc_start)
    end = parse_tc(tc_end)
    if start is None or end is None or start[1] != end[1] or end[0] < start[0]:
        return None
    return f"{float((end[0] - start[0]) / 3600):.8f}"


__all__ = [
    "tb_get_duration_hours_from_tc",
    "tb_link_api",
    "tb_write_log",
]
