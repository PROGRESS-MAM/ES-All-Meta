'''Small toolbox for metadata_file.py.'''

# --------- IMPORTS ---------
import os
from datetime import datetime
from pathlib import Path

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
    '''Return the duration between two hh:mm:ss:ff/fps timecodes in hours.'''
    if tc_start is None or tc_end is None:
        return None

    def parse_tc_to_seconds(tc_value: str) -> float:
        try:
            time_part, fps_value = tc_value.rsplit("/", 1)
            hours, minutes, seconds, frames = map(int, time_part.split(":"))
            fps = int(fps_value)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid timecode '{tc_value}'. Expected hh:mm:ss:ff/fps."
            ) from exc

        if fps <= 0 or min(hours, minutes, seconds, frames) < 0:
            raise ValueError(f"Invalid timecode values in '{tc_value}'.")

        return hours * 3600 + minutes * 60 + seconds + frames / fps

    duration_hours = (parse_tc_to_seconds(tc_end) - parse_tc_to_seconds(tc_start)) / 3600
    return f"{duration_hours:.4f}"


__all__ = [
    "tb_get_duration_hours_from_tc",
    "tb_link_api",
    "tb_write_log",
]
