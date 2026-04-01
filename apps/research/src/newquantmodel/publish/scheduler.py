from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from newquantmodel.config.settings import AppPaths
from newquantmodel.publish.real_pipeline import refresh_market
from newquantmodel.storage.json_store import read_json, write_json


SCHEDULES = {
    "crypto": {"kind": "hourly", "interval_hours": 4, "timezone": "UTC"},
    "cn_equity": {"kind": "daily", "hour": 16, "minute": 30, "timezone": "Asia/Shanghai"},
    "us_equity": {"kind": "daily", "hour": 17, "minute": 30, "timezone": "America/New_York"},
}


def _state_path(paths: AppPaths):
    return paths.reference_dir / "scheduler_state.json"


def _current_bucket(market: str, now_utc: datetime) -> str:
    schedule = SCHEDULES[market]
    local_now = now_utc.astimezone(ZoneInfo(schedule["timezone"]))
    if schedule["kind"] == "hourly":
        hour = int(local_now.hour // schedule["interval_hours"]) * schedule["interval_hours"]
        bucket = local_now.replace(hour=hour, minute=0, second=0, microsecond=0)
        return bucket.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    scheduled = local_now.replace(hour=schedule["hour"], minute=schedule["minute"], second=0, microsecond=0)
    if local_now < scheduled:
        scheduled = scheduled - timedelta(days=1)
    return scheduled.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_scheduler(paths: AppPaths, *, years: int = 5, limit: int | None = None, poll_seconds: int = 60, once: bool = False) -> None:
    while True:
        state = read_json(_state_path(paths), {})
        now_utc = datetime.now(timezone.utc)
        updated = False
        for market in SCHEDULES:
            bucket = _current_bucket(market, now_utc)
            if state.get(market) == bucket:
                continue
            refresh_market(paths, market=market, years=years, limit=limit)
            state[market] = bucket
            updated = True
        if updated:
            write_json(_state_path(paths), state)
        if once:
            return
        time.sleep(max(poll_seconds, 15))
