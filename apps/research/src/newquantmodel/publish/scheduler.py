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


def _default_market_state(market: str) -> dict[str, str | None]:
    return {
        "market": market,
        "lastCompletedBucket": None,
        "lastRunAt": None,
        "lastSuccessAt": None,
        "lastError": None,
        "nextScheduledAt": None,
    }


def _normalize_state(payload: dict) -> dict:
    markets = {
        market: _default_market_state(market)
        for market in SCHEDULES
    }
    worker = {
        "heartbeatAt": None,
        "pollSeconds": None,
        "lastLoopAt": None,
        "lastError": None,
        "status": "idle",
    }

    raw_markets = payload.get("markets") if isinstance(payload, dict) else None
    if isinstance(raw_markets, dict):
        for market, value in raw_markets.items():
            if market not in markets or not isinstance(value, dict):
                continue
            markets[market].update(value)
    elif isinstance(payload, dict):
        for market in SCHEDULES:
            legacy_bucket = payload.get(market)
            if isinstance(legacy_bucket, str):
                markets[market]["lastCompletedBucket"] = legacy_bucket

    raw_worker = payload.get("worker") if isinstance(payload, dict) else None
    if isinstance(raw_worker, dict):
        worker.update(raw_worker)

    return {"worker": worker, "markets": markets}


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


def _next_bucket(market: str, now_utc: datetime) -> str:
    schedule = SCHEDULES[market]
    local_now = now_utc.astimezone(ZoneInfo(schedule["timezone"]))
    if schedule["kind"] == "hourly":
        hour = int(local_now.hour // schedule["interval_hours"]) * schedule["interval_hours"]
        current_bucket = local_now.replace(hour=hour, minute=0, second=0, microsecond=0)
        next_bucket = current_bucket + timedelta(hours=schedule["interval_hours"])
        return next_bucket.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    scheduled = local_now.replace(hour=schedule["hour"], minute=schedule["minute"], second=0, microsecond=0)
    if local_now >= scheduled:
        scheduled = scheduled + timedelta(days=1)
    return scheduled.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_scheduler(paths: AppPaths, *, years: int = 5, limit: int | None = None, poll_seconds: int = 60, once: bool = False) -> None:
    while True:
        state = _normalize_state(read_json(_state_path(paths), {}))
        now_utc = datetime.now(timezone.utc)
        updated = False
        state["worker"]["heartbeatAt"] = now_utc.isoformat()
        state["worker"]["lastLoopAt"] = now_utc.isoformat()
        state["worker"]["pollSeconds"] = int(poll_seconds)
        state["worker"]["status"] = "running"
        write_json(_state_path(paths), state)
        for market in SCHEDULES:
            state["worker"]["heartbeatAt"] = datetime.now(timezone.utc).isoformat()
            state["worker"]["lastLoopAt"] = state["worker"]["heartbeatAt"]
            write_json(_state_path(paths), state)
            market_state = state["markets"][market]
            bucket = _current_bucket(market, now_utc)
            market_state["nextScheduledAt"] = _next_bucket(market, now_utc)
            if market_state.get("lastCompletedBucket") == bucket:
                continue
            market_state["lastRunAt"] = datetime.now(timezone.utc).isoformat()
            market_state["lastError"] = None
            state["worker"]["lastError"] = None
            write_json(_state_path(paths), state)
            try:
                refresh_market(paths, market=market, years=years, limit=limit)
                market_state["lastCompletedBucket"] = bucket
                market_state["lastSuccessAt"] = datetime.now(timezone.utc).isoformat()
                market_state["lastError"] = None
                updated = True
            except Exception as exc:
                market_state["lastError"] = str(exc)
                state["worker"]["lastError"] = str(exc)
                write_json(_state_path(paths), state)
                if once:
                    raise
        write_json(_state_path(paths), state)
        if once:
            return
        time.sleep(max(poll_seconds, 15))
