from __future__ import annotations

from io import BytesIO
from typing import Any

import requests


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def get_json(url: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> Any:
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_text(url: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> str:
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def get_bytes(url: str, *, params: dict[str, Any] | None = None, timeout: int = 30) -> bytes:
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.content


def head_ok(url: str, *, timeout: int = 15) -> bool:
    response = requests.head(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    return response.ok


def as_buffer(payload: bytes) -> BytesIO:
    return BytesIO(payload)
