"""Binance Futures price feed for momentum signal detection.

Uses REST polling (fapi ticker/price) because aggTrade WebSocket often connects
but delivers no frames on Render and some residential networks. aiohttp is used
for all HTTP so it matches the existing dependency stack.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from typing import Deque, Dict, Iterable, Literal, Optional, Tuple

import aiohttp

ASSET_TO_SYMBOL: dict[str, str] = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
    "doge": "DOGEUSDT",
    "bnb": "BNBUSDT",
    "hype": "HYPEUSDT",
}

FAPI_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/price"
POLL_INTERVAL_SEC = float(os.getenv("BINANCE_POLL_INTERVAL_SEC", "0.25"))

SignalDirection = Literal["UP", "DOWN"]

_buffers: Dict[str, Deque[Tuple[int, float]]] = {}
_last_update_ms: Dict[str, int] = {}
_tasks: list[asyncio.Task] = []
_session: Optional[aiohttp.ClientSession] = None
_running = False
_lock = asyncio.Lock()
_MAX_BUFFER_TICKS = 5000


def _normalize_asset(asset: str) -> str:
    return (asset or "").strip().lower()


def _buffer_for(asset: str) -> Deque[Tuple[int, float]]:
    key = _normalize_asset(asset)
    if key not in _buffers:
        _buffers[key] = deque()
    return _buffers[key]


def _prune(buffer: Deque[Tuple[int, float]], lookback_ms: int, now_ms: int) -> None:
    cutoff = now_ms - lookback_ms
    while buffer and buffer[0][0] < cutoff:
        buffer.popleft()


def record_price(asset: str, price: float, *, recv_ms: Optional[int] = None) -> None:
    """Append one tick; timestamps use local receive time (wall clock)."""
    if price <= 0:
        return
    ts_ms = recv_ms if recv_ms is not None else int(time.time() * 1000)
    key = _normalize_asset(asset)
    buf = _buffer_for(key)
    buf.append((ts_ms, price))
    while len(buf) > _MAX_BUFFER_TICKS:
        buf.popleft()
    _last_update_ms[key] = ts_ms


def _window_prices(
    asset: str, lookback_ms: int,
) -> Optional[Tuple[float, float, float]]:
    """Return (oldest_price, newest_price, delta_fraction) or None."""
    key = _normalize_asset(asset)
    buf = _buffer_for(key)
    if len(buf) < 2:
        return None
    now_ms = int(time.time() * 1000)
    _prune(buf, lookback_ms, now_ms)
    if len(buf) < 2:
        return None
    _oldest_ts, oldest_px = buf[0]
    _newest_ts, newest_px = buf[-1]
    if oldest_px <= 0 or newest_px <= 0:
        return None
    delta = (newest_px - oldest_px) / oldest_px
    return oldest_px, newest_px, delta


def is_feed_fresh(asset: str, stale_ms: int = 5000) -> bool:
    key = _normalize_asset(asset)
    last = _last_update_ms.get(key, 0)
    if last <= 0:
        return False
    return (int(time.time() * 1000) - last) <= stale_ms


def get_momentum_signal(
    asset: str, lookback_ms: int, min_delta: float,
) -> Optional[SignalDirection]:
    window = _window_prices(asset, lookback_ms)
    if window is None:
        return None
    _oldest, _newest, delta = window
    if delta >= min_delta:
        return "UP"
    if (-delta) >= min_delta:
        return "DOWN"
    return None


def get_momentum_status(
    asset: str, lookback_ms: int, min_delta: float,
) -> dict:
    """Dashboard helper: last computed delta and direction (may be below threshold)."""
    window = _window_prices(asset, lookback_ms)
    fresh = is_feed_fresh(asset)
    if window is None:
        return {"signal": None, "delta": 0.0, "delta_pct": 0.0, "fresh": fresh}
    _oldest, _newest, delta = window
    signal: Optional[SignalDirection] = None
    if delta >= min_delta:
        signal = "UP"
    elif (-delta) >= min_delta:
        signal = "DOWN"
    return {
        "signal": signal,
        "delta": round(delta, 6),
        "delta_pct": round(delta * 100, 4),
        "fresh": fresh,
    }


async def _poll_price_loop(asset: str, symbol: str) -> None:
    """Poll Binance Futures mark price over REST — reliable on Render."""
    global _session, _running
    backoff = 1.0
    first_tick_logged = False
    timeout = aiohttp.ClientTimeout(total=8)
    while _running:
        try:
            assert _session is not None
            async with _session.get(
                FAPI_TICKER_URL,
                params={"symbol": symbol.upper()},
                timeout=timeout,
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=body[:120],
                    )
                data = await resp.json()
                price = float(data.get("price", 0))
                if price <= 0:
                    raise ValueError(f"invalid price payload: {data!r}")
                recv_ms = int(time.time() * 1000)
                record_price(asset, price, recv_ms=recv_ms)
                backoff = 1.0
                if not first_tick_logged:
                    first_tick_logged = True
                    print(
                        f"📡 [Binance REST] {asset.upper()} polling {symbol} "
                        f"every {POLL_INTERVAL_SEC:.2f}s | first price={price}"
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"⚠️ [Binance REST] {symbol}: {e} — retry in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def start_binance_feed(assets: Iterable[str]) -> None:
    global _session, _running, _tasks
    async with _lock:
        if _running:
            return
        _running = True
        _session = aiohttp.ClientSession()
        symbols: dict[str, str] = {}
        for asset in assets:
            key = _normalize_asset(asset)
            sym = ASSET_TO_SYMBOL.get(key)
            if sym:
                symbols[key] = sym
            else:
                print(f"⚠️ [Binance Feed] No symbol mapping for asset {asset!r}")
        print(
            f"📡 [Binance Feed] REST poll mode ({POLL_INTERVAL_SEC:.2f}s interval) "
            f"for {len(symbols)} symbol(s)"
        )
        _tasks = [
            asyncio.create_task(_poll_price_loop(asset, symbol))
            for asset, symbol in symbols.items()
        ]


async def stop_binance_feed() -> None:
    global _session, _running, _tasks
    async with _lock:
        _running = False
        for task in _tasks:
            task.cancel()
        if _tasks:
            await asyncio.gather(*_tasks, return_exceptions=True)
        _tasks = []
        if _session:
            await _session.close()
            _session = None
