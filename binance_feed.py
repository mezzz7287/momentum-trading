"""Binance Futures aggTrade feed for momentum signal detection."""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Any, Deque, Dict, Iterable, Literal, Optional, Tuple

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


def _parse_agg_trade_payload(raw: Any) -> Optional[float]:
    """Extract price from raw or combined-stream aggTrade JSON."""
    if not isinstance(raw, dict):
        return None
    event = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    if not isinstance(event, dict):
        return None
    if event.get("e") not in (None, "aggTrade"):
        return None
    price_raw = event.get("p")
    if price_raw is None:
        return None
    try:
        price = float(price_raw)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


def _decode_ws_message(msg: aiohttp.WSMessage) -> Optional[Any]:
    if msg.type == aiohttp.WSMsgType.TEXT:
        raw = msg.data
    elif msg.type == aiohttp.WSMsgType.BINARY:
        raw = msg.data.decode("utf-8", errors="replace")
    else:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


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


async def _consume_stream(asset: str, symbol: str) -> None:
    global _session, _running
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@aggTrade"
    backoff = 1.0
    while _running:
        first_tick_logged = False
        try:
            assert _session is not None
            async with _session.ws_connect(url, heartbeat=20) as ws:
                print(f"📡 [Binance Futures] Connected: {symbol}@aggTrade → {asset.upper()}")
                backoff = 1.0
                async for msg in ws:
                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                        break
                    if msg.type == aiohttp.WSMsgType.ERROR:
                        raise ConnectionError(f"websocket error: {ws.exception()}")
                    payload = _decode_ws_message(msg)
                    if payload is None:
                        continue
                    price = _parse_agg_trade_payload(payload)
                    if price is None:
                        continue
                    recv_ms = int(time.time() * 1000)
                    record_price(asset, price, recv_ms=recv_ms)
                    if not first_tick_logged:
                        first_tick_logged = True
                        print(
                            f"📡 [Binance Futures] {asset.upper()} first tick "
                            f"price={price} buf={len(_buffer_for(asset))}"
                        )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"⚠️ [Binance Futures] {symbol}: {e} — reconnect in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


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
        _tasks = [
            asyncio.create_task(_consume_stream(asset, symbol))
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
