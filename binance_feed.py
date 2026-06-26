"""Spot/perp price feed for momentum signal detection.

Binance Futures is geo-blocked (HTTP 451) on Render and other US datacenters.
Default provider is Coinbase Exchange (spot); BNB/HYPE use Bybit linear.
Set PRICE_FEED=binance to force Binance Futures (works outside restricted regions).
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from typing import Deque, Dict, Iterable, Literal, Optional, Tuple

import aiohttp

# Binance Futures (blocked in US / Render)
BINANCE_SYMBOL: dict[str, str] = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
    "doge": "DOGEUSDT",
    "bnb": "BNBUSDT",
    "hype": "HYPEUSDT",
}

# Coinbase Exchange spot (US-friendly)
COINBASE_PRODUCT: dict[str, str] = {
    "btc": "BTC-USD",
    "eth": "ETH-USD",
    "sol": "SOL-USD",
    "xrp": "XRP-USD",
    "doge": "DOGE-USD",
}

# Bybit linear perps (fallback for assets not on Coinbase)
BYBIT_SYMBOL: dict[str, str] = dict(BINANCE_SYMBOL)

FAPI_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/price"
COINBASE_TICKER_URL = "https://api.exchange.coinbase.com/products/{product}/ticker"
BYBIT_TICKER_URL = "https://api.bybit.com/v5/market/tickers"

POLL_INTERVAL_SEC = float(os.getenv("BINANCE_POLL_INTERVAL_SEC", "0.25"))
# auto | coinbase | binance | bybit  (coinbase default — Render/US blocks Binance)
PRICE_FEED = os.getenv("PRICE_FEED", "coinbase").strip().lower()

SignalDirection = Literal["UP", "DOWN"]

_buffers: Dict[str, Deque[Tuple[int, float]]] = {}
_last_update_ms: Dict[str, int] = {}
_tasks: list[asyncio.Task] = []
_session: Optional[aiohttp.ClientSession] = None
_running = False
_lock = asyncio.Lock()
_MAX_BUFFER_TICKS = 5000
_resolved_feed: str = "coinbase"  # effective provider after auto-detect


def _normalize_asset(asset: str) -> str:
    return (asset or "").strip().lower()


def _feed_for_asset(asset: str, feed: str) -> Optional[Tuple[str, str]]:
    """Return (provider, symbol_or_product) for asset, or None if unsupported."""
    key = _normalize_asset(asset)
    if feed == "binance":
        sym = BINANCE_SYMBOL.get(key)
        return ("binance", sym) if sym else None
    if feed == "bybit":
        sym = BYBIT_SYMBOL.get(key)
        return ("bybit", sym) if sym else None
    if feed == "coinbase":
        product = COINBASE_PRODUCT.get(key)
        if product:
            return ("coinbase", product)
        sym = BYBIT_SYMBOL.get(key)
        return ("bybit", sym) if sym else None
    # auto: prefer coinbase path (Render-safe default after any 451)
    product = COINBASE_PRODUCT.get(key)
    if product:
        return ("coinbase", product)
    sym = BYBIT_SYMBOL.get(key)
    return ("bybit", sym) if sym else None


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


def _is_geo_blocked(resp: aiohttp.ClientResponse, body: str) -> bool:
    if resp.status == 451:
        return True
    return resp.status == 403 and "restricted location" in body.lower()


async def _fetch_price(
    session: aiohttp.ClientSession,
    provider: str,
    symbol: str,
    timeout: aiohttp.ClientTimeout,
) -> float:
    if provider == "binance":
        async with session.get(
            FAPI_TICKER_URL,
            params={"symbol": symbol.upper()},
            timeout=timeout,
        ) as resp:
            body = await resp.text()
            if resp.status != 200:
                if _is_geo_blocked(resp, body):
                    raise _GeoBlockedError(provider)
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=resp.status, message=body[:120],
                )
            data = await resp.json(content_type=None)
            price = float(data.get("price", 0))
    elif provider == "coinbase":
        url = COINBASE_TICKER_URL.format(product=symbol)
        async with session.get(url, timeout=timeout) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=resp.status, message=body[:120],
                )
            data = await resp.json(content_type=None)
            price = float(data.get("price", 0))
    elif provider == "bybit":
        async with session.get(
            BYBIT_TICKER_URL,
            params={"category": "linear", "symbol": symbol.upper()},
            timeout=timeout,
        ) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=resp.status, message=body[:120],
                )
            data = await resp.json(content_type=None)
            items = (data.get("result") or {}).get("list") or []
            if not items:
                raise ValueError(f"bybit empty ticker: {data!r}")
            price = float(items[0].get("lastPrice", 0))
    else:
        raise ValueError(f"unknown provider {provider!r}")

    if price <= 0:
        raise ValueError(f"invalid price from {provider}/{symbol}")
    return price


class _GeoBlockedError(Exception):
    def __init__(self, provider: str) -> None:
        self.provider = provider
        super().__init__(f"{provider} geo-blocked")


async def _poll_price_loop(asset: str) -> None:
    """Poll exchange REST ticker for one asset."""
    global _session, _running, _resolved_feed
    backoff = 1.0
    first_tick_logged = False
    timeout = aiohttp.ClientTimeout(total=8)
    while _running:
        provider_symbol = _feed_for_asset(asset, _resolved_feed)
        if provider_symbol is None:
            print(f"⚠️ [Price Feed] No feed mapping for asset {asset!r}")
            await asyncio.sleep(30.0)
            continue
        provider, symbol = provider_symbol
        try:
            assert _session is not None
            price = await _fetch_price(_session, provider, symbol, timeout)
            recv_ms = int(time.time() * 1000)
            record_price(asset, price, recv_ms=recv_ms)
            backoff = 1.0
            if not first_tick_logged:
                first_tick_logged = True
                print(
                    f"📡 [Price Feed] {asset.upper()} via {provider} {symbol} "
                    f"every {POLL_INTERVAL_SEC:.2f}s | first price={price}"
                )
        except asyncio.CancelledError:
            raise
        except _GeoBlockedError as e:
            if PRICE_FEED in ("auto", "binance") and _resolved_feed == "binance":
                _resolved_feed = "coinbase"
                print(
                    f"⚠️ [Price Feed] {e.provider} geo-blocked (451) — "
                    f"switching to Coinbase/Bybit for all assets"
                )
                backoff = 0.5
            else:
                print(f"⚠️ [Price Feed] {symbol} ({provider}): geo-blocked — retry in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            continue
        except Exception as e:
            print(f"⚠️ [Price Feed] {symbol} ({provider}): {e} — retry in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def start_binance_feed(assets: Iterable[str]) -> None:
    global _session, _running, _tasks, _resolved_feed
    async with _lock:
        if _running:
            return
        _running = True
        if PRICE_FEED == "auto":
            _resolved_feed = "binance"
        elif PRICE_FEED in ("coinbase", "binance", "bybit"):
            _resolved_feed = PRICE_FEED
        else:
            print(f"⚠️ [Price Feed] Unknown PRICE_FEED={PRICE_FEED!r}, using coinbase")
            _resolved_feed = "coinbase"

        _session = aiohttp.ClientSession(
            headers={"User-Agent": "mezzz-momentum-bot/1.0"},
        )
        asset_list = [_normalize_asset(a) for a in assets]
        mapped = [a for a in asset_list if _feed_for_asset(a, _resolved_feed)]
        for a in asset_list:
            if a not in mapped:
                print(f"⚠️ [Price Feed] No symbol mapping for asset {a!r}")
        print(
            f"📡 [Price Feed] REST poll ({POLL_INTERVAL_SEC:.2f}s) | "
            f"PRICE_FEED={PRICE_FEED} → {_resolved_feed} | {len(mapped)} asset(s)"
        )
        _tasks = [
            asyncio.create_task(_poll_price_loop(asset))
            for asset in mapped
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
