"""
Centralized configuration: trading_config.json workers + .env secrets/globals.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

SUPPORTED_TRADING_ASSETS: frozenset[str] = frozenset(
    {"btc", "eth", "sol", "xrp", "doge", "hype", "bnb"}
)
SUPPORTED_WINDOWS: frozenset[str] = frozenset({"5m"})
WINDOW_SECONDS: dict[str, int] = {"5m": 300}
MIN_SHARES: int = 5

MOMENTUM_MODES: frozenset[str] = frozenset(
    {"single_taker", "gtc_at_ask", "single_maker", "dual_hybrid"}
)

_ASSET_ALIASES: dict[str, str] = {
    "bitcoin": "btc",
    "ethereum": "eth",
    "solana": "sol",
    "ripple": "xrp",
}


def _fatal(message: str) -> None:
    print(f"❌ [config] {message}", file=sys.stderr)
    sys.exit(1)


def normalize_asset_slug(raw: str) -> str:
    token = (raw or "").strip().lower()
    if not token:
        raise ValueError("empty asset token")
    return _ASSET_ALIASES.get(token, token)


def normalize_window(raw: str) -> str:
    w = (raw or "").strip().lower()
    if w not in SUPPORTED_WINDOWS:
        raise ValueError(f"unsupported window {raw!r}")
    return w


def worker_key(asset: str, window: str) -> str:
    return f"{normalize_asset_slug(asset)}:{normalize_window(window)}"


def _parse_momentum_delta(name: str, value: Any, default: float) -> float:
    raw = value if value is not None else default
    try:
        v = float(raw)
    except (TypeError, ValueError):
        _fatal(f"{name}={raw!r} is not a valid number.")
    if v <= 0 or v != v or v in (float("inf"), float("-inf")):
        _fatal(f"{name} must be a positive fraction (got {raw!r}).")
    return v


def _parse_lookback_ms(name: str, value: Any, default: int) -> int:
    try:
        v = int(value if value is not None else default)
    except (TypeError, ValueError):
        _fatal(f"{name}={value!r} is not a valid integer.")
    if v < 100:
        _fatal(f"{name} must be >= 100 ms (got {value!r}).")
    return v


def _parse_momentum_mode(name: str, value: Any, default: str) -> str:
    raw = str(value if value is not None else default).strip().lower()
    if raw not in MOMENTUM_MODES:
        _fatal(
            f"{name}={raw!r} is invalid. "
            f"Use one of: {', '.join(sorted(MOMENTUM_MODES))}."
        )
    return raw


def _parse_order_size(name: str, value: Any, default: float) -> float:
    raw = value if value is not None else default
    try:
        v = float(raw)
    except (TypeError, ValueError):
        _fatal(f"{name}={raw!r} is not a valid number.")
    if v < MIN_SHARES or v != v or v in (float("inf"), float("-inf")):
        _fatal(f"{name} must be >= {MIN_SHARES} (got {raw!r}).")
    return v


def _parse_max_shares(name: str, value: Any, default: float) -> float:
    raw = value if value is not None else default
    try:
        v = float(raw)
    except (TypeError, ValueError):
        _fatal(f"{name}={raw!r} is not a valid number.")
    if v < MIN_SHARES or v != v or v in (float("inf"), float("-inf")):
        _fatal(f"{name} must be >= {MIN_SHARES} (got {raw!r}).")
    return v


def _parse_cooldown_ms(name: str, value: Any, default: int) -> int:
    try:
        v = int(value if value is not None else default)
    except (TypeError, ValueError):
        _fatal(f"{name}={value!r} is not a valid integer.")
    if v < 0:
        _fatal(f"{name} must be >= 0 (got {value!r}).")
    return v


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


def _load_env_sizing_overrides() -> Optional[dict[str, float]]:
    """Apply sizing overrides only when MOMENTUM_SIZE and MOMENTUM_MAX_SHARES are both set."""
    keys = ("MOMENTUM_SIZE", "MOMENTUM_MAX_SHARES")
    raw = {k: os.getenv(k, "").strip() for k in keys}
    set_keys = [k for k, v in raw.items() if v]
    if not set_keys:
        return None
    if len(set_keys) != len(keys):
        _fatal(
            "MOMENTUM_SIZE and MOMENTUM_MAX_SHARES must both be set together "
            f"to override sizing (found: {', '.join(set_keys)}). "
            "Omit both to use trading_config.json defaults."
        )
    fixed = _parse_order_size("MOMENTUM_SIZE", raw["MOMENTUM_SIZE"], 0.0)
    return {
        "momentum_size_min": fixed,
        "momentum_size_max": fixed,
        "momentum_max_shares": _parse_max_shares(
            "MOMENTUM_MAX_SHARES", raw["MOMENTUM_MAX_SHARES"], 0.0,
        ),
    }


def _load_env_size_range_overrides() -> Optional[dict[str, float]]:
    """Optional MOMENTUM_SIZE_MIN + MOMENTUM_SIZE_MAX for randomized order sizing."""
    keys = ("MOMENTUM_SIZE_MIN", "MOMENTUM_SIZE_MAX")
    raw = {k: os.getenv(k, "").strip() for k in keys}
    set_keys = [k for k, v in raw.items() if v]
    if not set_keys:
        return None
    if len(set_keys) != len(keys):
        _fatal(
            "MOMENTUM_SIZE_MIN and MOMENTUM_SIZE_MAX must both be set together "
            f"to override size range (found: {', '.join(set_keys)})."
        )
    lo = _parse_order_size("MOMENTUM_SIZE_MIN", raw["MOMENTUM_SIZE_MIN"], 0.0)
    hi = _parse_order_size("MOMENTUM_SIZE_MAX", raw["MOMENTUM_SIZE_MAX"], 0.0)
    if lo > hi:
        _fatal(f"MOMENTUM_SIZE_MIN ({lo}) cannot exceed MOMENTUM_SIZE_MAX ({hi}).")
    out: dict[str, float] = {"momentum_size_min": lo, "momentum_size_max": hi}
    max_shares_raw = os.getenv("MOMENTUM_MAX_SHARES", "").strip()
    if max_shares_raw:
        out["momentum_max_shares"] = _parse_max_shares(
            "MOMENTUM_MAX_SHARES", max_shares_raw, 0.0,
        )
    return out


ENV_SIZING_OVERRIDES: Optional[dict[str, float]] = _load_env_sizing_overrides()
ENV_SIZE_RANGE_OVERRIDES: Optional[dict[str, float]] = _load_env_size_range_overrides()


def _load_env_momentum_overrides() -> dict[str, float | int]:
    """Optional per-env momentum tuning (each key independent)."""
    out: dict[str, float | int] = {}
    delta_raw = os.getenv("MOMENTUM_MIN_DELTA", "").strip()
    if delta_raw:
        out["momentum_min_delta"] = _parse_momentum_delta(
            "MOMENTUM_MIN_DELTA", delta_raw, 0.0015,
        )
    lookback_raw = os.getenv("MOMENTUM_LOOKBACK_MS", "").strip()
    if lookback_raw:
        out["momentum_lookback_ms"] = _parse_lookback_ms(
            "MOMENTUM_LOOKBACK_MS", lookback_raw, 3000,
        )
    return out


ENV_MOMENTUM_OVERRIDES: dict[str, float | int] = _load_env_momentum_overrides()


DRY_RUN_DEFAULT: bool = _parse_bool_env("DRY_RUN_DEFAULT", _parse_bool_env("DRY_MODE", True))


@dataclass(frozen=True)
class WorkerConfig:
    asset: str
    window: str
    momentum_lookback_ms: int = 3000
    momentum_min_delta: float = 0.0015
    momentum_mode: str = "single_taker"
    momentum_size_min: float = 5.1
    momentum_size_max: float = 9.9
    momentum_max_shares: float = 10.2
    trade_cooldown_ms: int = 3000
    dry_run: bool = DRY_RUN_DEFAULT
    dry_run_fill_delay_min_ms: int = 200
    dry_run_fill_delay_max_ms: int = 2500
    listener_activate_secs: int = 300
    entry_seconds_left: int = 300
    enabled: bool = True

    @property
    def interval_seconds(self) -> int:
        return WINDOW_SECONDS[self.window]

    @property
    def key(self) -> str:
        return worker_key(self.asset, self.window)

    def market_slug(self, start_ts: int) -> str:
        return f"{self.asset}-updown-{self.window}-{start_ts}"


def _merge_worker_entry(raw: dict, defaults: dict) -> WorkerConfig:
    asset = normalize_asset_slug(str(raw.get("asset", "")))
    if asset not in SUPPORTED_TRADING_ASSETS:
        _fatal(f"Invalid asset {raw.get('asset')!r}. Supported: {sorted(SUPPORTED_TRADING_ASSETS)}")

    try:
        window = normalize_window(str(raw.get("window", "")))
    except ValueError:
        _fatal(
            f"Invalid window {raw.get('window')!r} for {asset}. "
            f"Supported: {sorted(SUPPORTED_WINDOWS)}"
        )

    momentum_lookback_ms = _parse_lookback_ms(
        "momentum_lookback_ms",
        raw.get("momentum_lookback_ms", defaults.get("momentum_lookback_ms")),
        int(defaults.get("momentum_lookback_ms", 3000)),
    )
    momentum_min_delta = _parse_momentum_delta(
        "momentum_min_delta",
        raw.get("momentum_min_delta", defaults.get("momentum_min_delta")),
        float(defaults.get("momentum_min_delta", 0.0015)),
    )
    momentum_mode = _parse_momentum_mode(
        "momentum_mode",
        raw.get("momentum_mode", defaults.get("momentum_mode")),
        str(defaults.get("momentum_mode", "single_taker")),
    )
    min_raw = raw.get("momentum_size_min", defaults.get("momentum_size_min"))
    max_raw = raw.get("momentum_size_max", defaults.get("momentum_size_max"))
    if min_raw is not None or max_raw is not None:
        if min_raw is None or max_raw is None:
            _fatal(f"{asset}:{window}: momentum_size_min and momentum_size_max must both be set.")
        momentum_size_min = _parse_order_size(
            "momentum_size_min", min_raw, float(defaults.get("momentum_size_min", 5.1)),
        )
        momentum_size_max = _parse_order_size(
            "momentum_size_max", max_raw, float(defaults.get("momentum_size_max", 9.9)),
        )
    else:
        fixed = _parse_order_size(
            "momentum_size",
            raw.get("momentum_size", defaults.get("momentum_size")),
            float(defaults.get("momentum_size", 10.0)),
        )
        momentum_size_min = fixed
        momentum_size_max = fixed
    momentum_max_shares = _parse_max_shares(
        "momentum_max_shares",
        raw.get("momentum_max_shares", defaults.get("momentum_max_shares")),
        float(defaults.get("momentum_max_shares", 10.2)),
    )
    trade_cooldown_ms = _parse_cooldown_ms(
        "trade_cooldown_ms",
        raw.get("trade_cooldown_ms", defaults.get("trade_cooldown_ms")),
        int(defaults.get("trade_cooldown_ms", 3000)),
    )

    if ENV_SIZING_OVERRIDES:
        momentum_size_min = ENV_SIZING_OVERRIDES["momentum_size_min"]
        momentum_size_max = ENV_SIZING_OVERRIDES["momentum_size_max"]
        momentum_max_shares = ENV_SIZING_OVERRIDES["momentum_max_shares"]
    elif ENV_SIZE_RANGE_OVERRIDES:
        momentum_size_min = ENV_SIZE_RANGE_OVERRIDES["momentum_size_min"]
        momentum_size_max = ENV_SIZE_RANGE_OVERRIDES["momentum_size_max"]
        if "momentum_max_shares" in ENV_SIZE_RANGE_OVERRIDES:
            momentum_max_shares = ENV_SIZE_RANGE_OVERRIDES["momentum_max_shares"]

    if momentum_size_min > momentum_size_max:
        _fatal(
            f"{asset}:{window}: momentum_size_min ({momentum_size_min}) "
            f"cannot exceed momentum_size_max ({momentum_size_max})"
        )
    if momentum_size_max > momentum_max_shares:
        _fatal(
            f"{asset}:{window}: momentum_size_max ({momentum_size_max}) "
            f"cannot exceed momentum_max_shares ({momentum_max_shares})"
        )

    if "momentum_min_delta" in ENV_MOMENTUM_OVERRIDES:
        momentum_min_delta = float(ENV_MOMENTUM_OVERRIDES["momentum_min_delta"])
    if "momentum_lookback_ms" in ENV_MOMENTUM_OVERRIDES:
        momentum_lookback_ms = int(ENV_MOMENTUM_OVERRIDES["momentum_lookback_ms"])

    dr_raw = raw.get("dry_run", defaults.get("dry_run"))
    if dr_raw is None:
        dry_run = DRY_RUN_DEFAULT
    else:
        dry_run = bool(dr_raw)

    dry_min = _parse_cooldown_ms(
        "dry_run_fill_delay_min_ms",
        raw.get("dry_run_fill_delay_min_ms", defaults.get("dry_run_fill_delay_min_ms")),
        int(defaults.get("dry_run_fill_delay_min_ms", 200)),
    )
    dry_max = _parse_cooldown_ms(
        "dry_run_fill_delay_max_ms",
        raw.get("dry_run_fill_delay_max_ms", defaults.get("dry_run_fill_delay_max_ms")),
        int(defaults.get("dry_run_fill_delay_max_ms", 2500)),
    )
    if dry_max < dry_min:
        _fatal(f"{asset}:{window}: dry_run_fill_delay_max_ms must be >= dry_run_fill_delay_min_ms")

    interval = WINDOW_SECONDS[window]
    listener_raw = raw.get("listener_activate_secs", defaults.get("listener_activate_secs"))
    entry_raw = raw.get("entry_seconds_left", defaults.get("entry_seconds_left"))
    env_listener = os.getenv("LISTENER_ACTIVATE_SECONDS", "").strip()
    env_entry = os.getenv("ENTRY_SECONDS_LEFT", "").strip()
    if listener_raw is not None:
        listener_secs = int(listener_raw)
    elif env_listener:
        listener_secs = int(env_listener)
    else:
        listener_secs = interval
    if entry_raw is not None:
        entry_secs = int(entry_raw)
    elif env_entry:
        entry_secs = int(env_entry)
    else:
        entry_secs = interval

    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        enabled = str(enabled).lower() in ("1", "true", "yes", "on")

    return WorkerConfig(
        asset=asset,
        window=window,
        momentum_lookback_ms=momentum_lookback_ms,
        momentum_min_delta=momentum_min_delta,
        momentum_mode=momentum_mode,
        momentum_size_min=momentum_size_min,
        momentum_size_max=momentum_size_max,
        momentum_max_shares=momentum_max_shares,
        trade_cooldown_ms=trade_cooldown_ms,
        dry_run=dry_run,
        dry_run_fill_delay_min_ms=dry_min,
        dry_run_fill_delay_max_ms=dry_max,
        listener_activate_secs=listener_secs,
        entry_seconds_left=entry_secs,
        enabled=enabled,
    )


def load_worker_configs(path: Optional[str] = None) -> Tuple[WorkerConfig, ...]:
    cfg_path = path or os.getenv("TRADING_CONFIG_PATH", "trading_config.json")
    if not os.path.isfile(cfg_path):
        _fatal(f"Trading config not found: {cfg_path}")

    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        _fatal(f"Invalid JSON in {cfg_path}: {e}")
    except OSError as e:
        _fatal(f"Cannot read {cfg_path}: {e}")

    if not isinstance(data, dict):
        _fatal(f"{cfg_path} must be a JSON object.")

    defaults = data.get("defaults") or {}
    workers_raw = data.get("workers")
    if not isinstance(workers_raw, list) or not workers_raw:
        _fatal(f"{cfg_path} must contain a non-empty 'workers' array.")

    seen: set[str] = set()
    out: list[WorkerConfig] = []
    for entry in workers_raw:
        if not isinstance(entry, dict):
            _fatal("Each worker entry must be a JSON object.")
        wc = _merge_worker_entry(entry, defaults)
        if not wc.enabled:
            continue
        if wc.key in seen:
            _fatal(f"Duplicate worker config: {wc.key}")
        seen.add(wc.key)
        out.append(wc)

    if not out:
        _fatal("No enabled workers in trading config.")

    return tuple(out)


WORKER_CONFIGS: Tuple[WorkerConfig, ...] = load_worker_configs()
TRADING_ASSETS: Tuple[str, ...] = tuple(dict.fromkeys(w.asset for w in WORKER_CONFIGS))
TRADING_ASSETS_UPPER: Tuple[str, ...] = tuple(a.upper() for a in TRADING_ASSETS)
ALL_TRACKED_ASSETS = TRADING_ASSETS
TOTAL_BOTS: int = len(WORKER_CONFIGS)


def asset_pnl_filename(asset: str, window: str = "5m") -> str:
    a = normalize_asset_slug(asset)
    w = normalize_window(window)
    return f"{a}_{w}_pnl_history.json"


PNL_FILES: list[str] = [asset_pnl_filename(w.asset, w.window) for w in WORKER_CONFIGS]


def validate_trading_assets() -> Tuple[str, ...]:
    if not TRADING_ASSETS:
        _fatal("No trading assets resolved from worker config.")
    return TRADING_ASSETS


def trading_assets_label(separator: str = " · ") -> str:
    labels = [f"{w.asset.upper()} {w.window}" for w in WORKER_CONFIGS]
    return separator.join(labels)


def _parse_positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        _fatal(f"{name}={raw!r} is not a valid number.")
    if value <= 0 or value != value or value in (float("inf"), float("-inf")):
        _fatal(f"{name} must be a positive number (got {raw!r}).")
    return value


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        _fatal(f"{name}={raw!r} is not a valid integer.")
    if value <= 0:
        _fatal(f"{name} must be a positive integer (got {raw!r}).")
    return value


ASSET_MAX_CUMULATIVE_LOSS: float = _parse_positive_float_env(
    "ASSET_MAX_CUMULATIVE_LOSS", 3.00,
)
ASSET_COOLDOWN_MINUTES: int = _parse_positive_int_env("ASSET_COOLDOWN_MINUTES", 30)
ASSET_COOLDOWN_SECONDS: int = ASSET_COOLDOWN_MINUTES * 60


def validate_asset_cooldown_config() -> tuple[float, int]:
    return ASSET_MAX_CUMULATIVE_LOSS, ASSET_COOLDOWN_MINUTES


print(
    f"📌 Workers ({len(WORKER_CONFIGS)}): "
    + ", ".join(f"{w.asset.upper()} {w.window}" for w in WORKER_CONFIGS)
)
print(
    f"🛡️  Asset cooldown: max loss ${ASSET_MAX_CUMULATIVE_LOSS:.2f} | "
    f"cooldown {ASSET_COOLDOWN_MINUTES} min (per asset+window)"
)
print(f"🧪 DRY_RUN_DEFAULT={DRY_RUN_DEFAULT}")
if WORKER_CONFIGS:
    wc0 = WORKER_CONFIGS[0]
    size_label = (
        f"{wc0.momentum_size_min}-{wc0.momentum_size_max}"
        if wc0.momentum_size_min != wc0.momentum_size_max
        else str(wc0.momentum_size_min)
    )
    sizing_src = ".env override" if (ENV_SIZING_OVERRIDES or ENV_SIZE_RANGE_OVERRIDES) else "trading_config.json"
    print(
        f"📐 Sizing ({sizing_src}): size={size_label} random | "
        f"max_shares={wc0.momentum_max_shares}"
    )
    print(
        f"📈 Momentum: lookback={wc0.momentum_lookback_ms}ms | "
        f"min_Δ={wc0.momentum_min_delta:.4f} ({wc0.momentum_min_delta * 100:.3f}%) | "
        f"mode={wc0.momentum_mode}"
        + (
            " [.env override]"
            if ENV_MOMENTUM_OVERRIDES
            else ""
        )
    )
