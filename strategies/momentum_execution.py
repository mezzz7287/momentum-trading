"""Execution modes for momentum strategy."""

from __future__ import annotations

import asyncio
import json
import random
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from strategies.base import MomentumDecision
from utils.clob_helpers import clamp_buy_price, maker_buy_price

if TYPE_CHECKING:
    from bot import MarketWorker


def _contra_side(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def _extract_fill_price(info: Dict[str, Any], limit_price: float) -> float:
    for key in ("avg_price", "average_price", "fill_price", "price"):
        raw = info.get(key)
        if raw is None:
            continue
        try:
            px = float(raw)
        except (TypeError, ValueError):
            continue
        if px > 1.0:
            px /= 100.0
        if px > 0:
            return round(px, 4)
    return round(limit_price, 4)


async def _poll_order_fill(
    worker: "MarketWorker",
    order_id: str,
    requested: float,
    limit_price: float,
) -> Tuple[float, float]:
    from bot import exec_log

    try:
        info = worker.account.get_order_status(order_id)
        if isinstance(info, str):
            info = json.loads(info)
        if isinstance(info, dict):
            matched = (
                info.get("size_matched")
                or info.get("sizeMatched")
                or info.get("matched_size")
                or 0
            )
            fill_size = min(float(matched), float(requested))
            if fill_size > 0:
                fill_price = _extract_fill_price(info, limit_price)
                exec_log(
                    "fill_confirmed",
                    order_id=order_id, side=None, size=fill_size,
                    price=fill_price, asset=worker.asset_type,
                    window=worker.window_slug,
                )
                return fill_size, fill_price
    except Exception as e:
        exec_log(
            "fill_timeout", order_id=order_id, error=str(e),
            asset=worker.asset_type, window=worker.window_slug,
        )
    return 0.0, round(limit_price, 4)


async def _simulate_dry_leg(
    worker: "MarketWorker",
    side: str,
    price: float,
    size: float,
) -> Tuple[str, float, float, float, bool]:
    cfg = worker.worker_config
    delay_ms = random.randint(cfg.dry_run_fill_delay_min_ms, cfg.dry_run_fill_delay_max_ms)
    await asyncio.sleep(delay_ms / 1000.0)
    completed_in_time = delay_ms <= cfg.trade_cooldown_ms
    return side, size, price, delay_ms / 1000.0, completed_in_time


async def _place_and_track(
    worker: "MarketWorker",
    side: str,
    price: float,
    size: float,
    order_type: str,
) -> Tuple[Optional[str], float, float]:
    from bot import exec_log

    if not worker.validate_momentum_order_size(side, size):
        return None, 0.0, price
    ok, order_id, filled_immediately = await worker.place_order_raw(
        side, price, size, order_type=order_type,
    )
    if not ok or not order_id:
        return None, 0.0, price
    exec_log(
        "order_placed", side=side, price=price, size=size,
        order_type=order_type, order_id=order_id,
        asset=worker.asset_type, window=worker.window_slug,
    )
    if filled_immediately and order_id == "dry-run":
        return order_id, size, price
    if filled_immediately:
        exec_log(
            "fill_confirmed", order_id=order_id, side=side,
            size=size, price=price, asset=worker.asset_type,
            window=worker.window_slug,
        )
        return order_id, size, price
    return order_id, 0.0, price


async def _finalize_after_cooldown(
    worker: "MarketWorker",
    legs: List[Tuple[str, Optional[str], float, float, float]],
) -> Dict[str, Tuple[float, float]]:
    """Poll each placed order after cooldown; cancel unfilled."""
    from bot import exec_log

    cfg = worker.worker_config
    await asyncio.sleep(cfg.trade_cooldown_ms / 1000.0)
    fills: Dict[str, Tuple[float, float]] = {}
    for side, order_id, order_size, limit_price, _kind in legs:
        if not order_id:
            continue
        if order_id == "dry-run":
            continue
        fill_size, fill_price = await _poll_order_fill(
            worker, order_id, order_size, limit_price,
        )
        if fill_size > 0:
            fills[side] = (fill_size, fill_price)
        else:
            exec_log(
                "fill_timeout", order_id=order_id, side=side,
                asset=worker.asset_type, window=worker.window_slug,
            )
            worker._try_cancel_order(order_id)
            exec_log(
                "order_cancelled", order_id=order_id, side=side,
                asset=worker.asset_type, window=worker.window_slug,
            )
    return fills


async def execute_momentum_decision(
    worker: "MarketWorker", decision: MomentumDecision,
) -> None:
    from bot import MomentumState, exec_log

    cfg = worker.worker_config
    mode = decision.mode
    side = decision.side
    size = float(decision.size)
    label = f"{worker.asset_type.upper()} {worker.window_slug}"

    exec_log(
        "signal_detected",
        direction=decision.signal_direction,
        side=side, delta=decision.signal_delta,
        mode=mode, asset=worker.asset_type, window=worker.window_slug,
    )

    if worker.is_dry_run():
        worker.momentum_state = MomentumState.PENDING
        try:
            print(
                f"\n🧪 [DRY MOMENTUM] {label} | mode={mode} "
                f"{decision.signal_direction} Δ={decision.signal_delta*100:.3f}% "
                f"| {side}@{round(decision.price*100)}c size={size}"
            )
            await _execute_dry(worker, decision)
        finally:
            worker.momentum_state = MomentumState.IDLE
        return

    worker.momentum_state = MomentumState.PENDING
    try:
        print(
            f"\n📈 [MOMENTUM] {label} | mode={mode} "
            f"{decision.signal_direction} Δ={decision.signal_delta*100:.3f}% "
            f"| {side} size={size}"
        )
        if mode == "single_taker":
            await _execute_single_taker(worker, decision)
        elif mode == "gtc_at_ask":
            await _execute_gtc_at_ask(worker, decision)
        elif mode == "single_maker":
            await _execute_single_maker(worker, decision)
        elif mode == "dual_hybrid":
            await _execute_dual_hybrid(worker, decision)
        else:
            exec_log("order_failed", error=f"unknown mode {mode!r}", asset=worker.asset_type)
    finally:
        worker.momentum_state = MomentumState.IDLE


async def _execute_single_taker(worker: "MarketWorker", decision: MomentumDecision) -> None:
    side = decision.side
    ask = worker.prices.get(side, decision.price)
    price = clamp_buy_price(ask, slippage=0.0)
    order_id, fill_size, fill_price = await _place_and_track(
        worker, side, price, decision.size, "FOK",
    )
    if fill_size > 0:
        worker.record_momentum_fill(side, fill_size, fill_price)


async def _execute_gtc_at_ask(worker: "MarketWorker", decision: MomentumDecision) -> None:
    side = decision.side
    price = round(worker.prices.get(side, decision.price), 2)
    order_id, _, _ = await _place_and_track(
        worker, side, price, decision.size, "GTC",
    )
    if not order_id:
        return
    if order_id == "dry-run":
        return
    fills = await _finalize_after_cooldown(
        worker, [(side, order_id, decision.size, price, "ask")],
    )
    if side in fills:
        fs, fp = fills[side]
        worker.record_momentum_fill(side, fs, fp)


async def _execute_single_maker(worker: "MarketWorker", decision: MomentumDecision) -> None:
    side = decision.side
    bid = worker.bids.get(side, 0.0)
    ask = worker.prices.get(side, decision.price)
    price = maker_buy_price(bid, ask)
    order_id, _, _ = await _place_and_track(
        worker, side, price, decision.size, "GTC",
    )
    if not order_id:
        return
    if order_id == "dry-run":
        return
    fills = await _finalize_after_cooldown(
        worker, [(side, order_id, decision.size, price, "bid")],
    )
    if side in fills:
        fs, fp = fills[side]
        worker.record_momentum_fill(side, fs, fp)


async def _execute_dual_hybrid(worker: "MarketWorker", decision: MomentumDecision) -> None:
    signal_side = decision.side
    contra = _contra_side(signal_side)
    taker_ask = worker.prices.get(signal_side, decision.price)
    taker_px = clamp_buy_price(taker_ask, slippage=0.0)
    contra_bid = worker.bids.get(contra, 0.0)
    contra_ask = worker.prices.get(contra, 0.0)
    maker_px = maker_buy_price(contra_bid, contra_ask)

    taker_res = await _place_and_track(
        worker, signal_side, taker_px, decision.size, "FOK",
    )
    maker_res = await _place_and_track(
        worker, contra, maker_px, decision.size, "GTC",
    )

    taker_id, taker_imm_size, taker_imm_px = taker_res
    maker_id, maker_imm_size, maker_imm_px = maker_res

    if taker_imm_size > 0:
        worker.record_momentum_fill(signal_side, taker_imm_size, taker_imm_px)

    pending: List[Tuple[str, Optional[str], float, float, float]] = []
    if maker_id and maker_imm_size <= 0 and maker_id != "dry-run":
        pending.append((contra, maker_id, decision.size, maker_px, "maker"))

    if pending:
        fills = await _finalize_after_cooldown(worker, pending)
        if contra in fills:
            fs, fp = fills[contra]
            worker.record_momentum_fill(contra, fs, fp)

    if taker_id and taker_imm_size <= 0 and maker_id and maker_imm_size <= 0:
        if taker_id != "dry-run":
            worker._try_cancel_order(taker_id)


async def _execute_dry(worker: "MarketWorker", decision: MomentumDecision) -> None:
    cfg = worker.worker_config
    mode = decision.mode
    side = decision.side
    size = decision.size

    if mode == "single_taker":
        legs = [(side, decision.price)]
        await asyncio.sleep(random.randint(
            cfg.dry_run_fill_delay_min_ms, cfg.dry_run_fill_delay_max_ms,
        ) / 1000.0)
        if True:
            worker.record_momentum_fill(side, size, decision.price)
            print(f"  🧪 [DRY MOMENTUM FILL] {side} {size:.2f}@{round(decision.price*100)}c")
        return

    if mode == "gtc_at_ask":
        legs = [(side, decision.price)]
    elif mode == "single_maker":
        bid = worker.bids.get(side, 0.0)
        ask = worker.prices.get(side, decision.price)
        legs = [(side, maker_buy_price(bid, ask))]
    elif mode == "dual_hybrid":
        contra = _contra_side(side)
        contra_bid = worker.bids.get(contra, 0.0)
        contra_ask = worker.prices.get(contra, 0.0)
        legs = [
            (side, clamp_buy_price(decision.price, slippage=0.0)),
            (contra, maker_buy_price(contra_bid, contra_ask)),
        ]
    else:
        legs = [(side, decision.price)]

    start = time.monotonic()
    leg_tasks = [
        asyncio.create_task(_simulate_dry_leg(worker, s, px, size))
        for s, px in legs
    ]
    await asyncio.sleep(cfg.trade_cooldown_ms / 1000.0)

    for task in leg_tasks:
        if not task.done():
            task.cancel()
            side_name = "?"
            print(f"  🧪 [DRY MOMENTUM CANCEL] leg timed out")
            continue
        try:
            leg_side, leg_size, leg_price, delay_sec, in_time = task.result()
        except asyncio.CancelledError:
            continue
        if in_time:
            worker.record_momentum_fill(leg_side, leg_size, leg_price)
            print(
                f"  🧪 [DRY MOMENTUM FILL] {leg_side} {leg_size:.2f}@"
                f"{round(leg_price*100)}c after {delay_sec:.2f}s"
            )
        else:
            print(
                f"  🧪 [DRY MOMENTUM CANCEL] {leg_side} too slow "
                f"({delay_sec:.2f}s > {cfg.trade_cooldown_ms}ms)"
            )

    elapsed = time.monotonic() - start
    print(f"  🧪 [DRY MOMENTUM] cycle done in {elapsed:.2f}s")
