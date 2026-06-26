"""Concurrent GTC bid placement and one-leg-fill handling for spread capture."""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Dict, List, Tuple

from strategies.base import SpreadDecision

if TYPE_CHECKING:
    from bot import MarketWorker


async def _simulate_dry_leg(
    worker: "MarketWorker",
    side: str,
    price: float,
    size: float,
) -> Tuple[str, float, float, float, bool]:
    """Return (side, size, price, delay_sec, completed_in_time)."""
    cfg = worker.worker_config
    delay_ms = random.randint(cfg.dry_run_fill_delay_min_ms, cfg.dry_run_fill_delay_max_ms)
    await asyncio.sleep(delay_ms / 1000.0)
    completed_in_time = delay_ms <= cfg.trade_cooldown_ms
    return side, size, price, delay_ms / 1000.0, completed_in_time


async def execute_spread_decision(worker: "MarketWorker", decision: SpreadDecision) -> None:
    from bot import SpreadState

    cfg = worker.worker_config
    mode_label = decision.mode.upper()
    yes_c = round(decision.yes_price * 100)
    no_c = round(decision.no_price * 100)

    legs: List[Tuple[str, float]] = []
    if decision.mode == "dual":
        legs = [("YES", decision.yes_price), ("NO", decision.no_price)]
    elif decision.rebalance_side:
        px = (
            decision.yes_price if decision.rebalance_side == "YES"
            else decision.no_price
        )
        legs = [(decision.rebalance_side, px)]

    if not legs:
        return

    for side, _price in legs:
        if not worker.validate_spread_order_size(side, decision.size):
            print(
                f"❌ [SPREAD ABORT] {worker.asset_type.upper()} {worker.window_slug} | "
                f"{side} size={decision.size} failed pre-submit sanity check"
            )
            return

    if worker.is_dry_run():
        worker.spread_state = SpreadState.PENDING
        try:
            print(
                f"\n🧪 [DRY SPREAD] {worker.asset_type.upper()} {worker.window_slug} | "
                f"mode={mode_label} edge={decision.edge:.4f} size={decision.size} | "
                f"YES@{yes_c}c NO@{no_c}c | sim delay "
                f"{cfg.dry_run_fill_delay_min_ms}-{cfg.dry_run_fill_delay_max_ms}ms/leg"
            )
            start = time.monotonic()
            leg_tasks = [
                asyncio.create_task(_simulate_dry_leg(worker, side, price, decision.size))
                for side, price in legs
            ]
            await asyncio.sleep(cfg.trade_cooldown_ms / 1000.0)

            fills: Dict[str, Tuple[float, float]] = {}
            for task in leg_tasks:
                if not task.done():
                    task.cancel()
                    continue
                try:
                    side, size, price, delay_sec, in_time = task.result()
                except asyncio.CancelledError:
                    continue
                if in_time:
                    fills[side] = (size, price)
                    worker.spread_inventory.record_buy(side, size, price)
                    worker.log_trade(side, price, "buy", size=size)
                    print(
                        f"  🧪 [DRY FILL] {side} {size:.2f}@{round(price*100)}c "
                        f"after {delay_sec:.2f}s"
                    )
                else:
                    print(
                        f"  🧪 [DRY CANCEL] {side} simulated fill too slow "
                        f"({delay_sec:.2f}s > {cfg.trade_cooldown_ms}ms cooldown)"
                    )

            if decision.mode == "dual":
                yes_fill = fills.get("YES", (0.0, 0.0))[0]
                no_fill = fills.get("NO", (0.0, 0.0))[0]
                if yes_fill > 0 and no_fill <= 0:
                    print("  🧪 [DRY] One-leg: YES filled, NO cancelled")
                elif no_fill > 0 and yes_fill <= 0:
                    print("  🧪 [DRY] One-leg: NO filled, YES cancelled")

            worker._log_spread_capture(decision, fills=fills or None, dry_run=True)
            elapsed = time.monotonic() - start
            print(f"  🧪 [DRY SPREAD] cycle done in {elapsed:.2f}s")
        finally:
            worker.spread_state = SpreadState.IDLE
        return

    worker.spread_state = SpreadState.PENDING
    try:
        print(
            f"\n📊 [SPREAD] {worker.asset_type.upper()} {worker.window_slug} | "
            f"{mode_label} edge={decision.edge:.4f} | "
            + " ".join(f"{s}@{round(p*100)}c" for s, p in legs)
        )

        order_size = float(decision.size)
        placed = await asyncio.gather(
            *[
                worker.place_spread_gtc(side, price, order_size)
                for side, price in legs
            ]
        )

        await asyncio.sleep(cfg.trade_cooldown_ms / 1000.0)

        fills: Dict[str, Tuple[float, float]] = {}
        for (side, price), result in zip(legs, placed):
            order_id, fill_size = result
            if order_id and order_id != "dry-run":
                fill_size = await worker.poll_order_fill(order_id, order_size)
            if fill_size > 0:
                fills[side] = (fill_size, price)
                worker.spread_inventory.record_buy(side, fill_size, price)
                worker.log_trade(side, price, "buy", size=fill_size)
            elif order_id and order_id not in ("dry-run", None):
                worker._try_cancel_order(order_id)

        if decision.mode == "dual":
            yes_fill = fills.get("YES", (0.0, 0.0))[0]
            no_fill = fills.get("NO", (0.0, 0.0))[0]
            if yes_fill > 0 and no_fill <= 0:
                no_oid = placed[1][0] if len(placed) > 1 else None
                if no_oid:
                    worker._try_cancel_order(no_oid)
            elif no_fill > 0 and yes_fill <= 0:
                yes_oid = placed[0][0] if placed else None
                if yes_oid:
                    worker._try_cancel_order(yes_oid)

        worker._log_spread_capture(decision, fills=fills)
    finally:
        worker.spread_state = SpreadState.IDLE
