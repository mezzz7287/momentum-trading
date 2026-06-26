"""Momentum strategy — enter Polymarket UP/DOWN on Binance price impulse."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from binance_feed import get_momentum_signal
from strategies.base import MomentumDecision
from strategies.momentum_execution import execute_momentum_decision

if TYPE_CHECKING:
    from bot import MarketWorker


class MomentumStrategy:
    async def evaluate(self, worker: "MarketWorker") -> Optional[MomentumDecision]:
        from bot import MomentumState, is_locked_price

        if worker.momentum_state == MomentumState.PENDING:
            return None

        cfg = worker.worker_config
        direction = get_momentum_signal(
            worker.asset_type, cfg.momentum_lookback_ms, cfg.momentum_min_delta,
        )
        if direction is None:
            return None

        side = "YES" if direction == "UP" else "NO"
        ask = worker.prices.get(side, 0.0)
        if ask <= 0 or is_locked_price(ask):
            return None

        size = worker.momentum_order_size(side)
        if size is None:
            return None

        window = worker.last_momentum_delta
        return MomentumDecision(
            side=side,
            price=round(ask, 2),
            size=size,
            signal_delta=window,
            mode=cfg.momentum_mode,
            signal_direction=direction,
        )

    async def execute(self, worker: "MarketWorker", decision: MomentumDecision) -> None:
        await execute_momentum_decision(worker, decision)
