"""Strategy protocol for momentum trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional, Protocol

if TYPE_CHECKING:
    from bot import MarketWorker

MomentumMode = Literal["single_taker", "gtc_at_ask", "single_maker", "dual_hybrid"]


@dataclass(frozen=True)
class MomentumDecision:
    side: str
    price: float
    size: float
    signal_delta: float
    mode: MomentumMode
    signal_direction: str = "UP"


class MomentumStrategyProtocol(Protocol):
    async def evaluate(self, worker: "MarketWorker") -> Optional[MomentumDecision]:
        ...

    async def execute(self, worker: "MarketWorker", decision: MomentumDecision) -> None:
        ...
