from dataclasses import dataclass
from typing import TypedDict

OrderbookLevel = tuple[float, float]


class OrderbookSnapshot(TypedDict):
  bids: list[OrderbookLevel]
  asks: list[OrderbookLevel]
  timestamp: int


@dataclass
class OrderbookConfig:
  exchange_id: str | None = None
  symbol: str | None = None
  limit: int = 50
  shm_name: str | None = None
  display: bool = True
