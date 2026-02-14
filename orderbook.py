import argparse
import asyncio
import struct
import msgpack
import ccxt.pro as ccxtpro
from display import display_orderbook
from multiprocessing import shared_memory

from models import OrderbookConfig, OrderbookSnapshot


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description='Stream orderbook for one or more symbols on one or more exchanges.')
  p.add_argument('-e', '--exchange', required=True, nargs='+', help='Exchange id(s), e.g. binance bybit')
  p.add_argument('-s', '--symbol', required=True, nargs='+', help='Symbol(s), e.g. BTC/USDT:USDT ETH/USDT:USDT')
  p.add_argument('-l', '--limit', type=int, default=50, help='Orderbook depth (default: 50)')
  p.add_argument('--no-display', action='store_true', help='Disable terminal display')
  return p.parse_args()


class Orderbook:

  def __init__(self, config: OrderbookConfig | None = None, **kwargs: object) -> None:
    cfg = config if isinstance(config, OrderbookConfig) else OrderbookConfig(**kwargs)
    self._exchange_id, self._symbol, self._limit = cfg.exchange_id, cfg.symbol, cfg.limit
    self._exchange = (getattr(ccxtpro, self._exchange_id))()
    self._shm_name = cfg.shm_name or f'orderbook_{cfg.exchange_id}_{cfg.symbol}_{cfg.limit}'
    self._shm = shared_memory.SharedMemory(name=cfg.shm_name) if cfg.shm_name else None
    self._display, self._version = cfg.display, 0

  async def close(self) -> None:
    await self._exchange.close()

  async def stream_orderbook(self, symbol: str | None = None, limit: int = 50) -> None:
    symbol = symbol or self._symbol
    limit = limit or self._limit
    if not symbol:
      raise ValueError('symbol for orderbook streaming is not defined.')
    while True:
      ob: OrderbookSnapshot = await self._exchange.watch_order_book(symbol, limit)
      payload = msgpack.packb({'bids': ob['bids'], 'asks': ob['asks'], 'ts': ob['timestamp']})
      if self._shm:
        self._version += 1
        struct.pack_into('q', self._shm.buf, 0, self._version)
        struct.pack_into('i', self._shm.buf, 8, len(payload))
        self._shm.buf[12:12 + len(payload)] = payload
        self._version += 1
        struct.pack_into('q', self._shm.buf, 0, self._version)
      if self._display:
        display_orderbook(ob, depth=min(limit, 10), symbol=symbol)


async def _run() -> None:
  args = _parse_args()
  orderbooks = [
    Orderbook(config=OrderbookConfig(
      exchange_id=ex, symbol=sym, limit=args.limit,
      display=not args.no_display,
    ))
    for ex in args.exchange
    for sym in args.symbol
  ]
  tasks = [asyncio.create_task(ob.stream_orderbook()) for ob in orderbooks]
  try:
    await asyncio.gather(*tasks)
  finally:
    for ob in orderbooks:
      await ob.close()


if __name__ == '__main__':
  asyncio.run(_run())
