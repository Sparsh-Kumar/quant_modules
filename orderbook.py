import argparse
import asyncio
import struct
import sys
import msgpack
import ccxt.pro as ccxtpro
from display import display_orderbook
from multiprocessing import shared_memory

from helpers import PAYLOAD_MAX, PAYLOAD_OFFSET, SIZE_OFFSET, VERSION_OFFSET
from models import OrderbookConfig, OrderbookSnapshot


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description='Stream orderbook for a single symbol on one exchange.')
  p.add_argument('-e', '--exchange', required=True, help='Exchange id, e.g. binance')
  p.add_argument('-s', '--symbol', required=True, help='Symbol, e.g. BTC/USDT:USDT')
  p.add_argument('-l', '--limit', type=int, default=50, help='Orderbook depth (default: 50)')
  p.add_argument('--shm-name', metavar='NAME', help='Shared memory name to write orderbook to')
  p.add_argument('--no-display', action='store_true', help='Disable terminal display')
  return p.parse_args()


class Orderbook:

  def __init__(self, config: OrderbookConfig | None = None, **kwargs: object) -> None:
    cfg = config if isinstance(config, OrderbookConfig) else OrderbookConfig(**kwargs)
    self._exchange_id, self._symbol, self._limit = cfg.exchange_id, cfg.symbol, cfg.limit
    if not self._exchange_id:
      raise ValueError('exchange_id is required.')
    self._exchange = (getattr(ccxtpro, self._exchange_id))()
    self._shm_name = cfg.shm_name or f'orderbook_{cfg.exchange_id}_{cfg.symbol}'
    if cfg.shm_name:
      self._shm = shared_memory.SharedMemory(
        name=cfg.shm_name, create=True, size=PAYLOAD_OFFSET + PAYLOAD_MAX,
      )
    else:
      self._shm = None
    self._display, self._version = cfg.display, 0

  async def close(self) -> None:
    await self._exchange.close()
    if self._shm:
      self._shm.close()

  async def stream_orderbook(self, symbol: str | None = None, limit: int = 50) -> None:
    symbol = symbol or self._symbol
    limit = limit or self._limit
    if not symbol:
      raise ValueError('symbol for orderbook streaming is not defined.')
    while True:
      ob: OrderbookSnapshot = await self._exchange.watch_order_book(symbol, limit)
      payload = msgpack.packb({'bids': ob['bids'], 'asks': ob['asks'], 'ts': ob['timestamp']})
      if self._shm and len(payload) <= PAYLOAD_MAX:
        self._version += 1
        struct.pack_into('q', self._shm.buf, VERSION_OFFSET, self._version)
        struct.pack_into('i', self._shm.buf, SIZE_OFFSET, len(payload))
        self._shm.buf[PAYLOAD_OFFSET:PAYLOAD_OFFSET + len(payload)] = payload
        self._version += 1
        struct.pack_into('q', self._shm.buf, VERSION_OFFSET, self._version)
      if self._display:
        display_orderbook(
          ob, depth=min(limit, 10), symbol=symbol,
          exchange=self._exchange_id,
          shm_name=self._shm_name if self._shm else None,
        )


async def _run() -> None:
  args = _parse_args()
  orderbook = Orderbook(config=OrderbookConfig(
    exchange_id=args.exchange, symbol=args.symbol, limit=args.limit,
    shm_name=args.shm_name,
    display=not args.no_display,
  ))
  if args.shm_name:
    actual = orderbook._shm.name if orderbook._shm else args.shm_name
    print('Shared memory name:', actual, file=sys.stderr)
    print('Read with: python -m spread_strategy -s', actual, file=sys.stderr)
  try:
    await orderbook.stream_orderbook()
  finally:
    await orderbook.close()


if __name__ == '__main__':
  asyncio.run(_run())
