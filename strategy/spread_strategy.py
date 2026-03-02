"""
Spread strategy: mid(book1) - mid(book2). Writes order requests to shm;
actual order placement is done by separate order-reader processes.

To get orders sent to exchanges:
  1. Start orderbook writers (e.g. binance_ob, bybit_ob).
  2. Start order readers: python -m binance_ws_orders  and  python -m bybit_ws_orders  (keep running).
  3. Run this strategy: python -m strategy.spread_strategy --binance-shm binance_ob --bybit-shm bybit_ob
"""
import argparse
import logging
import statistics
import sys
import time
from collections import deque

from binance_ws_orders import write_order_request as write_binance_order
from bybit_ws_orders import write_order_request as write_bybit_order
from strategy.base_strategy import StrategyBase


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description='Spread strategy: mid (Binance) - mid (Bybit).')
  p.add_argument('--binance-shm', required=True, metavar='NAME', help='Shared memory name for Binance orderbook (e.g. binance_ob)')
  p.add_argument('--bybit-shm', required=True, metavar='NAME', help='Shared memory name for Bybit orderbook (e.g. bybit_ob)')
  p.add_argument('--log-file', default='spread_strategy.log', metavar='PATH', help='Log file path for each iteration (default: spread_strategy.log)')
  return p.parse_args()


logger = logging.getLogger(__name__)


def _setup_logging(log_file: str) -> None:
  """Send log messages to both file and terminal (stdout)."""
  logger.setLevel(logging.INFO)
  logger.handlers.clear()
  logger.propagate = False
  fmt = logging.Formatter('%(message)s')
  fh = logging.FileHandler(log_file, mode='a', encoding='utf-8')
  fh.setFormatter(fmt)
  logger.addHandler(fh)
  sh = logging.StreamHandler(sys.stdout)
  sh.setFormatter(fmt)
  logger.addHandler(sh)

_PROFIT_THRESHOLD = 0.05 * 4  # spread_pct must be > this (0.2) to be profitable


class _OrdersSent(Exception):
  pass


class SpreadStrategy(StrategyBase):

  def __init__(self, shm_names: list[str]) -> None:
    super().__init__(shm_names)
    self._market_orders_sent = False
    self._index = 0
    self._spreads: deque[float] = deque(maxlen=2000)

  def run(self) -> None:
    try:
      super().run()
    except _OrdersSent as e:
      logger.info('Exiting after sending orders: %s', e)
      for shm in self._shms:
        try:
          shm.close()
        except Exception:
          pass
      sys.exit(0)  # exit immediately after order is punched

  def on_snapshots(self, snapshots: list[dict]) -> None:
    binance_book, bybit_book = snapshots[0], snapshots[1]
    a, b = binance_book, bybit_book
    if not (a.get('asks') and a.get('bids') and b.get('asks') and b.get('bids')):
      return
    mid_a = (a['bids'][0][0] + a['asks'][0][0]) / 2
    mid_b = (b['bids'][0][0] + b['asks'][0][0]) / 2
    spread = mid_a - mid_b
    self._spreads.append(spread)
    if len(self._spreads) < 2000:
      return
    t0 = time.perf_counter()
    stdev = statistics.stdev(self._spreads)
    z_score = (spread - statistics.mean(self._spreads)) / stdev if stdev > 0 else 0.0
    latency_ms = (time.perf_counter() - t0) * 1000
    line = f'index={self._index} mid_a={mid_a} mid_b={mid_b} spread={spread} z_score={z_score} latency_ms={latency_ms:.3f}'
    if abs(z_score) >= 2:
      best_bid_a, best_ask_a = a['bids'][0][0], a['asks'][0][0]
      best_bid_b, best_ask_b = b['bids'][0][0], b['asks'][0][0]
      if z_score > 0:
        real_spread = best_bid_a - best_ask_b
        buy_price = best_ask_b
      else:
        real_spread = best_bid_b - best_ask_a
        buy_price = best_ask_a
      spread_pct = (real_spread / buy_price) * 100 if buy_price > 0 else 0.0
      profitable = spread_pct > _PROFIT_THRESHOLD
      line += f' | spread_pct={spread_pct:.4f}% threshold={_PROFIT_THRESHOLD} profitable={profitable}'
      if profitable and not self._market_orders_sent:
        qty = '0.002'
        if z_score > 0:  # spread high → short Binance, long Bybit (mean revert down)
          line += f' | Orders: Binance SELL @ {best_bid_a} (short), Bybit BUY @ {best_ask_b} (long)'
          write_binance_order({'symbol': 'BTCUSDT', 'side': 'SELL', 'type': 'MARKET', 'quantity': qty})
          write_bybit_order({'symbol': 'BTCUSDT', 'side': 'Buy', 'orderType': 'Market', 'qty': qty, 'category': 'linear'})
        else:  # spread low → long Binance, short Bybit (mean revert up)
          line += f' | Orders: Binance BUY @ {best_ask_a} (long), Bybit SELL @ {best_bid_b} (short)'
          write_binance_order({'symbol': 'BTCUSDT', 'side': 'BUY', 'type': 'MARKET', 'quantity': qty})
          write_bybit_order({'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Market', 'qty': qty, 'category': 'linear'})
        self._market_orders_sent = True
        logger.info(line)  # log to file only when order is punched
        raise _OrdersSent('Orders sent.')
    print(line, flush=True)  # console only; file grows only on order
    self._index += 1


if __name__ == '__main__':
  args = _parse_args()
  _setup_logging(args.log_file)
  SpreadStrategy([args.binance_shm, args.bybit_shm]).run()
