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
import math
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
_ENTRY_SLIPPAGE_PCT = 0.02  # assume 0.02% worse fill when recording entry prices


def _mean_and_stdev(values: deque[float]) -> tuple[float, float]:
  """Single-pass mean and sample stdev over values. Returns (mean, stdev); stdev=0 if n<2."""
  n = len(values)
  if n < 2:
    return (values[0], 0.0) if n == 1 else (0.0, 0.0)
  sum_x = 0.0
  sum_x2 = 0.0
  for x in values:
    sum_x += x
    sum_x2 += x * x
  mean = sum_x / n
  variance = (sum_x2 - sum_x * sum_x / n) / (n - 1)
  stdev = math.sqrt(variance) if variance > 0 else 0.0
  return mean, stdev


class _OrdersSent(Exception):
  pass


class SpreadStrategy(StrategyBase):

  def __init__(self, shm_names: list[str]) -> None:
    super().__init__(shm_names)
    self._market_orders_sent = False
    self._position_direction: int | None = None  # 1 = short Binance / long Bybit, -1 = long Binance / short Bybit
    self._entry_price_binance: float | None = None
    self._entry_price_bybit: float | None = None
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
      sys.exit(0)  # exit after one complete trade (entry + exit)

  def on_snapshots(self, snapshots: list[dict]) -> None:
    binance_book, bybit_book = snapshots[0], snapshots[1]
    a, b = binance_book, bybit_book
    if not (a.get('asks') and a.get('bids') and b.get('asks') and b.get('bids')):
      return
    best_bid_a, best_ask_a = a['bids'][0][0], a['asks'][0][0]
    best_bid_b, best_ask_b = b['bids'][0][0], b['asks'][0][0]

    # Position open: check if (exit - entry) on each exchange sums to > 0, then close and exit.
    if self._market_orders_sent and self._position_direction is not None and self._entry_price_binance is not None and self._entry_price_bybit is not None:
      exit_price_binance = best_ask_a if self._position_direction == 1 else best_bid_a  # close short @ ask, close long @ bid
      exit_price_bybit = best_bid_b if self._position_direction == 1 else best_ask_b   # close long @ bid, close short @ ask
      # Long leg: net = exit - entry. Short leg: net = entry - exit.
      if self._position_direction == 1:  # Binance short, Bybit long
        exchange_1_net = self._entry_price_binance - exit_price_binance
        exchange_2_net = exit_price_bybit - self._entry_price_bybit
      else:  # Binance long, Bybit short
        exchange_1_net = exit_price_binance - self._entry_price_binance
        exchange_2_net = self._entry_price_bybit - exit_price_bybit
      net_value = exchange_1_net + exchange_2_net
      if net_value > 0:
        qty = '0.002'
        if self._position_direction == 1:
          write_binance_order({'symbol': 'BTCUSDT', 'side': 'BUY', 'type': 'MARKET', 'quantity': qty})
          write_bybit_order({'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Market', 'qty': qty, 'category': 'linear'})
          line = f'Close: Binance BUY @ {exit_price_binance}, Bybit SELL @ {exit_price_bybit} | ex1_net={exchange_1_net} ex2_net={exchange_2_net} net_value={net_value}'
        else:
          write_binance_order({'symbol': 'BTCUSDT', 'side': 'SELL', 'type': 'MARKET', 'quantity': qty})
          write_bybit_order({'symbol': 'BTCUSDT', 'side': 'Buy', 'orderType': 'Market', 'qty': qty, 'category': 'linear'})
          line = f'Close: Binance SELL @ {exit_price_binance}, Bybit BUY @ {exit_price_bybit} | ex1_net={exchange_1_net} ex2_net={exchange_2_net} net_value={net_value}'
        logger.info(line)
        raise _OrdersSent('Position closed.')  # one trade done → exit program
      logger.info(f'Position open | ex1_net={exchange_1_net} ex2_net={exchange_2_net} net_value={net_value} (waiting for >0)')
      return

    # No position: run entry logic (spread history, z-score, maybe open).
    mid_a = (best_bid_a + best_ask_a) / 2
    mid_b = (best_bid_b + best_ask_b) / 2
    spread = mid_a - mid_b
    self._spreads.append(spread)
    if len(self._spreads) < 2000:
      return
    t0 = time.perf_counter()
    mean, stdev = _mean_and_stdev(self._spreads)
    z_score = (spread - mean) / stdev if stdev > 0 else 0.0
    latency_ms = (time.perf_counter() - t0) * 1000
    line = f'index={self._index} mid_a={mid_a} mid_b={mid_b} spread={spread} z_score={z_score} latency_ms={latency_ms:.3f}'
    if abs(z_score) >= 4:
      # if z_score > 0:
      #   real_spread = best_bid_a - best_ask_b
      #   buy_price = best_ask_b
      # else:
      #   real_spread = best_bid_b - best_ask_a
      #   buy_price = best_ask_a
      # spread_pct = (real_spread / buy_price) * 100 if buy_price > 0 else 0.0
      # profitable = spread_pct > _PROFIT_THRESHOLD
      # line += f' | spread_pct={spread_pct:.4f}% threshold={_PROFIT_THRESHOLD} profitable={profitable}'
      # Enter based on spread (|z|) only; profitability check commented out.
      if not self._market_orders_sent:
        qty = '0.002'
        if z_score > 0:  # spread high → short Binance, long Bybit (mean revert down)
          line += f' | Orders: Binance SELL @ {best_bid_a} (short), Bybit BUY @ {best_ask_b} (long)'
          write_binance_order({'symbol': 'BTCUSDT', 'side': 'SELL', 'type': 'MARKET', 'quantity': qty})
          write_bybit_order({'symbol': 'BTCUSDT', 'side': 'Buy', 'orderType': 'Market', 'qty': qty, 'category': 'linear'})
          self._position_direction = 1
          # Slippage: sell fills worse (receive less), buy fills worse (pay more)
          self._entry_price_binance = best_bid_a * (1 - _ENTRY_SLIPPAGE_PCT / 100)
          self._entry_price_bybit = best_ask_b * (1 + _ENTRY_SLIPPAGE_PCT / 100)
        else:  # spread low → long Binance, short Bybit (mean revert up)
          line += f' | Orders: Binance BUY @ {best_ask_a} (long), Bybit SELL @ {best_bid_b} (short)'
          write_binance_order({'symbol': 'BTCUSDT', 'side': 'BUY', 'type': 'MARKET', 'quantity': qty})
          write_bybit_order({'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Market', 'qty': qty, 'category': 'linear'})
          self._position_direction = -1
          self._entry_price_binance = best_ask_a * (1 + _ENTRY_SLIPPAGE_PCT / 100)
          self._entry_price_bybit = best_bid_b * (1 - _ENTRY_SLIPPAGE_PCT / 100)
        self._market_orders_sent = True
        logger.info(line)
        # Do not exit; keep running to monitor for profitable close.
      else:
        print(line, flush=True)
    else:
      print(line, flush=True)
    self._index += 1


if __name__ == '__main__':
  args = _parse_args()
  _setup_logging(args.log_file)
  SpreadStrategy([args.binance_shm, args.bybit_shm]).run()
