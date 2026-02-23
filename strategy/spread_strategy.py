import argparse

from binance_ws_orders import write_order_request as write_binance_order
from bybit_ws_orders import write_order_request as write_bybit_order
from strategy.base_strategy import StrategyBase


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description='Spread strategy: mid (book 1) - mid (book 2).')
  p.add_argument('-s', '--shm', required=True, nargs='+', metavar='NAME', help='Shared memory name(s) to read orderbooks from (min 2), e.g. ob_ex1 ob_ex2')
  return p.parse_args()


class SpreadStrategy(StrategyBase):

  def __init__(self, shm_names: list[str]) -> None:
    super().__init__(shm_names)
    self._market_orders_sent = False

  def on_snapshots(self, snapshots: list[dict]) -> None:
    a, b = snapshots[0], snapshots[1]
    if not (a.get('asks') and a.get('bids') and b.get('asks') and b.get('bids')):
      return
    mid_a = (a['bids'][0][0] + a['asks'][0][0]) / 2
    mid_b = (b['bids'][0][0] + b['asks'][0][0]) / 2
    spread = mid_a - mid_b
    print(f'mid_a: {mid_a}, mid_b: {mid_b}, spread: {spread}')
    if not self._market_orders_sent:
      best_ask_a = a['asks'][0][0]
      best_bid_b = b['bids'][0][0]
      print(f'Sending BUY (Binance) at ref price {best_ask_a}')
      print(f'Sending SELL (Bybit) at ref price {best_bid_b}')
      write_binance_order({'symbol': 'BTCUSDT', 'side': 'BUY', 'type': 'MARKET', 'quantity': '0.001'})
      write_bybit_order({'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Market', 'qty': '0.001', 'category': 'linear'})
      self._market_orders_sent = True


if __name__ == '__main__':
  args = _parse_args()
  if len(args.shm) < 2:
    raise SystemExit('At least 2 shared memory names required.')
  SpreadStrategy(args.shm).run()
