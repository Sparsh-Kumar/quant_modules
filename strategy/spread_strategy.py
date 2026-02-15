import argparse

from strategy.base_strategy import StrategyBase


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description='Spread strategy: best ask (book 1) - best bid (book 2).')
  p.add_argument('-s', '--shm', required=True, nargs='+', metavar='NAME', help='Shared memory name(s) to read orderbooks from (min 2), e.g. ob_ex1 ob_ex2')
  return p.parse_args()


class SpreadStrategy(StrategyBase):

  def on_snapshots(self, snapshots: list[dict]) -> None:
    a, b = snapshots[0], snapshots[1]
    if not (a.get('asks') and a.get('bids') and b.get('asks') and b.get('bids')):
      return
    best_ask_a = a['asks'][0][0]
    best_bid_b = b['bids'][0][0]
    spread = best_ask_a - best_bid_b
    print(f'best_ask_a: {best_ask_a}, best_bid_b: {best_bid_b}, spread: {spread}')


if __name__ == '__main__':
  args = _parse_args()
  if len(args.shm) < 2:
    raise SystemExit('At least 2 shared memory names required.')
  SpreadStrategy(args.shm).run()
