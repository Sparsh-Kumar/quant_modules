import argparse

from strategy.base_strategy import StrategyBase


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description='Spread strategy: mid (book 1) - mid (book 2).')
  p.add_argument('-s', '--shm', required=True, nargs='+', metavar='NAME', help='Shared memory name(s) to read orderbooks from (min 2), e.g. ob_ex1 ob_ex2')
  return p.parse_args()


class SpreadStrategy(StrategyBase):

  def on_snapshots(self, snapshots: list[dict]) -> None:
    a, b = snapshots[0], snapshots[1]
    if not (a.get('asks') and a.get('bids') and b.get('asks') and b.get('bids')):
      return
    mid_a = (a['bids'][0][0] + a['asks'][0][0]) / 2
    mid_b = (b['bids'][0][0] + b['asks'][0][0]) / 2
    spread = mid_a - mid_b
    print(f'mid_a: {mid_a}, mid_b: {mid_b}, spread: {spread}')


if __name__ == '__main__':
  args = _parse_args()
  if len(args.shm) < 2:
    raise SystemExit('At least 2 shared memory names required.')
  SpreadStrategy(args.shm).run()
