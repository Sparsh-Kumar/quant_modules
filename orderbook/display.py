import os

from models import OrderbookSnapshot


def display_orderbook(orderbook: OrderbookSnapshot, depth: int = 10, symbol: str | None = None, exchange: str | None = None, shm_name: str | None = None) -> None:
  os.system('cls' if os.name == 'nt' else 'clear')
  bids = orderbook.get('bids', [])[:depth]
  asks = orderbook.get('asks', [])[:depth]
  ts = orderbook.get('timestamp') or orderbook.get('ts')

  sep = '  ' + '-' * 28

  def format_row(price: float, amount: float) -> str:
    return f'  {float(price):>12.2f}  {float(amount):>12.6f}'

  def format_levels(levels: list, reverse: bool = False) -> list[str]:
    order = reversed(levels) if reverse else levels
    return [format_row(p, a) for p, a in order]

  lines = ['']
  if exchange:
    lines.append(f'  {exchange}')
  if symbol:
    lines.append(f'  {symbol}')
  if shm_name:
    lines.append(f'  shm: {shm_name}')
  lines.extend(['  ASKS', sep, f'  {"Price":>12}  {"Amount":>12}', sep])
  lines.extend(format_levels(asks, reverse=True))
  lines.append(sep)
  lines.extend(format_levels(bids))
  lines.extend([sep, '  BIDS'])
  if ts:
    lines.append(f'  (updated: {ts})')
  lines.append('')
  print('\n'.join(lines))
