"""
Binance USDⓈ-M Futures WebSocket API: place market and limit orders.
Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api

Credentials are read from .env: BINANCE_API_KEY, BINANCE_API_SECRET (or pass explicitly).

- BinanceFuturesWSClient: one connection, reuse for multiple orders.
- place_market_order / place_limit_order: one-off orders.
- write_order_request: write an order spec to shm "binance_order" for the runner to send.
- run_order_reader: open WS connection, read from shm "binance_order" continuously, send orders.
"""

import struct
import hmac
import hashlib
import json
import msgpack
import os
import time
import uuid
from multiprocessing import shared_memory

import websocket  # type: ignore[import-untyped]
from dotenv import load_dotenv  # type: ignore[import-untyped]

from helpers import PAYLOAD_MAX, PAYLOAD_OFFSET, SIZE_OFFSET, VERSION_OFFSET

load_dotenv()

_BASE_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
_ORDER_SHM_NAME = "binance_order"


def _get_binance_credentials(api_key: str | None, api_secret: str | None) -> tuple[str, str]:
  key = api_key or os.getenv("BINANCE_API_KEY")
  secret = api_secret or os.getenv("BINANCE_API_SECRET")
  if not key or not secret:
    raise ValueError("Set BINANCE_API_KEY and BINANCE_API_SECRET in .env or pass them explicitly.")
  return key, secret


def _sign(params: dict, secret: str) -> str:
  """HMAC-SHA256 signature; params sorted alphabetically, query string signed."""
  query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
  return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def _build_order_params(
  symbol: str,
  side: str,
  order_type: str,
  quantity: str,
  *,
  price: str | None = None,
  time_in_force: str | None = None,
  reduce_only: bool = False,
  position_side: str | None = None,
  new_client_order_id: str | None = None,
  recv_window: int = 5000,
) -> dict:
  params = {
    "symbol": symbol,
    "side": side.upper(),
    "type": order_type,
    "quantity": quantity,
    "timestamp": int(time.time() * 1000),
  }
  if recv_window != 5000:
    params["recvWindow"] = recv_window
  if reduce_only:
    params["reduceOnly"] = "true"
  if position_side is not None:
    params["positionSide"] = position_side.upper()
  if new_client_order_id is not None:
    params["newClientOrderId"] = new_client_order_id
  if price is not None:
    params["price"] = price
  if time_in_force is not None:
    params["timeInForce"] = time_in_force
  return params


class BinanceFuturesWSClient:
  """
  Single WebSocket connection for Binance USDⓈ-M Futures; reuse for multiple orders.
  Connect once, then call place_market_order / place_limit_order as needed.
  Uses BINANCE_API_KEY and BINANCE_API_SECRET from .env if api_key/api_secret not given.
  """

  def __init__(self, api_key: str | None = None, api_secret: str | None = None):
    self._api_key, self._api_secret = _get_binance_credentials(api_key, api_secret)
    self._ws: websocket.WebSocket | None = None

  def connect(self) -> None:
    """Open WebSocket connection. Idempotent if already connected."""
    if self._ws is not None:
      return
    self._ws = websocket.create_connection(_BASE_URL)
    self._ws.settimeout(30)

  def close(self) -> None:
    """Close WebSocket connection."""
    if self._ws is not None:
      try:
        self._ws.close()
      finally:
        self._ws = None

  def __enter__(self) -> "BinanceFuturesWSClient":
    self.connect()
    return self

  def __exit__(self, *args: object) -> None:
    self.close()

  def _request(self, method: str, params: dict) -> dict:
    params = {**params, "apiKey": self._api_key}
    params["signature"] = _sign(params, self._api_secret)
    req = {"id": str(uuid.uuid4()), "method": method, "params": params}
    if self._ws is None:
      raise RuntimeError("Not connected. Call connect() or use context manager.")
    self._ws.settimeout(30)
    self._ws.send(json.dumps(req))
    request_id = req["id"]
    deadline = time.time() + 30
    while time.time() < deadline:
      raw = self._ws.recv()
      try:
        data = json.loads(raw)
      except json.JSONDecodeError:
        continue
      if data.get("id") == request_id:
        if data.get("status") != 200:
          err = data.get("error", {})
          raise RuntimeError(f"Order failed: {err.get('code')} {err.get('msg', '')}")
        return data.get("result", {})
    raise TimeoutError("No response from WebSocket")

  def place_market_order(
    self,
    symbol: str,
    side: str,
    quantity: str | float,
    *,
    reduce_only: bool = False,
    position_side: str | None = None,
    new_client_order_id: str | None = None,
  ) -> dict:
    """Place a MARKET order over the open connection."""
    qty = str(quantity) if not isinstance(quantity, str) else quantity
    params = _build_order_params(
      symbol, side, "MARKET", qty,
      reduce_only=reduce_only,
      position_side=position_side,
      new_client_order_id=new_client_order_id,
    )
    return self._request("order.place", params)

  def place_limit_order(
    self,
    symbol: str,
    side: str,
    quantity: str | float,
    price: str | float,
    *,
    time_in_force: str = "GTC",
    reduce_only: bool = False,
    position_side: str | None = None,
    new_client_order_id: str | None = None,
  ) -> dict:
    """Place a LIMIT order over the open connection."""
    qty = str(quantity) if not isinstance(quantity, str) else quantity
    pr = str(price) if not isinstance(price, str) else price
    params = _build_order_params(
      symbol, side, "LIMIT", qty,
      price=pr,
      time_in_force=time_in_force,
      reduce_only=reduce_only,
      position_side=position_side,
      new_client_order_id=new_client_order_id,
    )
    return self._request("order.place", params)


def _send_order(
  api_key: str,
  api_secret: str,
  symbol: str,
  side: str,
  order_type: str,
  quantity: str,
  *,
  price: str | None = None,
  time_in_force: str | None = None,
  reduce_only: bool = False,
  position_side: str | None = None,
  new_client_order_id: str | None = None,
  recv_window: int = 5000,
) -> dict:
  params = _build_order_params(
    symbol, side, order_type, quantity,
    price=price,
    time_in_force=time_in_force,
    reduce_only=reduce_only,
    position_side=position_side,
    new_client_order_id=new_client_order_id,
    recv_window=recv_window,
  )
  with BinanceFuturesWSClient(api_key, api_secret) as client:
    return client._request("order.place", params)


def place_market_order(
  symbol: str,
  side: str,
  quantity: str | float,
  api_key: str | None = None,
  api_secret: str | None = None,
  *,
  reduce_only: bool = False,
  position_side: str | None = None,
  new_client_order_id: str | None = None,
) -> dict:
  """
  Place a MARKET order on Binance USDⓈ-M Futures via WebSocket.
  Uses BINANCE_API_KEY and BINANCE_API_SECRET from .env if api_key/api_secret not given.

  :param symbol: e.g. "BTCUSDT"
  :param side: "BUY" or "SELL"
  :param quantity: Order size (sent as string per API)
  :param api_key: API key (optional, from .env)
  :param api_secret: API secret (optional, from .env)
  :param reduce_only: Reduce-only flag
  :param position_side: "BOTH", "LONG", or "SHORT" (hedge mode)
  :param new_client_order_id: Optional client order id
  :return: Order result dict (orderId, status, etc.)
  """
  key, secret = _get_binance_credentials(api_key, api_secret)
  qty = str(quantity) if not isinstance(quantity, str) else quantity
  return _send_order(
    key, secret, symbol, side, "MARKET", qty,
    reduce_only=reduce_only,
    position_side=position_side,
    new_client_order_id=new_client_order_id,
  )


def place_limit_order(
  symbol: str,
  side: str,
  quantity: str | float,
  price: str | float,
  api_key: str | None = None,
  api_secret: str | None = None,
  *,
  time_in_force: str = "GTC",
  reduce_only: bool = False,
  position_side: str | None = None,
  new_client_order_id: str | None = None,
) -> dict:
  """
  Place a LIMIT order on Binance USDⓈ-M Futures via WebSocket.
  Uses BINANCE_API_KEY and BINANCE_API_SECRET from .env if api_key/api_secret not given.

  :param symbol: e.g. "BTCUSDT"
  :param side: "BUY" or "SELL"
  :param quantity: Order size (sent as string per API)
  :param price: Limit price (sent as string per API)
  :param api_key: API key (optional, from .env)
  :param api_secret: API secret (optional, from .env)
  :param time_in_force: "GTC", "IOC", "FOK", or "GTD"
  :param reduce_only: Reduce-only flag
  :param position_side: "BOTH", "LONG", or "SHORT" (hedge mode)
  :param new_client_order_id: Optional client order id
  :return: Order result dict (orderId, status, etc.)
  """
  key, secret = _get_binance_credentials(api_key, api_secret)
  qty = str(quantity) if not isinstance(quantity, str) else quantity
  pr = str(price) if not isinstance(price, str) else price
  return _send_order(
    key, secret, symbol, side, "LIMIT", qty,
    price=pr,
    time_in_force=time_in_force,
    reduce_only=reduce_only,
    position_side=position_side,
    new_client_order_id=new_client_order_id,
  )


def _read_order_from_shm(shm: shared_memory.SharedMemory) -> dict | None:
  """Read one order payload from shm (same layout as helpers). Returns None if none ready."""
  buf = shm.buf
  v1 = struct.unpack_from("q", buf, VERSION_OFFSET)[0]
  if v1 % 2 != 0:
    return None
  size = struct.unpack_from("i", buf, SIZE_OFFSET)[0]
  if not (0 < size <= PAYLOAD_MAX):
    return None
  data = bytes(buf[PAYLOAD_OFFSET : PAYLOAD_OFFSET + size])
  v2 = struct.unpack_from("q", buf, VERSION_OFFSET)[0]
  if v1 != v2 or v2 % 2 != 0:
    return None
  return msgpack.unpackb(data, raw=False)


def _clear_order_shm(shm: shared_memory.SharedMemory) -> None:
  """Mark order as consumed so the same order is not sent again."""
  struct.pack_into("q", shm.buf, VERSION_OFFSET, 0)
  struct.pack_into("i", shm.buf, SIZE_OFFSET, 0)


def write_order_request(order: dict, shm_name: str = _ORDER_SHM_NAME) -> None:
  """
  Write an order request to shared memory. The runner (run_order_reader) will read and send it.

  Order dict must include: symbol, side, type ("MARKET" or "LIMIT"), quantity.
  For LIMIT add: price, and optionally time_in_force (default "GTC").
  Optional for both: reduce_only (bool), position_side (str), new_client_order_id (str).
  """
  payload = msgpack.packb(order)
  if len(payload) > PAYLOAD_MAX:
    raise ValueError(f"Order payload exceeds {PAYLOAD_MAX} bytes")
  try:
    shm = shared_memory.SharedMemory(name=shm_name, create=False, track=False)
  except TypeError:
    shm = shared_memory.SharedMemory(name=shm_name, create=False)
  try:
    struct.pack_into("q", shm.buf, VERSION_OFFSET, 1)
    struct.pack_into("i", shm.buf, SIZE_OFFSET, len(payload))
    shm.buf[PAYLOAD_OFFSET : PAYLOAD_OFFSET + len(payload)] = payload
    struct.pack_into("q", shm.buf, VERSION_OFFSET, 2)
  finally:
    shm.close()


def run_order_reader(shm_name: str = _ORDER_SHM_NAME) -> None:
  """
  Keep a WebSocket connection to Binance open; read from shm and send orders.
  Run as a separate process; use write_order_request() from elsewhere to submit orders.
  Clears each order from shm after one attempt (success or failure).
  Reconnects automatically if the socket is closed (e.g. idle timeout).
  """
  try:
    shm = shared_memory.SharedMemory(name=shm_name, create=True, size=PAYLOAD_OFFSET + PAYLOAD_MAX)
  except FileExistsError:
    try:
      shm = shared_memory.SharedMemory(name=shm_name, create=False, track=False)
    except TypeError:
      shm = shared_memory.SharedMemory(name=shm_name, create=False)
  _clear_order_shm(shm)
  client = BinanceFuturesWSClient()
  client.connect()
  print("Connected to Binance; polling shm 'binance_order'.", flush=True)
  try:
    while True:
      order = _read_order_from_shm(shm)
      if not order:
        time.sleep(0.05)
        continue
      symbol = order.get("symbol")
      side = order.get("side")
      order_type = (order.get("type") or "").upper()
      quantity = order.get("quantity")
      if not symbol or not side or not quantity or order_type not in ("MARKET", "LIMIT"):
        time.sleep(0.05)
        continue
      qty = str(quantity)
      reduce_only = order.get("reduce_only", False)
      position_side = order.get("position_side")
      client_order_id = order.get("new_client_order_id")
      print(f"Order from shm: {order_type} {side} {symbol} qty={qty}", flush=True)
      try:
        if order_type == "MARKET":
          client.place_market_order(
            symbol, side, qty,
            reduce_only=reduce_only,
            position_side=position_side,
            new_client_order_id=client_order_id,
          )
        else:
          price = order.get("price")
          if not price:
            time.sleep(0.05)
            continue
          client.place_limit_order(
            symbol, side, qty, str(price),
            time_in_force=order.get("time_in_force", "GTC"),
            reduce_only=reduce_only,
            position_side=position_side,
            new_client_order_id=client_order_id,
          )
      except Exception as e:
        print(f"Binance order failed: {e}", flush=True)
        if "closed" in str(e).lower():
          try:
            client.close()
          except Exception:
            pass
          client = BinanceFuturesWSClient()
          client.connect()
          print("Reconnected to Binance.", flush=True)
      finally:
        _clear_order_shm(shm)
      time.sleep(0.05)
  finally:
    client.close()
    shm.close()


if __name__ == "__main__":
  import sys
  print("Binance order reader: reading from shm 'binance_order'. Ctrl+C to stop.", file=sys.stderr)
  try:
    run_order_reader()
  except KeyboardInterrupt:
    pass

