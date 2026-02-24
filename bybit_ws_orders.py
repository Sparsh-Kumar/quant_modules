"""
Bybit V5 WebSocket Trade API: place market and limit orders (linear/USDT perp).
Docs: https://bybit-exchange.github.io/docs/v5/websocket/trade/guideline

Credentials from .env: BYBIT_API_KEY, BYBIT_API_SECRET (or pass explicitly).

Same pattern as binance_ws_orders: client, one-off place_*_order, write_order_request (shm),
run_order_reader (open WS, read from shm continuously, send orders). Shm layout matches helpers.
"""

import struct
import hmac
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

_BASE_URL = "wss://stream.bybit.com/v5/trade"
_ORDER_SHM_NAME = "bybit_order"
_RECV_WINDOW = "5000"


def _get_bybit_credentials(api_key: str | None, api_secret: str | None) -> tuple[str, str]:
  key = api_key or os.getenv("BYBIT_API_KEY")
  secret = api_secret or os.getenv("BYBIT_API_SECRET")
  if not key or not secret:
    raise ValueError("Set BYBIT_API_KEY and BYBIT_API_SECRET in .env or pass them explicitly.")
  return key, secret


def _auth_signature(secret: str, expires_ms: int) -> str:
  """Signature for auth: HMAC-SHA256(secret, 'GET/realtime' + expires)."""
  msg = f"GET/realtime{expires_ms}"
  return hmac.new(secret.encode(), msg.encode(), "sha256").hexdigest()


class BybitFuturesWSClient:
  """
  Single WebSocket connection to Bybit V5 trade; auth once, reuse for orders.
  Uses BYBIT_API_KEY and BYBIT_API_SECRET from .env if not given.
  """

  def __init__(self, api_key: str | None = None, api_secret: str | None = None):
    self._api_key, self._api_secret = _get_bybit_credentials(api_key, api_secret)
    self._ws: websocket.WebSocket | None = None

  def connect(self) -> None:
    if self._ws is not None:
      return
    self._ws = websocket.create_connection(_BASE_URL)
    self._ws.settimeout(30)
    expires = int((time.time() + 10) * 1000)
    sig = _auth_signature(self._api_secret, expires)
    auth_msg = json.dumps({"op": "auth", "args": [self._api_key, expires, sig]})
    self._ws.send(auth_msg)
    raw = self._ws.recv()
    data = json.loads(raw)
    if data.get("retCode") != 0:
      raise RuntimeError(f"Bybit auth failed: {data.get('retMsg', data)}")

  def close(self) -> None:
    if self._ws is not None:
      try:
        self._ws.close()
      finally:
        self._ws = None

  def __enter__(self) -> "BybitFuturesWSClient":
    self.connect()
    return self

  def __exit__(self, *args: object) -> None:
    self.close()

  def _request(self, op: str, args: list) -> dict:
    if self._ws is None:
      raise RuntimeError("Not connected. Call connect() or use context manager.")
    req_id = str(uuid.uuid4())[:36]
    timestamp = str(int(time.time() * 1000))
    req = {
      "reqId": req_id,
      "header": {
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
      },
      "op": op,
      "args": args,
    }
    self._ws.settimeout(30)
    self._ws.send(json.dumps(req))
    deadline = time.time() + 30
    while time.time() < deadline:
      raw = self._ws.recv()
      try:
        data = json.loads(raw)
      except json.JSONDecodeError:
        continue
      if data.get("reqId") != req_id:
        continue
      if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit {op} failed: {data.get('retMsg', '')}")
      return data.get("data", {})
    raise TimeoutError("No response from WebSocket")

  def place_market_order(
    self,
    symbol: str,
    side: str,
    qty: str,
    *,
    category: str = "linear",
    reduce_only: bool = False,
    position_idx: int | None = None,
    order_link_id: str | None = None,
  ) -> dict:
    """Place a MARKET order (linear USDT perp by default)."""
    side = "Buy" if side.upper() == "BUY" else "Sell"
    arg = {
      "category": category,
      "symbol": symbol,
      "side": side,
      "orderType": "Market",
      "qty": str(qty),
    }
    if reduce_only:
      arg["reduceOnly"] = True
    if position_idx is not None:
      arg["positionIdx"] = position_idx
    if order_link_id is not None:
      arg["orderLinkId"] = order_link_id
    return self._request("order.create", [arg])

  def place_limit_order(
    self,
    symbol: str,
    side: str,
    qty: str,
    price: str,
    *,
    category: str = "linear",
    time_in_force: str = "GTC",
    reduce_only: bool = False,
    position_idx: int | None = None,
    order_link_id: str | None = None,
  ) -> dict:
    """Place a LIMIT order (linear USDT perp by default)."""
    side = "Buy" if side.upper() == "BUY" else "Sell"
    arg = {
      "category": category,
      "symbol": symbol,
      "side": side,
      "orderType": "Limit",
      "qty": str(qty),
      "price": str(price),
      "timeInForce": time_in_force,
    }
    if reduce_only:
      arg["reduceOnly"] = True
    if position_idx is not None:
      arg["positionIdx"] = position_idx
    if order_link_id is not None:
      arg["orderLinkId"] = order_link_id
    return self._request("order.create", [arg])


def place_market_order(
  symbol: str,
  side: str,
  qty: str | float,
  api_key: str | None = None,
  api_secret: str | None = None,
  *,
  category: str = "linear",
  reduce_only: bool = False,
  position_idx: int | None = None,
  order_link_id: str | None = None,
) -> dict:
  """One-off MARKET order. Credentials from .env if not passed."""
  key, secret = _get_bybit_credentials(api_key, api_secret)
  with BybitFuturesWSClient(key, secret) as client:
    return client.place_market_order(
      symbol, side, str(qty),
      category=category,
      reduce_only=reduce_only,
      position_idx=position_idx,
      order_link_id=order_link_id,
    )


def place_limit_order(
  symbol: str,
  side: str,
  qty: str | float,
  price: str | float,
  api_key: str | None = None,
  api_secret: str | None = None,
  *,
  category: str = "linear",
  time_in_force: str = "GTC",
  reduce_only: bool = False,
  position_idx: int | None = None,
  order_link_id: str | None = None,
) -> dict:
  """One-off LIMIT order. Credentials from .env if not passed."""
  key, secret = _get_bybit_credentials(api_key, api_secret)
  with BybitFuturesWSClient(key, secret) as client:
    return client.place_limit_order(
      symbol, side, str(qty), str(price),
      category=category,
      time_in_force=time_in_force,
      reduce_only=reduce_only,
      position_idx=position_idx,
      order_link_id=order_link_id,
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
  Write an order request to shm. Runner (run_order_reader) will read and send it.
  Order dict: category (default linear), symbol, side, orderType ("Market"/"Limit"), qty;
  for Limit add price, timeInForce (default GTC). Optional: reduceOnly, positionIdx, orderLinkId.
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
  Open Bybit WebSocket, read from shm continuously; when an order appears, send it and clear.
  Run as a separate process; use write_order_request() from elsewhere to submit orders.
  """
  try:
    shm = shared_memory.SharedMemory(name=shm_name, create=True, size=PAYLOAD_OFFSET + PAYLOAD_MAX)
  except FileExistsError:
    try:
      shm = shared_memory.SharedMemory(name=shm_name, create=False, track=False)
    except TypeError:
      shm = shared_memory.SharedMemory(name=shm_name, create=False)
  _clear_order_shm(shm)
  client = BybitFuturesWSClient()
  client.connect()
  try:
    while True:
      order = _read_order_from_shm(shm)
      if not order:
        time.sleep(0.05)
        continue
      symbol = order.get("symbol")
      side = order.get("side")
      order_type = (order.get("orderType") or order.get("type") or "").strip().upper()
      qty = order.get("qty") or order.get("quantity")
      if not symbol or not qty or order_type not in ("MARKET", "LIMIT"):
        time.sleep(0.05)
        continue
      qty_str = str(qty)
      side = "Buy" if (str(side).upper() == "BUY") else "Sell"
      category = order.get("category", "linear")
      reduce_only = order.get("reduceOnly", order.get("reduce_only", False))
      position_idx = order.get("positionIdx", order.get("position_idx"))
      order_link_id = order.get("orderLinkId", order.get("order_link_id"))
      print(f"Order from shm: {order_type} {side} {symbol} qty={qty_str}", flush=True)
      try:
        if order_type == "MARKET":
          client.place_market_order(
            symbol, side, qty_str,
            category=category,
            reduce_only=reduce_only,
            position_idx=position_idx,
            order_link_id=order_link_id,
          )
        else:
          price = order.get("price")
          if not price:
            time.sleep(0.05)
            continue
          client.place_limit_order(
            symbol, side, qty_str, str(price),
            category=category,
            time_in_force=order.get("timeInForce", order.get("time_in_force", "GTC")),
            reduce_only=reduce_only,
            position_idx=position_idx,
            order_link_id=order_link_id,
          )
        _clear_order_shm(shm)
      except Exception as e:
        print(f"Bybit order failed: {e}", flush=True)
      time.sleep(0.05)
  finally:
    client.close()
    shm.close()


if __name__ == "__main__":
  import sys
  print("Bybit order reader: reading from shm 'bybit_order'. Ctrl+C to stop.", file=sys.stderr)
  try:
    run_order_reader()
  except KeyboardInterrupt:
    pass
