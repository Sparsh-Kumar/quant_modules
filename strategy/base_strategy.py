from abc import ABC, abstractmethod
from multiprocessing import shared_memory

from helpers import read_snapshot


class StrategyBase(ABC):

  def __init__(self, shm_names: list[str]) -> None:
    self._shm_names = shm_names
    self._shms: list[shared_memory.SharedMemory] = []

  def run(self) -> None:
    self._shms = []
    for n in self._shm_names:
      try:
        try:
          self._shms.append(shared_memory.SharedMemory(name=n, track=False))
        except TypeError:
          self._shms.append(shared_memory.SharedMemory(name=n))
      except FileNotFoundError:
        raise SystemExit(
          f"Shared memory '{n}' not found.\n"
          f"1) Start the orderbook writer and keep it running (use the exact name):\n"
          f"   python -m orderbook.orderbook -e <exchange> -s <symbol> --shm-name {n} --no-display\n"
          f"2) Run the strategy only while the writer(s) are still running.\n"
          f"3) On macOS: run all commands from the same directory (e.g. quant_modules) and the same venv."
        ) from None
    try:
      while True:
        snapshots = [read_snapshot(shm) for shm in self._shms]
        self.on_snapshots(snapshots)
    finally:
      for shm in self._shms:
        shm.close()

  @abstractmethod
  def on_snapshots(self, snapshots: list[dict]) -> None:
    pass
