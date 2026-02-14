import struct
import msgpack
from multiprocessing import shared_memory

VERSION_OFFSET = 0
SIZE_OFFSET = 8
PAYLOAD_OFFSET = 12


def read_snapshot(shm: shared_memory.SharedMemory) -> dict:
  buf = shm.buf
  while True:
    v1 = struct.unpack_from('q', buf, VERSION_OFFSET)[0]
    if v1 % 2 == 1:
      continue
    size = struct.unpack_from('i', buf, SIZE_OFFSET)[0]
    data = bytes(buf[PAYLOAD_OFFSET:PAYLOAD_OFFSET + size])
    v2 = struct.unpack_from('q', buf, VERSION_OFFSET)[0]
    if v1 == v2 and v2 % 2 == 0:
      return msgpack.unpackb(data, raw=False)
