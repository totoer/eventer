
import io
import asyncio
import pickle
import datetime
from collections import OrderedDict
from enum import IntEnum
from dataclasses import field, dataclass


@dataclass
class NodeInfo:

    is_master: bool
    delay: float


@dataclass
class Ping:

    versions: dict[str, float]
    host: str
    port: int


@dataclass
class Event:

    timestamp: float = field(init=False)
    name: str
    args: OrderedDict

    def __post_init__(self):
        n = datetime.datetime.now()
        self.timestamp = n.timestamp()


@dataclass
class Sync:

    log: list[Event]
    versions: dict[str, float]


class MType(IntEnum):

    NODE_INFO = 1
    NODE_INFO_RESPONSE = 2
    EVENT = 3
    PING = 4
    SYNC = 5
    SYNC_RESPONSE = 6


class Message:

    @property
    def node_id(self):
        return self._node_id

    @property
    def m_type(self) -> MType:
        return self._m_type

    @property
    def data(self) -> Event | Ping | Sync | NodeInfo | None:
        return self._data

    def __init__(
            self, node_id: str, m_type: MType,
            data: Event | Ping | Sync | NodeInfo | None) -> None:
        self._node_id = node_id
        self._m_type = m_type
        self._data = data

    def encode(self) -> bytes:
        buffer = io.BytesIO()

        r_node_id = bytes(self._node_id, encoding='ascii')
        r_node_id_size = len(r_node_id)
        buffer.write(r_node_id_size.to_bytes(2, 'big'))
        buffer.write(r_node_id)

        m_type = int(self._m_type)
        buffer.write(m_type.to_bytes(2, 'big'))

        if self._data:
            r_data = pickle.dumps(self._data)
            data_size = len(r_data)
            buffer.write(data_size.to_bytes(2, 'big'))
            buffer.write(r_data)

        return buffer.getvalue()

    async def send(self, host: str, port: int, delay: float):
        f = asyncio.open_connection(host, port)
        reader, writer = await asyncio.wait_for(f, delay)
        writer.write(self.encode())
        await writer.drain()
        return (reader, writer,)

    async def response(self, writer: asyncio.StreamWriter):
        writer.write(self.encode())
        await writer.drain()


def decode_message(data: bytes) -> Message:
    buffer = io.BytesIO(data)

    r_node_id_size = buffer.read(2)
    r_node_id = buffer.read(int.from_bytes(r_node_id_size, 'big'))
    node_id = str(r_node_id, encoding='ascii')

    r_m_type = buffer.read(2)
    r_data_size = buffer.read(2)

    m_type = int.from_bytes(r_m_type, 'big')
    data_size = int.from_bytes(r_data_size, 'big')

    r_data = buffer.read(data_size)
    _data = pickle.loads(r_data) if r_data != b'' else None

    return Message(node_id=node_id, m_type=m_type, data=_data)
