
from typing import Callable

import asyncio
import random
import uuid
from collections import OrderedDict


from .messages import MType, NodeInfo, Ping, Event, Sync, Message, decode_message
from .event_log import EventLog


Callback = Callable[[any], None]


class Eventer:

    @property
    def node_id(self):
        return f'{self._host}:{self._port}'

    @property
    def is_master(self):
        return self._master == (self._host, self._port,) \
            or len(self._nodes) == 0 \
            or self._master is None

    def __init__(
            self, log_workdir: str, host: str, port: int,
            nodes: list[tuple[str, int]],
            loop: asyncio.AbstractEventLoop | None = None) -> None:

        self._host = host
        self._port = port
        self._nodes = nodes
        self._delay = random.uniform(0.0, 2.0)
        self._event_loop = loop or asyncio.get_event_loop()

        self._emit_locker: asyncio.Future | None = None
        self._master: tuple[str, int] | None = None
        self._callbacks: dict[str, dict[uuid.UUID, Callback]] = {}
        self._event_log = EventLog(log_workdir)

    async def serve(self):
        """Start network message handling.

        Find master. If master exists sync data with master.
        Set `loop` delay more then master delay.

        Start loop
        """

        self._event_loop.create_task(asyncio.start_server(
            self._handle, self._host, self._port))

        ok = await self._find_master()
        if ok:
            await self._sync()

        self._loop_task = self._event_loop.create_task(self._loop())

    async def emit(self, name: str, **kwargs):
        if self._emit_locker is not None:
            await self._emit_locker
            self._emit_locker = None

        event = Event(name=name, args=OrderedDict(kwargs))
        await self._handle_emit(node_id=self.node_id, event=event)

    def on(self, name: str, c: Callback) -> uuid.UUID:
        if name not in self._callbacks:
            self._callbacks[name] = {}
        id = uuid.uuid1()
        self._callbacks[name][id] = c
        return id

    def remove(self, name: str, id: uuid.UUID):
        if name in self._callbacks and id in self._callbacks[name]:
            del self._callbacks[name][id]

    async def _run_callbacks(self, event: Event):
        if event.name in self._callbacks:
            for _, c in self._callbacks[event.name].items():
                await c(**event.args)

    async def _emit(self, host: str, port: int, event: Event):
        try:
            message = Message(node_id=self.node_id,
                              m_type=MType.EVENT, data=event)
            await message.send(host=host, port=port, delay=self._delay)

        except asyncio.CancelledError:
            return

        except asyncio.TimeoutError:
            return

        except ConnectionRefusedError:
            return

    async def _handle_emit(self, node_id: str, event: Event):
        if self.is_master:
            ok = self._event_log.append(node_id=node_id, event=event)

            for node in self._nodes:
                host = node[0]
                port = node[1]
                await self._emit(host=host, port=port, event=event)

            if ok:
                await self._run_callbacks(event=event)

        else:
            host = self._master[0]
            port = self._master[1]
            await self._emit(host=host, port=port, event=event)

    async def _loop(self):
        await asyncio.sleep(self._delay)
        self._emit_locker = self._event_loop.create_future()
        ok = True
        ping = Ping(versions=self._event_log.versions,
                    host=self._host, port=self._port)
        message = Message(node_id=self.node_id, m_type=MType.PING, data=ping)
        for node in self._nodes:
            host = node[0]
            port = node[1]
            try:
                reader, _ = await message.send(
                    host=host, port=port, delay=self._delay)
                buffer = await reader.read(2)
                if not buffer.startswith(b'Ok'):
                    ok = False
                    break

            except asyncio.CancelledError:
                continue

            except asyncio.TimeoutError:
                continue

            except ConnectionRefusedError:
                continue

        if ok:
            self._master = (self._host, self._port,)

        self._loop_task = self._event_loop.create_task(self._loop())
        self._emit_locker.set_result(1)

    async def _find_master(self) -> bool:
        message = Message(node_id=self.node_id,
                          m_type=MType.NODE_INFO, data=None)

        for node in self._nodes:
            host = node[0]
            port = node[1]
            try:
                reader, _ = await message.send(host=host, port=port, delay=1)
                buffer = await reader.read(1024)
                resp = decode_message(buffer)
                node_info: NodeInfo = resp.data
                if node_info.is_master:
                    self._master = node
                    self._delay = random.uniform(
                        node_info.delay, node_info.delay+1)
                    return True

            except asyncio.CancelledError:
                continue

            except asyncio.TimeoutError:
                continue

            except ConnectionRefusedError:
                continue

        return False

    async def _sync(self):
        message = Message(node_id=self.node_id, m_type=MType.SYNC, data=None)
        host = self._master[0]
        port = self._master[1]
        try:
            reader, _ = await message.send(host=host, port=port, delay=1)
            buffer = await reader.read(1024 * 8)
            resp = decode_message(buffer)
            data: Sync = resp.data
            self._event_log.restore(versions=data.versions, log=data.log)

        except asyncio.TimeoutError:
            return

    async def _on_node_info(self, writer: asyncio.StreamWriter):
        node_info = NodeInfo(is_master=self.is_master, delay=self._delay)
        message = Message(node_id=self.node_id,
                          m_type=MType.NODE_INFO_RESPONSE, data=node_info)
        await message.response(writer=writer)

    async def _on_ping(self, ping: Ping, writer: asyncio.StreamWriter):
        if self._loop_task is not None:
            self._loop_task.cancel()

        ok = True
        for node_id, version in self._event_log.versions.items():
            if node_id in ping.versions and version > ping.versions[node_id]:
                ok = False
                break

        if ok:
            self._master = (ping.host, ping.port,)
            writer.write(b'Ok')
            await writer.drain()

        else:
            writer.write(b'Failed')
            await writer.drain()

        self._loop_task = self._event_loop.create_task(self._loop())

    async def _on_event(
            self, node_id: str, event: Event,
            writer: asyncio.StreamWriter):
        if self.is_master:
            await self._handle_emit(node_id=node_id, event=event)
        else:
            ok = self._event_log.append(node_id=node_id, event=event)
            if ok:
                await self._run_callbacks(event=event)

        writer.write(b'Ok')
        await writer.drain()

    async def _on_sync(self, sync: Sync, writer: asyncio.StreamWriter):
        sync = Sync(log=self._event_log.log, versions=self._event_log.versions)
        message = Message(node_id=self.node_id,
                          m_type=MType.SYNC_RESPONSE, data=sync)
        await message.response(writer=writer)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        buffer = await reader.read(1024 * 8)

        if len(buffer) > 0:
            message = decode_message(buffer)
            if message.m_type == MType.PING:
                await self._on_ping(ping=message.data, writer=writer)

            elif message.m_type == MType.EVENT:
                await self._on_event(node_id=message.node_id, event=message.data, writer=writer)

            elif message.m_type == MType.NODE_INFO:
                await self._on_node_info(writer=writer)

            elif message.m_type == MType.SYNC:
                await self._on_sync(sync=message.data, writer=writer)
