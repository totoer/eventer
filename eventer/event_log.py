
import os
import pickle

from .messages import Event


class EventLog:

    @property
    def versions(self):
        return self._versions

    @property
    def log(self):
        return self._log

    @property
    def pick(self) -> Event:
        return self._log[-1] if len(self._log) > 0 else None

    def __init__(self, workdir: str, max_size: int = 1000):
        self._workdir = workdir
        self._versions_filepath = os.path.join(self._workdir, 'versions')
        self._log_filepath = os.path.join(self._workdir, 'event.log')

        self._max_size = max_size
        self._versions: dict[str, float] = {}
        self._log: list[Event] = []

        if os.path.exists(self._versions_filepath) and os.path.exists(self._log_filepath):
            try:
                self._load()
            except:
                self._store()

        else:
            self._store()

    def _load(self):
        with open(self._versions_filepath, 'rb') as fp:
            self._versions = pickle.load(fp)

        with open(self._log_filepath, 'rb') as fp:
            while True:
                r_next_event_size = fp.read(2)
                if r_next_event_size == b'':
                    break
                next_event_size = int.from_bytes(r_next_event_size, 'big')
                r_event = fp.read(next_event_size)
                event = pickle.loads(r_event)
                self._log.append(event)

    def restore(self, versions: dict[str, float], log: list[Event]):
        self._versions = versions
        self._log = log
        self._store()

    def _store(self):
        with open(self._versions_filepath, 'wb') as fp:
            pickle.dump(self._versions, fp)

        with open(self._log_filepath, 'wb') as fp:
            for event in self._log:
                r_event = pickle.dumps(event)
                next_event_size = len(r_event)
                fp.write(next_event_size.to_bytes(2, 'big'))
                fp.write(r_event)

    def _append(self, event: Event):
        with open(self._versions_filepath, 'wb') as fp:
            pickle.dump(self._versions, fp)

        with open(self._log_filepath, 'w+b') as fp:
            fp.seek(0, 2)
            r_event = pickle.dumps(event)
            event_size = len(r_event)
            fp.write(event_size.to_bytes(2, 'big'))
            fp.write(r_event)

    def append(self, node_id: str, event: Event) -> bool:
        if node_id in self._versions and self._versions[node_id] == event.timestamp:
            return False

        self._versions[node_id] = event.timestamp
        self._log.append(event)

        if len(self._log) > self._max_size:
            self._log = self._log[1:]
            self._store()

        else:
            self._append(event)

        return True
