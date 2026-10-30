"""AVG-09 test-only foreachBatch wrapper: record every batch_id attempt, fail after a MySQL COMMIT.

Never used by stream_to_mysql.py itself; tests/stream_with_faults.py plugs it into main().
The record is a JSON-lines file that survives restarts, so a re-run of the same batch_id shows
up as a second `committed` line, and the failure is injected only once.
"""

import json
import os
import time
import uuid

# PIDs repeat across container restarts; this tells Spark driver processes apart.
PROCESS = uuid.uuid4().hex


class InjectedFault(RuntimeError):
    pass


class _WatchedStatement:
    """Delegates to the real statement and remembers each UPSERT's (name, time)."""

    def __init__(self, statement, keys):
        self._statement, self._keys, self._name, self._time = statement, keys, None, None

    def setString(self, index, value):
        if index == 1:
            self._name = value
        self._statement.setString(index, value)

    def setLong(self, index, value):
        if index == 2:
            self._time = value
        self._statement.setLong(index, value)

    def addBatch(self):
        self._keys.append([self._name, self._time])
        self._statement.addBatch()

    def __getattr__(self, name):
        return getattr(self._statement, name)


class _WatchedConnection:
    """Delegates to the real (py4j or fake) connection and reports a successful commit."""

    def __init__(self, connection, on_commit):
        self._connection, self._on_commit, self._keys = connection, on_commit, []

    def prepareStatement(self, sql):
        return _WatchedStatement(self._connection.prepareStatement(sql), self._keys)

    def commit(self):
        self._connection.commit()
        self._on_commit(self._keys)

    def __getattr__(self, name):
        return getattr(self._connection, name)


class AfterCommitFault:
    """Inject a failure after the `fail_after`-th batch that committed rows to MySQL.

    mode 'exception' raises InjectedFault from foreachBatch, so the query stops with that
    batch committed in MySQL but not in Spark's commit log. mode 'pause' sleeps instead,
    so a test can kill the process inside the same gap.
    """

    def __init__(self, record_path, fail_after=None, mode='exception', pause_seconds=600):
        if mode not in ('exception', 'pause'):
            raise ValueError(f'unknown fault mode {mode!r}')
        self.record_path, self.fail_after, self.mode = record_path, fail_after, mode
        self.pause_seconds = pause_seconds
        self._batch_id, self._committed = None, False

    def records(self):
        try:
            with open(self.record_path) as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    def _record(self, batch_id, event, **extra):
        line = json.dumps({'batch_id': batch_id, 'event': event, 'process': PROCESS, 'pid': os.getpid(),
                           'wall_ms': int(time.time() * 1000), **extra})
        with open(self.record_path, 'a') as handle:
            handle.write(line + '\n')
            handle.flush()
            os.fsync(handle.fileno())  # must survive a kill right after

    def _on_commit(self, keys):
        self._committed = True
        # wall_ms - time of each key = Window end → MySQL COMMIT latency.
        self._record(self._batch_id, 'committed', keys=keys)

    def watch(self, connect):
        """Wrap the connection factory given to batch_writer."""
        return lambda: _WatchedConnection(connect(), self._on_commit)

    def wrap(self, write_batch):
        """Wrap the foreachBatch function built from the watched factory."""
        def wrapped(batch, batch_id):
            self._batch_id, self._committed = batch_id, False
            self._record(batch_id, 'start')
            write_batch(batch, batch_id)
            if not self._committed or self.fail_after is None:
                return
            records = self.records()
            # Once per fail_after value, so a restart with the same setting does not fail again.
            if any(r['event'] == 'inject' and r.get('fail_after') == self.fail_after for r in records):
                return
            if sum(r['event'] == 'committed' for r in records) < self.fail_after:
                return
            self._record(batch_id, 'inject', fail_after=self.fail_after)
            if self.mode == 'exception':
                raise InjectedFault(f'AVG-09: injected failure after COMMIT of batch {batch_id}')
            time.sleep(self.pause_seconds)
        return wrapped
