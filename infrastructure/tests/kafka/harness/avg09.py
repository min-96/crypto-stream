#!/usr/bin/env python3
"""AVG-09 harness: A representative average through real Kafka, MySQL and Spark.

Runs only under the dedicated Compose project `coin-market-avg09`, so its Kafka, MySQL and
Spark checkpoint volumes are new and no other project's containers or volumes are touched.

  ./avg09.py up            start Kafka x3, topics, MySQL (latest DDL), checkpoint volume
  ./avg09.py correctness   scenarios S1-S5 on a fresh checkpoint directory; evidence JSON
  ./avg09.py memory        docker memory per container
  ./avg09.py stop          stop the project's containers (volumes are kept)

Correctness feeds synthetic Preprocessed tickers with exact event times straight into
RegionKorea_TickerPreprocessing; the WebSocket → Collector → preprocessing path is the
performance phase. Spark runs tests/stream_with_faults.py: the production query plus the
test-only AfterCommitFault record/injection and a StreamingQueryProgress record.
"""

import json
import os
import subprocess
import sys
import time
import uuid
import base64
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
PROJECT = 'coin-market-avg09'
REPORTS = HERE / 'reports' / PROJECT
EVIDENCE = REPO / 'docs' / 'evidence'
TOPIC = 'RegionKorea_TickerPreprocessing'
SPARK = f'{PROJECT}-spark-average-1'
MYSQL_ROOT = 'harness-local-only'
FIELDS = ('opening_price', 'trade_price', 'max_price', 'min_price', 'prev_closing_price',
          'acc_trade_volume_24h', 'signed_change_price', 'signed_change_rate')
Q = Decimal('1E-18')
B = 1793318400000  # 2026-10-29T00:00:00Z; window k is [B + k min, B + (k + 1) min)
MIN = 60000


def E(k):
    """`time` of window k."""
    return B + (k + 1) * MIN


class Harness:
    def __init__(self, run_dir, checkpoint):
        self.run_dir, self.checkpoint = run_dir, checkpoint
        self.checks, self.timeline = [], []
        self.env = dict(os.environ, HARNESS_CLUSTER_ID=cluster_id(), HARNESS_REPORT_DIR=str(run_dir),
                        AVG09_CHECKPOINT=checkpoint)

    # --- plumbing -------------------------------------------------------------------------
    def compose(self, *args, input=None, check=True, env=None):
        result = subprocess.run(
            ['docker', 'compose', '--project-name', PROJECT, '-f', str(HERE / 'compose.yaml'),
             '-f', str(HERE / 'compose.local-pipeline.yaml'), '-f', str(HERE / 'compose.avg09.yaml'), *args],
            input=input, capture_output=True, text=True, env=env or self.env)
        if check and result.returncode != 0:
            raise RuntimeError(f'compose {" ".join(args)} failed: {result.stderr[-2000:]}')
        return result

    def log(self, message, **detail):
        entry = {'wall_ms': now_ms(), 'event': message, **detail}
        self.timeline.append(entry)
        print(f'[avg09] {message} {json.dumps(detail) if detail else ""}', flush=True)

    def check(self, name, ok, **detail):
        self.checks.append({'check': name, 'passed': bool(ok), **detail})
        print(f'[avg09] {"PASS" if ok else "FAIL"} {name} {json.dumps(detail, default=str)}', flush=True)
        return ok

    def wait(self, what, condition, timeout=240, interval=1):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = condition()
            if value:
                return value
            time.sleep(interval)
        raise TimeoutError(f'timed out waiting for {what}')

    def reset_input(self):
        """Start a run from an empty topic and table in the AVG-09 stack only. A fresh checkpoint
        reads the topic from `earliest`, so a previous run's records would join its windows.
        Within a run the topic, checkpoint and table are kept across every restart."""
        self.compose('rm', '-f', '-s', 'spark-average')
        kafka = ['exec', '-T', 'kafka-1', 'kafka-topics', '--bootstrap-server', 'kafka-1:9092']
        self.compose(*kafka, '--delete', '--if-exists', '--topic', TOPIC)
        self.wait('topic deletion', lambda: TOPIC not in self.compose(*kafka, '--list').stdout.split(), timeout=60)
        self.compose('run', '--rm', '--no-deps', 'init-topics')
        self.mysql('TRUNCATE TABLE coin_average')
        self.log('input reset', topic=TOPIC)

    def mysql(self, sql):
        out = self.compose('exec', '-T', 'mysql', 'mysql', '-uroot', f'-p{MYSQL_ROOT}', '--batch',
                           '--skip-column-names', 'coin_market', '-e', sql).stdout
        return [line.split('\t') for line in out.splitlines()]

    def averages(self):
        rows = self.mysql('SELECT name, time, ' + ', '.join(f'`{f}`' for f in FIELDS)
                          + ' FROM coin_average ORDER BY name, time')
        return {(r[0], int(r[1])): dict(zip(FIELDS, r[2:])) for r in rows}

    def records(self, name):
        path = self.run_dir / name
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    def committed(self):
        return [r for r in self.records('faults.jsonl') if r['event'] == 'committed']

    def end_offset(self):
        best = 0
        for p in self.records('progress.jsonl'):
            for source in p.get('sources', []):
                end = source.get('endOffset')
                end = json.loads(end) if isinstance(end, str) else end
                if end and TOPIC in end:
                    best = max(best, sum(end[TOPIC].values()))
        return best

    def spark_state(self):
        out = subprocess.run(['docker', 'inspect', SPARK, '--format', '{{json .State}}'],
                             capture_output=True, text=True)
        return json.loads(out.stdout) if out.returncode == 0 else None

    def spark_start(self, fail_after=None, mode='exception', trigger=None):
        env = dict(self.env, AVG09_FAIL_AFTER_COMMIT='' if fail_after is None else str(fail_after),
                   AVG09_FAULT_MODE=mode, AVG09_TRIGGER=trigger or '')
        self.compose('up', '-d', '--no-deps', '--force-recreate', 'spark-average', env=env)
        self.log('spark started', fail_after=fail_after, mode=mode, trigger=trigger)

    def spark_kill(self):
        subprocess.run(['docker', 'kill', '--signal', 'KILL', SPARK], capture_output=True, check=True)
        self.wait('spark container to stop', lambda: not self.spark_state()['Running'], timeout=30)
        self.log('spark killed (SIGKILL)')

    def wait_spark_exit(self):
        state = self.wait('spark query to fail and the container to exit',
                          lambda: (lambda s: s if s and not s['Running'] else None)(self.spark_state()))
        self.log('spark exited', exit_code=state['ExitCode'], oom_killed=state['OOMKilled'])
        return state

    def produce(self, *ticks):
        lines = ''.join(json.dumps(t) + '\n' for t in ticks)
        out = self.compose('run', '--rm', '-T', '--no-deps', 'avg09-tools', 'produce', input=lines).stdout
        offsets = [json.loads(line)['offset'] for line in out.splitlines() if line.startswith('{')]
        assert len(offsets) == len(ticks), out
        return offsets

    def step(self, label, *ticks, wait=True):
        """Produce, then wait until Spark has read past the records, so the next step lands in a
        later micro-batch. Returns the batch ids that read these offsets."""
        offsets = self.produce(*ticks)
        self.log(f'produced {label}', offsets=offsets)
        if not wait:
            return []
        last = max(offsets) + 1
        self.wait(f'Spark to read {label}', lambda: self.end_offset() >= last)
        batches = sorted({p['batchId'] for p in self.records('progress.jsonl')
                          if any(_reads(p, o) for o in offsets)})
        self.log(f'read {label}', batches=batches)
        return batches

    def expect(self, name, key, avg):
        """All 8 fields of `key`: field i = avg + i (tick() writes value + i into field i)."""
        got = self.averages().get(key)
        want = {f: format((Decimal(avg) + i).quantize(Q), 'f') for i, f in enumerate(FIELDS)}
        return self.check(name, got == want, key=list(key), expected=want['opening_price'],
                          got=None if got is None else got['opening_price'],
                          all_fields_match=got == want)

    def wait_row(self, key):
        return self.wait(f'MySQL row {key}', lambda: key in self.averages(), timeout=240)

    def dropped(self, batches):
        return sum(op.get('numRowsDroppedByWatermark', 0) for p in self.records('progress.jsonl')
                   if p['batchId'] in batches for op in p.get('stateOperators', []))


def _reads(progress, offset):
    for source in progress.get('sources', []):
        start, end = source.get('startOffset'), source.get('endOffset')
        start = json.loads(start) if isinstance(start, str) else start
        end = json.loads(end) if isinstance(end, str) else end
        if not end or TOPIC not in end:
            continue
        lo = sum(start[TOPIC].values()) if start and TOPIC in start else 0
        if lo <= offset < sum(end[TOPIC].values()):
            return True
    return False


def tick(market, coin, ms, value):
    return {'key': f'{market}:{coin}:KRW',
            'value': {'market': market, 'timestamp': ms / 1000, 'coin_symbol': coin,
                      'data': {f: str(Decimal(value) + i) for i, f in enumerate(FIELDS)}}}


def now_ms():
    return int(time.time() * 1000)


def cluster_id():
    REPORTS.mkdir(parents=True, exist_ok=True)
    path = REPORTS / 'cluster_id'
    if not path.exists():
        path.write_text(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))
    return path.read_text().strip()


# --- scenarios ------------------------------------------------------------------------------
def s1_equal_weight_boundary_late(h):
    """Exchanges in separate batches, 59.999/60.000 boundary, late events (ADR-024 (6))."""
    b1 = h.step('S1.1 UPBIT BTC 100, UPBIT ETH 100', tick('UPBIT', 'BTC', B + 1000, 100), tick('UPBIT', 'ETH', B + 1500, 100))
    b2 = h.step('S1.2 BITHUMB BTC 110, UPBIT ETH 100', tick('BITHUMB', 'BTC', B + 2000, 110), tick('UPBIT', 'ETH', B + 2500, 100))
    b3 = h.step('S1.3 UPBIT BTC 102 @59.999, UPBIT ETH 101', tick('UPBIT', 'BTC', B + 59999, 102), tick('UPBIT', 'ETH', B + 59998, 101))
    # A step's records may span two batches; what matters is that the steps never share one.
    h.check('S1 exchange data arrived in separate micro-batches',
            all([b1, b2, b3]) and max(b1) < min(b2) and max(b2) < min(b3), batches=[b1, b2, b3])
    h.step('S1.4 UPBIT BTC 1000 @60.000 (next window)', tick('UPBIT', 'BTC', B + MIN, 1000))
    h.step('S1.5 UPBIT BTC @70.000 → watermark reaches window 0 end', tick('UPBIT', 'BTC', B + 70000, 1000))
    h.wait_row(('ETH', E(0)))
    h.expect('S1 BTC window 0 = (avg(100, 102) + 110) / 2, 59.999 in, 60.000 out', ('BTC', E(0)), '105.5')
    h.expect('S1 ETH window 0 = 100·100·101 repeating decimal', ('ETH', E(0)), '100.333333333333333333')
    late = h.step('S1.6 late BITHUMB BTC 9999 for finalized window 0', tick('BITHUMB', 'BTC', B + 30000, 9999))
    h.wait('dropped count to be reported', lambda: h.dropped(late) > 0, timeout=30)
    h.check('S1 late event of a finalized window is dropped by watermark', h.dropped(late) > 0, dropped=h.dropped(late))
    h.expect('S1 BTC window 0 unchanged after the late event', ('BTC', E(0)), '105.5')
    h.step('S1.7 UPBIT BTC @110.000 → watermark 100.000', tick('UPBIT', 'BTC', B + 110000, 1000))
    older = h.step('S1.8 BITHUMB BTC 120 @90.000, older than watermark, window 1 open', tick('BITHUMB', 'BTC', B + 90000, 120))
    h.check('S1 event older than the watermark in an open window is not dropped', h.dropped(older) == 0, dropped=h.dropped(older))
    h.step('S1.9 UPBIT BTC @130.000 → window 1 final', tick('UPBIT', 'BTC', B + 130000, 1000))
    h.wait_row(('BTC', E(1)))
    h.expect('S1 BTC window 1 = (avg(1000, 1000, 1000) + 120) / 2', ('BTC', E(1)), '560')


def s5_restart_keeps_open_window_state(h):
    h.step('S5.1 UPBIT BTC 200 in window 3', tick('UPBIT', 'BTC', E(2) + 1000, 200))
    before = {r['process'] for r in h.records('faults.jsonl')}
    h.spark_kill()
    h.spark_start()
    h.step('S5.2 BITHUMB BTC 300 in window 3 after restart', tick('BITHUMB', 'BTC', E(2) + 2000, 300))
    h.step('S5.3 UPBIT BTC 50 in window 4 → windows 2, 3 final', tick('UPBIT', 'BTC', E(3) + 10000, 50))
    h.wait_row(('BTC', E(3)))
    h.expect('S5 window 2 state (UPBIT 1000 before the kill) survives the restart', ('BTC', E(2)), '1000')
    h.expect('S5 window 3 = (200 before kill + 300 after) / 2', ('BTC', E(3)), '250')
    after = {r['process'] for r in h.committed()} - before
    h.check('S5 a new Spark process committed after the restart', bool(after), new_processes=sorted(after))


def _after_commit_fault(h, label, mode, windows, values):
    """Close `windows` (k, k + 1) under a fault after COMMIT, restart, compare the re-run."""
    k = windows[0]
    fail_after = len(h.committed()) + 1
    h.spark_start(fail_after=fail_after, mode=mode)
    h.step(f'{label}.1 UPBIT BTC {values[0]} in window {k + 1}', tick('UPBIT', 'BTC', E(k) + 1000, values[0]))
    h.step(f'{label}.2 BITHUMB BTC {values[1]} in window {k + 1}', tick('BITHUMB', 'BTC', E(k) + 2000, values[1]))
    h.step(f'{label}.3 UPBIT BTC {values[2]} in window {k + 2} → windows {k}, {k + 1} final',
           tick('UPBIT', 'BTC', E(k + 1) + 10000, values[2]), wait=False)
    inject = h.wait('fault injection', lambda: next((r for r in h.records('faults.jsonl') if r['event'] == 'inject'
                                                     and r.get('fail_after') == fail_after), None))
    batch = inject['batch_id']
    if mode == 'pause':
        h.spark_kill()
        state = h.spark_state()
    else:
        state = h.wait_spark_exit()
    first = [r for r in h.committed() if r['batch_id'] == batch]
    before = h.averages()
    h.check(f'{label} Spark stopped after COMMIT ({mode})', not state['Running'] and len(first) == 1,
            batch_id=batch, exit_code=state['ExitCode'], keys=first and first[0]['keys'])
    h.spark_start()
    again = h.wait(f're-run of batch {batch}', lambda: (lambda c: c if len(c) == 2 else None)(
        [r for r in h.committed() if r['batch_id'] == batch]))
    after = h.averages()
    h.check(f'{label} same batch_id re-ran after restart with the same keys',
            again[0]['keys'] == again[1]['keys'] and again[0]['process'] != again[1]['process'],
            batch_id=batch, processes=[r['process'] for r in again],
            keys=again[1]['keys'])
    h.check(f'{label} re-run left the same rows and values', before == after, rows=len(after))
    return batch


def s3_exception_after_commit(h):
    batch = _after_commit_fault(h, 'S3', 'exception', (4,), (10, 20, 30))
    h.expect('S3 window 5 = (10 + 20) / 2', ('BTC', E(5)), '15')
    h.expect('S3 window 4 = UPBIT 50', ('BTC', E(4)), '50')
    return batch


def s4_kill_after_commit(h):
    _after_commit_fault(h, 'S4', 'pause', (6,), (40, 60, 70))
    h.expect('S4 window 7 = (40 + 60) / 2', ('BTC', E(7)), '50')
    h.expect('S4 window 6 = UPBIT 30', ('BTC', E(6)), '30')


def s2a_rollback_before_commit(h):
    """ETH row is locked by another transaction: the BTC rows already sent in the same
    transaction must be rolled back, and the re-run after restart must write all three."""
    h.step('S2a.1 UPBIT BTC 80, UPBIT ETH 90 in window 9', tick('UPBIT', 'BTC', E(8) + 1000, 80), tick('UPBIT', 'ETH', E(8) + 1000, 90))
    h.mysql('SET GLOBAL innodb_lock_wait_timeout = 5')
    values = ', '.join(["'0'"] * len(FIELDS))
    blocker = subprocess.Popen(
        ['docker', 'exec', '-i', f'{PROJECT}-mysql-1', 'mysql', '-uroot', f'-p{MYSQL_ROOT}', 'coin_market', '-e',
         f"START TRANSACTION; INSERT INTO coin_average VALUES ('ETH', {E(9)}, {values}); SELECT SLEEP(600);"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    blocker_id = h.wait('blocking transaction', lambda: (h.mysql(
        "SELECT id FROM information_schema.processlist WHERE info LIKE 'SELECT SLEEP(600)%'") or [[None]])[0][0])
    attempts_before = len([r for r in h.records('faults.jsonl') if r['event'] == 'start'])
    h.step('S2a.2 UPBIT BTC 1 in window 10 → windows 8, 9 final', tick('UPBIT', 'BTC', E(9) + 10000, 1), wait=False)
    state = h.wait_spark_exit()
    starts = [r for r in h.records('faults.jsonl') if r['event'] == 'start'][attempts_before:]
    failed = starts[-1]['batch_id']
    rows = h.averages()
    h.check('S2a write failed before COMMIT and Spark stopped', not state['Running']
            and not any(r['batch_id'] == failed for r in h.committed()), batch_id=failed, exit_code=state['ExitCode'])
    h.check('S2a BTC rows sent before the failing ETH row were rolled back',
            ('BTC', E(8)) not in rows and ('BTC', E(9)) not in rows)
    h.mysql(f'KILL {blocker_id}')
    blocker.wait(timeout=30)
    h.mysql('SET GLOBAL innodb_lock_wait_timeout = 50')
    h.spark_start()
    commit = h.wait(f're-run of batch {failed}', lambda: next((r for r in h.committed() if r['batch_id'] == failed), None))
    h.check('S2a re-run committed the failed batch once', len([r for r in h.committed() if r['batch_id'] == failed]) == 1,
            keys=commit['keys'])
    h.expect('S2a window 8 = UPBIT 70', ('BTC', E(8)), '70')
    h.expect('S2a BTC window 9 = 80', ('BTC', E(9)), '80')
    h.expect('S2a ETH window 9 = 90 (not the blocker\'s 0)', ('ETH', E(9)), '90')


def s2b_mysql_down(h):
    h.step('S2b.1 UPBIT BTC 5 in window 11', tick('UPBIT', 'BTC', E(10) + 1000, 5))
    h.compose('stop', 'mysql')
    h.log('mysql stopped')
    h.step('S2b.2 UPBIT BTC 7 in window 12 → windows 10, 11 final', tick('UPBIT', 'BTC', E(11) + 10000, 7), wait=False)
    state = h.wait_spark_exit()
    h.check('S2b batch failed while MySQL was down and Spark stopped', not state['Running'], exit_code=state['ExitCode'])
    h.compose('start', 'mysql')
    h.wait('mysql healthy', lambda: h.compose('exec', '-T', 'mysql', 'mysqladmin', 'ping', '-uroot', f'-p{MYSQL_ROOT}',
                                              '--silent', check=False).returncode == 0, timeout=120)
    h.log('mysql started')
    h.spark_start()
    h.wait_row(('BTC', E(11)))
    h.expect('S2b window 10 = 1 after MySQL recovery', ('BTC', E(10)), '1')
    h.expect('S2b window 11 = 5 after MySQL recovery', ('BTC', E(11)), '5')


EXPECTED_FINAL = {
    ('BTC', E(0)): '105.5', ('ETH', E(0)): '100.333333333333333333', ('BTC', E(1)): '560',
    ('BTC', E(2)): '1000', ('BTC', E(3)): '250', ('BTC', E(4)): '50', ('BTC', E(5)): '15',
    ('BTC', E(6)): '30', ('BTC', E(7)): '50', ('BTC', E(8)): '70', ('BTC', E(9)): '80',
    ('ETH', E(9)): '90', ('BTC', E(10)): '1', ('BTC', E(11)): '5',
}


def correctness():
    run = time.strftime('correctness-%Y%m%dT%H%M%SZ', time.gmtime())
    run_dir = REPORTS / run
    run_dir.mkdir(parents=True)
    h = Harness(run_dir, f'/checkpoints/{run}')  # fresh checkpoint directory in the AVG-09 volume
    started = now_ms()
    error = None
    try:
        h.reset_input()
        h.spark_start()
        for scenario in (s1_equal_weight_boundary_late, s5_restart_keeps_open_window_state,
                         s3_exception_after_commit, s4_kill_after_commit, s2a_rollback_before_commit, s2b_mysql_down):
            h.log(f'scenario {scenario.__name__}')
            scenario(h)
        final = h.averages()
        h.check('final: one row per (name, time), exactly the expected keys', set(final) == set(EXPECTED_FINAL),
                rows=len(final), unexpected=[list(k) for k in set(final) - set(EXPECTED_FINAL)],
                missing=[list(k) for k in set(EXPECTED_FINAL) - set(final)])
        for key, avg in EXPECTED_FINAL.items():
            h.expect(f'final value {key[0]} {key[1]}', key, avg)
    except Exception as exc:  # recorded in the evidence, then re-raised via the exit code
        error = f'{type(exc).__name__}: {exc}'
        print(f'[avg09] ERROR {error}', flush=True)
    reruns = {}
    for r in h.committed():
        reruns.setdefault(r['batch_id'], []).append(r['process'])
    evidence = {
        'task': 'AVG-09 correctness', 'project': PROJECT, 'checkpoint': h.checkpoint,
        'started_ms': started, 'finished_ms': now_ms(), 'error': error,
        'passed': error is None and all(c['passed'] for c in h.checks),
        'checks': h.checks,
        'rerun_batches': {str(b): processes for b, processes in reruns.items() if len(processes) > 1},
        'dropped_by_watermark_total': h.dropped({p['batchId'] for p in h.records('progress.jsonl')}),
        'timeline': h.timeline, 'memory': memory(),
        'records': {'faults': str(run_dir / 'faults.jsonl'), 'progress': str(run_dir / 'progress.jsonl')},
    }
    (run_dir / 'correctness.json').write_text(json.dumps(evidence, indent=2))
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / 'AVG-09-correctness.json').write_text(json.dumps(evidence, indent=2))
    print(f'[avg09] {"AVG09_CORRECTNESS_OK" if evidence["passed"] else "AVG09_CORRECTNESS_FAILED"} '
          f'{sum(c["passed"] for c in h.checks)}/{len(h.checks)} checks, evidence {run_dir / "correctness.json"}')
    return 0 if evidence['passed'] else 1


def memory():
    out = subprocess.run(['docker', 'stats', '--no-stream', '--format', '{{.Name}}\t{{.MemUsage}}'],
                         capture_output=True, text=True).stdout
    return dict(line.split('\t', 1) for line in out.splitlines() if '\t' in line)


def up():
    h = Harness(REPORTS / 'up', '/checkpoints/unused')
    (REPORTS / 'up').mkdir(parents=True, exist_ok=True)
    h.compose('up', '-d', '--wait', 'kafka-1', 'kafka-2', 'kafka-3', 'mysql')
    h.compose('up', '--no-deps', 'init-topics', 'checkpoint-init')
    h.compose('build', 'avg09-tools')
    for name, usage in sorted(memory().items()):
        print(f'{name}\t{usage}')
    return 0


def main(command):
    if command == 'up':
        return up()
    if command == 'correctness':
        return correctness()
    if command == 'memory':
        for name, usage in sorted(memory().items()):
            print(f'{name}\t{usage}')
        return 0
    if command == 'stop':
        Harness(REPORTS / 'up', '/checkpoints/unused').compose('stop')
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else ''))
