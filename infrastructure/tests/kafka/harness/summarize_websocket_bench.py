"""Check the complete 1/3/6-partition WebSocket benchmark matrix."""
import json
import sys
from pathlib import Path


def summarize(report):
    runs = []
    for partitions in (1, 3, 6):
        for kind in ('ticker', 'orderbook'):
            for rate in (100, 400):
                path = report / f'bench_{kind}_{partitions}p_{rate}r.json'
                run = json.loads(path.read_text())
                assert run['partitions'] == partitions and run['target_rate_per_second'] == rate
                assert run['sent'] == run['acknowledged'] == run['consumed'] == rate * 8
                assert sum(run['partition_counts'].values()) == run['consumed']
                runs.append(run)
    result = {
        'status': 'measured',
        'environment': 'single-host isolated Docker KRaft 3-node cluster',
        'producer': 'actual Collector collect() path, acks=all, idempotent',
        'source': '12 paced synthetic WebSocket sources with single-pair keys',
        'consumer': 'independent Kafka Raw consumer, no exchange conversion or Connect/Spark',
        'candidate_partitions': [1, 3, 6],
        'target_rates_per_second': [100, 400],
        'duration_seconds_each': 8,
        'runs': runs,
        'operational_partition_verdict': 'undetermined without production peak and allowed lag',
    }
    (report / 'websocket_throughput.json').write_text(json.dumps(result, indent=2) + '\n')
    print(f'KAF01_WEBSOCKET_MATRIX_OK runs={len(runs)}')


if __name__ == '__main__':
    summarize(Path(sys.argv[1]))
