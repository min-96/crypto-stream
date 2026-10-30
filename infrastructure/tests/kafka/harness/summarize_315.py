"""Keep a compact, checked summary of the 9x35 catch-up run."""
import json
import sys
from pathlib import Path


def summarize(report):
    raw = json.loads((report / 'bench_315_raw.json').read_text())
    downstream = json.loads((report / 'bench_315_downstream.json').read_text())
    spark = json.loads((report / 'bench_315_spark.json').read_text())
    total = raw['total_raw_records']
    assert total == downstream['total_raw'] == downstream['preprocessed_records']
    assert total == downstream['archived_value_objects'] == downstream['archived_key_objects']
    assert total == spark['read_committed_records'] == 1260
    assert spark['distinct_trade_pair_keys'] == 315
    assert all(run['sent'] == run['acknowledged'] == 630 for run in raw['runs'])
    assert sum(raw['raw_topic_counts'].values()) == total
    assert sum(downstream['preprocessed_topic_counts'].values()) == total
    assert downstream['samples'][0]['raw_pending'] == total
    assert downstream['samples'][-1]['raw_pending'] == 0
    assert downstream['samples'][-1]['archive_pending'] == 0
    pre_counts = {}
    for record in downstream['records']:
        label = f'{record["topic"]}:{record["partition"]}'
        pre_counts[label] = pre_counts.get(label, 0) + 1
    partition_backlog = []
    for run in raw['runs']:
        for raw_label, produced in sorted(run['partition_counts'].items()):
            topic, partition = raw_label.rsplit(':', 1)
            region = ('Korea' if topic.startswith('korea') else
                      'NE' if topic.startswith('ne') else 'Asia')
            kind = 'Ticker' if topic.endswith('-ticker') else 'Orderbook'
            pre_label = f'Region{region}_{kind}Preprocessing:{partition}'
            completed = pre_counts.get(pre_label, 0)
            assert produced == completed, (raw_label, produced, completed)
            partition_backlog.append({'raw_partition': raw_label,
                                      'preprocessed_partition': pre_label,
                                      'initial_backlog': produced,
                                      'final_backlog': produced - completed})
    result = {
        'status': 'complete',
        'project': json.loads((report / 'result.json').read_text())['project']
                   if (report / 'result.json').exists() else report.name,
        'synthetic_exchanges': raw['exchange_count'],
        'synthetic_coins_per_exchange': raw['coins_per_exchange'],
        'updates_per_key_and_kind': raw['updates_per_key_and_kind'],
        'raw_runs': raw['runs'],
        'raw_topic_counts': raw['raw_topic_counts'],
        'preprocessed_records': downstream['preprocessed_records'],
        'preprocessed_topic_counts': downstream['preprocessed_topic_counts'],
        'partition_backlog_start_and_end': partition_backlog,
        'preprocessing_catchup_seconds': downstream['preprocessing_catchup_seconds'],
        'archived_value_objects': downstream['archived_value_objects'],
        'archived_key_objects': downstream['archived_key_objects'],
        'archive_catchup_seconds': downstream['archive_catchup_seconds'],
        'lag_samples': downstream['samples'],
        'spark_read_committed_records': spark['read_committed_records'],
        'spark_distinct_keys': spark['distinct_trade_pair_keys'],
        'environment': 'isolated single-host Docker; Kafka heap 512 MiB per broker, Connect heap 768 MiB',
        'scope_limit': 'synthetic exchange conversion and Spark read only; no live exchanges or time-average query',
    }
    (report / 'bench_315_summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(f'BENCH_315_SUMMARY_OK raw={total} preprocessed={total} archived={total} spark={total}')


if __name__ == '__main__':
    summarize(Path(sys.argv[1]))
