"""Validate and preserve KAF-10's live KRaft failover observations."""
import json
import re
import sys
from pathlib import Path


def parse_status(value):
    def field(name):
        match = re.search(rf'^{name}:\s*(\S+)', value, re.MULTILINE)
        if not match:
            raise AssertionError(f'missing {name} in quorum status')
        return match.group(1)

    voters = field('CurrentVoters').strip('[]')
    return field('ClusterId'), int(field('LeaderId')), {int(x) for x in voters.split(',')}


def parse_isr(value):
    match = re.search(r'\bIsr:\s*([0-9,]+)', value)
    if not match:
        raise AssertionError('missing topic ISR')
    return {int(x) for x in match.group(1).split(',')}


def require_match(pattern, value, label):
    match = re.search(pattern, value)
    if not match:
        raise AssertionError(f'missing {label}')
    return int(match.group(1))


def verify(report):
    def read(name):
        return (report / name).read_text()

    baseline_id, baseline_leader, voters = parse_status(read('kraft_baseline_status.txt'))
    one_id, one_leader, one_voters = parse_status(read('kraft_one_down_status.txt'))
    recovered_id, recovered_leader, recovered_voters = parse_status(read('kraft_recovered_status.txt'))
    assert baseline_id == one_id == recovered_id
    assert voters == one_voters == recovered_voters == {1, 2, 3}
    assert baseline_leader in voters and one_leader in {1, 2} and recovered_leader in voters
    topic_descriptions = [read(f'kraft_{phase}_topic.txt')
                          for phase in ('baseline', 'one_down', 'recovered')]
    assert all('ReplicationFactor: 3' in description for description in topic_descriptions)
    baseline_isr, one_isr, recovered_isr = map(parse_isr, topic_descriptions)
    assert baseline_isr == {1, 2, 3}, baseline_isr
    assert one_isr == {1, 2}, one_isr
    assert recovered_isr == {1, 2, 3}, recovered_isr

    baseline_ack = require_match(r'BASELINE_ACK_OK offset=(\d+)', read('kraft_baseline_producer.txt'), 'baseline ACK')
    one_ack = require_match(r'ONE_DOWN_ACK_OK offset=(\d+)', read('kraft_one_down_producer.txt'), 'one-down ACK')
    assert one_ack > baseline_ack
    failure = read('kraft_two_down_producer.txt')
    assert re.search(r'TWO_DOWN_WRITE_FAILED_AS_EXPECTED type=\w+', failure), failure
    running = set(read('kraft_two_down_running.txt').splitlines())
    assert {service for service in running if service.startswith('kafka-')} == {'kafka-1'}, running
    recovered = read('kraft_recovered_producer.txt')
    preserved = require_match(r'RECOVERY_OK preserved_offset=(\d+)', recovered, 'preserved offset')
    new_ack = require_match(r'new_ack_offset=(\d+)', recovered, 'recovery ACK')
    assert preserved == baseline_ack and new_ack > one_ack
    marker = json.loads(read('failover.json'))
    assert marker['offset'] == preserved

    evidence = {
        'cluster_id': baseline_id,
        'voters': sorted(voters),
        'baseline': {'leader': baseline_leader, 'isr': sorted(baseline_isr), 'ack_offset': baseline_ack},
        'one_down': {'stopped': 3, 'leader': one_leader, 'isr': sorted(one_isr), 'ack_offset': one_ack},
        'two_down': {'stopped': [2, 3], 'running_kafka': ['kafka-1'],
                     'producer_error': re.search(r'type=(\w+)', failure).group(1),
                     'quorum_status_exit_code': int(read('kraft_two_down_status_exit.txt').strip()),
                     'quorum_status_observation': 'static voter list; one of three voters running'},
        'recovered': {'leader': recovered_leader, 'isr': sorted(recovered_isr),
                      'preserved_marker': marker['marker'], 'preserved_offset': preserved,
                      'new_ack_offset': new_ack},
    }
    (report / 'kraft_evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(f'KAF10_KRAFT_FAILOVER_OK cluster_id={baseline_id} preserved_offset={preserved}')


if __name__ == '__main__':
    verify(Path(sys.argv[1]))
