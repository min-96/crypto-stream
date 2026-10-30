import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[2] / 'kafka' / 'init-topics.sh'


class TopicContractTests(unittest.TestCase):
    def run_check(self, partitions='1', profile='legacy', mode='--check'):
        with tempfile.TemporaryDirectory() as directory:
            fake = Path(directory) / 'kafka-topics'
            calls = Path(directory) / 'calls'
            fake.write_text('''#!/bin/bash
echo "$*" >> "$CALLS"
if [[ " $* " == *" --describe "* ]]; then
  topic=''
  while (($#)); do
    if [[ "$1" == --topic ]]; then topic=$2; break; fi
    shift
  done
  config=''
  [[ "$topic" == connect-* ]] && config='cleanup.policy=compact'
  count=$PARTITIONS
  if [[ "$count" == profile ]]; then
    count=1
    if [[ "$PROFILE" == operational-candidate ]]; then
      case "$topic" in
        *-ticker|*_TickerPreprocessing) count=3 ;;
        *-orderbook|*_OrderbookPreprocessing) count=6 ;;
      esac
    fi
  fi
  echo "Topic: $topic PartitionCount: $count ReplicationFactor: 3 Configs: $config"
fi
''')
            fake.chmod(0o755)
            env = {**os.environ, 'PATH': f'{directory}:{os.environ["PATH"]}',
                   'CALLS': str(calls), 'PARTITIONS': partitions,
                   'PROFILE': profile, 'KAFKA_TOPIC_PROFILE': profile}
            result = subprocess.run(['bash', str(SCRIPT), mode], env=env,
                                    capture_output=True, text=True)
            return result, calls.read_text()

    def test_check_mode_is_read_only_and_accepts_expected_topics(self):
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls.count('--describe'), 21)
        self.assertNotIn('--create', calls)

    def test_check_mode_reports_partition_mismatch_without_changing_it(self):
        result, calls = self.run_check(partitions='3')
        self.assertEqual(result.returncode, 1)
        self.assertIn('expected partitions=1 RF=3', result.stderr)
        self.assertNotIn('--create', calls)

    def test_operational_candidate_checks_3_6_1_without_mutating_existing_topics(self):
        result, calls = self.run_check(partitions='profile', profile='operational-candidate')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls.count('--describe'), 21)
        self.assertNotIn('--create', calls)
        mismatch, calls = self.run_check(partitions='1', profile='operational-candidate')
        self.assertEqual(mismatch.returncode, 1)
        self.assertIn('expected partitions=3 RF=3', mismatch.stderr)
        self.assertIn('expected partitions=6 RF=3', mismatch.stderr)
        self.assertNotIn('--create', calls)

    def test_operational_candidate_creates_only_missing_topic_targets(self):
        result, calls = self.run_check(partitions='profile', profile='operational-candidate', mode='init')
        self.assertEqual(result.returncode, 0, result.stderr)
        creates = [line for line in calls.splitlines() if '--create' in line]
        self.assertEqual(len(creates), 21)
        self.assertEqual(sum('--partitions 3' in line for line in creates), 6)
        self.assertEqual(sum('--partitions 6' in line for line in creates), 6)
        self.assertEqual(sum('--partitions 1' in line for line in creates), 9)


if __name__ == '__main__':
    unittest.main()
