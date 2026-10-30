import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'harness'))

from verify_kraft_failover import parse_status, parse_isr


class KraftEvidenceTests(unittest.TestCase):
    def test_quorum_status_parses_voters_and_leader(self):
        status = 'ClusterId: abc\nLeaderId: 2\nCurrentVoters: [1,2,3]\n'
        self.assertEqual(parse_status(status), ('abc', 2, {1, 2, 3}))

    def test_topic_isr_parses_partition_description(self):
        description = ('Topic: asiaSocketDataInBTC-ticker PartitionCount: 1 ReplicationFactor: 3\n'
                       'Topic: asiaSocketDataInBTC-ticker Partition: 0 Leader: 1 '
                       'Replicas: 1,2,3 Isr: 1,2\n')
        self.assertEqual(parse_isr(description), {1, 2})


if __name__ == '__main__':
    unittest.main()
