import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


RUNNER = Path(__file__).parents[2] / 'spark' / 'verify-processed-sink.sh'


class ProcessedSinkRunnerTests(unittest.TestCase):
    def test_only_unique_project_is_torn_down(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / 'calls'
            fake = root / 'docker'
            fake.write_text('#!/bin/sh\necho "$*" >> "$DOCKER_CALLS"\n')
            fake.chmod(0o755)
            env = {**os.environ, 'PATH': f'{root}:{os.environ["PATH"]}',
                   'DOCKER_CALLS': str(calls)}
            result = subprocess.run(['bash', str(RUNNER)], env=env,
                                    text=True, capture_output=True, timeout=15)
            match = re.search(r'REPORT=(\S+)', result.stdout)
            self.assertIsNotNone(match, result.stdout + result.stderr)
            report = Path(match.group(1))
            status = json.loads((report / 'result.json').read_text())
            shutil.rmtree(report)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(status['stage'], 'complete')
            self.assertEqual(status['exit_code'], 0)
            commands = calls.read_text().splitlines()
            self.assertTrue(any('down -v --remove-orphans' in line for line in commands))
            self.assertTrue(any('verify_processed_sink.py' in line for line in commands))
            self.assertTrue(any('check_processed_sink.py' in line for line in commands))
            for line in commands:
                self.assertIn('--project-name coin-market-kaf08-', line)


if __name__ == '__main__':
    unittest.main()
