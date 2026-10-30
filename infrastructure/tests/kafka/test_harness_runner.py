import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


RUNNER = Path(__file__).parent / 'harness' / 'run.sh'


class HarnessRunnerTests(unittest.TestCase):
    def run_with_fake_docker(self, fail_up=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / 'docker-calls'
            fake = root / 'docker'
            fake.write_text('''#!/bin/sh
echo "$*" >> "$DOCKER_CALLS"
case "$*" in
  *"kafka-metadata-quorum"*)
    printf 'ClusterId:              isolated-test-id\\nLeaderId:               1\\nCurrentVoters:          [1,2,3]\\n'
    ;;
  *"up -d --build"*)
    if [ "$FAIL_UP" = 1 ]; then exit 42; fi
    ;;
esac
exit 0
''')
            fake.chmod(0o755)
            sleep = root / 'sleep'
            sleep.write_text('#!/bin/sh\nexit 0\n')
            sleep.chmod(0o755)
            env = {**os.environ, 'PATH': f'{root}:{os.environ["PATH"]}',
                   'DOCKER_CALLS': str(calls), 'FAIL_UP': '1' if fail_up else '0'}
            result = subprocess.run(['bash', str(RUNNER)], env=env,
                                    capture_output=True, text=True, timeout=15)
            match = re.search(r'REPORT=(\S+)', result.stdout)
            self.assertIsNotNone(match, result.stdout + result.stderr)
            report = Path(match.group(1))
            self.assertEqual(report.parent, RUNNER.parent / 'reports')
            status = json.loads((report / 'result.json').read_text())
            shutil.rmtree(report)
            return result, calls.read_text(), status

    def test_success_runs_all_phases_in_isolated_project(self):
        result, calls, status = self.run_with_fake_docker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(status['exit_code'], 0)
        self.assertEqual(status['stage'], 'complete')
        self.assertIn('KAFKA_HARNESS_OK', result.stdout)
        for phase in ('pipeline', 'spark-archive', 'baseline', 'one-down',
                      'two-down', 'recovered'):
            self.assertIn(f'tester {phase}', calls)
        self.assertIn('spark-test first', calls)
        self.assertIn('spark-test resume', calls)
        self.assertIn('stop kafka-3', calls)
        self.assertIn('stop kafka-2', calls)
        self.assertIn('down -v --remove-orphans', calls)
        for line in calls.splitlines():
            self.assertIn('--project-name coin-market-kaf-harness-', line)

    def test_startup_failure_still_cleans_only_isolated_project(self):
        result, calls, status = self.run_with_fake_docker(fail_up=True)
        self.assertEqual(result.returncode, 42)
        self.assertEqual(status['exit_code'], 42)
        self.assertEqual(status['stage'], 'startup')
        self.assertNotIn('KAFKA_HARNESS_OK', result.stdout)
        self.assertIn('down -v --remove-orphans', calls)
        for line in calls.splitlines():
            self.assertIn('--project-name coin-market-kaf-harness-', line)


if __name__ == '__main__':
    unittest.main()
