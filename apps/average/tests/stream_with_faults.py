"""AVG-09 test entrypoint: the production query (stream_to_mysql.main) plus test-only tools.

Environment:
  AVG09_FAULT_RECORD       JSON-lines batch record (AfterCommitFault), required
  AVG09_PROGRESS_RECORD    JSON-lines StreamingQueryProgress record, optional
  AVG09_FAIL_AFTER_COMMIT  fail after the N-th committed batch; unset = record only
  AVG09_FAULT_MODE         exception | pause
  AVG09_TRIGGER            processing-time trigger such as '5 seconds'; unset = Spark default
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql.streaming import StreamingQueryListener  # noqa: E402

from fault_injection import AfterCommitFault  # noqa: E402
from stream_to_mysql import create_spark, main  # noqa: E402


class ProgressRecorder(StreamingQueryListener):
    """Append every micro-batch's progress JSON (batch ids, input rows, watermark, drops,
    durations, Kafka start/end/latest offsets) to a file."""

    def __init__(self, path):
        self.path = path

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        with open(self.path, 'a') as handle:
            handle.write(event.progress.json.replace('\n', ' ') + '\n')

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


if os.environ.get('AVG09_PROGRESS_RECORD'):
    # main() calls create_spark() again and gets this same session.
    create_spark().streams.addListener(ProgressRecorder(os.environ['AVG09_PROGRESS_RECORD']))
fail_after = os.environ.get('AVG09_FAIL_AFTER_COMMIT')
fault = AfterCommitFault(os.environ['AVG09_FAULT_RECORD'],
                         fail_after=int(fail_after) if fail_after else None,
                         mode=os.environ.get('AVG09_FAULT_MODE', 'exception'))
main(wrap_connect=fault.watch, wrap_writer=fault.wrap, trigger=os.environ.get('AVG09_TRIGGER') or None)
