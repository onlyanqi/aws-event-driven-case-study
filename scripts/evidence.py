"""Run the assertions and write an honest, reproducible evidence report."""
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class Results(unittest.TextTestResult):
    def startTest(self, test):
        super().startTest(test)
        self.names.append(test.id())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.names = []


suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'))
result = unittest.TextTestRunner(verbosity=2, resultclass=Results).run(suite)
report = {
    'scope': 'Sequential Python/SQLite failure model; no AWS calls or load testing',
    'command': 'python3 scripts/evidence.py',
    'tests_run': result.testsRun,
    'failures': len(result.failures),
    'errors': len(result.errors),
    'skipped': len(result.skipped),
    'successful': result.wasSuccessful(),
    'scenarios': result.names,
}
(ROOT / 'evidence' / 'run.json').write_text(json.dumps(report, indent=2) + '\n')
sys.exit(0 if result.wasSuccessful() else 1)
