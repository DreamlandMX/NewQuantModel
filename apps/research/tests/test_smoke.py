from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from newquantmodel.cli.main import handle_smoke


class SmokeTest(unittest.TestCase):
    def test_smoke_creates_outputs(self) -> None:
        tempdir = tempfile.mkdtemp(prefix="newquantmodel-")
        self.addCleanup(lambda: shutil.rmtree(tempdir, ignore_errors=True))
        code = handle_smoke(tempdir)
        self.assertEqual(code, 0)
        root = Path(tempdir)
        self.assertTrue((root / "storage" / "published" / "forecasts.json").exists())
        self.assertTrue((root / "storage" / "published" / "trade-plans.json").exists())
        self.assertTrue((root / "storage" / "published" / "report-manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
