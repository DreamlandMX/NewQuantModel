from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pypdf import PdfReader

from newquantmodel.cli.main import handle_smoke
from newquantmodel.reporting.pdf_export import write_research_pdf


class ReportBundlePdfTest(unittest.TestCase):
    def test_smoke_pdf_contains_research_sections(self) -> None:
        tempdir = tempfile.mkdtemp(prefix="newquantmodel-report-")
        self.addCleanup(lambda: shutil.rmtree(tempdir, ignore_errors=True))

        code = handle_smoke(tempdir)
        self.assertEqual(code, 0)

        manifest = json.loads((Path(tempdir) / "storage" / "published" / "report-manifest.json").read_text(encoding="utf-8"))
        pdf_path = Path(manifest["pdfPath"])
        self.assertTrue(pdf_path.exists())

        text = "\n".join((page.extract_text() or "") for page in PdfReader(str(pdf_path)).pages)
        self.assertGreater(len(text.strip()), 400)
        self.assertIn("Executive Summary", text)
        self.assertIn("Top Actionable Trades", text)
        self.assertIn("Strategy Snapshot", text)
        self.assertIn("Data Health", text)
        self.assertIn("Universe Coverage", text)

    def test_pdf_writer_shows_explicit_empty_sections(self) -> None:
        tempdir = tempfile.mkdtemp(prefix="newquantmodel-empty-report-")
        self.addCleanup(lambda: shutil.rmtree(tempdir, ignore_errors=True))
        pdf_path = Path(tempdir) / "weekly_report.pdf"

        write_research_pdf(
            pdf_path,
            title="newquantmodel Weekly Research Report",
            generated_at="2026-03-31T12:00:00+00:00",
            forecasts=[],
            rankings=[],
            trade_plans=[],
            backtests=[],
            health=[],
            universes=[],
        )

        text = "\n".join((page.extract_text() or "") for page in PdfReader(str(pdf_path)).pages)
        self.assertIn("No data available for this section.", text)
        self.assertIn("Top Actionable Trades", text)


if __name__ == "__main__":
    unittest.main()
