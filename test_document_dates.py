"""Submission dates belong to each generated document, not its last edit."""
import copy
from datetime import datetime
import unittest
from unittest.mock import MagicMock, patch

import app


class DocumentDateTests(unittest.TestCase):
    def test_conversion_dates_order_but_preserves_source_quote(self):
        payload = {
            "quote_no": "0608-0808", "date": "2026-08-11T08:08:00-07:00",
            "customer": {}, "line_items": [], "fees": {}, "totals": {},
            "footer_notes": "", "order_meta": {"source_quote_number": "0608-0808"},
        }
        original = copy.deepcopy(payload)
        now = datetime.fromisoformat("2026-09-24T23:30:00-07:00")
        for template in ("order", "quote"):
            with self.subTest(template=template), \
                 patch.object(app, "get_pacific_now", return_value=now), \
                 patch.object(app, "generate_single_page_pdf", return_value=(b"pdf", 0)) as pdf, \
                 patch.object(app, "save_quote_to_gsheet_with_timeout", return_value=True) as save:
                app.handle_pdf_generation(payload, "0608-0808", template, MagicMock())
                expected = now.isoformat() if template == "order" else original["date"]
                self.assertEqual(pdf.call_args.kwargs["meta"]["submitted_date"], expected)
                self.assertEqual(save.call_args.args[0]["date"], expected)
                self.assertEqual(save.call_args.kwargs["record_type"], template)
                self.assertEqual(app._format_submitted_date(expected), expected[:10])
                self.assertEqual(payload, original)


if __name__ == "__main__":
    unittest.main()
