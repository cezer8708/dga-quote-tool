import unittest
from datetime import datetime
from unittest.mock import patch

import app


class SubmittedDateFormattingTests(unittest.TestCase):
    def test_uses_saved_iso_timestamp_date(self):
        self.assertEqual(
            app._format_submitted_date("2026-08-18T14:27:03.123456-07:00"),
            "2026-08-18",
        )

    def test_uses_datetime_date(self):
        self.assertEqual(
            app._format_submitted_date(datetime(2026, 8, 18, 14, 27)),
            "2026-08-18",
        )

    @patch("app.generate_single_page_pdf", return_value=(b"pdf", 0))
    def test_preview_passes_saved_date_to_pdf(self, generate_pdf_mock):
        payload = {
            "quote_no": "0803-1138-V2",
            "date": "2026-08-18T14:27:03-07:00",
            "customer": {},
            "line_items": [],
            "fees": {},
            "totals": {},
            "footer_notes": "",
            "order_meta": {"order_doc_number": "0803-1138-V2"},
        }

        app.generate_pdf_preview_data(payload, template="order")

        self.assertEqual(
            generate_pdf_mock.call_args.kwargs["meta"]["submitted_date"],
            payload["date"],
        )


if __name__ == "__main__":
    unittest.main()
