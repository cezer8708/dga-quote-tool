import threading
import time
import unittest
from unittest.mock import patch

import app


class SaveQuoteTimeoutTests(unittest.TestCase):
    @patch.object(app.st, "warning")
    @patch.object(app, "save_quote_to_gsheet")
    def test_returns_promptly_when_google_sheets_hangs(self, save_mock, warning_mock):
        release = threading.Event()
        save_mock.side_effect = lambda *_args: release.wait(5)

        started = time.monotonic()
        result = app.save_quote_to_gsheet_with_timeout({}, timeout_seconds=0.05)
        elapsed = time.monotonic() - started
        release.set()

        self.assertFalse(result)
        self.assertLess(elapsed, 0.5)
        warning_mock.assert_called_once()

    @patch.object(app, "save_quote_to_gsheet", return_value=True)
    def test_returns_success_when_save_finishes(self, _save_mock):
        self.assertTrue(app.save_quote_to_gsheet_with_timeout({}, timeout_seconds=0.5))


if __name__ == "__main__":
    unittest.main()
