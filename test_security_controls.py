import unittest
from unittest.mock import MagicMock, patch

import app
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph


class SecurityControlTests(unittest.TestCase):
    def test_credentials_fail_closed_when_not_configured(self):
        self.assertFalse(app._constant_time_credentials_match("user", "pass", "", ""))

    def test_credentials_match_exact_values(self):
        self.assertTrue(app._constant_time_credentials_match("user", "pass", "user", "pass"))
        self.assertFalse(app._constant_time_credentials_match("user", "wrong", "user", "pass"))

    def test_pdf_text_escapes_active_markup_and_preserves_lines(self):
        payload = '<img src="http://127.0.0.1/private"/>\nNormal & text'
        escaped = app._pdf_text(payload, preserve_newlines=True)
        self.assertNotIn("<img", escaped)
        self.assertIn("&lt;img", escaped)
        self.assertIn("<br/>", escaped)
        self.assertIn("&amp;", escaped)

    def test_escaped_pdf_text_never_invokes_image_loader(self):
        payload = '<img src="file:///private/secret.png"/>'
        with patch("reportlab.platypus.paraparser.ImageReader") as image_reader:
            Paragraph(app._pdf_text(payload), getSampleStyleSheet()["Normal"])
        image_reader.assert_not_called()

    @patch.object(app.load_all_quotes, "clear")
    def test_sheet_save_uses_raw_cell_semantics(self, _clear_mock):
        worksheet = MagicMock()
        worksheet.row_values.return_value = list(app.SAVED_QUOTE_HEADERS)
        sheet = MagicMock()
        sheet.get_worksheet.return_value = worksheet
        client = MagicMock()
        client.open_by_key.return_value = sheet
        payload = {
            "quote_no": "0827-1200",
            "date": "2026-08-27",
            "customer": {"company": '=HYPERLINK("https://example.invalid")'},
            "totals": {"grand_total": 1.0},
            "order_meta": {},
        }

        with patch.object(app, "get_gsheet_client", return_value=client):
            self.assertTrue(app.save_quote_to_gsheet(payload))

        self.assertEqual(worksheet.append_row.call_args.kwargs["value_input_option"], "RAW")
        self.assertIn(
            '=HYPERLINK("https://example.invalid")',
            worksheet.append_row.call_args.args[0],
        )

    def test_loaded_payload_never_restores_manager_authority(self):
        payload = {
            "customer": {},
            "line_items": [],
            "fees": {},
            "tax_meta": {},
            "discount_meta": {
                "manager_pricing_authorized": True,
                "manager_pricing_note": "Previously approved",
            },
            "footer_notes": "",
            "order_meta": {},
        }
        with patch.object(app, "clear_manager_credentials"):
            app.load_quote_payload_into_session(payload, "0827-1200")
        self.assertFalse(app.st.session_state["manager_pricing_authorized"])
        self.assertFalse(app.st.session_state["manager_pricing_checkbox"])

    def test_new_quote_version_revokes_manager_authority(self):
        app.st.session_state["quote_no"] = "0827-1200"
        app.st.session_state["manager_pricing_authorized"] = True
        app.st.session_state["manager_pricing_checkbox"] = True
        with (
            patch.object(app, "preserve_freight_for_next_rerun"),
            patch.object(app, "clear_manager_credentials"),
            patch.object(app.st, "rerun"),
        ):
            app.assign_new_quote_version()
        self.assertFalse(app.st.session_state["manager_pricing_authorized"])
        self.assertFalse(app.st.session_state["manager_pricing_checkbox"])

    def test_payload_records_application_but_not_authority(self):
        app.st.session_state["manager_pricing_authorized"] = True
        payload = app.get_current_payload(100, 0, 0, 0, 95, 0, 0, "", 5)
        self.assertTrue(payload["discount_meta"]["manager_pricing_applied"])
        self.assertFalse(payload["discount_meta"]["manager_pricing_authorized"])


if __name__ == "__main__":
    unittest.main()
