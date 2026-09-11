"""Exercise the production version callbacks with Streamlit's widget lifecycle."""
import ast
from pathlib import Path
import unittest

from streamlit.testing.v1 import AppTest


class QuoteVersionWidgetTests(unittest.TestCase):
    def test_each_version_button_resets_rendered_manager_widgets(self):
        tree = ast.parse(Path(__file__).with_name("app.py").read_text())
        names = {
            "assign_new_quote_version", "clear_manager_credentials",
            "preserve_freight_for_next_rerun", "restore_pending_freight_state",
            "capture_freight_state", "restore_freight_state",
        }
        functions = "\n".join(
            ast.unparse(node) for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in names
        )
        buttons = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "button":
                continue
            key = next((kw.value for kw in node.keywords if kw.arg == "key"), None)
            if isinstance(key, ast.Constant) and key.value in {
                "top_new_version", "sidebar_new_version", "bottom_new_version"
            }:
                node.func.value = ast.Name(id="st", ctx=ast.Load())
                buttons[key.value] = ast.unparse(node)
        self.assertEqual(len(buttons), 3)
        for key, button in buttons.items():
            with self.subTest(button=key):
                script = '''
import re
from datetime import datetime, timezone
import streamlit as st
PENDING_FREIGHT_STATE_KEY = "_pending_freight_state"
FREIGHT_NOTE_OPTIONS = []
def get_pacific_now():
    return datetime.now(timezone.utc)
def get_selected_freight_notes():
    return st.session_state.get("freight_notes", "")
''' + functions + '''
st.session_state.setdefault("quote_no", "0911-1200")
st.session_state.setdefault("manager_pricing_authorized", True)
restore_pending_freight_state()
st.checkbox("Manager Pricing", value=True, key="manager_pricing_checkbox")
st.text_input("Username", value="manager", key="manager_username")
st.text_input("Password", value="secret", key="manager_password")
st.number_input("Freight", value=125.0, key="freight_fee_input")
st.text_input("Freight notes", value="Lift gate", key="freight_notes_other")
''' + button
                app = AppTest.from_string(script).run()
                self.assertEqual(len(app.exception), 0)
                for version in (2, 3):
                    app.button(key=key).click().run()
                    self.assertEqual(len(app.exception), 0)
                    self.assertEqual(app.session_state["quote_no"], f"0911-1200-V{version}")
                    self.assertFalse(app.session_state["manager_pricing_authorized"])
                    self.assertFalse(app.checkbox[0].value)
                    self.assertEqual(app.session_state["manager_username"], "")
                    self.assertEqual(app.session_state["manager_password"], "")
                    self.assertEqual(app.number_input[0].value, 125.0)
                    self.assertEqual(app.session_state["freight_notes_other"], "Lift gate")


if __name__ == "__main__":
    unittest.main()
