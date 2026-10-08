"""Verify approval through real Streamlit form submission and reruns."""
import ast
from pathlib import Path
import unittest
from streamlit.testing.v1 import AppTest


class CourseOverrideWidgetTests(unittest.TestCase):
    def make_app(self, configured=True):
        names = {
            'reset_course_minimum_override', 'authorize_course_minimum_override',
            'render_course_minimum_override', 'course_minimum_override_active',
            '_constant_time_credentials_match', 'ensure_course_discount',
            'ensure_discount_line', 'ensure_course_discount_position',
            'find_course_discount_index', 'find_last_course_discount_anchor_index',
            'eligible_qty_for_discount', 'eligible_mach_2_pro_qty_for_discount',
            'is_basket_5_7_X', 'is_mach_2_pro',
        }
        tree = ast.parse(Path('app.py').read_text())
        functions = '\n'.join(ast.unparse(n) for n in tree.body
                              if isinstance(n, ast.FunctionDef) and n.name in names)
        script = '''
import hmac, uuid
import streamlit as st
ALLOW_COURSE_SKUS = {"M5CO", "M7CO", "MXCO"}
MACH_2_PRO_SKUS = {"M2IG", "M2P"}
ANNIVERSARY_DISCOUNT_SKU = "50th"
MACH_2_PRO_ANNIVERSARY_DISCOUNT_SKU = "50thM2"
MACH_2_PRO_COURSE_DISCOUNT_SKU = "CD-M2P"
COURSE_DISCOUNT_SKUS = ("CD", "50th", "CD-M2P", "50thM2")
def get_env(key, default=None):
    return {"MANAGER_USERNAME": "manager", "MANAGER_PASSWORD": "secret"}.get(key, default)
''' + functions + '''
st.session_state.setdefault("line_items", [{"sku": "MXPRSTD", "qty": 3}])
render_course_minimum_override()
ensure_course_discount(st.session_state["line_items"])
st.button("Unrelated rerun")
'''
        if not configured:
            script = script.replace('{"MANAGER_USERNAME": "manager", "MANAGER_PASSWORD": "secret"}', '{}')
        return AppTest.from_string(script).run()

    def test_missing_manager_configuration_is_reported(self):
        app = self.make_app(configured=False)
        app.text_input(key='course_minimum_override_note').set_value('Designer Discount')
        app.text_input(key='course_override_username').set_value('manager')
        app.text_input(key='course_override_password').set_value('secret')
        app.button[0].click().run()
        self.assertIn('not configured', app.error[0].value)
        self.assertEqual(app.text_input(key='course_override_username').value, 'manager')
        self.assertFalse(app.session_state['course_minimum_override_authorized'])

    def test_single_submission_applies_course_and_preserves_anniversary(self):
        app = self.make_app()
        self.assertEqual([i['sku'] for i in app.session_state['line_items']], ['MXPRSTD', '50th'])
        app.text_input(key='course_minimum_override_note').set_value('Designer Discount')
        app.text_input(key='course_override_username').set_value('manager')
        app.text_input(key='course_override_password').set_value('secret')
        app.button[0].click().run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(len(app.success), 1)
        totals = {i['sku']: i['total'] for i in app.session_state['line_items'] if 'total' in i}
        self.assertEqual(totals, {'CD': -300, '50th': -375})
        app.button[1].click().run()
        self.assertEqual(len(app.exception), 0)
        self.assertTrue(app.session_state['course_minimum_override_authorized'])
        app.button[0].click().run()
        self.assertFalse(app.session_state['course_minimum_override_authorized'])
        self.assertEqual([i['sku'] for i in app.session_state['line_items']], ['MXPRSTD', '50th'])

    def test_invalid_credentials_show_error_and_can_retry(self):
        app = self.make_app()
        app.text_input(key='course_minimum_override_note').set_value('Designer Discount')
        app.text_input(key='course_override_username').set_value('manager')
        app.text_input(key='course_override_password').set_value('wrong')
        app.button[0].click().run()
        self.assertEqual(len(app.error), 1)
        self.assertFalse(app.session_state['course_minimum_override_authorized'])
        app.text_input(key='course_override_username').set_value('manager')
        app.text_input(key='course_override_password').set_value('secret')
        app.button[0].click().run()
        self.assertEqual(len(app.success), 1)
        self.assertEqual(len(app.error), 0)
