import unittest
from unittest.mock import patch
import app


class CourseMinimumOverrideTests(unittest.TestCase):
    def test_thresholds_and_discount_switch(self):
        for sku, code, unit in [('M5STD', 'CD', -100), ('M2P', 'CD-M2P', -50)]:
            for qty, approved, enabled, expected in [
                (3, False, True, False), (3, True, True, True),
                (0, True, True, False), (9, False, True, True),
                (3, True, False, False),
            ]:
                with self.subTest(sku=sku, qty=qty, approved=approved, enabled=enabled):
                    items = [{'sku': sku, 'qty': qty}]
                    meta = {'apply_course_discount': enabled, 'apply_anniversary_discount': False,
                            'course_minimum_override_applied': approved,
                            'course_minimum_override_note': 'Replacement baskets'}
                    app.ensure_course_discount(items, meta)
                    discounts = [i for i in items if i.get('sku') == code]
                    self.assertEqual(bool(discounts), expected)
                    if expected:
                        self.assertEqual(discounts[0]['total'], qty * unit)
                    meta['course_minimum_override_applied'] = False
                    app.ensure_course_discount(items, meta)
                    self.assertEqual(any(i.get('sku') == code for i in items), qty >= 9 and enabled)

    def test_checkbox_needs_no_credentials_or_reason(self):
        app.st.session_state['course_minimum_override_applied'] = True
        self.assertTrue(app.course_minimum_override_active())
        app.reset_course_minimum_override()
        self.assertFalse(app.course_minimum_override_active())

    def test_saved_quote_restores_override(self):
        payload = {'customer': {}, 'line_items': [], 'fees': {}, 'tax_meta': {},
                   'discount_meta': {'course_minimum_override_applied': True},
                   'order_meta': {}}
        with patch.object(app, 'clear_manager_credentials'):
            app.load_quote_payload_into_session(payload, '1008-0842')
        self.assertTrue(app.course_minimum_override_active())
        payload['discount_meta'] = {}
        with patch.object(app, 'clear_manager_credentials'):
            app.load_quote_payload_into_session(payload, '1008-0842')
        self.assertFalse(app.course_minimum_override_active())
