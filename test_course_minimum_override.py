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

    def test_approval_requires_credentials_and_reason_and_clears_password(self):
        for valid, reason in [(True, 'Replacement'), (False, 'Replacement'), (True, '')]:
            app.st.session_state['course_override_username'] = 'manager'
            app.st.session_state['course_override_password'] = 'password'
            app.st.session_state['course_minimum_override_note'] = reason
            with patch.object(app, '_constant_time_credentials_match', return_value=valid):
                app.authorize_course_minimum_override()
            self.assertEqual(app.course_minimum_override_active(), bool(valid and reason))
            self.assertEqual(app.st.session_state['course_override_password'], '')
        app.reset_course_minimum_override()

    def test_saved_approval_does_not_grant_live_authority(self):
        app.st.session_state['course_minimum_override_applied'] = True
        app.st.session_state['course_minimum_override_note'] = 'Prior approval'
        app.reset_course_minimum_override()
        self.assertFalse(app.course_minimum_override_active())
