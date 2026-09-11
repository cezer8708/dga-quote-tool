import copy
import io
import unittest
from unittest.mock import patch

import app
from PyPDF2 import PdfReader
from quote_comparison import comparison_payloads, comparison_files
from pipedrive_workflow import WorkflowError


class ComparisonTests(unittest.TestCase):
    def source(self):
        return {'quote_no':'0908-1507','pipedrive_link':{'lead_id':'test','option_sku':'M2IG'}, 'customer':{'name':'Example'}, 'footer_notes':'Test',
            'order_meta':{},'discount_meta':{},
            'fees':{'freight':0,'drop_ship_fee':0},
            'totals':{'tax_rate_pct':.1},
            'line_items':[{'id':'basket','sku':'M2IG','name':'Mach 2 Pro - In Ground','qty':24,
                           'unit':300,'total':7200,'previewChecked':True},
                          {'id':'plate','sku':'NP','name':'Stock plate','qty':24,'unit':35,'total':840}]}

    def test_separate_discounts_tax_freight_and_source_preserved(self):
        source=self.source(); original=copy.deepcopy(source)
        selections=[{'sku':s,'freight':100} for s in ['M2IG','M5STD','M7STD','MXSTD']]
        results=comparison_payloads(vars(app),source,selections,'0908-1507')
        self.assertEqual(source,original)
        self.assertEqual([p['totals']['grand_total'] for p in results],[6314,8954,10274,11594])
        self.assertTrue(all(p['line_items'][0]['qty']==24 for p in results))
        self.assertEqual([p['quote_no'] for p in results],['0908-1507','0908-1507-V2','0908-1507-V3','0908-1507-V4'])
        self.assertEqual(len({p['pipedrive_link']['option_sku'] for p in results}),4)
        for i,p in enumerate(results):
            codes={r['sku'] for r in p['line_items']}
            self.assertEqual('CD-M2P' in codes,i==0)
            self.assertEqual('CD' in codes,i!=0)

    def test_manual_discount_removal_survives_recalculation_and_reload(self):
        for sku, setting, removed, retained in [
            ('M5STD', 'apply_course_discount', 'CD', '50th'),
            ('M5STD', 'apply_anniversary_discount', '50th', 'CD'),
            ('M2IG', 'apply_course_discount', 'CD-M2P', '50thM2'),
            ('M2IG', 'apply_anniversary_discount', '50thM2', 'CD-M2P'),
        ]:
            with self.subTest(sku=sku, setting=setting):
                source = self.source()
                result = comparison_payloads(vars(app), source, [{'sku':sku, 'freight':0}], '0908-1507')[0]
                app.load_quote_payload_into_session(result, result['quote_no'])
                discount = next(r for r in app.st.session_state['line_items'] if r['sku'] == removed)
                app.remove_item(discount['id'])
                self.assertFalse(app.st.session_state[setting])
                app.ensure_course_discount(app.st.session_state['line_items'])
                codes = {r['sku'] for r in app.st.session_state['line_items']}
                self.assertNotIn(removed, codes)
                self.assertIn(retained, codes)
                saved = app.get_current_payload(0, 0, 0, 0, 0, 0, 0, '', 0)
                app.load_quote_payload_into_session(saved, result['quote_no'])
                self.assertFalse(app.st.session_state[setting])
                compared = comparison_payloads(vars(app), saved, [{'sku':sku, 'freight':0}], '0908-1507')[0]
                self.assertNotIn(removed, {r['sku'] for r in compared['line_items']})
                app.st.session_state[setting] = True
                app.ensure_course_discount(app.st.session_state['line_items'])
                self.assertIn(removed, {r['sku'] for r in app.st.session_state['line_items']})

    def test_one_complete_page_per_option(self):
        results=comparison_payloads(vars(app),self.source(),
                    [{'sku':s,'freight':0} for s in ['M2IG','M5STD','M7STD','MXSTD']],'0908-1507')
        files=comparison_files(vars(app),results)
        self.assertEqual(len(files),4)
        for file,payload in zip(files,results):
            document=PdfReader(io.BytesIO(file['pdf']))
            self.assertEqual(len(document.pages),1)
            page=document.pages[0]
            self.assertIn(payload['quote_no'],page.extract_text())
            self.assertIn(f"{payload['totals']['grand_total']:,.2f}",page.extract_text())

    def test_mixed_baskets_cannot_silently_duplicate_options(self):
        source=self.source(); source['line_items'].append(dict(source['line_items'][0],id='other',sku='M5STD'))
        with self.assertRaises(WorkflowError):
            comparison_payloads(vars(app),source,[{'sku':'M7STD','freight':0}],'0908-1507')

    def test_existing_reviewed_price_is_preserved(self):
        source=self.source(); source['line_items'][0]['unit']=310
        result=comparison_payloads(vars(app),source,[{'sku':'M2IG','freight':0}],'0908-1507')[0]
        self.assertEqual(result['line_items'][0]['unit'],310)


if __name__=='__main__': unittest.main()
