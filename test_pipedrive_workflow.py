import copy
import tempfile
import unittest
from decimal import Decimal
from pipedrive_workflow import Store, WorkflowError, form_fields, suggest_items, product_plan, sync, basket_products, draft_key, quote_file_stem


def payload(qty=30):
    return {'quote_no':'Q-TEST','customer':{'name':'Cesar Quote Test'},
        'line_items':[{'id':'basket','sku':'M5CO','name':'Mach 5 Collar Only','qty':qty,'unit':495,'total':qty*495}],
        'fees':{'freight':125,'drop_ship_fee':10},
        'totals':{'sales_tax':42.25,'ten_percent_discount':100,'manager_discount':0,'grand_total':qty*495+77.25}}

PRODUCTS=[{'id':1,'code':'M5CO','name':'Mach 5 Collar Only','price':450},
 {'id':35,'code':'FT','name':'Freight'},{'id':38,'code':'TX','name':'Tax'},
 {'id':100,'code':'','name':'Drop Ship Fee'},{'id':42,'code':'ADD','name':'Additional Discount'},
 {'id':126,'code':'','name':'FT'}]

class Fake:
    def __init__(self): self.rows=[]; self.value=0; self.conversions=0; self.files=[];self.pending=False;self.posts=0;self.name='Cesar Quote Test'
    def get(self,path,**kw):
        if '/convert/status/' in path:return {'status':'running'} if self.pending else {'status':'completed','deal_id':7}
        if '/persons/' in path:return {'name':self.name}
        if '/leads/' in path:return {'person_id':1}
        return {'person_id':1,'value':self.value}
    def all(self,path,**kw):return copy.deepcopy(self.files if path.endswith('/files') else self.rows)
    def request(self,method,path,**kw):
        body=kw.get('json',{})
        if path.endswith('/convert/deal'):self.conversions+=1;return {'data':{'conversion_id':'job'}}
        if path=='/v1/files':
            file={'id':len(self.files)+1,'name':kw['files']['file'][0]}
            self.files.append(file)
            return {'data':file}
        elif path.startswith('/v1/files/'):
            next(f for f in self.files if f['id']==int(path.rsplit('/',1)[1])).update(body)
        elif path.endswith('/products'):
            self.posts+=1;self.rows.append(dict(body,id=self.posts,sum=body['quantity']*body['item_price']))
        elif '/products/' in path:
            id=int(path.rsplit('/',1)[1])
            if method=='DELETE':self.rows=[r for r in self.rows if r['id']!=id]
            else:
                row=next(r for r in self.rows if r['id']==id);row.update(body,sum=body['quantity']*body['item_price'])
        else:
            if 'value' in body:
                raise AssertionError('Pipedrive rejects direct value updates for deals with products.')
            self.value=sum(r['sum'] for r in self.rows)
        return {'data':{}}

class Tests(unittest.TestCase):
    def test_number_and_option_are_separate(self):
        with tempfile.TemporaryDirectory() as d:
            number=Store(d+'/db').assign('lead')['quote_no']
            self.assertRegex(number,r'^\d{4}-\d{4}$')
        p={'quote_no':'0908-1507-V2','pipedrive_link':{'option_sku':'M5STD'}}
        self.assertEqual(quote_file_stem(p),'0908-1507-V2_Quote')
        self.assertEqual(draft_key(p),'0908-1507-V2')

    def test_basket_families(self):
        import csv
        with open('products.csv') as source: catalog=list(csv.DictReader(source))
        for model, expected in [('Mach 2 Pro',2),('Mach 5',4),('Mach 7',4),('Mach X',4)]:
            self.assertEqual(len(basket_products(model,catalog)),expected)
        self.assertFalse(any(p['SKU'].startswith('MXPR') for p in basket_products('Mach X',catalog)))

    def test_form(self):
        f=form_fields('<b><u>Web Form submission summary</u></b><br><b>What type of basket?</b><br>Mach 5 Collar Only<br><b>How many baskets?</b><br>30')
        items,issues=suggest_items(f,[{'SKU':'M5CO','Name':'Mach 5 Collar Only','UnitPrice':495}])
        self.assertEqual(items[0]['qty'],30);self.assertFalse(issues)
        f['How many baskets?']='30 or 26';self.assertFalse(suggest_items(f,[{'SKU':'M5CO','Name':'Mach 5 Collar Only','UnitPrice':495}])[0])
    def test_plan(self):
        rows=product_plan(payload(),PRODUCTS)
        self.assertEqual(rows[0]['item_price'],495)
        self.assertEqual(next(r for r in rows if r['sku']=='FT')['product_id'],35)
        self.assertTrue(all(r['tax']==0 and r['tax_method']=='none' for r in rows))
        p=payload();p['totals']['grand_total']+=1
        with self.assertRaises(WorkflowError):product_plan(p,PRODUCTS)
        with self.assertRaises(WorkflowError):product_plan(payload(),PRODUCTS+[PRODUCTS[0]])
    def test_sync_revision_and_retry(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(d+'/db');store.assign('lead');api=Fake();p=payload()
            api.pending=True
            with self.assertRaises(WorkflowError):sync(api,store,'lead',p,product_plan(p,PRODUCTS),1,b'pdf')
            api.pending=False
            sync(api,store,'lead',p,product_plan(p,PRODUCTS),1,b'pdf')
            sync(api,store,'lead',p,product_plan(p,PRODUCTS),1,b'pdf2')
            self.assertEqual(api.conversions,1);self.assertEqual(api.posts,5);self.assertEqual(len(api.files),1)
            p=payload(26);sync(api,store,'lead',p,product_plan(p,PRODUCTS),1,b'revision')
            self.assertEqual(api.posts,5);self.assertEqual(api.rows[0]['quantity'],26)
            self.assertEqual(Decimal(str(api.value)),Decimal('12947.25'))
    def test_non_test_contact_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(d+'/db');store.assign('lead');api=Fake();api.name='Someone Else'
            with self.assertRaises(WorkflowError):sync(api,store,'lead',payload(),product_plan(payload(),PRODUCTS),1,b'pdf')
            self.assertEqual(api.conversions,0)
    def test_unmanaged_rows_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(d+'/db');store.assign('lead');api=Fake();api.rows=[{'id':8,'comments':'manual'}]
            with self.assertRaises(WorkflowError):sync(api,store,'lead',payload(),product_plan(payload(),PRODUCTS),1,b'pdf')
            self.assertEqual(api.posts,0)

if __name__=='__main__':unittest.main()
