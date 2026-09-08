"""UI adapters keep existing quoting calculations and PDF generation authoritative."""
import copy
import hashlib
import json
import streamlit as st
from pipedrive_workflow import Client, Store, WorkflowError, form_fields, plain, suggest_items, product_plan, sync, basket_products, attach_pdf, draft_key, quote_file_stem, quote_intakes, publish_saved_quote
from quote_comparison import comparison_payloads, comparison_files


@st.cache_data(ttl=60, show_spinner=False)
def pending_quote_intakes(domain, token):
    return quote_intakes(Client(domain, token))


@st.cache_data(ttl=300, show_spinner=False)
def read_intake_notes(domain, token, lead_id, deal_id=None):
    client = Client(domain, token)
    return client.all('/v1/notes', **({'deal_id':deal_id} if deal_id else {'lead_id':lead_id}))


def render_comparison(api, client, store, link, payload, valid_notes):
    st.subheader('Basket quote options')
    st.caption('Generate separate PDFs with sequential quote versions. Each uses the reviewed quantity, accessories, tax rate, and discount settings. Confirm freight for each option.')
    key = 'comparison_models_' + link['lead_id']
    if link.get('models'):
        st.session_state[key] = link['models']
    if key not in st.session_state:
        try:
            state = store.get(link['lead_id'])
            notes = read_intake_notes(client.base, client.session.headers['x-api-token'], link['lead_id'], state.get('deal_id'))
            note = next((n for n in notes if n['id'] == link.get('note_id')), None)
            if note is None:
                raise WorkflowError('The original form note could not be found.')
            fields = form_fields(note['content'])
            answer = next((v for k,v in fields.items() if 'which baskets' in k.lower()), '')
            st.session_state[key] = [m.strip() for m in answer.splitlines() if m.strip()]
        except WorkflowError as e:
            st.error(str(e))
    models = st.session_state.get(key, [])
    if not models:
        return None
    catalog = api['PRODUCTS'].to_dict('records')
    selected_models = st.multiselect('Options to include', models, default=models, key='batch_models_' + link['lead_id'])
    selections = []
    current_skus = {r['sku'] for r in payload['line_items']}
    portable = bool(current_skus & {'M2P', 'M5P', 'M7P', 'MXP', 'MXPRP'})
    for model in selected_models:
        choices = basket_products(model, catalog)
        if not choices:
            st.warning(f'{model} needs a product mapping.'); return None
        default = next((i for i,p in enumerate(choices) if p['SKU'] in current_skus), None)
        if default is None:
            default = next((i for i,p in enumerate(choices) if
                            ('Portable' in p['Name'] if portable else ('Standard' in p['Name'] or 'In Ground' in p['Name']))), 0)
        cols = st.columns([3, 1])
        product = cols[0].selectbox(model + ' configuration', choices, index=default,
                    format_func=lambda p: p['Name'], key='batch_config_' + link['lead_id'] + model)
        freight = cols[1].number_input(model + ' freight', min_value=0.0,
                    value=float(payload['fees']['freight']), step=1.0, key='batch_freight_' + link['lead_id'] + model)
        selections.append({'sku': product['SKU'], 'freight': freight})
    signature = hashlib.sha256(json.dumps([payload, selections], sort_keys=True).encode()).hexdigest()
    if st.button('Generate quote options', disabled=not (valid_notes and selections), type='primary'):
        try:
            state = store.assign(link['lead_id'])
            options = comparison_payloads(api, payload, selections, payload['quote_no'])
            files = comparison_files(api, options)
            st.session_state['comparison_bundle'] = {'signature':signature, 'files':files, 'options':options,
                                                    'reference':payload['quote_no']}
            failed = []
            for option in options:
                if not publish_saved_quote(api, store, link['lead_id'], option):
                    failed.append(option['quote_no'])
            st.session_state['comparison_bundle']['unsaved'] = failed
            st.session_state.pop('saved_quotes_snapshot_df', None)
            if not failed:
                st.success('All quote options are available in Saved Quotes.')
        except WorkflowError as e:
            st.error(str(e))
    bundle = st.session_state.get('comparison_bundle')
    if bundle and bundle['signature'] == signature:
        st.dataframe([{'Quote':p['quote_no'], 'Option':(p.get('pipedrive_link') or {}).get('option_sku',''), 'Total':p['totals']['grand_total']} for p in bundle['options']], hide_index=True)
        for file in bundle['files']:
            option = file['payload']
            st.download_button(f"Download {option['quote_no']} — {(option.get('pipedrive_link') or {}).get('option_sku','')}",
                data=file['pdf'], file_name=file['filename'], mime='application/pdf', on_click='ignore')
        if bundle.get('unsaved'):
            st.warning('Save not confirmed for: ' + ', '.join(bundle['unsaved']) + '. PDFs are downloadable. Check Saved Quotes before retrying.')
        st.caption('The separate PDFs will also be attached when you sync the current quote to the deal.')
        return bundle
    if bundle:
        st.info('The quote options changed. Generate them again to update the separate PDFs.')
    return None


@st.fragment(run_every=60)
def render_intake(api, domain, token):
    client, store = Client(domain, token), Store()
    active_link = st.session_state.get('pipedrive_link')
    old_number = st.session_state.get('quote_no', '')
    if active_link and old_number.startswith('Q-'):
        saved = store.get(active_link['lead_id'])
        new_number = saved.get('renumbered_quotes', {}).get(old_number)
        if new_number:
            st.session_state['quote_no'] = new_number
            st.session_state['order_doc_number_pdf'] = new_number
            active_link['option_sku'] = old_number.rsplit('-', 1)[-1]
            st.session_state.pop('pd_plan', None)
            st.session_state.pop('comparison_bundle', None)
            st.rerun()
    st.subheader('Submitted quote forms')
    if st.button('Refresh submitted forms'):
        pending_quote_intakes.clear()
    try:
        matches = pending_quote_intakes(domain, token)
    except WorkflowError as e:
        st.error(str(e)); return
    pending_ids = {r['id'] for r in matches}
    loaded = st.session_state.get('intake_read')
    if loaded and loaded['lead']['id'] not in pending_ids:
        st.session_state.pop('intake_read', None)
    if not matches:
        st.info('No pending quote-form submissions. Leads leave this list when converted to deals.')
    else:
        selected = st.selectbox('Submitted quote form', matches, format_func=lambda r:f'{r["title"]} • {r.get("add_time", "")[:10]}', key='submitted_quote_form')
        loaded = st.session_state.get('intake_read')
        if loaded and loaded['lead']['id'] != selected['id']:
            st.session_state.pop('intake_read', None)
        if st.button('Read form answers'):
            try:
                lead = client.get('/v1/leads/' + selected['id'])
                notes = client.all('/v1/notes', lead_id=lead['id'])
                forms = [n for n in notes if 'web form submission summary' in plain(n.get('content','')).lower()]
                forms.sort(key=lambda n:(n.get('add_time',''),n['id']), reverse=True)
                st.session_state['intake_read'] = {'lead':lead, 'forms':forms}
            except WorkflowError as e: st.error(str(e))
    loaded = st.session_state.get('intake_read')
    if not loaded: return
    lead = loaded['lead']
    st.write('Intake:',lead['title'])
    if not loaded['forms']:
        st.warning('No web form summary was found on this lead.'); return
    note = st.selectbox('Form submission', loaded['forms'], format_func=lambda n:f'{n.get("add_time", "")} • note {n["id"]}')
    fields = form_fields(note['content'])
    st.dataframe([{'Question':k,'Answer':v} for k,v in fields.items()], hide_index=True, use_container_width=True)
    catalog = api['PRODUCTS'].to_dict('records')
    proposed, issues = suggest_items(fields, catalog)
    basket_answer = next((v for k,v in fields.items() if 'which baskets' in k.lower()), '')
    models = [v.strip() for v in basket_answer.splitlines() if v.strip()]
    option = ''
    if models:
        st.info('Each basket model is a separate comparison quote. Select one option to review; its total includes only that model.')
        model = st.selectbox('Basket comparison option', models)
        choices = basket_products(model, catalog)
        selected_product = st.selectbox('Mounting / basket configuration', choices, index=None,
            placeholder='Choose the configuration for this option', format_func=lambda p:f"{p['SKU']} — {p['Name']}")
        import re
        requested = next((v for k,v in fields.items() if 'how many baskets' in k.lower()), '')
        quantity = st.number_input('Baskets in this comparison option', min_value=1,
            value=int(requested) if re.fullmatch(r'\d+',requested) and int(requested)>0 else 1, step=1)
        proposed = []
        def append_product(product, qty):
            import uuid
            proposed.append({'id':str(uuid.uuid4()),'sku':product['SKU'],'prev_sku':product['SKU'],
                'name':product['Name'],'qty':qty,'unit':float(product['UnitPrice']),
                'total':round(qty*float(product['UnitPrice']),2),'Notes':str(product.get('Notes') or ''),
                'previewChecked':True,'exclude_from_10_discount':False})
        if selected_product:
            option = selected_product['SKU']
            append_product(selected_product,quantity)
        for fragment, value, sku, title in [('number plates','stock','NP','Stock number plates'),
                                           ('number plates','custom','CNP','Custom number plates'),
                                           ('tee signs','basic color','BCTS','Basic Color tee signs')]:
            answer = next((v for k,v in fields.items() if fragment in k.lower()),'')
            if answer.strip().lower() == value:
                count = st.number_input(title + ' quantity (confirm)', min_value=0, value=quantity, step=1)
                if count:
                    product = next(p for p in catalog if p['SKU']==sku)
                    append_product(product,count)
        issues = [issue for issue in issues if not any(word in issue.lower() for word in ('which baskets', 'tee signs'))]
        st.caption('Accessory quantities start at the basket count for review. Freight, tax, and any special instructions remain editable in the quote.')
    if issues:
        st.warning('These selections need review in the quote builder:')
        for issue in issues: st.write(issue)
    st.caption(f'{len(proposed)} product row(s) ready. Previously saved edits for this option will be restored.')
    if st.button('Populate quote from this intake', type='primary', disabled=bool(models and not option)):
        try:
            person = client.get('/v1/persons/' + str(lead['person_id'])) if lead.get('person_id') else {}
            org = client.get('/v1/organizations/' + str(lead['organization_id'])) if lead.get('organization_id') else {}
            customer = api['pd_person_to_customer'](person or {}, org or {})
            for label, value in fields.items():
                key = label.lower().strip()
                mapped = {'name':'name','contact name':'name','company name':'company','email':'email','phone':'phone','phone number':'phone'}.get(key)
                if mapped and value and value != '(Blank)': customer[mapped] = value
                if key == 'shipping address' and value and value != '(Blank)':
                    addr,city,state,zip_code = api['_parse_us_address'](value)
                    customer.update(addr1=addr,city=city,state=state,zip=zip_code)
            state = store.assign(lead['id'])
            quote_no = state['quote_no']
            payload = next((p for p in reversed(list(state.get('quotes', {}).values())) if
                            (p.get('pipedrive_link') or {}).get('option_sku') == option), None) or {'customer':customer, 'line_items':proposed,
                'footer_notes':api['DEFAULT_FOOTER_NOTES'], 'fees':{},'totals':{},'tax_meta':{},'discount_meta':{},'order_meta':{}}
            api['load_quote_payload_into_session'](payload,payload.get('quote_no', quote_no))
            st.session_state['pipedrive_link'] = {'lead_id':lead['id'], 'note_id':note['id'], 'title':lead['title'], 'models':models, 'option_sku':option}
            st.session_state['intake_review_issues'] = issues
            st.rerun()
        except WorkflowError as e: st.error(str(e))


def render_sync(api, domain, token, payload):
    link = st.session_state.get('pipedrive_link')
    if not link: return
    client,store = Client(domain,token),Store()
    with st.expander('Pipedrive intake & deal sync', expanded=True):
        st.write(f'Linked intake: {link["title"]} • Quote {payload["quote_no"]}')
        st.caption('Sync the selected quote total to the deal. Each generated option is saved under its own quote number in Saved Quotes.')
        st.caption('Local test: conversion and updates are limited to the contact Cesar Quote Test. The sync attaches the quote PDF; you still send it manually.')
        valid_notes = (not payload.get('discount_meta', {}).get('active_discount_type') or
                       bool(payload.get('discount_meta', {}).get('discount_note', '').strip())) and (
                       not payload.get('discount_meta', {}).get('manager_pricing_applied') or
                       bool(payload.get('discount_meta', {}).get('manager_pricing_note', '').strip()))
        bundle = render_comparison(api, client, store, link, payload, valid_notes)
        if valid_notes and not bundle:
            pdf_bytes, _, _ = api['generate_pdf_preview_data'](payload)
            def save_downloaded_quote():
                saved = publish_saved_quote(api, store, link['lead_id'], payload)
                st.session_state['download_save_status'] = (payload['quote_no'], saved)
                st.session_state.pop('saved_quotes_snapshot_df', None)
            st.download_button('Download reviewed quote PDF', data=pdf_bytes,
                file_name=quote_file_stem(payload) + '.pdf', mime='application/pdf', on_click=save_downloaded_quote)
        if not valid_notes:
            st.warning('Enter the required discount / manager pricing reason before downloading or syncing.')
        save_status = st.session_state.get('download_save_status')
        if save_status and save_status[0] == payload['quote_no'] and not save_status[1]:
            st.warning('The download is ready, but saving to Saved Quotes was not confirmed. Check Saved Quotes before retrying.')
        if st.button('Sync to Pipedrive'):
            try:
                products = client.all('/v2/products')
                rows = product_plan(payload,products)
                st.session_state['pd_plan']={'rows':rows,'payload':copy.deepcopy(payload)}
                st.session_state['pd_reviewed'] = False
                st.session_state['pd_stages']=client.all('/v1/stages')
            except WorkflowError as e: st.error(str(e))
        plan=st.session_state.get('pd_plan')
        if plan:
            # A stale preview must never authorize a changed quote.
            import json
            current=json.dumps(payload,sort_keys=True,default=str)
            prior=json.dumps(plan['payload'],sort_keys=True,default=str)
            if current != prior:
                st.info('Quote changed. Click Sync to Pipedrive again to review the updated quote.');return
            st.dataframe([{k:r[k] for k in ('sku','name','quantity','item_price')} for r in plan['rows']],hide_index=True)
            stages=st.session_state.get('pd_stages',[])
            if not stages: st.error('No pipeline stages are available.');return
            stage=st.selectbox('Deal stage',stages,format_func=lambda s:f'{s.get("pipeline_name",s.get("pipeline_id"))} / {s["name"]}',
                index=next((i for i,s in enumerate(stages) if s['name']=='Initial Quote'),0))
            reviewed=st.checkbox('I reviewed products, quantities, shipping, tax, and the total.',key='pd_reviewed')
            if st.button('Create / update test deal',disabled=not (reviewed and valid_notes),type='primary'):
                try:
                    pdf,_,_=api['generate_pdf_preview_data'](payload)
                    # Revalidate mapping against live catalog just before writing.
                    rows=product_plan(payload,client.all('/v2/products'))
                    if not publish_saved_quote(api, store, link['lead_id'], payload):
                        raise WorkflowError('Saving this quote to Saved Quotes was not confirmed. Check Saved Quotes before retrying the sync.')
                    deal,total=sync(client,store,link['lead_id'],payload,rows,stage['id'],pdf)
                    if bundle:
                        for file in bundle['files']:
                            attach_pdf(client, deal, file['pdf'], quote_file_stem(file['payload']),
                                       identity=json.dumps(file['payload'], sort_keys=True, default=str).encode())
                    st.session_state['pd_sync_result'] = {'deal':deal, 'total':str(total), 'quote':payload['quote_no']}
                    pending_quote_intakes.clear()
                    st.session_state.pop('intake_read', None)
                    st.rerun()
                except WorkflowError as e: st.error(str(e))
        result = st.session_state.get('pd_sync_result')
        if result:
            st.success(f"Last synced quote {result['quote']}: deal {result['deal']} matched ${result['total']}. PDFs attached.")
            st.link_button('Open Pipedrive deal',client.base.removesuffix('/api')+f"/deal/{result['deal']}")
