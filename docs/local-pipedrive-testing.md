# Local Pipedrive intake testing

Start from the project directory:

```sh
PIPEDRIVE_LOCAL_TEST=1 new_tools/bin/python -m streamlit run app.py --server.address 127.0.0.1 --server.port 8502
```

Open http://127.0.0.1:8502. The normal deployed UI is unchanged unless the local-test flag is enabled.

1. Open Lookup Tools → Pipedrive. Search for Cesar Quote Test and read the form answers.
2. Select a basket comparison option and its mounting configuration. Confirm basket and accessory quantities. Each option contains the full requested basket count; models are not combined.
3. Populate the quote, review pricing, freight, tax, and notes. Use Save local intake draft before switching options. Quote numbers use the existing Pacific-time `MMDD-HHMM` format, with `-V2`, `-V3`, etc. for revisions. Comparison options share the quote number and are stored separately by basket SKU. Individual filenames use `0908-1507_Quote-M5STD.pdf`; the packet uses `0908-1507_Quote-Options.pdf`.
4. Download reviewed quote PDF. For a comparison packet, use All basket options in one PDF → Load requested comparison options, choose each configuration and freight amount, then Generate all options PDF. The result contains one complete quote per selected model and saves each option as a local draft. These local downloads do not save to the shared quote sheet or send email. The existing Generate & SAVE Quote PDF button retains its existing behavior.
5. Check Pipedrive product mapping, select the stage, review, and create/update the test deal. This writes to the real connected Pipedrive account, but only when the contact is named Cesar Quote Test. Sync only the chosen option; a later option updates the same deal rather than inflating the opportunity with all alternatives.

The quote controls transactional prices, fees, discounts, and tax. Pipedrive catalog prices are not modified. Unknown or ambiguous product matches block syncing. Tax is a separate TX row; product-level tax is disabled. Pipedrive calculates deal value from attached products; sync verifies this value without trying to overwrite it. Form text is treated as data and only explicit recognized choices are imported.

When a current comparison packet exists, deal sync attaches both the selected quote and the packet. The deal value represents only the selected quote. Uploads are verified by reading the deal files back; retrying the same comparison bytes reuses the existing file. Any change to the quote or comparison selections invalidates the generated packet until it is generated again.

Intake numbers, drafts, comparison options, and conversion job/deal IDs live in `.local/pipedrive.sqlite3`. Keep this local database for revisions. Do not delete it during a test. Sync reconciles rows using an intake-specific marker and verifies returned product sums and the saved deal value against the quote. Unmanaged deal rows block the sync. A lost conversion response blocks automatic conversion retry rather than risking a duplicate deal. Resume a known pending conversion by clicking sync again.

The current importer supports the observed course form, including multiple basket models, stock/custom plates, and Basic Color tee signs. Other answers remain visible for manual review. Mounting style is intentionally selected during review because the form does not specify it. Accessory quantities default to the basket count and are explicitly reviewable.

Tests: `new_tools/bin/python -m unittest test_pipedrive_workflow test_quote_comparison -v`.
