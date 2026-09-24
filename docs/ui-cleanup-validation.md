# Centered quoting UI validation

The final layout uses Streamlit's native centered container (`layout="centered"`), with no main-container max-width override. The PDF remains in the left sidebar. The final pass changes presentation only: existing widget keys, quote payloads, pricing, numbering/versioning, external integrations, and PDF rendering are retained.

## Layout

The Current Quote toolbar measures 40px high at 1440 × 1000, with both actions on one row. Customer labels sit above the inputs. Shipping and Billing share one field definition with Company/Name 50/50, Phone/Email 35/65, Address 100, and City/State/ZIP 50/20/30. Corresponding controls have matching width, height, and Y position. On mobile, each address stacks as a complete group.

Line items retain compact order-entry rows, small Add/Delete controls, a discount summary with Manage, and a compact Details popover. Notes retain multiline support for existing data but display at a compact initial height. Literal dollar amounts in discount summaries avoid Markdown math rendering. Fees use 30/35/35 columns. Closed disclosures measure 34px; totals remain right aligned. Final actions use content-width buttons.

Desktop and 390 × 844 mobile browser checks cover alignment, Details, and overflow. Screenshots are in `output/playwright/`: `centered-desktop.png`, `centered-order-row.png`, `centered-details.png`, and `centered-mobile.png`.

## Regression checks

All 27 tests pass with:

```sh
python -m unittest test_document_dates test_ui_workflow test_quote_version_widgets test_pipedrive_workflow test_quote_comparison test_security_controls
```

Interaction coverage includes new quotes and versions, legacy saved-quote loading, Pipedrive lookup, custom/catalog items, quantities/removal, both automatic discount families, 10% discount, manager authorization, manual/Santa Cruz tax, fees/freight, billing restoration, notes/footer, preview, and actual PDF generation. External lookup/save boundaries use fixtures; no production writes or deployment were performed.

A controlled before/after fixture with both product families, discounts, custom items, fees, tax, multiline addresses, and notes produced an identical complete payload, identical PDF text, and identical rendered PDF page pixels. Its grand total remained $8,275.15. This comparison used the source snapshot from before the order-row/centered passes.

Quote 0924-0859 was confirmed by the user to be unsaved. The controlled comparison is not a claim that this unsaved quote was recovered or verified.

Earlier cleanup work also moved manager authorization to a callback to retain downstream widget state and added a session-only backup for restoring separate billing information when Same as shipping is unchecked. Those behaviors are covered by the interaction tests.

## Order submission date follow-up

Processing an order now copies the payload and stamps its date with the current Pacific timestamp before generating the PDF and saving the order record. The source quote payload/session date is preserved. Quote generation continues to use its existing document date. A regression test covers both paths, matching PDF metadata to the saved record and checking source-payload immutability, including a Pacific late-night timestamp. This is an intentional behavior change after the presentation-only pass.

## Second address line follow-up

Shipping and Billing now expose Address #1 and Address #2. Optional `ship_addr2` and `bill_addr2` customer fields persist within the existing JSON payload; older records default to blank. Same-as-shipping copies/restores both lines. Quote and order PDFs include line 2 only when populated. Browser checks confirm matching widths and Y positions; interaction tests cover entry, saved loading, billing sync/restoration, both PDF templates, and new-quote reset.
