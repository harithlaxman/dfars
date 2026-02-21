# DFARS NDAA Data Pipeline — Summary

## Source: Implementation Tracker PDF

**File:** `DFARS_NDAA_Implementation_Tracker.pdf`
**Script:** `parse_tracker.py`

The PDF contains NDAA sections spanning **FY10–FY24** with implementation status, case numbers, and Final Rule FRN citations.

### Extraction Results → `ndaa_final_rule_citations.csv`

| Metric | Count |
|---|---|
| **Total rows extracted** | **252** |
| Implemented | 244 |
| Partially Implemented | 3 |
| Open | 2 |
| Closed | 2 |
| Multi-citation rows (newline-separated) | 4 |
| Empty citations | 0 |

**Rows by NDAA year:**

| Year | Rows | | Year | Rows | | Year | Rows |
|------|------|-|------|------|-|------|------|
| FY10 | 13   | | FY15 | 12   | | FY20 | 16   |
| FY11 | 16   | | FY16 | 21   | | FY21 | 16   |
| FY12 | 21   | | FY17 | 39   | | FY22 | 11   |
| FY13 | 20   | | FY18 | 27   | | FY23 | 10   |
| FY14 | 3    | | FY19 | 22   | | FY24 | 5    |

---

## Federal Register API Lookup

**Script:** `fetch_citation.py`
**Method:** Search by publication date + start page (replaces unreliable `conditions[term]` search)
**Fallback:** Broader "defense-department" agency for joint DoD/GSA/NASA rules

### Lookup Results → `ndaa_final_rule_with_rationale.csv`

| Metric | Count |
|---|---|
| **Citations matched to FR URL** | **246 / 252** (97.6%) |
| Not found | 6 |

**6 unresolved citations:**
- `79 FR 26091` (×2) — no page match on 2014-05-06
- `76 FR 52131` — no page match on 2011-08-19
- `77 FR 11353` — no page match on 2012-02-24
- `80 FR 4997` — no page match on 2015-01-29
- `75FR 13413` — malformed (missing space before FR in source PDF)

---

## HTML Downloads

**Script:** `download_htmls.py`
**Output:** `ndaa_cases/html_from_tracker/`

| Metric | Value |
|---|---|
| **Unique HTMLs downloaded** | **197** |
| Download failures | 0 |
| Total size | 8.2 MB |

> [!NOTE]
> 246 citation URLs map to 197 unique documents because some NDAA sections share the same Final Rule (e.g. multiple sections implemented in a single rulemaking).
