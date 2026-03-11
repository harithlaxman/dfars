"""
get_dfars_diffs.py

Parse all DFARS change docx files in data/dfars_changes/.
For each file produce section-level before/after text:
  - "before" = original text with strikethrough runs included and bold-bracketed
    additions stripped out.
  - "after"  = updated text with bold-bracketed additions retained (brackets
    removed) and strikethrough runs stripped out.

Heading detection rules (applied in order, first match wins):
  PART     – "PART <number>" (e.g. "PART 212—ACQUISITION OF COMMERCIAL ITEMS")
  SUBPART  – "SUBPART <number>.<number>" (e.g. "SUBPART 217.1 - MULTIYEAR …")
  SECTION  – digits and dots meeting DFARS numbering (e.g. "212.301", "252.247-7027")
              captured at the START of the paragraph text after stripping leading
              whitespace and tabs.

Output is a list of dicts, one per input file, written to data/dfars_diffs.json.
Each dict has:
  {
    "file": "<filename>",
    "case": "<case id>",            # inferred from filename
    "sections": [
      {
        "part": "PART 212—…",
        "subpart": "SUBPART 212.3—…",  # may be None
        "section": "212.301 …",
        "before": "<reconstructed before text>",
        "after":  "<reconstructed after text>"
      },
      …
    ]
  }
"""

import csv
import json
import os
import re
from pathlib import Path

from docx import Document
from docx.oxml.ns import qn

# ---------------------------------------------------------------------------
# Heading detection regexes
# ---------------------------------------------------------------------------

# PART heading: word PART followed by a number (allowing em-dash or hyphen)
_RE_PART = re.compile(r'^PART\s+\d{3}', re.IGNORECASE)

# SUBPART heading: word SUBPART followed by digits.digits
_RE_SUBPART = re.compile(r'^SUBPART\s+\d{3}\.\d+', re.IGNORECASE)

# Section / sub-section heading: starts with DFARS-style number
# Examples: 212.301  /  252.247-7027  /  206.303-70
_RE_SECTION = re.compile(r'^\d{3}\.\d+(?:-\d+)?')

# Bullets / clause indicators that are NOT section headings (e.g. (a), (1), (iv))
_RE_ALPHA_BULLET = re.compile(r'^\([a-zA-Z0-9]+\)')

# "* * * * *" filler lines
_RE_STARS = re.compile(r'^\*[\s\*]+\*$')


def _heading_type(text: str):
    """Return ('part'|'subpart'|'section'|None, text)."""
    t = text.strip().lstrip('\t ')
    if not t:
        return None, t
    if _RE_PART.match(t):
        return 'part', t
    if _RE_SUBPART.match(t):
        return 'subpart', t
    if _RE_SECTION.match(t):
        return 'section', t
    return None, t


# ---------------------------------------------------------------------------
# Run-level text extraction helpers
# ---------------------------------------------------------------------------

def _run_is_strike(run) -> bool:
    """True if the run has strikethrough formatting."""
    # Check direct strike attribute; also check rPr in XML for <w:strike> or <w:dstrike>
    if run.font.strike:
        return True
    rpr = run._r.find(qn('w:rPr'))
    if rpr is not None:
        if rpr.find(qn('w:strike')) is not None:
            return True
        if rpr.find(qn('w:dstrike')) is not None:
            return True
    return False


def _run_is_bold(run) -> bool:
    """True if the run is explicitly bold."""
    return bool(run.bold)


def _extract_versions(paragraph) -> tuple[str, str]:
    """
    Given a paragraph, return (before_text, after_text).

    Convention in DFARS change docs:
      - Strikethrough text  → exists in BEFORE, removed in AFTER
      - Bold text in [...]  → new in AFTER, not in BEFORE (but the *brackets*
                              are themselves bold; the enclosed content is added)

    We iterate runs and classify each one.
    """
    before_parts = []
    after_parts = []

    for run in paragraph.runs:
        text = run.text
        if not text:
            continue

        is_strike = _run_is_strike(run)
        is_bold = _run_is_bold(run)

        if is_strike:
            # Removed text – appears in before only
            before_parts.append(text)
            # Skip from after
        elif is_bold:
            # Added text – the brackets themselves are bold too, so strip outer [ ]
            # Collect the raw bold text; brackets will be stripped when building after
            before_parts.append('')          # nothing for before
            after_parts.append(text)        # keep for after (brackets stripped later)
        else:
            # Unchanged text
            before_parts.append(text)
            after_parts.append(text)

    before = ''.join(before_parts).strip()
    # Strip square brackets from added (bold) segments in after
    after_raw = ''.join(after_parts)
    # Remove outermost brackets: replace [text] patterns where [ and ] are bold
    # Because we've already concatenated, just strip all [ and ] characters that
    # originate from bold runs – simplest approach: strip brackets from the after string
    after = _strip_brackets(after_raw).strip()

    return before, after


def _strip_brackets(text: str) -> str:
    """Remove [ and ] characters used as change markers in the bold (added) text."""
    # We only want to remove the marker brackets, not brackets that are part of
    # regular references like "(Pub. L. 110-417)". Bold brackets come in pairs
    # wrapping added content. Since we've already isolated bold runs above, we
    # just strip all [ ] from the combined after string.
    return text.replace('[', '').replace(']', '')


# ---------------------------------------------------------------------------
# Paragraph text (full, raw)
# ---------------------------------------------------------------------------

def _para_text(paragraph) -> str:
    return paragraph.text.strip().lstrip('\t ')


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_docx(path: str) -> dict:
    """Parse a single DFARS change docx and return structured dict."""
    doc = Document(path)
    filename = os.path.basename(path)

    # Extract case id from filename (e.g. "2007-D002 LILO.docx" → "2007-D002")
    case_match = re.match(r'(\d{4}-[A-Z]\d+)', filename, re.IGNORECASE)
    case_id = case_match.group(1) if case_match else filename

    # -----------------------------------------------------------------------
    # Walk paragraphs, tracking current PART / SUBPART / SECTION context
    # -----------------------------------------------------------------------
    current_part: str | None = None
    current_subpart: str | None = None
    current_section: str | None = None

    # Accumulate paragraphs per section
    # Key: (part, subpart, section)  Value: list of (before, after) strings
    section_paras: dict[tuple, list[tuple[str, str]]] = {}

    def _section_key():
        return (current_part, current_subpart, current_section)

    def _add_para(before: str, after: str):
        key = _section_key()
        if key not in section_paras:
            section_paras[key] = []
        section_paras[key].append((before, after))

    for para in doc.paragraphs:
        raw = _para_text(para)
        if not raw:
            continue
        if _RE_STARS.match(raw):
            continue

        htype, htext = _heading_type(raw)

        if htype == 'part':
            current_part = htext
            current_subpart = None
            current_section = None
            continue

        if htype == 'subpart':
            current_subpart = htext
            current_section = None
            continue

        if htype == 'section':
            current_section = htext
            # Don't add the section heading line itself as content
            continue

        # Regular content paragraph – extract before/after
        if current_section is None and current_subpart is None and current_part is None:
            # Preamble / metadata lines – skip
            continue

        before, after = _extract_versions(para)
        if before or after:
            _add_para(before, after)

    # -----------------------------------------------------------------------
    # Build output sections list
    # -----------------------------------------------------------------------
    sections = []
    for (part, subpart, section), para_list in section_paras.items():
        before_text = '\n'.join(b for b, _ in para_list if b)
        after_text = '\n'.join(a for _, a in para_list if a)
        sections.append({
            'part': part,
            'subpart': subpart,
            'section': section,
            'before': before_text,
            'after': after_text,
        })

    return {
        'file': filename,
        'case': case_id,
        'sections': sections,
    }


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

def parse_all(input_dir: str, csv_path: str, output_path: str):
    input_path = Path(input_dir)

    # Read file list and document_number from CSV
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    print(f'Found {len(rows)} entries in {csv_path}')

    results = []
    errors = []
    for i, row in enumerate(rows, 1):
        docx_filename = row['dfars_changes_doc'].strip()
        document_number = row['document_number'].strip()
        fp = input_path / docx_filename
        if not fp.exists():
            print(f'[{i}/{len(rows)}] MISSING {docx_filename}')
            errors.append({'file': docx_filename, 'error': 'file not found'})
            continue
        try:
            result = parse_docx(str(fp))
            result['document_number'] = document_number
            results.append(result)
            n_sections = len(result['sections'])
            print(f'[{i}/{len(rows)}] {docx_filename}: {n_sections} section(s)')
        except Exception as e:
            print(f'[{i}/{len(rows)}] ERROR {docx_filename}: {e}')
            errors.append({'file': docx_filename, 'error': str(e)})

    output = {'results': results, 'errors': errors}
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f'\nDone. {len(results)} parsed, {len(errors)} errors.')
    print(f'Output written to {output_path}')
    return output


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    CHANGES_DIR = 'data/dfars_changes'
    CSV_FILE = 'data/doc_to_dfars.csv'
    OUTPUT_FILE = 'data/dfars_diffs.json'
    parse_all(CHANGES_DIR, CSV_FILE, OUTPUT_FILE)
