"""
Extract affected DFARS sections and amendment details from Federal Register
HTML files referenced in data/ndaa_final_rule_with_rationale.csv.

The input CSV already contains NDAA references (ndaa_year, ndaa_section).
This script only extracts the DFARS sections affected by each rule.

Outputs:
  1. data/ndaa_final_rule_with_dfars_sections.csv
     — original CSV with an added 'affected_dfars_sections' column
  2. data/ndaa_final_rule_dfars_changes.csv
     — detailed per-section rows with columns:
       ndaa_year, ndaa_section, case_number, document_id,
       affected_dfars_section, amendment_instruction, content
"""

import csv
import re
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm


# ── HTML parsing helpers (reused from extract_dfars_sections.py) ────


def _get_amendment_instruction(section_div: Tag) -> str:
    """
    Walk backwards from a div.section to find the amendment-part <p>
    that introduced it (e.g. "2. Revise section 216.102 to read as follows:").
    """
    for prev in section_div.previous_siblings:
        if isinstance(prev, Tag) and "amendment-part" in prev.get("class", []):
            text = prev.get_text(separator=" ", strip=True)
            if text:
                return text
    return ""


def _get_section_content(section_div: Tag) -> str:
    """
    Extract the textual content of a div.section, excluding the section
    number header itself.
    """
    parts = []
    for child in section_div.children:
        if isinstance(child, Tag):
            # Skip the section-number div
            if "sectno" in child.get("class", []):
                continue
            parts.append(child.get_text(separator=" ", strip=True))
    return "\n".join(p for p in parts if p)


def _collect_amended_section_instructions(
    soup: BeautifulSoup,
) -> dict[str, dict[str, str]]:
    """
    For sections that are '[Amended]' (no new content provided), collect
    the amendment-part instructions that describe the changes.

    Returns a dict mapping section_num -> {"instruction": ..., "content": ...}
    """
    result: dict[str, dict[str, str]] = {}
    for section_div in soup.find_all("div", class_="section"):
        subject = section_div.find("div", class_="section-subject")
        if not subject or "[Amended]" not in subject.get_text():
            continue
        sectno_div = section_div.find("div", class_="sectno-reference")
        if not sectno_div:
            continue
        sec_id = sectno_div.get("id", "")
        section_num = sec_id.replace("sectno-reference-", "").strip()
        if not section_num:
            continue

        main_instruction = ""
        sub_instructions = []
        for elem in section_div.find_all_next("p", class_="amendment-part"):
            if not isinstance(elem, Tag):
                continue
            if elem.find("span", class_="amendment-part-subnumber"):
                sub_instructions.append(
                    elem.get_text(separator=" ", strip=True)
                )
            elif not main_instruction:
                main_instruction = elem.get_text(separator=" ", strip=True)
            else:
                break

        result[section_num] = {
            "instruction": main_instruction,
            "content": "\n".join(sub_instructions) if sub_instructions else "",
        }
    return result


def extract_dfars_sections(html_path: Path) -> list[dict]:
    """
    Parse a single HTML file and return a list of dicts, one per affected
    DFARS section, with keys:
      - affected_dfars_section  (e.g. "216.102")
      - amendment_instruction   (e.g. "2. Revise section 216.102 ...")
      - content                 (the new section text, or sub-instructions)
    """
    with open(html_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    # Pre-collect [Amended] sub-instructions
    amended_instructions = _collect_amended_section_instructions(soup)

    results: list[dict] = []
    seen_sections: set[str] = set()

    for section_div in soup.find_all("div", class_="section"):
        sectno_div = section_div.find("div", class_="sectno-reference")
        if not sectno_div:
            continue
        sec_id = sectno_div.get("id", "")
        section_num = sec_id.replace("sectno-reference-", "").strip()
        if not section_num or section_num in seen_sections:
            continue
        seen_sections.add(section_num)

        amendment_instruction = _get_amendment_instruction(section_div)
        content = _get_section_content(section_div)

        # For [Amended] sections, use the collected instruction + sub-instructions
        if not content or content.strip() == "[Amended]":
            amended_info = amended_instructions.get(section_num, {})
            if amended_info:
                if amended_info.get("instruction"):
                    amendment_instruction = amended_info["instruction"]
                content = amended_info.get("content", content)

        results.append({
            "affected_dfars_section": section_num,
            "amendment_instruction": amendment_instruction,
            "content": content,
        })

    # Fallback: if no div.section found, try regex on amendment-part text
    if not results:
        for p_tag in soup.find_all("p", class_="amendment-part"):
            text = p_tag.get_text(separator=" ", strip=True)
            matches = re.findall(
                r"[Ss]ection\s+(\d{3}\.\d[\w\-\.]*)", text
            )
            for m in matches:
                if m not in seen_sections:
                    seen_sections.add(m)
                    results.append({
                        "affected_dfars_section": m,
                        "amendment_instruction": text,
                        "content": "",
                    })

    return results


def _extract_doc_id_from_url(url: str) -> str | None:
    """
    Extract the document ID from a Federal Register full_text HTML URL.
    E.g. 'https://www.federalregister.gov/.../2024-13863.html' -> '2024-13863'
    """
    url = url.strip()
    if not url:
        return None
    parsed = urlparse(url)
    # Last segment of the path, minus .html
    last_segment = parsed.path.rstrip("/").split("/")[-1]
    doc_id = last_segment.replace(".html", "")
    return doc_id if doc_id else None


# ── Main ───────────────────────────────────────────────────────────


def main():
    csv_path = Path("./data/ndaa_final_rule_with_rationale.csv")
    html_dir = Path("./data/fr_cases/html_from_tracker")
    output_path = Path("./data/ndaa_final_rule_with_dfars_sections.csv")
    detail_path = Path("./data/ndaa_final_rule_dfars_changes.csv")

    # Read existing CSV
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    print(f"Found {len(df)} rows in {csv_path}\n")

    all_details: list[dict] = []
    affected_sections_col: list[str] = []
    skipped: list[str] = []
    processed_count = 0

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Extracting DFARS sections"):
        url_field = row.get("fr_body_html_url", "")
        ndaa_year = row.get("ndaa_year", "")
        ndaa_section = row.get("ndaa_section", "")
        case_number = row.get("case_number", "")

        # Handle multi-URL rows (newline-separated)
        urls = [u.strip() for u in url_field.split("\n") if u.strip()]

        row_sections: list[str] = []
        row_has_data = False

        for url in urls:
            doc_id = _extract_doc_id_from_url(url)
            if not doc_id:
                continue

            html_file = html_dir / f"{doc_id}.html"
            if not html_file.exists():
                skipped.append(f"{doc_id} (row {idx})")
                continue

            sections = extract_dfars_sections(html_file)

            if not sections:
                skipped.append(f"{doc_id} (row {idx}, no sections)")
                continue

            row_has_data = True
            for s in sections:
                section_num = s["affected_dfars_section"]
                if section_num not in row_sections:
                    row_sections.append(section_num)
                all_details.append({
                    "ndaa_year": ndaa_year,
                    "ndaa_section": ndaa_section,
                    "case_number": case_number,
                    "document_id": doc_id,
                    "affected_dfars_section": section_num,
                    "amendment_instruction": s["amendment_instruction"],
                    "content": s["content"],
                })

        if row_has_data:
            processed_count += 1

        affected_sections_col.append(";".join(row_sections))

    # Add the new column to the dataframe
    df["affected_dfars_sections"] = affected_sections_col
    df.to_csv(output_path, index=False)
    print(f"\n✅ Wrote {output_path} with 'affected_dfars_sections' column.")

    # Write detail CSV
    with open(detail_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ndaa_year",
                "ndaa_section",
                "case_number",
                "document_id",
                "affected_dfars_section",
                "amendment_instruction",
                "content",
            ],
        )
        writer.writeheader()
        writer.writerows(all_details)

    print(f"✅ Wrote {len(all_details)} rows to {detail_path}")
    print(f"\n   Processed: {processed_count} / {len(df)} rows with data")
    if skipped:
        print(f"   Skipped {len(skipped)} document IDs:")
        for s in skipped:
            print(f"     - {s}")


if __name__ == "__main__":
    main()
