import requests
import pandas as pd
import os
import re
import csv
import time
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from tqdm import tqdm


DOWNLOAD_DIR = "./data/fr_case_htmls"
INPUT_CSV = "./data/fr_cases.csv"
OUTPUT_CSV = "./data/case_desc.csv"

# Mapping of column name -> list of heading keyword patterns to match
SUPP_SECTIONS = {
    "background": ["background"],
    "discussion": ["discussion and analysis", "discussion of public comments"],
    "applicability": ["applicability to contracts"],
    "expected_impact": ["expected impact", "expected cost savings"],
    "publication_not_required": ["publication of this final rule"],
    "executive_orders": ["executive orders 12866"],
    "congressional_review": ["congressional review act"],
    "regulatory_flexibility": ["regulatory flexibility act"],
    "paperwork_reduction": ["paperwork reduction act"],
}


def download_html(url, filepath):
    """Download HTML content from a URL and save to filepath."""
    try:
        response = requests.get(url.strip(), timeout=30)
        response.raise_for_status()
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)
        return True
    except Exception as e:
        tqdm.write(f"  Error downloading {url}: {e}")
        return False


def url_to_filename(url):
    """Extract a filename from the URL, e.g. '2024-13863.html'."""
    path = urlparse(url.strip()).path
    # URL looks like: .../full_text/html/2024/06/27/2024-13863.html
    basename = os.path.basename(path)
    return basename


def _get_div_text(soup, div_id):
    """Extract cleaned text from a preamble div by its id."""
    div = soup.find("div", id=div_id)
    if not div:
        return ""
    # Get only the paragraph text, skip the heading
    paragraphs = div.find_all("p")
    return " ".join(p.get_text(strip=True) for p in paragraphs)


def _classify_heading(heading_text):
    """Return the column name for a supplementary info heading, or None."""
    lower = heading_text.lower()
    # Strip leading roman numerals like "I. ", "II. "
    lower = re.sub(r"^[ivxlc]+\.\s*", "", lower)
    lower = re.sub(r"^[a-z]\.\s*", "", lower)
    for col, patterns in SUPP_SECTIONS.items():
        for pattern in patterns:
            if lower.startswith(pattern):
                return col
    return None


def _extract_section_text(h2_tag):
    """Extract all text between this h2 and the next h2 (or end of parent)."""
    parts = []
    for sibling in h2_tag.next_siblings:
        if sibling.name == "h2":
            break
        # Stop at list-of-subjects or signature blocks
        if hasattr(sibling, "get") and sibling.get("class"):
            classes = sibling.get("class", [])
            if "list-of-subjects" in classes or "signature" in classes:
                break
        if hasattr(sibling, "get_text"):
            text = sibling.get_text(strip=True)
            if text:
                parts.append(text)
    return " ".join(parts)


def parse_fr_html(filepath):
    """Parse an FR HTML file and return a dict of extracted fields."""
    with open(filepath, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    doc_number = os.path.splitext(os.path.basename(filepath))[0]

    # --- Preamble fields ---
    action = _get_div_text(soup, "action")
    summary = _get_div_text(soup, "summary")
    dates = _get_div_text(soup, "dates")

    # List of subjects
    list_of_subjects = ""
    los_div = soup.find("div", class_="list-of-subjects")
    if los_div:
        ul = los_div.find("ul")
        if ul:
            list_of_subjects = "; ".join(
                li.get_text(strip=True) for li in ul.find_all("li")
            )

    # --- Supplementary info sub-sections ---
    supp_data = {col: "" for col in SUPP_SECTIONS}
    supp_data["other_supplementary"] = ""

    supp_div = soup.find("div", class_="supplemental-info")
    if supp_div:
        other_parts = []
        for h2 in supp_div.find_all("h2"):
            heading_text = h2.get_text(strip=True)
            col = _classify_heading(heading_text)
            section_text = _extract_section_text(h2)

            if col:
                # Append in case there are multiple matches (unlikely but safe)
                if supp_data[col]:
                    supp_data[col] += " " + section_text
                else:
                    supp_data[col] = section_text
            else:
                if section_text:
                    other_parts.append(f"[{heading_text}] {section_text}")

        supp_data["other_supplementary"] = " ".join(other_parts)

    return {
        "document_number": doc_number,
        "action": action,
        "dates": dates,
        "summary": summary,
        **supp_data,
    }

def download_fr_docs(urls, save_dir):
    seen_urls = set()
    download_list = []  # (url, filepath)
    os.makedirs(save_dir, exist_ok=True)

    for url in urls:
        if url in seen_urls:
            continue
        seen_urls.add(url)
        filename = url_to_filename(url)
        filepath = os.path.join(save_dir, filename)
        download_list.append((url, filepath))

    success = []
    failed_urls = []

    for url, filepath in tqdm(download_list, desc="Downloading HTMLs"):
        # Skip if already downloaded
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            success.append(filepath)
            continue

        if download_html(url, filepath):
            success.append(filepath)
        else:
            failed_urls.append(url)

        time.sleep(0.5)

    return success, failed_urls

def extract_fr_docs(html_files):
    print(f"\nExtracting fields from {len(html_files)} HTML files...")

    rows = []
    parse_errors = []

    for html_file in tqdm(html_files, desc="Parsing HTMLs"):
        try:
            rows.append(parse_fr_html(html_file))
        except Exception as e:
            parse_errors.append((html_file, str(e)))

    return rows, parse_errors
    

def main():
    df = pd.read_csv(INPUT_CSV)
    
    # Download HTML from the urls in the CSV file.
    success, failed_urls = download_fr_docs(df["body_html_url"].to_list(), DOWNLOAD_DIR)
    print(f"\nDone! {len(success)} downloaded, {len(failed_urls)} failed.")
    if failed_urls:
        print(f"\nFailed URLs:")
        for url in failed_urls:
            print(f"  {url}")

    # Extract fields from all downloaded HTMLs into CSV
    df.set_index("document_number", inplace=True)
    
    rows, parse_errors = extract_fr_docs(success)
    for row in rows:
        doc_num = row["document_number"]
        # row["dfars_case"] = df.loc[doc_num, "dfars_case"]
        # row["publication_date"] = df.loc[doc_num, "publication_date"]
        
        # add columns to df
        df.loc[doc_num, "action"] = row["action"]
        df.loc[doc_num, "summary"] = row["summary"]
        for col in SUPP_SECTIONS:
            df.loc[doc_num, col] = row[col]

    df.to_csv(OUTPUT_CSV, index=False)

    if parse_errors:
        print(f"\n{len(parse_errors)} file(s) had parse errors:")
        for fname, err in parse_errors:
            print(f"  {fname}: {err}")

if __name__ == "__main__":
    main()
