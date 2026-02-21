import requests
import pandas as pd
import time
from datetime import datetime


def parse_fr_date(date_str):
    """Parse a date string in MM/DD/YY or MM/DD/YYYY format to YYYY-MM-DD."""
    date_str = date_str.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def fetch_citation_by_date_and_page(citation, fr_date):
    """
    Looks up a single FR citation (e.g. '88 FR 73238') by searching the
    Federal Register API for DARS documents published on the given date,
    then matching by start_page.

    Returns a dict with document metadata on success, or None on failure.
    """
    # Parse citation into volume and page
    parts = citation.strip().split(" FR ")
    if len(parts) != 2:
        print(f"  Could not parse citation format: {citation}")
        return None

    page = parts[1].strip()

    # Parse date
    iso_date = parse_fr_date(fr_date)
    if not iso_date:
        print(f"  Could not parse date: {fr_date}")
        return None

    fields = [
        "title",
        "document_number",
        "citation",
        "start_page",
        "body_html_url",
        "html_url",
        "publication_date",
    ]

    try:
        # First try: search within DARS agency
        response = requests.get(
            "https://www.federalregister.gov/api/v1/documents.json",
            params={
                "conditions[agencies][]": "defense-acquisition-regulations-system",
                "conditions[publication_date][gte]": iso_date,
                "conditions[publication_date][lte]": iso_date,
                "fields[]": fields,
                "per_page": "100",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        for result in data.get("results", []):
            if str(result.get("start_page")) == page:
                return result

        # Fallback: search under broader DoD agency (handles joint DoD/GSA/NASA rules)
        response = requests.get(
            "https://www.federalregister.gov/api/v1/documents.json",
            params={
                "conditions[agencies][]": "defense-department",
                "conditions[publication_date][gte]": iso_date,
                "conditions[publication_date][lte]": iso_date,
                "fields[]": fields,
                "per_page": "100",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        for result in data.get("results", []):
            if str(result.get("start_page")) == page:
                return result

        print(f"  No page match for {citation} on {iso_date}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"  API error for {citation}: {e}")
        return None


if __name__ == "__main__":
    dfars_df = pd.read_csv("./data/ndaa_final_rule_citations.csv")

    citations_col = dfars_df["frn_citation"]
    dates_col = dfars_df["fr_date"]

    fr_titles = []
    fr_html_urls = []

    print(f"Starting lookup for {len(dfars_df)} rows...\n")

    for idx, (citation_cell, date_cell) in enumerate(zip(citations_col, dates_col)):
        # Skip empty rows
        if pd.isna(citation_cell) or str(citation_cell).strip() == "":
            fr_titles.append("")
            fr_html_urls.append("")
            continue

        citation_cell = str(citation_cell).strip()
        date_cell = str(date_cell).strip()

        # Split multi-value citations and dates (separated by \n)
        individual_citations = [c.strip() for c in citation_cell.split("\n") if c.strip()]
        individual_dates = [d.strip() for d in date_cell.split("\n") if d.strip()]

        # If there are fewer dates than citations, reuse the last date
        while len(individual_dates) < len(individual_citations):
            individual_dates.append(individual_dates[-1])

        row_titles = []
        row_urls = []

        for citation, date in zip(individual_citations, individual_dates):
            doc = fetch_citation_by_date_and_page(citation, date)
            if doc:
                title = doc.get("title", "")
                html_url = doc.get("body_html_url", "") or doc.get("html_url", "")
                row_titles.append(title)
                row_urls.append(html_url)
                print(f"[{idx+1}/{len(dfars_df)}] Found: {citation} -> {doc.get('document_number')}")
            else:
                row_titles.append("Not Found")
                row_urls.append("")
                print(f"[{idx+1}/{len(dfars_df)}] Could not find: {citation}")

            # Rate limit
            time.sleep(0.5)

        # Join multiple results with newline
        fr_titles.append("\n".join(row_titles))
        fr_html_urls.append("\n".join(row_urls))

    # Add columns and save
    dfars_df["fr_title"] = fr_titles
    dfars_df["fr_body_html_url"] = fr_html_urls

    dfars_df.to_csv("./data/ndaa_final_rule_with_rationale.csv", index=False)
    print(f"\nDone! Results saved to ndaa_final_rule_with_rationale.csv")