import argparse
import re
import requests
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm

INPUT_CSV = "./data/tracker.csv"
OUTPUT_CSV = "./data/fr_cases.csv"

def parse_fr_date(date_str):
    """Parse a date string in MM/DD/YY or MM/DD/YYYY format to YYYY-MM-DD."""
    date_str = date_str.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def fetch_citation_by_date_and_page(citation=None, fr_date=None):
    fields = [
        "document_number",
        "citation",
        "start_page",
        "body_html_url",
        "publication_date",
        "title",
        "cfr_references",
    ]

    params = {
        "conditions[agencies][]": ["defense-acquisition-regulations-system", "defense-department"],
        "conditions[type][]": "RULE",
        "conditions[cfr][title]": "48",
        "conditions[cfr][part]": "200-299",
        "conditions[search_type_id]": "6",
        "fields[]": fields,
        "per_page": "1000",
    }

    # Initialize variables to avoid UnboundLocalError
    page = None
    iso_date = None

    # Parse citation into volume and page
    if citation is not None:
        parts = citation.strip().split(" FR ")
        if len(parts) != 2:
            tqdm.write(f"  Could not parse citation format: {citation}")
        else:
            page = parts[1].strip()

    # Parse date
    if fr_date is not None:
        iso_date = parse_fr_date(fr_date)
        if not iso_date:
            tqdm.write(f"  Could not parse date: {fr_date}")
    
    if page is not None and iso_date is not None:
        params["conditions[publication_date][gte]"] = iso_date
        params["conditions[publication_date][lte]"] = iso_date
    else:
        params["conditions[term]"] = "NDAA"

    try:
        response = requests.get(
            "https://www.federalregister.gov/api/v1/documents.json",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if page is not None:
            for result in data.get("results", []):
                if str(result.get("start_page")) == page:
                    return result
        else:
            return data.get("results", [])

        tqdm.write(f"  No page match for {citation} on {iso_date}")
        return None

    except requests.exceptions.RequestException as e:
        tqdm.write(f"  API error for {citation}: {e}")
        return None


def main():
    fr_cases = {}

    # First: manual seach for FR cases with NDAA in the content
    # API is provided by FR
    docs = fetch_citation_by_date_and_page()
    for doc in tqdm(docs, desc="Processing fetched FR documents - manual search"):
        case = doc["document_number"]
        if case not in fr_cases:
            fr_cases[case] = doc

    # Second: search for FR cases based on tracker
    # Unique cases identified by citation and publication date
    tracker = pd.read_csv(INPUT_CSV)
    for idx, row in tqdm(tracker.iterrows(), total=len(tracker), desc="Fetching FR documents based on tracker"):
        if pd.isna(row.get("citation")) or pd.isna(row.get("publication_date")):
            continue

        citations = str(row["citation"]).split(";")
        dates = str(row["publication_date"]).split(";")

        for citation, date in zip(citations, dates):
            doc = fetch_citation_by_date_and_page(citation, date)
            if doc:
                case = doc["document_number"]
                if case not in fr_cases:
                    fr_cases[case] = doc

    if fr_cases:
        for case, doc in list(fr_cases.items()):
            title = doc.get("title", "")
            match = re.search(r'DFARS Case ([A-Za-z0-9-]+)', title, re.IGNORECASE)
            if match:
                doc["dfars_case"] = match.group(1).strip()
                doc["cfr_references"] = [cit["part"] for cit in doc["cfr_references"]]
            else:
                # delete the doc
                del fr_cases[case]
        
        columns = [
            "document_number",
            "dfars_case",
            "citation",
            "publication_date",
            "cfr_references",
            "title",
            "body_html_url",
        ]

        df_out = pd.DataFrame(list(fr_cases.values()))
        df_out = df_out[columns]
        df_out.to_csv(OUTPUT_CSV, index=False)
        print(f"\nSuccessfully fetched {len(fr_cases)} unique FR documents.")
        print(f"Saved to {OUTPUT_CSV}")
    else:
        print("\nNo documents fetched.")

if __name__ == "__main__":
    main()