import os
import re
import ssl
import urllib.request
from urllib.parse import urljoin, quote

import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

INPUT_CSV = "data/case_desc.csv"
SAVE_DIR = "data/dfars_changes"
BASE_URL = "https://www.acq.osd.mil"
ARCHIVE_YEARS = range(2010, 2026)


def get_archive_htmls() -> dict[int, bytes]:
    """Fetch the change_notices HTML page for each year."""
    htmls = {}
    for year in tqdm(ARCHIVE_YEARS, desc="Fetching archive pages"):
        url = f"{BASE_URL}/dpap/dars/archive/{year}/change_notices.html"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
                htmls[year] = resp.read()
        except Exception as e:
            tqdm.write(f"  [WARN] Could not fetch {url}: {e}")
    return htmls


def extract_url_dict(htmls: dict[int, bytes]) -> dict[tuple[str, str], list[str]]:
    """Parse all archive HTMLs and build a dict keyed by (date, case_number) -> [urls].

    Actual URL pattern in the archive pages:
        /dpap/dars/dfars/changenotice/{year}/{YYYYMMDD}/{case}_(type)_DFARS_Text_LILO.docx
    e.g.
        /dpap/dars/dfars/changenotice/2024/20240926/2022-D013_(f)_DFARS_Text_LILO.docx

    Older years may use relative URLs like:
        ../../dfars/changenotice/2018/20180928/2018-D027 (f) DFARS Text LILO.docx

    The key is (date_str YYYYMMDD, case_number like '2022-D013').
    We exclude PGI-only files but keep DFARS text files.
    """
    # Matches the path segment:  .../changenotice/{year}/{YYYYMMDD}/{filename}.doc[x]
    # The case number is at the start of the filename: YYYY-DXXX
    path_re = re.compile(
        r"changenotice/\d{4}/(\d{8})/"  # group 1: YYYYMMDD date folder
        r"(\d{4}-D\d+)"                  # group 2: case number e.g. 2022-D013
        r"[^?#]*\.docx?",
        re.IGNORECASE,
    )

    url_dict: dict[tuple[str, str], list[str]] = {}

    for year, html in htmls.items():
        # Build a base URL so we can resolve relative hrefs for this year's page
        page_url = f"{BASE_URL}/dpap/dars/archive/{year}/change_notices.html"
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]

            # Skip PGI-only links
            if "PGI" in os.path.basename(href):
                continue

            # Resolve relative URLs against the page URL
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                full_url = BASE_URL + href
            else:
                full_url = urljoin(page_url, href)

            # Normalise to an absolute path for regex matching
            m = path_re.search(full_url)
            if not m:
                continue

            date_str = m.group(1)    # e.g. '20240926'
            case_number = m.group(2) # e.g. '2022-D013'

            key = (date_str, case_number)
            url_dict.setdefault(key, [])
            if full_url not in url_dict[key]:
                url_dict[key].append(full_url)

    tqdm.write(f"  Found {len(url_dict)} unique (date, case) entries across all archive pages.")
    return url_dict


def download_file(url: str, save_path: str) -> bool:
    """Download a file from url and save it to save_path."""
    try:
        # URL-encode just the path portion to handle spaces/parens in older filenames
        from urllib.parse import urlsplit, urlunsplit
        parts = urlsplit(url)
        safe_url = urlunsplit(parts._replace(path=quote(parts.path, safe="/:@!$&'()*+,;=")))
        req = urllib.request.Request(safe_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=60) as resp:
            with open(save_path, "wb") as f:
                f.write(resp.read())
        return True
    except Exception as e:
        tqdm.write(f"  [ERROR] Could not download {url}: {e}")
        return False


def main():
    os.makedirs(SAVE_DIR, exist_ok=True)

    # ── Step 1: Fetch all archive HTMLs ─────────────────────────────────────
    htmls = get_archive_htmls()

    # ── Step 2: Build (date, case_number) → [urls] dict ─────────────────────
    url_dict = extract_url_dict(htmls)

    # ── Step 3: Read case_desc.csv ───────────────────────────────────────────
    df = pd.read_csv(INPUT_CSV)

    # Normalise the date column to YYYYMMDD so it matches what we extract from URLs
    df["date_str"] = pd.to_datetime(df["publication_date"]).dt.strftime("%Y%m%d")

    success = 0
    failed = 0
    skipped = 0
    not_found = []

    # ── Step 4: For each case, find and download the doc file(s) ─────────────
    case_docs: dict[str, str] = {}  # dfars_case -> semicolon-joined filenames

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Downloading docs"):
        case = row["dfars_case"]
        date_str = row["date_str"]

        key = (date_str, case)
        urls = url_dict.get(key)

        if not urls:
            # Try searching all keys for this case number (date may differ)
            fallback = [
                u
                for (d, c), ul in url_dict.items()
                if c == case
                for u in ul
            ]
            if fallback:
                urls = fallback
                tqdm.write(f"  [INFO] Case {case}: date mismatch, using fallback URLs.")
            else:
                tqdm.write(f"  [MISS] No document found for case {case} (date {date_str})")
                not_found.append(case)
                continue

        downloaded_filenames = []
        for url in urls:
            filename = os.path.basename(url.split("?")[0])
            save_path = os.path.join(SAVE_DIR, filename)

            if os.path.exists(save_path):
                skipped += 1
                downloaded_filenames.append(filename)
                continue

            if download_file(url, save_path):
                success += 1
                downloaded_filenames.append(filename)
            else:
                failed += 1

        if downloaded_filenames:
            # change file names to docx
            # NOTE: this is a hack, change the file extensions to docx 
            # using doc2docx later
            downloaded_filenames = [
                fn.replace(".doc", ".docx")
                for fn in downloaded_filenames
            ]
            case_docs[case] = "; ".join(downloaded_filenames)

    # ── Step 5: Write results back to CSV ────────────────────────────────────
    df["dfars_changes_doc"] = df["dfars_case"].map(case_docs)
    output_path = "data/doc_to_dfars.csv"
    out_cols = ["document_number", "dfars_case", "publication_date", "dfars_changes_doc"]
    df_out = df[out_cols].dropna(subset=["dfars_changes_doc"])
    df_out.to_csv(output_path, index=False)

    tqdm.write(f"\nDone!")
    tqdm.write(f"  Downloaded : {success}")
    tqdm.write(f"  Skipped    : {skipped}  (already existed)")
    tqdm.write(f"  Failed     : {failed}")
    tqdm.write(f"  Not found  : {len(not_found)}  {not_found}")
    tqdm.write(f"  CSV saved to {output_path}")

if __name__ == "__main__":
    main()