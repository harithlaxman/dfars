import requests
import pandas as pd
import os
import time
from urllib.parse import urlparse


OUTPUT_DIR = "./data/fr_cases/html_from_tracker"


def download_html(url, filepath):
    """Download HTML content from a URL and save to filepath."""
    try:
        response = requests.get(url.strip(), timeout=30)
        response.raise_for_status()
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)
        return True
    except Exception as e:
        print(f"  Error downloading {url}: {e}")
        return False


def url_to_filename(url):
    """Extract a filename from the URL, e.g. '2024-13863.html'."""
    path = urlparse(url.strip()).path
    # URL looks like: .../full_text/html/2024/06/27/2024-13863.html
    basename = os.path.basename(path)
    return basename


if __name__ == "__main__":
    df = pd.read_csv("./data/ndaa_final_rule_with_rationale.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Collect all unique URLs to download
    seen_urls = set()
    download_list = []  # (url, filepath)

    for idx, row in df.iterrows():
        url_cell = row.get("fr_body_html_url", "")
        if pd.isna(url_cell) or str(url_cell).strip() == "":
            continue

        urls = [u.strip() for u in str(url_cell).split("\n") if u.strip()]
        for url in urls:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            filename = url_to_filename(url)
            filepath = os.path.join(OUTPUT_DIR, filename)
            download_list.append((url, filepath))

    print(f"Downloading {len(download_list)} unique HTML files to {OUTPUT_DIR}/\n")

    success = 0
    failed = 0
    for i, (url, filepath) in enumerate(download_list, 1):
        # Skip if already downloaded
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            print(f"[{i}/{len(download_list)}] Already exists: {os.path.basename(filepath)}")
            success += 1
            continue

        print(f"[{i}/{len(download_list)}] Downloading: {os.path.basename(filepath)}")
        if download_html(url, filepath):
            success += 1
        else:
            failed += 1

        time.sleep(0.5)

    print(f"\nDone! {success} downloaded, {failed} failed.")
