#!/usr/bin/env python3
"""
Add NDAA section text to ndaa_final_rule_dfars_changes.csv.

For each unique (ndaa_year, ndaa_section) pair in the CSV, looks up the
corresponding section text from the NDAA JSON files and adds it as a new
'ndaa_text' column.

Usage:
    python add_ndaa_text.py
"""

import csv
import re
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "ndaa"))
from ndaa_utils import load_ndaa, find_sections, get_section_text_flat

INPUT_CSV = os.path.join(os.path.dirname(__file__), "data", "ndaa_final_rule_dfars_changes.csv")
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "data", "ndaa_final_rule_dfars_changes.csv")


def parse_fy(fy_str):
    """Convert 'FY24' to 2024, 'FY10' to 2010, etc."""
    m = re.match(r"FY(\d+)", fy_str)
    if not m:
        return None
    num = int(m.group(1))
    if num < 100:
        return 2000 + num
    return num


def parse_section_enum(section_str):
    """
    Convert ndaa_section like '2881' or '831(a)(2)' to an enum like '2881.' or '831.'.
    The NDAA JSON uses '2881.' as the enum for section 2881.
    We strip any subsection references and add a trailing dot.
    """
    # Extract just the base section number (digits before any parenthetical)
    m = re.match(r"(\d+)", section_str)
    if not m:
        return None
    return m.group(1) + "."


def lookup_ndaa_text(ndaa_cache, fy_str, section_str):
    """Look up the NDAA section text, using a cache for loaded NDAAs."""
    fy = parse_fy(fy_str)
    if fy is None:
        return f"[Error: could not parse FY from '{fy_str}']"

    # Load NDAA JSON (cached)
    if fy not in ndaa_cache:
        try:
            ndaa_cache[fy] = load_ndaa(fy)
        except FileNotFoundError:
            ndaa_cache[fy] = None
            return f"[Error: NDAA JSON not found for FY{fy}]"

    ndaa = ndaa_cache[fy]
    if ndaa is None:
        return f"[Error: NDAA JSON not found for FY{fy}]"

    enum = parse_section_enum(section_str)
    if enum is None:
        return f"[Error: could not parse section from '{section_str}']"

    sections = find_sections(ndaa, enum=enum)
    if not sections:
        return f"[Not found: FY{fy} section {section_str}]"

    # Use the first match
    return get_section_text_flat(sections[0])


def main():
    # Read input CSV
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Read {len(rows)} rows from {INPUT_CSV}")

    # Build lookup cache: (ndaa_year, ndaa_section) -> ndaa_text
    ndaa_cache = {}  # fiscal_year -> loaded ndaa json
    text_cache = {}  # (fy_str, section_str) -> text

    unique_pairs = set()
    for row in rows:
        unique_pairs.add((row["ndaa_year"], row["ndaa_section"]))

    print(f"Looking up {len(unique_pairs)} unique (ndaa_year, ndaa_section) pairs...")

    found = 0
    not_found = 0
    for fy_str, section_str in sorted(unique_pairs):
        text = lookup_ndaa_text(ndaa_cache, fy_str, section_str)
        text_cache[(fy_str, section_str)] = text
        if text.startswith("["):
            not_found += 1
            print(f"  MISS: {fy_str} section {section_str} -> {text[:80]}")
        else:
            found += 1

    print(f"\nResults: {found} found, {not_found} not found out of {len(unique_pairs)} unique pairs")

    # Add ndaa_text column
    output_fieldnames = list(fieldnames) + ["ndaa_text"]

    for row in rows:
        key = (row["ndaa_year"], row["ndaa_section"])
        row["ndaa_text"] = text_cache.get(key, "[Error: lookup failed]")

    # Write output CSV
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
