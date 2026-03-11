#!/usr/bin/env python3
"""
NDAA Pipeline: scrape XML from govinfo.gov and convert to structured JSON.

Usage:
    python ndaa_pipeline.py scrape          # Download NDAA XMLs to ./xmls/
    python ndaa_pipeline.py convert         # Convert XMLs in ./xmls/ to JSON in ./jsons/
    python ndaa_pipeline.py all             # Scrape then convert (full pipeline)
"""

import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET

import requests
from tqdm import tqdm

# ─── Directories ──────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(__file__)
XML_DIR = os.path.join(BASE_DIR, "xmls")
JSON_DIR = os.path.join(BASE_DIR, "jsons")

# ─── Scraper constants ───────────────────────────────────────────────────────

NDAA_DATA = [
    {"year": 2025, "congress": 118, "bill": "hr5009"},
    {"year": 2024, "congress": 118, "bill": "hr2670"},
    {"year": 2023, "congress": 117, "bill": "hr7776"},
    {"year": 2022, "congress": 117, "bill": "s1605"},
    {"year": 2021, "congress": 116, "bill": "hr6395"},
    {"year": 2020, "congress": 116, "bill": "s1790"},
    {"year": 2019, "congress": 115, "bill": "hr5515"},
    {"year": 2018, "congress": 115, "bill": "hr2810"},
    {"year": 2017, "congress": 114, "bill": "s2943"},
    {"year": 2016, "congress": 114, "bill": "s1356"},
    {"year": 2015, "congress": 113, "bill": "hr3979"},
    {"year": 2014, "congress": 113, "bill": "hr3304"},
    {"year": 2013, "congress": 112, "bill": "hr4310"},
    {"year": 2012, "congress": 112, "bill": "hr1540"},
    {"year": 2011, "congress": 111, "bill": "hr6523"},
    {"year": 2010, "congress": 111, "bill": "hr2647"},
    {"year": 2009, "congress": 110, "bill": "hr5658"}, # does not work
    {"year": 2008, "congress": 110, "bill": "hr4986"}, # does not work
    {"year": 2007, "congress": 109, "bill": "hr5122"},
]

BILL_VERSIONS = ["enr", "eas"]

# ─── XML-to-JSON constants ───────────────────────────────────────────────────

# Structural elements that become nodes in the JSON tree
STRUCTURAL_TAGS = {
    "division", "title", "subtitle", "part", "subpart",
    "chapter", "subchapter", "section", "subsection",
    "paragraph", "subparagraph", "clause", "subclause", "item",
    "quoted-block",
}

# Elements to skip entirely (metadata/TOC extracted separately)
SKIP_TAGS = {"toc", "toc-entry", "toc-enum", "toc-quoted-entry",
             "multi-column-toc-entry"}

# Regex for US Code citations
US_CODE_PATTERN = re.compile(
    r'(?P<type>[Ss]ections?|[Cc]hapters?)'
    r'\s+'
    r'(?P<section>\d+\w*(?:\([^)]*\))*)'
    r'(?:\s+(?:through|and)\s+(?P<section_end>\d+\w*(?:\([^)]*\))*))?'
    r'\s+of\s+title\s+'
    r'(?P<title>\d+\w*)'
    r',?\s*United\s+States\s+Code'
)


# ─── Scraper ──────────────────────────────────────────────────────────────────

def _build_url(congress, bill, version):
    return (
        f"https://www.govinfo.gov/content/pkg/"
        f"BILLS-{congress}{bill}{version}/xml/"
        f"BILLS-{congress}{bill}{version}.xml"
    )


def download_ndaa_xml(year, congress, bill):
    """Download a single NDAA XML from govinfo.gov. Skips if already present."""
    output_path = os.path.join(XML_DIR, f"ndaa_{year}.xml")

    if os.path.exists(output_path):
        tqdm.write(f"[SKIP]  {year} — already downloaded")
        return True

    for version in BILL_VERSIONS:
        url = _build_url(congress, bill, version)
        # print(f"[GET]   {year} — trying {version}: {url}")
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200 and "<?xml" in response.text[:100]:
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(response.text)
                # print(f"[OK]    {year} — saved ({version}) to {output_path}")
                return True
            else:
                tqdm.write(f"[MISS]  {year} — {version} not available (HTTP {response.status_code})")
        except requests.RequestException as e:
            tqdm.write(f"[ERROR] {year} — {version}: {e}")
            return False
        time.sleep(1)
    tqdm.write(f"[FAIL]  {year} — no version found")
    return False


def scrape_all():
    """Download all NDAA XMLs."""
    os.makedirs(XML_DIR, exist_ok=True)
    success, fail = 0, 0
    for data in tqdm(NDAA_DATA, desc="Downloading NDAA XMLs"):
        ok = download_ndaa_xml(data["year"], data["congress"], data["bill"])
        if ok:
            success += 1
        else:
            fail += 1
        time.sleep(1)
    print(f"\nDownloaded NDAA XMLs: {success} downloaded, {fail} failed")


# ─── XML → JSON conversion helpers ───────────────────────────────────────────

def extract_citations(text):
    """Extract all US Code citations from a text string.

    Returns a list of dicts, each with:
      - type: 'section' or 'chapter'
      - section: the section/chapter number (e.g., '101(a)(16)', '907')
      - title: the USC title number (e.g., '10', '31')
      - section_end: (optional) end of range for 'through' citations
    """
    if not text:
        return []
    citations = []
    seen = set()
    for m in US_CODE_PATTERN.finditer(text):
        cite_type = m.group('type').lower().rstrip('s')
        section = m.group('section')
        title = m.group('title')
        section_end = m.group('section_end')
        key = (cite_type, section, title, section_end)
        if key not in seen:
            seen.add(key)
            cite = {
                'type': cite_type,
                'section': section,
                'title': title,
            }
            if section_end:
                cite['section_end'] = section_end
            citations.append(cite)
    return citations


def get_all_text(elem):
    """Recursively extract all text content from an element, flattening
    inline elements (quote, bold, term, external-xref, etc.) into plain text."""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        if child.tag in STRUCTURAL_TAGS or child.tag in SKIP_TAGS:
            if child.tail:
                parts.append(child.tail)
            continue
        if child.tag in ("enum", "header"):
            if child.tail:
                parts.append(child.tail)
            continue
        parts.append(get_all_text(child))
        if child.tail:
            parts.append(child.tail)
    text = "".join(parts).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def parse_node(elem):
    """Parse a structural XML element into a dict node."""
    node = {"type": elem.tag}

    elem_id = elem.get("id")
    if elem_id:
        node["id"] = elem_id

    enum_elem = elem.find("enum")
    if enum_elem is not None and enum_elem.text:
        node["enum"] = enum_elem.text.strip()
        node["enum"] = enum_elem.text.strip(".")

    header_elem = elem.find("header")
    if header_elem is not None:
        header_text = get_all_text(header_elem)
        if header_text:
            node["header"] = header_text

    text_parts = []
    for text_elem in elem.findall("text"):
        t = get_all_text(text_elem)
        if t:
            text_parts.append(t)

    direct_text = get_all_text(elem)
    if text_parts:
        node["text"] = " ".join(text_parts)
    elif direct_text and elem.tag == "quoted-block":
        node["text"] = direct_text

    all_text_for_citations = " ".join(
        filter(None, [node.get("text"), node.get("header")])
    )
    citations = extract_citations(all_text_for_citations)
    if citations:
        node["citations"] = citations

    children = []
    for child in elem:
        if child.tag in STRUCTURAL_TAGS:
            children.append(parse_node(child))
        elif child.tag == "text":
            for grandchild in child:
                if grandchild.tag in STRUCTURAL_TAGS:
                    children.append(parse_node(grandchild))

    if children:
        node["children"] = children

    return node


def extract_metadata(root, fiscal_year):
    """Extract metadata from the bill's dublinCore and form elements."""
    metadata = {"fiscal_year": fiscal_year}

    dc_ns = "http://purl.org/dc/elements/1.1/"
    dc_parent = root.find(".//dublinCore")
    if dc_parent is not None:
        for field in ["title", "publisher", "date", "format", "language", "rights"]:
            elem = dc_parent.find(f"{{{dc_ns}}}{field}")
            if elem is not None and elem.text:
                metadata[field] = elem.text.strip()

    form = root.find("form")
    if form is None:
        form = root.find("engrossed-amendment-form")
    if form is not None:
        for tag in ["congress", "session", "legis-num", "legis-type", "official-title"]:
            elem = form.find(tag)
            if elem is not None:
                text = get_all_text(elem)
                if text:
                    key = tag.replace("-", "_")
                    metadata[key] = text

    return metadata


def convert_xml_to_json(xml_path, fiscal_year):
    """Convert a single NDAA XML file to a JSON-friendly dict."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    metadata = extract_metadata(root, fiscal_year) # completely optional
    # metadata = {"fiscal_year": fiscal_year} use this if you don't want to extract metadata

    legis_body = root.find("legis-body")
    if legis_body is None:
        legis_body = root.find(".//legis-body")
    children = []
    if legis_body is not None:
        for child in legis_body:
            if child.tag in STRUCTURAL_TAGS:
                children.append(parse_node(child))

    return {
        "metadata": metadata,
        "children": children,
    }


def convert_all():
    """Convert all XML files in xmls/ to JSON in jsons/."""
    os.makedirs(JSON_DIR, exist_ok=True)

    xml_files = sorted(f for f in os.listdir(XML_DIR) if f.endswith(".xml"))
    print(f"Found {len(xml_files)} XML files to convert")

    for xml_file in tqdm(xml_files, desc="Converting XML to JSON"):
        fiscal_year = int(xml_file[5:9]) # file format is ndaa_YYYY.xml

        xml_path = os.path.join(XML_DIR, xml_file)
        json_file = xml_file.replace(".xml", ".json")
        json_path = os.path.join(JSON_DIR, json_file)

        try:
            data = convert_xml_to_json(xml_path, fiscal_year)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            tqdm.write(f"ERROR: {e}")

    print(f"\nDone! JSON files written to {JSON_DIR}/")


if __name__ == "__main__":
    scrape_all()
    convert_all()
