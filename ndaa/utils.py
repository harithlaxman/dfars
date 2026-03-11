"""
NDAA utility functions for fetching section/title content and US Code citations.

Usage:
    from ndaa.utils import get_section_text, get_title_text

    section = get_section_text("831", 2025)
    title   = get_title_text("VIII", 2025)
"""

import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "jsons")


def _load_ndaa(year: int) -> dict:
    path = os.path.join(JSON_DIR, f"ndaa_{year}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No NDAA JSON found for year {year}: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _collect_text(node: dict) -> str:
    parts: list[str] = []
    if "text" in node:
        parts.append(node["text"])
    for child in node.get("children", []):
        child_text = _collect_text(child)
        if child_text:
            parts.append(child_text)
    return " ".join(parts)


def _collect_citations(node: dict) -> list[dict]:
    """Recursively collect all US Code citations from a node and its children."""
    cits: list[dict] = []
    if "citations" in node:
        cits.extend(node["citations"])
    for child in node.get("children", []):
        cits.extend(_collect_citations(child))
    return cits


def _find_node(root: dict, node_type: str, enum_value: str) -> dict | None:
    """Recursively search for the first node matching a given type and enum."""
    if root.get("type") == node_type and root.get("enum") == enum_value:
        return root
    for child in root.get("children", []):
        result = _find_node(child, node_type, enum_value)
        if result is not None:
            return result
    return None


def _find_all_sections(node: dict) -> list[dict]:
    """Recursively find all section nodes under a given node."""
    sections: list[dict] = []
    if node.get("type") == "section":
        sections.append(node)
    else:
        for child in node.get("children", []):
            sections.extend(_find_all_sections(child))
    return sections


def _node_to_dict(node: dict) -> dict:
    """Convert a raw section node into a clean result dict."""
    return {
        "section": node.get("enum", ""),
        "header": node.get("header", ""),
        "text": _collect_text(node),
        "citations": _collect_citations(node),
    }


# ─── Public API ───────────────────────────────────────────────────────────────

def get_section_text(year: int, section_number: str) -> dict:
    node = _find_node(_load_ndaa(year), "section", section_number)
    if node is None:
        raise ValueError(
            f"Section {section_number} not found in NDAA {year}."
        )
    return _node_to_dict(node)


def get_subsection_text(year: int, section_number: str, subsection_number: str) -> dict:
    node = _find_node(_load_ndaa(year), "section", section_number)
    if node is None:
        raise ValueError(
            f"Section {section_number} not found in NDAA {year}."
        )

    subsection_node = _find_node(node, "subsection", subsection_number)
    if subsection_node is None:
        raise ValueError(
            f"Subsection {subsection_number} not found in NDAA {year}."
        )
    return _node_to_dict(subsection_node)


def get_title_text(year: int, title_number: str) -> dict:
    node = _find_node(_load_ndaa(year), "title", title_number)
    if node is None:
        raise ValueError(
            f"Title {title_number} not found in NDAA {year}."
        )

    section_nodes = _find_all_sections(node)
    sections = [_node_to_dict(s) for s in section_nodes]
    all_citations: list[dict] = []
    for s in sections:
        all_citations.extend(s["citations"])

    return {
        "title": node.get("enum", ""),
        "header": node.get("header", ""),
        "sections": sections,
        "citations": all_citations,
    }
