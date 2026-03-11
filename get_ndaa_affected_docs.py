"""
Identify which documents each NDAA section modifies.

Reads doc_to_ndaa.csv, fetches the NDAA text for each unique citation
using data/ndaa/utils, sends it to OpenAI to extract the documents
being amended/modified, and writes the results to data/ndaa_affected_docs.csv.
"""

import ast
from pathlib import Path

import pandas as pd
from pydantic import BaseModel
from tqdm import tqdm

import data.ndaa.utils as ndaa_utils
from openai_utils import connect_to_openai, get_structured_response


DOC_TO_NDAA_CSV = Path("./data/doc_to_ndaa.csv")
OUTPUT_CSV      = Path("./data/ndaa_affected_docs.csv")


# ── Pydantic models for structured output ──────────────────────────

class AffectedDocument(BaseModel):
    """A single document that the NDAA section modifies."""
    document_type: str   # e.g. "U.S. Code", "DFARS", "FAR", "CFR", "Public Law", etc.
    document_id: str     # e.g. "10 U.S.C. 4872", "DFARS 252.225-7009", "48 CFR 225.7003"
    action: str          # what the NDAA section does: "amends", "adds", "repeals", "redesignates", etc.

class AffectedDocuments(BaseModel):
    """All documents modified by a given NDAA section."""
    affected_documents: list[AffectedDocument]


# ── System prompt ─────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert at reading U.S. legislative text from National Defense Authorization Acts (NDAA).

Given the text of a specific NDAA section, identify every document or legal provision that this section directly amends, adds, repeals, redesignates, or otherwise modifies.

Rules:
- Only include documents/provisions that the NDAA section **directly** modifies (e.g. "amends section X of title Y", "adds paragraph (z) to section X", "repeals section X").
- Include United States Code sections (e.g. "10 U.S.C. 2302"), DFARS parts/sections/clauses, FAR parts/sections/clauses, CFR titles/parts, and any other specific legal documents referenced as being changed.
- For each affected document, return:
    document_type : The type of document (e.g. "U.S. Code", "DFARS", "FAR", "CFR", "Public Law")
    document_id   : The specific identifier (e.g. "10 U.S.C. 4872", "DFARS 252.225-7009")
    action        : What modification is being made (e.g. "amends", "adds", "repeals", "redesignates", "strikes and inserts")
- Do NOT include documents that are merely referenced for context, cross-references, or definitions.
- Do NOT infer documents that are not explicitly mentioned as being modified.
- If no documents are being modified, return an empty list.
"""


# ── Helpers ────────────────────────────────────────────────────────

def get_text(citation: dict) -> str | None:
    """Fetch the plain text for a single NDAA citation.  Returns None on failure."""
    year    = int(citation["ndaa_year"])
    title   = citation.get("title", "")
    section = citation.get("section", "")
    subsection = citation.get("subsection", "")

    text_data = None
    try:
        if title:
            text_data = ndaa_utils.get_title_text(year, title)
        elif section:
            if subsection:
                sub = subsection[:3]
                text_data = ndaa_utils.get_subsection_text(year, section, sub)
            else:
                text_data = ndaa_utils.get_section_text(year, section)
    except ValueError:
        tqdm.write(f"  ⚠ Section/subsection not found: {citation}")
    except FileNotFoundError:
        tqdm.write(f"  ⚠ NDAA file missing for year {year}")

    if text_data is None:
        return None

    # text_data is a dict with "text" key (and possibly "sections" for titles)
    if isinstance(text_data.get("text"), str):
        return text_data["text"]
    # For title-level results, concatenate all section texts
    if "sections" in text_data:
        return " ".join(s["text"] for s in text_data["sections"] if s.get("text"))

    return None


def citation_key(citation: dict) -> tuple:
    """Unique key for deduplication."""
    return (
        citation.get("ndaa_year", ""),
        citation.get("title", ""),
        citation.get("subtitle", ""),
        citation.get("section", ""),
        citation.get("subsection", ""),
    )


# ── Main ───────────────────────────────────────────────────────────

def main() -> None:
    df = pd.read_csv(DOC_TO_NDAA_CSV, dtype=str)
    print(f"Loaded {len(df)} rows from {DOC_TO_NDAA_CSV}")

    # Build a map: unique citation → list of document_numbers
    citation_to_docs: dict[tuple, list[str]] = {}
    citation_lookup: dict[tuple, dict] = {}  # key → original citation dict

    for _, row in df.iterrows():
        doc_num = row["document_number"]
        raw = row["citations"]
        if pd.isna(raw) or raw.strip() in ("", "[]"):
            continue
        citations = ast.literal_eval(raw)
        for cit in citations:
            key = citation_key(cit)
            citation_to_docs.setdefault(key, []).append(doc_num)
            citation_lookup.setdefault(key, cit)

    unique_citations = list(citation_to_docs.keys())
    print(f"Found {len(unique_citations)} unique NDAA citations to process")

    client = connect_to_openai()
    results: list[dict] = []
    skipped = 0

    for key in tqdm(unique_citations, desc="Querying OpenAI"):
        cit = citation_lookup[key]

        # Fetch NDAA text
        ndaa_text = get_text(cit)
        if ndaa_text is None:
            skipped += 1
            continue

        # Build a human-readable label for context
        label_parts = [f"NDAA {cit['ndaa_year']}"]
        if cit.get("title"):
            label_parts.append(f"Title {cit['title']}")
        if cit.get("section"):
            label_parts.append(f"Section {cit['section']}")
        if cit.get("subsection"):
            label_parts.append(f"Subsection {cit['subsection']}")
        label = ", ".join(label_parts)

        user_content = f"### {label}\n\n{ndaa_text}"

        try:
            response = get_structured_response(
                client,
                system_prompt=SYSTEM_PROMPT,
                content=user_content,
                output_format=AffectedDocuments,
            )
        except Exception as e:
            tqdm.write(f"  ✗ OpenAI error for {label}: {e}")
            skipped += 1
            continue

        # Format affected docs as a readable list
        affected = [
            f"{d.action}: {d.document_type} {d.document_id}"
            for d in response.affected_documents
        ]

        doc_nums = citation_to_docs[key]

        results.append({
            "ndaa_year": cit.get("ndaa_year", ""),
            "section": cit.get("section", ""),
            "subsection": cit.get("subsection", ""),
            "document_numbers": "; ".join(doc_nums),
            "affected_documents": "; ".join(affected) if affected else "",
        })

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_CSV, index=False)

    print(f"\nDone! Processed {len(results)} unique NDAA citations.")
    print(f"Skipped {skipped} citations (missing text or API error).")
    print(f"CSV → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
