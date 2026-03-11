"""
Extract NDAA Section/Title/Subtitle references from DFARS case background text.

Reads document_numbers from data/doc_to_dfars.csv, looks up the corresponding
'background' column in data/case_desc.csv, and feeds each background text to
an LLM to identify which NDAA Sections/Titles/Subtitles are being implemented.
Only captures what is explicitly mentioned in the text.
"""

import argparse
import csv
from pathlib import Path

import pandas as pd
from pydantic import BaseModel
from tqdm import tqdm

from openai_utils import connect_to_openai, get_structured_response


DOCS_PATH = Path("./data/doc_to_dfars.csv")
CASE_DESC_PATH    = Path("./data/case_desc.csv")
OUTPUT_CSV       = Path("./data/doc_to_ndaa.csv")


# ── Pydantic models for structured output ──────────────────────────

class NDAACitation(BaseModel):
    """A single NDAA citation explicitly mentioned in the text."""
    ndaa_year: str      # e.g. "2023"
    title: str          # e.g. "VIII"   (empty string if not mentioned)
    subtitle: str       # e.g. "A"   (empty string if not mentioned)
    section: str        # e.g. "802"  (empty string if not applicable)
    subsection: str     # e.g. "(a)"   (empty string if not mentioned)

    def to_dict(self):
        return {
            "ndaa_year": self.ndaa_year,
            "title": self.title,
            "subtitle": self.subtitle,
            "section": self.section,
            "subsection": self.subsection
        }

class NDAACitations(BaseModel):
    """All NDAA citations found in a piece of background text."""
    citations: list[NDAACitation]


# ── System prompt ─────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert at reading U.S. federal rulemaking documents.

Given the 'Background' section text from a Federal Register document, identify every NDAA (National Defense Authorization Act) Section, Title, Subtitle, and Subsection that this rule is specifically implementing.

Rules:
- Only capture NDAA references that are EXPLICITLY stated in the provided text.
- Only include references to provisions the rule is IMPLEMENTING or AMENDING regulations to comply with. Do NOT include NDAA items that are merely cited for context or background.
- For each citation, return:
    ndaa_year : The fiscal year of the NDAA (format: "YYYY", e.g. "2023").
    title     : The title if stated (format: "X", e.g. "VIII"). Use an empty string if not mentioned.
    subtitle  : The subtitle if stated (format: "X", e.g. "A"). Use an empty string if not mentioned.
    section   : The section number if stated (format: "XXXX", e.g. "802"). Use an empty string if no section is mentioned.
    subsection: The subsection if stated (format: "(X)", e.g. "(a)"). Use an empty string if not mentioned.
- Do NOT infer, guess, or add references beyond what is explicitly written.
- Do NOT include references to other statutes, executive orders, CFR parts, or Public Law numbers unless they are identified as an NDAA section/title/subtitle.
- If no NDAA is being implemented, return an empty list.

Example:

Input:
DoD published a proposed rule in the Federal Register at 88 FR 25609 on April 27, 2023, to implement section 844 of the National Defense Authorization Act (NDAA) for Fiscal Year (FY) 2021 (Pub. L. 116-283). Section 844 amends 10 U.S.C. 2533c (redesignated 10 U.S.C. 4872) and removes from the restriction “material melted” and replaces it with “material mined, refined, separated, melted”. In addition, the reference to “tungsten” is removed and replaced with “covered material” in the exception for commercially available-off-the-shelf (COTS) items to the restriction of 50 percent or more by weight. The final rule also implements section 854 of the NDAA for FY 2024 (Pub. L. 118-31) that amends the effective date in section 844(b) of the NDAA for FY 2021. Section 854 extends the effective date of the restriction from 5 years to 6 years. Nine respondents submitted public comments in response to the proposed rule.

Output:
[
    {
        "ndaa_year": "2021",
        "title": "",
        "subtitle": "",
        "section": "844",
        "subsection": ""
    },
    {
        "ndaa_year": "2024",
        "title": "",
        "subtitle": "",
        "section": "854",
        "subsection": ""
    }
]
"""


# ── Main ───────────────────────────────────────────────────────────

def main() -> None:
    # Load document numbers we care about
    doc_df = pd.read_csv(DOCS_PATH, dtype=str)
    document_numbers = doc_df["document_number"].dropna().unique().tolist()
    print(f"Found {len(document_numbers)} document numbers in doc_to_dfars.csv")

    # Load background text from case_desc.csv
    case_df = pd.read_csv(CASE_DESC_PATH, dtype=str)
    case_df.set_index("document_number", inplace=True)

    # Connect to LLM
    client = connect_to_openai()

    all_results: list[dict] = []
    skipped: list[str] = []

    for doc_num in tqdm(document_numbers, desc="Extracting NDAA citations"):
        # Look up background text
        if doc_num not in case_df.index:
            tqdm.write(f"No case_desc entry for document_number={doc_num!r}. Skipping.")
            skipped.append(doc_num)
            continue

        background = case_df.loc[doc_num, "background"]
        if pd.isna(background) or not str(background).strip():
            tqdm.write(f"Empty background for document_number={doc_num!r}. Skipping.")
            skipped.append(doc_num)
            continue

        try:
            response = get_structured_response(
                client,
                system_prompt=SYSTEM_PROMPT,
                content=background,
                output_format=NDAACitations,
            )

        except Exception as e:
            tqdm.write(f"Error processing {doc_num}: {e}")
            skipped.append(doc_num)
            continue
        
        citations = []
        for citation in response.citations:
            citations.append(citation.to_dict())

        all_results.append({
            "document_number": doc_num,
            "citations": citations
        })

    df = pd.DataFrame(all_results)
    df.to_csv(OUTPUT_CSV, index=False)

    processed = len(document_numbers) - len(skipped)
    print(
        f"\nDone! Extracted {len(all_results)} NDAA citations from "
        f"{processed} documents."
    )
    print(f"Skipped {len(skipped)} documents (no entry or empty background).")
    if skipped:
        print(f"Skipped IDs: {skipped}")
    print(f"CSV → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
