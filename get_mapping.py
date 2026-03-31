"""
Get 1:1 mapping of NDAA to DFARS sections and save valid pairs as JSON.
"""
import ast
import json
from difflib import SequenceMatcher

import pandas as pd
from tqdm import tqdm

from ndaa import utils as ndaa_utils

# ─── Config ───────────────────────────────────────────────────────────────────
DIFFS_JSON = "./data/dfars_diffs.json"
DOC_TO_NDAA = "./data/doc_to_ndaa.csv"
VALID_PAIRS_FILE = "./data/valid_pairs.json"


# ─── Helpers ──────────────────────────────────────────────────────────────────
def diff_score(text1: str, text2: str) -> float:
    """Character-level similarity ratio between two strings (0.0–1.0)."""
    similarity = SequenceMatcher(None, text1, text2).ratio()
    return round(similarity, 4)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    ndaa_doc_df = pd.read_csv(DOC_TO_NDAA)
    ndaa_doc_df.set_index("document_number", inplace=True)

    pairs = []
    diffs = None

    with open(DIFFS_JSON) as f:
        file = json.loads(f.read())
        diffs = file["results"]

    if diffs is None:
        print("No diffs to process")
        return

    for diff in tqdm(diffs, desc="getting valid dfars"):
        doc_no = diff["document_number"]
        ndaas = ast.literal_eval(ndaa_doc_df["citations"][doc_no])
        ndaas = [{"year": ndaa["ndaa_year"], "section": ndaa["section"]} for ndaa in ndaas]
        if len(ndaas) != 0:
            valid_dfars = []
            for section in diff["sections"]:
                if section["before"] != section["after"] and len(section["before"]) > 10:
                    if diff_score(section["before"], section["after"]) < 0.8:
                        section["document_number"] = doc_no
                        valid_dfars.append(section)

            pairs.extend([(dfars, ndaas) for dfars in valid_dfars])

    # Filter out invalid NDAAs
    valid_pairs = []
    for pair in tqdm(pairs, desc="filtering valid ndaas"):
        for ndaa in list(pair[1]):
            section_text = None
            try:
                section_text = ndaa_utils.get_section_text(ndaa["year"], ndaa["section"])
            except:
                pair[1].remove(ndaa)
                continue
            if section_text is None:
                pair[1].remove(ndaa)
            else:
                ndaa["title"] = section_text["header"]
                ndaa["text"] = section_text["text"]
        if pair[1]:  # only keep pairs that still have at least one valid NDAA
            valid_pairs.append(pair)

    print(f"\nTotal valid pairs: {len(valid_pairs)}")

    # Save valid pairs for agent pipelines
    pairs_export = [
        {
            "dfars": pair[0],
            "ndaas": pair[1],
        }
        for pair in valid_pairs
    ]
    with open(VALID_PAIRS_FILE, "w") as f:
        json.dump(pairs_export, f, indent=2)
    print(f"Saved {len(pairs_export)} pairs to {VALID_PAIRS_FILE}")


if __name__ == "__main__":
    main()