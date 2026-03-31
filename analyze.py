"""
Run LLM analysis on valid DFARS–NDAA pairs via OpenRouter Sonar Pro Search.
Reads pairs from ./data/valid_pairs.json (produced by get_mapping.py).
"""
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
from pydantic import BaseModel, Field
from tqdm import tqdm

# ─── Config ───────────────────────────────────────────────────────────────────
VALID_PAIRS_FILE = "./data/valid_pairs.json"
OUTPUT_FILE = "./data/llm_analysis_results.json"

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_MODEL = "perplexity/sonar-pro-search"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

MAX_WORKERS = 5  # parallel requests


# ─── Pydantic models for structured output ────────────────────────────────────
class Hop(BaseModel):
    node: str
    relation: str
    next: str


class NonImpactingNDAA(BaseModel):
    ndaa_year: int
    ndaa_section: str
    reasoning: str = Field(description="Brief explanation of why this NDAA section does not impact the DFARS section")


class ImpactChain(BaseModel):
    chain_id: int
    change_type: str
    direct_or_indirect: str
    ndaa_year: int
    ndaa_section: str
    confidence: str = Field(description="High, Medium, or Low")
    reasoning: str
    hops: list[Hop]
    list_of_changes: list[str]


class AnalysisResult(BaseModel):
    dfars_section: str
    affected: bool
    impact_chains: list[ImpactChain]
    non_impacting_ndaas: list[NonImpactingNDAA]


# ─── Helpers ──────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """\
You are a defense acquisition regulatory analyst specializing in the relationship \
between the National Defense Authorization Act (NDAA) and the Defense Federal \
Acquisition Regulation Supplement (DFARS).

You will be given:
1. A DFARS section (part, subpart, section number, and its current text).
2. One or more NDAA sections (year, section number, and full legislative text).

Your task:
- For each NDAA section, determine whether it impacts the given DFARS section.
- Search in depth: trace the chain of legal authority from the NDAA provision \
  through any intermediate statutes (US Code), FAR clauses, or other regulatory \
  references to show HOW the NDAA section affects the DFARS section.
- If an NDAA section does NOT impact the DFARS section, include it in the \
  "non_impacting_ndaas" list with a brief reasoning for why it does not apply.
- Every NDAA section provided must appear in EITHER "impact_chains" OR \
  "non_impacting_ndaas" — none should be omitted.
- If NONE of the NDAA sections impact the DFARS section, set "affected" to false \
  and return an empty impact_chains list (all NDAAs go in non_impacting_ndaas).

Be thorough and precise. Include the full reasoning chain with hops.\
"""


def build_user_prompt(dfars_section: dict, ndaa_sections: list[dict]) -> str:
    """Build the user message containing DFARS + NDAA data."""
    lines = [
        "## DFARS Section",
        f"- **Part**: {dfars_section['part']}",
        f"- **Subpart**: {dfars_section['subpart']}",
        f"- **Section**: {dfars_section['section']}",
        "",
        "### Current DFARS Text",
        dfars_section["before"],
        "",
        "---",
        "",
        "## NDAA Sections to Analyze",
    ]

    for i, ndaa in enumerate(ndaa_sections, 1):
        lines.extend([
            f"### NDAA Section {i}",
            f"- **Year**: {ndaa['year']}",
            f"- **Section**: {ndaa['section']}",
            "",
            ndaa.get("text", "(text unavailable)"),
            "",
        ])

    return "\n".join(lines)


def call_openrouter(system_prompt: str, user_prompt: str) -> dict:
    """Call OpenRouter API with structured output schema. Returns parsed JSON."""
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY is not set in the environment")

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    json_schema = {
        "name": "analysis_result",
        "strict": True,
        "schema": AnalysisResult.model_json_schema(),
    }

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": json_schema,
        },
    }

    resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    content = data["choices"][0]["message"]["content"]
    return json.loads(content)


def analyze_pair(idx: int, pair: dict) -> Optional[dict]:
    """Analyze a single pair dict. Returns result dict or None on error."""
    dfars_section = pair["dfars"]
    ndaa_sections = pair["ndaas"]
    user_prompt = build_user_prompt(dfars_section, ndaa_sections)

    try:
        result = call_openrouter(SYSTEM_PROMPT, user_prompt)
        result["document_number"] = dfars_section.get("document_number")
        return result
    except Exception as e:
        print(f"  [ERROR] Pair {idx}: {e}")
        return None


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    with open(VALID_PAIRS_FILE) as f:
        valid_pairs = json.load(f)

    print(f"Loaded {len(valid_pairs)} pairs from {VALID_PAIRS_FILE}")
    print(f"Using model: {OPENROUTER_MODEL}")
    print(f"Parallel workers: {MAX_WORKERS}\n")

    results = []
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(analyze_pair, i, pair): i
            for i, pair in enumerate(valid_pairs)
        }
        with tqdm(total=len(valid_pairs), desc="LLM analysis") as pbar:
            for future in as_completed(futures):
                idx = futures[future]
                result = future.result()
                if result is not None:
                    results.append(result)
                else:
                    errors += 1
                pbar.update(1)

    print(f"\nCompleted: {len(results)} successful, {errors} errors")

    # Save results
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
