"""
Evaluation pipeline for the NDAA ↔ DFARS agentic analysis framework.

Runs the agent pipeline on single-section cases from dfars_diffs.json,
then uses an LLM-as-a-judge to score the pipeline output against the
ground-truth "after" text.

Usage
-----
  # Run all single-section eval cases
  python eval.py

  # Limit to N cases
  python eval.py --limit 3

  # Custom output path
  python eval.py --output eval_results.json

  # Skip pipeline execution and only re-judge from a previous run
  python eval.py --rejudge eval_results.json
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

from graph import run_analysis, get_llm
from langchain_core.messages import HumanMessage, SystemMessage

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DIFFS_PATH = DATA_DIR / "dfars_diffs.json"
PAIRS_PATH = DATA_DIR / "valid_pairs.json"
DEFAULT_OUTPUT = DATA_DIR / "eval_results.json"


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------

def load_eval_cases() -> list[dict]:
    """Build evaluation cases from single-section diffs matched with NDAA pairs.

    Returns a list of dicts, each with:
      - case            : case ID (e.g. "2022-D014")
      - document_number : Federal Register doc number
      - dfars_section   : section identifier
      - dfars_before    : the "before" text (pipeline input)
      - dfars_after     : the "after" text (ground truth)
      - ndaa_texts      : list of formatted NDAA text strings
      - dfars_input     : formatted DFARS text for the pipeline (before only)
      - ndaa_input      : combined NDAA text for the pipeline
    """
    with open(DIFFS_PATH) as f:
        diffs = json.load(f)["results"]
    with open(PAIRS_PATH) as f:
        pairs = json.load(f)

    # Index pairs by (document_number, section)
    pair_index: dict[tuple[str, str], list[dict]] = {}
    for p in pairs:
        key = (p["dfars"]["document_number"], p["dfars"]["section"])
        pair_index.setdefault(key, []).append(p)

    # Also index by document_number alone (fallback)
    pair_by_doc: dict[str, list[dict]] = {}
    for p in pairs:
        doc = p["dfars"]["document_number"]
        pair_by_doc.setdefault(doc, []).append(p)

    eval_cases: list[dict] = []

    for entry in diffs:
        if len(entry["sections"]) != 1:
            continue

        sec = entry["sections"][0]
        doc_num = entry["document_number"]
        sec_name = sec["section"]

        # Must have both before and after, and they must differ
        if not sec["before"].strip() or not sec["after"].strip():
            continue
        if sec["before"] == sec["after"]:
            continue

        # Find matching NDAA pair(s)
        matched_pairs = pair_index.get((doc_num, sec_name), [])
        if not matched_pairs:
            # Fallback: match by document_number only
            matched_pairs = pair_by_doc.get(doc_num, [])
        if not matched_pairs:
            continue

        # Collect all unique NDAA texts across matched pairs
        seen_ndaas = set()
        ndaa_texts = []
        for p in matched_pairs:
            for ndaa in p.get("ndaas", []):
                ndaa_key = (ndaa.get("year"), ndaa.get("section"))
                if ndaa_key not in seen_ndaas:
                    seen_ndaas.add(ndaa_key)
                    ndaa_texts.append(
                        f"NDAA FY{ndaa.get('year', '?')} Section "
                        f"{ndaa.get('section', '?')}: "
                        f"{ndaa.get('title', '')}\n\n{ndaa.get('text', '')}"
                    )

        if not ndaa_texts:
            continue

        # Build pipeline inputs — DFARS uses only "before" text
        dfars_input_parts = [
            f"DFARS Section: {sec_name}",
            f"Part: {sec.get('part', '')}",
            f"Subpart: {sec.get('subpart', '')}",
            f"\nCurrent text:\n{sec['before']}",
        ]
        dfars_input = "\n".join(dfars_input_parts)
        ndaa_input = "\n\n---\n\n".join(ndaa_texts)

        eval_cases.append({
            "case": entry["case"],
            "document_number": doc_num,
            "dfars_section": sec_name,
            "dfars_before": sec["before"],
            "dfars_after": sec["after"],
            "ndaa_texts": ndaa_texts,
            "dfars_input": dfars_input,
            "ndaa_input": ndaa_input,
        })

    return eval_cases


# ---------------------------------------------------------------------------
# LLM-as-a-Judge
# ---------------------------------------------------------------------------

JUDGE_SYSTEM = """You are an expert evaluator for regulatory text drafting systems.
You compare a system's proposed DFARS revision against the official ground-truth
revision and score how well the system captured the required changes."""

JUDGE_PROMPT = """\
You are evaluating a system that reads an NDAA provision and the current DFARS
text, then proposes changes to the DFARS section.

Below you have:
1. The DFARS text BEFORE the change (the input the system received).
2. The official DFARS text AFTER the change (ground truth).
3. The system's proposed draft output.

Your job: determine how well the system's output captures the changes reflected
in the ground truth. The system output uses <added>/<removed> markup — focus on
the **substance** of the changes, not formatting or markup style.

---

DFARS BEFORE (system input):
\"\"\"
{before}
\"\"\"

DFARS AFTER (ground truth):
\"\"\"
{after}
\"\"\"

SYSTEM OUTPUT (proposed draft):
\"\"\"
{output}
\"\"\"

---

Evaluate on these dimensions and give a score for each (1-5 scale):

1. **Change Identification** (1-5): Did the system identify the correct changes
   that needed to be made? Compare the actual diff (before→after) with what the
   system proposed.

2. **Content Accuracy** (1-5): Is the substance of the system's proposed changes
   accurate and aligned with the ground truth? Are the right words, definitions,
   references, and requirements present?

3. **Completeness** (1-5): Did the system capture ALL changes from before→after,
   or did it miss some? Did it add unnecessary/hallucinated changes that aren't
   in the ground truth?

4. **Structural Fidelity** (1-5): Does the output preserve the regulatory
   structure (subsection numbering, paragraph organization, definitions placement)
   consistent with the ground truth?

Respond in **exactly** this format:

CHANGE_IDENTIFICATION: <score>
CONTENT_ACCURACY: <score>
COMPLETENESS: <score>
STRUCTURAL_FIDELITY: <score>
OVERALL: <average of the 4 scores, rounded to 1 decimal>
REASONING: <one paragraph explaining your evaluation>
"""


def judge_output(before: str, after: str, system_output: str) -> dict:
    """Use LLM-as-a-judge to evaluate pipeline output against ground truth."""
    llm = get_llm(temperature=0.0)

    prompt = JUDGE_PROMPT.format(
        before=before,
        after=after,
        output=system_output,
    )

    response = llm.invoke([
        SystemMessage(content=JUDGE_SYSTEM),
        HumanMessage(content=prompt),
    ])
    text = response.content.strip()

    # Parse scores
    scores = {}
    for key in ["CHANGE_IDENTIFICATION", "CONTENT_ACCURACY",
                "COMPLETENESS", "STRUCTURAL_FIDELITY", "OVERALL"]:
        match = re.search(rf"{key}:\s*(\d+\.?\d*)", text)
        if match:
            scores[key.lower()] = float(match.group(1))

    reasoning_match = re.search(r"REASONING:\s*(.*)", text, re.DOTALL)
    reasoning = reasoning_match.group(1).strip() if reasoning_match else text

    return {
        "scores": scores,
        "reasoning": reasoning,
        "raw_judge_output": text,
    }


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_eval(cases: list[dict], max_iter: int = 3) -> list[dict]:
    """Run the agent pipeline on each eval case and judge the output."""
    results = []
    total = len(cases)

    for idx, case in enumerate(cases, 1):
        label = f"[{idx}/{total}] {case['case']} — {case['dfars_section']}"
        print(f"\n{'='*72}")
        print(f"  {label}")
        print(f"{'='*72}")

        # --- Run pipeline ---
        print("  ▶ Running agent pipeline ...")
        try:
            pipeline_result = run_analysis(
                case["ndaa_input"],
                case["dfars_input"],
                max_iterations=max_iter,
            )
            draft = pipeline_result["draft_dfars_change"]
            pipeline_meta = {
                "is_related": pipeline_result["is_related"],
                "iterations": pipeline_result["iteration"],
                "relatedness_reasoning": pipeline_result["relatedness_reasoning"],
            }
        except Exception as exc:
            print(f"  ✗ Pipeline error: {exc}")
            draft = f"ERROR: {exc}"
            pipeline_meta = {"error": str(exc)}

        # --- Judge ---
        print("  ▶ Judging output ...")
        try:
            judgement = judge_output(
                case["dfars_before"],
                case["dfars_after"],
                draft,
            )
        except Exception as exc:
            print(f"  ✗ Judge error: {exc}")
            judgement = {"scores": {}, "reasoning": f"Judge error: {exc}",
                         "raw_judge_output": ""}

        overall = judgement["scores"].get("overall", "?")
        print(f"  → Overall score: {overall}")
        print(f"  → {judgement['reasoning'][:200]}")

        results.append({
            "case": case["case"],
            "document_number": case["document_number"],
            "dfars_section": case["dfars_section"],
            "pipeline": pipeline_meta,
            "draft_output": draft,
            "judgement": judgement,
        })

    return results


def rejudge(results: list[dict], cases_by_key: dict) -> list[dict]:
    """Re-run only the judge on previously collected pipeline outputs."""
    updated = []
    total = len(results)

    for idx, r in enumerate(results, 1):
        print(f"\n[{idx}/{total}] Re-judging {r['case']} — {r['dfars_section']}")

        key = (r["document_number"], r["dfars_section"])
        case = cases_by_key.get(key)
        if not case:
            print("  ✗ No matching eval case found, skipping")
            updated.append(r)
            continue

        try:
            judgement = judge_output(
                case["dfars_before"],
                case["dfars_after"],
                r["draft_output"],
            )
        except Exception as exc:
            print(f"  ✗ Judge error: {exc}")
            judgement = r.get("judgement", {})

        overall = judgement["scores"].get("overall", "?")
        print(f"  → Overall score: {overall}")

        r["judgement"] = judgement
        updated.append(r)

    return updated


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(results: list[dict]) -> None:
    """Print aggregate evaluation metrics."""
    print(f"\n{'='*72}")
    print("  EVALUATION SUMMARY")
    print(f"{'='*72}")
    print(f"  Cases evaluated: {len(results)}")

    score_keys = ["change_identification", "content_accuracy",
                  "completeness", "structural_fidelity", "overall"]

    for key in score_keys:
        values = [r["judgement"]["scores"].get(key)
                  for r in results if r["judgement"]["scores"].get(key) is not None]
        if values:
            avg = sum(values) / len(values)
            mn, mx = min(values), max(values)
            print(f"  {key:25s}  avg={avg:.2f}  min={mn:.1f}  max={mx:.1f}  n={len(values)}")

    # Per-case overview
    print(f"\n  {'Case':<15} {'Section':<45} {'Overall':>7}")
    print(f"  {'-'*15} {'-'*45} {'-'*7}")
    for r in results:
        score = r["judgement"]["scores"].get("overall", "?")
        sec = (r["dfars_section"] or "")[:45]
        print(f"  {r['case']:<15} {sec:<45} {score:>7}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate the NDAA↔DFARS agent pipeline")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of eval cases to run")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSON path (default: data/eval_results.json)")
    parser.add_argument("--max-iter", type=int, default=3,
                        help="Max research loop iterations for the pipeline")
    parser.add_argument("--rejudge", type=str, default=None,
                        help="Path to previous eval_results.json — re-run judge only")
    args = parser.parse_args()

    out_path = args.output or str(DEFAULT_OUTPUT)

    # Load eval dataset
    print("Loading evaluation cases ...")
    cases = load_eval_cases()
    print(f"  Found {len(cases)} single-section eval cases with NDAA pairs")

    if args.rejudge:
        # Re-judge mode
        with open(args.rejudge) as f:
            prev_results = json.load(f)
        cases_by_key = {(c["document_number"], c["dfars_section"]): c
                        for c in cases}
        results = rejudge(prev_results, cases_by_key)
    else:
        # Full eval mode
        if args.limit:
            cases = cases[:args.limit]
        print(f"  Running {len(cases)} case(s) ...")
        results = run_eval(cases, max_iter=args.max_iter)

    # Save
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to {out_path}")

    # Summary
    print_summary(results)


if __name__ == "__main__":
    main()
