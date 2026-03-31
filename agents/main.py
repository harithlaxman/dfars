"""
CLI runner for the NDAA ↔ DFARS analysis framework.

Usage
-----
  # Batch mode – read from valid_pairs.json (1:N flattened to 1:1)
  python main.py --pairs-file ../data/valid_pairs.json --limit 5

  # With inline text
  python main.py --ndaa "Section 845 of Pub.L. 117-263 ..." \
                 --dfars "DFARS 252.204-7012 ..."

  # With files
  python main.py --ndaa-file ndaa_sec845.txt --dfars-file dfars_204_7012.txt

  # Demo mode (uses built-in sample texts)
  python main.py --demo
"""

import argparse
import json
import os
import textwrap
from graph import run_analysis

# ---------------------------------------------------------------------------
# Sample texts for quick demo
# ---------------------------------------------------------------------------

SAMPLE_NDAA = textwrap.dedent("""\
SEC. 1505. CYBERSECURITY OF CONTRACTOR INFORMATION SYSTEMS.

(a) IN GENERAL.—Not later than 180 days after the date of the
enactment of this Act, the Secretary of Defense shall revise the
Defense Federal Acquisition Regulation Supplement to require
contractors and subcontractors that process, store, or transmit
controlled unclassified information on contractor information
systems to implement security requirements specified in NIST
Special Publication 800-171.

(b) ASSESSMENT REQUIREMENTS.—The revised regulation shall
require third-party assessments of contractor compliance with the
security requirements described in subsection (a), consistent with
the Cybersecurity Maturity Model Certification program.

(c) REPORTING.—Contractors shall report cyber incidents to the
Department of Defense within 72 hours of discovery, as specified
in regulations prescribed under section 391 of title 10, United
States Code.
""")

SAMPLE_DFARS = textwrap.dedent("""\
DFARS 252.204-7012 — Safeguarding Covered Defense Information and
Cyber Incident Reporting.

(a) Definitions. As used in this clause—
  "Covered defense information" means unclassified controlled
   technical information or other information as described in the
   Controlled Unclassified Information (CUI) Registry.
  "Cyber incident" means actions taken through the use of
   computer networks that result in a compromise or an actual or
   potentially adverse effect on an information system.

(b) Adequate security. The Contractor shall provide adequate
security on all covered contractor information systems. To provide
adequate security, the Contractor shall implement NIST SP 800-171,
as in effect at the time the solicitation is issued.

(c) Cyber incident reporting. When the Contractor discovers a
cyber incident that affects a covered contractor information system
or covered defense information, the Contractor shall report the
incident to DoD within 72 hours.

(d) Subcontracts. The Contractor shall include the substance of
this clause in all subcontracts, including subcontracts for
commercial products and commercial services.
""")


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

def print_banner(title: str) -> None:
    width = 72
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def print_section(label: str, content: str) -> None:
    print(f"\n--- {label} ---")
    print(content)


# ---------------------------------------------------------------------------
# Batch helpers – flatten 1:N into 1:1 pairs
# ---------------------------------------------------------------------------

def flatten_pairs(raw_pairs: list[dict]) -> list[dict]:
    """Flatten 1:N DFARS→NDAA entries into a list of 1:1 pair dicts.

    Each returned dict has:
      - dfars_section  (str)   – the DFARS section identifier
      - dfars_text     (str)   – combined DFARS text for analysis
      - ndaa_year      (str)   – NDAA fiscal year
      - ndaa_section   (str)   – NDAA section number
      - ndaa_title     (str)   – NDAA section title
      - ndaa_text      (str)   – NDAA text for analysis
      - document_number (str)  – Federal Register doc number
    """
    flat: list[dict] = []
    for entry in raw_pairs:
        dfars = entry["dfars"]
        dfars_label = dfars.get("section", "Unknown DFARS section")
        before = dfars.get("before", "")
        after = dfars.get("after", "")
        doc_num = dfars.get("document_number", "")

        # Build a descriptive DFARS text block
        dfars_text_parts = [
            f"DFARS Section: {dfars_label}",
            f"Part: {dfars.get('part', '')}",
            f"Subpart: {dfars.get('subpart', '')}",
        ]
        if before:
            dfars_text_parts.append(f"\nText BEFORE change:\n{before}")
        if after:
            dfars_text_parts.append(f"\nText AFTER change:\n{after}")

        dfars_text = "\n".join(dfars_text_parts)

        for ndaa in entry.get("ndaas", []):
            ndaa_text = (
                f"NDAA FY{ndaa.get('year', '?')} Section {ndaa.get('section', '?')}: "
                f"{ndaa.get('title', '')}\n\n{ndaa.get('text', '')}"
            )
            flat.append({
                "dfars_section": dfars_label,
                "dfars_text": dfars_text,
                "ndaa_year": ndaa.get("year", ""),
                "ndaa_section": ndaa.get("section", ""),
                "ndaa_title": ndaa.get("title", ""),
                "ndaa_text": ndaa_text,
                "document_number": doc_num,
            })
    return flat


def run_single_analysis(ndaa_text: str, dfars_text: str, max_iter: int) -> dict:
    """Run analysis and return a simplified results dict."""
    result = run_analysis(ndaa_text, dfars_text, max_iterations=max_iter)
    return {
        "is_related": result["is_related"],
        "relatedness_reasoning": result["relatedness_reasoning"],
        "iterations": result["iteration"],
        "searched_citations": result.get("searched_citations", []),
        "draft_dfars_change": result["draft_dfars_change"],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="NDAA ↔ DFARS agentic analysis")

    # Batch mode
    parser.add_argument("--pairs-file", type=str,
                        help="Path to valid_pairs.json (batch mode)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of 1:1 pairs to process (batch mode)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSON path for batch results "
                             "(default: ../data/analysis_results.json)")

    # Single-pair mode
    parser.add_argument("--ndaa", type=str, help="NDAA section text (inline)")
    parser.add_argument("--dfars", type=str, help="DFARS section text (inline)")
    parser.add_argument("--ndaa-file", type=str, help="Path to NDAA section text file")
    parser.add_argument("--dfars-file", type=str, help="Path to DFARS section text file")
    parser.add_argument("--demo", action="store_true", help="Run with built-in sample texts")

    # Shared
    parser.add_argument("--max-iter", type=int, default=3,
                        help="Max research loop iterations")
    args = parser.parse_args()

    # ── Batch mode: --pairs-file ──────────────────────────────────────────
    if args.pairs_file:
        with open(args.pairs_file) as f:
            raw_pairs = json.load(f)

        pairs = flatten_pairs(raw_pairs)
        if args.limit:
            pairs = pairs[: args.limit]

        total = len(pairs)
        print_banner(f"Batch Analysis – {total} pair(s)")

        results: list[dict] = []
        for idx, pair in enumerate(pairs, 1):
            label = (f"[{idx}/{total}] DFARS {pair['dfars_section']}  ↔  "
                     f"NDAA FY{pair['ndaa_year']} §{pair['ndaa_section']}")
            print(f"\n▶ {label}")

            try:
                analysis = run_single_analysis(
                    pair["ndaa_text"], pair["dfars_text"], args.max_iter
                )
            except Exception as exc:
                print(f"  ✗ Error: {exc}")
                analysis = {"error": str(exc)}

            results.append({
                "dfars_section": pair["dfars_section"],
                "ndaa_year": pair["ndaa_year"],
                "ndaa_section": pair["ndaa_section"],
                "ndaa_title": pair["ndaa_title"],
                "document_number": pair["document_number"],
                **analysis,
            })

            status = analysis.get("is_related", "?")
            print(f"  → Related: {status}")

        # Save results
        out_path = args.output or os.path.join(
            os.path.dirname(args.pairs_file), "analysis_results.json"
        )
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print_banner(f"Done – {len(results)} results saved to {out_path}")
        return

    # ── Single-pair modes ─────────────────────────────────────────────────
    if args.demo:
        ndaa_text, dfars_text = SAMPLE_NDAA, SAMPLE_DFARS
    elif args.ndaa_file and args.dfars_file:
        with open(args.ndaa_file) as f:
            ndaa_text = f.read()
        with open(args.dfars_file) as f:
            dfars_text = f.read()
    elif args.ndaa and args.dfars:
        ndaa_text, dfars_text = args.ndaa, args.dfars
    else:
        parser.error(
            "Provide --pairs-file, --ndaa/--dfars, "
            "--ndaa-file/--dfars-file, or --demo"
        )
        return

    print_banner("NDAA ↔ DFARS Agentic Analysis Framework")
    print_section("NDAA Input (first 300 chars)", ndaa_text[:300] + "…")
    print_section("DFARS Input (first 300 chars)", dfars_text[:300] + "…")

    print("\n▶ Running analysis graph …\n")
    result = run_analysis(ndaa_text, dfars_text, max_iterations=args.max_iter)

    print_banner("Results")
    print_section("Related?", "YES" if result["is_related"] else "NO")
    print_section("Reasoning", result["relatedness_reasoning"])
    print_section("Iterations used", str(result["iteration"]))

    if result.get("searched_citations"):
        print_section("Citations researched",
                      "\n".join(result["searched_citations"]))

    if result.get("research_context"):
        print_section("Research context gathered",
                      "\n\n".join(result["research_context"][:5]))

    print_banner("Proposed DFARS Change")
    print(result["draft_dfars_change"])


if __name__ == "__main__":
    main()

