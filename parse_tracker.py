"""
Parse DFARS NDAA Implementation Tracker PDF to extract NDAA sections
that have a FRN Citation in the Final Rule column, along with Case Numbers.
"""
import pdfplumber
import csv

PDF_PATH = "./DFARS_NDAA_Implementation_Tracker.pdf"
OUTPUT_CSV = "./data/ndaa_final_rule_citations.csv"


def extract_final_rule_citations(pdf_path):
    results = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            if not tables:
                continue

            for table in tables:
                for row in table:
                    if len(row) < 17:
                        continue

                    ndaa_year = (row[0] or "").strip().replace("\n", " ")
                    ndaa_section = (row[1] or "").strip().replace("\n", " ")
                    para = (row[2] or "").strip()
                    section_title = (row[3] or "").strip()
                    status = (row[4] or "").strip()
                    case_number = (row[9] or "").strip().replace("\n", "\n")
                    final_rule_frn = (row[14] or "").strip()
                    final_rule_date = (row[15] or "").strip()

                    # Normalize year: "FY 10" -> "FY10", etc.
                    ndaa_year = ndaa_year.replace("FY ", "FY")

                    # Skip header rows (repeated on each page)
                    if ndaa_year in ("NDAA Year", "Column 1", "Column1", "") or \
                       ndaa_section in ("NDAA Section", "Column2", "") or \
                       final_rule_frn in ("FRN Citation", "Column15", "Final Rule"):
                        continue

                    # Only include rows with a Final Rule FRN Citation
                    if final_rule_frn:
                        results.append(
                            {
                                "ndaa_year": ndaa_year,
                                "ndaa_section": ndaa_section,
                                "paragraph": para,
                                "section_title": section_title.replace("\n", " "),
                                "status": status,
                                "case_number": case_number,
                                "frn_citation": final_rule_frn,
                                "fr_date": final_rule_date,
                            }
                        )

    return results


def main():
    results = extract_final_rule_citations(PDF_PATH)

    # Print summary
    print(f"Found {len(results)} NDAA sections with Final Rule FRN Citations\n")
    print(f"{'NDAA Year':<10} {'Section':<10} {'Case Number':<15} {'FRN Citation':<20} {'Date':<12} {'Title'}")
    print("-" * 120)
    for r in results:
        title = r["section_title"][:50] + "..." if len(r["section_title"]) > 50 else r["section_title"]
        print(
            f"{r['ndaa_year']:<10} {r['ndaa_section']:<10} {r['case_number']:<15} "
            f"{r['frn_citation']:<20} {r['fr_date']:<12} {title}"
        )

    # Save to CSV
    if results:
        fieldnames = [
            "ndaa_year",
            "ndaa_section",
            "paragraph",
            "section_title",
            "status",
            "case_number",
            "frn_citation",
            "fr_date",
        ]
        with open(OUTPUT_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\nResults saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
