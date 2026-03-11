"""
Parse DFARS NDAA Implementation Tracker PDF to extract NDAA sections
that have a FRN Citation in the Final Rule column, along with Case Numbers.
"""
import pdfplumber
import csv

PDF_PATH = "./DFARS_NDAA_Implementation_Tracker.pdf"
OUTPUT_CSV = "./data/tracker.csv"


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
                    if status != "Implemented":
                        continue
                    case_number = (row[9] or "").strip()
                    final_rule_frn = (row[14] or "").strip()
                    final_rule_date = (row[15] or "").strip()

                    # Normalize year: "FY 10" -> "FY10", etc.
                    ndaa_year = ndaa_year.replace("FY", "20")
                    ndaa_year = ndaa_year.replace(" ", "")

                    # Skip header rows (repeated on each page)
                    if ndaa_year in ("NDAA Year", "Column 1", "Column1", "") or \
                       ndaa_section in ("NDAA Section", "Column2", "") or \
                       final_rule_frn in ("FRN Citation", "Column15", "Final Rule"):
                        continue

                    # Only include rows with a Final Rule FRN Citation
                    if final_rule_frn:
                        final_rule_frn = final_rule_frn.replace("\n", ";")
                        frns = []
                        for final_rule in final_rule_frn.split(";"):
                            if len(final_rule.split(" ")) < 3:
                                final_rule = final_rule[:2] + " " + final_rule[2:]
                            frns.append(final_rule)
                        final_rule_frn = ";".join(frns)

                        results.append(
                            {
                                "ndaa_year": ndaa_year,
                                "ndaa_section": ndaa_section,
                                "section_title": section_title.replace("\n", " "),
                                "status": status,
                                "case_number": case_number.replace("\n", ";"),
                                "citation": final_rule_frn,
                                "publication_date": final_rule_date.replace("\n", ";"),
                            }
                        )

    return results


def main():
    results = extract_final_rule_citations(PDF_PATH)

    print(f"Found {len(results)} NDAA sections with Final Rule FRN Citations\n")

    # Save to CSV
    if results:
        fieldnames = [
            "case_number",
            "publication_date",
            "status",
            "citation",
            "ndaa_year",
            "ndaa_section",
            "section_title",
        ]
        with open(OUTPUT_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\nResults saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
