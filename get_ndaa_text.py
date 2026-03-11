import ast

from tqdm import tqdm
import pandas as pd
from pathlib import Path

import data.ndaa.utils as ndaa_utils

DOC_TO_NDAA_CSV = Path("./data/doc_to_ndaa.csv")

def get_text(citation):
    year = citation["ndaa_year"]
    title = citation["title"]
    subtitle = citation["subtitle"]
    section = citation["section"]
    subsection = citation["subsection"]

    text = None

    try:
        if title != "":
            text = ndaa_utils.get_title_text(year, title)
        elif section != "":
            text = ndaa_utils.get_section_text(year, section)
            if subsection != "":
                subsection = subsection[:3]
                text = ndaa_utils.get_subsection_text(year, section, subsection)
    except ValueError:
        tqdm.write(f"Could not find text for citation: {citation}")
    except FileNotFoundError:
        tqdm.write(f"NDAA file does not exist for year: {year}")
    return text

def main():
    doc_to_ndaa_df = pd.read_csv(DOC_TO_NDAA_CSV)
    
    ndaa_text = []

    for _, row in tqdm(doc_to_ndaa_df.iterrows(), total=len(doc_to_ndaa_df)):
        citations = ast.literal_eval(row["citations"])
        for citation in citations:
            text = get_text(citation)
            if text is not None:
                citation["text"] = text
                ndaa_text.append(citation)

    ndaa_text_df = pd.DataFrame(ndaa_text)
    ndaa_text_df.to_csv("./data/ndaa_text.csv", index=False)

if __name__ == "__main__":
    main()