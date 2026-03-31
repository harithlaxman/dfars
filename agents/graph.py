"""
NDAA ↔ DFARS Agentic Analysis Framework
========================================
LangGraph-based multi-agent system that:
  1. Checks if an NDAA section and a DFARS section are related
  2. If unrelated, extracts citations/references from both sections
  3. Web-searches for referenced documents to gather context
  4. Re-evaluates relatedness with enriched context (loops)
  5. Once related, proposes a list of specific changes needed
  6. Drafts the DFARS revision based on the proposed change list

Graph topology:
  ┌──────────────┐
  │  START        │
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │ Relatedness  │◄──────────────────────────┐
  │   Agent      │                            │
  └──────┬───────┘                            │
         ▼                                    │
    ┌─────────┐   related    ┌────────────┐   │
    │ Router  │─────────────►│ Change     │   │
    └────┬────┘              │ List Agent │   │
         │ not related       └─────┬──────┘   │
         ▼                         ▼          │
  ┌──────────────┐          ┌────────────┐    │
  │  Citation    │          │ Drafting   │    │
  │  Extractor   │          │   Agent    │    │
  └──────┬───────┘          └─────┬──────┘    │
         ▼                        ▼           │
  ┌──────────────┐          ┌──────────┐      │
  │  Web Search  │          │   END    │      │
  │  Researcher  │          └──────────┘      │
  └──────┬───────┘                            │
         └────────────────────────────────────┘
"""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import os

from langchain_openai import AzureChatOpenAI
from langchain_community.tools import DuckDuckGoSearchResults
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph

# ---------------------------------------------------------------------------
# State definition
# ---------------------------------------------------------------------------

class GraphState(TypedDict):
    """Shared state flowing through the graph."""

    ndaa_section: str                                    # raw NDAA text
    dfars_section: str                                   # raw DFARS text
    is_related: bool                                     # latest relatedness verdict
    relatedness_reasoning: str                           # explanation for the verdict
    extracted_citations: list[str]                       # citations pulled from both sections
    searched_citations: Annotated[list[str], operator.add]  # citations already searched
    research_context: Annotated[list[str], operator.add]    # web-search results gathered
    iteration: int                                       # loop counter
    max_iterations: int                                  # safety cap
    proposed_changes: str                                # list of changes from change_list_agent
    draft_dfars_change: str                              # final output


# ---------------------------------------------------------------------------
# LLM & tools
# ---------------------------------------------------------------------------

def get_llm(temperature: float = 0.0) -> AzureChatOpenAI:
    """Return an Azure OpenAI instance.

    Required env vars (set before running):
        AZURE_OPENAI_API_KEY      – your Azure OpenAI key
        AZURE_OPENAI_ENDPOINT     – e.g. https://<resource>.openai.azure.com/
        AZURE_OPENAI_DEPLOYMENT   – deployment name (e.g. gpt-4o, gpt-4-turbo)
        AZURE_OPENAI_API_VERSION  – e.g. 2024-08-01-preview
    """
    return AzureChatOpenAI(
        azure_deployment="gpt-4.1",
        azure_endpoint=os.environ["OPENAI_ENDPOINT"],
        api_key=os.environ["OPENAI_API_KEY"],
        api_version="2025-03-01-preview",
        temperature=temperature,
        max_tokens=4096,
    )


search_tool = DuckDuckGoSearchResults(max_results=5)


# ---------------------------------------------------------------------------
# Node 1 – Relatedness Agent
# ---------------------------------------------------------------------------

def relatedness_agent(state: GraphState) -> dict:
    """Decide whether the NDAA and DFARS sections are substantively related."""
    llm = get_llm()

    # Build a context supplement from any research gathered so far
    extra_context = ""
    if state.get("research_context"):
        extra_context = (
            "\n\n--- Additional reference material gathered via web search ---\n"
            + "\n---\n".join(state["research_context"])
        )

    prompt = f"""You are a U.S. defense-acquisition policy analyst.

Given the following NDAA section and DFARS section, determine whether they are
**substantively related** — i.e. the NDAA provision would require, motivate, or
directly affect the DFARS clause or its subject matter.

NDAA SECTION:
\"\"\"
{state["ndaa_section"]}
\"\"\"

DFARS SECTION:
\"\"\"
{state["dfars_section"]}
\"\"\"
{extra_context}

Respond in **exactly** this format (no other text):
RELATED: yes  OR  RELATED: no
REASONING: <one-paragraph explanation>
"""
    response = llm.invoke([SystemMessage(content="You are a defense policy expert."),
                           HumanMessage(content=prompt)])
    text = response.content.strip()

    is_related = "RELATED: yes" in text.split("\n")[0]
    reasoning = text.split("REASONING:")[-1].strip() if "REASONING:" in text else text

    return {
        "is_related": is_related,
        "relatedness_reasoning": reasoning,
        "iteration": state.get("iteration", 0) + 1,
    }


# ---------------------------------------------------------------------------
# Router (conditional edge)
# ---------------------------------------------------------------------------

def route_after_relatedness(state: GraphState) -> str:
    """Branch: always do ≥1 research pass before drafting.

    Even when the pair is related on the first check, we still extract
    citations and web-search once to gather richer context for drafting.
    """
    # Always do at least one round of research before drafting
    if state["iteration"] <= 1:
        return "extract"
    if state["is_related"]:
        return "draft"
    if state["iteration"] >= state.get("max_iterations", 3):
        return "draft"          # give up searching, draft with what we have
    return "extract"


# ---------------------------------------------------------------------------
# Node 2 – Citation / Reference Extractor
# ---------------------------------------------------------------------------

def citation_extractor(state: GraphState) -> dict:
    """Pull every external citation, statute, or document reference from both sections."""
    llm = get_llm()

    prompt = f"""You are a legal citation extraction specialist.

IMPORTANT CONTEXT: The DFARS (Defense Federal Acquisition Regulation Supplement)
is codified as **Title 48, Chapter 2** of the Code of Federal Regulations (CFR).
When the DFARS text says "this part", "this subpart", or "this section", it is
referring to the specific DFARS part/subpart/section identified in the text below.
You MUST resolve these relative references into fully-qualified citations.

For example:
  - "this part" in DFARS Part 209 → "48 CFR Part 209" or "DFARS Part 209"
  - "Section 209.170-2 of this subpart" → "DFARS 209.170-2"
  - "this section" in Section 252.204-7012 → "DFARS 252.204-7012"

From the two regulatory texts below, extract **every** reference to an external
document, statute, executive order, USC section, CFR part, public law, OMB
circular, or similar authoritative source. Resolve ALL relative references
("this part", "this subpart", "this section") into their fully-qualified form
using the part, subpart, and section identifiers provided in the DFARS text.

Return ONLY a JSON array of strings — one element per unique citation.
Example: ["10 USC 2302", "FAR 15.4", "EO 13800", "Pub. L. 117-263 Sec. 845", "DFARS Part 209"]

NDAA SECTION:
\"\"\"
{state["ndaa_section"]}
\"\"\"

DFARS SECTION:
\"\"\"
{state["dfars_section"]}
\"\"\"
"""
    response = llm.invoke([HumanMessage(content=prompt)])
    text = response.content.strip()

    # Robustly parse the JSON array
    import json, re
    match = re.search(r"\[.*\]", text, re.DOTALL)
    citations: list[str] = []
    if match:
        try:
            citations = json.loads(match.group())
        except json.JSONDecodeError:
            citations = [c.strip().strip('"') for c in match.group()[1:-1].split(",")]

    # Filter out citations we already searched
    already = set(state.get("searched_citations", []))
    new_citations = [c for c in citations if c not in already]

    return {"extracted_citations": new_citations}


# ---------------------------------------------------------------------------
# Node 3 – Web-Search Researcher
# ---------------------------------------------------------------------------

def web_search_researcher(state: GraphState) -> dict:
    """Search the web for each extracted citation and summarise findings."""
    llm = get_llm(temperature=0.0)
    citations = state.get("extracted_citations", [])

    new_context: list[str] = []
    searched: list[str] = []

    for citation in citations[:5]:  # cap per iteration to avoid runaway costs
        query = f"{citation} defense acquisition regulation summary"
        try:
            raw_results = search_tool.invoke(query)
            results_text = raw_results if isinstance(raw_results, str) else str(raw_results)
        except Exception:
            results_text = "(no results)"

        # Ask LLM to distill the search results into useful context
        summary_prompt = f"""Summarise the following web-search results about
"{citation}" in 2-3 sentences that would help determine whether an NDAA
provision is related to a DFARS clause.

SEARCH RESULTS:
{results_text}
"""
        summary = llm.invoke([HumanMessage(content=summary_prompt)])
        new_context.append(f"[{citation}] {summary.content.strip()}")
        searched.append(citation)

    return {
        "research_context": new_context,
        "searched_citations": searched,
    }


# ---------------------------------------------------------------------------
# Node 4 – Change List Agent
# ---------------------------------------------------------------------------

def change_list_agent(state: GraphState) -> dict:
    """Propose a structured list of changes the DFARS section needs, without drafting."""
    llm = get_llm(temperature=0.0)

    extra_context = ""
    if state.get("research_context"):
        extra_context = (
            "\n\n--- Supporting research context ---\n"
            + "\n---\n".join(state["research_context"])
        )

    prompt = f"""You are a U.S. defense-acquisition policy analyst specializing in DFARS rulemaking.

Given the NDAA section and existing DFARS section below, produce a **detailed,
numbered list of specific changes** that must be made to the DFARS section to
comply with the NDAA provision.

Instructions:
  • Do NOT draft or rewrite the DFARS text — only list the changes.
  • Each item should describe ONE discrete change (addition, deletion, or modification).
  • For each change, specify:
      1. **Location**: Which subsection, paragraph, or definition is affected.
      2. **Type**: ADD (new text), REMOVE (delete existing text), or MODIFY (alter existing text).
      3. **Description**: What exactly should be changed and why the NDAA requires it.
  • If a new subsection or definition is needed, describe where it should be inserted
    and what it should cover.
  • If no changes are needed, state that clearly with justification.
  • Order the changes logically (e.g. definitions first, then substantive clauses,
    then reporting requirements).

NDAA SECTION (statutory driver):
\"\"\"
{state["ndaa_section"]}
\"\"\"

EXISTING DFARS SECTION:
\"\"\"
{state["dfars_section"]}
\"\"\"

Relatedness analysis: {state.get("relatedness_reasoning", "N/A")}
{extra_context}
"""
    response = llm.invoke([
        SystemMessage(content="You are a regulatory change analyst. Be specific and actionable."),
        HumanMessage(content=prompt),
    ])

    return {"proposed_changes": response.content.strip()}


# ---------------------------------------------------------------------------
# Node 5 – DFARS Drafting Agent
# ---------------------------------------------------------------------------

def drafting_agent(state: GraphState) -> dict:
    """Draft a proposed DFARS change based on the change list."""
    llm = get_llm(temperature=0.2)

    extra_context = ""
    if state.get("research_context"):
        extra_context = (
            "\n\n--- Supporting research context ---\n"
            + "\n---\n".join(state["research_context"])
        )

    prompt = f"""You are a senior DFARS regulatory drafter at the Department of Defense.

Using the proposed change list below, **directly edit** the existing DFARS text
to implement each change.

PROPOSED CHANGES:
\"\"\"
{state.get("proposed_changes", "N/A")}
\"\"\"

Instructions:
  • Implement every change from the list above into the DFARS text.
  • Output the revised DFARS text in full, with changes applied inline.
  • Wrap any NEW text you are adding with <added> tags, e.g. <added>new text here</added>.
  • Wrap any EXISTING text you are removing with <removed> tags, e.g. <removed>old text here</removed>.
  • If text is being replaced, show both: <removed>old text</removed> <added>new text</added>.
  • If new subsections or definitions are needed, insert them in the appropriate
    place within the existing structure, wrapped in <added> tags.
  • If the change list says no changes are needed, return the original text unchanged
    and state why.
  • At the end, add a short "Summary of Changes" section (3-5 bullet points max)
    listing what was changed and the statutory basis.

NDAA SECTION (statutory driver):
\"\"\"
{state["ndaa_section"]}
\"\"\"

EXISTING DFARS SECTION:
\"\"\"
{state["dfars_section"]}
\"\"\"

Relatedness analysis: {state.get("relatedness_reasoning", "N/A")}
{extra_context}
"""
    response = llm.invoke([
        SystemMessage(content="You directly edit DFARS regulatory text. Be precise. Follow the change list exactly."),
        HumanMessage(content=prompt),
    ])

    return {"draft_dfars_change": response.content.strip()}


# ---------------------------------------------------------------------------
# Graph assembly
# ---------------------------------------------------------------------------

def build_graph() -> StateGraph:
    """Construct and compile the LangGraph workflow."""
    workflow = StateGraph(GraphState)

    # -- nodes --
    workflow.add_node("relatedness_agent", relatedness_agent)
    workflow.add_node("citation_extractor", citation_extractor)
    workflow.add_node("web_search_researcher", web_search_researcher)
    workflow.add_node("change_list_agent", change_list_agent)
    workflow.add_node("drafting_agent", drafting_agent)

    # -- edges --
    workflow.set_entry_point("relatedness_agent")

    workflow.add_conditional_edges(
        "relatedness_agent",
        route_after_relatedness,
        {
            "draft": "change_list_agent",
            "extract": "citation_extractor",
        },
    )
    workflow.add_edge("citation_extractor", "web_search_researcher")
    workflow.add_edge("web_search_researcher", "relatedness_agent")   # loop back
    workflow.add_edge("change_list_agent", "drafting_agent")
    workflow.add_edge("drafting_agent", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------

def run_analysis(
    ndaa_text: str,
    dfars_text: str,
    max_iterations: int = 3,
) -> GraphState:
    """Run the full pipeline and return the final state."""
    graph = build_graph()
    initial_state: GraphState = {
        "ndaa_section": ndaa_text,
        "dfars_section": dfars_text,
        "is_related": False,
        "relatedness_reasoning": "",
        "extracted_citations": [],
        "searched_citations": [],
        "research_context": [],
        "iteration": 0,
        "max_iterations": max_iterations,
        "proposed_changes": "",
        "draft_dfars_change": "",
    }
    return graph.invoke(initial_state)
