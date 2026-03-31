# NDAA ↔ DFARS Agentic Analysis Framework

A LangGraph-based multi-agent system that determines whether an NDAA section and a DFARS section are related, researches external references to build context, and drafts proposed DFARS changes when a relationship is confirmed.

## Architecture

```
                    ┌──────────────────┐
                    │     START        │
                    └────────┬─────────┘
                             ▼
                    ┌──────────────────┐
               ┌───►│  Relatedness     │
               │    │     Agent        │
               │    └────────┬─────────┘
               │             ▼
               │       ┌──────────┐     related     ┌──────────────┐
               │       │  Router  │────────────────►│   Drafting   │
               │       └────┬─────┘                 │    Agent     │
               │            │ not related           └──────┬───────┘
               │            ▼                              ▼
               │    ┌──────────────────┐            ┌──────────┐
               │    │    Citation      │            │   END    │
               │    │    Extractor     │            └──────────┘
               │    └────────┬─────────┘
               │             ▼
               │    ┌──────────────────┐
               └────┤   Web Search     │
                    │   Researcher     │
                    └──────────────────┘
```

### Agents

| Agent | Role |
|---|---|
| **Relatedness Agent** | Evaluates whether the NDAA section substantively relates to the DFARS section, incorporating any research context gathered in prior loops. |
| **Citation Extractor** | Parses both sections for external references (USC, CFR, Public Laws, EOs, NIST pubs, FAR/DFARS cross-refs). |
| **Web Search Researcher** | Searches the web for each extracted citation, then summarises findings into reusable context. |
| **Drafting Agent** | Produces a publication-ready DFARS change proposal with preamble, tracked changes, and statutory basis. |

### State

All agents read from and write to a shared `GraphState` dictionary:

```python
class GraphState(TypedDict):
    ndaa_section: str
    dfars_section: str
    is_related: bool
    relatedness_reasoning: str
    extracted_citations: list[str]
    searched_citations: list[str]      # accumulates across loops
    research_context: list[str]        # accumulates across loops
    iteration: int
    max_iterations: int
    draft_dfars_change: str
```

The `searched_citations` and `research_context` fields use LangGraph's `operator.add` reducer, so each loop iteration *appends* rather than overwrites.

## Setup

```bash
pip install -r requirements.txt

# Set your Azure OpenAI credentials
export AZURE_OPENAI_API_KEY="your-azure-key"
export AZURE_OPENAI_ENDPOINT="https://<your-resource>.openai.azure.com/"
export AZURE_OPENAI_DEPLOYMENT="gpt-4o"           # your deployment name
export AZURE_OPENAI_API_VERSION="2024-08-01-preview"
```

## Usage

```bash
# Demo mode with built-in NDAA/DFARS sample (cybersecurity)
python main.py --demo

# With text files
python main.py --ndaa-file ndaa_section.txt --dfars-file dfars_section.txt

# Inline text
python main.py --ndaa "SEC. 845. ..." --dfars "DFARS 252.204-7012 ..."

# Control loop depth (default: 3)
python main.py --demo --max-iter 5
```

## Customisation

**Swap the LLM** — change the `azure_deployment` env var or modify `get_llm()` in `graph.py` to point at a different Azure deployment (e.g. `gpt-4-turbo`, `gpt-35-turbo`).

**Swap the search tool** — replace `DuckDuckGoSearchResults` with Tavily, SerpAPI, or any LangChain-compatible search tool.

**Add more nodes** — for example a "Compliance Gap Analyzer" between the relatedness check and the drafter, or a "Human-in-the-Loop" approval step before finalising the draft.

**Persist state** — LangGraph supports `SqliteSaver` and `PostgresSaver` checkpointers for durable state across runs.
