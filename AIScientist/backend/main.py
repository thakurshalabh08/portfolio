from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

BASE_DIR = Path(__file__).resolve().parents[1]
FRONTEND_DIR = BASE_DIR / "frontend"

UNIPROT_BASE_URL = "https://rest.uniprot.org"
UNIPROT_TIMEOUT = 20

app = FastAPI(
    title="AI Co-Scientist",
    description="Agentic AI co-scientist for UniProt-backed discovery workflows.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _frontend_file(path: str) -> FileResponse:
    asset = FRONTEND_DIR / path
    if not asset.exists():
        raise HTTPException(status_code=404, detail="Asset not found")
    return FileResponse(asset)


@app.get("/")
async def root() -> FileResponse:
    index_file = FRONTEND_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="Frontend not found")
    return FileResponse(index_file)


@app.get("/styles.css")
async def styles() -> FileResponse:
    return _frontend_file("styles.css")


@app.get("/app.js")
async def script() -> FileResponse:
    return _frontend_file("app.js")


@app.get("/api/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


def _extract_function(entry: Dict[str, Any]) -> Optional[str]:
    comments = entry.get("comments", [])
    for comment in comments:
        if comment.get("commentType") == "FUNCTION":
            texts = comment.get("texts", [])
            if texts:
                return texts[0].get("value")
    return None


def _summarize_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    summarized: List[Dict[str, Any]] = []
    for entry in entries:
        protein_desc = entry.get("proteinDescription", {})
        recommended = protein_desc.get("recommendedName", {})
        protein_name = None
        if recommended:
            protein_name = recommended.get("fullName", {}).get("value")
        if not protein_name:
            protein_name = entry.get("proteinDescription", {}).get("submissionNames", [{}])[0].get(
                "fullName", {}
            ).get("value")

        gene_names = entry.get("genes", [])
        gene_primary = None
        if gene_names:
            gene_primary = gene_names[0].get("geneName", {}).get("value")

        summarized.append(
            {
                "accession": entry.get("primaryAccession"),
                "id": entry.get("uniProtkbId"),
                "protein_name": protein_name,
                "gene": gene_primary,
                "organism": entry.get("organism", {}).get("scientificName"),
                "function": _extract_function(entry),
            }
        )
    return summarized


async def _uniprot_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{UNIPROT_BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=UNIPROT_TIMEOUT) as client:
        response = await client.get(url, params=params)
    if response.status_code != 200:
        if response.status_code == 400:
            detail = response.text
            try:
                detail = response.json().get("messages", response.text)
            except ValueError:
                detail = response.text
            raise HTTPException(status_code=400, detail=f"UniProt query error: {detail}")
        raise HTTPException(
            status_code=502,
            detail=f"UniProt request failed with status {response.status_code}",
        )
    return response.json()


@app.get("/api/search")
async def search(
    query: str = Query(..., min_length=2),
    size: int = Query(5, ge=1, le=25),
) -> Dict[str, Any]:
    data = await _uniprot_get(
        "/uniprotkb/search",
        {
            "query": query,
            "format": "json",
            "fields": "accession,id,protein_name,gene_primary,organism_name,comment(FUNCTION)",
            "size": size,
        },
    )
    entries = data.get("results", [])
    return {
        "count": data.get("results", []).__len__(),
        "entries": _summarize_entries(entries),
    }


@app.get("/api/entry/{accession}")
async def entry(accession: str) -> Dict[str, Any]:
    data = await _uniprot_get(f"/uniprotkb/{accession}", {"format": "json"})
    protein_desc = data.get("proteinDescription", {})
    recommended = protein_desc.get("recommendedName", {})
    protein_name = None
    if recommended:
        protein_name = recommended.get("fullName", {}).get("value")
    return {
        "accession": data.get("primaryAccession"),
        "id": data.get("uniProtkbId"),
        "protein_name": protein_name,
        "organism": data.get("organism", {}).get("scientificName"),
        "function": _extract_function(data),
        "genes": [
            gene.get("geneName", {}).get("value")
            for gene in data.get("genes", [])
            if gene.get("geneName")
        ],
    }


@app.post("/api/analyze")
async def analyze(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    query = str(payload.get("query", "")).strip()
    if len(query) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")

    focus = str(payload.get("focus", "mechanism of action")).strip()
    organism = str(payload.get("organism", "")).strip()

    query_terms = _build_query_terms(query, organism)

    data = await _uniprot_get(
        "/uniprotkb/search",
        {
            "query": query_terms,
            "format": "json",
            "fields": "accession,id,protein_name,gene_primary,organism_name,comment(FUNCTION)",
            "size": 5,
        },
    )
    entries = _summarize_entries(data.get("results", []))
    if not entries:
        return {
            "summary": "No UniProt entries matched the query. Try a broader term or remove filters.",
            "hypotheses": [],
            "tasks": [],
            "interpretation": [],
            "entries": [],
        }

    hypotheses: List[Dict[str, str]] = []
    interpretation: List[str] = []
    tasks: List[Dict[str, str]] = []
    for entry_item in entries[:3]:
        protein = entry_item.get("protein_name") or entry_item.get("id")
        gene = entry_item.get("gene") or "this gene"
        function = entry_item.get("function") or "a functional role that needs validation"
        organism_name = entry_item.get("organism") or "the relevant organism"

        hypotheses.append(
            {
                "statement": (
                    f"{protein} ({gene}) in {organism_name} may influence {focus} based on the "
                    f"reported function: {function}."
                ),
                "rationale": "Derived from UniProt functional annotation.",
            }
        )
        interpretation.append(
            f"{protein} shows functional annotation linked to {focus}; consider pathway mapping."
        )
        tasks.append(
            {
                "task": f"Retrieve pathway partners for {protein} and check for assay-ready reagents.",
                "data_needed": "Pathway databases, reagent catalogs, cell model availability.",
            }
        )

    return {
        "summary": (
            "Prioritized UniProt entries with functional context for hypothesis generation. "
            "Use the suggestions below to guide experimental planning."
        ),
        "entries": entries,
        "hypotheses": hypotheses,
        "interpretation": interpretation,
        "tasks": tasks,
        "meta": {
            "query": query,
            "organism": organism,
            "focus": focus,
            "requestor": request.client.host if request.client else "unknown",
        },
    }


def _build_query_terms(query: str, organism: str) -> str:
    if not organism:
        return query
    safe_organism = organism.replace('"', '\\"')
    return f'({query}) AND (organism_name:"{safe_organism}")'
