"""
Retrieval-Augmented Generation for Logic Apps errors.

Uses a local JSONL knowledge base + optional Azure OpenAI embeddings for retrieval,
then a chat completion grounded on retrieved chunks and live flow context.
"""

from __future__ import annotations

import json
import logging
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from logic_app_remediator.config import Settings

logger = logging.getLogger(__name__)

_VALID_ERROR_TYPES = frozenset({"404", "401", "timeout", "bad_request", "unknown"})
_EMBED_CACHE: Dict[str, List[float]] = {}


def _default_chunks_path() -> Path:
    return Path(__file__).resolve().parent / "knowledge" / "chunks.jsonl"


def load_chunks(settings: Settings) -> List[Dict[str, Any]]:
    path = settings.rag_knowledge_path or str(_default_chunks_path())
    p = Path(path)
    if not p.is_file():
        logger.warning("RAG knowledge file missing: %s", path)
        return []
    rows: List[Dict[str, Any]] = []
    with p.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def _keyword_scores(query: str, chunks: List[Dict[str, Any]]) -> List[Tuple[float, Dict[str, Any]]]:
    q_tokens = set(_tokenize(query))
    if not q_tokens:
        return [(0.0, c) for c in chunks]
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for c in chunks:
        text = f"{c.get('title','')} {c.get('text','')} {' '.join(c.get('tags') or [])}"
        tset = set(_tokenize(text))
        inter = len(q_tokens & tset)
        union = len(q_tokens | tset) or 1
        score = inter / union
        code = str(c.get("id") or "")
        for t in c.get("tags") or []:
            if t and t.lower() in query.lower():
                score += 0.15
        if code and code.lower() in query.lower():
            score += 0.1
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _embed_text(text: str, settings: Settings) -> Optional[List[float]]:
    dep = settings.rag_embedding_deployment
    if not dep or not settings.azure_openai_endpoint or not settings.azure_openai_api_key:
        return None
    url = (
        f"{settings.azure_openai_endpoint.rstrip('/')}/openai/deployments/{dep}"
        f"/embeddings?api-version={settings.azure_openai_api_version}"
    )
    try:
        r = requests.post(
            url,
            headers={
                "api-key": settings.azure_openai_api_key,
                "Content-Type": "application/json",
            },
            json={"input": text[:8000]},
            timeout=60,
        )
        r.raise_for_status()
        data = r.json().get("data") or []
        if not data:
            return None
        return data[0].get("embedding")
    except Exception as ex:
        logger.warning("Embedding request failed: %s", ex)
        return None


def _embedding_retrieve(
    query: str, chunks: List[Dict[str, Any]], settings: Settings, top_k: int
) -> Tuple[List[Dict[str, Any]], str]:
    q_emb = _embed_text(query, settings)
    if not q_emb:
        return [], "keyword"

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for c in chunks:
        cid = str(c.get("id") or "")
        text = f"{c.get('title','')}. {c.get('text','')}"
        if cid not in _EMBED_CACHE:
            emb = _embed_text(text, settings)
            if emb:
                _EMBED_CACHE[cid] = emb
        emb = _EMBED_CACHE.get(cid)
        if not emb:
            continue
        scored.append((_cosine(q_emb, emb), c))
    scored.sort(key=lambda x: x[0], reverse=True)
    if not scored:
        return [], "keyword"
    picked = [c for _, c in scored[:top_k]]
    return picked, "embedding"


def retrieve(
    chunks: List[Dict[str, Any]], query: str, settings: Settings
) -> Tuple[List[Dict[str, Any]], str]:
    top_k = max(1, settings.rag_top_k)
    if settings.rag_embedding_deployment:
        picked, mode = _embedding_retrieve(query, chunks, settings, top_k)
        if picked:
            return picked, mode
    kw = _keyword_scores(query, chunks)
    return [c for _, c in kw[:top_k]], "keyword"


def _build_query(error_json: Dict[str, Any], flow_context: Dict[str, Any]) -> str:
    parts = [
        str(error_json.get("code") or ""),
        str(error_json.get("message") or ""),
        str(error_json.get("statusCode") or ""),
        str(flow_context.get("failed_action_name") or ""),
        str(flow_context.get("action_type") or ""),
        str(flow_context.get("workflow_name") or ""),
    ]
    return " ".join(p for p in parts if p)


def _format_context_pack(retrieved: List[Dict[str, Any]]) -> str:
    blocks = []
    for i, c in enumerate(retrieved, 1):
        blocks.append(
            f"[{i}] id={c.get('id')} title={c.get('title')}\n{c.get('text','')}"
        )
    return "\n\n".join(blocks)


def analyze_with_rag(
    error_json: Dict[str, Any],
    flow_context: Dict[str, Any],
    baseline: Dict[str, Any],
    settings: Settings,
) -> Optional[Dict[str, Any]]:
    """
    RAG synthesis: retrieve KB chunks, then LLM must ground answer in chunks + flow_context.
    Returns fields to merge into analysis dict, or None on failure.
    """
    chunks = load_chunks(settings)
    if not chunks:
        return None

    query = _build_query(error_json, flow_context)
    retrieved, mode = retrieve(chunks, query, settings)
    if not retrieved:
        return None

    context_pack = _format_context_pack(retrieved)
    flow_json = json.dumps(flow_context, default=str)[:12000]
    err_json = json.dumps(error_json, default=str)[:8000]
    baseline_json = json.dumps(
        {k: baseline.get(k) for k in ("error_type", "fix_type", "exact_error_code", "exact_error_message", "root_cause")},
        default=str,
    )

    url = (
        f"{settings.azure_openai_endpoint.rstrip('/')}/openai/deployments/"
        f"{settings.azure_openai_deployment}/chat/completions"
        f"?api-version={settings.azure_openai_api_version}"
    )
    system = (
        "You analyze Azure Logic Apps run failures using retrieval-augmented reasoning.\n"
        "Rules:\n"
        "1) RETRIEVED_KNOWLEDGE is reference material only — do not contradict FLOW_CONTEXT.\n"
        "2) FLOW_CONTEXT and ERROR_JSON are ground truth from the live run.\n"
        "3) exact_error_in_flow must state the failing action name (if known) and quote or paraphrase the exact error from the run.\n"
        "4) If baseline classification conflicts with evidence, prefer FLOW_CONTEXT/ERROR_JSON.\n"
        "Return ONLY compact JSON with keys:\n"
        '{"exact_error_in_flow":"string","error_type":"404|401|timeout|bad_request|unknown",'
        '"fix_type":"string","recommendation":"string","root_cause":"string",'
        '"cited_source_ids":["id1"]}\n'
        "No markdown."
    )
    user = (
        f"RETRIEVED_KNOWLEDGE:\n{context_pack}\n\n"
        f"BASELINE_RULES:\n{baseline_json}\n\n"
        f"ERROR_JSON:\n{err_json}\n\n"
        f"FLOW_CONTEXT:\n{flow_json}\n"
    )
    try:
        r = requests.post(
            url,
            headers={
                "api-key": settings.azure_openai_api_key,
                "Content-Type": "application/json",
            },
            json={
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.15,
                "max_tokens": 600,
            },
            timeout=90,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        m = re.search(r"\{[\s\S]*\}", content)
        if not m:
            return None
        parsed = json.loads(m.group())
    except Exception as ex:
        logger.warning("RAG synthesis failed: %s", ex)
        return None

    out: Dict[str, Any] = {
        "rag_enriched": True,
        "rag_retrieval_mode": mode,
        "retrieved_sources": [
            {"id": c.get("id"), "title": c.get("title")} for c in retrieved
        ],
    }
    if parsed.get("exact_error_in_flow"):
        out["exact_error_in_flow"] = parsed["exact_error_in_flow"]
    et = parsed.get("error_type")
    if et in _VALID_ERROR_TYPES:
        out["error_type"] = et
    if parsed.get("fix_type"):
        out["fix_type"] = parsed["fix_type"]
    if parsed.get("recommendation"):
        out["recommendation"] = parsed["recommendation"]
    if parsed.get("root_cause"):
        out["root_cause"] = parsed["root_cause"]
    cited = parsed.get("cited_source_ids")
    if isinstance(cited, list):
        out["rag_cited_source_ids"] = [str(x) for x in cited if x]

    return out
