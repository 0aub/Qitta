"""Search utilities for the vector service.

This module wraps querying operations against a FAISS‑backed vector
store.  It supports filtering by case and returning the top‑k results
along with their metadata and similarity scores.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple


def query_vector_store(
    vector_store: Any,
    query: str,
    k: int = 5,
    case: str | None = None
) -> List[Dict[str, Any]]:
    """Query the vector store and return the top‑k documents.

    Parameters
    ----------
    vector_store : object
        The vector store implementing ``query`` method returning
        (doc, metadata, score) tuples.
    query : str
        The user query text.
    k : int, optional
        The number of results to return.  Default is 5.
    case : str, optional
        Restrict results to a given case by filtering on the metadata's
        ``case`` field.  If omitted, all cases are considered.

    Returns
    -------
    list of dict
        A list of search results with keys ``text``, ``metadata`` and
        ``score``.
    """
    results = vector_store.query(query, k)
    filtered: List[Dict[str, Any]] = []
    for doc, meta, score in results:
        if case and meta.get("case") != case:
            continue
        filtered.append({
            "text": doc,
            "metadata": meta,
            "score": score
        })
    return filtered