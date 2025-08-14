"""Embedding and vector index utilities.

This module defines helpers for loading sentence‑transformer models and
constructing FAISS indices based on a requested similarity metric.  The
embedding and index objects are encapsulated to make it easier to swap
models or index types in the future.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

import faiss  # type: ignore
from sentence_transformers import SentenceTransformer  # type: ignore


class EmbeddingModel:
    """Wraps a SentenceTransformer model loaded on demand.

    Parameters
    ----------
    model_name : str
        The huggingface identifier for the model.  A small model such as
        ``all-MiniLM-L6-v2`` is set by default via the environment
        variable ``EMBEDDING_MODEL``.  Larger models require more RAM
        but produce higher quality embeddings.
    """

    def __init__(self, model_name: str) -> None:
        self.model_name = model_name
        self._model: Optional[SentenceTransformer] = None

    @property
    def model(self) -> SentenceTransformer:
        if self._model is None:
            # Download and cache the model.  SentenceTransformer will
            # place files in ~/.cache by default.
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Compute embeddings for a list of texts.

        Returns a list of embedding vectors as Python lists.  We use
        ``convert_to_numpy=True`` to obtain NumPy arrays and then cast
        to lists so that FAISS can consume them.
        """
        vectors = self.model.encode(texts, convert_to_numpy=True)
        return vectors.tolist()  # type: ignore[return-value]


def _build_faiss_index(dim: int, metric: str) -> faiss.Index:
    """Create a FAISS index appropriate for the given metric.

    * ``cosine`` → ``IndexHNSWFlat`` with inner product (cosine
      similarity after normalisation).
    * ``inner_product`` → ``IndexFlatIP``.
    * ``l2`` → ``IndexFlatL2``.

    If an unknown metric is provided, a ``ValueError`` is raised.
    """
    metric = metric.lower()
    if metric == "cosine":
        # HNSW approximates cosine similarity by using inner product on
        # unit‑normalised vectors.  Note: we normalise embeddings when
        # indexing and querying.
        index = faiss.IndexHNSWFlat(dim, 32, faiss.METRIC_INNER_PRODUCT)
        index.hnsw.efConstruction = 40
    elif metric == "inner_product":
        index = faiss.IndexFlatIP(dim)
    elif metric == "l2":
        index = faiss.IndexFlatL2(dim)
    else:
        raise ValueError(f"Unsupported similarity metric: {metric}")
    return index


@lru_cache(maxsize=None)
def get_embedding_model(model_name: str) -> EmbeddingModel:
    """Get or create an EmbeddingModel by name.

    Uses an LRU cache so that repeated requests for the same model do
    not instantiate multiple SentenceTransformer objects.
    """
    return EmbeddingModel(model_name)


def build_index(vectors: list[list[float]], metric: str) -> tuple[faiss.Index, int]:
    """Construct a FAISS index from a list of embedding vectors.

    The function infers the dimensionality from the first vector and
    returns both the index and the dimension.  Vectors are normalised
    when ``metric == 'cosine'`` to ensure cosine similarity is
    equivalent to inner product.
    """
    if not vectors:
        raise ValueError("Cannot build index from an empty list of vectors")
    import numpy as np  # delayed import
    dim = len(vectors[0])
    metric_lower = metric.lower()
    index = _build_faiss_index(dim, metric_lower)
    arr = np.array(vectors, dtype='float32')
    if metric_lower == 'cosine':
        # Normalise each vector to unit length for cosine similarity
        faiss.normalize_L2(arr)
    index.add(arr)
    return index, dim