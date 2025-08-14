"""Entry point for the vector store service.

This FastAPI application exposes endpoints for ingesting documents into
 a FAISS‑backed vector store and for querying that store.  It supports
 multiple cases and allows the caller to specify the embedding model and
 similarity metric at ingestion time.  Each combination of case,
 embedding model and metric yields a separate in‑memory index.  You can
 persist the indices by mounting a volume at ``/vector_store`` and
 implementing saving/loading logic in a future version.
"""

from __future__ import annotations

import datetime
import logging
import pathlib
from typing import Dict, List, Optional, Tuple

import faiss  # type: ignore
from fastapi import FastAPI, HTTPException, Response as FastAPIResponse
from pydantic import BaseModel, Field

from .embeddings import get_embedding_model

# Additional imports for persistence and configuration
import os
import pickle
from .ingestion import ingest_sources
from .search import query_vector_store

# Prometheus metrics
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Define metrics for vectorstore service
REQUEST_COUNT = Counter(
    "vectorstore_request_count",
    "Number of requests received",
    labelnames=["endpoint", "method", "status"],
)
REQUEST_LATENCY = Histogram(
    "vectorstore_request_latency_seconds",
    "Request latency in seconds",
    labelnames=["endpoint"],
)

# ----------------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------------

def init_logger(case: Optional[str] = None) -> logging.Logger:
    """Initialise a logger that writes to /logs based on the current date.

    Logs for the base service are written to ``/logs/base/YYYY-MM-DD.log``.
    If ``case`` is provided, logs are also written to
    ``/logs/cases/<case>/YYYY-MM-DD.log``.  In addition to file handlers,
    logs are emitted to standard output.  This function should be
    called once per request handler to tag logs with the case.
    """
    today = datetime.date.today().isoformat()
    # Determine root directory for logs.  Default to /storage/logs but allow
    # override via the LOG_ROOT environment variable.  Do not hard‑code /logs
    # because logs are consolidated under storage for this project.
    root = pathlib.Path(os.getenv("LOG_ROOT", "/storage/logs"))
    handlers: List[logging.Handler] = []
    # Base service log directory: base/vectorstores
    base_dir = root / "base" / "vectorstores"
    base_dir.mkdir(parents=True, exist_ok=True)
    handlers.append(logging.FileHandler(base_dir / f"{today}.log"))
    # Case specific log directory: cases/<case>
    if case:
        case_dir = root / "cases" / case
        case_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(case_dir / f"{today}.log"))
    # Stream to console
    handlers.append(logging.StreamHandler())
    logger = logging.getLogger(f"vectorstores[{case or 'base'}]")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        formatter = logging.Formatter(fmt)
        for h in handlers:
            h.setFormatter(formatter)
            logger.addHandler(h)
    return logger

# ----------------------------------------------------------------------------
# Vector store implementation
# ----------------------------------------------------------------------------

class VectorStore:
    """A simple vector store using FAISS.

    Each instance maintains its own list of documents and metadata.  The
    index supports incremental addition of new documents.  Embeddings
    are computed using a shared EmbeddingModel instance retrieved via
    ``get_embedding_model``.
    """

    def __init__(self, embedding_model_name: str, metric: str) -> None:
        self.embedding_model_name = embedding_model_name
        self.metric = metric.lower()
        self._embedding_model = get_embedding_model(embedding_model_name)
        self.dim: Optional[int] = None
        # Sharding support: list of FAISS index wrappers, one per shard
        self.shards: List[faiss.IndexIDMap] = []
        # Documents and metadata per shard
        self.documents_shards: List[List[str]] = []
        self.metadatas_shards: List[List[dict]] = []
        # Next global document ID used across shards
        self._next_id: int = 0
        # Maximum documents per shard; beyond this a new shard is created
        self.shard_size: int = int(os.getenv("VECTOR_SHARD_SIZE", "100000"))

    def _create_index(self) -> faiss.IndexIDMap:
        """Create a new FAISS index for a shard based on the metric and dimension."""
        if self.dim is None:
            # Determine embedding dimension by encoding a dummy
            dummy_vec = self._embedding_model.embed(["dummy"])[0]
            self.dim = len(dummy_vec)
        if self.metric == 'cosine':
            base_index = faiss.IndexHNSWFlat(self.dim, 32, faiss.METRIC_INNER_PRODUCT)
            base_index.hnsw.efConstruction = 40
        elif self.metric == 'inner_product':
            base_index = faiss.IndexFlatIP(self.dim)
        elif self.metric == 'l2':
            base_index = faiss.IndexFlatL2(self.dim)
        else:
            raise ValueError(f"Unsupported metric {self.metric}")
        return faiss.IndexIDMap(base_index)

    def add_documents(self, texts: List[str], metadatas: Optional[List[dict]] = None) -> None:
        """Add documents and their metadata to the store with sharding.

        When the number of documents in the current shard reaches
        ``self.shard_size``, a new shard is created automatically.
        Embeddings are computed once for all texts.
        """
        if metadatas is None:
            metadatas = [{} for _ in texts]
        if len(texts) != len(metadatas):
            raise ValueError("texts and metadatas must be the same length")
        # Compute embeddings for all texts at once
        embeddings = self._embedding_model.embed(texts)
        import numpy as np
        for emb, text, meta in zip(embeddings, texts, metadatas):
            # Create a new shard if needed
            if not self.shards or len(self.documents_shards[-1]) >= self.shard_size:
                new_index = self._create_index()
                self.shards.append(new_index)
                self.documents_shards.append([])
                self.metadatas_shards.append([])
            # Normalise if cosine similarity
            vec = np.array([emb], dtype='float32')
            if self.metric == 'cosine':
                faiss.normalize_L2(vec)
            # Insert into current shard with global ID
            current_idx = len(self.shards) - 1
            shard_index = self.shards[current_idx]
            shard_index.add_with_ids(vec, np.array([self._next_id], dtype='int64'))  # type: ignore[arg-type]
            self.documents_shards[current_idx].append(text)
            self.metadatas_shards[current_idx].append(meta)
            self._next_id += 1

    def query(self, query_text: str, k: int = 5) -> List[Tuple[str, dict, float]]:
        """Return the top‑k documents similar to ``query_text`` across all shards.

        The method computes the query embedding once and searches each
        shard individually, combining and sorting results by similarity.
        """
        if not self.shards:
            return []
        emb = self._embedding_model.embed([query_text])[0]
        import numpy as np
        vec = np.array([emb], dtype='float32')
        if self.metric == 'cosine':
            faiss.normalize_L2(vec)
        results: List[Tuple[str, dict, float]] = []
        offset = 0
        for shard_idx, shard in enumerate(self.shards):
            scores, ids = shard.search(vec, k)
            # Map global IDs back to local documents
            for score, idx in zip(scores[0], ids[0]):
                if idx < 0:
                    continue
                # Determine which shard the id belongs to
                # Find the shard that contains this global id
                # Compute document index by subtracting offsets
                found = False
                cumulative = 0
                for s_docs, s_metas in zip(self.documents_shards, self.metadatas_shards):
                    if idx < cumulative + len(s_docs):
                        doc = s_docs[idx - cumulative]
                        meta = s_metas[idx - cumulative]
                        results.append((doc, meta, float(score)))
                        found = True
                        break
                    cumulative += len(s_docs)
                if not found:
                    # Should not happen
                    continue
        # Sort combined results by similarity descending and return top k
        results.sort(key=lambda x: x[2], reverse=True)
        return results[:k]

# ----------------------------------------------------------------------------
# FastAPI app definitions
# ----------------------------------------------------------------------------

app = FastAPI(title="Agentic Platform Vector Store")

# Initialise base logger at import time so that log directories are created
# when the service starts.  This ensures logs/base/vectorstores exists even
# before any requests are processed.
init_logger()

# Global vector stores keyed by (case, embedding_model, similarity)
VECTOR_STORES: Dict[Tuple[str, str, str], VectorStore] = {}

# -------------------------------------------------------------------------
# Persistence and configuration
# -------------------------------------------------------------------------
# Root directory on disk for storing persistent vector indices and metadata.
# This directory is mounted by the Docker compose file to survive container
# restarts.  It can be overridden via the ``VECTOR_INDEX_DIR`` environment
# variable but defaults to ``/storage/vector_index``.
VECTOR_ROOT = pathlib.Path(os.getenv("VECTOR_INDEX_DIR", "/storage/vector_index"))
VECTOR_ROOT.mkdir(parents=True, exist_ok=True)

# If ``VECTOR_USE_GPU`` is set to 'true', '1' or 'yes', FAISS indices will
# be moved onto the first available GPU when loaded from disk, and moved
# back to CPU when persisted.  This can accelerate search on systems with
# GPUs.  When no GPU is available the code silently falls back to CPU.
USE_GPU = os.getenv("VECTOR_USE_GPU", "false").lower() in ("1", "true", "yes")

def _slugify(value: str) -> str:
    """Sanitise a string into a filesystem‑friendly form."""
    return value.replace("/", "__").replace(":", "_")

def _get_store_dir(case: str, model: str, metric: str) -> pathlib.Path:
    """Return the directory path for a given (case, model, metric)."""
    return VECTOR_ROOT / case / _slugify(model) / metric

def _save_vector_store(case: str, model: str, metric: str, store: VectorStore) -> None:
    """Persist a vector store's shards, metadata and source texts to disk.

    For each shard, the FAISS index is written to ``index_<n>.faiss``.  A
    single ``metadata.pkl`` file stores the embedding model name, metric,
    per‑shard documents and metadata and the next global ID.  In addition,
    this function writes out a ``references.txt`` file listing the unique
    source URLs contained in the store's metadata, and saves each ingested
    document as an individual ``doc_<id>.txt`` file inside a ``texts``
    subdirectory.  These files provide a human‑readable trace of the
    documents used to build the vector store and can be useful for
    debugging and auditing.

    GPU indices are converted back to CPU indices before saving to
    guarantee portability across systems that may lack GPUs.
    """
    dir_path = _get_store_dir(case, model, metric)
    dir_path.mkdir(parents=True, exist_ok=True)
    # Save each shard's FAISS index
    for i, shard in enumerate(store.shards):
        idx = shard
        if USE_GPU:
            try:
                # Convert GPU index back to CPU
                idx = faiss.index_gpu_to_cpu(idx)  # type: ignore[attr-defined]
            except Exception:
                # Ignore conversion errors
                pass
        faiss.write_index(idx, str(dir_path / f"index_{i}.faiss"))
    # Persist metadata
    metadata = {
        "embedding_model": store.embedding_model_name,
        "metric": store.metric,
        "documents_shards": store.documents_shards,
        "metadatas_shards": store.metadatas_shards,
        "next_id": store._next_id,
    }
    with open(dir_path / "metadata.pkl", "wb") as f:
        pickle.dump(metadata, f)
    # Write human‑readable reference and text files
    try:
        # Collect unique source URLs from metadata
        sources: set[str] = set()
        for shard_metas in store.metadatas_shards:
            for meta in shard_metas:
                src = meta.get("source")
                if src:
                    sources.add(str(src))
        # Write references.txt
        if sources:
            with open(dir_path / "references.txt", "w", encoding="utf-8") as rf:
                rf.write("\n".join(sorted(sources)))
        # Save each document as a separate file
        texts_dir = dir_path / "texts"
        texts_dir.mkdir(parents=True, exist_ok=True)
        global_id = 0
        for shard_docs in store.documents_shards:
            for doc in shard_docs:
                doc_path = texts_dir / f"doc_{global_id}.txt"
                with open(doc_path, "w", encoding="utf-8") as tf:
                    tf.write(doc)
                global_id += 1
    except Exception as exc:
        # Log but do not propagate exceptions from reference/text file saving
        import logging
        logging.error("Failed to persist reference or text files: %s", exc)

def _load_vector_store(case: str, model: str, metric: str) -> Optional[VectorStore]:
    """Load a vector store from disk.  Returns None if not found.

    This function reconstructs all shards and their associated documents.
    """
    dir_path = _get_store_dir(case, model, metric)
    meta_path = dir_path / "metadata.pkl"
    if not meta_path.exists():
        return None
    # Load metadata
    with open(meta_path, "rb") as f:
        data = pickle.load(f)
    store = VectorStore(data["embedding_model"], data["metric"])
    store.documents_shards = data.get("documents_shards", [])
    store.metadatas_shards = data.get("metadatas_shards", [])
    store._next_id = data.get("next_id", 0)
    # Load shard indices
    shard_files = sorted([p for p in dir_path.glob("index_*.faiss")])
    for path in shard_files:
        idx = faiss.read_index(str(path))
        # Move to GPU if requested
        if USE_GPU and faiss.get_num_gpus() > 0:
            try:
                res = faiss.StandardGpuResources()
                idx = faiss.index_cpu_to_gpu(res, 0, idx)
            except Exception:
                pass
        store.shards.append(idx)  # type: ignore[arg-type]
    return store

class IngestPayload(BaseModel):
    sources: List[str] = Field(..., description="List of URLs to scrape or open data pages")
    case: str = Field(..., description="Name of the case for per‑case storage and logging")
    embedding_model: Optional[str] = Field(None, description="Override embedding model for this ingestion")
    similarity: Optional[str] = Field(None, description="Override similarity metric (cosine, inner_product, l2)")

class QueryPayload(BaseModel):
    query: str = Field(..., description="Search query text")
    k: int = Field(5, description="Number of results to return")
    case: Optional[str] = Field(None, description="Limit search to this case")
    embedding_model: Optional[str] = Field(None, description="Embedding model used to encode documents")
    similarity: Optional[str] = Field(None, description="Similarity metric (cosine, inner_product, l2)")

@app.get("/healthz")
async def healthz() -> dict:
    """Simple health check endpoint."""
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> FastAPIResponse:
    """Expose Prometheus metrics for scraping."""
    return FastAPIResponse(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/ingest")
async def ingest(payload: IngestPayload) -> dict:
    """Ingest documents into the vector store.

    This endpoint accepts a JSON payload specifying a list of sources
    (web pages or open data portals) and optional overrides for the
    embedding model and similarity metric.  The documents are scraped,
    encoded and added to a per‑case vector store.  Returns the number
    of documents indexed and any errors encountered.
    """
    endpoint = "/ingest"
    method = "POST"
    start_time = datetime.datetime.now().timestamp()
    status = "200"
    case = payload.case
    logger = init_logger(case)
    logger.info(
        f"Ingestion request received: sources={len(payload.sources)}, case={case}, model={payload.embedding_model}, metric={payload.similarity}"
    )
    model_name = payload.embedding_model or 'sentence-transformers/all-MiniLM-L6-v2'
    metric = payload.similarity or 'cosine'
    key = (case, model_name, metric)
    # Retrieve or create the vector store.  First attempt to load from disk
    # so that indices persist across container restarts.
    if key not in VECTOR_STORES:
        loaded = _load_vector_store(case, model_name, metric)
        if loaded is not None:
            VECTOR_STORES[key] = loaded
        else:
            VECTOR_STORES[key] = VectorStore(model_name, metric)
    store = VECTOR_STORES[key]
    try:
        result = ingest_sources(payload.sources, case, store, logger)
    except Exception as exc:
        status = "500"
        logger.error("Ingestion failed: %s", exc)
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    # Persist the store to disk after ingestion
    try:
        _save_vector_store(case, model_name, metric, store)
    except Exception as exc:
        logger.error("Failed to persist vector store: %s", exc)
    logger.info(f"Ingested {result.count} documents; errors: {len(result.errors)}")
    # Record success metrics
    REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
    return {"docs_indexed": result.count, "errors": result.errors}

@app.post("/query")
async def query(payload: QueryPayload) -> List[dict]:
    """Query the vector store and return top‑k results.

    The caller can specify the case, embedding model and similarity
    metric.  If the specified combination of case, model and metric
    has not been ingested yet, a 404 is returned.
    """
    case_name = payload.case or 'base'
    model_name = payload.embedding_model or 'sentence-transformers/all-MiniLM-L6-v2'
    metric = payload.similarity or 'cosine'
    key = (case_name, model_name, metric)
    logger = init_logger(case_name)
    logger.info(
        f"Query request: q='{payload.query}', k={payload.k}, case={case_name}, model={model_name}, metric={metric}"
    )
    endpoint = "/query"
    method = "POST"
    start_time = datetime.datetime.now().timestamp()
    status = "200"
    store = VECTOR_STORES.get(key)
    # Attempt to load from disk if not present in memory
    if store is None:
        store = _load_vector_store(case_name, model_name, metric)
        if store is not None:
            VECTOR_STORES[key] = store
    if store is None:
        status = "404"
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
        raise HTTPException(status_code=404, detail="Case/model/metric combination not found")
    try:
        results = query_vector_store(store, payload.query, payload.k, case_name)
    except Exception as exc:
        status = "500"
        logger.error("Query failed: %s", exc)
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    logger.info(f"Returned {len(results)} results for query '{payload.query}'")
    # Record success metrics
    REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
    return results
