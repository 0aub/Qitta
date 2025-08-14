# Vector Store Service

This directory contains the implementation of the vector store.  The
service wraps a FAISS index and exposes endpoints for ingesting
documents and querying them for similarity.  It supports multiple cases
and allows callers to specify their own embedding model and similarity
metric.

## Running the vector store

The vector store runs automatically when you start the full stack via
`docker compose up`.  To run just the vector store for testing, run:

```bash
docker compose up vectorstore
```

FAISS indices, embedding caches and logs are persisted under the
`storage` directory.  You can configure the host and port for this
service via `VECTOR_HOST`, `VECTOR_PORT` and `VECTOR_HOST_PORT` in
`.env`.

## API endpoints

### `POST /ingest`

Ingest documents into the vector store.  The request body must
include:

```json
{
  "sources": ["https://example.com", "https://open.data.gov.sa"],
  "case": "mewa",
  "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
  "similarity": "cosine"
}
```

`sources` can be arbitrary web pages or Saudi open data portal URLs.
The service will scrape the content, compute embeddings and insert
them into a per‑case index.  The `embedding_model` and `similarity`
fields are optional; if omitted the service uses its own defaults.

Returns a JSON object with statistics about the ingestion.

### `POST /query`

Retrieve the most similar documents to a query.  Example request:

```json
{
  "query": "Statistics about Saudi agriculture",
  "k": 3,
  "case": "mewa",
  "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
  "similarity": "cosine"
}
```

The service returns a list of documents with their metadata and
similarity scores.

## Logging

All activity is logged to `storage/logs` with daily rotation.  Per‑case
logs are stored under `storage/logs/cases/<case>`.