"""
Agent service
=============

This module implements a lightweight agent orchestrator.  It exposes a
``/run`` endpoint that accepts a goal description, the user's question and
optional parameters.  When invoked, the service performs the following
steps:

1. Query the vector store for relevant passages using the provided
   ``user_input``.  The number of passages returned and the embedding
   configuration can be controlled via the request payload.
2. Construct a prompt that embeds the retrieved context and the user's
   question.
3. Generate an answer either by calling a local model via the LLM
   service or by calling a remote provider through ``litellm``.  If
   ``api_base`` or ``model`` indicates a known remote provider (e.g. a
   Google Gemini model), the request is routed through ``litellm`` using
   the API key provided in the environment.  Otherwise the request is
   forwarded to the local LLM service.

This service does not hard‑code any model identifiers; callers must
specify which model to use.  Only secret tokens (e.g. for Gemini) are
read from the environment via ``LITELLM_API_KEY``.

All logs are written to daily‑rotated files under ``/logs`` as well as
emitted to the console.
"""

from __future__ import annotations

import datetime
import logging
import os
import pathlib
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException, Response as FastAPIResponse
from pydantic import BaseModel, Field

import json
import litellm

# Prometheus metrics
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Define Prometheus metrics for the agent service
REQUEST_COUNT = Counter(
    "agent_request_count",
    "Number of requests received",
    labelnames=["endpoint", "method", "status"],
)
REQUEST_LATENCY = Histogram(
    "agent_request_latency_seconds",
    "Request latency in seconds",
    labelnames=["endpoint"],
)

# Configuration defaults
# -------------------------------------------------------------------------
# Dynamic service discovery
# -------------------------------------------------------------------------
# Build the URLs for the vector‑store and LLM services from host + port
# environment variables.  docker‑compose injects VECTOR_PORT and LLM_PORT,
# but you can still override the full URL via VECTOR_ENDPOINT / LLM_ENDPOINT
# if you really need to.

VECTOR_HOST: str = os.getenv("VECTOR_HOST", "vectorstore")
LLM_HOST: str = os.getenv("LLM_HOST", "llm")

VECTOR_PORT: str = os.getenv("VECTOR_PORT", "8000")
LLM_PORT: str = os.getenv("LLM_PORT", "8000")

VECTOR_ENDPOINT: str = os.getenv("VECTOR_ENDPOINT", f"http://{VECTOR_HOST}:{VECTOR_PORT}",)
LLM_ENDPOINT: str = os.getenv("LLM_ENDPOINT", f"http://{LLM_HOST}:{LLM_PORT}",)

# Provider API keys and defaults.  These environment variables must be
# configured when using remote models.  See ``.env`` for details.
GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY: Optional[str] = os.getenv("ANTHROPIC_API_KEY")

def init_logger(case: Optional[str] = None) -> logging.Logger:
    """Create or retrieve a logger configured for the given case.

    Logs are written to ``LOG_ROOT/base/agent/YYYY-MM-DD.log`` and, if a
    case is provided, also to ``LOG_ROOT/cases/<case>/YYYY-MM-DD.log``.
    All logs are emitted to the console as well.  A unique logger name
    is derived from the case to avoid duplicate handlers.  ``LOG_ROOT``
    can be set via an environment variable; it defaults to
    ``/storage/logs``.
    """
    today = datetime.date.today().isoformat()
    log_root = pathlib.Path(os.getenv("LOG_ROOT", "/storage/logs"))
    handlers: list[logging.Handler] = []
    # Base log directory for the agent service
    base_dir = log_root / "base" / "agent"
    base_dir.mkdir(parents=True, exist_ok=True)
    handlers.append(logging.FileHandler(base_dir / f"{today}.log"))
    # Case specific directory (shared across services)
    if case:
        case_dir = log_root / "cases" / case
        case_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(case_dir / f"{today}.log"))
    # Console output
    handlers.append(logging.StreamHandler())
    logger = logging.getLogger(f"agent[{case or 'base'}]")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        formatter = logging.Formatter(fmt)
        for h in handlers:
            h.setFormatter(formatter)
            logger.addHandler(h)
    return logger


def query_kb(client: httpx.Client, query: str, k: int = 3, case: Optional[str] = None, embedding_model: Optional[str] = None, similarity: Optional[str] = None) -> str:
    """Query the vector store and return a single string of concatenated passages.

    Parameters beyond the query are optional and allow the caller to control
    the number of returned documents and the embedding configuration.  The
    function will raise an exception if the vector store is unreachable or
    returns an invalid response.
    """
    payload: Dict[str, Any] = {"query": query, "k": k}
    if case:
        payload["case"] = case
    if embedding_model:
        payload["embedding_model"] = embedding_model
    if similarity:
        payload["similarity"] = similarity
    try:
        response = client.post(f"{VECTOR_ENDPOINT}/query", json=payload, timeout=120)
        response.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"Failed to query vector store: {exc}") from exc
    data = response.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Invalid response from vector store: {data}")
    passages: list[str] = []
    for item in data:
        text = item.get("text") or item.get("document") or item.get("content")
        if text:
            passages.append(text)
    return "\n\n".join(passages)


app = FastAPI(title="Agentic Platform Agent Service")

# Initialise the base logger at import time.  This ensures that
# the ``/storage/logs/base/agent`` directory exists before requests are
# processed.
init_logger()


class GoalModel(BaseModel):
    name: str
    description: str


class RunPayload(BaseModel):
    goal: GoalModel = Field(..., description="Goal description for the agent")
    user_input: str = Field(..., description="The user's question or instruction")
    case: Optional[str] = Field(None, description="Name of the case for logging and retrieval")
    debug: bool = Field(False, description="Enable verbose debug logging")
    model: Optional[str] = Field(
        None,
        description=(
            "Identifier of the language model to use.  This value is passed directly to the"
            " local LLM service (`/generate`) or, if `api_base` is set or the model appears"
            " to be a remote provider (e.g. 'gemini/...'), to the remote provider via"
            " litellm."
        ),
    )
    api_base: Optional[str] = Field(
        None,
        description="Base URL for the remote language model API.  When set, generation is routed"
        " through litellm and `model` should refer to a remote provider's model ID."
    )
    k: Optional[int] = Field(
        3,
        description="Number of search results to retrieve from the vector store.  Defaults to 3."
    )
    embedding_model: Optional[str] = Field(
        None,
        description="Embedding model name to use when querying the vector store.  If omitted,"
        " the vector store will use its own default."
    )
    similarity: Optional[str] = Field(
        None,
        description="Similarity metric (cosine, inner_product, l2) to use when querying the vector store."
    )


@app.get("/healthz")
async def healthz() -> dict:
    """Return 200 OK when the service is healthy."""
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> FastAPIResponse:
    """Expose Prometheus metrics for scraping."""
    return FastAPIResponse(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/run")
async def run_agent(payload: RunPayload) -> Dict[str, Any]:
    """Handle an agent run request.

    A new agent is created for each request using the supplied goal and
    language model parameters.  The query_kb tool is registered to
    perform knowledge base lookups via the vector store.  Errors are
    surfaced as HTTP 500 responses with the original exception message.
    """
    endpoint = "/run"
    method = "POST"
    start_time = datetime.datetime.now().timestamp()
    status = "200"
    case = payload.case or "base"
    logger = init_logger(case)
    logger.info(
        f"Agent run request: goal={payload.goal.name}, user_input={payload.user_input[:80]}, case={case}, debug={payload.debug}"
    )
    # Step 1: Retrieve relevant passages from the vector store
    try:
        with httpx.Client() as client:
            context = query_kb(
                client,
                query=payload.user_input,
                k=payload.k or 3,
                case=payload.case,
                embedding_model=payload.embedding_model,
                similarity=payload.similarity,
            )
    except Exception as exc:
        logger.error("Vector store query failed: %s", exc)
        status = "500"
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # Step 2: Build a prompt combining the context and the question.  We use
    # a simple instruction to ground the LLM.
    prompt_parts = []
    if context.strip():
        prompt_parts.append(
            "The following passages were retrieved from the knowledge base and may be useful "
            "for answering the question."
        )
        prompt_parts.append(context)
    prompt_parts.append("Question: " + payload.user_input.strip())
    prompt_parts.append("Answer:")
    prompt = "\n\n".join(prompt_parts)

    # Step 3: Determine whether to use the local LLM service or a remote provider.
    # If an ``api_base`` is provided or the model identifier contains a known
    # provider keyword (e.g. gemini, gpt, openai, anthropic, claude) then the
    # request is considered remote.  Otherwise it will be handled by the local
    # LLM service.  The default local model can be overridden via the
    model_id = payload.model 
    api_base: Optional[str] = payload.api_base
    answer: str

    model_lower = model_id.lower()
    # Identify provider based on model_id.  We do not force a default API base;
    # litellm will use sensible defaults for known providers.  The caller can
    # override with api_base if required.
    provider: Optional[str] = None
    if any(kw in model_lower for kw in ["gemini"]):
        provider = "gemini"
    elif any(kw in model_lower for kw in ["openai", "gpt"]):
        provider = "openai"
    elif any(kw in model_lower for kw in ["anthropic", "claude"]):
        provider = "anthropic"
    # If api_base is provided explicitly we still treat the call as remote but
    # leave provider detection up to the caller
    use_remote = bool(api_base) or provider is not None

    if use_remote:
        # Choose API key and base URL based on provider
        # Choose API key based on provider
        if provider == "gemini":
            api_key = GEMINI_API_KEY
        elif provider == "openai":
            api_key = OPENAI_API_KEY
        elif provider == "anthropic":
            api_key = ANTHROPIC_API_KEY
        else:
            # Fallback: try any provided key
            api_key = GEMINI_API_KEY or OPENAI_API_KEY or ANTHROPIC_API_KEY
        base = api_base  # Use provided api_base if any; otherwise litellm default
        if not api_key:
            status = "500"
            REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
            REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
            raise HTTPException(status_code=500, detail="API key must be provided for remote models")
        try:
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ]
            completion = litellm.completion(
                model=model_id,
                messages=messages,
                api_base=base,
                api_key=api_key,
            )
            answer = completion["choices"][0]["message"]["content"].strip()
        except Exception as exc:
            logger.error("Remote generation failed: %s", exc)
            status = "500"
            REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
            REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    else:
        # Call local LLM service.  Perform a health check first.
        # Ensure that a model is provided when using the local LLM.  There is no default.
        if model_id is None:
            status = "400"
            REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
            REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
            raise HTTPException(status_code=400, detail="A model_id must be provided when using the local LLM")
        # Health check the local LLM
        try:
            httpx.get(f"{LLM_ENDPOINT}/healthz", timeout=10).raise_for_status()
        except Exception as exc:
            logger.error("Local LLM health check failed: %s", exc)
            status = "502"
            REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
            REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
            raise HTTPException(status_code=502, detail=f"Local LLM unreachable: {exc}") from exc
        # Generate using the local LLM
        try:
            url = f"{LLM_ENDPOINT}/generate"
            payload_body = {
                "model_id": model_id,
                "prompt": prompt,
                "max_tokens": 256,
                "temperature": 0.2,
            }
            logger.info("Calling local LLM %s", model_id)
            res = httpx.post(url, json=payload_body, timeout=300)
            res.raise_for_status()
            answer = res.json().get("response", "").strip()
        except Exception as exc:
            logger.error("Local LLM generation failed: %s", exc)
            status = "500"
            REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
            REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    logger.info(f"Generated answer of length {len(answer)}")
    # Record success metrics
    REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
    return {"answer": answer}
