"""
Dynamic LLM service
===================

This module defines a FastAPI application that can load and serve multiple
language models on demand.  Models are downloaded from HuggingFace (or any
repository compatible with `transformers`) the first time they are requested.
Downloaded models are stored on a shared `/models` volume so they persist
between container restarts.  If the model already exists on disk it is
loaded directly from the local cache.

Two endpoints are exposed:

  * `POST /load_model` — explicitly preload a model by identifier.  This
    endpoint returns immediately once the model is loaded into memory.
  * `POST /generate` — generate a completion given a prompt.  The request
    must specify a `model_id` and a `prompt`.  Optional fields such as
    `max_tokens` and `temperature` control the generation behaviour.  If the
    specified model has not yet been loaded into memory, the service will
    attempt to load it automatically before generation.

Notes
-----

* At present, models are loaded into CPU memory.  Loading large models may
  consume significant RAM.  You can attach a GPU to this container at
  deployment time and modify the `device` logic in `_load_model` to make
  use of it.
* This service does not implement streaming responses.  For large outputs
  consider splitting requests into smaller chunks.

"""

import os
import logging
import time
import datetime
from pathlib import Path
from typing import Dict, Tuple
from tqdm.auto import tqdm


from fastapi import FastAPI, HTTPException, Response as FastAPIResponse
from pydantic import BaseModel, Field
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    GenerationConfig,
)
from huggingface_hub import snapshot_download, logging as hf_logging

import torch
# Prometheus metrics
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Define Prometheus metrics for this service
REQUEST_COUNT = Counter(
    "llm_request_count",
    "Number of requests received",
    labelnames=["endpoint", "method", "status"],
)
REQUEST_LATENCY = Histogram(
    "llm_request_latency_seconds",
    "Request latency in seconds",
    labelnames=["endpoint"],
)

NEEDED_PATTERNS = [
    "model-*.safetensors",
    "model.safetensors.index.json",
    "config.json", "generation_config.json",
    "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json",
    "*.py",
]

# Configure logging for the LLM service.  Logs are written to a
# daily‑rotated file under ``LOG_ROOT/base/llm`` as well as to the
# console.  The log root defaults to ``/storage/logs`` but can be
# overridden via the ``LOG_ROOT`` environment variable.

logger = logging.getLogger("llm")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    today = datetime.date.today().isoformat()
    root = Path(os.getenv("LOG_ROOT", "/storage/logs"))
    base_dir = root / "base" / "llm"
    base_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(base_dir / f"{today}.log")
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

# Verbose transformer / HF-hub logs
hf_logging.set_verbosity_info()
os.environ["TRANSFORMERS_VERBOSITY"] = "info"
# Always show progress bars (even in non-TTY Docker logs)
os.environ.pop("HF_HUB_DISABLE_PROGRESS_BARS", None)

# Global cache of loaded models.  Keys are the original model identifiers
# supplied by the user.  Values are (model, tokenizer) tuples.
_MODELS: Dict[str, Tuple[AutoModelForCausalLM, AutoTokenizer]] = {}

# Directory where models are persisted on disk.  This path is mounted
# externally via docker-compose volumes.
MODEL_ROOT = Path("/models")
MODEL_ROOT.mkdir(parents=True, exist_ok=True)


class LoadModelRequest(BaseModel):
    """Body for the /load_model endpoint."""
    model_id: str = Field(..., description="HuggingFace model identifier or local path")


class GenerateRequest(BaseModel):
    """Body for the /generate endpoint."""
    model_id: str = Field(..., description="Identifier of the model to use")
    prompt: str = Field(..., description="Input text to generate from")
    max_tokens: int = Field(256, ge=1, le=2048, description="Maximum new tokens to generate")
    temperature: float = Field(0.7, ge=0.0, le=2.0, description="Sampling temperature")


def _slugify(model_id: str) -> str:
    """Sanitise a model identifier into a filesystem-friendly name."""
    return model_id.replace("/", "__")


def _load_model(model_id: str) -> Tuple[AutoModelForCausalLM, AutoTokenizer]:
    """
    Load a model and tokenizer from disk or Hugging Face hub, with noisy
    logging and a live progress bar on first download.

    * If the model is already in the in-memory cache, return it immediately.
    * Otherwise look for a cached copy under /models/<model_id-slug>.
    * If it is not cached, stream the repository with snapshot_download(),
      which shows a tqdm progress bar in the container logs.
    * Finally load the model + tokenizer into memory, move to GPU if
      available, cache them in _MODELS, and return the pair.
    """
    # 1 — return fast if we have it in RAM
    if model_id in _MODELS:
        return _MODELS[model_id]

    safe_name  = _slugify(model_id)
    target_dir = MODEL_ROOT / safe_name
    t0 = time.time()  # measure elapsed time

    # 2 — ensure the model files exist locally
    if target_dir.exists():
        logger.info("📦 Found cached model in %s — loading…", target_dir)
    else:
        logger.info("⬇️  Downloading model '%s' from Hugging Face hub", model_id)
        snapshot_download(
            repo_id=model_id,
            local_dir=target_dir,
            allow_patterns=NEEDED_PATTERNS, # <-- the filter
            resume_download=True, # keep broken downloads resumable
            tqdm_class=tqdm, # progress bar
        )

        logger.info("✅ Download finished in %.1fs", time.time() - t0)

    # 3 — load into memory
    logger.info("🔄 Deserialising model & tokenizer…")
    model = AutoModelForCausalLM.from_pretrained(
        target_dir,
        torch_dtype=torch.float16 if torch.cuda.is_available() else None,
    )
    tokenizer = AutoTokenizer.from_pretrained(target_dir)
    logger.info("🧩 Model in memory after %.1fs total", time.time() - t0)

    # 4 — ready the model
    model.eval()
    if torch.cuda.is_available():
        model.to("cuda")
        logger.info("⚡ Model moved to GPU")

    # 5 — cache and return
    _MODELS[model_id] = (model, tokenizer)
    return model, tokenizer


def _generate_text(model: AutoModelForCausalLM, tokenizer: AutoTokenizer, prompt: str, max_tokens: int, temperature: float) -> str:
    """Generate a response from a model given a prompt and parameters."""
    inputs = tokenizer(prompt, return_tensors="pt")
    if torch.cuda.is_available():
        inputs = {k: v.to('cuda') for k, v in inputs.items()}
    # `max_new_tokens` controls the length of the generated text
    generation_config = GenerationConfig(
        max_new_tokens=max_tokens,
        temperature=temperature,
        do_sample=temperature > 0,
        top_p=0.95,
        repetition_penalty=1.1,
    )
    output_ids = model.generate(**inputs, generation_config=generation_config)
    output_text = tokenizer.decode(output_ids[0], skip_special_tokens=True)
    return output_text


app = FastAPI()


@app.get("/healthz")
def healthz():
    """Liveness probe for container orchestration."""
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    """Expose Prometheus metrics for scraping."""
    return FastAPIResponse(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/load_model")
def load_model(request: LoadModelRequest):
    """Explicitly load a model into memory.

    Loading can be an expensive operation, so users may choose to call this
    endpoint ahead of time.  If the model is already loaded, this call is
    a no‑op.  Returns a confirmation message on success.
    """
    endpoint = "/load_model"
    method = "POST"
    start_time = datetime.datetime.now().timestamp()
    status = "200"
    try:
        _load_model(request.model_id)
    except Exception as exc:
        logger.exception("Failed to load model %s", request.model_id)
        status = "500"
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
    return {"status": "loaded", "model_id": request.model_id}


@app.post("/generate")
def generate(request: GenerateRequest):
    """Generate text given a prompt using a specified model.

    If the model has not been loaded previously, it is loaded from the
    persistent cache or downloaded.  Returns the full generated text.
    """
    endpoint = "/generate"
    method = "POST"
    start_time = datetime.datetime.now().timestamp()
    status = "200"
    try:
        model, tokenizer = _load_model(request.model_id)
        output_text = _generate_text(model, tokenizer, request.prompt, request.max_tokens, request.temperature)
    except Exception as exc:
        logger.exception("Generation failed with model %s", request.model_id)
        status = "500"
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=status).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(datetime.datetime.now().timestamp() - start_time)
    return {"response": output_text}