# LLM Service

This directory contains the implementation and build configuration for the
language model (LLM) service.  The service provides a simple HTTP API
that can dynamically load HuggingFace models on demand and generate
responses to prompts.  It does **not** require any API keys to run
locally—models are downloaded and cached automatically when first
requested.

## Running the LLM service

The service is packaged as a Docker image and is orchestrated via
`docker‑compose.yml`.  To launch only this service for development you
can run:

```bash
docker compose up llm
```

By default the container exposes port `${LLM_PORT:-8000}` internally and
`${LLM_HOST_PORT:-8001}` externally.  You can adjust these values in
the `.env` file.

## API endpoints

### `POST /load_model`

Load a model into memory.  The body must specify a `model_id`, e.g.:

```json
{
  "model_id": "tiiuae/falcon-7b-instruct"
}
```

If the model is already present in the cache it will be loaded from
disk.  Otherwise it will be downloaded from HuggingFace and cached in
the `storage/models_cache` directory.

### `POST /generate`

Generate text using a loaded model.  The body must include:

```json
{
  "model_id": "tiiuae/falcon-7b-instruct",
  "prompt": "Hello, world",
  "max_tokens": 128,
  "temperature": 0.7
}
```

If the specified `model_id` has not been loaded previously the service
will attempt to download and cache it automatically.  Generation is
performed on the CPU by default; attach a GPU to the container if you
need faster inference.

## Using remote LLMs

The LLM service does not handle remote providers such as Google Gemini
or OpenAI directly.  Instead, remote models are invoked through the
Agent service using [`litellm`](https://docs.litellm.ai/docs/).  To
configure access to remote providers set the appropriate API keys in
the `.env` file (e.g. `GEMINI_API_KEY`, `OPENAI_API_KEY`) and call
the Agent service with a model identifier that matches the provider
(e.g. `gemini/gemini-1.0-pro`).