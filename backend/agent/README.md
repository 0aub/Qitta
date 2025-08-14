# Agent Service

This directory defines the agent orchestrator.  The Agent service
coordinates retrieval from the vector store and generation via the LLM
service (local) or external providers (remote).  It exposes a single
API endpoint that accepts a goal description, a user query and
optional configuration.

## Running the Agent service

The Agent service runs as part of the overall platform when using
`docker‑compose.yml`.  To run it in isolation for debugging, execute:

```bash
docker compose up agent
```

Ports and service endpoints are configured via environment variables
defined in the `.env` file.  The service reads the following
variables:

| Variable           | Purpose                                                |
| ------------------ | ------------------------------------------------------ |
| `VECTOR_ENDPOINT`  | URL of the vector store (defaults to `vectorstore`)    |
| `LLM_ENDPOINT`     | URL of the local LLM service                           |
| `DEFAULT_LOCAL_MODEL` | Fallback model used when none is specified           |
| `GEMINI_API_KEY`   | API key for Google Gemini (remote)                    |
| `OPENAI_API_KEY`   | API key for OpenAI (remote)                           |
| `ANTHROPIC_API_KEY`| API key for Anthropic (remote)                        |
| `GEMINI_API_BASE`  | Base URL for Gemini API                               |
| `OPENAI_API_BASE`  | Base URL for OpenAI API                               |
| `ANTHROPIC_API_BASE`| Base URL for Anthropic API                           |

## API endpoint

### `POST /run`

Runs the agent loop.  Example payload:

```json
{
  "goal": {
    "name": "MEWA Q&A",
    "description": "Answer questions about MEWA and its affiliates."
  },
  "user_input": "Give me three stats about Saudi agriculture.",
  "case": "mewa",
  "model": "gemini/gemini-1.5-pro",
  "api_base": null,
  "k": 3
}
```

The service will:

1. Query the vector store using the `user_input` to retrieve up to `k`
   relevant passages.
2. Construct a prompt incorporating the retrieved context.
3. Decide whether to route the request to a remote provider or the local
   LLM service:
   * If `api_base` is provided or the `model` string contains keywords
     like `gemini`, `openai`, `gpt`, `anthropic` or `claude`, the
     request is treated as **remote**.  The agent selects an API key and
     base URL based on the detected provider and calls the remote
     model via [`litellm`](https://docs.litellm.ai/docs/).
   * Otherwise the request is sent to the local LLM service (see
     `llm/README.md`).
4. Return the generated answer as JSON.

### Example usage

```bash
curl -X POST http://localhost:${AGENT_HOST_PORT:-8002}/run \
  -H "Content-Type: application/json" \
  -d '{
        "goal": {"name": "Echo", "description": "Repeat the question"},
        "user_input": "What is the capital of Saudi Arabia?",
        "case": "mewa",
        "model": "tiiuae/falcon-7b-instruct",
        "k": 2
      }'
```

## Notes

* Ensure that at least one of the API keys (`GEMINI_API_KEY`,
  `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`) is set when requesting a remote
  provider.
* The Agent service writes logs to `storage/logs` using daily file
  rotation.  Check those logs when debugging.
* For development, mount the source code and logs using
  `docker-compose.dev.yml`.