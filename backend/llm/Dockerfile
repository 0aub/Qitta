FROM nvidia/cuda:12.6.1-runtime-ubuntu22.04

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt

COPY src ./src
RUN mkdir -p /models

ARG SERVICE_PORT=8000
ENV SERVICE_PORT=${SERVICE_PORT}

CMD ["sh","-c","uvicorn src.main:app --host 0.0.0.0 --port ${SERVICE_PORT}"]
