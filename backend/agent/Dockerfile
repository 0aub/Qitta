FROM python:3.11-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /app/src
WORKDIR /app

ARG SERVICE_PORT=8000
ENV SERVICE_PORT=${SERVICE_PORT}

CMD ["sh","-c","uvicorn src.main:app --host 0.0.0.0 --port ${SERVICE_PORT}"]
