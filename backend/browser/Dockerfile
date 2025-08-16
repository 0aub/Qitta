FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

# 1 ▸ virtual X server for headed mode
RUN apt-get update && apt-get install -y xvfb

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /app/src
WORKDIR /app

ARG SERVICE_PORT=8000
ENV SERVICE_PORT=${SERVICE_PORT}

EXPOSE ${SERVICE_PORT}

# 2 ▸ run uvicorn under xvfb; respect $BROWSER_HEADLESS
CMD ["sh", "-c", "xvfb-run -a uvicorn src.main:app --host 0.0.0.0 --port $SERVICE_PORT"]
