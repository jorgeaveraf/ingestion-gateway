FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ingestion_gateway ./ingestion_gateway
COPY README.md .

EXPOSE 8000

CMD ["uvicorn", "ingestion_gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
