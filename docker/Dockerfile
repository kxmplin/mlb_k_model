FROM python:3.11-slim

WORKDIR /app

# system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src
WORKDIR /app/src

ENV PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# default entrypoint
ENTRYPOINT ["python"]