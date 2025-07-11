# ---- Base image
FROM python:3.11-slim

# ---- Working dir inside container
WORKDIR /app
ENV PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ---- Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ---- Default command (interactive shell)
CMD ["bash"]
