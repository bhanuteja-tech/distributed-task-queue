FROM python:3.12-slim

# Install system utilities for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency specifications first for layer caching
COPY pyproject.toml README.md ./

# Install application dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Copy source code and migrations
COPY alembic.ini ./
COPY alembic/ ./alembic/
COPY src/ ./src/

# Install the package in editable mode for clean imports
RUN pip install --no-cache-dir -e .

EXPOSE 8000

# Default command runs the API server
CMD ["uvicorn", "taskqueue.api.app:app", "--host", "0.0.0.0", "--port", "8000"]

