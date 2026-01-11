# Chemistry Corpus Pipeline v1.1 - Docker Image
# Provides a reproducible environment for running the pipeline.
#
# Build:
#   docker build -t chem-pipeline:v1.1 .
#
# Run (dry-run):
#   docker run -v /data/chem:/data/chem chem-pipeline:v1.1 \
#       ./run_pipeline.sh --targets targets.yaml
#
# Run (execute):
#   docker run -v /data/chem:/data/chem chem-pipeline:v1.1 \
#       ./run_pipeline.sh --targets targets.yaml --execute

FROM python:3.11-slim

LABEL maintainer="Chemistry Corpus Pipeline"
LABEL version="1.1"
LABEL description="Ethical chemistry dataset collection pipeline with license compliance"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt requirements-lock.txt* ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make scripts executable
RUN chmod +x run_pipeline.sh

# Create data directories (will be overridden by volume mounts)
RUN mkdir -p /data/chem/pools/permissive \
    /data/chem/pools/copyleft \
    /data/chem/pools/quarantine \
    /data/chem/_staging \
    /data/chem/_manifests \
    /data/chem/_queues \
    /data/chem/_catalogs \
    /data/chem/_logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default command shows help
CMD ["./run_pipeline.sh", "--help"]
