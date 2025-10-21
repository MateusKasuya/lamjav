# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements_dbt.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements_dbt.txt

# Copy dbt project files
COPY dbt_project.yml .
COPY profiles.yml .
COPY packages.yml .
COPY macros/ macros/
COPY models/ models/
COPY seeds/ seeds/

# Install dbt packages
RUN dbt deps

# Default command runs dbt
CMD ["dbt", "run"]

