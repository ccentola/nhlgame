FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /opt/dagster/app

# Copy project files
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies
RUN uv sync --frozen

# Copy all source code
COPY . .

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME
COPY dagster.yaml $DAGSTER_HOME/
