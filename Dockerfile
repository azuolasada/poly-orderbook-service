# Use a specialized uv image for building
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

# Set the working directory
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy only the files needed for dependency installation
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev --no-install-project

# Final stage
FROM python:3.14-slim-bookworm

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Ensure the virtual environment is used
ENV PATH="/app/.venv/bin:$PATH"

# Copy the application code
COPY src/ /app/src/
COPY main.py /app/

# Run the application
CMD ["python", "main.py"]
