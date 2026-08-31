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

# Create a non-root user to run the app as
RUN useradd --create-home --shell /usr/sbin/nologin appuser

WORKDIR /app
RUN chown appuser:appuser /app

# Copy the virtual environment from the builder stage
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv

# Ensure the virtual environment is used
ENV PATH="/app/.venv/bin:$PATH"

# Copy the application code
COPY --chown=appuser:appuser src/ /app/src/
COPY --chown=appuser:appuser main.py /app/

USER appuser

# Liveness check: the app touches /tmp/heartbeat on every heartbeat interval (30s);
# unhealthy if it goes stale, which catches a hung/deadlocked event loop even
# without a crash (no HTTP endpoint to probe otherwise).
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import os,sys,time; p='/tmp/heartbeat'; sys.exit(0 if os.path.exists(p) and time.time() - os.path.getmtime(p) < 90 else 1)"

# Run the application
CMD ["python", "main.py"]
