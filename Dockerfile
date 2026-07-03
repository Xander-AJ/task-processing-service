FROM python:3.12-slim

WORKDIR /app

# Install runtime deps from the project metadata. Copy what pip needs to build
# the package first (better layer caching), then the rest of the source.
COPY pyproject.toml README.md ./
COPY app ./app
RUN pip install --no-cache-dir .

COPY alembic.ini ./
COPY alembic ./alembic

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
