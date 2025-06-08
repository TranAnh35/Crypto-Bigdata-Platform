FROM python:3.12 AS builder

ENV POETRY_VERSION=1.8.2
RUN pip install "poetry==$POETRY_VERSION"

ENV POETRY_VIRTUALENVS_IN_PROJECT=true

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-dev --only main

FROM python:3.12-slim AS final

WORKDIR /app

COPY --from=builder /app/.venv ./.venv

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/.venv/lib/python3.12/site-packages"

COPY src ./src
COPY test.py .

CMD ["python", "test.py"]