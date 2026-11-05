FROM ghcr.io/astral-sh/uv:0.13.6 AS uv

FROM python:3.13-alpine3.23 AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-alpine3.23 AS runtime
RUN apk add --no-cache pgbouncer postgresql18-client curl ca-certificates \
    && addgroup -g 10001 foxtrot \
    && adduser -D -u 10001 -G foxtrot foxtrot \
    && mkdir -p /run/foxtrot \
    && chown 10001:10001 /run/foxtrot \
    && chmod 0700 /run/foxtrot
COPY --from=build --chown=10001:10001 /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
USER 10001:10001
VOLUME ["/run/foxtrot", "/tmp"]
EXPOSE 6432 8080
ENTRYPOINT ["foxtrot-db-runtime"]
CMD ["backend"]
