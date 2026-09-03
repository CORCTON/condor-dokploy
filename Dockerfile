FROM alpine/git:2.49.1 AS source

ARG CONDOR_REPOSITORY=https://github.com/hummingbot/condor.git
ARG CONDOR_REF=main

RUN git clone --depth 1 --branch "${CONDOR_REF}" "${CONDOR_REPOSITORY}" /src


FROM alpine/git:2.49.1 AS extensions

ARG EXTENSIONS_REPOSITORY=https://github.com/CORCTON/condor.git
ARG EXTENSIONS_REF=main

RUN git clone --depth 1 --branch "${EXTENSIONS_REF}" "${EXTENSIONS_REPOSITORY}" /extensions


FROM alpine/git:2.49.1 AS hummingbot-api-source

ARG HUMMINGBOT_API_REPOSITORY=https://github.com/hummingbot/hummingbot-api.git
ARG HUMMINGBOT_API_REF=main

RUN git clone --depth 1 --branch "${HUMMINGBOT_API_REF}" \
    "${HUMMINGBOT_API_REPOSITORY}" /hummingbot-api


FROM python:3.12-alpine AS hummingbot-init

RUN pip install --no-cache-dir pyyaml==6.0.3

COPY --from=hummingbot-api-source /hummingbot-api/bots /opt/hummingbot-bots
COPY init_hummingbot.py /opt/condor-deploy/init_hummingbot.py

ENTRYPOINT ["python", "/opt/condor-deploy/init_hummingbot.py"]


FROM source AS assembled

COPY --from=extensions \
    /extensions/agents/_shared/routines/crypto_news.py \
    /extensions/agents/_shared/routines/fred_macro_snapshot.py \
    /extensions/agents/_shared/routines/gate_io_derivatives_snapshot.py \
    /extensions/agents/_shared/routines/gate_io_spot_screener.py \
    /src/agents/_shared/routines/


FROM node:24-bookworm-slim AS frontend

WORKDIR /src/frontend
COPY --from=assembled /src/frontend/package.json /src/frontend/package-lock.json ./
RUN npm ci
COPY --from=assembled /src/frontend/ ./
RUN npm run build


FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS python-deps

ENV DEBIAN_FRONTEND=noninteractive \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=assembled /src/pyproject.toml /src/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project


FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS condor

ENV DEBIAN_FRONTEND=noninteractive \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-deps /app/.venv /app/.venv
COPY --from=assembled /src/ /app/
COPY --from=frontend /src/frontend/dist /app/frontend/dist
COPY entrypoint.sh sync_assets.py /opt/condor-deploy/

RUN uv sync --frozen --offline --no-dev \
    && mkdir -p /opt/condor-assets \
    && mv /app/agents /opt/condor-assets/agents \
    && mv /app/routines /opt/condor-assets/routines \
    && ln -s /state/agents /app/agents \
    && ln -s /state/routines /app/routines \
    && ln -s /state/config.yml /app/config.yml \
    && ln -s /state/audit_log.yml /app/audit_log.yml \
    && chmod 0755 /opt/condor-deploy/entrypoint.sh

EXPOSE 443

ENTRYPOINT ["/opt/condor-deploy/entrypoint.sh"]
CMD ["/app/.venv/bin/python", "main.py"]
