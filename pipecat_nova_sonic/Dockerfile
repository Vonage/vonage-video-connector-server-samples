FROM python:3.13-slim-bookworm

# OS dependencies required by vonage-video-connector
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     libexpat1 \
     libglib2.0-0 \
     git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

ENTRYPOINT ["sh", "-c", "uv sync && uv run python pipecat_nova_sonic.py \"$@\"", "--"]
