FROM mcr.microsoft.com/devcontainers/python:3.9

# Drop the stale Yarn apt source that breaks apt-get update on trixie
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    curl \
    netcat-openbsd \
    kcat \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*