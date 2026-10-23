#!/bin/sh
# Rootless Podman launcher; prefer the user-installed Compose provider when present.
set -eu
cd "$(dirname "$0")/.."
if [ -z "${PODMAN_COMPOSE_PROVIDER:-}" ] && [ -x "$HOME/.local/bin/podman-compose" ]; then
    export PODMAN_COMPOSE_PROVIDER="$HOME/.local/bin/podman-compose"
fi
exec podman compose "$@"
