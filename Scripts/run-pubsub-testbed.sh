#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ID="${PUBSUB_TEST_PROJECT_ID:-${PUBSUB_PROJECT_ID:-google-cloud-swift-testbed}}"
HOST_PORT="${PUBSUB_TESTBED_HOST_PORT:-127.0.0.1:8085}"
HOST="${HOST_PORT%:*}"
PORT="${HOST_PORT##*:}"
DATA_DIR="${PUBSUB_TESTBED_DATA_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pubsub-testbed.XXXXXX")}"
LOG_FILE="${PUBSUB_TESTBED_LOG_FILE:-$DATA_DIR/emulator.log}"
DOCKER_IMAGE="${PUBSUB_TESTBED_DOCKER_IMAGE:-gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators}"

if [[ "${PUBSUB_TESTBED_ALLOW_REAL:-}" == "1" ]]; then
  export PUBSUB_TEST_PROJECT_ID="$PROJECT_ID"
  exec swift run --package-path "$ROOT_DIR" PubSubTestbed "$@"
fi

if [[ -n "${PUBSUB_EMULATOR_HOST:-}" ]]; then
  export PUBSUB_PROJECT_ID="$PROJECT_ID"
  exec swift run --package-path "$ROOT_DIR" PubSubTestbed "$@"
fi

stop_process_tree() {
  local pid="$1"
  local child
  local children
  children="$(pgrep -P "$pid" 2>/dev/null || true)"
  for child in $children; do
    stop_process_tree "$child"
  done
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
}

cleanup() {
  local status=$?
  if [[ -n "${EMULATOR_CONTAINER:-}" ]]; then
    docker rm -f "$EMULATOR_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [[ -n "${EMULATOR_PID:-}" ]]; then
    stop_process_tree "$EMULATOR_PID"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

has_working_java() {
  command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1
}

has_working_docker() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

wait_for_emulator() {
  # Require a completed HTTP exchange, not just a TCP connect: in the Docker
  # path, docker-proxy accepts connections on the published port as soon as
  # the container starts, long before the emulator inside is serving.
  for _ in $(seq 1 240); do
    if [[ -n "${EMULATOR_PID:-}" ]] && ! kill -0 "$EMULATOR_PID" >/dev/null 2>&1; then
      cat "$LOG_FILE" >&2 || true
      return 1
    fi
    if curl -s -o /dev/null --max-time 1 "http://$HOST:$PORT/"; then
      return 0
    fi
    sleep 0.25
  done

  cat "$LOG_FILE" >&2 || true
  echo "Pub/Sub emulator did not start on $HOST_PORT" >&2
  return 1
}

start_gcloud_emulator() {
  gcloud beta emulators pubsub start \
    --project="$PROJECT_ID" \
    --host-port="$HOST_PORT" \
    --data-dir="$DATA_DIR" \
    >"$LOG_FILE" 2>&1 &
  EMULATOR_PID=$!
}

start_docker_emulator() {
  EMULATOR_CONTAINER="pubsub-testbed-${PROJECT_ID//[^A-Za-z0-9_.-]/-}-$$"
  docker pull "$DOCKER_IMAGE" >"$LOG_FILE" 2>&1
  docker run --rm \
    --name "$EMULATOR_CONTAINER" \
    -p "$HOST_PORT:8085" \
    "$DOCKER_IMAGE" \
    gcloud beta emulators pubsub start \
      --project="$PROJECT_ID" \
      --host-port=0.0.0.0:8085 \
    >>"$LOG_FILE" 2>&1 &
  EMULATOR_PID=$!
}

stop_emulator_process() {
  if [[ -n "${EMULATOR_PID:-}" ]]; then
    stop_process_tree "$EMULATOR_PID"
    unset EMULATOR_PID
  fi
}

if curl -s -o /dev/null --max-time 1 "http://$HOST:$PORT/"; then
  echo "Refusing to start: an HTTP service is already responding on $HOST_PORT." >&2
  echo "Stop the existing process or set PUBSUB_TESTBED_HOST_PORT to an unused port." >&2
  exit 1
fi

if command -v gcloud >/dev/null 2>&1 && has_working_java; then
  start_gcloud_emulator
  if ! wait_for_emulator; then
    stop_emulator_process
    if has_working_docker; then
      echo "Local Pub/Sub emulator failed to start; falling back to Docker." >&2
      start_docker_emulator
      wait_for_emulator
    else
      cat >&2 <<'EOF'
The local Pub/Sub emulator failed to start and Docker is unavailable.

Install the gcloud pubsub-emulator component, install Docker, or start an
emulator yourself and export PUBSUB_EMULATOR_HOST.
EOF
      exit 127
    fi
  fi
elif has_working_docker; then
  start_docker_emulator
  wait_for_emulator
else
  cat >&2 <<'EOF'
Unable to auto-start the Pub/Sub emulator.

Install Google Cloud CLI plus a Java runtime, install Docker, or start an
emulator yourself and export PUBSUB_EMULATOR_HOST.
EOF
  exit 127
fi

export PUBSUB_EMULATOR_HOST="$HOST_PORT"
export PUBSUB_PROJECT_ID="$PROJECT_ID"

swift run --package-path "$ROOT_DIR" PubSubTestbed "$@"
