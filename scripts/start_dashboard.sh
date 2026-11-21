#!/usr/bin/env bash
# Start the Streamlit dashboard (spec T09) in the background and print its link.
# Last stage of scripts/run_all.sh (it reads the outputs of all earlier stages); also usable on its own:
#   bash scripts/start_dashboard.sh          # PORT=8501 by default
# Installs requirements-dashboard.txt if Streamlit is missing. An instance already serving the port is
# kept: the app re-reads the files on every page load, so it shows new outputs without a restart.
set -uo pipefail
cd "$(dirname "$0")/.."
PORT=${PORT:-8501}
PY=${PYTHON:-python3}
LOG=data/local/streamlit.log

if [ -n "${CODESPACE_NAME:-}" ]; then
  URL="https://${CODESPACE_NAME}-${PORT}.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}"
else
  URL="http://localhost:${PORT}"
fi
up() { [ "$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${PORT}")" = 200 ]; }

if up; then
  echo "dashboard already running on port ${PORT}"
else
  $PY -c "import streamlit" 2>/dev/null || $PY -m pip install -q -r requirements-dashboard.txt || exit 1
  mkdir -p data/local
  nohup $PY -m streamlit run src/visualisation/app.py --server.headless true --server.port "$PORT" >"$LOG" 2>&1 &
  echo "started dashboard (pid $!), log $LOG"
  for i in $(seq 1 30); do up && break; sleep 1; done
  up || { echo "dashboard did not answer on port ${PORT}; see $LOG"; tail -20 "$LOG"; exit 1; }
fi
echo "dashboard: $URL"
[ -n "${CODESPACE_NAME:-}" ] && echo "(private by default: open it signed in to GitHub, or set port ${PORT} to Public in the Ports tab)"
exit 0
