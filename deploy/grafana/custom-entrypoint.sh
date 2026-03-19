#!/bin/sh
set -eu

if [ -n "${GRAFANA_ADMIN_PASSWORD:-}" ] && [ -f /var/lib/grafana/grafana.db ]; then
  echo "Synchronizing Grafana admin password from environment"
  grafana cli --homepath /usr/share/grafana admin reset-admin-password "${GRAFANA_ADMIN_PASSWORD}" || \
    echo "Grafana admin password sync failed; continuing with existing credentials"
fi

/run.sh &
grafana_pid=$!

if [ -n "${GRAFANA_ADMIN_USER:-}" ] && [ -n "${GRAFANA_ADMIN_PASSWORD:-}" ]; then
  attempts=0
  until curl -fsS http://127.0.0.1:3000/api/health >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "${attempts}" -ge 30 ]; then
      echo "Grafana HTTP endpoint did not become ready for admin sync"
      break
    fi
    sleep 1
  done

  if curl -fsS -u "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
    http://127.0.0.1:3000/api/user >/dev/null 2>&1; then
    echo "Grafana admin user already matches configured login"
  elif curl -fsS -u "admin:${GRAFANA_ADMIN_PASSWORD}" \
    http://127.0.0.1:3000/api/user >/dev/null 2>&1; then
    payload=$(printf '{"login":"%s","email":"admin@localhost","name":"%s"}' \
      "${GRAFANA_ADMIN_USER}" "${GRAFANA_ADMIN_USER}")
    if curl -fsS -u "admin:${GRAFANA_ADMIN_PASSWORD}" \
      -H "Content-Type: application/json" \
      -X PUT \
      -d "${payload}" \
      http://127.0.0.1:3000/api/users/1 >/dev/null 2>&1; then
      echo "Synchronized Grafana admin login from environment"
    else
      echo "Grafana admin login sync failed; continuing with existing login"
    fi
  else
    echo "Grafana admin login sync skipped; current login is neither admin nor configured login"
  fi
fi

wait "${grafana_pid}"
