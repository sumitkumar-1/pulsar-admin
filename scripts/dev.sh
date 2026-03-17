#!/usr/bin/env bash
set -euo pipefail

trap 'kill 0' EXIT

docker compose up -d postgres

(cd backend && mvn -pl shared,api,worker -am -DskipTests install) &
wait

(cd backend/api && mvn spring-boot:run) &
(cd backend/worker && mvn spring-boot:run) &
(cd frontend && npm start) &

wait
