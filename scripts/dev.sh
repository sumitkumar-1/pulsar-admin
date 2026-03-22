#!/usr/bin/env bash
set -euo pipefail

trap 'kill 0' EXIT

docker compose up -d postgres pulsar

(cd backend && mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 -pl shared,api,worker -am -DskipTests install) &
wait

(cd backend/api && mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 spring-boot:run) &
(cd backend/worker && mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 spring-boot:run) &
(cd frontend && npm start) &

wait
