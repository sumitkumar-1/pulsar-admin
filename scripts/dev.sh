#!/usr/bin/env bash
set -euo pipefail

trap 'kill 0' EXIT

docker compose up -d postgres
#docker run -it -p 6650:6650 -p 8081:8080 apachepulsar/pulsar:latest bin/pulsar standalone

(cd backend && mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 -pl shared,api,worker -am -DskipTests install) &
wait

(cd backend/api && mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 spring-boot:run) &
(cd backend/worker && mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 spring-boot:run) &
(cd frontend && npm start) &

wait
