# Pulsar Admin Console

Operator-focused Pulsar admin console for day-to-day platform workflows with both safe mock mode and live REST mode.

## Overview

This repository includes:

- Angular frontend (`frontend/`)
- Spring Boot API (`backend/api`)
- Spring Boot worker for async jobs (`backend/worker`)
- Shared backend models (`backend/shared`)
- PostgreSQL bootstrap schema (`backend/db/init`)
- Local Docker setup for PostgreSQL and Pulsar standalone (`docker-compose.yml`)

The project is designed for:

- environment lifecycle management (connect, test, sync)
- topic and subscription operations
- test publish/consume workflows
- long-running replay/retry/search jobs with progress and timeline events
- optional safe mock mode for local demos and development

## Key Features

- Environment management
  - Add/edit/delete environments
  - `Test + Sync` and `Sync again`
  - Per-environment auto-refresh preference (user-controlled)
- Topic explorer and details
  - Health, backlog, rates, partitions, schema summary
- Topic operations
  - Peek messages
  - Reset cursor
  - Skip messages
  - Clear backlog (subscription-scoped)
  - Unload topic / terminate topic
  - Topic policies and schema admin
- Test message workflows
  - Publish bounded test messages
  - Consume bounded test messages with subscription mode options
- Replay / Retry workflow
  - `ACK_ONLY`, `ACK_AND_MOVE`, `SEARCH_ONLY`
  - CSV header-driven multi-field matching (row `AND`, rows `OR`)
  - Worker-backed long-running execution with status + event timeline
  - Search export download when `SEARCH_ONLY` completes

## Repository Structure

```text
backend/
  api/        Spring Boot API (controllers, services, REST/mock gateways)
  worker/     Spring Boot worker (queued replay/retry/search processing)
  shared/     Shared job/model/gateway contracts
  db/init/    Postgres init SQL
frontend/     Angular application
scripts/      Local helper scripts (including dev launcher)
docs/         Additional docs and notes
```

## Prerequisites

- Java 17
- Maven 3.9+
- Node.js 20+ and npm
- Docker + Docker Compose

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d postgres pulsar
```

Services:

- PostgreSQL: `localhost:5432`
- Pulsar broker: `pulsar://localhost:6650`
- Pulsar admin REST: `http://localhost:8081`

### 2. Start backend API + worker

```bash
cd backend
mvn -pl shared,api,worker -am -DskipTests install

cd api
mvn spring-boot:run
```

In a second terminal:

```bash
cd backend/worker
mvn spring-boot:run
```

### 3. Start frontend

```bash
cd frontend
npm install
npm start
```

Frontend runs at `http://localhost:4200` and proxies `/api/*` to `http://localhost:8080`.

## One-Command Dev Script

```bash
./scripts/dev.sh
```

Current behavior:

- Starts Postgres
- Builds backend modules
- Starts API, worker, and frontend

Note: local Pulsar startup in `scripts/dev.sh` is currently commented out, so run Pulsar via `docker compose` (or your own cluster) before using live features.

## Configuration

API defaults are in [`backend/api/src/main/resources/application.yml`](backend/api/src/main/resources/application.yml).

Common environment variables:

- `APP_PULSAR_GATEWAY_MODE` (default: `rest`)
  - `rest`: live Pulsar admin + client paths
  - `mock`: safe seeded responses for demo/dev
- `APP_DATASOURCE_URL`
- `APP_DATASOURCE_USERNAME`
- `APP_DATASOURCE_PASSWORD`

You can also force mock mode per browser session:

```text
http://localhost:4200/environments?mode=mock
```

## Local Environment Values (Live)

For local Pulsar standalone:

- `brokerUrl`: `pulsar://localhost:6650`
- `adminUrl`: `http://localhost:8081`
- `authMode`: `none`

## Replay/Retry Matching Semantics

`POST /topics/replay-copy` supports multipart with `request` + optional `idsFile` CSV.

CSV semantics:

- Header row defines top-level payload fields (`feedId,objectId,status`, etc.)
- Each non-empty row is one criteria object
- Within a row: all populated fields must match (`AND`)
- Across rows: any row match qualifies (`OR`)
- Blank cell means that field is not constrained for that row

## API Highlights

Not exhaustive, but frequently used endpoints:

- `GET /api/v1/environments`
- `POST /api/v1/environments/{envId}/test-connection`
- `POST /api/v1/environments/{envId}/sync`
- `GET /api/v1/environments/{envId}/topics`
- `GET /api/v1/environments/{envId}/topics/detail?topic=...`
- `POST /api/v1/environments/{envId}/topics/publish`
- `POST /api/v1/environments/{envId}/topics/consume`
- `POST /api/v1/environments/{envId}/topics/reset-cursor`
- `POST /api/v1/environments/{envId}/topics/skip-messages`
- `POST /api/v1/environments/{envId}/topics/clear-backlog`
- `POST /api/v1/environments/{envId}/topics/replay-copy`
- `GET /api/v1/environments/{envId}/topics/jobs/{jobId}`
- `GET /api/v1/environments/{envId}/topics/jobs/{jobId}/events`
- `GET /api/v1/environments/{envId}/topics/jobs/{jobId}/search-export`

## Compatibility Matrix

| Area | Version / Mode | Status | Notes |
| --- | --- | --- | --- |
| Java runtime | 17 | Supported | Required for backend modules |
| Spring Boot | 3.4.4 | Supported | Backend stack |
| Pulsar client dependency | 3.3.5 | Supported | Backend dependency pin |
| Local Pulsar Docker image | 4.1.1 | Supported (local) | Default in `docker-compose.yml` |
| Gateway mode | `rest` | Supported | Live flows |
| Gateway mode | `mock` | Supported | Demo/dev safe mode |
| Auth mode | `none` | Supported | Local standalone default |
| Auth mode | `token` | Supported | raw/token/env formats |
| Auth mode | `basic` | Supported | Supported in current client integration |

If you need broader Pulsar version confidence, run CI smoke tests across multiple Pulsar container tags.

## Testing

### Backend (API)

```bash
cd backend
mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 -pl api -am test
```

### Backend (Worker)

```bash
cd backend
mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 -pl worker -am test
```

### Frontend

```bash
cd frontend
npm run build
npm run test -- --watch=false --browsers=ChromeHeadless
```

### E2E

```bash
cd frontend
npm run e2e:install
npm run e2e:mock
npm run e2e:live
```

## Security Note

This project is currently focused on operator workflows and local/live admin capabilities. If publishing publicly, document your deployment model and add environment-specific controls such as:

- authentication and authorization (RBAC)
- audit logging
- secret management hardening
- CORS/domain restrictions
- approval policies for destructive actions

