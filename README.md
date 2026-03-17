# Pulsar Admin Console

Initial implementation of the Pulsar Admin UI kickoff plan.

## What is included

- Angular frontend with:
  - environment overview
  - environment switcher
  - topic explorer
  - topic details view
- Spring Boot API with mock-first Pulsar admin endpoints
- Spring Boot worker that processes queued replay and copy jobs
- PostgreSQL schema bootstrap for `environments`, `jobs`, and `job_events`
- Configurable Pulsar gateway mode:
  - `mock` for local-safe development
  - `rest` for real admin REST connection and metadata sync

## Local development

### 1. Start PostgreSQL

```bash
docker compose up -d postgres
```

### 2. Start the backend API

```bash
cd backend
mvn -pl shared,api -am -DskipTests install
cd api
mvn spring-boot:run
```

### 3. Start the worker

```bash
cd backend
mvn -pl shared,worker -am -DskipTests install
cd worker
mvn spring-boot:run
```

The install step makes sure the local `shared` module is available before running each Spring Boot app.
Replay and copy jobs are now queued by the API and completed by the worker.

### Gateway mode

By default the API uses the mock gateway:

```bash
APP_PULSAR_GATEWAY_MODE=mock
```

To try real Pulsar admin REST connectivity for environment connection tests and metadata sync:

```bash
APP_PULSAR_GATEWAY_MODE=rest
```

In `rest` mode:

- environment `Test Connection` and `Sync` use the configured `adminUrl` against Pulsar admin REST endpoints
- metadata sync now captures tenant and namespace inventory plus live topic subscriptions, stats, partition summaries, and schema presence when the Pulsar cluster exposes them
- `Peek Messages` now uses a read-only Pulsar client path against the configured `brokerUrl`
- `Reset Cursor` and `Skip Messages` use Pulsar admin REST endpoints
- the current live integration supports auth mode `none` and token-based environments using a raw token, `token:...`, or `env://VAR_NAME`
- basic auth can be stored for future work, but live peek still requires `none` or `token`

### 4. Start the frontend

```bash
cd frontend
npm install
npm start
```

The Angular dev server proxies `/api/*` to `http://localhost:8080`.

## API endpoints

- `GET /api/v1/environments`
- `GET /api/v1/environments/{envId}/health`
- `GET /api/v1/environments/{envId}/topics`
- `GET /api/v1/environments/{envId}/topics/detail?topic=...`
- `GET /api/v1/environments/{envId}/topics/peek?topic=...&limit=...`
- `POST /api/v1/environments/{envId}/topics/reset-cursor`
- `POST /api/v1/environments/{envId}/topics/skip-messages`
- `POST /api/v1/environments/{envId}/topics/replay-copy`
- `GET /api/v1/environments/{envId}/topics/jobs/{jobId}`
