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
