# Pulsar Admin Console

Operator-focused Pulsar admin console with mock and live modes, guided workflows, and browser-level verification for local development.

## What is included

- Angular frontend with:
  - environment overview
  - environment switcher
  - topic explorer
  - topic details view
- Spring Boot API with live-first routing and explicit mock override
- Spring Boot worker that processes queued replay and copy jobs
- PostgreSQL schema bootstrap for `environments`, `jobs`, and `job_events`
- Local Pulsar standalone support for live verification
- Configurable Pulsar gateway mode:
  - `rest` for real admin REST connection and metadata sync
  - `mock` for demos, debugging, and safe local exploration
- Browser-level E2E suites for mock and local live verification

## Local development

### 1. Start local infrastructure

```bash
docker compose up -d postgres pulsar
```

This starts:

- PostgreSQL on `localhost:5432`
- Pulsar broker on `pulsar://localhost:6650`
- Pulsar admin REST on `http://localhost:8081`

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

By default the API now runs live-first:

```bash
APP_PULSAR_GATEWAY_MODE=rest
```

To force safe demo mode by default:

```bash
APP_PULSAR_GATEWAY_MODE=mock
```

You can also override the mode per browser session by adding `?mode=mock` to the UI URL. That keeps the app in mock mode while you navigate, which is useful for demos and local debugging without changing server config. Without that flag, requests use the server's configured default mode.

Example:

```text
http://localhost:4200/environments?mode=mock
```

In `rest` mode:

- environment `Test Connection` and `Sync` use the configured `adminUrl` against Pulsar admin REST endpoints
- metadata sync now captures tenant and namespace inventory plus live topic subscriptions, stats, partition summaries, and schema presence when the Pulsar cluster exposes them
- `Peek Messages` now uses a read-only Pulsar client path against the configured `brokerUrl`
- `Reset Cursor` and `Skip Messages` use Pulsar admin REST endpoints
- the current live integration supports auth mode `none` and token-based environments using a raw token, `token:...`, or `env://VAR_NAME`
- basic auth can be stored for future work, but live peek still requires `none` or `token`

## Compatibility matrix

The table below reflects the versions and modes we currently validate in this repository.

| Area | Version / Mode | Status | Notes |
| --- | --- | --- | --- |
| Backend Java runtime | Java 17 | Supported | Required for Spring Boot services in this repo. |
| Pulsar Java client in API | 3.3.5 | Supported | Backend dependency is pinned to `org.apache.pulsar:pulsar-client:3.3.5`. |
| Local Docker Pulsar image | 4.1.1 | Supported (local dev) | Default image in `docker-compose.yml` for local standalone. |
| Gateway mode | `rest` | Supported | Live admin + publish/consume paths. |
| Gateway mode | `mock` | Supported | Safe local/demo mode. |
| Auth mode (live test messaging) | `none` | Supported | Works for local standalone by default. |
| Auth mode (live test messaging) | `token` | Supported | Token formats: raw token, `token:...`, or `env://VAR_NAME`. |
| Auth mode (live test messaging) | `basic` | Supported | Uses Pulsar client `AuthenticationBasic` for publish/consume. |
| Pulsar broker compatibility | Other versions | Not yet verified | We expect many versions to work, but only listed combinations are currently tested. |

If you want broader confidence, add CI matrix runs across multiple Pulsar container versions and execute publish/consume smoke tests per version.

### 5. One-command local dev

```bash
./scripts/dev.sh
```

That command starts:

- Postgres
- Pulsar standalone
- backend API
- worker
- Angular dev server

If `./scripts/dev.sh` fails on the Pulsar container step, check whether these ports are already in use:

- `6650`
- `8081`

This usually means another local Pulsar container or broker is already running.

## Local live environment values

For a local real environment, use:

- `brokerUrl`: `pulsar://localhost:6650`
- `adminUrl`: `http://localhost:8081`
- `authMode`: `none`

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
- `POST /api/v1/environments/{envId}/topics`
- `GET /api/v1/environments/{envId}/topics/detail?topic=...`
- `POST /api/v1/environments/{envId}/topics/subscriptions`
- `DELETE /api/v1/environments/{envId}/topics/subscriptions?topic=...&subscription=...`
- `GET /api/v1/environments/{envId}/topics/peek?topic=...&limit=...`
- `POST /api/v1/environments/{envId}/topics/reset-cursor`
- `POST /api/v1/environments/{envId}/topics/skip-messages`
- `POST /api/v1/environments/{envId}/topics/replay-copy`
- `GET /api/v1/environments/{envId}/topics/jobs/{jobId}`

## Roadmap

The current implementation already covers the first operator layer:

- environment management and sync
- topic explorer and details
- live or mock message peek
- reset cursor
- skip messages
- replay and copy jobs

The next planned expansion is the admin surface that makes this a fuller one-stop Pulsar console:

- create topics
- create, update, and delete subscriptions
- unload and terminate topics
- topic and namespace policy editing
- later: namespace and tenant management

Security, RBAC, approvals, and deeper governance are intentionally deferred until the broader admin surface is in place.

## Verification

Unit/component/controller checks:

```bash
cd backend
mvn -Dmaven.repo.local=/Users/skumar/experimental/pulsar-admin/.m2 -pl api -am test

cd /Users/skumar/experimental/pulsar-admin/frontend
npm test -- --watch=false --browsers=ChromeHeadless
```

Browser E2E:

```bash
cd /Users/skumar/experimental/pulsar-admin/frontend
npm run e2e:install
npm run e2e:mock
npm run e2e:live
```

See [docs/e2e-verification.md](/Users/skumar/experimental/pulsar-admin/docs/e2e-verification.md) and [docs/admin-operations.md](/Users/skumar/experimental/pulsar-admin/docs/admin-operations.md) for the support matrix and local workflow notes.
