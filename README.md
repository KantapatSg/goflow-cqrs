# GoFlow CQRS

GoFlow is a learning-focused Go backend: a modular monolith that separates commands from read projections while keeping the system small enough to understand end-to-end.

## Included vertical slice

- Gin command/query HTTP API
- PostgreSQL write model persisted with GORM
- Separate `goflow_read` tables queried with `database/sql`
- Domain-owned Job state machine and `JobAttempt` lifecycle
- Transactional outbox: each command persists state and an event in one transaction
- Asynchronous, idempotent read-model projector with `FOR UPDATE SKIP LOCKED` claims and a bounded worker channel
- Docker Compose PostgreSQL, API, and Swagger UI

The worker and job actions are intentionally visible in the database: inspect `jobs`, `job_attempts`, `outbox_events`, and `goflow_read.*` while exercising the API.

## Run

```bash
copy .env.example .env
docker compose up --build
```

- API: <http://localhost:8080>
- health check: <http://localhost:8080/healthz>
- Swagger UI: <http://localhost:8081>

The SQL migration is mounted into a fresh PostgreSQL volume. To recreate a local database deliberately:

```bash
docker compose down -v
docker compose up --build
```

## Demo flow

1. `POST /v1/projects` with `{ "name": "Demo" }`
2. `POST /v1/projects/{projectID}/tasks` with `{ "name":"echo", "kind":"demo", "maxAttempts":2, "timeoutMs":30000 }`
3. `POST /v1/tasks/{taskID}/jobs` with `{ "idempotencyKey":"demo-1", "input":{} }`
4. Wait briefly for the projection, then `GET /v1/jobs/{jobID}`
5. `POST /v1/jobs/{jobID}/start` with `{ "workerId":"manual-demo" }`
6. Use the returned `attempt.id` with `/complete` or `/fail`

Command responses come from the write side. GET endpoints read asynchronously updated projections, so a newly created resource can return `404` briefly.

## State machine

```text
QUEUED → RUNNING → SUCCESS
                 ↘ FAILED → RETRYING → RUNNING

QUEUED / RETRYING / FAILED → CANCELLED
```

Running-job cancellation is intentionally deferred: it needs a cooperative executor and cancellation acknowledgement, not only a database status change.

## Development checks

```bash
go test ./...
go test -race ./...
go vet ./...
```

## Important boundaries

`internal/domain` contains no Gin, GORM, SQL, or worker dependencies. `internal/app/command` owns command orchestration. PostgreSQL write and read adapters implement persistence separately. `internal/projection` consumes the outbox and updates only `goflow_read`.

## Intentional next steps

- replace manual `/start` demonstration endpoint with a real task executor worker pool
- add keyset cursor pagination to `ListJobs`
- add integration tests against PostgreSQL for transaction rollback, duplicate events, ordering, and graceful shutdown
- document ADRs for CQRS, aggregate boundaries, and transactional outbox
