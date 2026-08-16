CREATE TABLE IF NOT EXISTS projects (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  version BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES projects(id),
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
  timeout_ms BIGINT NOT NULL CHECK (timeout_ms > 0),
  version BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (project_id, name)
);
CREATE INDEX IF NOT EXISTS tasks_project_id_idx ON tasks(project_id);

CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES projects(id),
  task_id UUID NOT NULL REFERENCES tasks(id),
  status TEXT NOT NULL,
  version BIGINT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
  active_attempt_id UUID,
  idempotency_key TEXT,
  input JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (task_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs(status, created_at);
CREATE INDEX IF NOT EXISTS jobs_task_created_idx ON jobs(task_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS job_attempts (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES jobs(id),
  attempt_no INTEGER NOT NULL,
  status TEXT NOT NULL,
  worker_id TEXT,
  failure TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  UNIQUE (job_id, attempt_no)
);
CREATE UNIQUE INDEX IF NOT EXISTS job_attempts_one_running_idx ON job_attempts(job_id) WHERE status = 'RUNNING';

CREATE TABLE IF NOT EXISTS outbox_events (
  sequence BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  aggregate_type TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  aggregate_version BIGINT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  claimed_by TEXT,
  claim_until TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  processed_at TIMESTAMPTZ,
  last_error TEXT,
  UNIQUE (aggregate_type, aggregate_id, aggregate_version)
);
CREATE INDEX IF NOT EXISTS outbox_ready_idx ON outbox_events(available_at, sequence) WHERE processed_at IS NULL;

CREATE SCHEMA IF NOT EXISTS goflow_read;
CREATE TABLE IF NOT EXISTS goflow_read.project_views (
  project_id UUID PRIMARY KEY, name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS goflow_read.task_views (
  task_id UUID PRIMARY KEY, project_id UUID NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL,
  max_attempts INTEGER NOT NULL, timeout_ms BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS task_views_project_idx ON goflow_read.task_views(project_id, created_at DESC, task_id DESC);
CREATE TABLE IF NOT EXISTS goflow_read.job_views (
  job_id UUID PRIMARY KEY, project_id UUID NOT NULL, task_id UUID NOT NULL, status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL, last_error TEXT, created_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS job_views_project_idx ON goflow_read.job_views(project_id, created_at DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS job_views_task_idx ON goflow_read.job_views(task_id, created_at DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS job_views_status_idx ON goflow_read.job_views(status, created_at DESC, job_id DESC);
CREATE TABLE IF NOT EXISTS goflow_read.job_attempt_views (
  job_id UUID NOT NULL, attempt_no INTEGER NOT NULL, status TEXT NOT NULL, failure TEXT,
  started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, PRIMARY KEY(job_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS goflow_read.project_dashboard_views (
  project_id UUID PRIMARY KEY, queued_count BIGINT NOT NULL DEFAULT 0, running_count BIGINT NOT NULL DEFAULT 0,
  success_count BIGINT NOT NULL DEFAULT 0, failed_count BIGINT NOT NULL DEFAULT 0, cancelled_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS goflow_read.projection_receipts (
  projector_name TEXT NOT NULL, event_id UUID NOT NULL, processed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(projector_name, event_id)
);
