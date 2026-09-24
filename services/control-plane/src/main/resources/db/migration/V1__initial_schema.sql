CREATE TABLE sessions (
    id          VARCHAR(40) PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE runs (
    id               VARCHAR(40) PRIMARY KEY,
    session_id       VARCHAR(40) NOT NULL REFERENCES sessions(id),
    idempotency_key  VARCHAR(255) NOT NULL UNIQUE,
    request_hash     VARCHAR(64) NOT NULL,
    input            TEXT NOT NULL,
    status           VARCHAR(32) NOT NULL,
    output           JSONB,
    version          BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT runs_status_check CHECK (status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED'))
);

CREATE TABLE tasks (
    id          VARCHAR(40) PRIMARY KEY,
    run_id      VARCHAR(40) NOT NULL REFERENCES runs(id),
    name        VARCHAR(100) NOT NULL,
    status      VARCHAR(32) NOT NULL,
    input       JSONB NOT NULL,
    output      JSONB,
    version     BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tasks_status_check CHECK (status IN ('PENDING', 'LEASED', 'EXECUTING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT one_task_per_run UNIQUE (run_id)
);

CREATE TABLE domain_events (
    sequence        BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(40) NOT NULL UNIQUE,
    aggregate_type  VARCHAR(32) NOT NULL,
    aggregate_id    VARCHAR(40) NOT NULL,
    run_id          VARCHAR(40) NOT NULL REFERENCES runs(id),
    task_id         VARCHAR(40) REFERENCES tasks(id),
    event_type      VARCHAR(100) NOT NULL,
    payload         JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tasks_pending ON tasks (created_at) WHERE status = 'PENDING';
CREATE INDEX idx_domain_events_run_sequence ON domain_events (run_id, sequence);

CREATE FUNCTION reject_domain_event_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'domain_events is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER domain_events_immutable
BEFORE UPDATE OR DELETE ON domain_events
FOR EACH ROW EXECUTE FUNCTION reject_domain_event_mutation();
