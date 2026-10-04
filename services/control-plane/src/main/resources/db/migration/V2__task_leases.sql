ALTER TABLE tasks
    ADD COLUMN lease_owner VARCHAR(255),
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE tasks
    ADD CONSTRAINT tasks_attempt_check CHECK (attempt >= 0),
    ADD CONSTRAINT tasks_max_attempts_check CHECK (max_attempts > 0);

CREATE INDEX idx_tasks_ready_for_lease
    ON tasks (next_attempt_at, created_at, id)
    WHERE status = 'PENDING';

CREATE INDEX idx_tasks_expired_leases
    ON tasks (lease_expires_at)
    WHERE status IN ('LEASED', 'EXECUTING');
