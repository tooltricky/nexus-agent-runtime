ALTER TABLE runs DROP CONSTRAINT runs_status_check;
ALTER TABLE runs ADD CONSTRAINT runs_status_check
    CHECK (status IN ('CREATED','QUEUED','RUNNING','PAUSED','SUCCEEDED','FAILED','CANCELLED','TIMEOUT'));
ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('PENDING','LEASED','EXECUTING','RETRYING','PAUSED','SUCCEEDED','FAILED','CANCELLED','TIMEOUT'));
ALTER TABLE runs ADD CONSTRAINT runs_paused_fields_check CHECK (status <> 'PAUSED' OR output IS NULL);
ALTER TABLE tasks ADD CONSTRAINT tasks_paused_fields_check
    CHECK (status <> 'PAUSED' OR (output IS NULL AND lease_owner IS NULL AND lease_expires_at IS NULL));
ALTER TABLE tasks ADD CONSTRAINT tasks_id_run_unique UNIQUE (id, run_id);

-- Latest checkpoint doubles as a durable invalidation fence, even after resume.
CREATE TABLE task_pause_checkpoints (
    task_id VARCHAR(40) PRIMARY KEY,
    run_id VARCHAR(40) NOT NULL UNIQUE REFERENCES runs(id),
    checkpoint_id UUID NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    run_status VARCHAR(32) NOT NULL CHECK (run_status IN ('QUEUED','RUNNING')),
    task_status VARCHAR(32) NOT NULL CHECK (task_status IN ('PENDING','LEASED','EXECUTING','RETRYING')),
    attempt INTEGER NOT NULL CHECK (attempt >= 0),
    task_version BIGINT NOT NULL CHECK (task_version >= 0),
    run_version BIGINT NOT NULL CHECK (run_version >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL CHECK (isfinite(next_attempt_at)),
    paused_at TIMESTAMPTZ NOT NULL CHECK (isfinite(paused_at)),
    consumed_at TIMESTAMPTZ CHECK (isfinite(consumed_at)),
    disposition VARCHAR(32),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(id, run_id),
    CHECK ((consumed_at IS NULL AND disposition IS NULL) OR
           (consumed_at IS NOT NULL AND disposition IS NOT NULL AND
            disposition IN ('RESUMED','ATTEMPTS_EXHAUSTED','CANCELLED','TIMEOUT'))),
    CHECK (consumed_at IS NULL OR consumed_at >= paused_at),
    CHECK (run_status = 'RUNNING' OR task_status = 'PENDING')
);

CREATE FUNCTION enforce_pause_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_run TEXT;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN target_run := NEW.id;
    ELSIF TG_OP = 'DELETE' THEN target_run := OLD.run_id;
    ELSE target_run := NEW.run_id; END IF;
    IF EXISTS (
        SELECT 1 FROM runs r JOIN tasks t ON t.run_id = r.id
        LEFT JOIN task_pause_checkpoints c ON c.task_id = t.id
        WHERE r.id = target_run AND (
            (r.status = 'PAUSED') <> (t.status = 'PAUSED') OR
            (t.status = 'PAUSED' AND (c.task_id IS NULL OR c.consumed_at IS NOT NULL OR
                c.run_id <> r.id OR c.attempt <> t.attempt OR c.task_version + 1 <> t.version OR
                c.run_version + 1 <> r.version OR c.next_attempt_at <> t.next_attempt_at)) OR
            (t.status <> 'PAUSED' AND c.task_id IS NOT NULL AND c.consumed_at IS NULL)
        )
    ) THEN RAISE EXCEPTION 'Run/Task pause and checkpoint must agree' USING ERRCODE = '23514'; END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER runs_pause_checkpoint AFTER INSERT OR UPDATE ON runs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_pause_checkpoint();
CREATE CONSTRAINT TRIGGER tasks_pause_checkpoint AFTER INSERT OR UPDATE ON tasks
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_pause_checkpoint();
CREATE CONSTRAINT TRIGGER checkpoint_pause_state AFTER INSERT OR UPDATE OR DELETE ON task_pause_checkpoints
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_pause_checkpoint();

DROP INDEX idx_tasks_deadline;
CREATE INDEX idx_tasks_deadline ON tasks (deadline_at, id)
    WHERE deadline_at IS NOT NULL AND status IN ('PENDING','LEASED','EXECUTING','RETRYING','PAUSED');
