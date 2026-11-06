-- Preserve historical rows and immutable events: NULL means no deadline.
ALTER TABLE runs ADD COLUMN deadline_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN deadline_at TIMESTAMPTZ;
ALTER TABLE runs DROP CONSTRAINT runs_status_check;
ALTER TABLE runs ADD CONSTRAINT runs_status_check
    CHECK (status IN ('CREATED','QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','TIMEOUT'));
ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('PENDING','LEASED','EXECUTING','RETRYING','SUCCEEDED','FAILED','CANCELLED','TIMEOUT'));
ALTER TABLE runs ADD CONSTRAINT runs_deadline_finite_check CHECK (deadline_at IS NULL OR isfinite(deadline_at));
ALTER TABLE tasks ADD CONSTRAINT tasks_deadline_finite_check CHECK (deadline_at IS NULL OR isfinite(deadline_at));
ALTER TABLE runs ADD CONSTRAINT runs_timeout_fields_check
    CHECK (status <> 'TIMEOUT' OR (deadline_at IS NOT NULL AND output IS NULL));
ALTER TABLE tasks ADD CONSTRAINT tasks_timeout_fields_check
    CHECK (status <> 'TIMEOUT' OR
        (deadline_at IS NOT NULL AND output IS NULL AND lease_owner IS NULL AND lease_expires_at IS NULL));
ALTER TABLE tasks ADD CONSTRAINT tasks_lease_deadline_check
    CHECK (deadline_at IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= deadline_at);

-- B08 retains one Task per Run and a single shared deadline. Check at commit so creation
-- and migration/test updates can write both rows in either order, while mismatches cannot commit.
CREATE FUNCTION enforce_shared_deadline() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_run TEXT;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN target_run := NEW.id; ELSE target_run := NEW.run_id; END IF;
    IF EXISTS (SELECT 1 FROM tasks t JOIN runs r ON r.id = t.run_id
               WHERE r.id = target_run AND t.deadline_at IS DISTINCT FROM r.deadline_at) THEN
        RAISE EXCEPTION 'Run and Task deadlines must match' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER runs_shared_deadline
    AFTER INSERT OR UPDATE ON runs DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_shared_deadline();
CREATE CONSTRAINT TRIGGER tasks_shared_deadline
    AFTER INSERT OR UPDATE ON tasks DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_shared_deadline();
CREATE INDEX idx_tasks_deadline ON tasks (deadline_at, id)
    WHERE deadline_at IS NOT NULL AND status IN ('PENDING','LEASED','EXECUTING','RETRYING');
