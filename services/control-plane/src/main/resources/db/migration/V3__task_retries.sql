ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('PENDING', 'LEASED', 'EXECUTING', 'RETRYING', 'SUCCEEDED', 'FAILED'));

DROP INDEX idx_tasks_ready_for_lease;
CREATE INDEX idx_tasks_ready_for_lease
    ON tasks (next_attempt_at, created_at, id)
    WHERE status IN ('PENDING', 'RETRYING');
