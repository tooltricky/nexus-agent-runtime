-- Add cancellation without changing applied migrations or rewriting event history.
ALTER TABLE runs DROP CONSTRAINT runs_status_check;
ALTER TABLE runs ADD CONSTRAINT runs_status_check
    CHECK (status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'));

ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('PENDING', 'LEASED', 'EXECUTING', 'RETRYING', 'SUCCEEDED', 'FAILED', 'CANCELLED'));

-- NULL identifies the unchanged legacy event envelope (0.1).
-- Future writers must supply an explicit schema version; do not backfill old events.
ALTER TABLE domain_events ADD COLUMN schema_version VARCHAR(32);

ALTER TABLE runs ADD CONSTRAINT runs_cancelled_output_check
    CHECK (status <> 'CANCELLED' OR output IS NULL);
ALTER TABLE tasks ADD CONSTRAINT tasks_cancelled_fields_check
    CHECK (status <> 'CANCELLED' OR
           (output IS NULL AND lease_owner IS NULL AND lease_expires_at IS NULL));
