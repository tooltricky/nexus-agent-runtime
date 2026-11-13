-- Historical ownership is explicit; it must never be inferred from user input.
CREATE TABLE tenants (
    id VARCHAR(100) PRIMARY KEY CHECK (btrim(id) <> '')
);
CREATE TABLE projects (
    tenant_id VARCHAR(100) NOT NULL REFERENCES tenants(id),
    id VARCHAR(100) NOT NULL CHECK (btrim(id) <> ''),
    PRIMARY KEY (tenant_id, id)
);
INSERT INTO tenants VALUES ('legacy');
INSERT INTO projects VALUES ('legacy', 'legacy');

ALTER TABLE sessions ADD COLUMN tenant_id VARCHAR(100), ADD COLUMN project_id VARCHAR(100);
ALTER TABLE runs ADD COLUMN tenant_id VARCHAR(100), ADD COLUMN project_id VARCHAR(100);
ALTER TABLE tasks ADD COLUMN tenant_id VARCHAR(100), ADD COLUMN project_id VARCHAR(100);
UPDATE sessions SET tenant_id = 'legacy', project_id = 'legacy';
UPDATE runs SET tenant_id = 'legacy', project_id = 'legacy';
UPDATE tasks SET tenant_id = 'legacy', project_id = 'legacy';
-- Flush existing deferred deadline/pause checks before subsequent ALTER TABLE.
-- Do not disable any triggers or weaken their checks.
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;
ALTER TABLE sessions
    ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN project_id SET NOT NULL,
    ADD CONSTRAINT sessions_project_fk FOREIGN KEY (tenant_id, project_id) REFERENCES projects(tenant_id, id),
    ADD CONSTRAINT sessions_owned_id UNIQUE (id, tenant_id, project_id);
ALTER TABLE runs
    ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN project_id SET NOT NULL,
    ADD CONSTRAINT runs_owned_session_fk FOREIGN KEY (session_id, tenant_id, project_id)
        REFERENCES sessions(id, tenant_id, project_id),
    ADD CONSTRAINT runs_owned_id UNIQUE (id, tenant_id, project_id),
    DROP CONSTRAINT runs_idempotency_key_key,
    ADD CONSTRAINT runs_tenant_idempotency_key UNIQUE (tenant_id, idempotency_key);
ALTER TABLE tasks
    ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN project_id SET NOT NULL,
    ADD CONSTRAINT tasks_owned_run_fk FOREIGN KEY (run_id, tenant_id, project_id)
        REFERENCES runs(id, tenant_id, project_id),
    ADD CONSTRAINT tasks_owned_id UNIQUE (id, run_id, tenant_id, project_id);
-- Also reject a valid Task ID attached to the wrong Run in the original event row.
ALTER TABLE domain_events ADD CONSTRAINT event_task_run_fk
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(id, run_id);

CREATE FUNCTION reject_ownership_change() RETURNS trigger AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.project_id IS DISTINCT FROM OLD.project_id THEN
        RAISE EXCEPTION 'resource ownership is immutable';
    END IF;
    IF TG_TABLE_NAME = 'runs' AND to_jsonb(NEW)->>'session_id' IS DISTINCT FROM to_jsonb(OLD)->>'session_id' THEN
        RAISE EXCEPTION 'Run Session is immutable';
    END IF;
    IF TG_TABLE_NAME = 'tasks' AND to_jsonb(NEW)->>'run_id' IS DISTINCT FROM to_jsonb(OLD)->>'run_id' THEN
        RAISE EXCEPTION 'Task Run is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER sessions_ownership BEFORE UPDATE ON sessions FOR EACH ROW EXECUTE FUNCTION reject_ownership_change();
CREATE TRIGGER runs_ownership BEFORE UPDATE ON runs FOR EACH ROW EXECUTE FUNCTION reject_ownership_change();
CREATE TRIGGER tasks_ownership BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION reject_ownership_change();

-- No UPDATE of historical domain_events and no disabling of its append-only trigger.
CREATE TABLE event_ownership (
    event_id VARCHAR(40) PRIMARY KEY REFERENCES domain_events(event_id),
    run_id VARCHAR(40) NOT NULL,
    task_id VARCHAR(40),
    tenant_id VARCHAR(100) NOT NULL,
    project_id VARCHAR(100) NOT NULL,
    FOREIGN KEY (run_id, tenant_id, project_id) REFERENCES runs(id, tenant_id, project_id),
    FOREIGN KEY (task_id, run_id, tenant_id, project_id) REFERENCES tasks(id, run_id, tenant_id, project_id)
);
CREATE FUNCTION check_event_ownership() RETURNS trigger AS $$
DECLARE original domain_events%ROWTYPE;
BEGIN
    SELECT * INTO STRICT original FROM domain_events WHERE event_id = NEW.event_id;
    IF original.run_id IS DISTINCT FROM NEW.run_id OR original.task_id IS DISTINCT FROM NEW.task_id THEN
        RAISE EXCEPTION 'event ownership must match original event';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER event_ownership_matches BEFORE INSERT ON event_ownership
    FOR EACH ROW EXECUTE FUNCTION check_event_ownership();
INSERT INTO event_ownership (event_id, run_id, task_id, tenant_id, project_id)
    SELECT e.event_id, e.run_id, e.task_id, r.tenant_id, r.project_id
    FROM domain_events e JOIN runs r ON r.id = e.run_id;
CREATE TRIGGER event_ownership_immutable BEFORE UPDATE OR DELETE ON event_ownership
    FOR EACH ROW EXECUTE FUNCTION reject_domain_event_mutation();
CREATE FUNCTION assign_event_ownership() RETURNS trigger AS $$
BEGIN
    INSERT INTO event_ownership (event_id, run_id, task_id, tenant_id, project_id)
        SELECT NEW.event_id, NEW.run_id, NEW.task_id, tenant_id, project_id FROM runs WHERE id = NEW.run_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'event Run ownership missing'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER domain_event_assign_ownership AFTER INSERT ON domain_events
    FOR EACH ROW EXECUTE FUNCTION assign_event_ownership();
CREATE INDEX idx_event_ownership_scope ON event_ownership(tenant_id, project_id, run_id);
