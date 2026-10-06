-- Additive schema for the tenant-scoped ingestion outbox.
-- Source contract: ingestion_triggers.TRIGGER_DDL. This file is standalone so
-- deployment tooling can review/apply it without importing application code.
-- Prerequisites: tenant, document, workflow_pipeline_run, and
-- solf_workflow_versions already exist. Apply through the normal migration
-- process with a role authorized to create policies/functions; no app startup
-- or package promotion executes this script implicitly.
BEGIN;

CREATE TABLE IF NOT EXISTS ikos_ingestion_trigger (
    tenant_id UUID NOT NULL REFERENCES tenant(tenant_id),
    trigger_key TEXT NOT NULL CHECK (btrim(trigger_key) <> ''),
    workflow_key TEXT NOT NULL CHECK (btrim(workflow_key) <> ''),
    document_types TEXT[] NOT NULL DEFAULT '{}',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, trigger_key),
    CHECK (array_position(document_types, NULL) IS NULL)
);

CREATE TABLE IF NOT EXISTS ikos_ingestion_delivery (
    delivery_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    trigger_key TEXT NOT NULL,
    event_key TEXT NOT NULL,
    event_payload JSONB NOT NULL CHECK (jsonb_typeof(event_payload) = 'object'),
    event_sha256 TEXT NOT NULL CHECK (event_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    run_id BIGINT REFERENCES workflow_pipeline_run(run_id),
    workflow_key TEXT,
    workflow_version_id BIGINT REFERENCES solf_workflow_versions(workflow_version_id),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, trigger_key, event_key),
    UNIQUE (run_id),
    CHECK ((run_id IS NULL AND workflow_key IS NULL AND workflow_version_id IS NULL)
        OR (run_id IS NOT NULL AND workflow_key IS NOT NULL AND workflow_version_id IS NOT NULL)),
    CHECK (status <> 'completed' OR run_id IS NOT NULL),
    FOREIGN KEY (tenant_id, trigger_key)
        REFERENCES ikos_ingestion_trigger(tenant_id, trigger_key)
);

CREATE INDEX IF NOT EXISTS ikos_ingestion_delivery_queue
    ON ikos_ingestion_delivery(tenant_id, delivery_id) WHERE status = 'queued';

CREATE OR REPLACE FUNCTION ikos_ingestion_delivery_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.tenant_id, NEW.trigger_key, NEW.event_key,
           NEW.event_payload, NEW.event_sha256)
       IS DISTINCT FROM ROW(OLD.tenant_id, OLD.trigger_key, OLD.event_key,
                            OLD.event_payload, OLD.event_sha256) THEN
        RAISE EXCEPTION 'ingestion event identity and payload are immutable';
    END IF;
    IF OLD.run_id IS NOT NULL AND
       ROW(NEW.run_id, NEW.workflow_key, NEW.workflow_version_id)
       IS DISTINCT FROM ROW(OLD.run_id, OLD.workflow_key, OLD.workflow_version_id) THEN
        RAISE EXCEPTION 'ingestion run identity is immutable';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ikos_ingestion_delivery_immutable ON ikos_ingestion_delivery;
CREATE TRIGGER ikos_ingestion_delivery_immutable BEFORE UPDATE ON ikos_ingestion_delivery
    FOR EACH ROW EXECUTE FUNCTION ikos_ingestion_delivery_immutable();

ALTER TABLE ikos_ingestion_trigger ENABLE ROW LEVEL SECURITY;
ALTER TABLE ikos_ingestion_trigger FORCE ROW LEVEL SECURITY;
ALTER TABLE ikos_ingestion_delivery ENABLE ROW LEVEL SECURITY;
ALTER TABLE ikos_ingestion_delivery FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS ikos_ingestion_trigger_tenant ON ikos_ingestion_trigger;
CREATE POLICY ikos_ingestion_trigger_tenant ON ikos_ingestion_trigger AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
DROP POLICY IF EXISTS ikos_ingestion_trigger_access ON ikos_ingestion_trigger;
CREATE POLICY ikos_ingestion_trigger_access ON ikos_ingestion_trigger
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
DROP POLICY IF EXISTS ikos_ingestion_delivery_tenant ON ikos_ingestion_delivery;
CREATE POLICY ikos_ingestion_delivery_tenant ON ikos_ingestion_delivery AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
DROP POLICY IF EXISTS ikos_ingestion_delivery_access ON ikos_ingestion_delivery;
CREATE POLICY ikos_ingestion_delivery_access ON ikos_ingestion_delivery
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);

COMMIT;