-- Versioned IKOS domain-extension package registry and promotion audit.
-- Safe/additive. Apply through a migration runner or object_db.create_tables().
BEGIN;

CREATE TABLE IF NOT EXISTS ikos_extension_package (
    package_id BIGSERIAL PRIMARY KEY,
    extension_key VARCHAR(128) NOT NULL UNIQUE,
    display_name VARCHAR(256) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_by VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ikos_extension_package_version (
    package_version_id BIGSERIAL PRIMARY KEY,
    package_id BIGINT NOT NULL REFERENCES ikos_extension_package(package_id) ON DELETE RESTRICT,
    version VARCHAR(64) NOT NULL,
    manifest JSONB NOT NULL,
    sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'validated' CHECK (status IN ('validated','deprecated')),
    created_by VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (package_id, version)
);
CREATE INDEX IF NOT EXISTS idx_ikos_extension_version_created
    ON ikos_extension_package_version(package_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ikos_extension_deployment (
    deployment_id BIGSERIAL PRIMARY KEY,
    package_version_id BIGINT NOT NULL REFERENCES ikos_extension_package_version(package_version_id) ON DELETE RESTRICT,
    target_scope VARCHAR(16) NOT NULL CHECK (target_scope IN ('tenant','global')),
    target_tenant_id UUID NOT NULL REFERENCES tenant(tenant_id) ON DELETE RESTRICT,
    package_sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('applied','failed')),
    change_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    deployed_by VARCHAR(256) NOT NULL,
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((target_scope = 'tenant' AND target_tenant_id <> '00000000-0000-0000-0000-000000000001'::uuid)
        OR (target_scope = 'global' AND target_tenant_id = '00000000-0000-0000-0000-000000000001'::uuid)),
    UNIQUE (package_version_id, target_scope, target_tenant_id)
);
CREATE INDEX IF NOT EXISTS idx_ikos_extension_deployment_target
    ON ikos_extension_deployment(target_scope, target_tenant_id, deployed_at DESC);

DO $ikos_extension_rls$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'ikos_extension_package', 'ikos_extension_package_version', 'ikos_extension_deployment'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE format('DROP POLICY IF EXISTS ikos_platform_extension_admin ON %I', table_name);
        EXECUTE format(
            'CREATE POLICY ikos_platform_extension_admin ON %I USING (''platform.configuration.publish'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '',''))) WITH CHECK (''platform.configuration.publish'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '','')))',
            table_name
        );
    END LOOP;
END
$ikos_extension_rls$;

COMMIT;
