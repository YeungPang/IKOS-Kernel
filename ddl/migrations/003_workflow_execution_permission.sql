-- Additive authorization catalog upgrade. Review role grants before applying.
BEGIN;
INSERT INTO permission (permission_key, description)
VALUES ('workflows.run', 'Execute tenant workflows and dispatch ingestion deliveries')
ON CONFLICT (permission_key) DO NOTHING;
INSERT INTO role_permission (role_id, permission_key)
SELECT role_id, 'workflows.run' FROM role WHERE role_key = 'tenant_admin'
ON CONFLICT DO NOTHING;
-- Service principals need explicit membership/role grants and documents.write,
-- plus each domain capability's permission; no authority comes from packages.
COMMIT;