-- IKOS full kernel DDL (SOLF + inference + RAG core).
-- Includes core object graph, SOLF rules/clauses/facts/predicates, workflow execution,
-- pipeline run execution, interaction clarification runtime, generated script runtime,
-- and event bus persistence tables.
-- Excludes domain-specific tables (ledger/accounting/hr/crm/project/etc.).

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Shared database tenant and role-based authorization foundation.
CREATE TABLE IF NOT EXISTS tenant (
    tenant_id UUID PRIMARY KEY,
    tenant_key VARCHAR(128) NOT NULL UNIQUE,
    display_name VARCHAR(256) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO tenant (tenant_id, tenant_key, display_name)
VALUES ('00000000-0000-0000-0000-000000000001', 'legacy', 'Legacy tenant')
ON CONFLICT (tenant_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS ikos_user (
    user_id UUID PRIMARY KEY,
    external_subject VARCHAR(256) NOT NULL UNIQUE,
    display_name VARCHAR(256),
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_membership (
    membership_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES ikos_user(user_id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS role (
    role_id BIGSERIAL PRIMARY KEY,
    role_key VARCHAR(128) NOT NULL UNIQUE,
    display_name VARCHAR(256) NOT NULL
);

CREATE TABLE IF NOT EXISTS permission (
    permission_key VARCHAR(128) PRIMARY KEY,
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS role_permission (
    role_id BIGINT NOT NULL REFERENCES role(role_id) ON DELETE CASCADE,
    permission_key VARCHAR(128) NOT NULL REFERENCES permission(permission_key) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_key)
);

CREATE TABLE IF NOT EXISTS membership_role (
    membership_id BIGINT NOT NULL REFERENCES tenant_membership(membership_id) ON DELETE CASCADE,
    role_id BIGINT NOT NULL REFERENCES role(role_id) ON DELETE CASCADE,
    PRIMARY KEY (membership_id, role_id)
);

INSERT INTO role (role_key, display_name) VALUES
    ('employee', 'Employee'), ('finance_viewer', 'Finance Viewer'),
    ('finance_operator', 'Finance Operator'), ('tenant_admin', 'Tenant Administrator')
ON CONFLICT (role_key) DO NOTHING;
INSERT INTO permission (permission_key, description) VALUES
    ('documents.read', 'Read tenant documents'), ('documents.write', 'Create or update tenant documents'),
    ('finance.read', 'Read finance resources'), ('finance.write', 'Modify finance resources'),
    ('documents.restricted.read', 'Read restricted tenant documents'),
    ('tenant.members.manage', 'Manage tenant membership and roles'),
    ('tenant.configuration.manage', 'Manage configuration for the active tenant'),
    ('platform.configuration.publish', 'Publish shared configuration for all tenants')
ON CONFLICT (permission_key) DO NOTHING;
INSERT INTO role_permission (role_id, permission_key)
SELECT r.role_id, p.permission_key FROM role r CROSS JOIN permission p
WHERE (r.role_key = 'employee' AND p.permission_key = 'documents.read')
   OR (r.role_key = 'finance_viewer' AND p.permission_key IN ('documents.read', 'finance.read'))
   OR (r.role_key = 'finance_operator' AND p.permission_key IN ('documents.read', 'documents.write', 'finance.read', 'finance.write'))
    OR (r.role_key = 'tenant_admin' AND p.permission_key IN ('documents.read', 'documents.write', 'documents.restricted.read', 'finance.read', 'finance.write', 'tenant.members.manage', 'tenant.configuration.manage'))
ON CONFLICT DO NOTHING;

-- Core object/document graph
CREATE TABLE IF NOT EXISTS object_class (
    class_id            BIGSERIAL PRIMARY KEY,
    class_name          VARCHAR(256) NOT NULL UNIQUE,
    parent_class_id     BIGINT REFERENCES object_class(class_id) ON DELETE SET NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    entry_date          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_object_class_name ON object_class(class_name);
CREATE INDEX IF NOT EXISTS idx_object_class_parent ON object_class(parent_class_id);
CREATE INDEX IF NOT EXISTS idx_object_class_metadata_gin ON object_class USING GIN(metadata);

CREATE TABLE IF NOT EXISTS object_instance (
    object_id           BIGSERIAL PRIMARY KEY,
    tenant_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    object_name         VARCHAR(256) NOT NULL,
    canonical_full_name VARCHAR(256),
    class_name          VARCHAR(256) NOT NULL REFERENCES object_class(class_name) ON DELETE RESTRICT,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              VARCHAR(32) NOT NULL DEFAULT 'active',
    valid_from          DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until         DATE,
    valid_daterange     daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    entry_date          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, object_name, class_name)
);

CREATE INDEX IF NOT EXISTS idx_object_instance_name ON object_instance(object_name);
CREATE INDEX IF NOT EXISTS idx_object_instance_name_lower ON object_instance (LOWER(object_name));
CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name ON object_instance(canonical_full_name);
CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name_lower ON object_instance (LOWER(canonical_full_name));
CREATE INDEX IF NOT EXISTS idx_object_instance_class_name ON object_instance(class_name);
CREATE INDEX IF NOT EXISTS idx_object_instance_metadata_gin ON object_instance USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_object_instance_name_trgm ON object_instance USING GIN (LOWER(object_name) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name_trgm ON object_instance USING GIN (LOWER(canonical_full_name) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_object_instance_status ON object_instance(status);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'object_instance' AND column_name = 'valid_daterange'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_object_instance_valid_daterange ON object_instance USING GIST (valid_daterange);
    END IF;
END $$;

-- User-authored executable configuration is tenant-owned, not global.
DO $$
DECLARE t TEXT;
DECLARE policy_expr TEXT := 'tenant_id = COALESCE(NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid, ''00000000-0000-0000-0000-000000000001''::uuid)';
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'business_rules','solf_clauses','solf_workflow_registry','solf_workflow_versions',
        'solf_workflow_steps','business_rule_workflow_links','workflow_resource_alias',
        'workflow_extension_pack','workflow_extension_version','workflow_extension_rule',
        'workflow_extension_attachment','workflow_generated_python_script',
        'workflow_generated_python_script_audit','action_resolution_plan',
        'workflow_action_log','interaction_clarification_thread','interaction_clarification_turn'
    ] LOOP
        IF to_regclass(format('public.%I', t)) IS NULL THEN
            CONTINUE;
        END IF;
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT ''00000000-0000-0000-0000-000000000001''', t);
        EXECUTE format('ALTER TABLE %I ALTER COLUMN tenant_id SET DEFAULT COALESCE(NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid, ''00000000-0000-0000-0000-000000000001''::uuid)', t);
        IF t = ANY(ARRAY['business_rules','solf_clauses','solf_workflow_registry','solf_workflow_versions','solf_workflow_steps','business_rule_workflow_links','workflow_resource_alias','workflow_extension_pack','workflow_extension_version','workflow_extension_rule','workflow_extension_attachment','workflow_generated_python_script']) THEN
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS configuration_scope VARCHAR(16) NOT NULL DEFAULT ''tenant''', t);
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', t, 'ck_' || t || '_configuration_scope');
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I CHECK (configuration_scope IN (''tenant'', ''global''))', t, 'ck_' || t || '_configuration_scope');
        END IF;
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(tenant_id)', 'idx_' || t || '_tenant_id', t);
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format('DROP POLICY IF EXISTS ikos_tenant_isolation ON %I', t);
        IF t = ANY(ARRAY['business_rules','solf_clauses','solf_workflow_registry','solf_workflow_versions','solf_workflow_steps','business_rule_workflow_links','workflow_resource_alias','workflow_extension_pack','workflow_extension_version','workflow_extension_rule','workflow_extension_attachment','workflow_generated_python_script']) THEN
            EXECUTE format('CREATE POLICY ikos_tenant_isolation ON %I USING ((%s AND configuration_scope = ''tenant'') OR configuration_scope = ''global'') WITH CHECK (%s AND ((configuration_scope = ''tenant'' AND ''tenant.configuration.manage'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '',''))) OR (configuration_scope = ''global'' AND ''platform.configuration.publish'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '','')))))', t, policy_expr, policy_expr);
        ELSE
            EXECUTE format('CREATE POLICY ikos_tenant_isolation ON %I USING (%s) WITH CHECK (%s)', t, policy_expr, policy_expr);
        END IF;
    END LOOP;

    IF to_regclass('public.solf_clauses') IS NOT NULL THEN
        ALTER TABLE solf_clauses DROP CONSTRAINT IF EXISTS solf_clauses_clause_name_entity_class_key;
        DROP INDEX IF EXISTS uq_solf_clauses_tenant_name;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_clauses_tenant_name ON solf_clauses(tenant_id, clause_name, entity_class);
    END IF;
    IF to_regclass('public.solf_workflow_registry') IS NOT NULL THEN
        ALTER TABLE solf_workflow_registry DROP CONSTRAINT IF EXISTS solf_workflow_registry_workflow_key_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_workflow_registry_tenant_key ON solf_workflow_registry(tenant_id, workflow_key);
    END IF;
    IF to_regclass('public.workflow_extension_pack') IS NOT NULL THEN
        ALTER TABLE workflow_extension_pack DROP CONSTRAINT IF EXISTS workflow_extension_pack_extension_key_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_extension_pack_tenant_key ON workflow_extension_pack(tenant_id, extension_key);
    END IF;
    IF to_regclass('public.workflow_generated_python_script') IS NOT NULL THEN
        ALTER TABLE workflow_generated_python_script DROP CONSTRAINT IF EXISTS workflow_generated_python_script_script_key_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_generated_script_tenant_key ON workflow_generated_python_script(tenant_id, script_key);
    END IF;

    IF to_regclass('public.solf_workflow_registry') IS NOT NULL THEN
        ALTER TABLE solf_workflow_registry DROP CONSTRAINT IF EXISTS solf_workflow_registry_workflow_key_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_workflow_registry_tenant_key ON solf_workflow_registry(tenant_id, workflow_key);
    END IF;
    IF to_regclass('public.workflow_extension_pack') IS NOT NULL THEN
        ALTER TABLE workflow_extension_pack DROP CONSTRAINT IF EXISTS workflow_extension_pack_extension_key_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_extension_pack_tenant_key ON workflow_extension_pack(tenant_id, extension_key);
    END IF;
    IF to_regclass('public.workflow_generated_python_script') IS NOT NULL THEN
        ALTER TABLE workflow_generated_python_script DROP CONSTRAINT IF EXISTS workflow_generated_python_script_script_key_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_generated_script_tenant_key ON workflow_generated_python_script(tenant_id, script_key);
    END IF;
    IF to_regclass('public.solf_clauses') IS NOT NULL THEN
        ALTER TABLE solf_clauses DROP CONSTRAINT IF EXISTS solf_clauses_clause_name_entity_class_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_clauses_tenant_name ON solf_clauses(tenant_id, clause_name, entity_class);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS object_relationship (
    relationship_id     BIGSERIAL PRIMARY KEY,
    tenant_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    relationship_name   VARCHAR(64) NOT NULL,
    relationship_cat    VARCHAR(64) NOT NULL,
    src_object_id       BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    tar_object_id       BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    confidence          NUMERIC(5,4),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    valid_from          DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until         DATE,
    valid_daterange     daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    entry_date          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, relationship_name, src_object_id, tar_object_id)
);

CREATE INDEX IF NOT EXISTS idx_object_relationship_name ON object_relationship(relationship_name);
CREATE INDEX IF NOT EXISTS idx_object_relationship_src ON object_relationship(src_object_id);
CREATE INDEX IF NOT EXISTS idx_object_relationship_tar ON object_relationship(tar_object_id);
CREATE INDEX IF NOT EXISTS idx_object_relationship_cat ON object_relationship(relationship_cat);
CREATE INDEX IF NOT EXISTS idx_object_relationship_metadata_gin ON object_relationship USING GIN(metadata);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'object_relationship' AND column_name = 'valid_daterange'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_object_relationship_valid_daterange ON object_relationship USING GIST (valid_daterange);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS document (
    doc_id                BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    doc_name              VARCHAR(512),
    doc_key               VARCHAR(512),
    doc_path              VARCHAR(1024),
    markdown_path         VARCHAR(1024),
    doc_cat               VARCHAR(128),
    doc_type              VARCHAR(128),
    doc_date              DATE,
    doc_desc              TEXT,
    doc_theme             TEXT,
    keyword_text          TEXT,
    keyword_vec           TSVECTOR,
    identifiers_kv        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                VARCHAR(32) NOT NULL DEFAULT 'active',
    access_level          VARCHAR(32) NOT NULL DEFAULT 'tenant' CHECK (access_level IN ('tenant','finance','restricted')),
    valid_from            DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until           DATE,
    valid_daterange       daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    entry_date            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_docs_cat ON document(doc_cat);
CREATE INDEX IF NOT EXISTS idx_docs_tenant_created ON document(tenant_id, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_docs_type ON document(doc_type);
CREATE INDEX IF NOT EXISTS idx_docs_key ON document(doc_key);
CREATE INDEX IF NOT EXISTS idx_docs_key_lower ON document (LOWER(doc_key));
CREATE INDEX IF NOT EXISTS idx_docs_key_trgm ON document USING GIN (LOWER(doc_key) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_path ON document(doc_path);
CREATE INDEX IF NOT EXISTS idx_docs_markdown_path ON document(markdown_path);
CREATE UNIQUE INDEX IF NOT EXISTS uq_document_tenant_doc_path_nonnull ON document(tenant_id, doc_path) WHERE doc_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_docs_date ON document(doc_date);
CREATE INDEX IF NOT EXISTS idx_docs_identifiers_gin ON document USING GIN(identifiers_kv);
CREATE INDEX IF NOT EXISTS idx_docs_metadata_gin ON document USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_docs_keyword_vec ON document USING GIN(keyword_vec);
CREATE INDEX IF NOT EXISTS idx_docs_keyword_trgm ON document USING GIN (LOWER(keyword_text) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_user_description_trgm ON document USING GIN (LOWER(metadata->>'user_description') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_user_tags_trgm ON document USING GIN (LOWER(metadata->>'user_tags') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_status ON document(status);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'document' AND column_name = 'valid_daterange'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_docs_valid_daterange ON document USING GIST (valid_daterange);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS document_content (
    content_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    doc_id BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    content_type VARCHAR(64) NOT NULL,
    content_version INTEGER NOT NULL DEFAULT 1,
    content_text TEXT,
    content_bytes BYTEA,
    object_uri TEXT,
    checksum_sha256 VARCHAR(64),
    source_name VARCHAR(512),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_by VARCHAR(256),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (content_text IS NOT NULL OR content_bytes IS NOT NULL OR object_uri IS NOT NULL),
    UNIQUE (tenant_id, doc_id, content_type, content_version)
);
CREATE INDEX IF NOT EXISTS idx_document_content_tenant_doc ON document_content(tenant_id, doc_id);
CREATE INDEX IF NOT EXISTS idx_document_content_active ON document_content(tenant_id, doc_id, content_type, content_version DESC) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS part (
    part_id               BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    doc_id                BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    part_key              VARCHAR(128) NOT NULL,
    class_id              BIGINT REFERENCES object_class(class_id) ON DELETE CASCADE,
    object_id             BIGINT REFERENCES object_instance(object_id) ON DELETE CASCADE,
    relationship_id       BIGINT REFERENCES object_relationship(relationship_id) ON DELETE CASCADE,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    entry_date            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, doc_id, part_key)
);

CREATE INDEX IF NOT EXISTS idx_part_doc ON part(doc_id);
CREATE INDEX IF NOT EXISTS idx_part_class ON part(class_id);
CREATE INDEX IF NOT EXISTS idx_part_object ON part(object_id);
CREATE INDEX IF NOT EXISTS idx_part_relationship ON part(relationship_id);
CREATE INDEX IF NOT EXISTS idx_part_metadata_gin ON part USING GIN(metadata);

CREATE TABLE IF NOT EXISTS document_table_cell (
    cell_id               BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    doc_id                BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    table_index           INTEGER NOT NULL,
    row_index             INTEGER NOT NULL,
    row_label             TEXT,
    column_index          INTEGER NOT NULL,
    column_label          TEXT,
    cell_value            TEXT,
    source_kind           VARCHAR(32) NOT NULL DEFAULT 'document',
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, doc_id, table_index, row_index, column_index)
);

CREATE INDEX IF NOT EXISTS idx_doc_table_cell_doc_id ON document_table_cell(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_table_cell_row_label_trgm ON document_table_cell USING GIN (LOWER(COALESCE(row_label, '')) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_doc_table_cell_column_label_trgm ON document_table_cell USING GIN (LOWER(COALESCE(column_label, '')) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_doc_table_cell_metadata_gin ON document_table_cell USING GIN(metadata);

CREATE TABLE IF NOT EXISTS attribute (
    attr_id              BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    src_id               BIGINT NOT NULL,
    src_type             VARCHAR(16) NOT NULL CHECK (src_type IN ('part','document','class','object','relationship')),
    attr_type            VARCHAR(64) NOT NULL,
    valid_from           DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until          DATE,
    valid_range tsrange GENERATED ALWAYS AS (tsrange(valid_from, valid_until)) STORED,
    valid_daterange      daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    attr_json            JSONB NOT NULL,
    search_txt           TEXT,
    search_vec           TSVECTOR,
    entry_date           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_attr_src ON attribute(src_type, src_id);
CREATE INDEX IF NOT EXISTS idx_attr_type ON attribute(attr_type);
CREATE INDEX IF NOT EXISTS idx_attr_json_gin ON attribute USING GIN(attr_json);
CREATE INDEX IF NOT EXISTS idx_attr_search_vec ON attribute USING GIN(search_vec);
CREATE INDEX IF NOT EXISTS idx_attributes_valid_range ON attribute USING GIST (valid_range);
CREATE INDEX IF NOT EXISTS idx_attributes_valid_daterange ON attribute USING GIST (valid_daterange);

CREATE TABLE IF NOT EXISTS resolution_ambiguity_queue (
    queue_id             BIGSERIAL PRIMARY KEY,
    entity_id            VARCHAR(128),
    object_name          VARCHAR(256) NOT NULL,
    class_name           VARCHAR(256) NOT NULL,
    doc_id               BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    payload              JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolution_result    JSONB NOT NULL DEFAULT '{}'::jsonb,
    status               VARCHAR(32) NOT NULL DEFAULT 'pending',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_status ON resolution_ambiguity_queue(status);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_doc_id ON resolution_ambiguity_queue(doc_id);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_class_name ON resolution_ambiguity_queue(class_name);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_created_at ON resolution_ambiguity_queue(created_at);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_payload_gin ON resolution_ambiguity_queue USING GIN(payload);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_result_gin ON resolution_ambiguity_queue USING GIN(resolution_result);

CREATE TABLE IF NOT EXISTS tx_match_index_pending (
    pending_id            BIGSERIAL PRIMARY KEY,
    doc_id                BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    source                VARCHAR(64) NOT NULL DEFAULT 'ingestion',
    status                VARCHAR(32) NOT NULL DEFAULT 'pending',
    payload               JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message         TEXT,
    retry_count           INTEGER NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tx_match_pending_status ON tx_match_index_pending(status);
CREATE INDEX IF NOT EXISTS idx_tx_match_pending_doc_id ON tx_match_index_pending(doc_id);
CREATE INDEX IF NOT EXISTS idx_tx_match_pending_created_at ON tx_match_index_pending(created_at);
CREATE INDEX IF NOT EXISTS idx_tx_match_pending_payload_gin ON tx_match_index_pending USING GIN(payload);

CREATE TABLE IF NOT EXISTS object_alias (
    alias_id              BIGSERIAL PRIMARY KEY,
    primary_object_id     BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    alias_object_id       BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    alias_type            VARCHAR(64) NOT NULL DEFAULT 'same_entity',
    confidence            NUMERIC(5,4),
    status                VARCHAR(32) NOT NULL DEFAULT 'active',
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (primary_object_id <> alias_object_id),
    UNIQUE (primary_object_id, alias_object_id)
);

CREATE INDEX IF NOT EXISTS idx_object_alias_primary ON object_alias(primary_object_id);
CREATE INDEX IF NOT EXISTS idx_object_alias_alias ON object_alias(alias_object_id);
CREATE INDEX IF NOT EXISTS idx_object_alias_status ON object_alias(status);
CREATE INDEX IF NOT EXISTS idx_object_alias_type ON object_alias(alias_type);
CREATE INDEX IF NOT EXISTS idx_object_alias_metadata_gin ON object_alias USING GIN(metadata);

CREATE TABLE IF NOT EXISTS semantic_patterns (
    pattern_id           BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    pattern_text         VARCHAR(512) NOT NULL,
    semantic_concept     VARCHAR(128) NOT NULL,
    mapped_attributes    JSONB NOT NULL DEFAULT '{}'::jsonb,
    computation_rule     TEXT,
    entity_class         VARCHAR(128),
    source_type          VARCHAR(32) NOT NULL DEFAULT 'seeded' CHECK (source_type IN ('seeded','learned','manual')),
    confidence           NUMERIC(5,4) NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    pattern_language     VARCHAR(32) NOT NULL DEFAULT 'en',
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by           VARCHAR(128),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, pattern_text, entity_class, pattern_language)
);

CREATE INDEX IF NOT EXISTS idx_semantic_patterns_text ON semantic_patterns(pattern_text);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_text_lower ON semantic_patterns (LOWER(pattern_text));
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_concept ON semantic_patterns(semantic_concept);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_entity_class ON semantic_patterns(entity_class);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_source_type ON semantic_patterns(source_type);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_confidence ON semantic_patterns(confidence);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_language ON semantic_patterns(pattern_language);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_metadata_gin ON semantic_patterns USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_mapped_attr_gin ON semantic_patterns USING GIN(mapped_attributes);
CREATE UNIQUE INDEX IF NOT EXISTS uq_semantic_patterns_tenant_pattern_id ON semantic_patterns(tenant_id, pattern_id);

CREATE TABLE IF NOT EXISTS pattern_synonyms (
    synonym_id           BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    pattern_id           BIGINT NOT NULL,
    synonym_text         VARCHAR(512) NOT NULL,
    language             VARCHAR(32) NOT NULL DEFAULT 'en',
    semantic_distance    NUMERIC(5,4) NOT NULL DEFAULT 0.0 CHECK (semantic_distance >= 0 AND semantic_distance <= 1),
    match_type           VARCHAR(32) NOT NULL DEFAULT 'exact' CHECK (match_type IN ('exact','template','fuzzy','semantic')),
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, pattern_id, synonym_text, language),
    FOREIGN KEY (tenant_id, pattern_id) REFERENCES semantic_patterns(tenant_id, pattern_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_pattern_id ON pattern_synonyms(pattern_id);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_text ON pattern_synonyms(synonym_text);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_text_lower ON pattern_synonyms (LOWER(synonym_text));
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_language ON pattern_synonyms(language);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_match_type ON pattern_synonyms(match_type);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_semantic_distance ON pattern_synonyms(semantic_distance);

CREATE TABLE IF NOT EXISTS semantic_terms (
    term_id              BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    kind                 VARCHAR(32) NOT NULL CHECK (kind IN ('attribute','relationship','entity_type','category')),
    canonical_name       VARCHAR(256) NOT NULL,
    term_text            VARCHAR(512) NOT NULL,
    language             VARCHAR(32) NOT NULL DEFAULT 'und',
    source_type          VARCHAR(64) NOT NULL DEFAULT 'ingest' CHECK (source_type IN ('seeded','ingest','solf_clause','manual','llm_generated')),
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, kind, canonical_name, term_text, language)
);

CREATE INDEX IF NOT EXISTS idx_semantic_terms_kind ON semantic_terms(kind);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_canonical_name ON semantic_terms(canonical_name);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_term_text ON semantic_terms(term_text);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_term_text_lower ON semantic_terms (LOWER(term_text));
CREATE INDEX IF NOT EXISTS idx_semantic_terms_source_type ON semantic_terms(source_type);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_metadata_gin ON semantic_terms USING GIN(metadata);

CREATE TABLE IF NOT EXISTS query_term_alias (
    alias_id              BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    alias_text            VARCHAR(512) NOT NULL,
    canonical_name        VARCHAR(256) NOT NULL,
    kind                  VARCHAR(32) NOT NULL CHECK (kind IN ('attribute','relationship','entity_type','category')),
    language              VARCHAR(32) NOT NULL DEFAULT 'und',
    priority              INTEGER NOT NULL DEFAULT 100,
    source_type           VARCHAR(64) NOT NULL DEFAULT 'manual',
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT true,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, alias_text, kind, language)
);

CREATE INDEX IF NOT EXISTS idx_query_term_alias_text ON query_term_alias(alias_text);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_text_lower ON query_term_alias (LOWER(alias_text));
CREATE INDEX IF NOT EXISTS idx_query_term_alias_canonical_name ON query_term_alias(canonical_name);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_kind ON query_term_alias(kind);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_is_active ON query_term_alias(is_active);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_priority ON query_term_alias(priority);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_metadata_gin ON query_term_alias USING GIN(metadata);

CREATE TABLE IF NOT EXISTS query_alias_candidate (
    candidate_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    alias_text VARCHAR(512) NOT NULL,
    canonical_name VARCHAR(256) NOT NULL,
    kind VARCHAR(32) NOT NULL CHECK (kind IN ('attribute','relationship','entity_type','category','document_hint')),
    language VARCHAR(32) NOT NULL DEFAULT 'und',
    confidence NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    first_seen_doc_id BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    last_seen_doc_id BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'proposed' CHECK (status IN ('proposed','approved','rejected')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, alias_text, canonical_name, kind, language)
);
CREATE INDEX IF NOT EXISTS idx_query_alias_candidate_tenant_status ON query_alias_candidate(tenant_id, status);

CREATE TABLE IF NOT EXISTS source_schema_taxonomy (
    taxonomy_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    source_id BIGINT NOT NULL,
    schema_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    column_name VARCHAR(256),
    category_name VARCHAR(256) NOT NULL,
    role_name VARCHAR(64) NOT NULL DEFAULT 'entity_candidate' CHECK (role_name IN ('entity_candidate','identity','attribute','filter','measure','temporal','relationship_key')),
    confidence NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, source_id, schema_name, table_name, column_name, category_name, role_name)
);
CREATE INDEX IF NOT EXISTS idx_source_schema_taxonomy_tenant_category ON source_schema_taxonomy(tenant_id, category_name, role_name, is_active);
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_schema_taxonomy_tenant_key
ON source_schema_taxonomy(tenant_id, source_id, schema_name, table_name, (COALESCE(column_name, '')), category_name, role_name);

CREATE TABLE IF NOT EXISTS category_registry (
    category_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_name VARCHAR(256) NOT NULL,
    source_scope VARCHAR(32) NOT NULL CHECK (source_scope IN ('internal','external','document_kb')),
    access_method VARCHAR(32) NOT NULL CHECK (access_method IN ('sql','semantic_lookup','rag','document_facet')),
    source_id BIGINT,
    schema_name VARCHAR(256),
    table_name VARCHAR(256),
    column_name VARCHAR(256),
    entity_types JSONB NOT NULL DEFAULT '[]'::jsonb,
    attributes JSONB NOT NULL DEFAULT '[]'::jsonb,
    document_types JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, category_name, source_scope, access_method, source_id, schema_name, table_name, column_name)
);
CREATE INDEX IF NOT EXISTS idx_category_registry_tenant_name ON category_registry(tenant_id, category_name);
CREATE UNIQUE INDEX IF NOT EXISTS uq_category_registry_tenant_key
ON category_registry(tenant_id, category_name, source_scope, access_method, (COALESCE(source_id, 0)), (COALESCE(schema_name, '')), (COALESCE(table_name, '')), (COALESCE(column_name, '')));
CREATE INDEX IF NOT EXISTS idx_category_registry_coverage ON category_registry USING GIN(entity_types, attributes, document_types);

CREATE TABLE IF NOT EXISTS categories (
    category_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    name VARCHAR(256) NOT NULL,
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name),
    UNIQUE (tenant_id, category_id)
);

CREATE TABLE IF NOT EXISTS category_sources (
    category_source_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_id BIGINT NOT NULL,
    source_type VARCHAR(32) NOT NULL CHECK (source_type IN ('internal_db','external_db','documents')),
    source_id BIGINT,
    access_method VARCHAR(32) NOT NULL CHECK (access_method IN ('sql','semantic_lookup','rag','document_facet')),
    priority INTEGER NOT NULL DEFAULT 100,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, category_source_id),
    FOREIGN KEY (tenant_id, category_id) REFERENCES categories(tenant_id, category_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_category_sources_tenant_identity ON category_sources(tenant_id, category_id, source_type, COALESCE(source_id,0), access_method);

CREATE TABLE IF NOT EXISTS category_entities (
    category_entity_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_source_id BIGINT NOT NULL,
    entity_type VARCHAR(256) NOT NULL,
    source_table VARCHAR(256),
    source_column VARCHAR(256),
    role_name VARCHAR(64) NOT NULL DEFAULT 'entity_candidate',
    confidence NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    FOREIGN KEY (tenant_id, category_source_id) REFERENCES category_sources(tenant_id, category_source_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_category_entities_tenant_identity ON category_entities(tenant_id, category_source_id, entity_type, COALESCE(source_table,''), COALESCE(source_column,''), role_name);

CREATE TABLE IF NOT EXISTS category_attributes (
    category_attribute_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_source_id BIGINT NOT NULL,
    attribute_name VARCHAR(256) NOT NULL,
    canonical_name VARCHAR(256) NOT NULL,
    source_table VARCHAR(256),
    source_column VARCHAR(256),
    role_name VARCHAR(64) NOT NULL DEFAULT 'attribute',
    confidence NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    FOREIGN KEY (tenant_id, category_source_id) REFERENCES category_sources(tenant_id, category_source_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_category_attributes_tenant_identity ON category_attributes(tenant_id, category_source_id, canonical_name, COALESCE(source_table,''), COALESCE(source_column,''), role_name);

-- Query intent policy is kernel-wide, not organization vocabulary.
CREATE TABLE IF NOT EXISTS query_pattern_registry (
    pattern_id BIGSERIAL PRIMARY KEY,
    pattern_key VARCHAR(128) NOT NULL UNIQUE,
    display_name VARCHAR(256) NOT NULL,
    intent_key VARCHAR(128) NOT NULL,
    description TEXT,
    allowed_source_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_access_methods JSONB NOT NULL DEFAULT '[]'::jsonb,
    priority INTEGER NOT NULL DEFAULT 100,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- SOLF rule/inference schema
CREATE TABLE IF NOT EXISTS fact (
    fact_id              BIGSERIAL PRIMARY KEY,
    predicate_id         BIGINT,
    subject_id           BIGINT,
    object_id            BIGINT,
    doc_id               BIGINT,
    valid_from           DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until          DATE,
    valid_range tsrange GENERATED ALWAYS AS (tsrange(valid_from, valid_until)) STORED,
    confidence           NUMERIC(5,4),
    metadata             JSONB,
    entry_date           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_subject ON fact(subject_id);
CREATE INDEX IF NOT EXISTS idx_fact_object ON fact(object_id);
CREATE INDEX IF NOT EXISTS idx_fact_valid_range ON fact USING GIST (valid_range);
CREATE INDEX IF NOT EXISTS idx_fact_metadata_gin ON fact USING GIN(metadata);

CREATE TABLE IF NOT EXISTS predicate (
    predicate_id         BIGSERIAL PRIMARY KEY,
    predicate_name       VARCHAR(64) NOT NULL UNIQUE,
    description          TEXT
);

CREATE TABLE IF NOT EXISTS clause (
    clause_id            BIGSERIAL PRIMARY KEY,
    clause_type          VARCHAR(32) NOT NULL,
    predicate_id         BIGINT,
    rule_json            JSONB NOT NULL,
    entry_date           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clause_type ON clause(clause_type);
CREATE INDEX IF NOT EXISTS idx_clause_predicate ON clause(predicate_id);
CREATE INDEX IF NOT EXISTS idx_clause_rule_gin ON clause USING GIN(rule_json);

CREATE TABLE IF NOT EXISTS solf_clauses (
    clause_id            BIGSERIAL PRIMARY KEY,
    clause_name          VARCHAR(256) NOT NULL,
    clause_type          VARCHAR(32) NOT NULL CHECK (clause_type IN ('query_pattern','resolve_policy','ingest_rule','computation_rule')),
    entity_class         VARCHAR(128),
    clause_body          TEXT NOT NULL,
    referenced_patterns  BIGINT[] DEFAULT '{}',
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active            BOOLEAN NOT NULL DEFAULT true,
    created_by           VARCHAR(128),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (clause_name, entity_class)
);

CREATE INDEX IF NOT EXISTS idx_solf_clauses_name ON solf_clauses(clause_name);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_type ON solf_clauses(clause_type);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_entity_class ON solf_clauses(entity_class);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_is_active ON solf_clauses(is_active);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_created_at ON solf_clauses(created_at);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_metadata_gin ON solf_clauses USING GIN(metadata);

CREATE TABLE IF NOT EXISTS business_rules (
    rule_id               BIGSERIAL PRIMARY KEY,
    rule_name             VARCHAR(256) NOT NULL,
    rule_text             TEXT NOT NULL,
    structured_rule       JSONB NOT NULL DEFAULT '{}'::jsonb,
    solf_script           TEXT NOT NULL DEFAULT '',
    scope                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT true,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_business_rules_active ON business_rules(is_active);
CREATE INDEX IF NOT EXISTS idx_business_rules_name ON business_rules(rule_name);
CREATE INDEX IF NOT EXISTS idx_business_rules_scope_gin ON business_rules USING GIN(scope);
CREATE INDEX IF NOT EXISTS idx_business_rules_structured_gin ON business_rules USING GIN(structured_rule);
CREATE INDEX IF NOT EXISTS idx_business_rules_metadata_gin ON business_rules USING GIN(metadata);

-- SOLF workflow and execution runtime
CREATE TABLE IF NOT EXISTS solf_workflow_registry (
    workflow_id           BIGSERIAL PRIMARY KEY,
    workflow_key          VARCHAR(256) NOT NULL UNIQUE,
    workflow_name         VARCHAR(256) NOT NULL,
    description           TEXT NOT NULL DEFAULT '',
    domain                VARCHAR(64),
    status                VARCHAR(32) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','review','published','deprecated','archived')),
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_status ON solf_workflow_registry(status);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_domain ON solf_workflow_registry(domain);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_active ON solf_workflow_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_metadata_gin ON solf_workflow_registry USING GIN(metadata);

CREATE TABLE IF NOT EXISTS solf_workflow_versions (
    workflow_version_id   BIGSERIAL PRIMARY KEY,
    workflow_id           BIGINT NOT NULL REFERENCES solf_workflow_registry(workflow_id) ON DELETE CASCADE,
    version_no            INTEGER NOT NULL,
    rule_id               BIGINT REFERENCES business_rules(rule_id) ON DELETE SET NULL,
    graph_spec            JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_contract        JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_contract       JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_workflow_id ON solf_workflow_versions(workflow_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_rule_id ON solf_workflow_versions(rule_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_active ON solf_workflow_versions(is_active);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_metadata_gin ON solf_workflow_versions USING GIN(metadata);

CREATE TABLE IF NOT EXISTS solf_workflow_steps (
    step_id               BIGSERIAL PRIMARY KEY,
    workflow_version_id   BIGINT NOT NULL REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE CASCADE,
    step_order            INTEGER NOT NULL,
    step_key              VARCHAR(256) NOT NULL,
    step_kind             VARCHAR(32) NOT NULL CHECK (step_kind IN ('clause','class_transform','class_generate','class_iterate','python_binding')),
    clause_id             BIGINT REFERENCES solf_clauses(clause_id) ON DELETE SET NULL,
    clause_name           VARCHAR(256),
    input_class           VARCHAR(128),
    output_class          VARCHAR(128),
    operation             VARCHAR(64),
    python_module         VARCHAR(256),
    python_function       VARCHAR(256),
    config                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_version_id, step_order),
    UNIQUE (workflow_version_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_version_id ON solf_workflow_steps(workflow_version_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_kind ON solf_workflow_steps(step_kind);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_clause_id ON solf_workflow_steps(clause_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_config_gin ON solf_workflow_steps USING GIN(config);

CREATE TABLE IF NOT EXISTS workflow_pipeline_run (
    run_id              BIGSERIAL PRIMARY KEY,
    tenant_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    workflow_version_id BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE SET NULL,
    workflow_key        VARCHAR(256),
    run_status          VARCHAR(32) NOT NULL DEFAULT 'pending'
                        CHECK (run_status IN ('pending','running','paused','completed','failed','cancelled')),
    paused_at_step_key  VARCHAR(256),
    input_context       JSONB NOT NULL DEFAULT '{}'::jsonb,
    current_context     JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_context      JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_by          VARCHAR(128),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_status ON workflow_pipeline_run(run_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_tenant_status ON workflow_pipeline_run(tenant_id, run_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_workflow_version ON workflow_pipeline_run(workflow_version_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_workflow_key ON workflow_pipeline_run(workflow_key);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_created_at ON workflow_pipeline_run(created_at);

CREATE TABLE IF NOT EXISTS workflow_pipeline_run_step (
    step_run_id         BIGSERIAL PRIMARY KEY,
    run_id              BIGINT NOT NULL REFERENCES workflow_pipeline_run(run_id) ON DELETE CASCADE,
    step_key            VARCHAR(256) NOT NULL,
    step_order          INTEGER NOT NULL,
    step_status         VARCHAR(32) NOT NULL DEFAULT 'pending'
                        CHECK (step_status IN ('pending','running','completed','paused','failed','skipped')),
    pause_reason        VARCHAR(64),
    interaction_prompt  TEXT,
    missing_data_desc   TEXT,
    required_doc_types  JSONB NOT NULL DEFAULT '[]'::jsonb,
    input_snapshot      JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_snapshot     JSONB NOT NULL DEFAULT '{}'::jsonb,
    user_response       JSONB NOT NULL DEFAULT '{}'::jsonb,
    doc_refs            JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_message       TEXT,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_step_run_id ON workflow_pipeline_run_step(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_step_status ON workflow_pipeline_run_step(step_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_step_order ON workflow_pipeline_run_step(run_id, step_order);

CREATE TABLE IF NOT EXISTS workflow_pipeline_run_document_link (
    link_id              BIGSERIAL PRIMARY KEY,
    run_id               BIGINT NOT NULL REFERENCES workflow_pipeline_run(run_id) ON DELETE CASCADE,
    workflow_version_id  BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE SET NULL,
    workflow_key         VARCHAR(256),
    document_id          BIGINT NOT NULL,
    source               VARCHAR(32) NOT NULL DEFAULT 'system'
                         CHECK (source IN ('input_context','resume','step_response','system')),
    step_key             VARCHAR(256) NOT NULL DEFAULT '',
    linked_by            VARCHAR(128),
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, document_id, source, step_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_run_id ON workflow_pipeline_run_document_link(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_doc_id ON workflow_pipeline_run_document_link(document_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_workflow_key ON workflow_pipeline_run_document_link(workflow_key);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_source ON workflow_pipeline_run_document_link(source);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_metadata_gin ON workflow_pipeline_run_document_link USING GIN(metadata);

CREATE TABLE IF NOT EXISTS workflow_generated_python_script (
    script_id BIGSERIAL PRIMARY KEY,
    script_key VARCHAR(256) NOT NULL UNIQUE,
    script_name VARCHAR(256) NOT NULL,
    script_source TEXT NOT NULL,
    entrypoint VARCHAR(128) NOT NULL DEFAULT 'run',
    approval_status VARCHAR(32) NOT NULL DEFAULT 'draft' CHECK (approval_status IN ('draft','approved','deprecated','archived')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wgps_script_key ON workflow_generated_python_script(script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_approval_status ON workflow_generated_python_script(approval_status);
CREATE INDEX IF NOT EXISTS idx_wgps_is_active ON workflow_generated_python_script(is_active);
CREATE INDEX IF NOT EXISTS idx_wgps_metadata_gin ON workflow_generated_python_script USING GIN(metadata);

CREATE TABLE IF NOT EXISTS workflow_generated_python_script_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT,
    step_key VARCHAR(256),
    script_id BIGINT REFERENCES workflow_generated_python_script(script_id) ON DELETE SET NULL,
    script_key VARCHAR(256),
    entrypoint VARCHAR(128),
    execution_status VARCHAR(32) NOT NULL,
    duration_ms INTEGER,
    input_hash VARCHAR(64),
    output_hash VARCHAR(64),
    input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key VARCHAR(256),
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wgps_audit_run_id ON workflow_generated_python_script_audit(run_id);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_step_key ON workflow_generated_python_script_audit(step_key);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_script_key ON workflow_generated_python_script_audit(script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_status ON workflow_generated_python_script_audit(execution_status);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_executed_at ON workflow_generated_python_script_audit(executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_metadata_gin ON workflow_generated_python_script_audit USING GIN(metadata);
CREATE UNIQUE INDEX IF NOT EXISTS uq_wgps_audit_idempotency
ON workflow_generated_python_script_audit(run_id, step_key, script_key, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS business_rule_workflow_links (
    link_id               BIGSERIAL PRIMARY KEY,
    rule_id               BIGINT NOT NULL REFERENCES business_rules(rule_id) ON DELETE CASCADE,
    workflow_id           BIGINT NOT NULL REFERENCES solf_workflow_registry(workflow_id) ON DELETE CASCADE,
    workflow_version_id   BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE CASCADE,
    link_type             VARCHAR(32) NOT NULL DEFAULT 'uses' CHECK (link_type IN ('uses','creates','extends','overrides')),
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (rule_id, workflow_id, workflow_version_id, link_type)
);

CREATE INDEX IF NOT EXISTS idx_business_rule_workflow_links_rule_id ON business_rule_workflow_links(rule_id);

-- Interaction + event runtime
CREATE TABLE IF NOT EXISTS workflow_action_log (
    id BIGSERIAL PRIMARY KEY,
    action_name TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    success BOOLEAN NOT NULL,
    message TEXT,
    result_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_workflow_action_log_name ON workflow_action_log(action_name);
CREATE INDEX IF NOT EXISTS idx_workflow_action_log_created ON workflow_action_log(created_at);

CREATE TABLE IF NOT EXISTS interaction_clarification_thread (
    thread_id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(128),
    channel VARCHAR(32) NOT NULL DEFAULT 'chat',
    user_ref VARCHAR(128),
    original_query TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending_clarification'
        CHECK (status IN ('pending_clarification','resolved','abandoned')),
    reason_code VARCHAR(64) NOT NULL DEFAULT 'ambiguous_value',
    clarification_question TEXT NOT NULL,
    expected_input_type VARCHAR(64) NOT NULL DEFAULT 'free_text',
    ambiguity_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    provenance_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_clar_thread_status ON interaction_clarification_thread(status);
CREATE INDEX IF NOT EXISTS idx_clar_thread_created_at ON interaction_clarification_thread(created_at DESC);

CREATE TABLE IF NOT EXISTS interaction_clarification_turn (
    turn_id BIGSERIAL PRIMARY KEY,
    thread_id BIGINT NOT NULL REFERENCES interaction_clarification_thread(thread_id) ON DELETE CASCADE,
    turn_index INTEGER NOT NULL,
    role VARCHAR(16) NOT NULL CHECK (role IN ('user','assistant','system')),
    message_text TEXT NOT NULL,
    source_type VARCHAR(64),
    answer_source VARCHAR(64),
    confidence NUMERIC(5,4),
    ambiguity_flag BOOLEAN NOT NULL DEFAULT FALSE,
    ambiguity_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    provenance_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    linked_doc_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    linked_object_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (thread_id, turn_index)
);
CREATE INDEX IF NOT EXISTS idx_clar_turn_thread ON interaction_clarification_turn(thread_id, turn_index);

CREATE TABLE IF NOT EXISTS event_subscriptions (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    listener_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(event_type, listener_name)
);

CREATE TABLE IF NOT EXISTS event_log (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    source_entity TEXT NOT NULL,
    entity_id BIGINT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags TEXT[],
    policy_mode TEXT,
    delivered BOOLEAN NOT NULL DEFAULT TRUE,
    listener_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_subscriptions_event ON event_subscriptions(event_type);
CREATE INDEX IF NOT EXISTS idx_event_subscriptions_enabled ON event_subscriptions(enabled);
CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type);
CREATE INDEX IF NOT EXISTS idx_event_log_entity ON event_log(source_entity, entity_id);
CREATE INDEX IF NOT EXISTS idx_event_log_created ON event_log(created_at);

-- Tenant boundary is enforced by PostgreSQL in addition to application filters.
ALTER TABLE document ENABLE ROW LEVEL SECURITY;
ALTER TABLE document FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON document;
CREATE POLICY ikos_tenant_isolation ON document
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid
    AND (access_level <> 'finance' OR 'finance.read' = ANY(string_to_array(current_setting('ikos.permissions', true), ',')))
    AND (access_level <> 'restricted' OR 'documents.restricted.read' = ANY(string_to_array(current_setting('ikos.permissions', true), ','))))
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid
    AND (access_level <> 'finance' OR 'finance.write' = ANY(string_to_array(current_setting('ikos.permissions', true), ',')))
    AND (access_level <> 'restricted' OR 'documents.restricted.read' = ANY(string_to_array(current_setting('ikos.permissions', true), ','))));

ALTER TABLE document_content ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_content FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON document_content;
CREATE POLICY ikos_tenant_isolation ON document_content
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid
    AND EXISTS (SELECT 1 FROM document d WHERE d.doc_id = document_content.doc_id AND d.tenant_id = document_content.tenant_id))
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid
    AND EXISTS (SELECT 1 FROM document d WHERE d.doc_id = document_content.doc_id AND d.tenant_id = document_content.tenant_id));

ALTER TABLE workflow_pipeline_run ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_pipeline_run FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON workflow_pipeline_run;
CREATE POLICY ikos_tenant_isolation ON workflow_pipeline_run
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);

ALTER TABLE workflow_pipeline_run_step ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_pipeline_run_step FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON workflow_pipeline_run_step;
CREATE POLICY ikos_tenant_isolation ON workflow_pipeline_run_step
USING (EXISTS (SELECT 1 FROM workflow_pipeline_run r WHERE r.run_id = workflow_pipeline_run_step.run_id AND r.tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid))
WITH CHECK (EXISTS (SELECT 1 FROM workflow_pipeline_run r WHERE r.run_id = workflow_pipeline_run_step.run_id AND r.tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid));

ALTER TABLE workflow_pipeline_run_document_link ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_pipeline_run_document_link FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON workflow_pipeline_run_document_link;
CREATE POLICY ikos_tenant_isolation ON workflow_pipeline_run_document_link
USING (EXISTS (SELECT 1 FROM workflow_pipeline_run r WHERE r.run_id = workflow_pipeline_run_document_link.run_id AND r.tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid))
WITH CHECK (EXISTS (SELECT 1 FROM workflow_pipeline_run r WHERE r.run_id = workflow_pipeline_run_document_link.run_id AND r.tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid));

ALTER TABLE object_instance ENABLE ROW LEVEL SECURITY;
ALTER TABLE object_instance FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON object_instance;
CREATE POLICY ikos_tenant_isolation ON object_instance
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);

ALTER TABLE object_relationship ENABLE ROW LEVEL SECURITY;
ALTER TABLE object_relationship FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON object_relationship;
CREATE POLICY ikos_tenant_isolation ON object_relationship
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);

ALTER TABLE part ENABLE ROW LEVEL SECURITY;
ALTER TABLE part FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON part;
CREATE POLICY ikos_tenant_isolation ON part
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);

ALTER TABLE document_table_cell ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_table_cell FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON document_table_cell;
CREATE POLICY ikos_tenant_isolation ON document_table_cell
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);

ALTER TABLE attribute ENABLE ROW LEVEL SECURITY;
ALTER TABLE attribute FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON attribute;
CREATE POLICY ikos_tenant_isolation ON attribute
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'semantic_patterns','pattern_synonyms','semantic_terms','query_term_alias',
        'query_alias_candidate','source_schema_taxonomy','category_registry','categories',
        'category_sources','category_entities','category_attributes'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format('DROP POLICY IF EXISTS ikos_tenant_isolation ON %I', t);
        EXECUTE format(
            'CREATE POLICY ikos_tenant_isolation ON %I USING (tenant_id = NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid)',
            t
        );
    END LOOP;
END $$;

-- Apply tenant ownership after all configuration tables have been created.
DO $$
DECLARE t TEXT;
DECLARE policy_expr TEXT := 'tenant_id = COALESCE(NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid, ''00000000-0000-0000-0000-000000000001''::uuid)';
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'business_rules','solf_clauses','solf_workflow_registry','solf_workflow_versions',
        'solf_workflow_steps','business_rule_workflow_links','workflow_resource_alias',
        'workflow_extension_pack','workflow_extension_version','workflow_extension_rule',
        'workflow_extension_attachment','workflow_generated_python_script',
        'workflow_generated_python_script_audit','action_resolution_plan',
        'workflow_action_log','interaction_clarification_thread','interaction_clarification_turn'
    ] LOOP
        IF to_regclass(format('public.%I', t)) IS NULL THEN
            CONTINUE;
        END IF;
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT ''00000000-0000-0000-0000-000000000001''', t);
        EXECUTE format('ALTER TABLE %I ALTER COLUMN tenant_id SET DEFAULT COALESCE(NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid, ''00000000-0000-0000-0000-000000000001''::uuid)', t);
        IF t = ANY(ARRAY['business_rules','solf_clauses','solf_workflow_registry','solf_workflow_versions','solf_workflow_steps','business_rule_workflow_links','workflow_resource_alias','workflow_extension_pack','workflow_extension_version','workflow_extension_rule','workflow_extension_attachment','workflow_generated_python_script']) THEN
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS configuration_scope VARCHAR(16) NOT NULL DEFAULT ''tenant''', t);
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', t, 'ck_' || t || '_configuration_scope');
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I CHECK (configuration_scope IN (''tenant'', ''global''))', t, 'ck_' || t || '_configuration_scope');
        END IF;
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(tenant_id)', 'idx_' || t || '_tenant_id', t);
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format('DROP POLICY IF EXISTS ikos_tenant_isolation ON %I', t);
        IF t = ANY(ARRAY['business_rules','solf_clauses','solf_workflow_registry','solf_workflow_versions','solf_workflow_steps','business_rule_workflow_links','workflow_resource_alias','workflow_extension_pack','workflow_extension_version','workflow_extension_rule','workflow_extension_attachment','workflow_generated_python_script']) THEN
            EXECUTE format('CREATE POLICY ikos_tenant_isolation ON %I USING ((%s AND configuration_scope = ''tenant'') OR configuration_scope = ''global'') WITH CHECK (%s AND ((configuration_scope = ''tenant'' AND ''tenant.configuration.manage'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '',''))) OR (configuration_scope = ''global'' AND ''platform.configuration.publish'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '','')))))', t, policy_expr, policy_expr);
        ELSE
            EXECUTE format('CREATE POLICY ikos_tenant_isolation ON %I USING (%s) WITH CHECK (%s)', t, policy_expr, policy_expr);
        END IF;
    END LOOP;

    IF to_regclass('public.solf_workflow_registry') IS NOT NULL THEN
        ALTER TABLE solf_workflow_registry DROP CONSTRAINT IF EXISTS solf_workflow_registry_workflow_key_key;
        DROP INDEX IF EXISTS uq_solf_workflow_registry_tenant_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_workflow_registry_scope_key ON solf_workflow_registry(tenant_id, configuration_scope, workflow_key);
    END IF;
    IF to_regclass('public.workflow_extension_pack') IS NOT NULL THEN
        ALTER TABLE workflow_extension_pack DROP CONSTRAINT IF EXISTS workflow_extension_pack_extension_key_key;
        DROP INDEX IF EXISTS uq_workflow_extension_pack_tenant_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_extension_pack_scope_key ON workflow_extension_pack(tenant_id, configuration_scope, extension_key);
    END IF;
    IF to_regclass('public.workflow_generated_python_script') IS NOT NULL THEN
        ALTER TABLE workflow_generated_python_script DROP CONSTRAINT IF EXISTS workflow_generated_python_script_script_key_key;
        DROP INDEX IF EXISTS uq_workflow_generated_script_tenant_key;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_generated_script_scope_key ON workflow_generated_python_script(tenant_id, configuration_scope, script_key);
    END IF;
    IF to_regclass('public.solf_clauses') IS NOT NULL THEN
        ALTER TABLE solf_clauses DROP CONSTRAINT IF EXISTS solf_clauses_clause_name_entity_class_key;
        DROP INDEX IF EXISTS uq_solf_clauses_tenant_name;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_clauses_scope_name ON solf_clauses(tenant_id, configuration_scope, clause_name, (COALESCE(entity_class, '')));
    END IF;
END $$;

-- Composite ownership references prevent cross-tenant workflow/rule/script links.
DO $$ BEGIN
    IF to_regclass('public.business_rules') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_business_rules_tenant_id ON business_rules(tenant_id, rule_id);
    END IF;
    IF to_regclass('public.solf_clauses') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_clauses_tenant_id ON solf_clauses(tenant_id, clause_id);
    END IF;
    IF to_regclass('public.solf_workflow_registry') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_workflow_registry_tenant_id ON solf_workflow_registry(tenant_id, workflow_id);
    END IF;
    IF to_regclass('public.solf_workflow_versions') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_workflow_versions_tenant_id ON solf_workflow_versions(tenant_id, workflow_version_id);
    END IF;
    IF to_regclass('public.workflow_extension_pack') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_extension_pack_tenant_id ON workflow_extension_pack(tenant_id, extension_pack_id);
    END IF;
    IF to_regclass('public.workflow_extension_version') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_extension_version_tenant_id ON workflow_extension_version(tenant_id, extension_version_id);
    END IF;
    IF to_regclass('public.workflow_generated_python_script') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_generated_python_script_tenant_id ON workflow_generated_python_script(tenant_id, script_id);
    END IF;
    IF to_regclass('public.workflow_pipeline_run') IS NOT NULL THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_pipeline_run_tenant_id ON workflow_pipeline_run(tenant_id, run_id);
    END IF;

    IF to_regclass('public.solf_workflow_versions') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_versions_tenant_workflow') THEN
        ALTER TABLE solf_workflow_versions ADD CONSTRAINT fk_solf_workflow_versions_tenant_workflow FOREIGN KEY (tenant_id, workflow_id) REFERENCES solf_workflow_registry(tenant_id, workflow_id) ON DELETE CASCADE;
    END IF;
    IF to_regclass('public.solf_workflow_versions') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_versions_tenant_rule') THEN
        ALTER TABLE solf_workflow_versions ADD CONSTRAINT fk_solf_workflow_versions_tenant_rule FOREIGN KEY (tenant_id, rule_id) REFERENCES business_rules(tenant_id, rule_id);
    END IF;
    IF to_regclass('public.solf_workflow_steps') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_steps_tenant_version') THEN
        ALTER TABLE solf_workflow_steps ADD CONSTRAINT fk_solf_workflow_steps_tenant_version FOREIGN KEY (tenant_id, workflow_version_id) REFERENCES solf_workflow_versions(tenant_id, workflow_version_id) ON DELETE CASCADE;
    END IF;
    IF to_regclass('public.solf_workflow_steps') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_steps_tenant_clause') THEN
        ALTER TABLE solf_workflow_steps ADD CONSTRAINT fk_solf_workflow_steps_tenant_clause FOREIGN KEY (tenant_id, clause_id) REFERENCES solf_clauses(tenant_id, clause_id);
    END IF;
    IF to_regclass('public.workflow_extension_version') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_extension_version_tenant_pack') THEN
        ALTER TABLE workflow_extension_version ADD CONSTRAINT fk_workflow_extension_version_tenant_pack FOREIGN KEY (tenant_id, extension_pack_id) REFERENCES workflow_extension_pack(tenant_id, extension_pack_id) ON DELETE CASCADE;
    END IF;
    IF to_regclass('public.workflow_extension_rule') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_extension_rule_tenant_version') THEN
        ALTER TABLE workflow_extension_rule ADD CONSTRAINT fk_workflow_extension_rule_tenant_version FOREIGN KEY (tenant_id, extension_version_id) REFERENCES workflow_extension_version(tenant_id, extension_version_id) ON DELETE CASCADE;
    END IF;
    IF to_regclass('public.workflow_pipeline_run_step') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_pipeline_step_tenant_run') THEN
        ALTER TABLE workflow_pipeline_run_step ADD CONSTRAINT fk_workflow_pipeline_step_tenant_run FOREIGN KEY (tenant_id, run_id) REFERENCES workflow_pipeline_run(tenant_id, run_id) ON DELETE CASCADE;
    END IF;
    IF to_regclass('public.workflow_pipeline_run_document_link') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_pipeline_document_link_tenant_run') THEN
        ALTER TABLE workflow_pipeline_run_document_link ADD CONSTRAINT fk_workflow_pipeline_document_link_tenant_run FOREIGN KEY (tenant_id, run_id) REFERENCES workflow_pipeline_run(tenant_id, run_id) ON DELETE CASCADE;
    END IF;
    IF to_regclass('public.workflow_mutation_journal') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_mutation_journal_tenant_run') THEN
        ALTER TABLE workflow_mutation_journal ADD CONSTRAINT fk_workflow_mutation_journal_tenant_run FOREIGN KEY (tenant_id, run_id) REFERENCES workflow_pipeline_run(tenant_id, run_id) ON DELETE CASCADE;
    END IF;
END $$;
