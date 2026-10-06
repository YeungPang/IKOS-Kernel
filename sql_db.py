import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json

from security_context import get_security_context

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def get_connection() -> psycopg2.extensions.connection:
    connection = psycopg2.connect(
        database=os.getenv("IDMS_DB_NAME", "idms_demo"),
        host=os.getenv("IDMS_DB_HOST", "localhost"),
        user=os.getenv("IDMS_DB_USER", "postgres"),
        password=os.getenv("IDMS_DB_PASSWORD", "Postgresql"),
        port=os.getenv("IDMS_DB_PORT", "5432"),
    )
    context = get_security_context(required=False)
    tenant_id = context.tenant_id if context is not None else "00000000-0000-0000-0000-000000000001"
    user_id = context.user_id if context is not None else ""
    permissions = ",".join(sorted(context.permissions)) if context is not None else ""
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('ikos.tenant_id', %s, false)", (tenant_id,))
        cursor.execute("SELECT set_config('ikos.user_id', %s, false)", (user_id,))
        cursor.execute("SELECT set_config('ikos.permissions', %s, false)", (permissions,))
    return connection


def current_tenant_id() -> str:
    context = get_security_context()
    assert context is not None
    return context.tenant_id


def apply_security_context(connection: Any) -> None:
    """Set RLS session variables on an existing connection."""
    context = get_security_context(required=False)
    if context is None:
        return
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('ikos.tenant_id', %s, false)", (context.tenant_id,))
        cursor.execute("SELECT set_config('ikos.user_id', %s, false)", (context.user_id,))


create_node_table = """
-- Deprecated: node table removed in favor of object_instance.
"""

create_edge_table = """
-- Deprecated: edge table removed in favor of object_relationship.
"""

create_document_table = """
CREATE TABLE IF NOT EXISTS document (
    doc_id                BIGSERIAL PRIMARY KEY,
    doc_key               VARCHAR(512),
    doc_path              VARCHAR(1024),
    markdown_path         VARCHAR(1024),
    doc_cat               VARCHAR(128),
    doc_type              VARCHAR(128),
    doc_date              DATE,
    doc_theme             TEXT,
    keyword_text          TEXT,
    keyword_vec           TSVECTOR,
    identifiers_kv        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    entry_date            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_docs_cat ON document(doc_cat);
CREATE INDEX IF NOT EXISTS idx_docs_type ON document(doc_type);
CREATE INDEX IF NOT EXISTS idx_docs_path ON document(doc_path);
CREATE INDEX IF NOT EXISTS idx_docs_markdown_path ON document(markdown_path);
CREATE INDEX IF NOT EXISTS idx_docs_date ON document(doc_date);
CREATE INDEX IF NOT EXISTS idx_docs_identifiers_gin ON document USING GIN(identifiers_kv);
CREATE INDEX IF NOT EXISTS idx_docs_metadata_gin ON document USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_docs_keyword_vec ON document USING GIN(keyword_vec);
"""

create_part_table = """
CREATE TABLE IF NOT EXISTS part (
    part_id              BIGSERIAL PRIMARY KEY,
    doc_id               BIGINT NOT NULL,
    part_key             VARCHAR(128) NOT NULL,
    class_id             BIGINT,
    object_id            BIGINT,
    relationship_id      BIGINT,
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    entry_date           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (doc_id, part_key)
);

ALTER TABLE part ADD COLUMN IF NOT EXISTS doc_id BIGINT;
ALTER TABLE part ADD COLUMN IF NOT EXISTS part_key VARCHAR(128);
ALTER TABLE part ADD COLUMN IF NOT EXISTS class_id BIGINT;
ALTER TABLE part ADD COLUMN IF NOT EXISTS object_id BIGINT;
ALTER TABLE part ADD COLUMN IF NOT EXISTS relationship_id BIGINT;
ALTER TABLE part ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE part ADD COLUMN IF NOT EXISTS entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_part_doc ON part(doc_id);
CREATE INDEX IF NOT EXISTS idx_part_class ON part(class_id);
CREATE INDEX IF NOT EXISTS idx_part_object ON part(object_id);
CREATE INDEX IF NOT EXISTS idx_part_relationship ON part(relationship_id);
CREATE INDEX IF NOT EXISTS idx_part_metadata_gin ON part USING GIN(metadata);
"""

create_attribute_table = """
CREATE TABLE IF NOT EXISTS attribute (
    attr_id              BIGSERIAL PRIMARY KEY,
    src_id               BIGINT NOT NULL,
    src_type             VARCHAR(16) NOT NULL CHECK (src_type IN ('part','document','class','object','relationship')),
    attr_type            VARCHAR(64) NOT NULL,
    doc_id               BIGINT,
    valid_from           DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until          DATE,
    valid_range tsrange GENERATED ALWAYS AS (tsrange(valid_from, valid_until)) STORED,
    attr_json            JSONB NOT NULL,
    search_txt           TEXT,
    search_vec           TSVECTOR,
    entry_date           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS doc_id BIGINT;
ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS valid_from DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS valid_until DATE;
ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS attr_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS search_txt TEXT;
ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS search_vec TSVECTOR;
ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_attr_src ON attribute(src_type, src_id);
CREATE INDEX IF NOT EXISTS idx_attr_doc ON attribute(doc_id);
CREATE INDEX IF NOT EXISTS idx_attr_type ON attribute(attr_type);
CREATE INDEX IF NOT EXISTS idx_attr_json_gin ON attribute USING GIN(attr_json);
CREATE INDEX IF NOT EXISTS idx_attr_search_vec ON attribute USING GIN(search_vec);
CREATE INDEX IF NOT EXISTS idx_attributes_valid_range ON attribute USING GIST (valid_range);

"""

create_fact_table = """
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

ALTER TABLE IF EXISTS fact ADD COLUMN IF NOT EXISTS doc_id BIGINT;
ALTER TABLE IF EXISTS fact ADD COLUMN IF NOT EXISTS valid_from DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE IF EXISTS fact ADD COLUMN IF NOT EXISTS valid_until DATE;
ALTER TABLE IF EXISTS fact ADD COLUMN IF NOT EXISTS confidence NUMERIC(5,4);
ALTER TABLE IF EXISTS fact ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE IF EXISTS fact ADD COLUMN IF NOT EXISTS entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_fact_subject ON fact(subject_id);
CREATE INDEX IF NOT EXISTS idx_fact_object ON fact(object_id);
CREATE INDEX IF NOT EXISTS idx_fact_valid_range ON fact USING GIST (valid_range);
CREATE INDEX IF NOT EXISTS idx_fact_metadata_gin ON fact USING GIN(metadata);
"""

create_clause_table = """
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
"""

create_predicate_table = """
CREATE TABLE IF NOT EXISTS predicate (
    predicate_id         BIGSERIAL PRIMARY KEY,
    predicate_name       VARCHAR(64) NOT NULL UNIQUE,
    description          TEXT
);
"""

create_account_table = """
CREATE TABLE IF NOT EXISTS accounts (
    id              BIGSERIAL PRIMARY KEY,
    legal_entity_ref TEXT NOT NULL DEFAULT 'global',
    account_code    TEXT NOT NULL,   -- e.g. 1000, 2000, 3200
    name            TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    currency        TEXT DEFAULT 'CHF',
    parent_id       BIGINT REFERENCES accounts(id),
    is_active       BOOLEAN DEFAULT TRUE,
    UNIQUE (legal_entity_ref, account_code)
);
"""

create_journal_table = """
CREATE TABLE IF NOT EXISTS journals (
    id              BIGSERIAL PRIMARY KEY,
    legal_entity_ref TEXT NOT NULL DEFAULT 'global',
    journal_code    TEXT NOT NULL,   -- e.g. "PURCHASES", "SALES", "CASH"
    name            TEXT NOT NULL,
    description     TEXT,
    UNIQUE (legal_entity_ref, journal_code)
);
"""

create_ledger_entries_table = """
CREATE TABLE IF NOT EXISTS ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    journal_id      BIGINT REFERENCES journals(id),
    document_id     BIGINT,                 -- link to invoice/bill/receipt
    legal_entity_ref TEXT,
    entity_id       BIGINT,                 -- supplier/customer
    account_id      BIGINT REFERENCES accounts(id),
    posting_date    DATE NOT NULL,
    description     TEXT,
    debit           NUMERIC(18,2) DEFAULT 0,
    credit          NUMERIC(18,2) DEFAULT 0,
    currency        TEXT DEFAULT 'CHF',
    created_at      TIMESTAMPTZ DEFAULT now()
);
"""

create_invoices_table = """
CREATE TABLE IF NOT EXISTS invoices (
    id                  BIGSERIAL PRIMARY KEY,
    document_uid        TEXT UNIQUE,          -- from IDMS
    invoice_number      TEXT,
    invoice_date        DATE,
    due_date            DATE,
    customer_entity_id  BIGINT,               -- link to IDMS entity
    currency            TEXT,
    total_net           NUMERIC(18,2),
    total_vat           NUMERIC(18,2),
    total_gross         NUMERIC(18,2),
    vat_rate            NUMERIC(5,2),
    source_document     TEXT,                 -- file path or ID
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_invoice_line_items_table = """
CREATE TABLE IF NOT EXISTS invoice_line_items (
    id              BIGSERIAL PRIMARY KEY,
    invoice_id      BIGINT REFERENCES invoices(id) ON DELETE CASCADE,
    description     TEXT,
    quantity        NUMERIC(18,4),
    unit_price      NUMERIC(18,4),
    net_amount      NUMERIC(18,2),
    vat_rate        NUMERIC(5,2),
    vat_amount      NUMERIC(18,2),
    account_id      BIGINT REFERENCES accounts(id)   -- revenue account
);
"""

create_bills_table = """
CREATE TABLE IF NOT EXISTS bills (
    id                  BIGSERIAL PRIMARY KEY,
    document_uid        TEXT UNIQUE,
    bill_number         TEXT,
    bill_date           DATE,
    due_date            DATE,
    supplier_entity_id  BIGINT,
    currency            TEXT,
    total_net           NUMERIC(18,2),
    total_vat           NUMERIC(18,2),
    total_gross         NUMERIC(18,2),
    vat_rate            NUMERIC(5,2),
    source_document     TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_bill_line_items_table = """
CREATE TABLE IF NOT EXISTS bill_line_items (
    id              BIGSERIAL PRIMARY KEY,
    bill_id         BIGINT REFERENCES bills(id) ON DELETE CASCADE,
    description     TEXT,
    quantity        NUMERIC(18,4),
    unit_price      NUMERIC(18,4),
    net_amount      NUMERIC(18,2),
    vat_rate        NUMERIC(5,2),
    vat_amount      NUMERIC(18,2),
    account_id      BIGINT REFERENCES accounts(id)   -- expense account
);
"""

create_receipts_table = """
CREATE TABLE IF NOT EXISTS receipts (
    id                  BIGSERIAL PRIMARY KEY,
    document_uid        TEXT UNIQUE,
    receipt_date        DATE NOT NULL,
    supplier_entity_id  BIGINT,                 -- from IDMS entity resolution
    currency            TEXT NOT NULL,
    total_amount        NUMERIC(18,2) NOT NULL,
    total_vat           NUMERIC(18,2),
    payment_method      TEXT,                   -- cash, card, mobile, etc.
    expense_category    TEXT,                   -- travel, meals, hotel, etc.
    source_document     TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_receipt_line_items_table = """
CREATE TABLE IF NOT EXISTS receipt_line_items (
    id              BIGSERIAL PRIMARY KEY,
    receipt_id      BIGINT REFERENCES receipts(id) ON DELETE CASCADE,
    description     TEXT,
    quantity        NUMERIC(18,4),
    unit_price      NUMERIC(18,4),
    net_amount      NUMERIC(18,2),
    vat_rate        NUMERIC(5,2),
    vat_amount      NUMERIC(18,2),
    account_id      BIGINT REFERENCES accounts(id)   -- expense account
);
"""

create_asset_register_table = """
CREATE TABLE IF NOT EXISTS asset_register (
    id                  BIGSERIAL PRIMARY KEY,
    asset_code          TEXT UNIQUE,
    description         TEXT,
    acquisition_date    DATE,
    acquisition_cost    NUMERIC(18,2),
    useful_life_months  INT,
    depreciation_method TEXT,   -- straight_line, declining_balance
    residual_value      NUMERIC(18,2),
    account_id          BIGINT REFERENCES accounts(id),  -- asset account
    accumulated_dep_id  BIGINT REFERENCES accounts(id),  -- contra-asset
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_expense_classification_rules_table = """
CREATE TABLE IF NOT EXISTS expense_classification_rules (
    id              BIGSERIAL PRIMARY KEY,
    keyword         TEXT,
    account_id      BIGINT REFERENCES accounts(id),
    priority        INT DEFAULT 100
);
"""

create_payments_table = """
CREATE TABLE IF NOT EXISTS payments (
    id                  BIGSERIAL PRIMARY KEY,
    bill_id             BIGINT REFERENCES bills(id),
    payment_date        DATE NOT NULL,
    amount              NUMERIC(18,2) NOT NULL,
    currency            TEXT,
    payment_method      TEXT,        -- bank transfer, credit card, cash
    bank_account_id     BIGINT,
    ledger_posted       BOOLEAN DEFAULT FALSE
);
"""

create_expense_claims_table = """
CREATE TABLE IF NOT EXISTS expense_claims (
    id                  BIGSERIAL PRIMARY KEY,
    employee_entity_id  BIGINT,
    claim_date          DATE,
    total_amount        NUMERIC(18,2),
    status              TEXT,        -- submitted, approved, paid
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_expense_claim_items_table = """
CREATE TABLE IF NOT EXISTS expense_claim_items (
    id                  BIGSERIAL PRIMARY KEY,
    claim_id            BIGINT REFERENCES expense_claims(id),
    receipt_id          BIGINT REFERENCES receipts(id),
    account_id          BIGINT,      -- travel, meals, hotel, etc.
    amount              NUMERIC(18,2)
);
"""

create_crm_leads_table = """
CREATE TABLE IF NOT EXISTS crm_leads (
    id                  BIGSERIAL PRIMARY KEY,
    entity_id           BIGINT,                 -- link to IDMS entity
    source              TEXT,                   -- website, referral, event
    status              TEXT,                   -- new, contacted, qualified, lost
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_crm_opportunities_table = """
CREATE TABLE IF NOT EXISTS crm_opportunities (
    id                  BIGSERIAL PRIMARY KEY,
    entity_id           BIGINT,                 -- customer
    name                TEXT NOT NULL,
    stage               TEXT,                   -- prospect, proposal, negotiation, won, lost
    expected_value      NUMERIC(18,2),
    currency            TEXT DEFAULT 'CHF',
    close_date          DATE,
    probability         NUMERIC(5,2),
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_crm_activities_table = """
CREATE TABLE IF NOT EXISTS crm_activities (
    id                  BIGSERIAL PRIMARY KEY,
    entity_id           BIGINT,                 -- customer or contact
    opportunity_id      BIGINT,
    activity_type       TEXT,                   -- call, email, meeting, note
    subject             TEXT,
    description         TEXT,
    activity_date       TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_crm_tasks_table = """
CREATE TABLE IF NOT EXISTS crm_tasks (
    id                  BIGSERIAL PRIMARY KEY,
    entity_id           BIGINT,                 -- customer
    opportunity_id      BIGINT,
    assigned_to         BIGINT,                 -- employee entity
    subject             TEXT,
    due_date            DATE,
    status              TEXT,                   -- open, in_progress, done
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_crm_contacts_table = """
CREATE TABLE IF NOT EXISTS crm_contacts (
    id                  BIGSERIAL PRIMARY KEY,
    entity_id           BIGINT,                 -- company
    contact_entity_id   BIGINT,                 -- person
    role                TEXT,                   -- CEO, buyer, manager
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""
create_purchase_orders_table = """
CREATE TABLE IF NOT EXISTS purchase_orders (
    id                  BIGSERIAL PRIMARY KEY,
    po_number           TEXT UNIQUE,
    supplier_entity_id  BIGINT,
    order_date          DATE,
    status              TEXT,   -- draft, approved, sent, received, closed
    currency            TEXT,
    total_amount        NUMERIC(18,2),
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_purchase_order_items_table = """
CREATE TABLE IF NOT EXISTS purchase_order_items (
    id              BIGSERIAL PRIMARY KEY,
    po_id           BIGINT REFERENCES purchase_orders(id),
    product_id      BIGINT,
    description     TEXT,
    quantity        NUMERIC(18,4),
    unit_price      NUMERIC(18,4),
    total_amount    NUMERIC(18,2)
);
"""

create_inventory_items_table = """
CREATE TABLE IF NOT EXISTS inventory_items (
    id              BIGSERIAL PRIMARY KEY,
    sku             TEXT UNIQUE,
    name            TEXT,
    description     TEXT,
    unit_of_measure TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);
"""

create_stock_movements_table = """
CREATE TABLE IF NOT EXISTS stock_movements (
    id              BIGSERIAL PRIMARY KEY,
    item_id         BIGINT REFERENCES inventory_items(id),
    movement_type   TEXT,   -- receipt, issue, adjustment
    quantity        NUMERIC(18,4),
    movement_date   TIMESTAMPTZ,
    reference_type  TEXT,   -- PO, SO, GRN, etc.
    reference_id    BIGINT,
    created_at      TIMESTAMPTZ DEFAULT now()
);
"""

create_projects_table = """
CREATE TABLE IF NOT EXISTS projects (
    id                  BIGSERIAL PRIMARY KEY,
    project_code        TEXT UNIQUE,
    name                TEXT NOT NULL,
    customer_entity_id  BIGINT,                -- link to IDMS entities
    description         TEXT,
    start_date          DATE,
    end_date            DATE,
    status              TEXT,                  -- planned, active, on_hold, completed, archived
    budget_amount       NUMERIC(18,2),
    currency            TEXT DEFAULT 'CHF',
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_project_members_table = """
CREATE TABLE IF NOT EXISTS project_members (
    id                  BIGSERIAL PRIMARY KEY,
    project_id          BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    member_entity_id    BIGINT NOT NULL,       -- employee / contractor entity
    role                TEXT,                  -- project_manager, developer, designer, etc.
    created_at          TIMESTAMPTZ DEFAULT now(),
    UNIQUE(project_id, member_entity_id)
);
"""

create_project_milestones_table = """
CREATE TABLE IF NOT EXISTS project_milestones (
    id              BIGSERIAL PRIMARY KEY,
    project_id      BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    description     TEXT,
    due_date        DATE,
    status          TEXT,                      -- open, completed, cancelled
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
"""

create_project_tasks_table = """
CREATE TABLE IF NOT EXISTS project_tasks (
    id                  BIGSERIAL PRIMARY KEY,
    project_id          BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    milestone_id        BIGINT REFERENCES project_milestones(id),
    assigned_to         BIGINT,                -- employee entity
    name                TEXT NOT NULL,
    description         TEXT,
    status              TEXT,                  -- open, in_progress, blocked, done, cancelled
    priority            TEXT,                  -- low, medium, high, critical
    start_date          DATE,
    due_date            DATE,
    estimated_hours     NUMERIC(10,2),
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_project_time_entries_table = """
CREATE TABLE IF NOT EXISTS project_time_entries (
    id                  BIGSERIAL PRIMARY KEY,
    project_id          BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id             BIGINT REFERENCES project_tasks(id),
    employee_entity_id  BIGINT NOT NULL,
    entry_date          DATE NOT NULL,
    hours               NUMERIC(10,2) NOT NULL,
    hourly_rate         NUMERIC(18,2),
    cost_amount         NUMERIC(18,2),         -- hours * hourly_rate (can be precomputed)
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_project_expenses_table = """
CREATE TABLE IF NOT EXISTS project_expenses (
    id                  BIGSERIAL PRIMARY KEY,
    project_id          BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    bill_id             BIGINT,                -- supplier bill
    receipt_id          BIGINT,                -- receipt
    account_id          BIGINT,                -- expense account
    expense_date        DATE NOT NULL,
    amount              NUMERIC(18,2) NOT NULL,
    currency            TEXT,
    description         TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_project_task_dependencies_table = """
CREATE TABLE IF NOT EXISTS project_task_dependencies (
    id                  BIGSERIAL PRIMARY KEY,
    task_id             BIGINT NOT NULL REFERENCES project_tasks(id) ON DELETE CASCADE,
    depends_on_task_id  BIGINT NOT NULL REFERENCES project_tasks(id) ON DELETE CASCADE,
    dependency_type     TEXT,                  -- finish_to_start, start_to_start, finish_to_finish, start_to_finish
    lag_days            NUMERIC(10,2),         -- optional offset
    created_at          TIMESTAMPTZ DEFAULT now(),
    UNIQUE(task_id, depends_on_task_id)
);
"""

create_project_documents_table = """
CREATE TABLE IF NOT EXISTS project_documents (
    id                  BIGSERIAL PRIMARY KEY,
    project_id          BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    document_uid        TEXT NOT NULL,         -- IDMS document identifier
    document_type       TEXT,                  -- spec, contract, SOW, etc.
    created_at          TIMESTAMPTZ DEFAULT now(),
    UNIQUE(project_id, document_uid)
);
"""

create_workflow_cases_table = """
CREATE TABLE IF NOT EXISTS workflow_cases (
    id                  BIGSERIAL PRIMARY KEY,
    case_no             TEXT UNIQUE,
    case_type           TEXT,
    subject_ref         BIGINT,
    initiated_by_ref    BIGINT,
    current_step        TEXT,
    sla_due_at          TIMESTAMPTZ,
    status              TEXT,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_approvals_table = """
CREATE TABLE IF NOT EXISTS approvals (
    id                  BIGSERIAL PRIMARY KEY,
    approval_no         TEXT UNIQUE,
    case_ref            BIGINT REFERENCES workflow_cases(id) ON DELETE CASCADE,
    approver_ref        BIGINT,
    decision            TEXT,
    decision_at         TIMESTAMPTZ,
    comment             TEXT,
    escalation_level    INT DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);
"""

create_compliance_actions_table = """
CREATE TABLE IF NOT EXISTS compliance_actions (
    id                      BIGSERIAL PRIMARY KEY,
    action_no               TEXT UNIQUE,
    action_type             TEXT,
    authority_ref           BIGINT,
    workflow_case_ref       BIGINT REFERENCES workflow_cases(id) ON DELETE SET NULL,
    subject_ref             BIGINT,
    responsible_party_ref   BIGINT,
    legal_basis             TEXT,
    due_date                DATE,
    due_date_key            BIGINT,
    completion_date         DATE,
    status                  TEXT,
    priority                TEXT,
    escalation_level        INT DEFAULT 0,
    source_document_ref     TEXT,
    evidence_ref            TEXT,
    created_at              TIMESTAMPTZ DEFAULT now(),
    updated_at              TIMESTAMPTZ DEFAULT now()
);
"""

create_workflow_action_log_table = """
CREATE TABLE IF NOT EXISTS workflow_action_log (
    id                  BIGSERIAL PRIMARY KEY,
    action_name         TEXT NOT NULL,
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    success             BOOLEAN NOT NULL,
    message             TEXT,
    result_data         JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

create_event_subscriptions_table = """
CREATE TABLE IF NOT EXISTS event_subscriptions (
    id                  BIGSERIAL PRIMARY KEY,
    event_type          TEXT NOT NULL,
    listener_name       TEXT NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(event_type, listener_name)
);
"""

create_event_log_table = """
CREATE TABLE IF NOT EXISTS event_log (
    id                  BIGSERIAL PRIMARY KEY,
    event_type          TEXT NOT NULL,
    source_entity       TEXT NOT NULL,
    entity_id           BIGINT,
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags                TEXT[],
    policy_mode         TEXT,
    delivered           BOOLEAN NOT NULL DEFAULT TRUE,
    listener_count      INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

create_notification_log_table = """
CREATE TABLE IF NOT EXISTS notification_log (
    id                  BIGSERIAL PRIMARY KEY,
    channel             TEXT NOT NULL,
    subject             TEXT NOT NULL,
    message             TEXT NOT NULL,
    recipient           TEXT,
    success             BOOLEAN NOT NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

create_workflow_state_transition_log_table = """
CREATE TABLE IF NOT EXISTS workflow_state_transition_log (
    id                  BIGSERIAL PRIMARY KEY,
    entity_type         TEXT NOT NULL,
    entity_id           BIGINT NOT NULL,
    from_state          TEXT,
    to_state            TEXT,
    transition_event    TEXT NOT NULL,
    actor_ref           TEXT,
    allowed             BOOLEAN NOT NULL,
    reason              TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

create_workflow_metric_snapshots_table = """
CREATE TABLE IF NOT EXISTS workflow_metric_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_label      TEXT,
    metrics             JSONB NOT NULL DEFAULT '{}'::jsonb,
    health_status       TEXT NOT NULL DEFAULT 'healthy',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

create_workflow_alert_policies_table = """
CREATE TABLE IF NOT EXISTS workflow_alert_policies (
    metric_key          TEXT PRIMARY KEY,
    metric_label        TEXT NOT NULL,
    warning_threshold   DOUBLE PRECISION NOT NULL,
    critical_threshold  DOUBLE PRECISION NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

create_workflow_alert_incidents_table = """
CREATE TABLE IF NOT EXISTS workflow_alert_incidents (
    id                  BIGSERIAL PRIMARY KEY,
    incident_key        TEXT NOT NULL,
    metric_key          TEXT NOT NULL,
    severity            TEXT NOT NULL,
    title               TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'open',
    current_value       DOUBLE PRECISION,
    threshold_value     DOUBLE PRECISION,
    occurrence_count    INT NOT NULL DEFAULT 1,
    details             JSONB NOT NULL DEFAULT '{}'::jsonb,
    opened_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at     TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(incident_key, status)
);
"""

create_indexes = """
CREATE INDEX IF NOT EXISTS idx_accounts_parent ON accounts(parent_id);
CREATE INDEX IF NOT EXISTS idx_accounts_entity_ref ON accounts(legal_entity_ref);
CREATE INDEX IF NOT EXISTS idx_accounts_type ON accounts(type);
CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active);

CREATE INDEX IF NOT EXISTS idx_journals_entity_ref ON journals(legal_entity_ref);
CREATE INDEX IF NOT EXISTS idx_journals_name ON journals(name);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_journal ON ledger_entries(journal_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_document ON ledger_entries(document_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_legal_entity ON ledger_entries(legal_entity_ref);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_entity ON ledger_entries(entity_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_account ON ledger_entries(account_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_posting_date ON ledger_entries(posting_date);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_posting ON ledger_entries(account_id, posting_date);

CREATE INDEX IF NOT EXISTS idx_invoices_number ON invoices(invoice_number);
CREATE INDEX IF NOT EXISTS idx_invoices_date ON invoices(invoice_date);
CREATE INDEX IF NOT EXISTS idx_invoices_due_date ON invoices(due_date);
CREATE INDEX IF NOT EXISTS idx_invoices_customer ON invoices(customer_entity_id);
CREATE INDEX IF NOT EXISTS idx_invoices_source_document ON invoices(source_document);

CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice ON invoice_line_items(invoice_id);
CREATE INDEX IF NOT EXISTS idx_invoice_items_account ON invoice_line_items(account_id);

CREATE INDEX IF NOT EXISTS idx_bills_number ON bills(bill_number);
CREATE INDEX IF NOT EXISTS idx_bills_date ON bills(bill_date);
CREATE INDEX IF NOT EXISTS idx_bills_due_date ON bills(due_date);
CREATE INDEX IF NOT EXISTS idx_bills_supplier ON bills(supplier_entity_id);

CREATE INDEX IF NOT EXISTS idx_bill_items_bill ON bill_line_items(bill_id);
CREATE INDEX IF NOT EXISTS idx_bill_items_account ON bill_line_items(account_id);

CREATE INDEX IF NOT EXISTS idx_receipts_date ON receipts(receipt_date);
CREATE INDEX IF NOT EXISTS idx_receipts_supplier ON receipts(supplier_entity_id);
CREATE INDEX IF NOT EXISTS idx_receipts_payment_method ON receipts(payment_method);
CREATE INDEX IF NOT EXISTS idx_receipts_expense_category ON receipts(expense_category);

CREATE INDEX IF NOT EXISTS idx_receipt_items_receipt ON receipt_line_items(receipt_id);
CREATE INDEX IF NOT EXISTS idx_receipt_items_account ON receipt_line_items(account_id);

CREATE INDEX IF NOT EXISTS idx_asset_register_acq_date ON asset_register(acquisition_date);
CREATE INDEX IF NOT EXISTS idx_asset_register_account ON asset_register(account_id);
CREATE INDEX IF NOT EXISTS idx_asset_register_acc_dep ON asset_register(accumulated_dep_id);

CREATE INDEX IF NOT EXISTS idx_expense_rules_keyword_priority ON expense_classification_rules(keyword, priority);
CREATE INDEX IF NOT EXISTS idx_expense_rules_account ON expense_classification_rules(account_id);

CREATE INDEX IF NOT EXISTS idx_payments_bill ON payments(bill_id);
CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_payments_bank_account ON payments(bank_account_id);
CREATE INDEX IF NOT EXISTS idx_payments_posted ON payments(ledger_posted);

CREATE INDEX IF NOT EXISTS idx_expense_claims_employee ON expense_claims(employee_entity_id);
CREATE INDEX IF NOT EXISTS idx_expense_claims_date ON expense_claims(claim_date);
CREATE INDEX IF NOT EXISTS idx_expense_claims_status ON expense_claims(status);

CREATE INDEX IF NOT EXISTS idx_expense_claim_items_claim ON expense_claim_items(claim_id);
CREATE INDEX IF NOT EXISTS idx_expense_claim_items_receipt ON expense_claim_items(receipt_id);
CREATE INDEX IF NOT EXISTS idx_expense_claim_items_account ON expense_claim_items(account_id);

CREATE INDEX IF NOT EXISTS idx_crm_leads_entity ON crm_leads(entity_id);
CREATE INDEX IF NOT EXISTS idx_crm_leads_status ON crm_leads(status);
CREATE INDEX IF NOT EXISTS idx_crm_leads_source ON crm_leads(source);
CREATE INDEX IF NOT EXISTS idx_crm_leads_updated_at ON crm_leads(updated_at);

CREATE INDEX IF NOT EXISTS idx_crm_opportunities_entity ON crm_opportunities(entity_id);
CREATE INDEX IF NOT EXISTS idx_crm_opportunities_stage ON crm_opportunities(stage);
CREATE INDEX IF NOT EXISTS idx_crm_opportunities_close_date ON crm_opportunities(close_date);

CREATE INDEX IF NOT EXISTS idx_crm_activities_entity ON crm_activities(entity_id);
CREATE INDEX IF NOT EXISTS idx_crm_activities_opportunity ON crm_activities(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_crm_activities_date ON crm_activities(activity_date);
CREATE INDEX IF NOT EXISTS idx_crm_activities_type ON crm_activities(activity_type);

CREATE INDEX IF NOT EXISTS idx_crm_tasks_entity ON crm_tasks(entity_id);
CREATE INDEX IF NOT EXISTS idx_crm_tasks_opportunity ON crm_tasks(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_crm_tasks_assigned_to ON crm_tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_crm_tasks_due_date ON crm_tasks(due_date);
CREATE INDEX IF NOT EXISTS idx_crm_tasks_status ON crm_tasks(status);

CREATE INDEX IF NOT EXISTS idx_crm_contacts_entity ON crm_contacts(entity_id);
CREATE INDEX IF NOT EXISTS idx_crm_contacts_contact_entity ON crm_contacts(contact_entity_id);

CREATE INDEX IF NOT EXISTS idx_purchase_orders_supplier ON purchase_orders(supplier_entity_id);
CREATE INDEX IF NOT EXISTS idx_purchase_orders_date ON purchase_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_purchase_orders_status ON purchase_orders(status);

CREATE INDEX IF NOT EXISTS idx_purchase_order_items_po ON purchase_order_items(po_id);
CREATE INDEX IF NOT EXISTS idx_purchase_order_items_product ON purchase_order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_inventory_items_name ON inventory_items(name);

CREATE INDEX IF NOT EXISTS idx_stock_movements_item ON stock_movements(item_id);
CREATE INDEX IF NOT EXISTS idx_stock_movements_date ON stock_movements(movement_date);
CREATE INDEX IF NOT EXISTS idx_stock_movements_reference ON stock_movements(reference_type, reference_id);

CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
CREATE INDEX IF NOT EXISTS idx_projects_customer ON projects(customer_entity_id);

CREATE INDEX IF NOT EXISTS idx_project_members_member ON project_members(member_entity_id);

CREATE INDEX IF NOT EXISTS idx_project_milestones_project ON project_milestones(project_id);
CREATE INDEX IF NOT EXISTS idx_project_milestones_due_date ON project_milestones(due_date);
CREATE INDEX IF NOT EXISTS idx_project_milestones_status ON project_milestones(status);

CREATE INDEX IF NOT EXISTS idx_tasks_project ON project_tasks(project_id);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned_to ON project_tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_tasks_milestone ON project_tasks(milestone_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON project_tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_due_date ON project_tasks(due_date);

CREATE INDEX IF NOT EXISTS idx_time_entries_project ON project_time_entries(project_id);
CREATE INDEX IF NOT EXISTS idx_time_entries_employee ON project_time_entries(employee_entity_id);
CREATE INDEX IF NOT EXISTS idx_time_entries_task ON project_time_entries(task_id);
CREATE INDEX IF NOT EXISTS idx_time_entries_entry_date ON project_time_entries(entry_date);

CREATE INDEX IF NOT EXISTS idx_expenses_project ON project_expenses(project_id);
CREATE INDEX IF NOT EXISTS idx_expenses_date ON project_expenses(expense_date);
CREATE INDEX IF NOT EXISTS idx_expenses_bill ON project_expenses(bill_id);
CREATE INDEX IF NOT EXISTS idx_expenses_receipt ON project_expenses(receipt_id);

CREATE INDEX IF NOT EXISTS idx_project_task_dependencies_task ON project_task_dependencies(task_id);
CREATE INDEX IF NOT EXISTS idx_project_task_dependencies_depends_on ON project_task_dependencies(depends_on_task_id);

CREATE INDEX IF NOT EXISTS idx_documents_project ON project_documents(project_id);
CREATE INDEX IF NOT EXISTS idx_documents_uid ON project_documents(document_uid);

CREATE INDEX IF NOT EXISTS idx_workflow_cases_type ON workflow_cases(case_type);
CREATE INDEX IF NOT EXISTS idx_workflow_cases_status ON workflow_cases(status);
CREATE INDEX IF NOT EXISTS idx_workflow_cases_sla_due ON workflow_cases(sla_due_at);

CREATE INDEX IF NOT EXISTS idx_approvals_case ON approvals(case_ref);
CREATE INDEX IF NOT EXISTS idx_approvals_approver ON approvals(approver_ref);
CREATE INDEX IF NOT EXISTS idx_approvals_decision ON approvals(decision);

CREATE INDEX IF NOT EXISTS idx_compliance_actions_case ON compliance_actions(workflow_case_ref);
CREATE INDEX IF NOT EXISTS idx_compliance_actions_due_date ON compliance_actions(due_date);
CREATE INDEX IF NOT EXISTS idx_compliance_actions_due_key ON compliance_actions(due_date_key);
CREATE INDEX IF NOT EXISTS idx_compliance_actions_status ON compliance_actions(status);

CREATE INDEX IF NOT EXISTS idx_workflow_action_log_name ON workflow_action_log(action_name);
CREATE INDEX IF NOT EXISTS idx_workflow_action_log_created ON workflow_action_log(created_at);

CREATE INDEX IF NOT EXISTS idx_event_subscriptions_event ON event_subscriptions(event_type);
CREATE INDEX IF NOT EXISTS idx_workflow_metric_snapshots_created ON workflow_metric_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_workflow_metric_snapshots_health ON workflow_metric_snapshots(health_status);
CREATE INDEX IF NOT EXISTS idx_workflow_alert_policies_enabled ON workflow_alert_policies(enabled);
CREATE INDEX IF NOT EXISTS idx_workflow_alert_incidents_status ON workflow_alert_incidents(status);
CREATE INDEX IF NOT EXISTS idx_workflow_alert_incidents_metric ON workflow_alert_incidents(metric_key);
CREATE INDEX IF NOT EXISTS idx_workflow_alert_incidents_last_seen ON workflow_alert_incidents(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_event_subscriptions_enabled ON event_subscriptions(enabled);

CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type);
CREATE INDEX IF NOT EXISTS idx_event_log_entity ON event_log(source_entity, entity_id);
CREATE INDEX IF NOT EXISTS idx_event_log_created ON event_log(created_at);

CREATE INDEX IF NOT EXISTS idx_notification_log_channel ON notification_log(channel);
CREATE INDEX IF NOT EXISTS idx_notification_log_success ON notification_log(success);
CREATE INDEX IF NOT EXISTS idx_notification_log_created ON notification_log(created_at);

CREATE INDEX IF NOT EXISTS idx_state_log_entity ON workflow_state_transition_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_state_log_allowed ON workflow_state_transition_log(allowed);
CREATE INDEX IF NOT EXISTS idx_state_log_created ON workflow_state_transition_log(created_at);
"""


def repair_legacy_schema(connection: psycopg2.extensions.connection) -> None:
    """Repair known legacy schema drift without destructive drops.

    This is safe to run repeatedly before normal bootstrap.
    """
    base_statements = [
        # Legacy document variants may not have these columns yet.
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS doc_id BIGSERIAL;",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS doc_name VARCHAR(512);",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS doc_desc TEXT;",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS valid_from DATE;",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS valid_until DATE;",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS doc_path VARCHAR(1024);",

        # Legacy part variants may be missing critical columns used by indexes/joins.
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS doc_id BIGINT;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS part_key VARCHAR(128);",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS class_id BIGINT;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS object_id BIGINT;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS relationship_id BIGINT;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW();",
        "ALTER TABLE IF EXISTS part DROP COLUMN IF EXISTS part_type CASCADE;",
        "ALTER TABLE IF EXISTS part DROP COLUMN IF EXISTS node_id CASCADE;",
        "ALTER TABLE IF EXISTS part DROP COLUMN IF EXISTS edge_id CASCADE;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS part_doc_id_part_type_part_key_key;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_part_doc_key ON part(doc_id, part_key);",

        # Keep bootstrap idempotent on migrated DBs.
        "CREATE INDEX IF NOT EXISTS idx_docs_path ON document(doc_path);",
        "DROP TABLE IF EXISTS edge CASCADE;",
        "DROP TABLE IF EXISTS node CASCADE;",

        # Multi-company ledger ownership additions (table-specific statements below).
        "ALTER TABLE IF EXISTS invoices ADD COLUMN IF NOT EXISTS legal_entity_ref TEXT;",
        "ALTER TABLE IF EXISTS bills ADD COLUMN IF NOT EXISTS legal_entity_ref TEXT;",
        "ALTER TABLE IF EXISTS receipts ADD COLUMN IF NOT EXISTS legal_entity_ref TEXT;",
    ]

    table_specific_statements: dict[str, list[str]] = {
        "accounts": [
            "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS legal_entity_ref TEXT;",
            "ALTER TABLE accounts ALTER COLUMN legal_entity_ref SET DEFAULT 'global';",
            "UPDATE accounts SET legal_entity_ref = COALESCE(NULLIF(TRIM(legal_entity_ref), ''), 'global') WHERE legal_entity_ref IS NULL OR TRIM(legal_entity_ref) = '';",
            "ALTER TABLE accounts ALTER COLUMN legal_entity_ref SET NOT NULL;",
            "ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_account_code_key;",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_entity_code_unique ON accounts(legal_entity_ref, account_code);",
            "CREATE INDEX IF NOT EXISTS idx_accounts_entity_ref ON accounts(legal_entity_ref);",
        ],
        "journals": [
            "ALTER TABLE journals ADD COLUMN IF NOT EXISTS legal_entity_ref TEXT;",
            "ALTER TABLE journals ALTER COLUMN legal_entity_ref SET DEFAULT 'global';",
            "UPDATE journals SET legal_entity_ref = COALESCE(NULLIF(TRIM(legal_entity_ref), ''), 'global') WHERE legal_entity_ref IS NULL OR TRIM(legal_entity_ref) = '';",
            "ALTER TABLE journals ALTER COLUMN legal_entity_ref SET NOT NULL;",
            "ALTER TABLE journals DROP CONSTRAINT IF EXISTS journals_journal_code_key;",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_journals_entity_code_unique ON journals(legal_entity_ref, journal_code);",
            "CREATE INDEX IF NOT EXISTS idx_journals_entity_ref ON journals(legal_entity_ref);",
        ],
        "ledger_entries": [
            "ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS legal_entity_ref TEXT;",
            "CREATE INDEX IF NOT EXISTS idx_ledger_entries_legal_entity ON ledger_entries(legal_entity_ref);",
        ],
    }

    table_exists_sql = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = %s
    )
    """

    try:
        with connection.cursor() as cursor:
            for sql in base_statements:
                cursor.execute(sql)

            for table_name, statements in table_specific_statements.items():
                cursor.execute(table_exists_sql, (table_name,))
                exists_row = cursor.fetchone()
                table_exists = bool(exists_row and exists_row[0])
                if not table_exists:
                    logging.info(
                        "Legacy schema repair skipped table-specific migration for missing table '%s'",
                        table_name,
                    )
                    continue
                for sql in statements:
                    cursor.execute(sql)
        connection.commit()
        logging.info("Legacy schema repair completed successfully")
    except Exception:
        connection.rollback()
        logging.exception("Legacy schema repair failed")
        raise

def create_tables(connection: psycopg2.extensions.connection, recreate: bool = False) -> None:
    ddl_statements = [
        create_document_table,
        create_part_table,
        create_attribute_table,
        create_predicate_table,
        create_fact_table,
        create_clause_table,
        create_account_table,
        create_journal_table,
        create_ledger_entries_table,
        create_invoices_table,
        create_invoice_line_items_table,
        create_bills_table,
        create_bill_line_items_table,
        create_receipts_table,
        create_receipt_line_items_table,
        create_asset_register_table,
        create_expense_classification_rules_table,
        create_payments_table,
        create_expense_claims_table,
        create_expense_claim_items_table,
        create_crm_leads_table,
        create_crm_opportunities_table,
        create_crm_activities_table,
        create_crm_tasks_table,
        create_crm_contacts_table,
        create_purchase_orders_table,
        create_purchase_order_items_table,
        create_inventory_items_table,
        create_stock_movements_table,
        create_projects_table,
        create_project_members_table,
        create_project_milestones_table,
        create_project_tasks_table,
        create_project_time_entries_table,
        create_project_expenses_table,
        create_project_task_dependencies_table,
        create_project_documents_table,
        create_workflow_cases_table,
        create_approvals_table,
        create_compliance_actions_table,
        create_workflow_action_log_table,
        create_event_subscriptions_table,
        create_event_log_table,
        create_notification_log_table,
        create_workflow_state_transition_log_table,
        create_workflow_metric_snapshots_table,
        create_workflow_alert_policies_table,
        create_workflow_alert_incidents_table,
        create_indexes,
    ]

    drop_statements = [
        "DROP TABLE IF EXISTS clause CASCADE;",
        "DROP TABLE IF EXISTS fact CASCADE;",
        "DROP TABLE IF EXISTS predicate CASCADE;",
        "DROP TABLE IF EXISTS attribute CASCADE;",
        "DROP TABLE IF EXISTS part CASCADE;",
        "DROP TABLE IF EXISTS document CASCADE;",
        "DROP TABLE IF EXISTS edge CASCADE;",
        "DROP TABLE IF EXISTS node CASCADE;",
        "DROP TABLE IF EXISTS accounts CASCADE;",
        "DROP TABLE IF EXISTS journals CASCADE;",
        "DROP TABLE IF EXISTS ledger_entries CASCADE;",
        "DROP TABLE IF EXISTS invoices CASCADE;",
        "DROP TABLE IF EXISTS invoice_line_items CASCADE;",
        "DROP TABLE IF EXISTS bills CASCADE;",
        "DROP TABLE IF EXISTS bill_line_items CASCADE;",
        "DROP TABLE IF EXISTS receipts CASCADE;",
        "DROP TABLE IF EXISTS receipt_line_items CASCADE;",
        "DROP TABLE IF EXISTS asset_register CASCADE;",
        "DROP TABLE IF EXISTS expense_classification_rules CASCADE;",
        "DROP TABLE IF EXISTS payments CASCADE;",
        "DROP TABLE IF EXISTS expense_claim_items CASCADE;",
        "DROP TABLE IF EXISTS expense_claims CASCADE;",
        "DROP TABLE IF EXISTS crm_leads CASCADE;",
        "DROP TABLE IF EXISTS crm_opportunities CASCADE;",
        "DROP TABLE IF EXISTS crm_activities CASCADE;",
        "DROP TABLE IF EXISTS crm_tasks CASCADE;",
        "DROP TABLE IF EXISTS crm_contacts CASCADE;",
        "DROP TABLE IF EXISTS purchase_orders CASCADE;",
        "DROP TABLE IF EXISTS purchase_order_items CASCADE;",
        "DROP TABLE IF EXISTS inventory_items CASCADE;",
        "DROP TABLE IF EXISTS stock_movements CASCADE;",
        "DROP TABLE IF EXISTS project_time_entries CASCADE;",
        "DROP TABLE IF EXISTS project_task_dependencies CASCADE;",
        "DROP TABLE IF EXISTS project_tasks CASCADE;",
        "DROP TABLE IF EXISTS project_milestones CASCADE;",
        "DROP TABLE IF EXISTS project_members CASCADE;",
        "DROP TABLE IF EXISTS projects CASCADE;",
        "DROP TABLE IF EXISTS project_expenses CASCADE;",
        "DROP TABLE IF EXISTS project_documents CASCADE;",
        "DROP TABLE IF EXISTS approvals CASCADE;",
        "DROP TABLE IF EXISTS compliance_actions CASCADE;",
        "DROP TABLE IF EXISTS workflow_cases CASCADE;",
        "DROP TABLE IF EXISTS workflow_action_log CASCADE;",
        "DROP TABLE IF EXISTS event_subscriptions CASCADE;",
        "DROP TABLE IF EXISTS event_log CASCADE;",
        "DROP TABLE IF EXISTS notification_log CASCADE;",
        "DROP TABLE IF EXISTS workflow_state_transition_log CASCADE;",
        "DROP TABLE IF EXISTS workflow_metric_snapshots CASCADE;",
        "DROP TABLE IF EXISTS workflow_alert_incidents CASCADE;",
        "DROP TABLE IF EXISTS workflow_alert_policies CASCADE;",
    ]

    migration_statements = [
        "ALTER TABLE document ADD COLUMN IF NOT EXISTS doc_name VARCHAR(512);",
        "ALTER TABLE document ADD COLUMN IF NOT EXISTS doc_desc TEXT;",
        "ALTER TABLE document ADD COLUMN IF NOT EXISTS valid_from DATE;",
        "ALTER TABLE document ADD COLUMN IF NOT EXISTS valid_until DATE;",
        "ALTER TABLE document ADD COLUMN IF NOT EXISTS doc_path VARCHAR(1024);",
        "CREATE INDEX IF NOT EXISTS idx_docs_path ON document(doc_path);",
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = ANY (current_schemas(false))
                  AND table_name = 'document'
                  AND column_name = 'doc_id'
            ) THEN
                ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS part_doc_id_fkey;
                ALTER TABLE IF EXISTS part
                    ADD CONSTRAINT part_doc_id_fkey
                    FOREIGN KEY (doc_id) REFERENCES document(doc_id) ON DELETE CASCADE;
            END IF;
        END
        $$;
        """,
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = ANY (current_schemas(false))
                  AND table_name = 'document'
                  AND column_name = 'doc_id'
            ) THEN
                ALTER TABLE IF EXISTS attribute DROP CONSTRAINT IF EXISTS attribute_doc_id_fkey;
                ALTER TABLE IF EXISTS attribute
                    ADD CONSTRAINT attribute_doc_id_fkey
                    FOREIGN KEY (doc_id) REFERENCES document(doc_id) ON DELETE CASCADE;
            END IF;
        END
        $$;
        """,
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = ANY (current_schemas(false))
                  AND table_name = 'document'
                  AND column_name = 'doc_id'
            ) THEN
                ALTER TABLE IF EXISTS fact DROP CONSTRAINT IF EXISTS fact_doc_id_fkey;
                ALTER TABLE IF EXISTS fact
                    ADD CONSTRAINT fact_doc_id_fkey
                    FOREIGN KEY (doc_id) REFERENCES document(doc_id) ON DELETE CASCADE;
            END IF;
        END
        $$;
        """,
    ]

    try:
        if not recreate:
            repair_legacy_schema(connection)
        with connection.cursor() as cursor:
            if recreate:
                for sql in drop_statements:
                    try:
                        cursor.execute(sql)
                    except Exception:
                        logging.exception("Failed drop statement during schema bootstrap: %s", sql)
                        raise
            for sql in ddl_statements:
                try:
                    cursor.execute(sql)
                except Exception:
                    logging.exception("Failed DDL statement during schema bootstrap: %s", sql)
                    raise
            for sql in migration_statements:
                try:
                    cursor.execute(sql)
                except Exception:
                    logging.exception("Failed migration statement during schema bootstrap: %s", sql)
                    raise
        connection.commit()
        logging.info("Schema tables created successfully")
    except Exception:
        connection.rollback()
        logging.exception("Failed while creating schema tables")
        raise


def insert_document(
    conn: psycopg2.extensions.connection,
    doc_name: str,
    doc_path: str | None,
    doc_type: str | None,
    doc_date: date | None,
    doc_desc: str | None,
    keywords: list[str],
    identifiers_kv: dict,
    metadata: dict | None = None,
    valid_from: date | None = None,
    valid_until: date | None = None,
) -> tuple[int, str]:
    keyword_text = " ".join(k for k in keywords if k)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO document (
                doc_name, doc_path, doc_type, doc_date, doc_desc,
                keyword_text, keyword_vec,
                identifiers_kv, metadata,
                valid_from, valid_until
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, to_tsvector('simple', %s),
                %s, %s,
                COALESCE(%s, CURRENT_DATE), %s
            )
            RETURNING doc_id, entry_date::text;
            """,
            (
                doc_name,
                doc_path,
                doc_type,
                doc_date,
                doc_desc,
                keyword_text,
                keyword_text,
                Json(identifiers_kv or {}),
                Json(metadata or {}),
                valid_from,
                valid_until,
            ),
        )
        doc_id, entry_date_text = cur.fetchone()
    conn.commit()
    return doc_id, entry_date_text


def upsert_node(
    conn: psycopg2.extensions.connection,
    node_name: str,
    node_type: str,
    metadata: dict | None = None,
) -> int:
    raise RuntimeError("upsert_node is deprecated; use object_instance/object_db APIs")


def upsert_edge(
    conn: psycopg2.extensions.connection,
    edge_name: str,
    edge_cat: str,
    src_node_id: int,
    tar_node_id: int,
    confidence: float | None = None,
    metadata: dict | None = None,
) -> int:
    raise RuntimeError("upsert_edge is deprecated; use object_relationship/object_db APIs")


def insert_part(
    conn: psycopg2.extensions.connection,
    doc_id: int,
    part_key: str,
    class_id: int | None = None,
    object_id: int | None = None,
    relationship_id: int | None = None,
    metadata: dict | None = None,
    part_type: str | None = None,
    node_id: int | None = None,
    edge_id: int | None = None,
) -> int:
    # Backward compatibility for legacy callers that still pass node/edge semantics.
    if object_id is None and isinstance(node_id, int):
        object_id = node_id
    if relationship_id is None and isinstance(edge_id, int):
        relationship_id = edge_id
    if isinstance(part_type, str):
        lowered = part_type.strip().lower()
        if lowered == "class" and class_id is None and isinstance(node_id, int):
            class_id = node_id
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO part (doc_id, part_key, class_id, object_id, relationship_id, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (doc_id, part_key)
            DO UPDATE SET
                class_id = COALESCE(EXCLUDED.class_id, part.class_id),
                object_id = COALESCE(EXCLUDED.object_id, part.object_id),
                relationship_id = COALESCE(EXCLUDED.relationship_id, part.relationship_id),
                metadata = COALESCE(part.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)
            RETURNING part_id;
            """,
            (doc_id, part_key, class_id, object_id, relationship_id, Json(metadata or {})),
        )
        part_id = cur.fetchone()[0]
    conn.commit()
    return part_id


def insert_attribute(
    conn: psycopg2.extensions.connection,
    src_id: int,
    src_type: str,
    attr_type: str,
    attr_json: dict,
    doc_id: int | None = None,
    valid_from: date | None = None,
    valid_until: date | None = None,
) -> int:
    search_txt = jsonb_to_search_text(attr_json)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO attribute (
                src_id, src_type, attr_type,
                doc_id, valid_from, valid_until,
                attr_json, search_txt, search_vec
            )
            VALUES (
                %s, %s, %s,
                %s, COALESCE(%s, CURRENT_DATE), %s,
                %s, %s, to_tsvector('simple', %s)
            )
            RETURNING attr_id;
            """,
            (
                src_id,
                src_type,
                attr_type,
                doc_id,
                valid_from,
                valid_until,
                Json(attr_json or {}),
                search_txt,
                search_txt,
            ),
        )
        attr_id = cur.fetchone()[0]
    conn.commit()
    return attr_id


def jsonb_to_search_text(value: dict) -> str:
    parts: list[str] = []
    for k, v in (value or {}).items():
        if isinstance(v, list):
            rendered = " ".join(str(x) for x in v)
        elif isinstance(v, dict):
            rendered = " ".join(f"{ik}:{iv}" for ik, iv in v.items())
        else:
            rendered = str(v)
        parts.append(f"{k} {rendered}")
    return " ".join(parts)


# ============ Markdown Management Helpers ============

def get_document_info(conn: psycopg2.extensions.connection, doc_id: int) -> dict | None:
    """
    Fetch document metadata including markdown path.
    
    Args:
        conn: Database connection
        doc_id: Document ID
    
    Returns:
        Dict with doc_id, doc_key, doc_type, metadata, or None if not found
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id, doc_key, doc_type, metadata
                FROM document
                WHERE doc_id = %s
                """,
                (doc_id,),
            )
            row = cur.fetchone()
            if row:
                return {
                    "doc_id": row[0],
                    "doc_key": row[1],
                    "doc_type": row[2],
                    "metadata": row[3] or {},
                }
    except Exception as e:
        logging.error("Failed to fetch document info for doc_id %s: %s", doc_id, e)
    
    return None


def update_document_metadata(
    conn: psycopg2.extensions.connection,
    doc_id: int,
    metadata_updates: dict,
) -> bool:
    """
    Update document metadata (e.g., markdown_path, ingestion_status, etc.).
    
    Args:
        conn: Database connection
        doc_id: Document ID
        metadata_updates: Dict of key-value pairs to update/add
    
    Returns:
        True if successful, False otherwise
    """
    try:
        with conn.cursor() as cur:
            for key, value in metadata_updates.items():
                cur.execute(
                    """
                    UPDATE document
                    SET metadata = jsonb_set(
                        COALESCE(metadata, '{}'::jsonb),
                        %s,
                        to_jsonb(%s::text)
                    )
                    WHERE doc_id = %s
                    """,
                    ([key], str(value), doc_id),
                )
        conn.commit()
        return True
    except Exception as e:
        logging.error("Failed to update document metadata for doc_id %s: %s", doc_id, e)
        conn.rollback()
        return False


def list_documents_by_status(
    conn: psycopg2.extensions.connection,
    status: str = "ingested",
    limit: int = 100,
) -> list[dict]:
    """
    List documents with a specific ingestion status.
    
    Args:
        conn: Database connection
        status: Status value to filter by (e.g., 'ingested', 're_processing')
        limit: Max results
    
    Returns:
        List of document dicts
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id, doc_key, doc_type, metadata
                FROM document
                WHERE metadata->>'ingestion_status' = %s
                ORDER BY doc_id DESC
                LIMIT %s
                """,
                (status, limit),
            )
            return [
                {
                    "doc_id": row[0],
                    "doc_key": row[1],
                    "doc_type": row[2],
                    "metadata": row[3] or {},
                }
                for row in cur.fetchall()
            ]
    except Exception as e:
        logging.error("Failed to list documents by status: %s", e)
    
    return []


if __name__ == "__main__":
    connection = get_connection()
    try:
        create_tables(connection, recreate=False)
    finally:
        connection.close()
