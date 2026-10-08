from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from ikos_config import (
    SOLF_PROPOSAL_PRIORITY_HIGH_THRESHOLD,
    SOLF_PROPOSAL_PRIORITY_MEDIUM_THRESHOLD,
    SOLF_PROPOSAL_SUGGEST_OCCURRENCE_THRESHOLD,
    SOLF_PROPOSAL_SUGGEST_PRIORITY_THRESHOLD,
)


LOGGER = logging.getLogger("idms.domain_db")
MONEY_QUANT = Decimal("0.01")


DEFAULT_CHART_OF_ACCOUNTS: list[dict[str, Any]] = [
    {"account_number": 1000, "account_name": "Cash and Cash Equivalents", "account_type": "asset"},
    {"account_number": 1020, "account_name": "Bank CHF", "account_type": "asset"},
    {"account_number": 1060, "account_name": "Input VAT Receivable", "account_type": "asset"},
    {"account_number": 1100, "account_name": "Accounts Receivable", "account_type": "asset"},
    {"account_number": 2000, "account_name": "Accounts Payable", "account_type": "liability"},
    {"account_number": 2200, "account_name": "VAT Payable", "account_type": "liability"},
    {"account_number": 2800, "account_name": "Equity", "account_type": "equity"},
    {"account_number": 2900, "account_name": "Purchase Commitment Reserve", "account_type": "liability"},
    {"account_number": 3000, "account_name": "Domestic Sales", "account_type": "revenue"},
    {"account_number": 3200, "account_name": "Export Sales", "account_type": "revenue"},
    {"account_number": 4000, "account_name": "Cost of Goods Sold", "account_type": "expense"},
    {"account_number": 4200, "account_name": "Purchases", "account_type": "expense"},
    {"account_number": 6500, "account_name": "Office and Admin Expense", "account_type": "expense"},
    {"account_number": 9100, "account_name": "Purchase Commitment Expense", "account_type": "expense"},
]


ENTERPRISE_CHART_OF_ACCOUNTS: list[dict[str, Any]] = [
    {"account_number": 1000, "account_name": "Cash on Hand", "account_type": "asset"},
    {"account_number": 1020, "account_name": "Main Bank CHF", "account_type": "asset"},
    {"account_number": 1030, "account_name": "Main Bank EUR", "account_type": "asset"},
    {"account_number": 1060, "account_name": "Input VAT Receivable", "account_type": "asset"},
    {"account_number": 1100, "account_name": "Trade Receivables", "account_type": "asset"},
    {"account_number": 1170, "account_name": "Allowance for Doubtful Accounts", "account_type": "asset"},
    {"account_number": 1300, "account_name": "Inventory - Merchandise", "account_type": "asset"},
    {"account_number": 1500, "account_name": "Machinery and Equipment", "account_type": "asset"},
    {"account_number": 1600, "account_name": "Accumulated Depreciation", "account_type": "asset"},
    {"account_number": 2000, "account_name": "Trade Payables", "account_type": "liability"},
    {"account_number": 2100, "account_name": "Accrued Expenses", "account_type": "liability"},
    {"account_number": 2200, "account_name": "VAT Payable", "account_type": "liability"},
    {"account_number": 2300, "account_name": "Payroll Liabilities", "account_type": "liability"},
    {"account_number": 2500, "account_name": "Long-term Debt", "account_type": "liability"},
    {"account_number": 2800, "account_name": "Share Capital and Reserves", "account_type": "equity"},
    {"account_number": 2900, "account_name": "Purchase Commitment Reserve", "account_type": "liability"},
    {"account_number": 3000, "account_name": "Domestic Product Revenue", "account_type": "revenue"},
    {"account_number": 3010, "account_name": "Service Revenue", "account_type": "revenue"},
    {"account_number": 3200, "account_name": "Export Revenue", "account_type": "revenue"},
    {"account_number": 4000, "account_name": "COGS - Materials", "account_type": "expense"},
    {"account_number": 4010, "account_name": "COGS - Subcontracting", "account_type": "expense"},
    {"account_number": 5000, "account_name": "Salaries and Wages", "account_type": "expense"},
    {"account_number": 6200, "account_name": "Rent Expense", "account_type": "expense"},
    {"account_number": 6300, "account_name": "Utilities Expense", "account_type": "expense"},
    {"account_number": 6500, "account_name": "General and Administrative Expense", "account_type": "expense"},
    {"account_number": 6800, "account_name": "Depreciation Expense", "account_type": "expense"},
    {"account_number": 9100, "account_name": "Purchase Commitment Expense", "account_type": "expense"},
]


CHART_OF_ACCOUNTS_PROFILES: dict[str, list[dict[str, Any]]] = {
    "swiss_sme": DEFAULT_CHART_OF_ACCOUNTS,
    "enterprise": ENTERPRISE_CHART_OF_ACCOUNTS,
}


DEFAULT_HR_DEPARTMENTS: list[dict[str, Any]] = [
    {"department_code": "HR", "department_name": "Human Resources"},
    {"department_code": "FIN", "department_name": "Finance"},
    {"department_code": "OPS", "department_name": "Operations"},
    {"department_code": "SALES", "department_name": "Sales"},
    {"department_code": "IT", "department_name": "Information Technology"},
]


ENTERPRISE_HR_DEPARTMENTS: list[dict[str, Any]] = [
    {"department_code": "HR", "department_name": "Human Resources"},
    {"department_code": "FIN", "department_name": "Finance and Controlling"},
    {"department_code": "TAX", "department_name": "Tax and Compliance"},
    {"department_code": "OPS", "department_name": "Operations"},
    {"department_code": "PROC", "department_name": "Procurement"},
    {"department_code": "LEGAL", "department_name": "Legal"},
    {"department_code": "SALES", "department_name": "Sales"},
    {"department_code": "CS", "department_name": "Customer Success"},
    {"department_code": "IT", "department_name": "Information Technology"},
    {"department_code": "RD", "department_name": "Research and Development"},
]


DEFAULT_HR_ROLES: list[dict[str, Any]] = [
    {"role_code": "EMP", "role_name": "Employee", "role_family": "staff", "seniority_level": "associate"},
    {"role_code": "MGR", "role_name": "Manager", "role_family": "management", "seniority_level": "manager"},
    {"role_code": "DIR", "role_name": "Director", "role_family": "management", "seniority_level": "director"},
    {"role_code": "HRBP", "role_name": "HR Business Partner", "role_family": "hr", "seniority_level": "senior"},
    {"role_code": "ACC", "role_name": "Accountant", "role_family": "finance", "seniority_level": "specialist"},
]


ENTERPRISE_HR_ROLES: list[dict[str, Any]] = [
    {"role_code": "EMP", "role_name": "Employee", "role_family": "staff", "seniority_level": "associate"},
    {"role_code": "SPE", "role_name": "Specialist", "role_family": "professional", "seniority_level": "specialist"},
    {"role_code": "MGR", "role_name": "Manager", "role_family": "management", "seniority_level": "manager"},
    {"role_code": "SMGR", "role_name": "Senior Manager", "role_family": "management", "seniority_level": "senior_manager"},
    {"role_code": "DIR", "role_name": "Director", "role_family": "management", "seniority_level": "director"},
    {"role_code": "VP", "role_name": "Vice President", "role_family": "executive", "seniority_level": "vp"},
    {"role_code": "HRBP", "role_name": "HR Business Partner", "role_family": "hr", "seniority_level": "senior"},
    {"role_code": "PAY", "role_name": "Payroll Specialist", "role_family": "hr", "seniority_level": "specialist"},
    {"role_code": "ACC", "role_name": "Accountant", "role_family": "finance", "seniority_level": "specialist"},
    {"role_code": "CTRL", "role_name": "Controller", "role_family": "finance", "seniority_level": "manager"},
]


HR_MASTER_PROFILES: dict[str, dict[str, list[dict[str, Any]]]] = {
    "swiss_sme": {
        "departments": DEFAULT_HR_DEPARTMENTS,
        "roles": DEFAULT_HR_ROLES,
    },
    "enterprise": {
        "departments": ENTERPRISE_HR_DEPARTMENTS,
        "roles": ENTERPRISE_HR_ROLES,
    },
}


SWISS_TAX_LEDGER_ROUTER = {
    "U81": {"rate": Decimal("0.081"), "box_declaration": 303, "type": "sales"},
    "UEX": {"rate": Decimal("0.000"), "box_declaration": 221, "type": "sales_exempt"},
    "ULA": {"rate": Decimal("0.000"), "box_declaration": 221, "type": "sales_exempt"},
    "IPM81": {"rate": Decimal("0.081"), "box_declaration": 400, "type": "input_tax_materials"},
    "IPB81": {"rate": Decimal("0.081"), "box_declaration": 405, "type": "input_tax_overhead"},
    "BZM81": {"rate": Decimal("0.081"), "box_declaration": 383, "type": "reverse_charge_service"},
    "NONE": {"rate": Decimal("0.000"), "box_declaration": None, "type": "not_applicable"},
}


create_chart_of_accounts_table = """
CREATE TABLE IF NOT EXISTS chart_of_accounts (
    legal_entity_ref VARCHAR(128) NOT NULL DEFAULT 'global',
    account_number INT NOT NULL,
    account_name VARCHAR(100) NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    UNIQUE (legal_entity_ref, account_number)
);

CREATE INDEX IF NOT EXISTS idx_coa_entity_ref ON chart_of_accounts(legal_entity_ref);
"""


create_transactions_table = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    journal_sequence_number BIGSERIAL NOT NULL,
    legal_entity_ref VARCHAR(128),
    object_id BIGINT REFERENCES object_instance(object_id),
    source_document_id BIGINT REFERENCES document(doc_id),
    source_type VARCHAR(50) NOT NULL,
    transaction_date DATE NOT NULL,
    description TEXT NOT NULL,
    receipt_archive_url VARCHAR(512),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_journal_seq ON transactions(journal_sequence_number);
CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_tx_legal_entity_ref ON transactions(legal_entity_ref);
CREATE INDEX IF NOT EXISTS idx_tx_object_id ON transactions(object_id);
CREATE INDEX IF NOT EXISTS idx_tx_source_document_id ON transactions(source_document_id);
CREATE INDEX IF NOT EXISTS idx_tx_source_type ON transactions(source_type);
CREATE INDEX IF NOT EXISTS idx_tx_metadata_gin ON transactions USING GIN(metadata);
"""


create_ledger_lines_table = """
CREATE TABLE IF NOT EXISTS ledger_lines (
    line_id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    legal_entity_ref VARCHAR(128),
    account_number INT NOT NULL,
    direction VARCHAR(6) NOT NULL CHECK (direction IN ('debit','credit')),
    source_currency VARCHAR(3) NOT NULL,
    amount_source_currency DECIMAL(12, 2) NOT NULL,
    exchange_rate_to_chf DECIMAL(10, 6) NOT NULL,
    amount_chf DECIMAL(12, 2) NOT NULL,
    swiss_vat_code VARCHAR(10) DEFAULT 'NONE',
    swiss_vat_box INT DEFAULT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_ledger_tx_id ON ledger_lines(transaction_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entity_ref ON ledger_lines(legal_entity_ref);
CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger_lines(account_number);
CREATE INDEX IF NOT EXISTS idx_ledger_acc_dir_amt ON ledger_lines(account_number, direction, amount_chf);
CREATE INDEX IF NOT EXISTS idx_ledger_swiss_vat_box ON ledger_lines(swiss_vat_box) WHERE swiss_vat_box IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ledger_metadata_gin ON ledger_lines USING GIN(metadata);
"""


create_accounting_booking_review_table = """
CREATE TABLE IF NOT EXISTS accounting_booking_review (
    review_id BIGSERIAL PRIMARY KEY,
    object_id BIGINT REFERENCES object_instance(object_id) ON DELETE CASCADE,
    object_name VARCHAR(256),
    class_name VARCHAR(128),
    source_type VARCHAR(50),
    source_document_id BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    legal_entity_ref VARCHAR(128),
    transaction_id VARCHAR(64),
    transaction_date DATE,
    description TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    preview_lines_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    validation_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','cancelled')),
    posted_transaction_id VARCHAR(64),
    review_note TEXT,
    reviewed_by VARCHAR(128),
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_status ON accounting_booking_review(status);
CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_object_id ON accounting_booking_review(object_id);
CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_created_at ON accounting_booking_review(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_source_type ON accounting_booking_review(source_type);
"""


create_domain_definition_audit_log_table = """
CREATE TABLE IF NOT EXISTS domain_definition_audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    definition_type VARCHAR(64) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    key_value VARCHAR(256) NOT NULL,
    changed_by VARCHAR(256),
    change_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    before_state JSONB,
    after_state JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_domain_audit_definition_type ON domain_definition_audit_log(definition_type);
CREATE INDEX IF NOT EXISTS idx_domain_audit_operation ON domain_definition_audit_log(operation);
CREATE INDEX IF NOT EXISTS idx_domain_audit_key_value ON domain_definition_audit_log(key_value);
CREATE INDEX IF NOT EXISTS idx_domain_audit_timestamp ON domain_definition_audit_log(change_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_domain_audit_metadata_gin ON domain_definition_audit_log USING GIN(metadata);
"""


create_source_database_registry_table = """
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

CREATE TABLE IF NOT EXISTS source_database_registry (
    source_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    source_key VARCHAR(256) NOT NULL,
    source_name VARCHAR(256) NOT NULL,
    db_host VARCHAR(256),
    db_port INT,
    db_name VARCHAR(256) NOT NULL,
    db_user VARCHAR(256),
    db_schema VARCHAR(256),
    connection_hint TEXT,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, source_key)
);

ALTER TABLE source_database_registry ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE source_database_registry
SET tenant_id = '00000000-0000-0000-0000-000000000001'
WHERE tenant_id IS NULL;
ALTER TABLE source_database_registry
    ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE source_database_registry ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE source_database_registry
    DROP CONSTRAINT IF EXISTS source_database_registry_source_key_key;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'source_database_registry'::regclass
          AND conname = 'source_database_registry_tenant_source_key_key'
    ) THEN
        ALTER TABLE source_database_registry
            ADD CONSTRAINT source_database_registry_tenant_source_key_key
            UNIQUE (tenant_id, source_key);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_source_database_registry_status ON source_database_registry(status);
CREATE INDEX IF NOT EXISTS idx_source_database_registry_db_name ON source_database_registry(db_name);
CREATE INDEX IF NOT EXISTS idx_source_database_registry_metadata_gin ON source_database_registry USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_source_database_registry_tenant_key ON source_database_registry(tenant_id, source_key);
ALTER TABLE source_database_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_database_registry FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_tenant_isolation ON source_database_registry;
CREATE POLICY ikos_tenant_isolation ON source_database_registry
USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
"""


create_source_schema_inventory_table = """
CREATE TABLE IF NOT EXISTS source_schema_inventory (
    inventory_id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES source_database_registry(source_id) ON DELETE CASCADE,
    schema_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    object_kind VARCHAR(32) NOT NULL DEFAULT 'table',
    fingerprint TEXT NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (source_id, schema_name, table_name)
);

CREATE INDEX IF NOT EXISTS idx_source_schema_inventory_source ON source_schema_inventory(source_id);
CREATE INDEX IF NOT EXISTS idx_source_schema_inventory_fingerprint ON source_schema_inventory(fingerprint);
CREATE INDEX IF NOT EXISTS idx_source_schema_inventory_metadata_gin ON source_schema_inventory USING GIN(metadata);
"""


create_source_entity_mapping_table = """
CREATE TABLE IF NOT EXISTS source_entity_mapping (
    mapping_id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES source_database_registry(source_id) ON DELETE CASCADE,
    schema_name VARCHAR(256) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    source_pk_value TEXT NOT NULL,
    target_class_name VARCHAR(256) NOT NULL,
    target_object_id BIGINT REFERENCES object_instance(object_id) ON DELETE SET NULL,
    target_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_row_hash TEXT NOT NULL,
    source_row_updated_at TIMESTAMPTZ,
    source_row_fingerprint TEXT NOT NULL,
    mapped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, schema_name, table_name, source_pk_value, target_class_name)
);

CREATE INDEX IF NOT EXISTS idx_source_entity_mapping_source ON source_entity_mapping(source_id);
CREATE INDEX IF NOT EXISTS idx_source_entity_mapping_target ON source_entity_mapping(target_object_id);
CREATE INDEX IF NOT EXISTS idx_source_entity_mapping_updated_at ON source_entity_mapping(updated_at);
CREATE INDEX IF NOT EXISTS idx_source_entity_mapping_metadata_gin ON source_entity_mapping USING GIN(target_metadata);
"""


create_source_sync_state_table = """
CREATE TABLE IF NOT EXISTS source_sync_state (
    sync_id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES source_database_registry(source_id) ON DELETE CASCADE,
    sync_scope VARCHAR(128) NOT NULL DEFAULT 'entity_mapping',
    last_sync_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error TEXT,
    sync_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, sync_scope)
);

CREATE INDEX IF NOT EXISTS idx_source_sync_state_source ON source_sync_state(source_id);
CREATE INDEX IF NOT EXISTS idx_source_sync_state_last_sync_at ON source_sync_state(last_sync_at DESC);
CREATE INDEX IF NOT EXISTS idx_source_sync_state_metadata_gin ON source_sync_state USING GIN(sync_metadata);
"""


create_solf_attribute_proposals_table = """
CREATE TABLE IF NOT EXISTS solf_attribute_proposals (
    proposal_id BIGSERIAL PRIMARY KEY,
    class_name VARCHAR(256) NOT NULL,
    attribute_name VARCHAR(256) NOT NULL,
    first_seen_doc_id BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    last_seen_doc_id BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    sample_value TEXT,
    occurrence_count BIGINT NOT NULL DEFAULT 1,
    status VARCHAR(32) NOT NULL DEFAULT 'proposed',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (class_name, attribute_name)
);

CREATE INDEX IF NOT EXISTS idx_solf_attr_proposals_class ON solf_attribute_proposals(class_name);
CREATE INDEX IF NOT EXISTS idx_solf_attr_proposals_status ON solf_attribute_proposals(status);
CREATE INDEX IF NOT EXISTS idx_solf_attr_proposals_metadata_gin ON solf_attribute_proposals USING GIN(metadata);
"""


create_solf_schema_promotion_batches_table = """
CREATE TABLE IF NOT EXISTS solf_schema_promotion_batches (
    batch_id BIGSERIAL PRIMARY KEY,
    batch_name VARCHAR(256) NOT NULL,
    class_name VARCHAR(256),
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    proposal_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    patch_preview TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_solf_schema_batches_status ON solf_schema_promotion_batches(status);
CREATE INDEX IF NOT EXISTS idx_solf_schema_batches_class_name ON solf_schema_promotion_batches(class_name);
CREATE INDEX IF NOT EXISTS idx_solf_schema_batches_metadata_gin ON solf_schema_promotion_batches USING GIN(metadata);
"""


def create_tables(connection: Any, recreate: bool = False) -> None:
    ddl_statements = [
        create_chart_of_accounts_table,
        create_transactions_table,
        create_ledger_lines_table,
        create_accounting_booking_review_table,
        create_solf_attribute_proposals_table,
        create_solf_schema_promotion_batches_table,
        create_source_database_registry_table,
        create_source_schema_inventory_table,
        create_source_entity_mapping_table,
        create_source_sync_state_table,
        create_domain_definition_audit_log_table,
    ]

    drop_statements = [
        "DROP TABLE IF EXISTS solf_attribute_proposals CASCADE;",
        "DROP TABLE IF EXISTS solf_schema_promotion_batches CASCADE;",
        "DROP TABLE IF EXISTS accounting_booking_review CASCADE;",
        "DROP TABLE IF EXISTS ledger_lines CASCADE;",
        "DROP TABLE IF EXISTS transactions CASCADE;",
        "DROP TABLE IF EXISTS chart_of_accounts CASCADE;",
    ]

    with connection.cursor() as cursor:
        if recreate:
            for sql in drop_statements:
                cursor.execute(sql)
        for sql in ddl_statements:
            cursor.execute(sql)
        if not recreate:
            migration_statements = [
                "ALTER TABLE IF EXISTS source_database_registry ADD COLUMN IF NOT EXISTS db_user VARCHAR(256);",
                "ALTER TABLE IF EXISTS chart_of_accounts ADD COLUMN IF NOT EXISTS legal_entity_ref VARCHAR(128);",
                "ALTER TABLE IF EXISTS chart_of_accounts ALTER COLUMN legal_entity_ref SET DEFAULT 'global';",
                "UPDATE chart_of_accounts SET legal_entity_ref = COALESCE(NULLIF(TRIM(legal_entity_ref), ''), 'global') WHERE legal_entity_ref IS NULL OR TRIM(legal_entity_ref) = '';",
                "ALTER TABLE IF EXISTS chart_of_accounts ALTER COLUMN legal_entity_ref SET NOT NULL;",
                "ALTER TABLE IF EXISTS chart_of_accounts DROP CONSTRAINT IF EXISTS chart_of_accounts_pkey;",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_coa_entity_account_unique ON chart_of_accounts(legal_entity_ref, account_number);",
                "CREATE INDEX IF NOT EXISTS idx_coa_entity_ref ON chart_of_accounts(legal_entity_ref);",
                "ALTER TABLE IF EXISTS transactions ADD COLUMN IF NOT EXISTS legal_entity_ref VARCHAR(128);",
                "CREATE INDEX IF NOT EXISTS idx_tx_legal_entity_ref ON transactions(legal_entity_ref);",
                "ALTER TABLE IF EXISTS ledger_lines ADD COLUMN IF NOT EXISTS legal_entity_ref VARCHAR(128);",
                "CREATE INDEX IF NOT EXISTS idx_ledger_entity_ref ON ledger_lines(legal_entity_ref);",
                "ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS posted_transaction_id VARCHAR(64);",
                "ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS review_note TEXT;",
                "ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS reviewed_by VARCHAR(128);",
                "ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;",
                "CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_status ON accounting_booking_review(status);",
                "CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_object_id ON accounting_booking_review(object_id);",
            ]
            for sql in migration_statements:
                cursor.execute(sql)
    seed_chart_of_accounts(connection)
    connection.commit()


def upsert_solf_attribute_proposals(connection: Any, proposals: list[dict[str, Any]]) -> int:
    if not proposals:
        return 0

    sql = """
    INSERT INTO solf_attribute_proposals (
        class_name,
        attribute_name,
        first_seen_doc_id,
        last_seen_doc_id,
        sample_value,
        occurrence_count,
        status,
        metadata,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
    ON CONFLICT (class_name, attribute_name)
    DO UPDATE SET
        last_seen_doc_id = EXCLUDED.last_seen_doc_id,
        sample_value = COALESCE(solf_attribute_proposals.sample_value, EXCLUDED.sample_value),
        occurrence_count = COALESCE(solf_attribute_proposals.occurrence_count, 0) + EXCLUDED.occurrence_count,
        metadata = COALESCE(solf_attribute_proposals.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    """

    written = 0
    with connection.cursor() as cursor:
        for proposal in proposals:
            class_name = str(proposal.get("class_name") or "").strip().lower()
            attribute_name = str(proposal.get("attribute_name") or "").strip().lower()
            if not class_name or not attribute_name:
                continue

            metadata = proposal.get("metadata") if isinstance(proposal.get("metadata"), dict) else {}
            cursor.execute(
                sql,
                (
                    class_name,
                    attribute_name,
                    proposal.get("first_seen_doc_id"),
                    proposal.get("last_seen_doc_id"),
                    str(proposal.get("sample_value") or "")[:1000] or None,
                    int(proposal.get("occurrence_count") or 1),
                    str(proposal.get("status") or "proposed").strip().lower() or "proposed",
                    json.dumps(metadata, ensure_ascii=False, default=str),
                ),
            )
            written += 1

    connection.commit()
    return written


def get_approved_solf_attribute_extensions(connection: Any) -> dict[str, list[str]]:
    sql = """
    SELECT class_name, attribute_name
    FROM solf_attribute_proposals
    WHERE status = 'approved'
    ORDER BY class_name, attribute_name
    """

    extensions: dict[str, list[str]] = {}
    with connection.cursor() as cursor:
        cursor.execute(sql)
        for class_name, attribute_name in cursor.fetchall() or []:
            normalized_class = str(class_name or "").strip().lower()
            normalized_attribute = str(attribute_name or "").strip().lower()
            if not normalized_class or not normalized_attribute:
                continue
            extensions.setdefault(normalized_class, []).append(normalized_attribute)

    return extensions


def list_solf_attribute_proposals(
    connection: Any,
    status: str | None = None,
    class_name: str | None = None,
    priority_bucket: str | None = None,
    suggested_only: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    normalized_status = str(status or "").strip().lower()
    if normalized_status:
        clauses.append("status = %s")
        params.append(normalized_status)

    normalized_class_name = str(class_name or "").strip().lower()
    if normalized_class_name:
        clauses.append("class_name = %s")
        params.append(normalized_class_name)

    sql = """
    SELECT
        proposal_id,
        class_name,
        attribute_name,
        first_seen_doc_id,
        last_seen_doc_id,
        sample_value,
        occurrence_count,
        status,
        metadata,
        created_at,
        updated_at
    FROM solf_attribute_proposals
    """
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY occurrence_count DESC, updated_at DESC LIMIT %s"
    params.append(int(limit))

    results: list[dict[str, Any]] = []
    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        for row in cursor.fetchall() or []:
            results.append(
                _enrich_solf_attribute_proposal_record(
                    {
                        "proposal_id": int(row[0]),
                        "class_name": str(row[1] or "").strip().lower(),
                        "attribute_name": str(row[2] or "").strip().lower(),
                        "first_seen_doc_id": row[3],
                        "last_seen_doc_id": row[4],
                        "sample_value": row[5],
                        "occurrence_count": int(row[6] or 0),
                        "status": str(row[7] or "").strip().lower(),
                        "metadata": row[8] if isinstance(row[8], dict) else {},
                        "created_at": row[9].isoformat() if row[9] is not None else None,
                        "updated_at": row[10].isoformat() if row[10] is not None else None,
                    }
                )
            )
    normalized_priority_bucket = str(priority_bucket or "").strip().lower()
    if normalized_priority_bucket:
        results = [row for row in results if str(row.get("priority_bucket") or "").strip().lower() == normalized_priority_bucket]

    if suggested_only:
        results = [row for row in results if bool(row.get("suggested_for_approval"))]

    results.sort(
        key=lambda row: (
            -int(row.get("priority_score") or 0),
            -int(row.get("occurrence_count") or 0),
            str(row.get("updated_at") or ""),
        )
    )
    return results[: int(limit)]


def update_solf_attribute_proposal_status(
    connection: Any,
    proposal_id: int,
    status: str,
    reviewed_by: str = "api:user",
    note: str = "",
) -> dict[str, Any] | None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"proposed", "approved", "rejected"}:
        raise ValueError("status must be one of proposed, approved, rejected")

    sql = """
    UPDATE solf_attribute_proposals
    SET
        status = %s,
        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
        updated_at = NOW()
    WHERE proposal_id = %s
    RETURNING
        proposal_id,
        class_name,
        attribute_name,
        first_seen_doc_id,
        last_seen_doc_id,
        sample_value,
        occurrence_count,
        status,
        metadata,
        created_at,
        updated_at
    """

    review_metadata = {
        "reviewed_by": str(reviewed_by or "api:user").strip() or "api:user",
        "review_note": str(note or "").strip(),
    }

    with connection.cursor() as cursor:
        cursor.execute(sql, (normalized_status, json.dumps(review_metadata, ensure_ascii=False), int(proposal_id)))
        row = cursor.fetchone()
    connection.commit()

    if row is None:
        return None

    return _enrich_solf_attribute_proposal_record(
        {
            "proposal_id": int(row[0]),
            "class_name": str(row[1] or "").strip().lower(),
            "attribute_name": str(row[2] or "").strip().lower(),
            "first_seen_doc_id": row[3],
            "last_seen_doc_id": row[4],
            "sample_value": row[5],
            "occurrence_count": int(row[6] or 0),
            "status": str(row[7] or "").strip().lower(),
            "metadata": row[8] if isinstance(row[8], dict) else {},
            "created_at": row[9].isoformat() if row[9] is not None else None,
            "updated_at": row[10].isoformat() if row[10] is not None else None,
        }
    )


def _score_solf_attribute_proposal(proposal: dict[str, Any]) -> tuple[int, str]:
    occurrence_count = max(0, int(proposal.get("occurrence_count") or 0))
    status = str(proposal.get("status") or "").strip().lower()
    recurrence_bonus = max(0, occurrence_count - 1)
    status_bonus = 2 if status == "proposed" else 1 if status == "approved" else 0
    score = occurrence_count + recurrence_bonus + status_bonus

    if score >= max(0, int(SOLF_PROPOSAL_PRIORITY_HIGH_THRESHOLD)):
        bucket = "high"
    elif score >= max(0, int(SOLF_PROPOSAL_PRIORITY_MEDIUM_THRESHOLD)):
        bucket = "medium"
    else:
        bucket = "low"
    return score, bucket


def _enrich_solf_attribute_proposal_record(proposal: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(proposal)
    score, bucket = _score_solf_attribute_proposal(enriched)
    enriched["priority_score"] = score
    enriched["priority_bucket"] = bucket
    occurrence_count = max(0, int(enriched.get("occurrence_count") or 0))
    status = str(enriched.get("status") or "").strip().lower()
    suggested_for_approval = bool(
        status == "proposed"
        and (
            occurrence_count >= max(1, int(SOLF_PROPOSAL_SUGGEST_OCCURRENCE_THRESHOLD))
            or score >= max(1, int(SOLF_PROPOSAL_SUGGEST_PRIORITY_THRESHOLD))
        )
    )
    if suggested_for_approval:
        if occurrence_count >= max(1, int(SOLF_PROPOSAL_SUGGEST_OCCURRENCE_THRESHOLD)):
            suggestion_reason = f"recurs_across_documents:{occurrence_count}"
        else:
            suggestion_reason = f"high_priority_score:{score}"
    else:
        suggestion_reason = ""
    enriched["suggested_for_approval"] = suggested_for_approval
    enriched["suggestion_reason"] = suggestion_reason
    return enriched


def create_solf_schema_promotion_batch(
    connection: Any,
    batch_name: str,
    proposal_ids: list[int],
    patch_preview: str,
    class_name: str | None = None,
    created_by: str = "api:user",
    note: str = "",
) -> dict[str, Any]:
    sql = """
    INSERT INTO solf_schema_promotion_batches (
        batch_name,
        class_name,
        status,
        proposal_ids,
        patch_preview,
        metadata,
        updated_at
    )
    VALUES (%s, %s, 'draft', %s::jsonb, %s, %s::jsonb, NOW())
    RETURNING batch_id, batch_name, class_name, status, proposal_ids, patch_preview, metadata, created_at, updated_at
    """

    metadata = {
        "created_by": str(created_by or "api:user").strip() or "api:user",
        "note": str(note or "").strip(),
    }
    normalized_ids = sorted({int(item) for item in proposal_ids if int(item) > 0})

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                str(batch_name or "").strip() or "solf_schema_promotion_batch",
                str(class_name or "").strip().lower() or None,
                json.dumps(normalized_ids),
                str(patch_preview or ""),
                json.dumps(metadata, ensure_ascii=False),
            ),
        )
        row = cursor.fetchone()
    connection.commit()

    return {
        "batch_id": int(row[0]),
        "batch_name": str(row[1] or "").strip(),
        "class_name": str(row[2] or "").strip().lower() or None,
        "status": str(row[3] or "").strip().lower(),
        "proposal_ids": row[4] if isinstance(row[4], list) else [],
        "patch_preview": str(row[5] or ""),
        "metadata": row[6] if isinstance(row[6], dict) else {},
        "created_at": row[7].isoformat() if row[7] is not None else None,
        "updated_at": row[8].isoformat() if row[8] is not None else None,
    }


def list_solf_schema_promotion_batches(connection: Any, limit: int = 100) -> list[dict[str, Any]]:
    sql = """
    SELECT batch_id, batch_name, class_name, status, proposal_ids, patch_preview, metadata, created_at, updated_at
    FROM solf_schema_promotion_batches
    ORDER BY updated_at DESC, batch_id DESC
    LIMIT %s
    """

    results: list[dict[str, Any]] = []
    with connection.cursor() as cursor:
        cursor.execute(sql, (int(limit),))
        for row in cursor.fetchall() or []:
            results.append(
                {
                    "batch_id": int(row[0]),
                    "batch_name": str(row[1] or "").strip(),
                    "class_name": str(row[2] or "").strip().lower() or None,
                    "status": str(row[3] or "").strip().lower(),
                    "proposal_ids": row[4] if isinstance(row[4], list) else [],
                    "patch_preview": str(row[5] or ""),
                    "metadata": row[6] if isinstance(row[6], dict) else {},
                    "created_at": row[7].isoformat() if row[7] is not None else None,
                    "updated_at": row[8].isoformat() if row[8] is not None else None,
                }
            )
    return results


def get_solf_schema_promotion_batch(connection: Any, batch_id: int) -> dict[str, Any] | None:
    sql = """
    SELECT batch_id, batch_name, class_name, status, proposal_ids, patch_preview, metadata, created_at, updated_at
    FROM solf_schema_promotion_batches
    WHERE batch_id = %s
    LIMIT 1
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (int(batch_id),))
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "batch_id": int(row[0]),
        "batch_name": str(row[1] or "").strip(),
        "class_name": str(row[2] or "").strip().lower() or None,
        "status": str(row[3] or "").strip().lower(),
        "proposal_ids": row[4] if isinstance(row[4], list) else [],
        "patch_preview": str(row[5] or ""),
        "metadata": row[6] if isinstance(row[6], dict) else {},
        "created_at": row[7].isoformat() if row[7] is not None else None,
        "updated_at": row[8].isoformat() if row[8] is not None else None,
    }


def update_solf_schema_promotion_batch_status(
    connection: Any,
    batch_id: int,
    status: str,
    reviewed_by: str = "api:user",
    note: str = "",
) -> dict[str, Any] | None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"draft", "reviewed", "exported", "applied"}:
        raise ValueError("status must be one of draft, reviewed, exported, applied")

    sql = """
    UPDATE solf_schema_promotion_batches
    SET
        status = %s,
        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
        updated_at = NOW()
    WHERE batch_id = %s
    RETURNING batch_id, batch_name, class_name, status, proposal_ids, patch_preview, metadata, created_at, updated_at
    """

    metadata = {
        "reviewed_by": str(reviewed_by or "api:user").strip() or "api:user",
        "review_note": str(note or "").strip(),
    }

    with connection.cursor() as cursor:
        cursor.execute(sql, (normalized_status, json.dumps(metadata, ensure_ascii=False), int(batch_id)))
        row = cursor.fetchone()
    connection.commit()

    if row is None:
        return None

    return {
        "batch_id": int(row[0]),
        "batch_name": str(row[1] or "").strip(),
        "class_name": str(row[2] or "").strip().lower() or None,
        "status": str(row[3] or "").strip().lower(),
        "proposal_ids": row[4] if isinstance(row[4], list) else [],
        "patch_preview": str(row[5] or ""),
        "metadata": row[6] if isinstance(row[6], dict) else {},
        "created_at": row[7].isoformat() if row[7] is not None else None,
        "updated_at": row[8].isoformat() if row[8] is not None else None,
    }


def update_solf_schema_promotion_batch_audit_link(
    connection: Any,
    batch_id: int,
    audit_metadata: dict[str, Any],
) -> dict[str, Any] | None:
    sql = """
    UPDATE solf_schema_promotion_batches
    SET
        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
        updated_at = NOW()
    WHERE batch_id = %s
    RETURNING batch_id, batch_name, class_name, status, proposal_ids, patch_preview, metadata, created_at, updated_at
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (json.dumps(audit_metadata, ensure_ascii=False, default=str), int(batch_id)))
        row = cursor.fetchone()
    connection.commit()

    if row is None:
        return None

    return {
        "batch_id": int(row[0]),
        "batch_name": str(row[1] or "").strip(),
        "class_name": str(row[2] or "").strip().lower() or None,
        "status": str(row[3] or "").strip().lower(),
        "proposal_ids": row[4] if isinstance(row[4], list) else [],
        "patch_preview": str(row[5] or ""),
        "metadata": row[6] if isinstance(row[6], dict) else {},
        "created_at": row[7].isoformat() if row[7] is not None else None,
        "updated_at": row[8].isoformat() if row[8] is not None else None,
    }


def _get_coa_profile_name() -> str:
    requested = str(os.getenv("IKOS_COA_PROFILE", "swiss_sme") or "swiss_sme").strip().lower()
    if requested in CHART_OF_ACCOUNTS_PROFILES:
        return requested
    LOGGER.warning("Unknown IKOS_COA_PROFILE='%s'; falling back to swiss_sme", requested)
    return "swiss_sme"


def get_seed_records_for_profile(profile_name: str) -> list[dict[str, Any]]:
    return CHART_OF_ACCOUNTS_PROFILES.get(profile_name, DEFAULT_CHART_OF_ACCOUNTS)


def _get_hr_profile_name() -> str:
    requested = str(os.getenv("IKOS_HR_PROFILE", "swiss_sme") or "swiss_sme").strip().lower()
    if requested in HR_MASTER_PROFILES:
        return requested
    LOGGER.warning("Unknown IKOS_HR_PROFILE='%s'; falling back to swiss_sme", requested)
    return "swiss_sme"


def seed_hr_master_data(connection: Any, profile_name: str | None = None, overwrite: bool = False) -> dict[str, int]:
    selected_profile = (profile_name or _get_hr_profile_name()).strip().lower()
    profile = HR_MASTER_PROFILES.get(selected_profile, HR_MASTER_PROFILES["swiss_sme"])
    departments = profile.get("departments") or []
    roles = profile.get("roles") or []

    dep_count_sql = "SELECT COUNT(*) FROM hr_department_master"
    role_count_sql = "SELECT COUNT(*) FROM hr_role_master"

    dep_upsert_sql = """
    INSERT INTO hr_department_master (department_code, department_name, active, metadata, updated_at)
    VALUES (%s, %s, %s, %s::jsonb, NOW())
    ON CONFLICT (department_code)
    DO UPDATE SET
        department_name = EXCLUDED.department_name,
        active = EXCLUDED.active,
        metadata = COALESCE(hr_department_master.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    """

    role_upsert_sql = """
    INSERT INTO hr_role_master (role_code, role_name, role_family, seniority_level, active, metadata, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
    ON CONFLICT (role_code)
    DO UPDATE SET
        role_name = EXCLUDED.role_name,
        role_family = EXCLUDED.role_family,
        seniority_level = EXCLUDED.seniority_level,
        active = EXCLUDED.active,
        metadata = COALESCE(hr_role_master.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    """

    written_departments = 0
    written_roles = 0
    with connection.cursor() as cursor:
        cursor.execute(dep_count_sql)
        dep_existing = int(cursor.fetchone()[0])
        cursor.execute(role_count_sql)
        role_existing = int(cursor.fetchone()[0])

        if overwrite or dep_existing == 0:
            for row in departments:
                code = str(row.get("department_code") or "").strip().upper()
                name = str(row.get("department_name") or "").strip()
                if not code or not name:
                    continue
                cursor.execute(
                    dep_upsert_sql,
                    (
                        code,
                        name,
                        True,
                        json.dumps({"profile": selected_profile}, ensure_ascii=False),
                    ),
                )
                written_departments += 1

        if overwrite or role_existing == 0:
            for row in roles:
                code = str(row.get("role_code") or "").strip().upper()
                name = str(row.get("role_name") or "").strip()
                role_family = str(row.get("role_family") or "").strip().lower() or None
                seniority_level = str(row.get("seniority_level") or "").strip().lower() or None
                if not code or not name:
                    continue
                cursor.execute(
                    role_upsert_sql,
                    (
                        code,
                        name,
                        role_family,
                        seniority_level,
                        True,
                        json.dumps({"profile": selected_profile}, ensure_ascii=False),
                    ),
                )
                written_roles += 1

    return {
        "departments_seeded": written_departments,
        "roles_seeded": written_roles,
        "profile": selected_profile,
    }


def seed_chart_of_accounts(
    connection: Any,
    records: list[dict[str, Any]] | None = None,
    overwrite: bool = False,
    legal_entity_ref: str = "global",
) -> int:
    if isinstance(records, list) and records:
        seed_rows = records
    else:
        profile_name = _get_coa_profile_name()
        seed_rows = get_seed_records_for_profile(profile_name)
    if not seed_rows:
        return 0

    scope_ref = str(legal_entity_ref or "global").strip() or "global"
    upsert_sql = """
    INSERT INTO chart_of_accounts (legal_entity_ref, account_number, account_name, account_type)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (legal_entity_ref, account_number)
    DO UPDATE SET
        account_name = EXCLUDED.account_name,
        account_type = EXCLUDED.account_type
    """

    with connection.cursor() as cursor:
        written = 0
        for row in seed_rows:
            account_number = int(row.get("account_number") or 0)
            account_name = str(row.get("account_name") or "").strip()
            account_type = str(row.get("account_type") or "").strip().lower()
            if account_number <= 0 or not account_name or not account_type:
                continue
            cursor.execute(upsert_sql, (scope_ref, account_number, account_name, account_type))
            written += 1
    return written


_ACCOUNT_TYPES = {"asset", "liability", "equity", "revenue", "expense"}
_DOMAIN_DEFINITION_TYPES = {
    "account_definition": "account_definition",
    "account": "account_definition",
    "chart_of_accounts": "account_definition",
    "coa": "account_definition",
    "chart of accounts": "account_definition",
    "ledger_account": "account_definition",
    "konto": "account_definition",
    "konten": "account_definition",
    "hr_department_definition": "hr_department_definition",
    "department_definition": "hr_department_definition",
    "department": "hr_department_definition",
    "dept": "hr_department_definition",
    "abteilung": "hr_department_definition",
    "hr_role_definition": "hr_role_definition",
    "role_definition": "hr_role_definition",
    "role": "hr_role_definition",
    "position": "hr_role_definition",
    "job_role": "hr_role_definition",
    "funktion": "hr_role_definition",
    "stelle": "hr_role_definition",
}


def normalize_definition_type(definition_type: str) -> str:
    normalized = str(definition_type or "").strip().lower()
    return _DOMAIN_DEFINITION_TYPES.get(normalized, normalized)


def _normalize_account_definition(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str], list[str]]:
    missing: list[str] = []
    errors: list[str] = []

    legal_entity_ref = str(payload.get("legal_entity_ref") or "global").strip() or "global"
    account_name = str(payload.get("account_name") or "").strip()
    account_type = str(payload.get("account_type") or "").strip().lower()

    account_number_raw = payload.get("account_number")
    try:
        account_number = int(account_number_raw) if account_number_raw is not None else 0
    except Exception:
        account_number = 0

    if account_number <= 0:
        if account_number_raw in (None, ""):
            missing.append("account_number")
        else:
            errors.append("account_number_invalid")

    if not account_name:
        missing.append("account_name")

    if not account_type:
        missing.append("account_type")
    elif account_type not in _ACCOUNT_TYPES:
        errors.append("account_type_invalid")

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    normalized = {
        "legal_entity_ref": legal_entity_ref,
        "account_number": account_number,
        "account_name": account_name,
        "account_type": account_type,
        "metadata": metadata,
    }
    return normalized, missing, errors


def _normalize_department_definition(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str], list[str]]:
    missing: list[str] = []
    errors: list[str] = []

    department_code = str(payload.get("department_code") or "").strip().upper()
    department_name = str(payload.get("department_name") or "").strip()
    active = bool(payload.get("active", True))
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    if not department_code:
        missing.append("department_code")
    if not department_name:
        missing.append("department_name")

    normalized = {
        "department_code": department_code,
        "department_name": department_name,
        "active": active,
        "metadata": metadata,
    }
    return normalized, missing, errors


def _normalize_role_definition(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str], list[str]]:
    missing: list[str] = []
    errors: list[str] = []

    role_code = str(payload.get("role_code") or "").strip().upper()
    role_name = str(payload.get("role_name") or "").strip()
    role_family = str(payload.get("role_family") or "").strip().lower() or None
    seniority_level = str(payload.get("seniority_level") or "").strip().lower() or None
    active = bool(payload.get("active", True))
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    if not role_code:
        missing.append("role_code")
    if not role_name:
        missing.append("role_name")

    normalized = {
        "role_code": role_code,
        "role_name": role_name,
        "role_family": role_family,
        "seniority_level": seniority_level,
        "active": active,
        "metadata": metadata,
    }
    return normalized, missing, errors


def validate_domain_definition_payload(
    definition_type: str,
    payload: dict[str, Any],
    operation: str,
) -> dict[str, Any]:
    normalized_type = normalize_definition_type(definition_type)
    normalized_op = str(operation or "").strip().lower()
    content = payload if isinstance(payload, dict) else {}

    if normalized_type == "account_definition":
        normalized, missing, errors = _normalize_account_definition(content)
        key_fields = ["legal_entity_ref", "account_number"]
    elif normalized_type == "hr_department_definition":
        normalized, missing, errors = _normalize_department_definition(content)
        key_fields = ["department_code"]
    elif normalized_type == "hr_role_definition":
        normalized, missing, errors = _normalize_role_definition(content)
        key_fields = ["role_code"]
    else:
        return {
            "valid": False,
            "definition_type": normalized_type,
            "operation": normalized_op,
            "missing_fields": [],
            "errors": ["unsupported_definition_type"],
            "normalized_payload": {},
        }

    if normalized_op in {"delete", "read", "get"}:
        missing = [field for field in key_fields if not normalized.get(field)]
        errors = []
    elif normalized_op == "update":
        missing_key = [field for field in key_fields if not normalized.get(field)]
        missing = sorted(set(missing + missing_key))

    return {
        "valid": not missing and not errors,
        "definition_type": normalized_type,
        "operation": normalized_op,
        "missing_fields": sorted(set(missing)),
        "errors": sorted(set(errors)),
        "normalized_payload": normalized,
    }


def create_domain_definition(connection: Any, definition_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    validation = validate_domain_definition_payload(definition_type, payload, operation="create")
    if not validation["valid"]:
        return {"success": False, "message": "Validation failed", "validation": validation}

    normalized_type = validation["definition_type"]
    data = validation["normalized_payload"]

    with connection.cursor() as cursor:
        if normalized_type == "account_definition":
            cursor.execute(
                "SELECT 1 FROM chart_of_accounts WHERE legal_entity_ref = %s AND account_number = %s LIMIT 1",
                (data["legal_entity_ref"], int(data["account_number"])),
            )
            if cursor.fetchone() is not None:
                return {
                    "success": False,
                    "message": "Account definition already exists",
                    "definition_type": normalized_type,
                    "key": {
                        "legal_entity_ref": data["legal_entity_ref"],
                        "account_number": int(data["account_number"]),
                    },
                }
            cursor.execute(
                """
                INSERT INTO chart_of_accounts (legal_entity_ref, account_number, account_name, account_type)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    data["legal_entity_ref"],
                    int(data["account_number"]),
                    data["account_name"],
                    data["account_type"],
                ),
            )
            connection.commit()
            key_str = f"{data['legal_entity_ref']}/{data['account_number']}"
            log_domain_definition_audit(
                connection,
                normalized_type,
                "create",
                key_str,
                before_state=None,
                after_state=data,
            )
            return {
                "success": True,
                "operation": "create",
                "definition_type": normalized_type,
                "key": {
                    "legal_entity_ref": data["legal_entity_ref"],
                    "account_number": int(data["account_number"]),
                },
            }

        if normalized_type == "hr_department_definition":
            cursor.execute(
                "SELECT 1 FROM hr_department_master WHERE department_code = %s LIMIT 1",
                (data["department_code"],),
            )
            if cursor.fetchone() is not None:
                return {
                    "success": False,
                    "message": "Department definition already exists",
                    "definition_type": normalized_type,
                    "key": {"department_code": data["department_code"]},
                }
            cursor.execute(
                """
                INSERT INTO hr_department_master (department_code, department_name, active, metadata, updated_at)
                VALUES (%s, %s, %s, %s::jsonb, NOW())
                """,
                (
                    data["department_code"],
                    data["department_name"],
                    bool(data["active"]),
                    json.dumps(data.get("metadata") or {}, ensure_ascii=False, default=str),
                ),
            )
            connection.commit()
            log_domain_definition_audit(
                connection,
                normalized_type,
                "create",
                data["department_code"],
                before_state=None,
                after_state=data,
            )
            return {
                "success": True,
                "operation": "create",
                "definition_type": normalized_type,
                "key": {"department_code": data["department_code"]},
            }

        if normalized_type == "hr_role_definition":
            cursor.execute(
                "SELECT 1 FROM hr_role_master WHERE role_code = %s LIMIT 1",
                (data["role_code"],),
            )
            if cursor.fetchone() is not None:
                return {
                    "success": False,
                    "message": "Role definition already exists",
                    "definition_type": normalized_type,
                    "key": {"role_code": data["role_code"]},
                }
            cursor.execute(
                """
                INSERT INTO hr_role_master (role_code, role_name, role_family, seniority_level, active, metadata, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                """,
                (
                    data["role_code"],
                    data["role_name"],
                    data.get("role_family"),
                    data.get("seniority_level"),
                    bool(data["active"]),
                    json.dumps(data.get("metadata") or {}, ensure_ascii=False, default=str),
                ),
            )
            connection.commit()
            log_domain_definition_audit(
                connection,
                normalized_type,
                "create",
                data["role_code"],
                before_state=None,
                after_state=data,
            )
            return {
                "success": True,
                "operation": "create",
                "definition_type": normalized_type,
                "key": {"role_code": data["role_code"]},
            }

    return {"success": False, "message": "Unsupported definition_type", "definition_type": normalized_type}


def update_domain_definition(connection: Any, definition_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    validation = validate_domain_definition_payload(definition_type, payload, operation="update")
    if not validation["valid"]:
        return {"success": False, "message": "Validation failed", "validation": validation}

    normalized_type = validation["definition_type"]
    data = validation["normalized_payload"]

    with connection.cursor() as cursor:
        if normalized_type == "account_definition":
            cursor.execute(
                """
                UPDATE chart_of_accounts
                SET account_name = %s,
                    account_type = %s
                WHERE legal_entity_ref = %s
                  AND account_number = %s
                """,
                (
                    data["account_name"],
                    data["account_type"],
                    data["legal_entity_ref"],
                    int(data["account_number"]),
                ),
            )
            updated = int(cursor.rowcount)
            connection.commit()
            if updated <= 0:
                return {
                    "success": False,
                    "message": "Account definition not found",
                    "definition_type": normalized_type,
                    "key": {
                        "legal_entity_ref": data["legal_entity_ref"],
                        "account_number": int(data["account_number"]),
                    },
                }
            return {
                "success": True,
                "operation": "update",
                "definition_type": normalized_type,
                "rows_affected": updated,
            }

        if normalized_type == "hr_department_definition":
            cursor.execute(
                """
                UPDATE hr_department_master
                SET department_name = %s,
                    active = %s,
                    metadata = COALESCE(hr_department_master.metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE department_code = %s
                """,
                (
                    data["department_name"],
                    bool(data["active"]),
                    json.dumps(data.get("metadata") or {}, ensure_ascii=False, default=str),
                    data["department_code"],
                ),
            )
            updated = int(cursor.rowcount)
            connection.commit()
            if updated <= 0:
                return {
                    "success": False,
                    "message": "Department definition not found",
                    "definition_type": normalized_type,
                    "key": {"department_code": data["department_code"]},
                }
            return {
                "success": True,
                "operation": "update",
                "definition_type": normalized_type,
                "rows_affected": updated,
            }

        if normalized_type == "hr_role_definition":
            cursor.execute(
                """
                UPDATE hr_role_master
                SET role_name = %s,
                    role_family = %s,
                    seniority_level = %s,
                    active = %s,
                    metadata = COALESCE(hr_role_master.metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE role_code = %s
                """,
                (
                    data["role_name"],
                    data.get("role_family"),
                    data.get("seniority_level"),
                    bool(data["active"]),
                    json.dumps(data.get("metadata") or {}, ensure_ascii=False, default=str),
                    data["role_code"],
                ),
            )
            updated = int(cursor.rowcount)
            connection.commit()
            if updated <= 0:
                return {
                    "success": False,
                    "message": "Role definition not found",
                    "definition_type": normalized_type,
                    "key": {"role_code": data["role_code"]},
                }
            return {
                "success": True,
                "operation": "update",
                "definition_type": normalized_type,
                "rows_affected": updated,
            }

    return {"success": False, "message": "Unsupported definition_type", "definition_type": normalized_type}


def delete_domain_definition(connection: Any, definition_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    validation = validate_domain_definition_payload(definition_type, payload, operation="delete")
    if not validation["valid"]:
        return {"success": False, "message": "Validation failed", "validation": validation}

    normalized_type = validation["definition_type"]
    data = validation["normalized_payload"]
    key_str = ""

    with connection.cursor() as cursor:
        if normalized_type == "account_definition":
            key_str = f"{data['legal_entity_ref']}/{data['account_number']}"
            cursor.execute(
                "DELETE FROM chart_of_accounts WHERE legal_entity_ref = %s AND account_number = %s",
                (data["legal_entity_ref"], int(data["account_number"])),
            )
        elif normalized_type == "hr_department_definition":
            key_str = data["department_code"]
            cursor.execute(
                "DELETE FROM hr_department_master WHERE department_code = %s",
                (data["department_code"],),
            )
        elif normalized_type == "hr_role_definition":
            key_str = data["role_code"]
            cursor.execute(
                "DELETE FROM hr_role_master WHERE role_code = %s",
                (data["role_code"],),
            )
        else:
            return {"success": False, "message": "Unsupported definition_type", "definition_type": normalized_type}
        deleted = int(cursor.rowcount)
    connection.commit()

    if deleted <= 0:
        return {
            "success": False,
            "message": "Definition not found",
            "definition_type": normalized_type,
            "rows_affected": 0,
        }
    
    log_domain_definition_audit(
        connection,
        normalized_type,
        "delete",
        key_str,
        before_state=data,
        after_state=None,
    )
    
    return {
        "success": True,
        "operation": "delete",
        "definition_type": normalized_type,
        "rows_affected": deleted,
    }


def get_domain_definition(connection: Any, definition_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    validation = validate_domain_definition_payload(definition_type, payload, operation="read")
    if not validation["valid"]:
        return {"success": False, "message": "Validation failed", "validation": validation}

    normalized_type = validation["definition_type"]
    data = validation["normalized_payload"]

    with connection.cursor() as cursor:
        if normalized_type == "account_definition":
            cursor.execute(
                """
                SELECT legal_entity_ref, account_number, account_name, account_type
                FROM chart_of_accounts
                WHERE legal_entity_ref = %s
                  AND account_number = %s
                LIMIT 1
                """,
                (data["legal_entity_ref"], int(data["account_number"])),
            )
            row = cursor.fetchone()
            if row is None:
                return {"success": False, "message": "Definition not found", "definition_type": normalized_type}
            return {
                "success": True,
                "operation": "read",
                "definition_type": normalized_type,
                "definition": {
                    "legal_entity_ref": str(row[0] or "global").strip() or "global",
                    "account_number": int(row[1] or 0),
                    "account_name": str(row[2] or "").strip(),
                    "account_type": str(row[3] or "").strip().lower(),
                },
            }

        if normalized_type == "hr_department_definition":
            cursor.execute(
                """
                SELECT department_code, department_name, active, metadata
                FROM hr_department_master
                WHERE department_code = %s
                LIMIT 1
                """,
                (data["department_code"],),
            )
            row = cursor.fetchone()
            if row is None:
                return {"success": False, "message": "Definition not found", "definition_type": normalized_type}
            return {
                "success": True,
                "operation": "read",
                "definition_type": normalized_type,
                "definition": {
                    "department_code": str(row[0] or "").strip().upper(),
                    "department_name": str(row[1] or "").strip(),
                    "active": bool(row[2]),
                    "metadata": row[3] if isinstance(row[3], dict) else {},
                },
            }

        if normalized_type == "hr_role_definition":
            cursor.execute(
                """
                SELECT role_code, role_name, role_family, seniority_level, active, metadata
                FROM hr_role_master
                WHERE role_code = %s
                LIMIT 1
                """,
                (data["role_code"],),
            )
            row = cursor.fetchone()
            if row is None:
                return {"success": False, "message": "Definition not found", "definition_type": normalized_type}
            return {
                "success": True,
                "operation": "read",
                "definition_type": normalized_type,
                "definition": {
                    "role_code": str(row[0] or "").strip().upper(),
                    "role_name": str(row[1] or "").strip(),
                    "role_family": str(row[2] or "").strip().lower() or None,
                    "seniority_level": str(row[3] or "").strip().lower() or None,
                    "active": bool(row[4]),
                    "metadata": row[5] if isinstance(row[5], dict) else {},
                },
            }

    return {"success": False, "message": "Unsupported definition_type", "definition_type": normalized_type}


def list_domain_definitions(
    connection: Any,
    definition_type: str,
    filters: dict[str, Any] | None = None,
    limit: int = 200,
    offset: int = 0,
    include_inactive: bool = False,
) -> dict[str, Any]:
    normalized_type = normalize_definition_type(definition_type)
    filters = filters if isinstance(filters, dict) else {}
    limit = max(1, min(int(limit or 200), 5000))
    offset = max(0, int(offset or 0))

    with connection.cursor() as cursor:
        if normalized_type == "account_definition":
            legal_entity_ref = str(filters.get("legal_entity_ref") or "").strip()
            if legal_entity_ref:
                cursor.execute(
                    """
                    SELECT legal_entity_ref, account_number, account_name, account_type
                    FROM chart_of_accounts
                    WHERE legal_entity_ref = %s
                    ORDER BY account_number ASC
                    LIMIT %s OFFSET %s
                    """,
                    (legal_entity_ref, limit, offset),
                )
            else:
                cursor.execute(
                    """
                    SELECT legal_entity_ref, account_number, account_name, account_type
                    FROM chart_of_accounts
                    ORDER BY legal_entity_ref ASC, account_number ASC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
            rows = cursor.fetchall() or []
            return {
                "success": True,
                "operation": "list",
                "definition_type": normalized_type,
                "count": len(rows),
                "definitions": [
                    {
                        "legal_entity_ref": str(row[0] or "global").strip() or "global",
                        "account_number": int(row[1] or 0),
                        "account_name": str(row[2] or "").strip(),
                        "account_type": str(row[3] or "").strip().lower(),
                    }
                    for row in rows
                ],
            }

        if normalized_type == "hr_department_definition":
            sql = """
            SELECT department_code, department_name, active, metadata
            FROM hr_department_master
            """
            params: list[Any] = []
            if not include_inactive:
                sql += " WHERE active = TRUE"
            sql += " ORDER BY department_code ASC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall() or []
            return {
                "success": True,
                "operation": "list",
                "definition_type": normalized_type,
                "count": len(rows),
                "definitions": [
                    {
                        "department_code": str(row[0] or "").strip().upper(),
                        "department_name": str(row[1] or "").strip(),
                        "active": bool(row[2]),
                        "metadata": row[3] if isinstance(row[3], dict) else {},
                    }
                    for row in rows
                ],
            }

        if normalized_type == "hr_role_definition":
            sql = """
            SELECT role_code, role_name, role_family, seniority_level, active, metadata
            FROM hr_role_master
            """
            params = []
            if not include_inactive:
                sql += " WHERE active = TRUE"
            sql += " ORDER BY role_code ASC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall() or []
            return {
                "success": True,
                "operation": "list",
                "definition_type": normalized_type,
                "count": len(rows),
                "definitions": [
                    {
                        "role_code": str(row[0] or "").strip().upper(),
                        "role_name": str(row[1] or "").strip(),
                        "role_family": str(row[2] or "").strip().lower() or None,
                        "seniority_level": str(row[3] or "").strip().lower() or None,
                        "active": bool(row[4]),
                        "metadata": row[5] if isinstance(row[5], dict) else {},
                    }
                    for row in rows
                ],
            }

    return {
        "success": False,
        "operation": "list",
        "definition_type": normalized_type,
        "message": "Unsupported definition_type",
        "count": 0,
        "definitions": [],
    }


def get_existing_account_numbers(connection: Any, account_numbers: list[int], legal_entity_ref: str | None = None) -> set[int]:
    normalized = sorted({int(number) for number in account_numbers if int(number) > 0})
    if not normalized:
        return set()

    if legal_entity_ref:
        sql = """
        SELECT account_number
        FROM chart_of_accounts
        WHERE account_number = ANY(%s)
          AND legal_entity_ref IN (%s, 'global')
        """
        params = (normalized, str(legal_entity_ref).strip())
    else:
        sql = """
        SELECT account_number
        FROM chart_of_accounts
        WHERE account_number = ANY(%s)
        """
        params = (normalized,)
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    return {int(row[0]) for row in rows}


def _tokenize_hint_text(value: Any) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    return {token for token in re.split(r"[^a-z0-9]+", text) if len(token) >= 2}


def _is_travel_context_hint(value: Any) -> bool:
    tokens = _tokenize_hint_text(value)
    if not tokens:
        return False
    travel_tokens = {
        "travel",
        "travelling",
        "trip",
        "ticket",
        "rail",
        "train",
        "bahn",
        "zug",
        "reise",
        "fahr",
        "fahrt",
        "transport",
        "journey",
    }
    return bool(tokens.intersection(travel_tokens))


def _choose_travel_expense_account_number(candidates: list[dict[str, Any]], fallback: int) -> int:
    if not candidates:
        return int(fallback)

    for candidate in candidates:
        number = int(candidate.get("account_number") or 0)
        if number == 6400:
            return 6400

    for candidate in candidates:
        number = int(candidate.get("account_number") or 0)
        if number <= 0:
            continue
        name_tokens = _tokenize_hint_text(candidate.get("account_name"))
        if {"travel", "travelling", "reise", "trip"}.intersection(name_tokens):
            return number

    return int(fallback)


def _fetch_accounts_by_type(
    connection: Any,
    legal_entity_ref: str | None,
    account_types: list[str],
) -> list[dict[str, Any]]:
    normalized_types = [str(item or "").strip().lower() for item in account_types if str(item or "").strip()]
    if not normalized_types:
        return []

    entity_ref = str(legal_entity_ref or "global").strip() or "global"
    sql = """
    SELECT account_number, account_name, account_type, legal_entity_ref
    FROM chart_of_accounts
    WHERE account_type = ANY(%s)
      AND legal_entity_ref IN (%s, 'global')
    ORDER BY
      CASE WHEN legal_entity_ref = %s THEN 0 ELSE 1 END,
      account_number
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (normalized_types, entity_ref, entity_ref))
        rows = cursor.fetchall()

    return [
        {
            "account_number": int(row[0]),
            "account_name": str(row[1] or ""),
            "account_type": str(row[2] or "").strip().lower(),
            "legal_entity_ref": str(row[3] or "global").strip() or "global",
        }
        for row in rows
        if int(row[0]) > 0
    ]


def _choose_best_account_number(
    candidates: list[dict[str, Any]],
    hint_text: str,
    direction: str,
    fallback: int,
) -> int:
    if not candidates:
        return int(fallback)

    hint_tokens = _tokenize_hint_text(hint_text)
    payable_tokens = {"payable", "payables", "kreditor", "kreditoren", "creditor", "vendor", "supplier", "verbindlichkeit", "verbindlichkeiten", "trade"}

    scored: list[tuple[int, int]] = []
    for candidate in candidates:
        number = int(candidate.get("account_number") or 0)
        if number <= 0:
            continue
        name_tokens = _tokenize_hint_text(candidate.get("account_name"))
        overlap = len(hint_tokens.intersection(name_tokens))
        score = overlap * 3
        if direction == "credit" and name_tokens.intersection(payable_tokens):
            score += 5
        scored.append((score, number))

    if not scored:
        return int(fallback)

    scored.sort(key=lambda item: (-item[0], item[1]))
    best_score, best_number = scored[0]
    if best_score <= 0:
        return int(fallback)
    return int(best_number)


def align_ledger_lines_to_chart_of_accounts(
    connection: Any,
    lines: list[dict[str, Any]],
    legal_entity_ref: str | None,
    description_hint: str = "",
) -> list[dict[str, Any]]:
    if not lines:
        return []

    account_numbers = [int(line.get("account_number") or 0) for line in lines if int(line.get("account_number") or 0) > 0]
    if not account_numbers:
        return lines

    existing = get_existing_account_numbers(connection, account_numbers, legal_entity_ref=legal_entity_ref)

    expense_accounts = _fetch_accounts_by_type(connection, legal_entity_ref, ["expense"])
    liability_accounts = _fetch_accounts_by_type(connection, legal_entity_ref, ["liability"])
    default_debit = _choose_best_account_number(expense_accounts, description_hint, "debit", 4200)
    default_credit = _choose_best_account_number(liability_accounts, description_hint, "credit", 2000)
    is_travel_context = _is_travel_context_hint(description_hint)
    travel_debit = _choose_travel_expense_account_number(expense_accounts, fallback=default_debit)

    aligned: list[dict[str, Any]] = []
    for line in lines:
        direction = str(line.get("direction") or "").strip().lower()
        line_meta = line.get("metadata") if isinstance(line.get("metadata"), dict) else {}
        line_hint = str(line_meta.get("line_description") or "")
        combined_hint = " ".join(item for item in (description_hint, line_hint) if item).strip()
        line_is_travel = is_travel_context or _is_travel_context_hint(combined_hint)

        current_number = int(line.get("account_number") or 0)
        if current_number > 0 and current_number in existing:
            if direction == "debit" and line_is_travel and current_number == 4200 and travel_debit != 4200:
                updated = dict(line)
                updated["account_number"] = int(travel_debit)
                updated_meta = dict(line_meta)
                updated_meta["original_account_number"] = current_number
                updated_meta["travel_account_inferred"] = True
                updated_meta["travel_account_hint"] = combined_hint[:240]
                updated["metadata"] = updated_meta
                aligned.append(updated)
                continue
            aligned.append(line)
            continue

        replacement = default_debit if direction == "debit" else default_credit
        if direction == "debit":
            if line_is_travel:
                replacement = travel_debit
            else:
                replacement = _choose_best_account_number(expense_accounts, combined_hint, direction, default_debit)
        elif direction == "credit":
            replacement = _choose_best_account_number(liability_accounts, combined_hint, direction, default_credit)

        updated = dict(line)
        updated["account_number"] = int(replacement)
        updated_meta = dict(line_meta)
        updated_meta["original_account_number"] = current_number
        updated_meta["account_inferred_from_coa"] = True
        updated_meta["account_inference_hint"] = combined_hint[:240]
        updated["metadata"] = updated_meta
        aligned.append(updated)

    return aligned


def _looks_like_entity_reference_token(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return bool(text and re.fullmatch(r"e\d+", text))


def _resolve_doc_local_entity_name(connection: Any, doc_id: int | None, entity_token: str) -> str:
    token = str(entity_token or "").strip()
    if not _looks_like_entity_reference_token(token):
        return ""
    if doc_id is None or int(doc_id) <= 0:
        return ""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT object_name
            FROM object_instance
            WHERE metadata->>'doc_id' = %s
              AND metadata->>'entity_id' = %s
            ORDER BY entry_date DESC, object_id DESC
            LIMIT 1
            """,
            (str(int(doc_id)), token),
        )
        row = cursor.fetchone()
    return str((row or [""])[0] or "").strip()


def _resolve_legal_entity_ref(connection: Any, payload: dict[str, Any], attributes: dict[str, Any]) -> str:
    candidate_keys = (
        "legal_entity_ref",
        "company_ref",
        "employer_ref",
        "current_employer_ref",
        "owner_ref",
    )

    doc_id_raw = attributes.get("doc_id")
    if doc_id_raw in (None, ""):
        doc_id_raw = payload.get("doc_id")
    try:
        doc_id = int(doc_id_raw) if doc_id_raw not in (None, "") else None
    except (TypeError, ValueError):
        doc_id = None

    for key in candidate_keys:
        value = attributes.get(key)
        if value is None:
            value = payload.get(key)
        text = str(value or "").strip()
        if not text:
            continue
        if _looks_like_entity_reference_token(text):
            resolved = _resolve_doc_local_entity_name(connection, doc_id, text)
            if resolved:
                return resolved
            continue
        if text:
            return text
    return "global"


def _build_account_alignment_hint(connection: Any, payload: dict[str, Any], attributes: dict[str, Any]) -> str:
    hint_parts: list[str] = []

    direct_keys = (
        "description",
        "source_type",
        "document_type",
        "booking_particulars",
        "booking_debit_account_name",
        "source_document_ref",
    )
    for key in direct_keys:
        value = str(attributes.get(key) or "").strip()
        if value:
            hint_parts.append(value)

    object_name = str(payload.get("object_name") or "").strip()
    if object_name:
        hint_parts.append(object_name)

    doc_id_raw = payload.get("doc_id")
    try:
        doc_id = int(doc_id_raw) if doc_id_raw is not None else 0
    except (TypeError, ValueError):
        doc_id = 0

    if doc_id > 0:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT class_name, object_name, metadata
                FROM object_instance
                WHERE metadata->>'doc_id' = %s
                  AND class_name IN ('invoice', 'bill', 'receipt', 'ticket', 'service', 'organization')
                ORDER BY object_id ASC
                """,
                (str(doc_id),),
            )
            rows = cursor.fetchall() or []

        metadata_keys = (
            "invoice_type",
            "description",
            "issuer_name",
            "supplier_name",
            "vendor_name",
            "entity_name",
            "booking_particulars",
            "payment_method",
        )
        for class_name, row_object_name, metadata in rows:
            class_text = str(class_name or "").strip()
            if class_text:
                hint_parts.append(class_text)
            object_text = str(row_object_name or "").strip()
            if object_text:
                hint_parts.append(object_text)
            md = metadata if isinstance(metadata, dict) else {}
            for key in metadata_keys:
                value = str(md.get(key) or "").strip()
                if value:
                    hint_parts.append(value)

    deduped: list[str] = []
    seen: set[str] = set()
    for part in hint_parts:
        normalized = part.strip()
        if not normalized:
            continue
        marker = normalized.lower()
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(normalized)
    return " ".join(deduped)


def department_exists(connection: Any, department_code: str) -> bool:
    code = str(department_code or "").strip().upper()
    if not code:
        return False

    sql = """
    SELECT 1
    FROM hr_department_master
    WHERE department_code = %s
      AND active = TRUE
    LIMIT 1
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (code,))
        return cursor.fetchone() is not None


def role_exists(connection: Any, role_code: str) -> bool:
    code = str(role_code or "").strip().upper()
    if not code:
        return False

    sql = """
    SELECT 1
    FROM hr_role_master
    WHERE role_code = %s
      AND active = TRUE
    LIMIT 1
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (code,))
        return cursor.fetchone() is not None


def validate_hr_payload(class_name: str, attributes: dict[str, Any], connection: Any | None = None) -> dict[str, Any]:
    normalized = str(class_name or "").strip().lower()
    attrs = attributes if isinstance(attributes, dict) else {}

    def _department_ref_value() -> str:
        return str(attrs.get("department_ref") or attrs.get("current_department_ref") or "").strip().upper()

    def _role_ref_value() -> str:
        return str(attrs.get("role_ref") or attrs.get("current_role_ref") or "").strip().upper()

    def _append_ref_errors(errors: list[str]) -> list[str]:
        if connection is None:
            return errors
        department_ref = _department_ref_value()
        role_ref = _role_ref_value()

        if department_ref and not department_exists(connection, department_ref):
            errors.append("unknown_department_ref")
        if role_ref and not role_exists(connection, role_ref):
            errors.append("unknown_role_ref")
        return errors

    if normalized == "employee_profile":
        has_key = bool(str(attrs.get("employee_no") or "").strip()) or bool(str(attrs.get("ahv_number") or "").strip())
        errors: list[str] = []
        if not has_key:
            errors.append("employee_identifier_missing")
        errors = _append_ref_errors(errors)
        valid = not errors
        return {
            "valid": valid,
            "reason": "ok" if valid else ",".join(errors),
            "required": ["employee_no_or_ahv_number"],
            "errors": errors,
        }

    if normalized == "employment_contract":
        has_employee_ref = bool(str(attrs.get("person_ref") or attrs.get("employee_ref") or "").strip())
        has_employer_ref = bool(str(attrs.get("employer_ref") or "").strip())
        has_start = bool(str(attrs.get("start_date") or attrs.get("effective_date") or "").strip())
        errors: list[str] = []
        if not has_employee_ref:
            errors.append("employee_ref_missing")
        if not has_employer_ref:
            errors.append("employer_ref_missing")
        if not has_start:
            errors.append("start_date_missing")
        errors = _append_ref_errors(errors)
        valid = not errors
        return {
            "valid": valid,
            "reason": "ok" if valid else ",".join(errors),
            "required": ["employee_ref", "employer_ref", "start_date"],
            "errors": errors,
        }

    return {"valid": True, "reason": "not_applicable", "required": [], "errors": []}


def upsert_hr_employee_profile(connection: Any, payload: dict[str, Any], object_row: dict[str, Any]) -> dict[str, Any]:
    attrs = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    object_id = int(object_row.get("object_id"))

    sql = """
    INSERT INTO hr_employee (
        object_id,
        employee_no,
        ahv_number,
        permit_type,
        source_tax_code,
        employment_status,
        current_employer_ref,
        current_department_ref,
        current_role_ref,
        metadata,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
    ON CONFLICT (object_id)
    DO UPDATE SET
        employee_no = EXCLUDED.employee_no,
        ahv_number = EXCLUDED.ahv_number,
        permit_type = EXCLUDED.permit_type,
        source_tax_code = EXCLUDED.source_tax_code,
        employment_status = EXCLUDED.employment_status,
        current_employer_ref = EXCLUDED.current_employer_ref,
        current_department_ref = EXCLUDED.current_department_ref,
        current_role_ref = EXCLUDED.current_role_ref,
        metadata = COALESCE(hr_employee.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    RETURNING hr_employee_id
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                object_id,
                attrs.get("employee_no"),
                attrs.get("ahv_number"),
                attrs.get("permit_type"),
                attrs.get("source_tax_code"),
                attrs.get("employment_status"),
                attrs.get("current_employer_ref"),
                attrs.get("current_department_ref"),
                attrs.get("current_role_ref"),
                json.dumps({"entity_id": payload.get("entity_id"), "attributes": attrs}, ensure_ascii=False, default=str),
            ),
        )
        hr_employee_id = int(cursor.fetchone()[0])

    connection.commit()
    return {"written": True, "domain_table": "hr_employee", "domain_id": hr_employee_id}


def upsert_hr_employment_contract(connection: Any, payload: dict[str, Any], object_row: dict[str, Any]) -> dict[str, Any]:
    attrs = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    object_id = int(object_row.get("object_id"))

    sql = """
    INSERT INTO hr_employment (
        object_id,
        employee_ref,
        employer_ref,
        department_ref,
        role_ref,
        manager_ref,
        employment_type,
        work_percentage,
        salary_amount,
        salary_currency,
        source_tax_code,
        contract_status,
        start_date,
        end_date,
        metadata,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
    ON CONFLICT (object_id)
    DO UPDATE SET
        employee_ref = EXCLUDED.employee_ref,
        employer_ref = EXCLUDED.employer_ref,
        department_ref = EXCLUDED.department_ref,
        role_ref = EXCLUDED.role_ref,
        manager_ref = EXCLUDED.manager_ref,
        employment_type = EXCLUDED.employment_type,
        work_percentage = EXCLUDED.work_percentage,
        salary_amount = EXCLUDED.salary_amount,
        salary_currency = EXCLUDED.salary_currency,
        source_tax_code = EXCLUDED.source_tax_code,
        contract_status = EXCLUDED.contract_status,
        start_date = EXCLUDED.start_date,
        end_date = EXCLUDED.end_date,
        metadata = COALESCE(hr_employment.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    RETURNING hr_employment_id
    """

    work_percentage = _to_decimal(attrs.get("work_percentage")) if attrs.get("work_percentage") is not None else None
    salary_amount = _to_money(attrs.get("salary_amount")) if attrs.get("salary_amount") is not None else None

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                object_id,
                attrs.get("person_ref") or attrs.get("employee_ref"),
                attrs.get("employer_ref"),
                attrs.get("department_ref"),
                attrs.get("role_ref"),
                attrs.get("manager_ref"),
                attrs.get("employment_type"),
                work_percentage,
                salary_amount,
                attrs.get("salary_currency"),
                attrs.get("source_tax_code"),
                attrs.get("status"),
                attrs.get("start_date") or attrs.get("effective_date"),
                attrs.get("end_date"),
                json.dumps({"entity_id": payload.get("entity_id"), "attributes": attrs}, ensure_ascii=False, default=str),
            ),
        )
        hr_employment_id = int(cursor.fetchone()[0])

    connection.commit()
    return {"written": True, "domain_table": "hr_employment", "domain_id": hr_employment_id}


def upsert_hr_domain_record(connection: Any, payload: dict[str, Any], object_row: dict[str, Any]) -> dict[str, Any]:
    class_name = str(payload.get("class_name") or "").strip().lower()
    if class_name == "employee_profile":
        return upsert_hr_employee_profile(connection, payload, object_row)
    if class_name == "employment_contract":
        return upsert_hr_employment_contract(connection, payload, object_row)
    return {"written": False, "domain_table": None, "domain_id": None}


def delete_hr_records_by_object_id(connection: Any, object_id: int) -> dict[str, int]:
    counts = {"hr_employee_deleted": 0, "hr_employment_deleted": 0}
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM hr_employee WHERE object_id = %s", (int(object_id),))
        counts["hr_employee_deleted"] = int(cursor.rowcount)
        cursor.execute("DELETE FROM hr_employment WHERE object_id = %s", (int(object_id),))
        counts["hr_employment_deleted"] = int(cursor.rowcount)
    connection.commit()
    return counts


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _to_money(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    return _to_decimal(value, default).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def _normalize_direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"dr", "debit", "d", "soll", "debet"}:
        return "debit"
    if text in {"cr", "credit", "c", "haben", "kredit"}:
        return "credit"
    return ""


def _pick_value(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) is not None:
            return mapping.get(key)
    return None


def _map_swiss_vat(vat_code: Any) -> tuple[str, int | None]:
    code = str(vat_code or "NONE").strip().upper() or "NONE"
    route = SWISS_TAX_LEDGER_ROUTER.get(code, SWISS_TAX_LEDGER_ROUTER["NONE"])
    return code, route.get("box_declaration")


def normalize_ledger_lines(attributes: dict[str, Any]) -> list[dict[str, Any]]:
    lines_raw = _pick_value(attributes, "ledger_lines", "lines", "buchungszeilen", "journal_lines")
    if not isinstance(lines_raw, list):
        lines_raw = []

    normalized: list[dict[str, Any]] = []
    for index, line in enumerate(lines_raw, start=1):
        if not isinstance(line, dict):
            continue

        direction = _normalize_direction(_pick_value(line, "direction", "seite", "richtung"))
        if not direction:
            continue

        try:
            account_number = int(_pick_value(line, "account_number", "account", "konto_nummer", "konto", "sachkonto") or 0)
        except (TypeError, ValueError):
            account_number = 0
        if account_number <= 0:
            continue

        source_currency = str(_pick_value(line, "source_currency", "currency", "waehrung") or attributes.get("currency") or "CHF").strip().upper()
        amount_source = _to_money(_pick_value(line, "amount_source_currency", "amount", "betrag") or 0)
        exchange_rate = _to_decimal(_pick_value(line, "exchange_rate_to_chf", "exchange_rate", "wechselkurs") or 1)
        amount_chf = _to_money(_pick_value(line, "amount_chf", "betrag_chf") or (amount_source * exchange_rate))
        vat_code, vat_box = _map_swiss_vat(_pick_value(line, "swiss_vat_code", "vat_code", "mwst_code", "ust_code"))

        normalized.append(
            {
                "line_no": int(_pick_value(line, "line_no", "zeile", "positionsnummer") or index),
                "account_number": account_number,
                "direction": direction,
                "source_currency": source_currency,
                "amount_source_currency": amount_source,
                "exchange_rate_to_chf": exchange_rate,
                "amount_chf": amount_chf,
                "swiss_vat_code": vat_code,
                "swiss_vat_box": vat_box,
                "metadata": {
                    "line_description": _pick_value(line, "line_description", "description", "beschreibung"),
                    "tax_rate": _pick_value(line, "tax_rate", "mwst_satz", "ust_satz"),
                    "raw": line,
                },
            }
        )

    return normalized


def validate_double_entry(
    lines: list[dict[str, Any]],
    connection: Any | None = None,
    legal_entity_ref: str | None = None,
) -> dict[str, Any]:
    if not lines:
        return {
            "valid": False,
            "reason": "no_ledger_lines",
            "debit_total": "0.00",
            "credit_total": "0.00",
            "missing_account_numbers": [],
        }

    account_numbers = [int(line.get("account_number") or 0) for line in lines]
    missing_account_numbers: list[int] = []
    if connection is not None:
        existing_accounts = get_existing_account_numbers(connection, account_numbers, legal_entity_ref=legal_entity_ref)
        missing_account_numbers = sorted({number for number in account_numbers if number > 0 and number not in existing_accounts})
        if missing_account_numbers:
            return {
                "valid": False,
                "reason": "unknown_account_numbers",
                "debit_total": "0.00",
                "credit_total": "0.00",
                "missing_account_numbers": missing_account_numbers,
            }

    debit_total = sum((line["amount_chf"] for line in lines if line["direction"] == "debit"), Decimal("0"))
    credit_total = sum((line["amount_chf"] for line in lines if line["direction"] == "credit"), Decimal("0"))
    balanced = debit_total.quantize(MONEY_QUANT) == credit_total.quantize(MONEY_QUANT)

    return {
        "valid": balanced,
        "reason": "balanced" if balanced else "debit_credit_mismatch",
        "debit_total": str(debit_total.quantize(MONEY_QUANT)),
        "credit_total": str(credit_total.quantize(MONEY_QUANT)),
        "missing_account_numbers": missing_account_numbers,
    }


def _build_transaction_id(attributes: dict[str, Any], payload: dict[str, Any]) -> str:
    doc_id = payload.get("doc_id")
    doc_id_int: int | None = None
    if doc_id is not None:
        try:
            parsed_doc_id = int(doc_id)
            if parsed_doc_id > 0:
                doc_id_int = parsed_doc_id
        except (TypeError, ValueError):
            doc_id_int = None

    existing = str(attributes.get("transaction_id") or "").strip()
    if existing:
        safe_existing = re.sub(r"[^A-Za-z0-9_.:-]+", "_", existing).strip("_")
        if doc_id_int is not None:
            return f"doc{doc_id_int}-{safe_existing}"[:64]
        return safe_existing[:64]

    entity_id = str(payload.get("entity_id") or "tx").strip() or "tx"
    if doc_id_int is not None:
        return f"doc{doc_id_int}-{entity_id}"[:64]
    return f"tx-{uuid.uuid4().hex[:20]}"


def _prepare_accounting_transaction_context(
    connection: Any,
    payload: dict[str, Any],
    object_row: dict[str, Any],
) -> dict[str, Any]:
    attributes = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    doc_id = payload.get("doc_id")
    ledger_lines = normalize_ledger_lines(attributes)
    legal_entity_ref = _resolve_legal_entity_ref(connection, payload, attributes)
    description_hint = _build_account_alignment_hint(connection, payload, attributes)
    ledger_lines = align_ledger_lines_to_chart_of_accounts(
        connection=connection,
        lines=ledger_lines,
        legal_entity_ref=legal_entity_ref,
        description_hint=description_hint,
    )
    validation = validate_double_entry(
        ledger_lines,
        connection=connection,
        legal_entity_ref=legal_entity_ref,
    )
    transaction_id = _build_transaction_id(attributes, payload)
    transaction_date = (
        attributes.get("transaction_date")
        or payload.get("effective_from")
        or payload.get("recorded_on")
        or date.today().isoformat()
    )
    source_type = str(attributes.get("source_type") or attributes.get("document_type") or "document_ingest")
    description = str(attributes.get("description") or payload.get("object_name") or "Accounting transaction")
    receipt_archive_url = attributes.get("receipt_archive_url")
    object_id = object_row.get("object_id")

    return {
        "doc_id": doc_id,
        "attributes": attributes,
        "ledger_lines": ledger_lines,
        "validation": validation,
        "legal_entity_ref": legal_entity_ref,
        "transaction_id": transaction_id,
        "transaction_date": transaction_date,
        "source_type": source_type,
        "description": description,
        "receipt_archive_url": receipt_archive_url,
        "object_id": object_id,
    }


def upsert_transaction_and_lines(
    connection: Any,
    payload: dict[str, Any],
    object_row: dict[str, Any],
) -> dict[str, Any]:
    context = _prepare_accounting_transaction_context(
        connection=connection,
        payload=payload,
        object_row=object_row,
    )
    attributes = context["attributes"]
    doc_id = context["doc_id"]
    ledger_lines = context["ledger_lines"]
    legal_entity_ref = context["legal_entity_ref"]
    validation = context["validation"]
    if not validation["valid"]:
        return {
            "ok": False,
            "validation": validation,
            "transaction_id": None,
            "ledger_lines_written": 0,
        }

    transaction_id = context["transaction_id"]
    transaction_date = context["transaction_date"]
    source_type = context["source_type"]
    description = context["description"]
    receipt_archive_url = context["receipt_archive_url"]
    object_id = context["object_id"]

    tx_sql = """
    INSERT INTO transactions (
        transaction_id,
        legal_entity_ref,
        object_id,
        source_document_id,
        source_type,
        transaction_date,
        description,
        receipt_archive_url,
        metadata
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (transaction_id)
    DO UPDATE SET
        legal_entity_ref = EXCLUDED.legal_entity_ref,
        object_id = EXCLUDED.object_id,
        source_document_id = EXCLUDED.source_document_id,
        source_type = EXCLUDED.source_type,
        transaction_date = EXCLUDED.transaction_date,
        description = EXCLUDED.description,
        receipt_archive_url = EXCLUDED.receipt_archive_url,
        metadata = COALESCE(transactions.metadata, '{}'::jsonb) || EXCLUDED.metadata
    """

    delete_lines_sql = "DELETE FROM ledger_lines WHERE transaction_id = %s"
    line_sql = """
    INSERT INTO ledger_lines (
        transaction_id,
        legal_entity_ref,
        account_number,
        direction,
        source_currency,
        amount_source_currency,
        exchange_rate_to_chf,
        amount_chf,
        swiss_vat_code,
        swiss_vat_box,
        metadata
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """

    obsolete_tx_lookup_sql = """
    SELECT transaction_id
    FROM transactions
    WHERE object_id = %s
        AND source_document_id = %s
        AND transaction_id <> %s
    """
    delete_obsolete_lines_sql = "DELETE FROM ledger_lines WHERE transaction_id = %s"
    delete_obsolete_tx_sql = "DELETE FROM transactions WHERE transaction_id = %s"

    with connection.cursor() as cursor:
        cursor.execute(
            tx_sql,
            (
                transaction_id,
                legal_entity_ref,
                object_id,
                doc_id,
                source_type,
                transaction_date,
                description,
                receipt_archive_url,
                json.dumps(
                    {
                        "entity_id": payload.get("entity_id"),
                        "legal_entity_ref": legal_entity_ref,
                        "class_name": payload.get("class_name"),
                        "validation": validation,
                        "attributes": attributes,
                    },
                    ensure_ascii=False,
                    default=str,
                ),
            ),
        )

        cursor.execute(delete_lines_sql, (transaction_id,))
        for line in ledger_lines:
            cursor.execute(
                line_sql,
                (
                    transaction_id,
                    legal_entity_ref,
                    line["account_number"],
                    line["direction"],
                    line["source_currency"],
                    line["amount_source_currency"],
                    line["exchange_rate_to_chf"],
                    line["amount_chf"],
                    line["swiss_vat_code"],
                    line["swiss_vat_box"],
                    json.dumps(line.get("metadata") or {}, ensure_ascii=False, default=str),
                ),
            )

        # Keep one canonical accounting transaction per object/document pair and
        # remove stale historical IDs from earlier derivation strategies.
        if object_id is not None and doc_id is not None:
            cursor.execute(obsolete_tx_lookup_sql, (object_id, doc_id, transaction_id))
            for tx_row in cursor.fetchall() or []:
                stale_tx_id = str((tx_row or [""])[0] or "").strip()
                if not stale_tx_id:
                    continue
                cursor.execute(delete_obsolete_lines_sql, (stale_tx_id,))
                cursor.execute(delete_obsolete_tx_sql, (stale_tx_id,))

    connection.commit()
    return {
        "ok": True,
        "validation": validation,
        "transaction_id": transaction_id,
        "ledger_lines_written": len(ledger_lines),
    }


def enqueue_accounting_booking_review(
    connection: Any,
    payload: dict[str, Any],
    object_row: dict[str, Any],
) -> dict[str, Any]:
    _ensure_accounting_booking_review_table(connection)
    context = _prepare_accounting_transaction_context(
        connection=connection,
        payload=payload,
        object_row=object_row,
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO accounting_booking_review (
                object_id,
                object_name,
                class_name,
                source_type,
                source_document_id,
                legal_entity_ref,
                transaction_id,
                transaction_date,
                description,
                payload_json,
                preview_lines_json,
                validation_json,
                status,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, 'pending', NOW())
            RETURNING review_id
            """,
            (
                context.get("object_id"),
                object_row.get("object_name"),
                object_row.get("class_name"),
                context.get("source_type"),
                context.get("doc_id"),
                context.get("legal_entity_ref"),
                context.get("transaction_id"),
                context.get("transaction_date"),
                context.get("description"),
                json.dumps(payload, ensure_ascii=False, default=str),
                json.dumps(context.get("ledger_lines") or [], ensure_ascii=False, default=str),
                json.dumps(context.get("validation") or {}, ensure_ascii=False, default=str),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    review_id = int(row[0]) if row else 0
    return {
        "review_id": review_id,
        "status": "pending",
        "validation": context.get("validation") or {},
        "transaction_id": context.get("transaction_id"),
        "transaction_date": context.get("transaction_date"),
        "source_type": context.get("source_type"),
        "legal_entity_ref": context.get("legal_entity_ref"),
        "description": context.get("description"),
        "preview_lines": context.get("ledger_lines") or [],
    }


def list_accounting_booking_reviews(
    connection: Any,
    status: str | None = "pending",
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    _ensure_accounting_booking_review_table(connection)
    limit = max(1, min(int(limit or 100), 1000))
    offset = max(0, int(offset or 0))
    status_norm = str(status or "").strip().lower()
    params: list[Any] = []
    where_sql = ""
    if status_norm:
        where_sql = "WHERE status = %s"
        params.append(status_norm)
    sql = f"""
    SELECT
        review_id,
        object_id,
        object_name,
        class_name,
        source_type,
        source_document_id,
        legal_entity_ref,
        transaction_id,
        transaction_date,
        description,
        status,
        validation_json,
        reviewed_by,
        reviewed_at,
        created_at,
        updated_at
    FROM accounting_booking_review
    {where_sql}
    ORDER BY review_id DESC
    LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "review_id": int(row[0]),
                "object_id": int(row[1]) if row[1] is not None else None,
                "object_name": row[2],
                "class_name": row[3],
                "source_type": row[4],
                "source_document_id": int(row[5]) if row[5] is not None else None,
                "legal_entity_ref": row[6],
                "transaction_id": row[7],
                "transaction_date": str(row[8]) if row[8] is not None else None,
                "description": row[9],
                "status": row[10],
                "validation": row[11] if isinstance(row[11], dict) else {},
                "reviewed_by": row[12],
                "reviewed_at": str(row[13]) if row[13] is not None else None,
                "created_at": str(row[14]) if row[14] is not None else None,
                "updated_at": str(row[15]) if row[15] is not None else None,
            }
        )
    return out


def get_accounting_booking_review(connection: Any, review_id: int) -> dict[str, Any] | None:
    _ensure_accounting_booking_review_table(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                review_id,
                object_id,
                object_name,
                class_name,
                source_type,
                source_document_id,
                legal_entity_ref,
                transaction_id,
                transaction_date,
                description,
                payload_json,
                preview_lines_json,
                validation_json,
                status,
                posted_transaction_id,
                review_note,
                reviewed_by,
                reviewed_at,
                created_at,
                updated_at
            FROM accounting_booking_review
            WHERE review_id = %s
            LIMIT 1
            """,
            (int(review_id),),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "review_id": int(row[0]),
        "object_id": int(row[1]) if row[1] is not None else None,
        "object_name": row[2],
        "class_name": row[3],
        "source_type": row[4],
        "source_document_id": int(row[5]) if row[5] is not None else None,
        "legal_entity_ref": row[6],
        "transaction_id": row[7],
        "transaction_date": str(row[8]) if row[8] is not None else None,
        "description": row[9],
        "payload": row[10] if isinstance(row[10], dict) else {},
        "preview_lines": row[11] if isinstance(row[11], list) else [],
        "validation": row[12] if isinstance(row[12], dict) else {},
        "status": row[13],
        "posted_transaction_id": row[14],
        "review_note": row[15],
        "reviewed_by": row[16],
        "reviewed_at": str(row[17]) if row[17] is not None else None,
        "created_at": str(row[18]) if row[18] is not None else None,
        "updated_at": str(row[19]) if row[19] is not None else None,
    }


def approve_accounting_booking_review(
    connection: Any,
    review_id: int,
    booking_updates: dict[str, Any] | None = None,
    reviewed_by: str | None = None,
    review_note: str | None = None,
) -> dict[str, Any]:
    review = get_accounting_booking_review(connection, review_id)
    if review is None:
        return {"ok": False, "reason": "not_found"}
    if str(review.get("status") or "").lower() != "pending":
        return {"ok": False, "reason": "not_pending", "status": review.get("status")}

    payload = review.get("payload") if isinstance(review.get("payload"), dict) else {}
    payload = dict(payload)
    attributes = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    attributes = dict(attributes)
    updates = booking_updates if isinstance(booking_updates, dict) else {}

    for field in ("transaction_id", "transaction_date", "description", "source_type", "legal_entity_ref", "receipt_archive_url"):
        if field in updates:
            attributes[field] = updates.get(field)
    if "ledger_lines" in updates and isinstance(updates.get("ledger_lines"), list):
        attributes["ledger_lines"] = updates.get("ledger_lines")

    payload["attributes"] = attributes

    object_row = {
        "object_id": review.get("object_id"),
        "object_name": review.get("object_name"),
        "class_name": review.get("class_name") or "accounting_transaction",
    }
    post_result = upsert_transaction_and_lines(connection=connection, payload=payload, object_row=object_row)
    if not bool(post_result.get("ok")):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE accounting_booking_review
                SET payload_json = %s::jsonb,
                    preview_lines_json = %s::jsonb,
                    validation_json = %s::jsonb,
                    reviewed_by = %s,
                    review_note = %s,
                    reviewed_at = NOW(),
                    updated_at = NOW()
                WHERE review_id = %s
                """,
                (
                    json.dumps(payload, ensure_ascii=False, default=str),
                    json.dumps(attributes.get("ledger_lines") or [], ensure_ascii=False, default=str),
                    json.dumps(post_result.get("validation") or {}, ensure_ascii=False, default=str),
                    str(reviewed_by or "api:user").strip() or "api:user",
                    str(review_note or "").strip() or None,
                    int(review_id),
                ),
            )
        connection.commit()
        return {
            "ok": False,
            "reason": "validation_failed",
            "validation": post_result.get("validation") or {},
            "status": "pending",
        }

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE accounting_booking_review
            SET status = 'approved',
                posted_transaction_id = %s,
                payload_json = %s::jsonb,
                preview_lines_json = %s::jsonb,
                validation_json = %s::jsonb,
                reviewed_by = %s,
                review_note = %s,
                reviewed_at = NOW(),
                updated_at = NOW()
            WHERE review_id = %s
            """,
            (
                post_result.get("transaction_id"),
                json.dumps(payload, ensure_ascii=False, default=str),
                json.dumps(attributes.get("ledger_lines") or [], ensure_ascii=False, default=str),
                json.dumps(post_result.get("validation") or {}, ensure_ascii=False, default=str),
                str(reviewed_by or "api:user").strip() or "api:user",
                str(review_note or "").strip() or None,
                int(review_id),
            ),
        )
    connection.commit()
    return {
        "ok": True,
        "status": "approved",
        "transaction_id": post_result.get("transaction_id"),
        "ledger_lines_written": int(post_result.get("ledger_lines_written") or 0),
        "validation": post_result.get("validation") or {},
    }


def cancel_accounting_booking_review(
    connection: Any,
    review_id: int,
    reviewed_by: str | None = None,
    review_note: str | None = None,
) -> dict[str, Any]:
    review = get_accounting_booking_review(connection, review_id)
    if review is None:
        return {"ok": False, "reason": "not_found"}
    if str(review.get("status") or "").lower() != "pending":
        return {"ok": False, "reason": "not_pending", "status": review.get("status")}

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE accounting_booking_review
            SET status = 'cancelled',
                reviewed_by = %s,
                review_note = %s,
                reviewed_at = NOW(),
                updated_at = NOW()
            WHERE review_id = %s
            """,
            (
                str(reviewed_by or "api:user").strip() or "api:user",
                str(review_note or "").strip() or None,
                int(review_id),
            ),
        )
    connection.commit()
    return {"ok": True, "status": "cancelled"}


def _ensure_accounting_booking_review_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(create_accounting_booking_review_table)
        cursor.execute("ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS posted_transaction_id VARCHAR(64);")
        cursor.execute("ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS review_note TEXT;")
        cursor.execute("ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS reviewed_by VARCHAR(128);")
        cursor.execute("ALTER TABLE IF EXISTS accounting_booking_review ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_status ON accounting_booking_review(status);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounting_booking_review_object_id ON accounting_booking_review(object_id);")
    connection.commit()


def delete_transaction_by_object_id(connection: Any, object_id: int) -> int:
    sql = "DELETE FROM transactions WHERE object_id = %s"
    with connection.cursor() as cursor:
        cursor.execute(sql, (int(object_id),))
        deleted = cursor.rowcount
    connection.commit()
    return int(deleted)


def list_journal_lines(
    connection: Any,
    start_date: str | None = None,
    end_date: str | None = None,
    legal_entity_ref: str | None = None,
    account_number: int | None = None,
    direction: str | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 1000), 5000))
    offset = max(0, int(offset or 0))

    where_clauses = ["1=1"]
    params: list[Any] = []

    if start_date:
        where_clauses.append("t.transaction_date >= %s")
        params.append(str(start_date))
    if end_date:
        where_clauses.append("t.transaction_date <= %s")
        params.append(str(end_date))
    if legal_entity_ref:
        where_clauses.append("COALESCE(t.legal_entity_ref, l.legal_entity_ref, 'global') = %s")
        params.append(str(legal_entity_ref).strip())
    if account_number is not None:
        where_clauses.append("l.account_number = %s")
        params.append(int(account_number))
    if direction:
        where_clauses.append("LOWER(l.direction) = %s")
        params.append(str(direction).strip().lower())

    sql = f"""
    SELECT
        t.transaction_id,
        t.journal_sequence_number,
        t.transaction_date,
        t.legal_entity_ref,
        t.source_type,
        t.source_document_id,
        t.object_id,
        t.description,
        t.receipt_archive_url,
        t.created_at,
        l.line_id,
        l.account_number,
        l.direction,
        l.source_currency,
        l.amount_source_currency,
        l.exchange_rate_to_chf,
        l.amount_chf,
        l.swiss_vat_code,
        l.swiss_vat_box,
        l.metadata,
        coa.account_name,
        coa.account_type
    FROM transactions t
    JOIN ledger_lines l ON l.transaction_id = t.transaction_id
    LEFT JOIN chart_of_accounts coa
      ON coa.account_number = l.account_number
     AND coa.legal_entity_ref IN (COALESCE(t.legal_entity_ref, 'global'), 'global')
    WHERE {' AND '.join(where_clauses)}
    ORDER BY t.transaction_date ASC, t.journal_sequence_number ASC, l.line_id ASC
    LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "transaction_id": row[0],
                "journal_sequence_number": int(row[1]) if row[1] is not None else None,
                "transaction_date": row[2].isoformat() if row[2] is not None else None,
                "legal_entity_ref": row[3],
                "source_type": row[4],
                "source_document_id": int(row[5]) if row[5] is not None else None,
                "object_id": int(row[6]) if row[6] is not None else None,
                "description": row[7],
                "receipt_archive_url": row[8],
                "created_at": row[9].isoformat() if row[9] is not None else None,
                "line_id": int(row[10]) if row[10] is not None else None,
                "account_number": int(row[11]) if row[11] is not None else None,
                "direction": row[12],
                "source_currency": row[13],
                "amount_source_currency": str(row[14]) if row[14] is not None else "0.00",
                "exchange_rate_to_chf": str(row[15]) if row[15] is not None else "1.0",
                "amount_chf": str(row[16]) if row[16] is not None else "0.00",
                "swiss_vat_code": row[17],
                "swiss_vat_box": int(row[18]) if row[18] is not None else None,
                "line_metadata": row[19] or {},
                "account_name": row[20],
                "account_type": row[21],
            }
        )
    return out


def summarize_vat_return(
    connection: Any,
    start_date: str,
    end_date: str,
    legal_entity_ref: str | None = None,
) -> dict[str, Any]:
    where_clauses = ["t.transaction_date >= %s", "t.transaction_date <= %s"]
    params: list[Any] = [str(start_date), str(end_date)]

    if legal_entity_ref:
        where_clauses.append("COALESCE(t.legal_entity_ref, l.legal_entity_ref, 'global') = %s")
        params.append(str(legal_entity_ref).strip())

    sql = f"""
    SELECT
        COALESCE(l.swiss_vat_box, 0) AS vat_box,
        COALESCE(NULLIF(TRIM(l.swiss_vat_code), ''), 'NONE') AS vat_code,
        LOWER(l.direction) AS direction,
        SUM(l.amount_chf) AS amount_chf
    FROM ledger_lines l
    JOIN transactions t ON t.transaction_id = l.transaction_id
    WHERE {' AND '.join(where_clauses)}
    GROUP BY COALESCE(l.swiss_vat_box, 0), COALESCE(NULLIF(TRIM(l.swiss_vat_code), ''), 'NONE'), LOWER(l.direction)
    ORDER BY vat_box ASC, vat_code ASC, direction ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

    by_box: dict[str, dict[str, Any]] = {}
    debit_total = Decimal("0")
    credit_total = Decimal("0")

    for vat_box, vat_code, side, amount in rows:
        amount_dec = _to_money(amount)
        if side == "debit":
            debit_total += amount_dec
        elif side == "credit":
            credit_total += amount_dec

        box_key = str(int(vat_box or 0))
        record = by_box.get(box_key)
        if record is None:
            record = {
                "vat_box": int(vat_box or 0),
                "vat_codes": set(),
                "debit_chf": Decimal("0"),
                "credit_chf": Decimal("0"),
            }
            by_box[box_key] = record

        record["vat_codes"].add(str(vat_code or "NONE"))
        if side == "debit":
            record["debit_chf"] += amount_dec
        elif side == "credit":
            record["credit_chf"] += amount_dec

    boxes: list[dict[str, Any]] = []
    for key in sorted(by_box.keys(), key=lambda item: int(item)):
        item = by_box[key]
        debit_chf = item["debit_chf"].quantize(MONEY_QUANT)
        credit_chf = item["credit_chf"].quantize(MONEY_QUANT)
        boxes.append(
            {
                "vat_box": int(item["vat_box"]),
                "vat_codes": sorted(item["vat_codes"]),
                "debit_chf": str(debit_chf),
                "credit_chf": str(credit_chf),
                "net_chf": str((credit_chf - debit_chf).quantize(MONEY_QUANT)),
            }
        )

    return {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "legal_entity_ref": str(legal_entity_ref or ""),
        "boxes": boxes,
        "totals": {
            "debit_chf": str(debit_total.quantize(MONEY_QUANT)),
            "credit_chf": str(credit_total.quantize(MONEY_QUANT)),
            "net_chf": str((credit_total - debit_total).quantize(MONEY_QUANT)),
        },
    }


def log_domain_definition_audit(
    connection: Any,
    definition_type: str,
    operation: str,
    key_value: str,
    before_state: dict[str, Any] | None = None,
    after_state: dict[str, Any] | None = None,
    changed_by: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """
    Log a domain definition change to audit log for compliance and debugging.
    
    Args:
        connection: Database connection
        definition_type: Normalized type (account_definition, hr_department_definition, hr_role_definition)
        operation: Operation performed (create, update, delete, read)
        key_value: Key identifying the record (account number, department code, role code)
        before_state: State before change (JSONB), None for create
        after_state: State after change (JSONB), None for delete
        changed_by: User/system that made the change (default: system)
        metadata: Additional metadata (default: {})
    
    Returns:
        True if successfully logged, False otherwise
    """
    try:
        if not connection:
            LOGGER.warning("No database connection for audit logging")
            return False
        
        normalized_type = str(definition_type or "").strip().lower()
        normalized_op = str(operation or "").strip().lower()
        normalized_key = str(key_value or "").strip()
        normalized_user = str(changed_by or "system").strip() or "system"
        normalized_metadata = metadata if isinstance(metadata, dict) else {}
        
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO domain_definition_audit_log
            (definition_type, operation, key_value, changed_by, before_state, after_state, metadata, change_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """
            params = (
                normalized_type,
                normalized_op,
                normalized_key,
                normalized_user,
                json.dumps(before_state) if before_state else None,
                json.dumps(after_state) if after_state else None,
                json.dumps(normalized_metadata),
            )
            cursor.execute(sql, params)
            connection.commit()
            return True
    except Exception as e:
        LOGGER.error(f"Error logging domain definition audit: {e}")
        try:
            connection.rollback()
        except Exception:
            pass
        return False


def get_domain_definition_audit_log(
    connection: Any,
    definition_type: str | None = None,
    key_value: str | None = None,
    operation: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    Retrieve audit log entries for domain definitions.
    
    Args:
        connection: Database connection
        definition_type: Filter by definition type (optional)
        key_value: Filter by record key (optional)
        operation: Filter by operation type (optional)
        limit: Maximum number of results (default: 100)
        offset: Offset for pagination (default: 0)
    
    Returns:
        List of audit log entries
    """
    try:
        if not connection:
            return []
        
        sql = "SELECT audit_id, definition_type, operation, key_value, changed_by, change_timestamp, before_state, after_state, metadata FROM domain_definition_audit_log WHERE 1=1"
        params = []
        
        if definition_type:
            sql += " AND definition_type = %s"
            params.append(str(definition_type or "").strip().lower())
        
        if key_value:
            sql += " AND key_value = %s"
            params.append(str(key_value or "").strip())
        
        if operation:
            sql += " AND operation = %s"
            params.append(str(operation or "").strip().lower())
        
        sql += " ORDER BY change_timestamp DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall() or []
            return [
                {
                    "audit_id": row[0],
                    "definition_type": row[1],
                    "operation": row[2],
                    "key_value": row[3],
                    "changed_by": row[4],
                    "change_timestamp": str(row[5]) if row[5] else None,
                    "before_state": json.loads(row[6]) if row[6] else None,
                    "after_state": json.loads(row[7]) if row[7] else None,
                    "metadata": json.loads(row[8]) if row[8] else {},
                }
                for row in rows
            ]
    except Exception as e:
        LOGGER.error(f"Error retrieving domain definition audit log: {e}")
        return []
