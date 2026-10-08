# IKOS-Kernel configuration prefix migration

Scope: IKOS-Kernel only. **61 distinct uppercase configuration tokens** were
migrated: 60 complete names plus one dynamic source-password prefix. Names used
only in configuration diagnostics are included. The inventory was obtained with
the **case-sensitive** expression `\bIDMS_[A-Z][A-Z0-9_]*`; lowercase module names
are not configuration identifiers.

## Exact old → new mapping

| Old | New |
| --- | --- |
| IDMS_COA_PROFILE | IKOS_COA_PROFILE |
| IDMS_DB_HOST | IKOS_DB_HOST |
| IDMS_DB_NAME | IKOS_DB_NAME |
| IDMS_DB_PASSWORD | IKOS_DB_PASSWORD |
| IDMS_DB_PORT | IKOS_DB_PORT |
| IDMS_DB_USER | IKOS_DB_USER |
| IDMS_DISCOVERY_DATA_STORE_ID | IKOS_DISCOVERY_DATA_STORE_ID |
| IDMS_DISCOVERY_LOCATION | IKOS_DISCOVERY_LOCATION |
| IDMS_DYNAMIC_ATTRIBUTE_LLM_SYNONYM_LIMIT | IKOS_DYNAMIC_ATTRIBUTE_LLM_SYNONYM_LIMIT |
| IDMS_EAGER_SOLF_POLICY_INIT | IKOS_EAGER_SOLF_POLICY_INIT |
| IDMS_EMAIL_FROM | IKOS_EMAIL_FROM |
| IDMS_EMAIL_PASSWORD | IKOS_EMAIL_PASSWORD |
| IDMS_EMAIL_USER | IKOS_EMAIL_USER |
| IDMS_ENABLE_DISCOVERY_INDEX | IKOS_ENABLE_DISCOVERY_INDEX |
| IDMS_ENABLE_DYNAMIC_ATTRIBUTE_LLM_SYNONYMS | IKOS_ENABLE_DYNAMIC_ATTRIBUTE_LLM_SYNONYMS |
| IDMS_ENABLE_QDRANT_INDEX | IKOS_ENABLE_QDRANT_INDEX |
| IDMS_ENABLE_RELATION_TABLE_ENRICHMENT | IKOS_ENABLE_RELATION_TABLE_ENRICHMENT |
| IDMS_ENABLE_SEED_ATTRIBUTE_ALIASES | IKOS_ENABLE_SEED_ATTRIBUTE_ALIASES |
| IDMS_ENABLE_TIMING | IKOS_ENABLE_TIMING |
| IDMS_FORCE_PPARSER | IKOS_FORCE_PPARSER |
| IDMS_GITHUB_API_BASE_URL | IKOS_GITHUB_API_BASE_URL |
| IDMS_GITHUB_OWNER | IKOS_GITHUB_OWNER |
| IDMS_GITHUB_REPO | IKOS_GITHUB_REPO |
| IDMS_GITHUB_REPOSITORY | IKOS_GITHUB_REPOSITORY |
| IDMS_GITHUB_TOKEN | IKOS_GITHUB_TOKEN |
| IDMS_HR_PROFILE | IKOS_HR_PROFILE |
| IDMS_INFERENCE_MAX_DIRECTIVES_PER_RULE | IKOS_INFERENCE_MAX_DIRECTIVES_PER_RULE |
| IDMS_INFERENCE_MAX_RULES | IKOS_INFERENCE_MAX_RULES |
| IDMS_INFERENCE_MAX_WORKFLOW_PROCESS_ADDITIONS | IKOS_INFERENCE_MAX_WORKFLOW_PROCESS_ADDITIONS |
| IDMS_INGESTION_MARKDOWN_RULE | IKOS_INGESTION_MARKDOWN_RULE |
| IDMS_INGEST_EMBEDDING_BATCH_SIZE | IKOS_INGEST_EMBEDDING_BATCH_SIZE |
| IDMS_LLAMAPARSE_TIMEOUT_SECONDS | IKOS_LLAMAPARSE_TIMEOUT_SECONDS |
| IDMS_LOG_DIR | IKOS_LOG_DIR |
| IDMS_LOG_LEVEL | IKOS_LOG_LEVEL |
| IDMS_MAINTENANCE_TOKEN | IKOS_MAINTENANCE_TOKEN |
| IDMS_MATCH_ALLOWED_PURPOSES | IKOS_MATCH_ALLOWED_PURPOSES |
| IDMS_MATCH_ALLOW_COLLECTION_OVERRIDE | IKOS_MATCH_ALLOW_COLLECTION_OVERRIDE |
| IDMS_MATCH_QDRANT_COLLECTION | IKOS_MATCH_QDRANT_COLLECTION |
| IDMS_MAX_MARKDOWN_GENERATION_RUNS_PER_INGEST | IKOS_MAX_MARKDOWN_GENERATION_RUNS_PER_INGEST |
| IDMS_NOTIFICATION_CHANNELS | IKOS_NOTIFICATION_CHANNELS |
| IDMS_OPENROUTER_BASE_URL | IKOS_OPENROUTER_BASE_URL |
| IDMS_OPENROUTER_EMBEDDING_MODEL | IKOS_OPENROUTER_EMBEDDING_MODEL |
| IDMS_OPENROUTER_MD_TEXT_MODEL | IKOS_OPENROUTER_MD_TEXT_MODEL |
| IDMS_OPENROUTER_MD_VISION_MODEL | IKOS_OPENROUTER_MD_VISION_MODEL |
| IDMS_PERSIST_MARKDOWN | IKOS_PERSIST_MARKDOWN |
| IDMS_QDRANT_AUTO_RECREATE_ON_DIM_MISMATCH | IKOS_QDRANT_AUTO_RECREATE_ON_DIM_MISMATCH |
| IDMS_QDRANT_EMBEDDING_DIMENSION | IKOS_QDRANT_EMBEDDING_DIMENSION |
| IDMS_RECREATE_ATTRIBUTE_EMBEDDING_COLLECTION_ON_DIM_MISMATCH | IKOS_RECREATE_ATTRIBUTE_EMBEDDING_COLLECTION_ON_DIM_MISMATCH |
| IDMS_REUSE_MARKDOWN | IKOS_REUSE_MARKDOWN |
| IDMS_SEMANTIC_BEST_EFFORT_THRESHOLD | IKOS_SEMANTIC_BEST_EFFORT_THRESHOLD |
| IDMS_SMTP_HOST | IKOS_SMTP_HOST |
| IDMS_SMTP_PORT | IKOS_SMTP_PORT |
| IDMS_SOLF_LOAD_TIMEOUT_SEC | IKOS_SOLF_LOAD_TIMEOUT_SEC |
| IDMS_SOLF_RETRY_COOLDOWN_SEC | IKOS_SOLF_RETRY_COOLDOWN_SEC |
| IDMS_SOURCE_DB_PASSWORD | IKOS_SOURCE_DB_PASSWORD |
| IDMS_SOURCE_DB_PASSWORD_ | IKOS_SOURCE_DB_PASSWORD_ |
| IDMS_SPACY_ALLOW_CROSS_LANGUAGE_FALLBACK | IKOS_SPACY_ALLOW_CROSS_LANGUAGE_FALLBACK |
| IDMS_SPACY_MODEL | IKOS_SPACY_MODEL |
| IDMS_SPACY_STRICT_LANGUAGE | IKOS_SPACY_STRICT_LANGUAGE |
| IDMS_STRUCTURED_TABLE_ROW_LIMIT | IKOS_STRUCTURED_TABLE_ROW_LIMIT |
| IDMS_WEBHOOK_URL | IKOS_WEBHOOK_URL |

The trailing-underscore source-password entry denotes the dynamic family
`IKOS_SOURCE_DB_PASSWORD_<SOURCE_KEY>`, not a standalone variable. Existing
source-key sanitization (uppercase, replace non-alphanumeric characters with
underscores) and password precedence remain unchanged: per-source, shared source,
then main database password. Explicit request payload behavior is unchanged.

Discovery configuration names occur in hints for compatibility paths; this
migration does not enable Discovery or change the central disabled settings.

## External deployment action required

Rename externally managed environment variable **keys** according to the table
in deployment manifests, service/process configuration, CI/CD injection, and
secret-manager bindings. Preserve their values; do not place credentials in this
document or source control. Update private dotenv configuration outside this
change, then restart workers/API processes so import-time settings reload.
There is **no fallback to legacy uppercase keys** in the migrated runtime paths.
An old-only deployment will use defaults or reject missing required settings.

The central configuration already used the new prefix and was not changed.
[sql_db.py](../sql_db.py) now imports `DB_NAME`, `DB_HOST`, `DB_USER`,
`DB_PASSWORD`, and `DB_PORT` from [ikos_config.py](../ikos_config.py). It no longer
loads dotenv separately or maintains duplicate database defaults. In particular,
the old hardcoded password and old database-name fallback were removed. Existing
tenant/user/permission session configuration is unchanged, including the
maintenance fallback when no request security context is active.

This is not a database/collection rename or schema migration. Retain persisted
database and collection names as deployment values if needed.

## Exact files changed by this task (22)

Paths below are relative to IKOS-Kernel. Preexisting SOLF changes in other files
are not part of this list; existing edits in the two overlapping runtime files
were preserved.

- [attribute_embedding_index.py](../attribute_embedding_index.py)
- [business_rules.py](../business_rules.py)
- [domain_db.py](../domain_db.py)
- [idms_api_server/routers/business_rules.py](../idms_api_server/routers/business_rules.py)
- [idms_api_server/routers/ingestion.py](../idms_api_server/routers/ingestion.py)
- [idms_api_server/routers/system.py](../idms_api_server/routers/system.py)
- [ikos_api_server/routers/business_rules.py](../ikos_api_server/routers/business_rules.py)
- [ikos_api_server/routers/ingestion.py](../ikos_api_server/routers/ingestion.py)
- [ikos_api_server/routers/system.py](../ikos_api_server/routers/system.py)
- [ikos_api_server/schemas.py](../ikos_api_server/schemas.py)
- [ingest.py](../ingest.py)
- [interaction.py](../interaction.py)
- [md_gen.py](../md_gen.py)
- [notifier.py](../notifier.py)
- [query_engine.py](../query_engine.py)
- [runtime_logging.py](../runtime_logging.py)
- [spacy_preparser.py](../spacy_preparser.py)
- [sql_db.py](../sql_db.py)
- [tx_match_index.py](../tx_match_index.py)
- [web/ikos_test_app.html](../web/ikos_test_app.html)
- [tests/test_configuration_prefix_migration.py](../tests/test_configuration_prefix_migration.py) (new)
- [doc/CONFIGURATION_PREFIX_MIGRATION.md](CONFIGURATION_PREFIX_MIGRATION.md) (new)

The compatibility namespace's schemas module reexports the IKOS schemas; it
contained no legacy uppercase configuration token and required no edit.

## Intentionally retained matches and exclusions

- [business_rules.py](../business_rules.py) retains the unrelated existing
  artifact filename `IDMS_SOLF_LLM_GENERATION_PACK.md`. It is not configuration.
- This document intentionally contains old names in the mapping and search
  expression for deployment migration/auditing.
- Tests construct the legacy prefix from fragments to avoid self-matching. They
  reject legacy uppercase tokens across Python sources except the exact artifact
  filename, with an additional environment-call check that does not exempt it.
- `IDMSInteractionTools`, lowercase modules, logger identities, and all other
  non-configuration names are unchanged. No private dotenv, Git metadata,
  generated, or cache files were edited. No commits, pushes, or live DB actions.

## Regression validation

The new tests cover notifier construction, isolated logging configuration, spaCy
model selection, transaction-match configuration, both ingestion router
namespaces, markdown configuration, dynamic source-password precedence, and SQL
connection arguments/security context. Heavy client-startup modules are tested
by executing their actual AST helper bodies/configuration assignments in isolated
namespaces; SQL uses a mocked central configuration and mocked connection.
No parser, SMTP, webhook, vector-store, LLM, or database request is made.

Verified results: **14 migration tests passed; 371 full-suite tests passed**
(six dependency/API deprecation warnings). Python sources passed syntax parsing
and the Git whitespace check passed. The final case-sensitive source/document
scan found **63 matching lines in two files**: one preserved artifact reference
in the runtime, plus 61 mapping rows and one artifact explanation in this
document. There are **zero remaining legacy uppercase configuration reads**.
Private dotenv files, generated/cache content, and Git metadata were excluded.