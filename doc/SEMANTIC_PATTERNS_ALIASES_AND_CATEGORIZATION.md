# Semantic patterns, aliases, and categorization in IKOS-Kernel

This guide explains how IKOS interprets query wording, normalizes domain terms, and decides which data source can answer a query. These mechanisms are related, but they solve different problems.

## At a glance

| Mechanism | Main question it answers | Stored in | Example |
|---|---|---|---|
| Semantic pattern | “What kind of question does this phrase express?” | `semantic_patterns`, optionally mirrored in a Qdrant semantic-pattern collection | “How long has Acme been a customer?” → tenure concept |
| Pattern synonym | “Is this another phrase for a particular query pattern?” | `pattern_synonyms`, linked to a `semantic_patterns` row | “How many years old?” is an alternative wording for the age pattern |
| Semantic term | “What vocabulary has ingestion or rules associated with a canonical schema term?” | `semantic_terms` | “turnover” associated with canonical `revenue` |
| Query-term alias | “Which canonical attribute/category/relationship should this exact term resolve to at query time?” | `query_term_alias` | “VAT number” → `vat_no` |
| Query-alias candidate | “What possible alias has been observed and may be approved?” | `query_alias_candidate` | An LLM proposes “annual turnover” → `revenue`; an administrator reviews it |
| Category definition and registry | “Which data sources and retrieval methods can serve this category?” | `categories` and its source/mapping tables; `category_registry` projection | Category `invoice` is available from the document KB using RAG/document facets |
| Query-pattern registry | “For this intent, which source scopes and access methods are allowed?” | `query_pattern_registry` | A document-reference intent may use `document_kb` and `rag` |

The rough progression is:

```text
Question wording
    │
    ├─ normalize individual terms ── semantic terms / aliases ── canonical schema vocabulary
    │
    ├─ recognize query shape ─────── semantic pattern / synonym / query-pattern registry
    │
    └─ resolve available sources ─── category definitions / category registry
                                          │
                                          └─ select an allowed retrieval method
```

## 1. Semantic patterns: recognize a question shape

A `semantic_patterns` row represents a phrase or phrase template and the meaning IKOS assigns to it. Its important fields are:

- `pattern_text`: a literal phrase or a template containing `*` wildcards.
- `semantic_concept`: a normalized concept such as `age`, `duration`, or a domain concept.
- `mapped_attributes`: the canonical attributes needed to answer the question.
- `computation_rule`: optional expression describing a derived value, such as a duration calculation.
- `entity_class`: optional entity restriction.
- `pattern_language`, `confidence`, `source_type`, and `metadata`: language, trust/ranking and provenance information.

`PatternLibrary.match_pattern()` in [pattern_library.py](../pattern_library.py) tries deterministic matches in stages:

1. Direct exact phrase lookup.
2. Exact lookup of a `pattern_synonyms` phrase linked to a pattern. The stored `semantic_distance` reduces the match score (`1 - semantic_distance`).
3. Wildcard-template matching for patterns containing `*`.
4. Regex-style matching as a final deterministic fallback.

The final acceptance logic applies confidence thresholds: high-confidence exact matches can return immediately; otherwise template and regex candidates are considered, with the strongest remaining candidate selected. Long questions are rejected by the deterministic matcher to avoid expensive broad scans.

`QueryEngine` uses this library in its deterministic pre-parser and normal query parser. A matched `SemanticPattern` is converted to a `ParsedQuery`, carrying such details as the matched pattern, concept, mapped attribute, entity hint, confidence, and parse method. For some questions, virtual patterns and vector similarity provide additional fallback matching. Vector pattern points are synchronized from database patterns plus a small in-code virtual-pattern set; they complement rather than replace the database registry.

Pattern seeds exist in `PatternLibrary.seed_initial_patterns()`. That method defines general and some domain-oriented examples, but it is a method that must be invoked by setup code; constructing `PatternLibrary` alone does not call it automatically.

## 2. Pattern synonyms: alternate wording for a pattern

`pattern_synonyms` is a child table: each synonym belongs to a `semantic_patterns.pattern_id`. It does not define a separate mapping from arbitrary wording to a database column. It says that a particular phrase is another way to express the parent pattern.

The table tracks language, match type (`exact`, `template`, `fuzzy`, `semantic`), distance, and metadata. In the current deterministic matcher, the synonym path performs an exact, case-normalized phrase comparison and uses `semantic_distance` to adjust confidence. The other `match_type` values describe the synonym but do not, by themselves, implement separate fuzzy or embedding matching algorithms in that exact lookup path.

## 3. Semantic terms versus query-term aliases

These names are similar, but their roles differ.

### `semantic_terms`: learned or declared vocabulary

`semantic_terms` associates a term with a canonical name and a kind (`attribute`, `relationship`, `entity_type`, or `category`). It also records language, provenance, and metadata.

IKOS populates terms from sources including:

- SOLF class definitions and allowed attributes.
- Ingested entity attributes and relationships.
- Structured business rules.
- Ingestion-generated terminology and, where enabled, LLM-expanded aliases.

The `AttributeEmbeddingIndex` reads attribute semantic terms and uses them alongside its canonical attribute vocabulary for schema matching. Thus, semantic terms can help map varied user wording to schema vocabulary by lexical/vector similarity. They are not themselves complete query plans and do not state which database or collection should be queried.

### `query_term_alias`: approved, active normalization rules

A `query_term_alias` is a direct active mapping:

```text
(alias_text, kind, language) → canonical_name
```

Kinds include attribute, relationship, entity type, and category. It has priority, source, active state, and metadata. The query engine loads active aliases into its schema-alias cache in `_refresh_schema_alias_cache()` and uses that vocabulary in attribute-name normalization. Static canonical aliases are loaded first; database aliases then override matching normalized keys according to the order in which rows are applied. The query engine refreshes this cache during initialization and at the explicit refresh path.

This mapping is useful for a curated, deterministic correction such as “VAT number” → `vat_no`. It is different from a broad learned semantic similarity search.

### `query_alias_candidate`: proposed, not automatically trusted

Ingestion may discover terminology candidates, including LLM-expanded terms, and upsert them to `query_alias_candidate`. Their status starts as `proposed`. They do **not** become active query aliases just by being observed. `promote_query_alias_candidate()` copies an approved candidate into `query_term_alias` and marks the candidate approved. This review gate prevents noisy inferred vocabulary from silently changing query behavior.

## 4. Categorization: map a query to available data

IKOS has two related category models.

### Category definitions: configuration and detailed mappings

The `categories` family (`categories`, `category_sources`, `category_entities`, and `category_attributes`) describes named categories and the sources, retrieval methods, entity mappings, and attribute mappings associated with each one. `upsert_category_definition()` writes this structured configuration. It can describe internal DB, external DB, or document sources and methods such as SQL, semantic lookup, RAG, or document facets.

External schema discovery has a separate `source_schema_taxonomy` table that associates external source tables/columns with categories and roles such as identity, attribute, filter, measure, or temporal field.

### `category_registry`: compact coverage index for planning

`category_registry` is a unified, denormalized projection of what a source can answer. A row identifies a `category_name`, `source_scope` (`internal`, `external`, or `document_kb`), `access_method` (`sql`, `semantic_lookup`, `rag`, or `document_facet`), optional source/schema/table/column, and coverage lists for entity types, attributes, and document types. Confidence and metadata provide ranking and provenance.

The projection is populated from:

- Document ingestion: document category/type, semantic profile concepts, query categories, extracted entities, and attributes.
- Internal-registry rebuild: active object classes and attributes, plus document categories/types and extracted facets/actions.
- External source registration and schema taxonomy.

The source/category API can also define categories and mappings directly. The category definitions are the richer configuration; the category registry is the query-time coverage view. They are related but are **not the same table**, and keeping their projections refreshed matters.

### Query planning order

`QueryEngine._classify_query_pattern()` classifies a parsed query into a universal intent such as identity, attribute, relationship, collection, document reference, document attribute, amount, duration, status, responsibility, location, or workflow action. It loads the matching `query_pattern_registry` definition, which constrains the allowed source scopes and access methods.

Then `_resolve_query_sources()`:

1. Collects category/entity hints, attributes, document type, and extraction hints from the parsed query and query plan.
2. Optionally consults a SOLF category-inference hook.
3. Resolves matching coverage rows through `resolve_category_registry_for_query()`.
4. Filters the candidates using the query pattern’s allowed scopes and retrieval methods.
5. Chooses a preferred source scope. Document hints favor `document_kb`; a named scalar fact can favor an available internal source; otherwise the configured source preference and candidate priority apply.
6. Records the decision and candidates in `criteria.source_resolution`, including whether a validated candidate exists and which access method should be used.

That `authorized` field means the planner found a category candidate allowed by the registered query pattern. It is **not a substitute for user authorization**. Tenant and RBAC checks must still decide whether the caller can access the selected data.

## Example: “What is the VAT number for Supplier A?”

1. The parser/query planner recognizes a named-entity attribute lookup.
2. `_normalize_attribute_name()` may normalize “VAT number” through the active alias cache or semantic schema index to `vat_no`.
3. The category/source planner looks for the supplier entity and `vat_no` coverage in the category registry.
4. The universal query-pattern definition limits which source scopes/access methods are eligible.
5. The planner returns a source-resolution object; execution uses the selected supported retrieval path.
6. The database tenant/RBAC policy independently filters records for the caller. A finance or restricted category needs its corresponding permission even when the source registry says that source exists.

For “Show invoices about Supplier A,” category candidates can instead identify `document_kb` coverage with RAG or document-facet access, based on ingested `doc_cat`, `doc_type`, and document profile concepts.

## What this is—and is not

- **Semantic pattern:** query intent/shape → concept and likely schema mapping.
- **Pattern synonym:** alternate phrase → an existing semantic pattern.
- **Semantic term:** vocabulary evidence → a canonical schema term, potentially searchable lexically or by embedding.
- **Query-term alias:** curated active phrase → canonical term.
- **Query-alias candidate:** unapproved suggestion awaiting review.
- **Category definition:** configured source and schema mappings.
- **Category registry:** query-time coverage projection.
- **Query-pattern registry:** allowed source/method policy for an intent.

None of these should be confused with the others. In particular, categorization answers “where could this data come from?”; RBAC/RLS answers “may this caller read it?”

## Current implementation caveats

1. **Vocabulary and category configuration is tenant-scoped.** Semantic patterns, synonyms, semantic terms, active aliases, alias candidates, external schema taxonomy, category definitions/mappings, and the category coverage registry carry `tenant_id` and are intended to use tenant RLS. Existing rows are assigned to the legacy tenant during migration. `query_pattern_registry` remains globally shared because it defines kernel-wide intent and retrieval policy, not organization vocabulary.
2. **Alias cache freshness is explicit.** `QueryEngine` reads active aliases into a cache. Changes should trigger the refresh path or a query-engine refresh/restart; a database write alone does not guarantee every running instance immediately sees it.
3. **Candidate promotion is a trust boundary.** Keep proposed aliases out of active lookup until reviewed. An incorrect alias can change which field or category a query targets.
4. **Category coverage can be incomplete or stale.** A missing registry row may prevent a valid source from being selected; a wrong row may point the planner toward a source that cannot answer the query. Rebuild/update category projections as internal and external schemas change.
5. **Pattern confidence is a routing aid, not proof of an answer.** Continue to validate entity resolution, source authorization, and retrieved evidence after matching.
6. **Tenant filtering must apply to vectors and process caches too.** Semantic-pattern vectors and learned attribute/relationship vectors must carry tenant ownership and be queried with tenant filters. Built-in canonical attribute vectors may be explicitly marked global. Shared Qdrant collections must only be incrementally updated; deleting/recreating one would erase every tenant's vectors.
7. **Tenant-specific pattern defaults require provisioning.** `PatternLibrary.seed_initial_patterns()` remains opt-in; new tenants do not inherit another tenant's custom patterns. Provision the desired defaults for each tenant as part of tenant setup.
8. **External source ownership is separate from category mapping.** Source registrations are tenant-scoped; taxonomy and category mappings must only refer to source IDs owned by the active tenant. The runtime and install DDL should be deployed together so source ownership and vocabulary/category RLS are consistent.

## Relevant implementation files

- [pattern_library.py](../pattern_library.py): pattern storage, exact/synonym/template/regex matching, learning helpers.
- [object_db.py](../object_db.py): DDL, upserts, alias candidate promotion, category resolution, registry rebuild.
- [query_engine.py](../query_engine.py): pattern-to-query parsing, alias cache, query-pattern classification, source/category resolution.
- [attribute_embedding_index.py](../attribute_embedding_index.py): schema vocabulary/semantic-term indexing and attribute matching.
- [ingest.py](../ingest.py): semantic-term and alias-candidate generation; document category registry projection.
- [rule_ingestion.py](../rule_ingestion.py): ingestion of user-authored rules/patterns into their corresponding registries.
