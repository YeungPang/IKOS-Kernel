# IKOS Core Knowledge Lexicon

The current additive vocabulary release is [`extensions/core-knowledge/1.1.0/extension.json`](../extensions/core-knowledge/1.1.0/extension.json). Version `1.0.0` remains unchanged because imported package key/version pairs are immutable. Version 1.1.0 retains its original vocabulary and common semantic query patterns, then adds domain-neutral English semantic terms organized under stable `core_*` category names.

## Added coverage

The vocabulary includes individually stored terms and phrases for:

1. temporal points, intervals, boundaries, and recurrence (`today`, `yesterday`, `next`, `since`, `fiscal year`, `weekly`);
2. quantitative aggregation, ranking, comparisons, and changes (`sum`, `median`, `top`, `greater than`, `variance`, `increase`);
3. spatial/layout relations, document structures, coordinates, and locations (`above`, `cell`, `table`, `bounding box`, `destination`);
4. interrogatives, inquiry directives, and modality (`who`, `where`, `compare`, `verify`, `is it possible to`, `must`);
5. generic document types, provenance/integrity, and processing states (`attachment`, `issuer`, `checksum`, `parsed`, `redacted`);
6. workflow states, directives, and governance (`pending`, `awaiting input`, `resume`, `compensate`, `checkpoint`);
7. logical connectives, quantifiers, and set terms (`unless`, `only if`, `every`, `subset of`, `intersection`);
8. conversational etiquette, clarification language, and meta-system command phrases (`thank you`, `could you clarify`, `show sources`, `audit trail`).

The `semantic_term` rows use `kind="category"` and stable generic `canonical_name` values beginning `core_`. They contain no customer, finance, HR, sales, or other line-of-business vocabulary. Existing semantic patterns and query aliases from 1.0.0 are preserved. The new lexicon does not add query aliases for ambiguous operators such as “last”, “top”, “between”, or “status”; resolving these to fields or query operators without schema/context is unsafe.

## Promote the new version

Build and validate from the kernel root:

```powershell
python scripts/package_ikos_extension.py extensions/core-knowledge/1.1.0 --check
python scripts/package_ikos_extension.py extensions/core-knowledge/1.1.0
```

Then use the platform extension API sequence described in [`EXTENSION_PACKAGE_PROMOTION.md`](EXTENSION_PACKAGE_PROMOTION.md): import the generated JSON, inspect against an active target tenant, review the create/update/conflict summary and immutable SHA-256, and explicitly promote with that SHA. Promotion is not automatic. Existing 1.0.0 deployments remain unchanged until 1.1.0 is promoted. The package registry does not remove terms omitted from a later release; this release intentionally carries forward the complete 1.0.0 baseline.

## Runtime boundary: vocabulary is not a parser

This is an embedded, portable vocabulary data set within the existing semantic-term mechanism. By itself it does **not** install or claim to provide:

- a precompiled FastText model or a measured sub-2-ms intent classifier;
- a deterministic intent-arbitration/FSM implementation for every conversational phrase;
- a new SOLF lexer/parser grammar or AST operators for natural-language logic, arithmetic, quantifiers, or set operations;
- Duckling or another general temporal expression parser;
- timezone-aware ISO-8601 range normalization for phrases such as “last month”;
- new SQL aggregation/comparison semantics, multimodal coordinate extraction, or system meta-command authorization/routing.

IKOS already has separate query planner, semantic-pattern, SOLF, workflow, document, and temporal facilities, but adding a term to this package does not automatically wire it into every one of those paths. Relative calendar phrases need a reference instant, timezone, locale, calendar/fiscal-year policy, and inclusive/exclusive boundary contract. Keep those semantics in tested runtime code rather than guessing them from a lexicon. Similarly, a phrase like “rollback” is vocabulary, not permission to execute a rollback; normal authorization and workflow lifecycle checks still apply.

This version deliberately adds the missing portable lexical layer only. Deterministic temporal parsing and grammar/operator dispatch should be implemented and tested as separate runtime features, then referenced here for discoverability once integrated.

## Validation

The manifest is validated with the normal package builder. Tests check the eight functional groups, individual phrase storage (not semicolon-delimited pseudo-lists), expected vocabulary examples, and package validity. Promotion/runtime effect tests belong to their respective kernel modules; this data package does not execute code.
