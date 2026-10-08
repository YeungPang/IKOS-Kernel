# Bundled generic SOLF program

## Loading contract and ownership

- `solf_script.txt` is the canonical entry point. Its first-line, fixed baseline
  marker composes `kernel_script.txt` before the neutral additions. The existing
  baseline is retained, not copied: each definition has one source owner.
- Use `solf_program.read_program()` for the **complete program text**, including
  class discovery, planner symbols, schema previews, and audit hashes. A hash of
  the raw entry-point file alone does not describe the complete program.
- `SOLFInterpreter.load_program_file()` accepts either bundled filename (bare
  names resolve relative to this repository, independent of cwd). Absolute paths
  to either bundled file load the same complete program. Explicit outside paths
  retain ordinary file semantics; there is no filename fallback for custom files.
- `load_program_script()` recognizes only the exact first-line fixed marker. It
  does not implement arbitrary include/import paths. Raw `kernel_script.txt`
  text remains usable as the baseline alone.
- Ingestion and interaction readers and both API namespaces use the same reader.
  The ingestion initializer remains `ingest.build_solf_interpreter()`. Its
  existing runtime-rule hydration is separate from the bundled definitions.
- The actual parser interface is `solf_parser.parse_script`, attached as a
  `parse` adapter to `SOLFInterpreter` (there is no `SOLFParser` class here).

## Retained and added

Retained: generic entity CRUD/merge/matching, query/answer/style policies,
document plans, workflow-case plans and terminal-transition approval guards,
scheduler commands, event publish/subscription policies, and composition.

Added: category source routing, person matching, sanitized organization/company
matching (`registration_no`, legal name/address/country only), entity update and
mutation-reversal guards/plans, approval plans, scheduler listing, neutral
person/organization/company/document/email/event/meeting/location/venue classes,
and workflow-case/approval classes and policy maps. All parents and clause calls
resolve within the bundle or existing generic `solf_function` adapters. Matching
does not mandate customer codes or use tax/customs identifiers.

Excluded: accounting/ledger/invoice/payment, tax and jurisdiction-specific
reporting, HR/employment/payroll/child allowances, CRM/customer/supplier,
inventory/product/order/delivery, project/task/budget/simulation, compliance
business rules, business notification examples, regression demos, and their maps
and seed/reset helpers. No domain-function calls are introduced.

Policies are default orchestration policies, **not authorization**. Python
adapters retain permission/tenant/SQL allowlist/confirmation checks. SQL plans
are data; loading the program never executes their statements. Generic approval
and workflow execution still require the existing optional workflow tables and
adapters to be provisioned by the parent application. Domain extensions and any
remaining Python domain routes are a separate parent concern, not enabled by
this bundled program. No IDMS-Demo or vendored-parent files are changed.

## Validation

The real parser/interpreter tests in `tests/test_solf_program.py` block database
and external HTTP effects. They cover full definition loading, class discovery,
generic CRUD/matching/routing/plans/guards/maps, both filename entry points from
another cwd, ingestion hydration fallback, interaction planner symbols, and
both schema API namespaces' previews and effective-program audit hashes.

Two existing interpreter defects encountered during the port are covered too:
object method literals are retained as ASTs without adapter calls at load time;
dictionary values and computed scalar keys bind in the caller's scope and are
evaluated once. Parent applications should consume the shared reader (or the
interpreter's program-file loader), not hash/parse the raw entry point in isolation.