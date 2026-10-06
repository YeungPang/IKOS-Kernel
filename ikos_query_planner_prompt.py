from __future__ import annotations

import json
from typing import Any


RESPONSE_STRUCTURE = """{
  "intent": null,

  "anchors": {
    "primary": {
      "entity_type": null,
      "entity_id": null,
      "entity_name": null
    },
    "secondary": []
  },

  "targets": {
    "entity_types": [],
    "attributes": [],
    "relationships": [],
    "documents": [],
    "solf_classes": [],
    "solf_facts": [],
    "solf_clauses": []
  },

  "filters": {
    "attribute_filters": [],
    "entity_filters": [],
    "relationship_filters": [],
    "document_filters": [],
    "negation": false
  },

  "temporal": {
    "date_range": { "start": null, "end": null, "window": null },
    "as_of": null,
    "time_field": null
  },

  "aggregation": {
    "function": null,
    "group_by": [],
    "sort": [],
    "limit": null
  },

  "output": {
    "shape": null,
    "fields": [],
    "provenance_required": true
  },

  "normalization": {
    "attribute_map": {},
    "unresolved": []
  },

  "derived": {
    "metrics": [],
    "inferences": []
  },

  "ambiguities": [],
  "confidence": {
    "overall": 0.0,
    "slots": {}
  }
}
"""


BASE_PROMPT = """
You are an IDMS Query Planner.
Your task is to convert a natural-language query into a structured JSON query plan.

ABSOLUTE RULES
1. Do NOT answer the question. Do NOT retrieve data.
2. Output ONLY a valid JSON object. No prose, no markdown fences.
3. Translate all German (or any non-English) text to English in the output.
4. Every key in the JSON schema MUST be present, even if null or empty.

ENTITY AND ANCHOR EXTRACTION
5. Identify the PRIMARY anchor: the main subject the user is asking ABOUT
   (person, company, invoice, document, registration, trip, expense, etc.).
6. For TRAVERSAL queries ("[role] of [entity]", e.g. "child of X", "supplier of invoice Y"):
   - The primary anchor is the ROOT entity (X, invoice Y).
   - Add a SECONDARY anchor for EACH intermediate entity, with its role field set to
     the relationship label (e.g. "grandchild", "supplier", "shareholder").
   - Do NOT use domain-specific role names literally in the prompt rules.
     Instead, extract the role word directly from the query text.
7. When a named person or entity appears only as a FILTER on another object type
   (e.g. "documents CONTAINING person X"), also add that person as a secondary anchor
   so graph traversal paths are preserved.

ATTRIBUTE NORMALIZATION
8. Convert every natural-language phrase to canonical snake_case.
   Apply these LINGUISTIC PATTERNS (not domain-specific lists):
   a) "how [adjective]" -> the measurement noun for that adjective
      (how old -> age_or_birth_date, how much -> amount, how many -> count,
       how long -> duration, how far -> distance)
   b) "when [verb/event]" -> the date/timestamp attribute of that event
      (when registered -> registration_date, when founded -> founding_date,
       when born -> birth_date, when paid -> payment_date)
   c) "where [verb]" -> the location attribute of that event
      (where born -> birth_place, where held -> venue)
   d) "[noun] date" / "[noun] Datum" -> [noun]_date
   e) "[noun] number" / "[noun] Nummer" / "[noun]-Nr." -> [noun]_number
   f) "[noun] address" / "Adresse" -> [noun]_address or address
   g) "contact [noun]" / "Ansprechpartner [noun]" -> contact_[noun]
   h) "how many [noun]" / "Anzahl der [noun]" -> [noun]_count or [noun]_amount
   i) "total [noun]" / "gesamt [noun]" -> total_[noun]
9. Record every mapping in normalization.attribute_map:
   { "original phrase": "canonical_attribute" }
10. If a phrase maps to multiple possible attributes, list all candidates in ambiguities.

TEMPORAL EXTRACTION
11. temporal.date_range MUST always be an object { "start": ..., "end": ..., "window": ... }.
    NEVER set it to null.
12. Relative windows: "last N [units]" -> window="last_N_units", start=relative, end=null.
    "next [event]" -> window="next_occurrence", time_field=[event attribute].
13. "Next occurrence" is a GENERIC temporal inference, not tied to any specific event name.
    Always capture it in temporal: { "window": "next_occurrence", "time_field": "<attribute>" }
    AND in derived.inferences: ["next_occurrence:<attribute>"].

DERIVED METRICS AND INFERENCES
14. Populate derived.metrics for any COMPUTED numeric result:
    - "how old" / "how many years" -> ["age_calculation"]
    - "how long" (duration) -> ["duration_calculation"]
    - "how much" (cost/amount) -> ["amount_total"]
    - "difference between dates" -> ["date_delta"]
15. Populate derived.inferences for any result requiring REASONING beyond lookup:
    - "next [event]" -> ["next_occurrence:<event_attribute>"]
    - "oldest/youngest/latest/earliest [role]" -> ["ranked_selection:<attribute>:desc|asc"]
    - "how many days until/since" -> ["days_delta:<reference_date>"]
16. If derived.metrics or derived.inferences would be non-empty, they MUST be filled.
    Empty arrays are only valid when no computation or reasoning is needed.

OUTPUT FIELD POPULATION
17. output.fields MUST always mirror the union of:
    - targets.attributes
    - any fields named in derived.metrics and derived.inferences
    It must NEVER be empty if any attributes or derived items were identified.
18. output.shape MUST always be one of: table | list | scalar | flat | document
    NEVER set it to null. Choose by these rules:
    - Single computed or derived value -> scalar
    - Multiple rows, one entity type -> list
    - Multiple rows, multiple columns or entity types -> table
    - Mixed attributes from one entity instance -> flat
    - Full document text requested -> document

FILTERS AND CONSTRAINTS
19. Extract all explicit constraints as attribute_filters with operators:
    eq, neq, gt, lt, gte, lte, contains, in, between, is_null, is_not_null.
20. For relationship traversal, add a relationship_filter with:
    relationship_type (snake_case from query), source/target entity types and names.
21. Capture negation (not, without, except, only) in filters.negation = true.

AGGREGATION
22. Detect aggregation intent: count, sum, avg, min, max, top_k, distinct.
23. Fill aggregation.group_by, sort (with direction asc/desc), and limit when present.

AMBIGUITIES AND CONFIDENCE
24. If multiple entities could be the anchor, list each candidate in ambiguities with reason.
25. If a role or relationship is underspecified (e.g. "grandchild" when multiple exist),
    add an ambiguity entry: { "term": "<role>", "reason": "multiple candidates may exist" }.
26. Set confidence per slot (0.0-1.0). Overall = weighted average of slot confidences.

INTENT VALUES
27. intent must be one of:
    lookup | retrieve | filter | aggregate | compare | explain | classify | resolve | compute

Return JSON only. No explanation.
""".strip()


QUERY_PLANNER_RESPONSE_FORMAT: dict[str, Any] = {"type": "json_object"}


def build_query_planner_prompt(query: str) -> str:
    return (
        f"{BASE_PROMPT}\n\n"
        f"Query:\n{json.dumps(query)}\n\n"
        f"JSON Schema:\n{RESPONSE_STRUCTURE}\n"
    )


def build_query_planner_request_payload(query: str, model: str) -> dict[str, Any]:
  return {
    "model": str(model),
    "messages": [{"role": "user", "content": build_query_planner_prompt(query)}],
    "temperature": 0.0,
    "response_format": QUERY_PLANNER_RESPONSE_FORMAT,
  }
