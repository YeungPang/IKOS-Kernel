from __future__ import annotations

import re
from typing import Any


def _normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s_\-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _slug(value: str) -> str:
    text = _normalize_text(value)
    text = text.replace("-", "_").replace(" ", "_")
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _tokenize(value: str) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", _normalize_text(value)) if token}


WORKFLOW_PROCESS_TAXONOMY: dict[str, list[dict[str, Any]]] = {
    "generic": [
        {
            "process": "booking_process",
            "label": "Booking Process",
            "aliases": [
                "booking process",
                "journal booking",
                "posting process",
                "book transaction",
                "buchungsprozess",
            ],
            "keywords": ["booking", "book", "posting", "journal", "entry"],
            "expands_to": ["booking_validation", "account_selection", "vat_classification", "posting_finalize"],
        },
        {
            "process": "ingest",
            "label": "Document Ingestion",
            "aliases": ["ingestion", "import", "extract and ingest", "load document"],
            "keywords": ["ingest", "import", "extract", "parse", "load"],
            "expands_to": [],
        },
        {
            "process": "validation",
            "label": "Validation",
            "aliases": ["validation step", "check step", "verify"],
            "keywords": ["validate", "validation", "check", "verify", "enforce"],
            "expands_to": [],
        },
    ],
    "accounting": [
        {
            "process": "booking_validation",
            "label": "Booking Validation",
            "aliases": ["validate booking", "validate journal", "pre booking check", "double entry validation"],
            "keywords": ["validation", "validate", "check", "journal", "double", "entry", "balance"],
            "expands_to": [],
        },
        {
            "process": "account_selection",
            "label": "Account Selection",
            "aliases": ["select account", "choose account", "account mapping", "account assignment", "konto auswahl", "konto zuordnung"],
            "keywords": ["account", "gl", "ledger", "mapping", "assignment", "chart", "coa"],
            "expands_to": [],
        },
        {
            "process": "vat_classification",
            "label": "VAT Classification",
            "aliases": ["vat step", "mwst step", "tax code selection", "vat box mapping"],
            "keywords": ["vat", "mwst", "ust", "tax", "box", "declaration"],
            "expands_to": [],
        },
        {
            "process": "posting_finalize",
            "label": "Posting Finalization",
            "aliases": ["final posting", "post journal", "persist booking", "save journal"],
            "keywords": ["post", "finalize", "persist", "commit", "journal"],
            "expands_to": [],
        },
    ],
    "hr": [
        {
            "process": "employee_profile_validation",
            "label": "Employee Profile Validation",
            "aliases": ["employee validation", "profile validation", "hr profile check"],
            "keywords": ["employee", "profile", "hr", "validate", "check"],
            "expands_to": [],
        },
        {
            "process": "employment_contract_validation",
            "label": "Employment Contract Validation",
            "aliases": ["contract validation", "employment validation", "contract check"],
            "keywords": ["employment", "contract", "start date", "employer", "employee"],
            "expands_to": [],
        },
    ],
    "crm": [
        {
            "process": "lead_capture",
            "label": "Lead Capture",
            "aliases": ["lead capture", "capture lead", "new lead intake"],
            "keywords": ["lead", "capture", "intake", "prospect"],
            "expands_to": [],
        },
        {
            "process": "lead_qualification",
            "label": "Lead Qualification",
            "aliases": ["qualify lead", "lead scoring", "qualification review"],
            "keywords": ["lead", "qualification", "score", "fit"],
            "expands_to": [],
        },
        {
            "process": "opportunity_progression",
            "label": "Opportunity Progression",
            "aliases": ["opportunity stage update", "pipeline progression", "deal progression"],
            "keywords": ["opportunity", "pipeline", "stage", "deal"],
            "expands_to": [],
        },
        {
            "process": "customer_followup",
            "label": "Customer Follow-up",
            "aliases": ["customer follow up", "account follow up", "next customer action"],
            "keywords": ["customer", "followup", "account", "next action"],
            "expands_to": [],
        },
        {
            "process": "case_management",
            "label": "Case Management",
            "aliases": ["crm case management", "support case", "ticket workflow", "case handling"],
            "keywords": ["case", "ticket", "support", "sla", "incident"],
            "expands_to": [],
        },
    ],
    "erp": [
        {
            "process": "sales_full_cycle",
            "label": "Sales Full Cycle",
            "aliases": [
                "sales full cycle",
                "full sales workflow",
                "quotation to payment",
                "request to payment",
                "end to end sales process",
            ],
            "keywords": ["sales", "quotation", "order", "delivery", "invoice", "payment", "crm", "erp"],
            "expands_to": ["order_to_cash"],
        },
        {
            "process": "procure_to_pay_validation",
            "label": "Procure to Pay Validation",
            "aliases": ["p2p validation", "procure to pay", "purchase to pay"],
            "keywords": ["procure", "pay", "purchase", "vendor", "invoice"],
            "expands_to": [],
        },
        {
            "process": "three_way_match",
            "label": "Three-way Match",
            "aliases": ["3 way match", "po invoice gr match", "invoice match"],
            "keywords": ["three way", "po", "invoice", "goods receipt", "match"],
            "expands_to": [],
        },
        {
            "process": "payment_run_approval",
            "label": "Payment Run Approval",
            "aliases": ["payment approval", "payment run", "payables approval"],
            "keywords": ["payment", "approval", "payables", "release"],
            "expands_to": [],
        },
        {
            "process": "inventory_reconciliation",
            "label": "Inventory Reconciliation",
            "aliases": ["inventory recon", "stock reconciliation", "inventory variance review"],
            "keywords": ["inventory", "stock", "reconciliation", "variance"],
            "expands_to": [],
        },
        {
            "process": "order_to_cash",
            "label": "Order to Cash",
            "aliases": ["o2c", "order to cash", "sales order to collection", "invoice to cash"],
            "keywords": ["order", "cash", "invoice", "collection", "dso"],
            "expands_to": [],
        },
    ],
    "project_management": [
        {
            "process": "scope_baseline",
            "label": "Scope Baseline",
            "aliases": ["scope baseline", "baseline scope", "scope freeze"],
            "keywords": ["scope", "baseline", "deliverable"],
            "expands_to": [],
        },
        {
            "process": "milestone_tracking",
            "label": "Milestone Tracking",
            "aliases": ["track milestone", "milestone review", "delivery milestone"],
            "keywords": ["milestone", "tracking", "delivery", "schedule"],
            "expands_to": [],
        },
        {
            "process": "risk_issue_review",
            "label": "Risk and Issue Review",
            "aliases": ["risk review", "issue review", "raid review"],
            "keywords": ["risk", "issue", "mitigation", "blocker"],
            "expands_to": [],
        },
        {
            "process": "change_control",
            "label": "Change Control",
            "aliases": ["change control", "change request review", "scope change approval"],
            "keywords": ["change", "control", "approval", "request"],
            "expands_to": [],
        },
        {
            "process": "governance_review",
            "label": "Governance Review",
            "aliases": ["governance review", "steering committee review", "project gate review"],
            "keywords": ["governance", "steering", "gate", "review"],
            "expands_to": [],
        },
    ],
    "workflow": [
        {
            "process": "workflow_transition_policy",
            "label": "Workflow Transition Policy",
            "aliases": ["state transition", "workflow transition", "transition policy"],
            "keywords": ["workflow", "state", "transition", "policy", "event"],
            "expands_to": [],
        },
        {
            "process": "workflow_execute",
            "label": "Workflow Execution",
            "aliases": ["execute workflow", "workflow routing", "control flow"],
            "keywords": ["workflow", "execute", "control", "flow", "sequence", "selection", "iteration", "backtracking"],
            "expands_to": [],
        },
    ],
}


_CONTROL_FLOW_ALIASES: dict[str, str] = {
    "sequence": "sequence",
    "sequential": "sequence",
    "selection": "selection",
    "selective": "selection",
    "iteration": "iteration",
    "iterative": "iteration",
    "backtracking": "backtracking",
    "backtrack": "backtracking",
}


def list_workflow_process_taxonomy(domain: str | None = None) -> dict[str, Any]:
    selected_domain = _slug(domain or "")
    if selected_domain and selected_domain in WORKFLOW_PROCESS_TAXONOMY:
        return {"domains": {selected_domain: WORKFLOW_PROCESS_TAXONOMY[selected_domain]}}

    return {"domains": WORKFLOW_PROCESS_TAXONOMY}


def suggest_workflow_processes(
    text: str,
    domain: str | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    normalized_text = _normalize_text(text)
    text_tokens = _tokenize(normalized_text)
    if not normalized_text:
        return []

    selected_domain = _slug(domain or "")
    domain_names = [selected_domain] if selected_domain in WORKFLOW_PROCESS_TAXONOMY else list(WORKFLOW_PROCESS_TAXONOMY.keys())

    candidates: list[dict[str, Any]] = []
    for domain_name in domain_names:
        for item in WORKFLOW_PROCESS_TAXONOMY.get(domain_name, []):
            process = _slug(item.get("process") or "")
            if not process:
                continue
            keywords = {_slug(k).replace("_", " ") for k in item.get("keywords") or [] if str(k).strip()}
            aliases = [_normalize_text(alias) for alias in (item.get("aliases") or []) if str(alias).strip()]

            matched_aliases = [alias for alias in aliases if alias and alias in normalized_text]
            keyword_hits = []
            for keyword in keywords:
                keyword_tokens = _tokenize(keyword)
                if keyword_tokens and keyword_tokens.issubset(text_tokens):
                    keyword_hits.append(keyword)

            score = float(len(matched_aliases) * 4 + len(keyword_hits) * 2)
            if selected_domain and selected_domain == domain_name:
                score += 0.5

            if score <= 0:
                continue

            confidence = min(0.99, round(score / 10.0, 3))
            candidates.append(
                {
                    "domain": domain_name,
                    "process": process,
                    "label": str(item.get("label") or process.replace("_", " ").title()),
                    "confidence": confidence,
                    "score": score,
                    "matched_aliases": matched_aliases,
                    "matched_keywords": sorted(set(keyword_hits)),
                    "expands_to": list(item.get("expands_to") or []),
                }
            )

    candidates.sort(key=lambda row: (-float(row.get("score") or 0.0), str(row.get("process") or "")))
    return candidates[: max(1, int(top_k or 5))]


def canonicalize_workflow_process(
    text: str,
    domain: str | None = None,
    min_confidence: float = 0.35,
) -> dict[str, Any]:
    raw_text = str(text or "").strip()
    if not raw_text:
        return {
            "raw_text": raw_text,
            "normalized_process": "",
            "domain": _slug(domain or "") or None,
            "confidence": 0.0,
            "matched": False,
            "candidates": [],
        }

    candidates = suggest_workflow_processes(raw_text, domain=domain, top_k=5)
    best = candidates[0] if candidates else None
    best_confidence = float(best.get("confidence") or 0.0) if best else 0.0

    if best and best_confidence >= float(min_confidence):
        normalized_process = str(best.get("process") or "")
        normalized_domain = str(best.get("domain") or "") or None
        matched = True
    else:
        normalized_process = _slug(raw_text)
        normalized_domain = _slug(domain or "") or None
        matched = False

    return {
        "raw_text": raw_text,
        "normalized_process": normalized_process,
        "domain": normalized_domain,
        "confidence": best_confidence if best else 0.0,
        "matched": matched,
        "candidates": candidates,
    }


def canonicalize_control_flow(value: str) -> str:
    token = _slug(value)
    return _CONTROL_FLOW_ALIASES.get(token, "")
