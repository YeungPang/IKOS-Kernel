"""
API Router: Rule Ingestion

Exposes endpoints for classifying and ingesting natural language rules as:
- Patterns (query templates)
- SOLF clauses (procedural rules)
- Facts (domain assertions)
- Classes (schema definitions)
"""

from typing import Any, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Depends
import logging

logger = logging.getLogger(__name__)

# Try to import dependencies; if not available, defer to runtime
try:
    from rule_ingestion import RuleClassifier, RuleIngestionHandler, RuleType, ClassifiedRule
except ImportError:
    logger.warning("rule_ingestion module not available; endpoints will return 503")
    RuleClassifier = None
    RuleIngestionHandler = None


# ============================================================================
# Request/Response Models
# ============================================================================

class RuleClassificationRequest(BaseModel):
    """Request to classify a natural language rule."""
    rule_text: str = Field(..., description="Natural language rule description")
    use_llm: bool = Field(default=False, description="Use LLM for classification (if available)")


class ClassificationMetadata(BaseModel):
    """Classification result metadata."""
    rule_type: str = Field(..., description="PATTERN, CLAUSE, FACT, or CLASS")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Classification confidence")
    rationale: str = Field(..., description="Explanation of classification")
    extracted_metadata: dict = Field(default_factory=dict, description="Extracted rule metadata")
    suggested_parameters: dict = Field(default_factory=dict, description="Suggested creation parameters")


class RuleClassificationResponse(BaseModel):
    """Response from rule classification endpoint."""
    rule_text: str
    classification: ClassificationMetadata
    next_steps: dict = Field(default_factory=dict, description="Suggested next actions")


class RuleIngestionRequest(BaseModel):
    """Request to ingest a natural language rule."""
    rule_text: str = Field(..., description="Natural language rule description")
    auto_classify: bool = Field(default=True, description="Auto-classify before storing")
    store_if_confident: bool = Field(default=True, description="Store if confidence >= 0.70")


class IngestionResult(BaseModel):
    """Result of rule ingestion."""
    rule_text: str
    rule_type: str
    confidence: float
    storage_location: Optional[str] = Field(None, description="Database table where rule was stored")
    created_id: Optional[int] = Field(None, description="ID of created resource")
    status: str = Field(..., description="Operation status: stored_*, error, etc.")
    error_message: Optional[str] = None


class RuleIngestionBatchRequest(BaseModel):
    """Request to ingest multiple rules."""
    rules: list[str] = Field(..., min_items=1, max_items=100, description="List of NL rules")
    auto_classify: bool = Field(default=True)
    store_if_confident: bool = Field(default=True)


class RuleIngestionBatchResponse(BaseModel):
    """Response from batch ingestion."""
    total_rules: int
    successfully_stored: int
    failed: int
    results: list[IngestionResult]
    summary: dict


# ============================================================================
# Router Setup
# ============================================================================

router = APIRouter(prefix="/api/rules", tags=["rule-ingestion"])


# ============================================================================
# Dependencies
# ============================================================================

def get_rule_classifier() -> RuleClassifier:
    """Get configured rule classifier."""
    if not RuleClassifier:
        raise HTTPException(status_code=503, detail="Rule ingestion module not available")
    # Optionally pass genai_client if available
    return RuleClassifier(genai_client=None)


def get_rule_ingestion_handler() -> RuleIngestionHandler:
    """Get configured rule ingestion handler."""
    if not RuleIngestionHandler:
        raise HTTPException(status_code=503, detail="Rule ingestion module not available")

    from database import get_db_connection
    from pattern_library import PatternLibrary

    conn = get_db_connection()
    try:
        pattern_lib = PatternLibrary(conn)
    except Exception as e:
        logger.warning(f"PatternLibrary initialization failed: {e}")
        pattern_lib = None

    return RuleIngestionHandler(
        db_connection_fn=get_db_connection,
        pattern_library=pattern_lib,
        genai_client=None,
    )


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/classify", response_model=RuleClassificationResponse)
async def classify_rule(
    request: RuleClassificationRequest,
    classifier: RuleClassifier = Depends(get_rule_classifier),
) -> RuleClassificationResponse:
    """
    Classify a natural language rule into type: PATTERN, CLAUSE, FACT, or CLASS.

    This endpoint analyzes a rule description and determines its type with confidence score
    and suggested parameters for storage.

    **Examples:**
    - "What is the invoice due date from X to Y?" → PATTERN
    - "When invoice overdue, escalate to management" → CLAUSE
    - "Company X is registered in Canton Y" → FACT
    - "Invoice has: number, due_date, amount" → CLASS
    """
    try:
        classified = classifier.classify(request.rule_text)

        return RuleClassificationResponse(
            rule_text=classified.raw_input,
            classification=ClassificationMetadata(
                rule_type=classified.rule_type.value,
                confidence=classified.confidence,
                rationale=classified.classification_rationale,
                extracted_metadata=classified.extracted_metadata,
                suggested_parameters=classified.suggested_parameters,
            ),
            next_steps={
                "action": "ingest" if classified.confidence >= 0.70 else "review",
                "endpoint": f"/api/rules/ingest",
                "confidence_threshold": 0.70,
                "current_confidence": classified.confidence,
            },
        )
    except Exception as e:
        logger.error(f"Classification error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ingest", response_model=IngestionResult)
async def ingest_rule(
    request: RuleIngestionRequest,
    handler: RuleIngestionHandler = Depends(get_rule_ingestion_handler),
) -> IngestionResult:
    """
    Classify and ingest a natural language rule into the appropriate storage backend.

    Automatically routes the rule to:
    - **semantic_patterns** for query templates
    - **solf_clauses** for procedural rules
    - **solf_clauses (fact)** for domain assertions
    - **object_class** for schema definitions

    Returns the storage location and created resource ID.
    """
    try:
        result = handler.ingest_rule(request.rule_text)

        # Check confidence if auto-store is enabled
        if request.store_if_confident and result.get("confidence", 0) < 0.70:
            return IngestionResult(
                rule_text=request.rule_text,
                rule_type=result.get("rule_type", "unknown"),
                confidence=result.get("confidence", 0),
                status="rejected_low_confidence",
                error_message=f"Confidence {result.get('confidence', 0):.2f} below threshold 0.70",
            )

        return IngestionResult(
            rule_text=request.rule_text,
            rule_type=result.get("rule_type", "unknown"),
            confidence=result.get("confidence", 0),
            storage_location=result.get("storage_location"),
            created_id=result.get("created_id"),
            status=result.get("status", "error"),
            error_message=result.get("error_message"),
        )
    except Exception as e:
        logger.error(f"Ingestion error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ingest-batch", response_model=RuleIngestionBatchResponse)
async def ingest_rules_batch(
    request: RuleIngestionBatchRequest,
    handler: RuleIngestionHandler = Depends(get_rule_ingestion_handler),
) -> RuleIngestionBatchResponse:
    """
    Classify and ingest multiple natural language rules in a single request.

    Processes up to 100 rules per request. Returns aggregate statistics and per-rule results.
    """
    try:
        results = []
        successfully_stored = 0
        failed = 0

        for rule_text in request.rules:
            try:
                result = handler.ingest_rule(rule_text)

                # Filter by confidence if needed
                if request.store_if_confident and result.get("confidence", 0) < 0.70:
                    result["status"] = "rejected_low_confidence"
                    failed += 1
                elif result.get("status", "").startswith("stored"):
                    successfully_stored += 1
                else:
                    failed += 1

                results.append(
                    IngestionResult(
                        rule_text=rule_text,
                        rule_type=result.get("rule_type", "unknown"),
                        confidence=result.get("confidence", 0),
                        storage_location=result.get("storage_location"),
                        created_id=result.get("created_id"),
                        status=result.get("status", "error"),
                        error_message=result.get("error_message"),
                    )
                )
            except Exception as e:
                logger.error(f"Batch ingestion error for rule '{rule_text}': {e}")
                results.append(
                    IngestionResult(
                        rule_text=rule_text,
                        rule_type="unknown",
                        confidence=0.0,
                        status="error",
                        error_message=str(e),
                    )
                )
                failed += 1

        # Build summary
        type_counts = {}
        storage_counts = {}
        for result in results:
            type_counts[result.rule_type] = type_counts.get(result.rule_type, 0) + 1
            if result.storage_location:
                storage_counts[result.storage_location] = storage_counts.get(result.storage_location, 0) + 1

        return RuleIngestionBatchResponse(
            total_rules=len(request.rules),
            successfully_stored=successfully_stored,
            failed=failed,
            results=results,
            summary={
                "type_distribution": type_counts,
                "storage_distribution": storage_counts,
                "success_rate": successfully_stored / len(request.rules) if request.rules else 0.0,
            },
        )
    except Exception as e:
        logger.error(f"Batch ingestion error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/types")
async def get_rule_types() -> dict[str, Any]:
    """
    Get available rule types and their characteristics.

    Returns description, keywords, and example rules for each type.
    """
    return {
        "types": [
            {
                "name": "PATTERN",
                "description": "Query templates and synonyms for information lookup",
                "keywords": ["query", "ask", "find", "what", "when", "template", "match"],
                "example": "What is the invoice due date from X to Y?",
                "storage": "semantic_patterns",
                "use_case": "Natural language query understanding and parsing",
            },
            {
                "name": "CLAUSE",
                "description": "Procedural rules with conditions and actions",
                "keywords": ["when", "then", "if", "trigger", "execute", "policy"],
                "example": "When invoice is overdue for 30 days, escalate to management",
                "storage": "solf_clauses",
                "use_case": "Business process automation and workflow triggers",
            },
            {
                "name": "FACT",
                "description": "Domain assertions and knowledge statements",
                "keywords": ["is", "are", "has", "registered", "located", "defined"],
                "example": "Company X is registered in Canton Y",
                "storage": "solf_clauses (fact)",
                "use_case": "Knowledge graph population and domain reasoning",
            },
            {
                "name": "CLASS",
                "description": "Schema entity definitions and data structures",
                "keywords": ["class", "type", "entity", "attribute", "property", "field"],
                "example": "Invoice class has: invoice_number, due_date, amount, status",
                "storage": "object_class",
                "use_case": "Schema management and entity type definitions",
            },
        ],
        "confidence_threshold": 0.70,
        "batch_limit": 100,
    }


# ============================================================================
# Error Handlers
# ============================================================================

@router.get("/health")
async def health_check() -> dict[str, str]:
    """Health check for rule ingestion service."""
    try:
        classifier = RuleClassifier()
        return {"status": "healthy", "classifier": "available"}
    except Exception as e:
        return {"status": "degraded", "classifier": str(e)}
