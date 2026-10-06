"""
Rule Ingestion Pipeline: NL-to-Pattern/Clause/Fact/Class Classification & Auto-Seeding

Accepts natural language rule descriptions and automatically classifies them as:
- PATTERN: Query templates/synonyms (e.g., "invoice due date from X to Y")
- CLAUSE: SOLF procedures/computations (e.g., "when invoice overdue, escalate")
- FACT: Domain assertions (e.g., "Company X is registered in Canton Y")
- CLASS: Schema entity definitions (e.g., "invoice has attributes: amount, due_date")
"""

import json
import logging
import re
from dataclasses import dataclass, asdict
from typing import Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)

class RuleType(str, Enum):
    """Classification of ingested rules."""
    PATTERN = "pattern"
    CLAUSE = "clause"
    FACT = "fact"
    CLASS = "class"


@dataclass
class ClassifiedRule:
    """Result of NL rule classification."""
    rule_type: RuleType
    confidence: float
    raw_input: str
    classification_rationale: str
    extracted_metadata: dict[str, Any]
    suggested_parameters: dict[str, Any]


class RuleClassifier:
    """Classifies natural language rules into pattern/clause/fact/class types."""

    # Keywords/patterns for each rule type
    PATTERN_KEYWORDS = {
        "query", "ask", "find", "what", "when", "how", "show", "search", "lookup",
        "retrieve", "list", "match", "template", "synonym", "from", "to", "between",
    }

    CLAUSE_KEYWORDS = {
        "when", "then", "if", "execute", "trigger", "perform", "action", "rule",
        "policy", "require", "check", "validate", "enforce", "apply", "run", "compute",
        "derive", "calculate", "escalate", "alert", "notify", "process",
    }

    FACT_KEYWORDS = {
        "is", "are", "has", "have", "belongs", "registered", "located", "defined",
        "state", "represent", "called", "known", "named", "equal", "contains",
    }

    CLASS_KEYWORDS = {
        "class", "entity", "object", "type", "schema", "attribute", "field", "property",
        "relationship", "relationship", "contains", "has", "inherit", "extends",
        "define", "datatype", "structure",
    }

    def __init__(self, genai_client=None):
        """
        Initialize classifier with optional LLM client for advanced classification.

        Args:
            genai_client: Optional GenAI client for LLM-based classification
        """
        self.genai_client = genai_client

    def classify(self, nl_rule: str) -> ClassifiedRule:
        """
        Classify a natural language rule into type: pattern, clause, fact, or class.

        Args:
            nl_rule: Natural language rule description

        Returns:
            ClassifiedRule with type, confidence, and extracted metadata
        """
        if not nl_rule or not str(nl_rule).strip():
            raise ValueError("Rule input cannot be empty")

        rule_text = str(nl_rule).strip()

        # Try LLM classification first if available
        if self.genai_client:
            try:
                llm_result = self._classify_with_llm(rule_text)
                if llm_result:
                    return llm_result
            except Exception as e:
                logger.warning(f"LLM classification failed, falling back to heuristic: {e}")

        # Heuristic-based classification
        return self._classify_heuristic(rule_text)

    def _classify_heuristic(self, rule_text: str) -> ClassifiedRule:
        """Heuristic classification based on keywords and patterns."""
        text_lower = rule_text.lower()
        tokens = set(re.findall(r"\b\w+\b", text_lower))

        scores = {
            RuleType.PATTERN: self._score_type(tokens, self.PATTERN_KEYWORDS),
            RuleType.CLAUSE: self._score_type(tokens, self.CLAUSE_KEYWORDS),
            RuleType.FACT: self._score_type(tokens, self.FACT_KEYWORDS),
            RuleType.CLASS: self._score_type(tokens, self.CLASS_KEYWORDS),
        }

        # Check structural patterns
        pattern_score = scores[RuleType.PATTERN]
        clause_score = scores[RuleType.CLAUSE]
        fact_score = scores[RuleType.FACT]
        class_score = scores[RuleType.CLASS]

        # Pattern-specific: template with placeholders/wildcards
        if any(marker in rule_text for marker in ["*", "$", "X", "Y", "Z", "...", "[", "]", "from", "to"]):
            pattern_score += 0.2

        # Clause-specific: conditional logic
        if any(marker in text_lower for marker in ["when", "then", "if", "->", "else"]):
            clause_score += 0.3

        # Class-specific: structural definitions
        if any(marker in text_lower for marker in ["class", "type", "define", "attribute", "property", "field"]):
            class_score += 0.3

        # Fact-specific: simple assertions
        if text_lower.count(" is ") > 0 and any(marker in text_lower for marker in ["is", "are", "has", "have"]):
            fact_score += 0.25

        # Normalize to 0-1
        total = sum([pattern_score, clause_score, fact_score, class_score])
        if total > 0:
            scores = {k: v / total for k, v in scores.items()}
        else:
            scores = {k: 0.25 for k in scores}

        # Determine winner
        rule_type = max(scores, key=scores.get)
        confidence = scores[rule_type]

        # Extract metadata based on type
        extracted_metadata = self._extract_metadata(rule_text, rule_type)
        suggested_parameters = self._suggest_parameters(rule_text, rule_type, extracted_metadata)

        rationale = f"Heuristic classification: {rule_type.value} score {confidence:.2f}"

        return ClassifiedRule(
            rule_type=rule_type,
            confidence=confidence,
            raw_input=rule_text,
            classification_rationale=rationale,
            extracted_metadata=extracted_metadata,
            suggested_parameters=suggested_parameters,
        )

    def _classify_with_llm(self, rule_text: str) -> Optional[ClassifiedRule]:
        """Use LLM to classify and extract metadata."""
        if not self.genai_client:
            return None

        system_prompt = """You are an expert at classifying natural language rules into categories:
- PATTERN: Query templates/synonyms for information lookup (e.g., "invoice due date from X to Y")
- CLAUSE: Procedural rules with conditions and actions (e.g., "when invoice overdue, escalate")
- FACT: Domain assertions/knowledge statements (e.g., "Company X is in Canton Y")
- CLASS: Schema/entity definitions (e.g., "invoice has amount, due_date, status")

Respond in JSON: {
  "type": "PATTERN|CLAUSE|FACT|CLASS",
  "confidence": 0.0-1.0,
  "rationale": "...",
  "metadata": {...},
  "parameters": {...}
}"""

        user_prompt = f"Classify this rule:\n{rule_text}"

        try:
            response = self.genai_client.complete(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.3,
                max_tokens=500,
            )

            result_text = str(response.get("content") or "").strip()
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1].strip()
                if result_text.startswith("json"):
                    result_text = result_text[4:].strip()

            parsed = json.loads(result_text)
            rule_type = RuleType(parsed.get("type", "pattern").lower())

            return ClassifiedRule(
                rule_type=rule_type,
                confidence=float(parsed.get("confidence", 0.5)),
                raw_input=rule_text,
                classification_rationale=parsed.get("rationale", ""),
                extracted_metadata=parsed.get("metadata", {}),
                suggested_parameters=parsed.get("parameters", {}),
            )
        except Exception as e:
            logger.debug(f"LLM classification parse error: {e}")
            return None

    def _score_type(self, tokens: set[str], keywords: set[str]) -> float:
        """Calculate match score for a rule type."""
        if not tokens or not keywords:
            return 0.0
        overlap = len(tokens & keywords)
        return overlap / len(keywords) if keywords else 0.0

    def _extract_metadata(self, rule_text: str, rule_type: RuleType) -> dict[str, Any]:
        """Extract rule-specific metadata."""
        metadata = {}

        if rule_type == RuleType.PATTERN:
            # Extract template structure and placeholders
            wildcards = re.findall(r"\*|\$\d+|\[.*?\]", rule_text)
            metadata["wildcards"] = wildcards
            metadata["template_parts"] = re.split(r"\*|\$\d+|\[.*?\]", rule_text)

        elif rule_type == RuleType.CLAUSE:
            # Extract conditions and actions
            if_then_match = re.search(r"(?:when|if)\s+(.+?)\s+(?:then|do)\s+(.+?)(?:\.|$)", rule_text, re.IGNORECASE)
            if if_then_match:
                metadata["condition"] = if_then_match.group(1)
                metadata["action"] = if_then_match.group(2)

        elif rule_type == RuleType.FACT:
            # Extract subject, predicate, object
            subject_match = re.search(r"^(.+?)\s+(?:is|are|has|have)\s+(.+?)(?:\.|$)", rule_text, re.IGNORECASE)
            if subject_match:
                metadata["subject"] = subject_match.group(1)
                metadata["predicate"] = subject_match.group(2)

        elif rule_type == RuleType.CLASS:
            # Extract class name and attributes
            class_match = re.search(r"(?:class|entity|type)\s+(\w+)", rule_text, re.IGNORECASE)
            if class_match:
                metadata["class_name"] = class_match.group(1)

            # Extract attribute list
            attr_match = re.search(r"(?:has|contains|attributes?):?\s*(.+?)(?:\.|$)", rule_text, re.IGNORECASE)
            if attr_match:
                attr_text = attr_match.group(1)
                metadata["attributes"] = [a.strip() for a in re.split(r",|;|\band\b", attr_text)]

        return metadata

    def _suggest_parameters(
        self, rule_text: str, rule_type: RuleType, extracted_metadata: dict[str, Any]
    ) -> dict[str, Any]:
        """Suggest parameters for rule creation."""
        parameters = {}

        if rule_type == RuleType.PATTERN:
            # Suggest pattern_library.add_pattern() parameters
            parameters["pattern_text"] = rule_text
            parameters["semantic_concept"] = self._infer_semantic_concept(rule_text)
            parameters["confidence"] = 0.85  # Default
            parameters["pattern_language"] = self._infer_language(rule_text)
            parameters["entity_class"] = self._infer_entity_class(rule_text)

        elif rule_type == RuleType.CLAUSE:
            # Suggest solf_clauses parameters
            parameters["clause_name"] = self._infer_clause_name(extracted_metadata)
            parameters["clause_type"] = self._infer_clause_type(extracted_metadata)
            parameters["entity_class"] = self._infer_entity_class(rule_text)

        elif rule_type == RuleType.FACT:
            # Suggest fact storage (in solf_clauses or separate fact table)
            parameters["subject"] = extracted_metadata.get("subject", "")
            parameters["predicate"] = extracted_metadata.get("predicate", "")
            parameters["entity_class"] = self._infer_entity_class(rule_text)

        elif rule_type == RuleType.CLASS:
            # Suggest class definition parameters
            parameters["class_name"] = extracted_metadata.get("class_name", self._infer_class_name(rule_text))
            parameters["attributes"] = extracted_metadata.get("attributes", [])
            parameters["parent_class"] = self._infer_parent_class(rule_text)

        return parameters

    def _infer_semantic_concept(self, text: str) -> str:
        """Infer semantic concept from pattern text."""
        # Convert pattern to snake_case concept
        words = re.findall(r"\b[a-z]+\b", text.lower())
        return "_".join(words[:4])  # Use first 4 words

    def _infer_clause_name(self, metadata: dict) -> str:
        """Infer clause name from condition."""
        condition = metadata.get("condition", "")
        if condition:
            words = re.findall(r"\b[a-z]+\b", condition.lower())
            return "_".join(words[:3])
        return "custom_clause"

    def _infer_clause_type(self, metadata: dict) -> str:
        """Determine clause type."""
        action = metadata.get("action", "").lower()
        if any(w in action for w in ["compute", "derive", "calculate"]):
            return "computation_rule"
        elif any(w in action for w in ["trigger", "alert", "escalate", "notify"]):
            return "resolve_policy"
        else:
            return "ingest_rule"

    def _infer_entity_class(self, text: str) -> Optional[str]:
        """Infer entity class from text."""
        text_lower = text.lower()
        entity_keywords = {
            "invoice": "invoice",
            "company": "company",
            "person": "person",
            "employee": "employee",
            "document": "document",
            "contract": "contract",
            "transaction": "transaction",
            "ledger": "ledger",
            "account": "account",
        }
        for keyword, entity_class in entity_keywords.items():
            if keyword in text_lower:
                return entity_class
        return None

    def _infer_language(self, text: str) -> str:
        """Infer language from text."""
        german_words = {"von", "für", "fuer", "ist", "alle", "wenn", "dann", "falligkeitsdatum"}
        words = set(re.findall(r"\b[a-z]+\b", text.lower()))
        if words & german_words:
            return "de"
        return "en"

    def _infer_semantic_concept(self, text: str) -> str:
        """Infer semantic concept."""
        words = re.findall(r"\b[a-z]+\b", text.lower())
        return "_".join(words[:4])

    def _infer_class_name(self, text: str) -> str:
        """Infer class name from text."""
        match = re.search(r"(?:class|entity|type)\s+(\w+)", text, re.IGNORECASE)
        if match:
            return match.group(1)
        words = re.findall(r"\b[a-z]+\b", text.lower())
        return words[0].capitalize() if words else "CustomClass"

    def _infer_parent_class(self, text: str) -> Optional[str]:
        """Infer parent class inheritance."""
        match = re.search(r"(?:extends|inherits from|is a|subclass of)\s+(\w+)", text, re.IGNORECASE)
        if match:
            return match.group(1)
        return None


class RuleIngestionHandler:
    """Handles ingestion of classified rules into appropriate storage."""

    def __init__(
        self,
        db_connection_fn,
        pattern_library=None,
        genai_client=None,
    ):
        """
        Initialize handler with dependencies.

        Args:
            db_connection_fn: Function returning DB connection
            pattern_library: PatternLibrary instance
            genai_client: Optional GenAI client
        """
        self.db_connection_fn = db_connection_fn
        self.pattern_library = pattern_library
        self.classifier = RuleClassifier(genai_client=genai_client)

    def ingest_rule(self, nl_rule: str) -> dict[str, Any]:
        """
        Ingest a natural language rule: classify and store in appropriate backend.

        Args:
            nl_rule: Natural language rule description

        Returns:
            dict with ingestion result, storage location, and IDs
        """
        # Classify
        classified = self.classifier.classify(nl_rule)
        result = {
            "input": nl_rule,
            "rule_type": classified.rule_type.value,
            "confidence": classified.confidence,
            "classification_rationale": classified.classification_rationale,
            "storage_location": None,
            "created_id": None,
            "status": "classified",
        }

        try:
            # Route to appropriate handler
            if classified.rule_type == RuleType.PATTERN:
                stored_id = self._ingest_pattern(classified)
                result["storage_location"] = "semantic_patterns"
                result["created_id"] = stored_id
                result["status"] = "stored_pattern"

            elif classified.rule_type == RuleType.CLAUSE:
                stored_id = self._ingest_clause(classified)
                result["storage_location"] = "solf_clauses"
                result["created_id"] = stored_id
                result["status"] = "stored_clause"

            elif classified.rule_type == RuleType.FACT:
                stored_id = self._ingest_fact(classified)
                result["storage_location"] = "solf_clauses (fact)"
                result["created_id"] = stored_id
                result["status"] = "stored_fact"

            elif classified.rule_type == RuleType.CLASS:
                stored_id = self._ingest_class(classified)
                result["storage_location"] = "object_class"
                result["created_id"] = stored_id
                result["status"] = "stored_class"

        except Exception as e:
            result["status"] = "error"
            result["error_message"] = str(e)
            logger.error(f"Rule ingestion failed: {e}", exc_info=True)

        return result

    def _ingest_pattern(self, classified: ClassifiedRule) -> Optional[int]:
        """Ingest as semantic pattern."""
        if not self.pattern_library:
            raise RuntimeError("PatternLibrary not available")

        params = classified.suggested_parameters
        pattern_id = self.pattern_library.add_pattern(
            pattern_text=params.get("pattern_text", ""),
            semantic_concept=params.get("semantic_concept", ""),
            mapped_attributes={
                params.get("semantic_concept"): params.get("pattern_text"),
            },
            computation_rule="",
            entity_class=params.get("entity_class"),
            confidence=params.get("confidence", 0.85),
            pattern_language=params.get("pattern_language", "en"),
        )
        logger.info(f"Ingested pattern {pattern_id}: {params.get('pattern_text')}")
        return pattern_id

    def _ingest_clause(self, classified: ClassifiedRule) -> Optional[int]:
        """Ingest as SOLF clause."""
        if not self.pattern_library:
            raise RuntimeError("PatternLibrary not available")

        params = classified.suggested_parameters
        clause_id = self.pattern_library.upsert_solf_clause(
            clause_name=params.get("clause_name", ""),
            entity_class=params.get("entity_class"),
            clause_type=params.get("clause_type", "ingest_rule"),
            clause_body=classified.raw_input,  # Store NL as body for now
            metadata={
                "source": "rule_ingestion",
                "rule_type": "clause",
                "classification_confidence": float(classified.confidence),
            },
            is_active=True,
            created_by="system:rule_ingestion",
        )
        logger.info(f"Ingested clause {clause_id}: {params.get('clause_name')}")
        return clause_id

    def _ingest_fact(self, classified: ClassifiedRule) -> Optional[int]:
        """Ingest as SOLF fact-like policy clause in the SQL-backed clause catalog."""
        if not self.pattern_library:
            raise RuntimeError("PatternLibrary not available")

        params = classified.suggested_parameters
        fact_id = self.pattern_library.upsert_solf_clause(
            clause_name=f"fact_{params.get('subject', 'unknown').replace(' ', '_')}",
            entity_class=params.get("entity_class"),
            # Keep within DB-allowed clause types; mark subtype in metadata.
            clause_type="resolve_policy",
            clause_body=classified.raw_input,
            metadata={
                "source": "rule_ingestion",
                "rule_type": "fact",
                "classification_confidence": float(classified.confidence),
                "subject": params.get("subject", ""),
                "predicate": params.get("predicate", ""),
            },
            is_active=True,
            created_by="system:rule_ingestion",
        )
        logger.info(f"Ingested fact {fact_id}: {classified.raw_input}")
        return fact_id

    def _ingest_class(self, classified: ClassifiedRule) -> Optional[int]:
        """Ingest as schema class."""
        conn = self.db_connection_fn()
        try:
            params = classified.suggested_parameters
            class_name = params.get("class_name", "")
            attributes = params.get("attributes", [])
            parent_class = params.get("parent_class")

            # Insert into object_class table
            query = """
                INSERT INTO object_class (class_name, parent_class_id, description)
                SELECT %s, (SELECT id FROM object_class WHERE class_name = %s), %s
                WHERE NOT EXISTS (SELECT 1 FROM object_class WHERE class_name = %s)
                RETURNING id
            """

            cursor = conn.cursor()
            cursor.execute(query, (class_name, parent_class, classified.raw_input, class_name))
            result = cursor.fetchone()
            class_id = result[0] if result else None
            conn.commit()

            if class_id:
                # Store attributes in schema (implementation depends on schema storage)
                logger.info(f"Ingested class {class_id}: {class_name} with attributes {attributes}")

            return class_id
        finally:
            if conn:
                conn.close()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    classifier = RuleClassifier()

    # Test cases
    test_rules = [
        "What is the invoice due date from Regional Tax Office to ExampleCo GmbH?",
        "When an invoice is overdue for 30 days, escalate to management",
        "Company ExampleCo GmbH is registered in Canton Zug",
        "Invoice class has attributes: invoice_number, due_date, amount, status",
    ]

    for rule in test_rules:
        result = classifier.classify(rule)
        print(f"\nInput: {rule}")
        print(f"Type: {result.rule_type.value} (confidence: {result.confidence:.2f})")
        print(f"Rationale: {result.classification_rationale}")
        print(f"Metadata: {result.extracted_metadata}")
        print(f"Suggested Parameters: {result.suggested_parameters}")
