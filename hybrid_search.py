#!/usr/bin/env python3
"""Hybrid vector search utilities for IDMS-Demo.

Combines sparse (BM25-style token frequency) and dense (semantic) vectors
for improved search accuracy on keyword-heavy and semantic queries.
"""

import hashlib
import re
from typing import Any


def encode_sparse_vector(text: str) -> tuple[list[int], list[float]]:
    """
    Encode text as sparse vector using MD5-hashed token indices and frequency.
    
    BM25-like algorithm:
    - Tokenize text (alphanumeric, dashes, dots, slashes)
    - Hash each token to a stable integer index
    - Count token frequency (inverse document length normalization optional)
    - Return (indices, values) for Qdrant sparse vector format
    """
    if not text or not isinstance(text, str):
        return [], []
    
    # Tokenize: lowercase, alphanumeric + special chars commonly found in business docs
    tokens = re.findall(r"[\w\-/.:]+", text.lower())
    
    if not tokens:
        return [], []
    
    # Build frequency map using stable token indices
    freq_map: dict[int, float] = {}
    for token in tokens:
        # Use MD5 hash to create stable, deterministic indices
        token_hash = hashlib.md5(token.encode("utf-8")).hexdigest()
        # Take first 8 hex chars (32 bits) to get ~4B index space
        token_idx = int(token_hash[:8], 16)
        freq_map[token_idx] = freq_map.get(token_idx, 0.0) + 1.0
    
    # Sort by index for consistency
    sorted_items = sorted(freq_map.items())
    indices = [idx for idx, _ in sorted_items]
    values = [freq for _, freq in sorted_items]
    
    return indices, values


def normalize_sparse_values(indices: list[int], values: list[float]) -> tuple[list[int], list[float]]:
    """
    Normalize sparse vector values using L2 norm.
    
    Ensures fair scoring across different document lengths.
    """
    if not values:
        return indices, values
    
    # Compute L2 norm
    norm = sum(v * v for v in values) ** 0.5
    
    if norm == 0:
        return indices, values
    
    # Normalize
    normalized_values = [v / norm for v in values]
    return indices, normalized_values


def sparse_cosine_similarity(
    indices1: list[int],
    values1: list[float],
    indices2: list[int],
    values2: list[float],
) -> float:
    """
    Compute cosine similarity between two sparse vectors.
    
    Sparse vectors represented as (indices, values) tuples.
    Uses coordinate-wise multiplication for efficiency.
    """
    if not indices1 or not indices2:
        return 0.0
    
    # Create dict lookup for second vector
    v2_map = dict(zip(indices2, values2))
    
    # Compute dot product (only shared indices contribute)
    dot_product = 0.0
    for idx, val in zip(indices1, values1):
        if idx in v2_map:
            dot_product += val * v2_map[idx]
    
    # Compute norms
    norm1 = sum(v * v for v in values1) ** 0.5
    norm2 = sum(v * v for v in values2) ** 0.5
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return dot_product / (norm1 * norm2)


def merge_hybrid_results(
    sparse_results: list[dict[str, Any]],
    dense_results: list[dict[str, Any]],
    sparse_weight: float = 0.3,
    dense_weight: float = 0.7,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Merge sparse and dense search results using weighted scoring.
    
    Args:
        sparse_results: Results from sparse (BM25) search with 'id', 'score', 'payload'
        dense_results: Results from dense (semantic) search with 'id', 'score', 'payload'
        sparse_weight: Weight for sparse search scores (keyword/exact matching)
        dense_weight: Weight for dense search scores (semantic similarity)
        limit: Maximum number of results to return
    
    Returns:
        Merged and sorted results by combined score
    """
    # Normalize weights
    total = sparse_weight + dense_weight
    sparse_weight = sparse_weight / total
    dense_weight = dense_weight / total
    
    # Build score maps
    score_map: dict[str, dict[str, Any]] = {}
    
    # Add sparse results
    for result in sparse_results or []:
        result_id = str(result.get("id", ""))
        if result_id not in score_map:
            score_map[result_id] = {
                "id": result_id,
                "payload": result.get("payload", {}),
                "sparse_score": 0.0,
                "dense_score": 0.0,
            }
        score_map[result_id]["sparse_score"] = float(result.get("score", 0.0))
    
    # Add dense results
    for result in dense_results or []:
        result_id = str(result.get("id", ""))
        if result_id not in score_map:
            score_map[result_id] = {
                "id": result_id,
                "payload": result.get("payload", {}),
                "sparse_score": 0.0,
                "dense_score": 0.0,
            }
        else:
            # Prefer payload from dense results (more reliable)
            score_map[result_id]["payload"] = result.get("payload", score_map[result_id]["payload"])
        score_map[result_id]["dense_score"] = float(result.get("score", 0.0))
    
    # Compute combined scores
    merged = []
    for result_id, scores in score_map.items():
        combined_score = (
            scores["sparse_score"] * sparse_weight +
            scores["dense_score"] * dense_weight
        )
        merged.append({
            "id": scores["id"],
            "score": combined_score,
            "sparse_score": scores["sparse_score"],
            "dense_score": scores["dense_score"],
            "payload": scores["payload"],
        })
    
    # Sort by combined score descending
    merged.sort(key=lambda x: x["score"], reverse=True)
    
    # Return top-k
    return merged[:limit]


def should_use_hybrid_search(query_text: str) -> bool:
    """
    Heuristic to decide if hybrid search would be beneficial.
    
    Returns True for keyword-heavy or exact-match queries.
    Returns False for pure semantic queries.
    """
    if not query_text:
        return False
    
    # Exact match patterns: identifiers, dates, specific values
    if re.search(r"(CHE-|DE|FR|IT|AT)\d+", query_text):  # Country/ID codes
        return True
    if re.search(r"\d{2,4}[.-]\d{2,4}[.-]\d{2,4}", query_text):  # Dates
        return True
    if re.search(r"[A-Z]{2,3}\s*\d+", query_text):  # Codes
        return True
    
    # Multiple quoted phrases: keyword search
    if query_text.count('"') >= 2:
        return True
    
    # Keywords for structured data: attribute names, field names
    keyword_patterns = [
        r"\b(eori|vat|registration|number|address|email|phone|date)\b",
        r"\b(name|surname|birth|place|nationality|company|organization)\b",
    ]
    if any(re.search(p, query_text, re.IGNORECASE) for p in keyword_patterns):
        return True
    
    # If query is very short (likely keyword search)
    if len(query_text.split()) <= 3:
        return True
    
    return False
