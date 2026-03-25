"""Embedding-Based Role Taxonomy for Nova AI Suite.

A learned semantic space for job roles. Understands that 'ML Engineer' is closer
to 'AI Research Scientist' than 'Software Engineer' in the talent market.
Replaces keyword-based role matching with vector similarity.
"""

import json
import logging
import math
import os
import threading
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# Pre-computed role embeddings (simplified TF-IDF-like vectors)
# In production, these would come from a trained model or embedding API
# Each role has a 20-dimensional vector representing its position in talent space
# Dimensions roughly correspond to: [technical, medical, sales, creative, analytical,
#   leadership, operations, finance, legal, hr, manual, research, customer-facing,
#   data, design, strategy, education, engineering, science, communication]

ROLE_EMBEDDINGS: Dict[str, List[float]] = {
    # Engineering
    "software engineer": [
        0.9,
        0.0,
        0.0,
        0.1,
        0.3,
        0.1,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.2,
        0.1,
        0.3,
        0.1,
        0.1,
        0.0,
        0.9,
        0.1,
        0.1,
    ],
    "frontend developer": [
        0.9,
        0.0,
        0.0,
        0.4,
        0.2,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.2,
        0.1,
        0.5,
        0.0,
        0.0,
        0.8,
        0.0,
        0.1,
    ],
    "backend developer": [
        0.95,
        0.0,
        0.0,
        0.0,
        0.3,
        0.0,
        0.1,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.0,
        0.4,
        0.0,
        0.1,
        0.0,
        0.9,
        0.1,
        0.0,
    ],
    "devops engineer": [
        0.9,
        0.0,
        0.0,
        0.0,
        0.2,
        0.1,
        0.4,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.0,
        0.2,
        0.0,
        0.1,
        0.0,
        0.85,
        0.0,
        0.1,
    ],
    "mobile developer": [
        0.85,
        0.0,
        0.0,
        0.3,
        0.2,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.3,
        0.1,
        0.4,
        0.0,
        0.0,
        0.8,
        0.0,
        0.1,
    ],
    "qa engineer": [
        0.7,
        0.0,
        0.0,
        0.0,
        0.5,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.1,
        0.2,
        0.0,
        0.1,
        0.0,
        0.7,
        0.0,
        0.2,
    ],
    "full stack developer": [
        0.9,
        0.0,
        0.0,
        0.2,
        0.3,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.2,
        0.3,
        0.3,
        0.1,
        0.0,
        0.9,
        0.0,
        0.1,
    ],
    # Data & AI
    "data scientist": [
        0.7,
        0.0,
        0.0,
        0.1,
        0.9,
        0.1,
        0.0,
        0.1,
        0.0,
        0.0,
        0.0,
        0.7,
        0.1,
        0.9,
        0.1,
        0.3,
        0.1,
        0.5,
        0.5,
        0.2,
    ],
    "ml engineer": [
        0.85,
        0.0,
        0.0,
        0.0,
        0.7,
        0.1,
        0.1,
        0.0,
        0.0,
        0.0,
        0.0,
        0.8,
        0.0,
        0.8,
        0.0,
        0.2,
        0.0,
        0.8,
        0.6,
        0.1,
    ],
    "ai research scientist": [
        0.6,
        0.0,
        0.0,
        0.0,
        0.8,
        0.1,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.95,
        0.0,
        0.7,
        0.0,
        0.2,
        0.2,
        0.5,
        0.9,
        0.3,
    ],
    "data analyst": [
        0.4,
        0.0,
        0.1,
        0.1,
        0.8,
        0.0,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.3,
        0.2,
        0.9,
        0.1,
        0.2,
        0.0,
        0.2,
        0.1,
        0.3,
    ],
    "data engineer": [
        0.8,
        0.0,
        0.0,
        0.0,
        0.5,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.0,
        0.2,
        0.0,
        0.9,
        0.0,
        0.1,
        0.0,
        0.8,
        0.2,
        0.1,
    ],
    # Healthcare
    "registered nurse": [
        0.0,
        0.9,
        0.0,
        0.0,
        0.1,
        0.1,
        0.1,
        0.0,
        0.0,
        0.0,
        0.3,
        0.0,
        0.8,
        0.0,
        0.0,
        0.0,
        0.1,
        0.0,
        0.3,
        0.4,
    ],
    "physician": [
        0.0,
        0.95,
        0.0,
        0.0,
        0.3,
        0.3,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.5,
        0.7,
        0.0,
        0.0,
        0.2,
        0.2,
        0.0,
        0.6,
        0.3,
    ],
    "pharmacist": [
        0.0,
        0.85,
        0.0,
        0.0,
        0.4,
        0.1,
        0.1,
        0.0,
        0.1,
        0.0,
        0.1,
        0.3,
        0.6,
        0.1,
        0.0,
        0.1,
        0.1,
        0.0,
        0.5,
        0.3,
    ],
    "medical technologist": [
        0.3,
        0.7,
        0.0,
        0.0,
        0.4,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.2,
        0.3,
        0.3,
        0.2,
        0.0,
        0.0,
        0.0,
        0.3,
        0.4,
        0.1,
    ],
    # Sales
    "account executive": [
        0.0,
        0.0,
        0.9,
        0.1,
        0.2,
        0.2,
        0.0,
        0.1,
        0.0,
        0.0,
        0.0,
        0.0,
        0.9,
        0.1,
        0.0,
        0.3,
        0.0,
        0.0,
        0.0,
        0.7,
    ],
    "sales manager": [
        0.0,
        0.0,
        0.85,
        0.1,
        0.2,
        0.7,
        0.1,
        0.1,
        0.0,
        0.1,
        0.0,
        0.0,
        0.8,
        0.1,
        0.0,
        0.5,
        0.0,
        0.0,
        0.0,
        0.6,
    ],
    "business development": [
        0.0,
        0.0,
        0.7,
        0.1,
        0.3,
        0.3,
        0.0,
        0.1,
        0.0,
        0.0,
        0.0,
        0.1,
        0.7,
        0.1,
        0.0,
        0.6,
        0.0,
        0.0,
        0.0,
        0.6,
    ],
    # Marketing
    "product marketing manager": [
        0.1,
        0.0,
        0.3,
        0.5,
        0.4,
        0.3,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.2,
        0.4,
        0.3,
        0.2,
        0.6,
        0.0,
        0.0,
        0.0,
        0.7,
    ],
    "content marketing": [
        0.0,
        0.0,
        0.2,
        0.8,
        0.2,
        0.1,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.3,
        0.1,
        0.3,
        0.3,
        0.1,
        0.0,
        0.0,
        0.9,
    ],
    "seo specialist": [
        0.4,
        0.0,
        0.2,
        0.3,
        0.5,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.2,
        0.2,
        0.5,
        0.1,
        0.2,
        0.0,
        0.2,
        0.0,
        0.4,
    ],
    # Executive
    "cto": [
        0.7,
        0.0,
        0.1,
        0.0,
        0.3,
        0.95,
        0.2,
        0.1,
        0.0,
        0.0,
        0.0,
        0.2,
        0.2,
        0.2,
        0.1,
        0.9,
        0.0,
        0.6,
        0.2,
        0.4,
    ],
    "cfo": [
        0.0,
        0.0,
        0.1,
        0.0,
        0.5,
        0.95,
        0.2,
        0.9,
        0.2,
        0.0,
        0.0,
        0.1,
        0.1,
        0.3,
        0.0,
        0.9,
        0.0,
        0.0,
        0.0,
        0.3,
    ],
    "vp engineering": [
        0.7,
        0.0,
        0.0,
        0.0,
        0.2,
        0.9,
        0.2,
        0.1,
        0.0,
        0.0,
        0.0,
        0.1,
        0.2,
        0.1,
        0.1,
        0.8,
        0.0,
        0.7,
        0.1,
        0.4,
    ],
    # Operations
    "warehouse manager": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
        0.5,
        0.9,
        0.1,
        0.0,
        0.0,
        0.5,
        0.0,
        0.3,
        0.1,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.3,
    ],
    "logistics coordinator": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.2,
        0.1,
        0.9,
        0.1,
        0.0,
        0.0,
        0.3,
        0.0,
        0.4,
        0.2,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.4,
    ],
    "truck driver": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.7,
        0.0,
        0.0,
        0.0,
        0.8,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.1,
    ],
    # Finance
    "financial analyst": [
        0.2,
        0.0,
        0.1,
        0.0,
        0.8,
        0.1,
        0.0,
        0.9,
        0.1,
        0.0,
        0.0,
        0.3,
        0.1,
        0.7,
        0.0,
        0.3,
        0.0,
        0.1,
        0.1,
        0.3,
    ],
    "accountant": [
        0.1,
        0.0,
        0.0,
        0.0,
        0.5,
        0.0,
        0.1,
        0.9,
        0.2,
        0.0,
        0.0,
        0.1,
        0.1,
        0.3,
        0.0,
        0.1,
        0.0,
        0.0,
        0.0,
        0.2,
    ],
}


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors.

    Args:
        a: First vector.
        b: Second vector.

    Returns:
        Cosine similarity score between 0 and 1.
    """
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class RoleTaxonomy:
    """Semantic role taxonomy using embedding-based similarity."""

    def __init__(self) -> None:
        """Initialize the taxonomy with pre-computed embeddings."""
        self._embeddings: Dict[str, List[float]] = dict(ROLE_EMBEDDINGS)
        self._lock = threading.Lock()

    def find_similar_roles(self, query: str, top_k: int = 5) -> List[Tuple[str, float]]:
        """Find the most similar roles to a query.

        Args:
            query: Job title to search for.
            top_k: Number of similar roles to return.

        Returns:
            List of (role_name, similarity_score) tuples, sorted by similarity.
        """
        query_lower = query.lower().strip()

        # Exact match check
        if query_lower in self._embeddings:
            query_vec = self._embeddings[query_lower]
        else:
            # Find best partial match and use its embedding as seed
            query_vec = self._infer_embedding(query_lower)

        similarities: List[Tuple[str, float]] = []
        for role, vec in self._embeddings.items():
            if role == query_lower:
                continue
            sim = _cosine_similarity(query_vec, vec)
            similarities.append((role, round(sim, 4)))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

    def _infer_embedding(self, query: str) -> List[float]:
        """Infer an embedding for an unknown role by averaging similar known roles.

        Args:
            query: Lowercase job title not found in the embedding table.

        Returns:
            Best-guess 20-dimensional embedding vector.
        """
        best_score = 0.0
        best_vec: List[float] = [0.0] * 20

        for role, vec in self._embeddings.items():
            # Simple keyword overlap scoring
            query_words = set(query.split())
            role_words = set(role.split())
            overlap = len(query_words & role_words)
            if overlap > best_score:
                best_score = overlap
                best_vec = vec
            # Also check substring match
            elif any(w in role for w in query_words) or any(
                w in query for w in role_words
            ):
                score = 0.5
                if score > best_score:
                    best_score = score
                    best_vec = vec

        return best_vec if best_score > 0 else [0.5] * 20

    def get_role_cluster(self, role: str) -> Dict[str, Any]:
        """Get the role's cluster and nearest neighbors.

        Args:
            role: Job title.

        Returns:
            Dict with cluster info, similar roles, and recruitment insights.
        """
        similar = self.find_similar_roles(role, top_k=8)

        # Determine primary cluster by looking at top similar roles
        cluster_scores: Dict[str, float] = {}
        clusters: Dict[str, List[str]] = {
            "engineering": [
                "software engineer",
                "frontend developer",
                "backend developer",
                "devops engineer",
                "full stack developer",
            ],
            "data_science": [
                "data scientist",
                "ml engineer",
                "ai research scientist",
                "data analyst",
                "data engineer",
            ],
            "healthcare": [
                "registered nurse",
                "physician",
                "pharmacist",
                "medical technologist",
            ],
            "sales": ["account executive", "sales manager", "business development"],
            "marketing": [
                "product marketing manager",
                "content marketing",
                "seo specialist",
            ],
            "executive": ["cto", "cfo", "vp engineering"],
            "operations": [
                "warehouse manager",
                "logistics coordinator",
                "truck driver",
            ],
            "finance": ["financial analyst", "accountant"],
        }

        for cluster_name, members in clusters.items():
            for sim_role, sim_score in similar:
                if sim_role in members:
                    cluster_scores[cluster_name] = (
                        cluster_scores.get(cluster_name, 0) + sim_score
                    )

        primary_cluster = (
            max(cluster_scores, key=cluster_scores.get) if cluster_scores else "general"
        )

        return {
            "role": role,
            "primary_cluster": primary_cluster,
            "cluster_confidence": round(
                cluster_scores.get(primary_cluster, 0) / max(len(similar), 1), 3
            ),
            "similar_roles": [{"role": r, "similarity": s} for r, s in similar[:5]],
            "talent_pool_overlap": [r for r, s in similar if s > 0.85],
        }

    def get_taxonomy_stats(self) -> Dict[str, Any]:
        """Return taxonomy stats for /api/health.

        Returns:
            Dict with total_roles, dimensions, and clusters count.
        """
        return {
            "total_roles": len(self._embeddings),
            "dimensions": 20,
            "clusters": 8,
        }


# Global singleton
_taxonomy = RoleTaxonomy()


def get_role_taxonomy() -> RoleTaxonomy:
    """Get the global role taxonomy instance.

    Returns:
        The singleton RoleTaxonomy instance.
    """
    return _taxonomy
