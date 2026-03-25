"""Multi-Agent Negotiation framework for collaborative plan generation.

Implements agent-to-agent communication where BudgetAgent, ChannelAgent,
and AudienceAgent negotiate resource allocation for media plans. Each agent
proposes, counter-proposes, and converges on an optimal plan configuration.

Architecture:
    - Agent protocol defines negotiate(), propose(), accept() interface
    - NegotiationSession orchestrates rounds with configurable max iterations
    - Agents communicate via Proposal objects (immutable value types)
    - Convergence is reached when all agents accept the current proposal
    - If max rounds exhausted, the best-scoring proposal is returned

Linear: JOV-23
"""

from __future__ import annotations

import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Data Types
# ═══════════════════════════════════════════════════════════════════════════════


class ProposalStatus(Enum):
    """Status of a proposal in the negotiation lifecycle."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    COUNTER = "counter"


@dataclass(frozen=True)
class Proposal:
    """Immutable value object representing a negotiation proposal.

    Each proposal contains a budget allocation across channels and
    audience segments, plus metadata about its origin and scoring.
    """

    proposal_id: str
    agent_name: str
    round_num: int
    budget_allocation: dict[str, float]
    channel_mix: dict[str, float]
    audience_splits: dict[str, float]
    score: float = 0.0
    rationale: str = ""
    status: ProposalStatus = ProposalStatus.PENDING

    def to_dict(self) -> dict[str, Any]:
        """Serialize proposal to dictionary for API responses."""
        return {
            "proposal_id": self.proposal_id,
            "agent_name": self.agent_name,
            "round_num": self.round_num,
            "budget_allocation": self.budget_allocation,
            "channel_mix": self.channel_mix,
            "audience_splits": self.audience_splits,
            "score": self.score,
            "rationale": self.rationale,
            "status": self.status.value,
        }


@dataclass
class NegotiationContext:
    """Shared context available to all agents during negotiation.

    Contains the plan parameters, constraints, and any enrichment data
    that agents can use to inform their proposals.
    """

    total_budget: float
    job_title: str
    location: str
    industry: str
    goals: list[str] = field(default_factory=list)
    constraints: dict[str, Any] = field(default_factory=dict)
    enrichment_data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize context to dictionary."""
        return {
            "total_budget": self.total_budget,
            "job_title": self.job_title,
            "location": self.location,
            "industry": self.industry,
            "goals": self.goals,
            "constraints": self.constraints,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Agent Protocol (ABC)
# ═══════════════════════════════════════════════════════════════════════════════


class Agent(ABC):
    """Abstract base class defining the agent negotiation protocol.

    Every agent must implement three methods:
    - propose(): Generate an initial or counter-proposal
    - negotiate(): Evaluate another agent's proposal and respond
    - accept(): Decide whether to accept the current consensus
    """

    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    def propose(self, context: NegotiationContext, round_num: int) -> Proposal:
        """Generate a proposal based on the current negotiation context.

        Args:
            context: Shared negotiation context with budget and constraints.
            round_num: Current negotiation round (1-indexed).

        Returns:
            A Proposal reflecting this agent's preferred allocation.
        """
        ...

    @abstractmethod
    def negotiate(
        self, proposal: Proposal, context: NegotiationContext, round_num: int
    ) -> Proposal:
        """Evaluate a peer proposal and return acceptance or counter-proposal.

        Args:
            proposal: The proposal from another agent to evaluate.
            context: Shared negotiation context.
            round_num: Current round number.

        Returns:
            A Proposal with status ACCEPTED or COUNTER.
        """
        ...

    @abstractmethod
    def accept(self, proposal: Proposal, context: NegotiationContext) -> bool:
        """Decide whether to accept a consensus proposal.

        Args:
            proposal: The current best proposal being considered.
            context: Shared negotiation context.

        Returns:
            True if this agent accepts the proposal.
        """
        ...


# ═══════════════════════════════════════════════════════════════════════════════
# Concrete Agents
# ═══════════════════════════════════════════════════════════════════════════════


class BudgetAgent(Agent):
    """Agent specializing in budget efficiency and ROI optimization.

    Favors cost-effective channels and penalizes over-concentration
    in expensive channels. Aims to maximize reach per dollar.
    """

    def __init__(self) -> None:
        super().__init__("BudgetAgent")
        # Default cost-per-click benchmarks (USD) for scoring
        self._cpc_benchmarks: dict[str, float] = {
            "indeed": 0.35,
            "linkedin": 2.50,
            "google_ads": 1.80,
            "facebook": 0.90,
            "glassdoor": 1.20,
            "programmatic": 0.60,
            "career_fairs": 0.40,
            "referrals": 0.15,
        }

    def propose(self, context: NegotiationContext, round_num: int) -> Proposal:
        """Propose a budget-optimized allocation favoring low-CPC channels."""
        budget = context.total_budget
        # Weight channels inversely by cost -- cheaper channels get more budget
        total_inv_cpc = sum(1.0 / c for c in self._cpc_benchmarks.values())
        allocation: dict[str, float] = {}
        channel_mix: dict[str, float] = {}
        for ch, cpc in self._cpc_benchmarks.items():
            weight = (1.0 / cpc) / total_inv_cpc
            allocation[ch] = round(budget * weight, 2)
            channel_mix[ch] = round(weight * 100, 1)

        audience_splits = {
            "active_seekers": 50.0,
            "passive_talent": 35.0,
            "referrals": 15.0,
        }
        score = self._score_proposal(allocation, budget)

        return Proposal(
            proposal_id=uuid.uuid4().hex[:12],
            agent_name=self.name,
            round_num=round_num,
            budget_allocation=allocation,
            channel_mix=channel_mix,
            audience_splits=audience_splits,
            score=score,
            rationale=f"Cost-optimized allocation maximizing reach per dollar across {len(allocation)} channels",
        )

    def negotiate(
        self, proposal: Proposal, context: NegotiationContext, round_num: int
    ) -> Proposal:
        """Evaluate proposal for budget efficiency. Counter if too concentrated."""
        # Check if any single channel exceeds 40% of total budget
        total = sum(proposal.budget_allocation.values()) or 1.0
        max_share = (
            max(proposal.budget_allocation.values()) / total
            if proposal.budget_allocation
            else 0
        )

        if max_share > 0.40:
            # Counter-propose with more balanced allocation
            counter = self.propose(context, round_num)
            # Blend: 60% our proposal, 40% theirs
            blended_alloc = {}
            blended_mix = {}
            all_channels = set(counter.budget_allocation) | set(
                proposal.budget_allocation
            )
            for ch in all_channels:
                ours = counter.budget_allocation.get(ch, 0.0)
                theirs = proposal.budget_allocation.get(ch, 0.0)
                blended_alloc[ch] = round(0.6 * ours + 0.4 * theirs, 2)
                our_mix = counter.channel_mix.get(ch, 0.0)
                their_mix = proposal.channel_mix.get(ch, 0.0)
                blended_mix[ch] = round(0.6 * our_mix + 0.4 * their_mix, 1)

            return Proposal(
                proposal_id=uuid.uuid4().hex[:12],
                agent_name=self.name,
                round_num=round_num,
                budget_allocation=blended_alloc,
                channel_mix=blended_mix,
                audience_splits=proposal.audience_splits,
                score=self._score_proposal(blended_alloc, context.total_budget),
                rationale="Counter-proposal: rebalanced to avoid over-concentration in expensive channels",
                status=ProposalStatus.COUNTER,
            )

        # Accept if reasonably balanced
        return Proposal(
            proposal_id=proposal.proposal_id,
            agent_name=self.name,
            round_num=round_num,
            budget_allocation=proposal.budget_allocation,
            channel_mix=proposal.channel_mix,
            audience_splits=proposal.audience_splits,
            score=self._score_proposal(
                proposal.budget_allocation, context.total_budget
            ),
            rationale="Accepted: budget allocation is sufficiently balanced",
            status=ProposalStatus.ACCEPTED,
        )

    def accept(self, proposal: Proposal, context: NegotiationContext) -> bool:
        """Accept if no single channel exceeds 45% of budget."""
        total = sum(proposal.budget_allocation.values()) or 1.0
        max_share = (
            max(proposal.budget_allocation.values()) / total
            if proposal.budget_allocation
            else 0
        )
        return max_share <= 0.45

    def _score_proposal(
        self, allocation: dict[str, float], total_budget: float
    ) -> float:
        """Score a budget allocation based on cost efficiency (0-100)."""
        if not allocation or total_budget <= 0:
            return 0.0
        weighted_cpc = 0.0
        total_spend = sum(allocation.values()) or 1.0
        for ch, spend in allocation.items():
            cpc = self._cpc_benchmarks.get(ch, 1.50)
            weighted_cpc += (spend / total_spend) * cpc
        # Lower weighted CPC = higher score. Baseline CPC ~$1.50
        efficiency = max(0.0, min(100.0, (1.5 / max(weighted_cpc, 0.01)) * 60))
        return round(efficiency, 1)


class ChannelAgent(Agent):
    """Agent specializing in channel diversity and effectiveness.

    Ensures the plan uses a healthy mix of channels appropriate for
    the role type and industry. Penalizes single-channel strategies.
    """

    def __init__(self) -> None:
        super().__init__("ChannelAgent")
        self._channel_effectiveness: dict[str, dict[str, float]] = {
            "technology": {
                "linkedin": 0.30,
                "github_jobs": 0.15,
                "indeed": 0.15,
                "google_ads": 0.15,
                "programmatic": 0.10,
                "referrals": 0.10,
                "career_fairs": 0.05,
            },
            "healthcare": {
                "indeed": 0.25,
                "linkedin": 0.20,
                "google_ads": 0.15,
                "programmatic": 0.15,
                "career_fairs": 0.10,
                "referrals": 0.10,
                "glassdoor": 0.05,
            },
            "default": {
                "indeed": 0.25,
                "linkedin": 0.20,
                "google_ads": 0.15,
                "glassdoor": 0.10,
                "programmatic": 0.10,
                "facebook": 0.10,
                "referrals": 0.05,
                "career_fairs": 0.05,
            },
        }

    def propose(self, context: NegotiationContext, round_num: int) -> Proposal:
        """Propose channel mix optimized for the industry and role."""
        industry_key = context.industry.lower().strip()
        mix = self._channel_effectiveness.get(
            industry_key, self._channel_effectiveness["default"]
        )
        budget = context.total_budget
        allocation = {ch: round(budget * pct, 2) for ch, pct in mix.items()}
        channel_mix = {ch: round(pct * 100, 1) for ch, pct in mix.items()}

        audience_splits = {
            "active_seekers": 45.0,
            "passive_talent": 40.0,
            "referrals": 15.0,
        }
        score = self._score_diversity(channel_mix)

        return Proposal(
            proposal_id=uuid.uuid4().hex[:12],
            agent_name=self.name,
            round_num=round_num,
            budget_allocation=allocation,
            channel_mix=channel_mix,
            audience_splits=audience_splits,
            score=score,
            rationale=f"Industry-optimized {industry_key} channel mix across {len(mix)} channels",
        )

    def negotiate(
        self, proposal: Proposal, context: NegotiationContext, round_num: int
    ) -> Proposal:
        """Evaluate channel diversity. Counter if too few channels used."""
        active_channels = sum(1 for v in proposal.channel_mix.values() if v > 3.0)

        if active_channels < 4:
            # Too concentrated -- counter with our diverse mix
            counter = self.propose(context, round_num)
            # Blend 50/50
            blended_alloc = {}
            blended_mix = {}
            all_channels = set(counter.budget_allocation) | set(
                proposal.budget_allocation
            )
            for ch in all_channels:
                ours = counter.budget_allocation.get(ch, 0.0)
                theirs = proposal.budget_allocation.get(ch, 0.0)
                blended_alloc[ch] = round(0.5 * ours + 0.5 * theirs, 2)
                our_mix = counter.channel_mix.get(ch, 0.0)
                their_mix = proposal.channel_mix.get(ch, 0.0)
                blended_mix[ch] = round(0.5 * our_mix + 0.5 * their_mix, 1)

            return Proposal(
                proposal_id=uuid.uuid4().hex[:12],
                agent_name=self.name,
                round_num=round_num,
                budget_allocation=blended_alloc,
                channel_mix=blended_mix,
                audience_splits=proposal.audience_splits,
                score=self._score_diversity(blended_mix),
                rationale=f"Counter-proposal: expanded to {len(blended_mix)} channels for better diversity",
                status=ProposalStatus.COUNTER,
            )

        return Proposal(
            proposal_id=proposal.proposal_id,
            agent_name=self.name,
            round_num=round_num,
            budget_allocation=proposal.budget_allocation,
            channel_mix=proposal.channel_mix,
            audience_splits=proposal.audience_splits,
            score=self._score_diversity(proposal.channel_mix),
            rationale=f"Accepted: {active_channels} active channels provides good diversity",
            status=ProposalStatus.ACCEPTED,
        )

    def accept(self, proposal: Proposal, context: NegotiationContext) -> bool:
        """Accept if at least 4 channels with >3% share each."""
        active = sum(1 for v in proposal.channel_mix.values() if v > 3.0)
        return active >= 4

    def _score_diversity(self, channel_mix: dict[str, float]) -> float:
        """Score channel diversity using Shannon entropy (0-100)."""
        import math

        total = sum(channel_mix.values()) or 1.0
        entropy = 0.0
        for pct in channel_mix.values():
            if pct > 0:
                p = pct / total
                entropy -= p * math.log2(p)
        # Normalize: max entropy for N channels is log2(N)
        max_entropy = math.log2(max(len(channel_mix), 1)) if channel_mix else 1.0
        normalized = entropy / max_entropy if max_entropy > 0 else 0
        return round(normalized * 100, 1)


class AudienceAgent(Agent):
    """Agent specializing in audience targeting and segmentation.

    Ensures appropriate splits between active job seekers, passive
    talent, and referral pipelines based on role difficulty.
    """

    def __init__(self) -> None:
        super().__init__("AudienceAgent")

    def propose(self, context: NegotiationContext, round_num: int) -> Proposal:
        """Propose audience-optimized allocation emphasizing passive talent for hard-to-fill roles."""
        # Determine role difficulty from context
        difficulty = self._assess_difficulty(context)
        if difficulty == "hard":
            audience_splits = {
                "active_seekers": 30.0,
                "passive_talent": 50.0,
                "referrals": 20.0,
            }
        elif difficulty == "medium":
            audience_splits = {
                "active_seekers": 40.0,
                "passive_talent": 40.0,
                "referrals": 20.0,
            }
        else:
            audience_splits = {
                "active_seekers": 55.0,
                "passive_talent": 30.0,
                "referrals": 15.0,
            }

        # Allocate budget proportional to audience splits with channel defaults
        budget = context.total_budget
        allocation = {
            "indeed": round(budget * 0.20, 2),
            "linkedin": round(budget * 0.25, 2),
            "google_ads": round(budget * 0.15, 2),
            "programmatic": round(budget * 0.15, 2),
            "referrals": round(budget * (audience_splits["referrals"] / 100), 2),
            "career_fairs": round(budget * 0.05, 2),
        }
        # Distribute remainder to glassdoor
        allocated = sum(allocation.values())
        allocation["glassdoor"] = round(budget - allocated, 2)

        channel_mix = {
            ch: round((spend / budget) * 100, 1) if budget > 0 else 0.0
            for ch, spend in allocation.items()
        }

        return Proposal(
            proposal_id=uuid.uuid4().hex[:12],
            agent_name=self.name,
            round_num=round_num,
            budget_allocation=allocation,
            channel_mix=channel_mix,
            audience_splits=audience_splits,
            score=self._score_audience_fit(audience_splits, difficulty),
            rationale=f"Audience-optimized for {difficulty}-difficulty role: {audience_splits['passive_talent']}% passive talent focus",
        )

    def negotiate(
        self, proposal: Proposal, context: NegotiationContext, round_num: int
    ) -> Proposal:
        """Evaluate audience splits. Counter if passive talent is underweighted for hard roles."""
        difficulty = self._assess_difficulty(context)
        passive_pct = proposal.audience_splits.get("passive_talent", 0.0)

        needs_counter = (difficulty == "hard" and passive_pct < 35.0) or (
            difficulty == "medium" and passive_pct < 25.0
        )

        if needs_counter:
            # Counter with better audience splits but keep their channel mix
            counter = self.propose(context, round_num)
            # Blend audience splits (70% ours, 30% theirs) but keep channel alloc from theirs
            blended_audience: dict[str, float] = {}
            all_segments = set(counter.audience_splits) | set(proposal.audience_splits)
            for seg in all_segments:
                ours = counter.audience_splits.get(seg, 0.0)
                theirs = proposal.audience_splits.get(seg, 0.0)
                blended_audience[seg] = round(0.7 * ours + 0.3 * theirs, 1)

            return Proposal(
                proposal_id=uuid.uuid4().hex[:12],
                agent_name=self.name,
                round_num=round_num,
                budget_allocation=proposal.budget_allocation,
                channel_mix=proposal.channel_mix,
                audience_splits=blended_audience,
                score=self._score_audience_fit(blended_audience, difficulty),
                rationale=f"Counter: increased passive talent to {blended_audience.get('passive_talent', 0):.0f}% for {difficulty}-difficulty role",
                status=ProposalStatus.COUNTER,
            )

        return Proposal(
            proposal_id=proposal.proposal_id,
            agent_name=self.name,
            round_num=round_num,
            budget_allocation=proposal.budget_allocation,
            channel_mix=proposal.channel_mix,
            audience_splits=proposal.audience_splits,
            score=self._score_audience_fit(proposal.audience_splits, difficulty),
            rationale=f"Accepted: audience splits appropriate for {difficulty}-difficulty role",
            status=ProposalStatus.ACCEPTED,
        )

    def accept(self, proposal: Proposal, context: NegotiationContext) -> bool:
        """Accept if passive talent share is appropriate for role difficulty."""
        difficulty = self._assess_difficulty(context)
        passive = proposal.audience_splits.get("passive_talent", 0.0)
        if difficulty == "hard":
            return passive >= 30.0
        if difficulty == "medium":
            return passive >= 20.0
        return True

    def _assess_difficulty(self, context: NegotiationContext) -> str:
        """Assess hiring difficulty based on job title and industry."""
        title_lower = context.job_title.lower()
        hard_keywords = [
            "senior",
            "staff",
            "principal",
            "director",
            "vp",
            "security",
            "clearance",
            "ts/sci",
            "machine learning",
            "ai ",
            "architect",
            "surgeon",
            "anesthesiologist",
        ]
        medium_keywords = [
            "manager",
            "lead",
            "engineer",
            "developer",
            "analyst",
            "specialist",
            "scientist",
            "nurse",
            "pharmacist",
        ]
        if any(kw in title_lower for kw in hard_keywords):
            return "hard"
        if any(kw in title_lower for kw in medium_keywords):
            return "medium"
        return "easy"

    def _score_audience_fit(self, splits: dict[str, float], difficulty: str) -> float:
        """Score how well audience splits match the role difficulty (0-100)."""
        passive = splits.get("passive_talent", 0.0)
        referrals = splits.get("referrals", 0.0)
        if difficulty == "hard":
            # Hard roles: want high passive + referrals
            return round(min(100.0, passive * 1.5 + referrals * 1.0), 1)
        if difficulty == "medium":
            # Medium: balanced approach scores best
            balance = 100.0 - abs(splits.get("active_seekers", 0) - passive) * 0.8
            return round(max(0.0, balance), 1)
        # Easy: active seekers dominate, that is fine
        active = splits.get("active_seekers", 0.0)
        return round(min(100.0, active + referrals * 0.5), 1)


# ═══════════════════════════════════════════════════════════════════════════════
# Negotiation Session
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class NegotiationResult:
    """Result of a completed negotiation session."""

    converged: bool
    rounds_taken: int
    final_proposal: Proposal
    all_proposals: list[dict[str, Any]]
    agent_scores: dict[str, float]
    duration_ms: float

    def to_dict(self) -> dict[str, Any]:
        """Serialize result for API responses."""
        return {
            "converged": self.converged,
            "rounds_taken": self.rounds_taken,
            "final_proposal": self.final_proposal.to_dict(),
            "all_proposals": self.all_proposals,
            "agent_scores": self.agent_scores,
            "duration_ms": self.duration_ms,
        }


def run_negotiation(
    context: NegotiationContext,
    max_rounds: int = 5,
    agents: list[Agent] | None = None,
) -> NegotiationResult:
    """Run a multi-agent negotiation session to convergence.

    The negotiation loop:
    1. First agent proposes an initial allocation
    2. Each subsequent agent evaluates and may counter-propose
    3. If all agents accept, convergence is reached
    4. Otherwise, the best-scoring counter-proposal becomes the basis
       for the next round
    5. After max_rounds, the highest-scoring proposal is returned

    Args:
        context: The negotiation context with budget, role, and constraints.
        max_rounds: Maximum negotiation rounds before forced convergence.
        agents: Optional list of agents. Defaults to Budget+Channel+Audience.

    Returns:
        NegotiationResult with the final agreed-upon proposal.
    """
    start_time = time.time()

    if agents is None:
        agents = [BudgetAgent(), ChannelAgent(), AudienceAgent()]

    if not agents:
        raise ValueError("At least one agent is required for negotiation")

    if max_rounds < 1 or max_rounds > 20:
        raise ValueError("max_rounds must be between 1 and 20")

    all_proposals: list[dict[str, Any]] = []
    best_proposal: Proposal | None = None
    best_score: float = -1.0

    for round_num in range(1, max_rounds + 1):
        logger.info(f"Negotiation round {round_num}/{max_rounds}")

        # First agent proposes (or uses previous best as basis)
        if best_proposal is None:
            current = agents[0].propose(context, round_num)
        else:
            current = best_proposal

        round_accepted = True

        # Each agent evaluates the current proposal
        for agent in agents:
            response = agent.negotiate(current, context, round_num)
            all_proposals.append(
                {
                    "round": round_num,
                    "agent": agent.name,
                    "action": response.status.value,
                    "score": response.score,
                    "rationale": response.rationale,
                }
            )

            if response.status == ProposalStatus.COUNTER:
                round_accepted = False
                # Track best counter-proposal
                if response.score > best_score:
                    best_score = response.score
                    best_proposal = response
            elif response.status == ProposalStatus.ACCEPTED:
                if response.score > best_score:
                    best_score = response.score
                    best_proposal = response

        # Check if all agents accept the current best
        if round_accepted and best_proposal is not None:
            all_accept = all(agent.accept(best_proposal, context) for agent in agents)
            if all_accept:
                duration_ms = round((time.time() - start_time) * 1000, 1)
                logger.info(
                    f"Negotiation converged in {round_num} rounds ({duration_ms}ms)"
                )
                return NegotiationResult(
                    converged=True,
                    rounds_taken=round_num,
                    final_proposal=best_proposal,
                    all_proposals=all_proposals,
                    agent_scores={a.name: best_proposal.score for a in agents},
                    duration_ms=duration_ms,
                )

    # Max rounds exhausted -- return best proposal
    duration_ms = round((time.time() - start_time) * 1000, 1)
    if best_proposal is None:
        # Fallback: use first agent's proposal
        best_proposal = agents[0].propose(context, max_rounds)

    logger.info(
        f"Negotiation completed after {max_rounds} rounds without full convergence ({duration_ms}ms)"
    )
    return NegotiationResult(
        converged=False,
        rounds_taken=max_rounds,
        final_proposal=best_proposal,
        all_proposals=all_proposals,
        agent_scores={a.name: best_proposal.score for a in agents},
        duration_ms=duration_ms,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# API Handler (called from app.py)
# ═══════════════════════════════════════════════════════════════════════════════


def handle_negotiate_request(body: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Handle a POST /api/plan/negotiate request.

    Expected body:
        {
            "budget": 50000,
            "job_title": "Senior Software Engineer",
            "location": "San Francisco, CA",
            "industry": "Technology",
            "goals": ["reduce_time_to_fill", "increase_quality"],
            "max_rounds": 5
        }

    Args:
        body: Parsed JSON request body.

    Returns:
        Tuple of (response_dict, status_code).
    """
    # Validate required fields
    budget = body.get("budget")
    if budget is None:
        return {"error": "budget is required"}, 400
    try:
        budget = float(budget)
    except (ValueError, TypeError):
        return {"error": "budget must be a number"}, 400
    if budget <= 0:
        return {"error": "budget must be positive"}, 400

    job_title = body.get("job_title") or ""
    if not job_title.strip():
        return {"error": "job_title is required"}, 400

    location = body.get("location") or ""
    industry = body.get("industry") or "general"
    goals = body.get("goals") or []
    if not isinstance(goals, list):
        return {"error": "goals must be a list"}, 400

    max_rounds = body.get("max_rounds", 5)
    try:
        max_rounds = int(max_rounds)
    except (ValueError, TypeError):
        max_rounds = 5
    max_rounds = max(1, min(max_rounds, 20))

    context = NegotiationContext(
        total_budget=budget,
        job_title=job_title.strip(),
        location=location.strip(),
        industry=industry.strip(),
        goals=goals,
    )

    try:
        result = run_negotiation(context, max_rounds=max_rounds)
        return {
            "negotiation": result.to_dict(),
            "context": context.to_dict(),
        }, 200
    except ValueError as ve:
        return {"error": str(ve)}, 400
    except Exception as e:
        logger.error(f"Negotiation failed: {e}", exc_info=True)
        return {"error": f"Negotiation failed: {e}"}, 500
