"""Goal representation (agent container).

This file mirrors ``agentic_modules/core/goal.py``.  It defines the
:class:`Goal` class used to describe what an agent should accomplish.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Goal:
    """Represents a goal or sub‑goal for an agent.

    Attributes
    ----------
    name: str
        A short identifier for the goal.
    description: str
        A natural language description of what needs to be achieved.
    sub_goals: Optional[List[Goal]]
        A list of sub‑goals that further refine this goal.
    """

    name: str
    description: str
    sub_goals: Optional[List["Goal"]] = field(default_factory=list)

    def to_prompt(self) -> str:
        parts: List[str] = [f"Goal: {self.name}\n{self.description}\n"]
        if self.sub_goals:
            for i, sg in enumerate(self.sub_goals, 1):
                parts.append(f"Sub‑goal {i}: {sg.name}\n{sg.description}\n")
        return "\n".join(parts)
