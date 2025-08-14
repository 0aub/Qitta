"""Conversation memory (agent container).

This file mirrors ``agentic_modules/core/memory.py``.  It defines the
:class:`Memory` class used to store chat history for agents.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Memory:
    """Conversation memory for an agent.

    Stores chat messages as a list of dictionaries with keys ``role`` and
    ``content``.  Roles can be "system", "user", "assistant" or
    "tool".  You may also store additional metadata such as the name
    of the tool used.
    """

    messages: List[Dict[str, str]] = field(default_factory=list)

    def add_message(self, role: str, content: str, name: Optional[str] = None) -> None:
        msg: Dict[str, str] = {"role": role, "content": content}
        if name:
            msg["name"] = name
        self.messages.append(msg)

    def get_history(self) -> List[Dict[str, str]]:
        return list(self.messages)

    def clear(self) -> None:
        self.messages.clear()
