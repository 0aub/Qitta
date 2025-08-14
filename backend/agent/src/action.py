"""Action module (agent container).

This file mirrors ``agentic_modules/core/action.py``.  See that
module for comprehensive documentation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass
class Action:
    """Represents an executable action for an agent.

    Parameters
    ----------
    name : str
        The name used by the agent language to refer to this action.
    description : str
        A human‑readable description of what the action does.
    parameters : Dict[str, Dict[str, Any]]
        A dictionary describing the JSON schema for each argument.  Each key
        corresponds to an argument name and its value must include at
        minimum a ``type`` key.  See the OpenAI function calling docs for
        details.
    callback : Callable
        The Python function to invoke when the action is called.  It will
        receive keyword arguments corresponding to the defined parameters
        and must return a string result.  If the action ends the agent
        execution, the callback should raise ``StopIteration``.
    """

    name: str
    description: str
    parameters: Dict[str, Dict[str, Any]]
    callback: Callable[..., Any]

    def to_openai_function(self) -> Dict[str, Any]:
        """Return a JSON schema representing this action for OpenAI tools."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": {
                "type": "object",
                "properties": self.parameters,
                "required": [k for k, v in self.parameters.items() if v.get("required", True)],
            },
        }
