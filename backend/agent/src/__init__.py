"""Core components for agentic modules (agent container).

This subpackage provides the fundamental classes used to build
agentic pipelines: :class:`Action`, :class:`Agent`, :class:`Goal`,
:class:`Memory` and language interfaces.  These files mirror the
top-level ``agentic_modules/core`` so the container can work without
installing the package separately.
"""

from .action import Action
from .agent import Agent
from .agent_language import JSONActionLanguage, FunctionCallingActionLanguage
from .goal import Goal
from .memory import Memory
from .decorators import register_action

__all__ = [
    "Action",
    "Agent",
    "JSONActionLanguage",
    "FunctionCallingActionLanguage",
    "Goal",
    "Memory",
    "register_action",
]