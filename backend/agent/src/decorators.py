"""Decorator helpers (agent container).

This file mirrors ``agentic_modules/core/decorators.py`` and provides
the ``register_action`` decorator for turning plain functions into
:class:`Action` instances.
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict, Optional

from .action import Action


def register_action(name: Optional[str] = None, description: Optional[str] = None, parameters: Optional[Dict[str, Dict[str, Any]]] = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to declare a function as an agent action.

    See the top-level ``agentic_modules.core.decorators.register_action`` for
    documentation.  This implementation is identical and provided here to
    keep the agent container self-contained.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        action_name = name or func.__name__
        action_desc = description or (func.__doc__ or "No description provided.").strip()
        # If parameters are provided explicitly, use them; otherwise infer
        params_schema: Dict[str, Dict[str, Any]] = {}
        if parameters:
            params_schema = parameters
        else:
            import inspect

            sig = inspect.signature(func)
            for param in sig.parameters.values():
                if param.default is inspect.Parameter.empty and param.annotation is inspect._empty:
                    param_type = "string"
                else:
                    annotation = param.annotation
                    if annotation in (int, float):
                        param_type = "number"
                    elif annotation is bool:
                        param_type = "boolean"
                    else:
                        param_type = "string"
                params_schema[param.name] = {"type": param_type, "required": param.default is inspect.Parameter.empty}
        action = Action(name=action_name, description=action_desc, parameters=params_schema, callback=func)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        wrapper.action = action  # type: ignore[attr-defined]
        return wrapper

    return decorator
