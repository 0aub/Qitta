"""Language interfaces (agent container).

This file mirrors ``agentic_modules/core/agent_language.py``.  It
provides the JSON and function calling protocols for interacting with
language models via the ``litellm`` package.  See the original module
for full documentation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import litellm  # type: ignore

from .action import Action


class JSONActionLanguage:
    """Simple protocol that instructs the model to output JSON.

    The prompt templates used by this language ask the model to pick an
    available tool and return a JSON object with two keys: ``tool_name`` and
    ``args``.  ``tool_name`` must match one of the registered actions and
    ``args`` must be a JSON object of named arguments matching the
    action's schema.  If the model wants to end the conversation it
    should set ``tool_name`` to ``finish`` and include a ``response``
    field in ``args``.  No other keys should be included.
    """

    def __init__(self, model: str, system_prompt: Optional[str] = None, debug: bool = False, **model_kwargs: Any) -> None:
        self.model = model
        self.system_prompt = system_prompt or (
            "You are an autonomous agent.  You must decide which tool to call based on the user's request. "
            "When calling a tool you must respond with a JSON object with the following keys:\n"
            "tool_name: the name of the tool you are calling, chosen from the provided tools.\n"
            "args: a JSON object of arguments to that tool.\n"
            "If no further actions are necessary, respond with tool_name=finish and args={'response': '<final answer>'}. "
            "Do not include any other keys."
        )
        # Additional kwargs forwarded to litellm.completion
        self.model_kwargs: Dict[str, Any] = dict(model_kwargs)
        self.actions: Dict[str, Action] = {}
        self.debug = debug

    def register_action(self, action: Action) -> None:
        """Register an action for this language."""
        self.actions[action.name] = action

    def _build_prompt(self, goal_prompt: str, memory_messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Construct the chat messages to send to the model."""
        messages: List[Dict[str, str]] = []
        # System prompt describing the protocol
        messages.append({"role": "system", "content": self.system_prompt})
        # Include goal and available tools description
        tool_descriptions = []
        for action in self.actions.values():
            params_desc = ", ".join(
                [f"{name}: {param.get('type', 'string')}" for name, param in action.parameters.items()]
            )
            tool_descriptions.append(f"{action.name}: {action.description} (params: {params_desc})")
        tools_str = "\n".join(tool_descriptions)
        messages.append({"role": "system", "content": f"Available tools:\n{tools_str}"})
        # Include the goal description
        messages.append({"role": "system", "content": goal_prompt})
        # Append previous conversation history
        for msg in memory_messages:
            messages.append(msg)
        return messages

    def generate_response(self, goal: str, memory_messages: List[Dict[str, str]]) -> str:
        """Generate a response from the language model.

        This method builds the prompt using the goal description and
        conversation history, then calls ``litellm.completion`` with the
        configured model.  It returns the assistant's message content as a
        string.  If ``debug`` is True, the prompt and raw response are
        printed to the console.
        """
        prompt_messages = self._build_prompt(goal, memory_messages)
        if self.debug:
            print("\n[DEBUG] Prompt messages:")
            for msg in prompt_messages:
                truncated = msg["content"][:200]
                print(f"{msg['role']}: {truncated}{'...' if len(msg['content'])>200 else ''}")
        # Always request JSON response format if supported
        kwargs = {
            **self.model_kwargs,
            "response_format": {"type": "json_object"},
        }
        response = litellm.completion(
            model=self.model,
            messages=prompt_messages,
            **kwargs,
        )
        if self.debug:
            print(f"[DEBUG] Raw response: {response}")
        # Extract content from response in various formats
        content: Optional[str] = None
        if hasattr(response, "choices"):
            choices = getattr(response, "choices", [])
            if not choices:
                raise RuntimeError(f"Language model returned no choices. Raw response: {response}")
            choice = choices[0]
            if hasattr(choice, "message"):
                content = getattr(choice.message, "content", None)
            elif isinstance(choice, dict):
                content = choice.get("message", {}).get("content")
        elif isinstance(response, dict):
            choices = response.get("choices")
            if choices:
                msg = choices[0].get("message")
                if msg:
                    content = msg.get("content")
        elif isinstance(response, str):
            content = response
        if content is None:
            content = str(response)
        return content

    def parse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse the assistant's JSON response into a dict.

        The expected format is a JSON object with keys ``tool_name`` and
        ``args``.  If parsing fails or the format is invalid, a
        ``ValueError`` is raised.
        """
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Could not parse JSON response: {e}\nResponse text: {response_text}") from e
        if not isinstance(data, dict) or "tool_name" not in data:
            raise ValueError(f"Response must be an object with 'tool_name' and 'args'. Got: {data}")
        # ensure args is a dict
        args = data.get("args")
        if args is None:
            args = {}
        elif not isinstance(args, dict):
            raise ValueError(f"The 'args' field must be a JSON object. Got: {args}")
        return {"tool_name": data["tool_name"], "args": args}


class FunctionCallingActionLanguage(JSONActionLanguage):
    """An extension of the JSON language that uses the OpenAI function calling API.

    This protocol registers the available actions as functions and passes
    them to ``litellm.completion`` via the ``tools`` parameter.  The
    model is instructed to call one of the provided functions and the
    returned function call is parsed into an action invocation.
    """

    def generate_response(self, goal: str, memory_messages: List[Dict[str, str]]) -> Any:
        # build messages as usual (system messages, goal, history)
        messages = self._build_prompt(goal, memory_messages)
        # register functions in the required schema
        tools = [action.to_openai_function() for action in self.actions.values()]
        response = litellm.completion(
            model=self.model,
            messages=messages,
            tools=tools,
            tool_choice={"type": "function", "function": {"name": "auto"}},
            **self.model_kwargs,
        )
        # parse the function call from the first choice
        invocation: Optional[Dict[str, Any]] = None
        if hasattr(response, "choices") and response.choices:
            choice = response.choices[0]
            # In OpenAI style responses the function call is under tool_calls
            if hasattr(choice, "message"):
                message = choice.message
                if hasattr(message, "tool_calls") and message.tool_calls:
                    tc = message.tool_calls[0]
                    invocation = {"tool_name": tc.function.name, "args": json.loads(tc.function.arguments or "{}")}
                elif hasattr(message, "function_call") and message.function_call:
                    fc = message.function_call
                    invocation = {"tool_name": fc.name, "args": json.loads(fc.arguments or "{}")}
        # fallback: treat as JSON response
        if invocation is None:
            content = super().generate_response(goal, memory_messages)
            invocation = super().parse_response(content)
        return invocation

    def parse_response(self, response_text: str) -> Dict[str, Any]:  # pragma: no cover
        # Not used in function calling; generate_response returns invocation directly
        raise NotImplementedError
