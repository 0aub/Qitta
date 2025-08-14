"""Agent orchestrator (agent container).

This file mirrors ``agentic_modules/core/agent.py``.  See that
module for comprehensive documentation.
"""

from __future__ import annotations

from typing import Any, Dict
import traceback

from .goal import Goal
from .memory import Memory
from .action import Action
from .agent_language import JSONActionLanguage, FunctionCallingActionLanguage


class Agent:
    """An autonomous agent that uses a language model to decide which tools to call.

    Parameters
    ----------
    goal : Goal
        The goal the agent is working towards.
    agent_language : JSONActionLanguage
        The language interface used to communicate with the LLM.
    memory : Memory, optional
        Conversation memory.  If omitted a new :class:`Memory` is created.
    debug : bool, optional
        When True the agent prints detailed logs of prompts, responses,
        parsed tool invocations and action results.
    """

    def __init__(self, goal: Goal, agent_language: JSONActionLanguage, memory: Memory | None = None, debug: bool = False) -> None:
        self.goal = goal
        self.agent_language = agent_language
        self.memory = memory or Memory()
        self.actions: Dict[str, Action] = {}
        self.debug = debug

    def register_action(self, action: Action) -> None:
        """Register a new action with the agent and its language."""
        self.actions[action.name] = action
        self.agent_language.register_action(action)

    def run(self, user_input: str) -> str:
        """Run the agent loop until it returns a final answer.

        Parameters
        ----------
        user_input : str
            The initial request from the user.  This will be added to the
            conversation memory and used to contextualise the first call to
            the model.

        Returns
        -------
        str
            The final answer provided by the agent when it calls the
            ``finish`` action.
        """
        # Add the initial user message
        self.memory.add_message("user", user_input)
        goal_prompt = self.goal.to_prompt()
        while True:
            # Build model call and get response
            try:
                resp = self.agent_language.generate_response(goal_prompt, self.memory.get_history())
            except Exception as e:
                raise RuntimeError(f"Failed to call language model: {e}") from e
            # Determine whether the language returned an invocation directly
            invocation: Dict[str, Any]
            if isinstance(resp, dict) and "tool_name" in resp:
                invocation = resp  # type: ignore[assignment]
            else:
                # parse the JSON response into a dict
                try:
                    invocation = self.agent_language.parse_response(resp)  # type: ignore[assignment]
                except Exception as e:
                    raise RuntimeError(f"Failed to parse model response: {e}\nResponse: {resp}") from e
            tool_name = invocation.get("tool_name")
            args = invocation.get("args", {}) or {}
            if self.debug:
                print(f"[DEBUG] Invocation: tool_name={tool_name}, args={args}")
            # If the model indicates the conversation is finished, return the response
            if tool_name == "finish":
                final_message = args.get("response", "")
                self.memory.add_message("assistant", final_message)
                return final_message
            # Lookup the action callback
            action = self.actions.get(tool_name)
            if not action:
                raise ValueError(f"Model requested unknown tool '{tool_name}'")
            # Execute the action
            try:
                result = action.callback(**args)
            except StopIteration as end:
                # Use StopIteration to finish early
                final_message = str(end.value) if end.value is not None else ""
                self.memory.add_message("assistant", final_message)
                return final_message
            except Exception as exc:
                # Record exceptions and pass back error message
                tb = traceback.format_exc()
                self.memory.add_message("tool", f"Error executing {tool_name}: {exc}\n{tb}", name=tool_name)
                continue
            if self.debug:
                print(f"[DEBUG] Tool {tool_name} returned: {result}")
            # Append the result as a tool message
            self.memory.add_message("tool", str(result), name=tool_name)
