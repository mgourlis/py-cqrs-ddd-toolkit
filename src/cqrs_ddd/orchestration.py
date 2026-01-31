"""
Orchestration abstractions.

Defines the interface for starting and managing workflows/orchestrations
without coupling to a specific engine (like Temporal).
"""
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Any, Optional

TInput = TypeVar("TInput")
TResult = TypeVar("TResult")


class AbstractOrchestrator(ABC, Generic[TInput, TResult]):
    """
    Interface for an Orchestrator that manages a workflow.
    
    TInput: The input data type for the workflow.
    TResult: The result type (e.g., Workflow ID or final result).
    """

    @abstractmethod
    async def run(self, input_data: TInput) -> TResult:
        """
        Start or execute the orchestration.
        """
        raise NotImplementedError
