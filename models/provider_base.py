from abc import ABC, abstractmethod


class BaseProvider(ABC):

    @abstractmethod
    async def generate(self, question: str, context: str | None = None):
        """
        Every provider must implement this.
        Must return:
        {
            "provider": str,
            "reasoning": str | None,
            "final_answer": str,
            "latency": float,
            "raw": dict
        }
        """
        pass
    