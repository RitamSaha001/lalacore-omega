from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseProvider(ABC):

    def __init__(self, provider_id: int, name: str):
        self.provider_id = provider_id
        self.name = name

    @abstractmethod
    async def generate(
        self,
        question: str,
        subject: str,
        difficulty: int
    ) -> Dict[str, Any]:
        """
        Must return:
        {
            "answer": str,
            "raw": str,
            "confidence": float
        }
        """
        pass