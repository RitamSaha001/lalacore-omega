from __future__ import annotations

from typing import Dict, List


class RebuttalEngine:
    """
    Generates structured rebuttal hints between disagreeing provider answers.
    """

    def build(self, responses: List[Dict]) -> List[Dict]:
        rebuttals = []

        for i in range(len(responses)):
            for j in range(i + 1, len(responses)):
                a = responses[i]
                b = responses[j]
                if a.get("final_answer") == b.get("final_answer"):
                    continue

                rebuttals.append(
                    {
                        "provider": a.get("provider"),
                        "against": b.get("provider"),
                        "rebuttal": self._hint(a, b),
                    }
                )
                rebuttals.append(
                    {
                        "provider": b.get("provider"),
                        "against": a.get("provider"),
                        "rebuttal": self._hint(b, a),
                    }
                )

        return rebuttals

    def _hint(self, src: Dict, tgt: Dict) -> str:
        return (
            f"Re-check why '{src.get('final_answer', '')}' is stronger than "
            f"'{tgt.get('final_answer', '')}'. Focus on constraints and verification."
        )
