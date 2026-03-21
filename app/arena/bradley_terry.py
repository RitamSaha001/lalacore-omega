import math
from collections import defaultdict

from core.safe_math import clipped_division, safe_log, safe_sigmoid


class BradleyTerryEngine:
    """
    Advanced Bradley–Terry Engine (Omega Stable Version)

    Compatible with PairwiseEngine:
        matches = [(winner, loser, margin), ...]

    Features:
    - Stable logistic prediction
    - Iterative MLE fitting
    - Margin-weighted likelihood
    - L2 regularization
    - Learning-rate decay
    - Theta clipping
    - Uncertainty estimation
    """

    def __init__(
        self,
        learning_rate=0.05,
        reg_lambda=0.01,
        max_theta=8.0,
        min_iterations=8,
        max_iterations=64,
    ):
        self.base_lr = learning_rate
        self.reg_lambda = reg_lambda
        self.max_theta = max_theta
        self.min_iterations = min_iterations
        self.max_iterations = max_iterations

        self.theta = {}
        self.match_counts = defaultdict(int)

    # =====================================================
    # INITIALIZATION
    # =====================================================

    def initialize(self, providers):
        self.theta = {p: 0.0 for p in providers}
        self.match_counts = defaultdict(int)

    # =====================================================
    # STABLE LOGISTIC PREDICTION
    # =====================================================

    def predict(self, i, j):
        diff = self.theta[i] - self.theta[j]
        return safe_sigmoid(diff)

    # =====================================================
    # FULL MLE FIT
    # =====================================================

    def fit(self, matches, iterations=30):

        if not matches:
            return

        for step in range(iterations):

            # Learning rate decay
            lr = self.base_lr / (1 + 0.1 * step)

            gradients = defaultdict(float)

            for winner, loser, margin in matches:

                self.match_counts[winner] += 1
                self.match_counts[loser] += 1

                p = self.predict(winner, loser)

                # Bounded margin influence
                weight = 1 + math.tanh(margin)

                gradients[winner] += weight * (1 - p)
                gradients[loser] -= weight * p

            # Apply gradients + regularization
            for provider in self.theta:

                grad = gradients[provider]

                # L2 regularization (shrink toward 0)
                grad -= self.reg_lambda * self.theta[provider]

                self.theta[provider] += lr * grad

                # Clip theta to prevent explosion
                if self.theta[provider] > self.max_theta:
                    self.theta[provider] = self.max_theta
                elif self.theta[provider] < -self.max_theta:
                    self.theta[provider] = -self.max_theta

    def schedule_iterations(self, entropy: float, provider_count: int) -> int:
        """
        Entropy-normalized adaptive scheduling.
        """
        if provider_count <= 1:
            return self.min_iterations

        max_entropy = safe_log(provider_count, fallback=0.0)
        normalized_entropy = 0.0 if max_entropy <= 0 else min(1.0, max(0.0, clipped_division(entropy, max_entropy, fallback=0.0)))
        span = self.max_iterations - self.min_iterations
        return int(round(self.min_iterations + span * normalized_entropy))

    # =====================================================
    # UNCERTAINTY ESTIMATE
    # =====================================================

    def get_uncertainty(self, provider):
        count = self.match_counts.get(provider, 0)

        if count == 0:
            return 1.0  # maximum uncertainty

        return 1 / math.sqrt(count)

    def uncertainty_map(self):
        return {provider: self.get_uncertainty(provider) for provider in self.theta}
