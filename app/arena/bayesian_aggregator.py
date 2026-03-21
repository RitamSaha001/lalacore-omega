from core.safe_math import clipped_division, safe_log, safe_sigmoid, safe_softmax


class BayesianAggregator:
    """
    Log-space fusion aggregator.

    Combines:
    - Local BT probability
    - Global skill prior
    - Critic score
    - Deterministic verification signal
    - BT uncertainty penalty

    Backward compatibility:
    - compute(...): returns posteriors only
    - compute(..., return_details=True): returns structured diagnostics
    """

    def __init__(
        self,
        w_local=1.0,
        w_skill=0.8,
        w_critic=0.7,
        w_structure=0.55,
        w_process=0.45,
        w_det=1.25,
        uncertainty_lambda=0.9,
        epsilon=1e-9,
    ):
        self.w_local = w_local
        self.w_skill = w_skill
        self.w_critic = w_critic
        self.w_structure = w_structure
        self.w_process = w_process
        self.w_det = w_det
        self.uncertainty_lambda = uncertainty_lambda
        self.epsilon = epsilon

    def compute(
        self,
        responses,
        thetas,
        uncertainties=None,
        entropy=None,
        return_details=False,
    ):
        uncertainties = uncertainties or {}

        provider_logs = {}
        deterministic_present = any(bool(r.get("deterministic_pass")) for r in responses)

        for response in responses:
            provider = response["provider"]
            theta = float(thetas.get(provider, 0.0))

            local_prob = self._sigmoid(theta)
            skill = max(self.epsilon, float(response.get("skill", 0.5)))
            critic = max(self.epsilon, float(response.get("critic_score", 0.5)))
            structure = max(self.epsilon, float(response.get("structural_coherence", response.get("coherence", 0.5))))
            process_reward = max(self.epsilon, float(response.get("process_reward", 0.5)))
            det_signal = 1.0 if bool(response.get("deterministic_pass")) else 0.15
            uncertainty = float(uncertainties.get(provider, 1.0))

            if deterministic_present and not bool(response.get("deterministic_pass")):
                # Deterministic verification supremacy.
                det_signal = min(det_signal, 0.05)

            log_score = (
                self.w_local * safe_log(max(local_prob, self.epsilon))
                + self.w_skill * safe_log(skill)
                + self.w_critic * safe_log(critic)
                + self.w_structure * safe_log(structure)
                + self.w_process * safe_log(process_reward)
                + self.w_det * safe_log(max(det_signal, self.epsilon))
                - self.uncertainty_lambda * uncertainty
            )

            provider_logs[provider] = {
                "log_score": log_score,
                "local_prob": local_prob,
                "skill": skill,
                "critic": critic,
                "structure": structure,
                "process_reward": process_reward,
                "det_signal": det_signal,
                "uncertainty": uncertainty,
            }

        posteriors = self._stable_softmax(provider_logs)

        if not posteriors:
            posteriors = self._uniform(responses)

        posteriors = self._collapse_fallback(posteriors, responses)

        ranked = sorted(posteriors.items(), key=lambda x: x[1], reverse=True)
        winner_margin = 1.0 if len(ranked) == 1 else max(0.0, ranked[0][1] - ranked[1][1])
        confidence = max(0.0, min(1.0, winner_margin * (1.0 - self._mean_uncertainty(provider_logs))))

        details = {
            "posteriors": posteriors,
            "winner_margin": winner_margin,
            "confidence": confidence,
            "entropy": entropy,
            "deterministic_dominance": deterministic_present,
            "provider_logs": provider_logs,
        }

        if return_details:
            return details

        return posteriors

    def _stable_softmax(self, provider_logs):
        if not provider_logs:
            return {}

        providers = list(provider_logs.keys())
        probs = safe_softmax([provider_logs[p]["log_score"] for p in providers])
        if not probs:
            return {}
        return {provider: prob for provider, prob in zip(providers, probs)}

    def _uniform(self, responses):
        if not responses:
            return {}

        value = clipped_division(1.0, len(responses), fallback=0.0)
        return {response["provider"]: value for response in responses}

    def _collapse_fallback(self, posteriors, responses):
        if not posteriors:
            return posteriors

        ranked = sorted(posteriors.items(), key=lambda x: x[1], reverse=True)
        if len(ranked) < 2:
            return posteriors

        margin = abs(ranked[0][1] - ranked[1][1])
        if margin >= 1e-6:
            return posteriors

        deterministic = [r for r in responses if bool(r.get("deterministic_pass"))]
        if deterministic:
            weight = 1.0 / len(deterministic)
            return {r["provider"]: weight for r in deterministic}

        return posteriors

    def _sigmoid(self, x):
        return safe_sigmoid(x)

    def _mean_uncertainty(self, provider_logs):
        if not provider_logs:
            return 1.0
        vals = [row["uncertainty"] for row in provider_logs.values()]
        return sum(vals) / len(vals)
