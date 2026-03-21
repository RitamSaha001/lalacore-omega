#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.automation.feeder_engine import FeederEngine


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _print(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=True))


def _split_csv_like(value: str | Sequence[str] | None) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        out: List[str] = []
        for item in value:
            s = str(item or "").strip().lower()
            if s and s not in out:
                out.append(s)
        return out
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return _split_csv_like(parsed)
        except Exception:
            pass
    parts = [p.strip().lower() for chunk in text.split("|") for p in chunk.split(",")]
    out = []
    for part in parts:
        if part and part not in out:
            out.append(part)
    return out


def _first_non_empty(row: Dict[str, Any], keys: Sequence[str], default: str = "") -> str:
    for key in keys:
        if key not in row:
            continue
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return default


def _normalize_row(
    row: Dict[str, Any],
    *,
    default_source_tag: str,
    auto_classify: bool,
) -> Dict[str, Any] | None:
    question = _first_non_empty(row, ("question", "prompt", "query", "text"), default="")
    if not question:
        return None

    subject = _first_non_empty(row, ("subject",), default="")
    difficulty = _first_non_empty(row, ("difficulty", "level"), default="")

    if auto_classify and (not subject or not difficulty):
        from core.lalacore_x.classifier import ProblemClassifier

        profile = ProblemClassifier().classify(question)
        if not subject:
            subject = profile.subject
        if not difficulty:
            difficulty = profile.difficulty

    if not subject:
        subject = "general"
    if not difficulty:
        difficulty = "unknown"

    clusters_raw = row.get("concept_cluster", row.get("concept_clusters", row.get("clusters", "")))
    clusters = _split_csv_like(clusters_raw)
    source_tag = _first_non_empty(row, ("source_tag", "source"), default=default_source_tag)

    return {
        "question": question,
        "subject": subject,
        "difficulty": difficulty,
        "concept_cluster": clusters,
        "source_tag": source_tag,
    }


def _load_rows_from_json_obj(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [row for row in obj if isinstance(row, dict)]
    if isinstance(obj, dict):
        if isinstance(obj.get("questions"), list):
            return [row for row in obj["questions"] if isinstance(row, dict)]
        if isinstance(obj.get("items"), list):
            return [row for row in obj["items"] if isinstance(row, dict)]
        if "question" in obj:
            return [obj]
    return []


def load_questions_file(path: Path, *, auto_classify: bool, default_source_tag: str) -> List[Dict[str, Any]]:
    suffix = path.suffix.lower()
    rows: List[Dict[str, Any]] = []

    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
    elif suffix == ".jsonl":
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        rows = _load_rows_from_json_obj(obj)
    else:
        raise ValueError("Unsupported input type. Use .json, .jsonl, or .csv")

    out = []
    for row in rows:
        item = _normalize_row(
            row,
            default_source_tag=default_source_tag,
            auto_classify=auto_classify,
        )
        if item is not None:
            out.append(item)
    return out


def enqueue_items(feeder: FeederEngine, items: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    added = 0
    duplicate = 0
    errors = 0
    queued: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for item in items:
        try:
            result = feeder.enqueue_question(
                question=item["question"],
                subject=item.get("subject", "general"),
                difficulty=item.get("difficulty", "unknown"),
                concept_cluster=item.get("concept_cluster", []),
                source_tag=item.get("source_tag", "manual_batch"),
            )
        except Exception as exc:
            errors += 1
            failures.append(
                {
                    "question": str(item.get("question", ""))[:180],
                    "error": str(exc),
                }
            )
            continue

        if result.get("added"):
            added += 1
        if result.get("duplicate"):
            duplicate += 1
        if isinstance(result.get("queue_item"), dict):
            queued.append(result["queue_item"])

    return {
        "requested": len(items),
        "added": added,
        "duplicate": duplicate,
        "errors": errors,
        "queue_items": queued,
        "failures": failures,
    }


class ArenaOnlyExecutor:
    """
    Arena-only runner that keeps queue semantics from FeederEngine,
    but executes only provider generation + arena selection.
    """

    def __init__(self, providers: Sequence[str]):
        self.providers = [str(p).strip() for p in providers if str(p).strip()]
        self._inited = False

    def _lazy_init(self) -> None:
        if self._inited:
            return

        from app.arena.entropy import compute_entropy
        from core.lalacore_x.arena import AdvancedArenaLayer, ArenaJudge
        from core.lalacore_x.classifier import ProblemClassifier
        from core.lalacore_x.providers import ProviderFabric
        from core.lalacore_x.retrieval import ConceptVault
        from verification.verifier import verify_solution

        self.compute_entropy = compute_entropy
        self.advanced_arena = AdvancedArenaLayer(similarity_engine=None)
        self.judge = ArenaJudge()
        self.classifier = ProblemClassifier()
        self.fabric = ProviderFabric()
        self.vault = ConceptVault(root="data/vault")
        self.verify_solution = verify_solution
        self._inited = True

    def _candidate_disagreement(self, arena_inputs: Sequence[Dict[str, Any]]) -> float:
        answers = [str(row.get("final_answer", "")).strip().lower() for row in arena_inputs if str(row.get("final_answer", "")).strip()]
        if len(answers) <= 1:
            return 0.0
        return min(1.0, (len(set(answers)) - 1) / max(len(answers), 1))

    def _select_providers(self) -> List[str]:
        available = self.fabric.available_providers()
        chosen = []

        for provider in self.providers:
            if provider in chosen:
                continue
            chosen.append(provider)

        if not chosen:
            chosen = list(available)

        if "mini" not in chosen:
            chosen.insert(0, "mini")

        if len(chosen) < 2:
            chosen.append("openrouter")

        return chosen

    async def solve(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self._lazy_init()

        question = str(row.get("question", "")).strip()
        if not question:
            raise ValueError("empty question")

        profile = self.classifier.classify(question)
        retrieved = self.vault.retrieve(question, subject=profile.subject, top_k=5)
        providers = self._select_providers()
        candidates = await self.fabric.generate_many(providers, question, profile, retrieved)
        if not candidates:
            raise RuntimeError("arena-only generation returned no candidates")

        reliability: Dict[str, float] = {}
        verification_by_provider: Dict[str, Dict[str, Any]] = {}
        for candidate in candidates:
            reliability[candidate.provider] = max(0.05, min(1.0, float(candidate.confidence)))
            try:
                ver = self.verify_solution(
                    question=question,
                    predicted_answer=str(candidate.final_answer or ""),
                    difficulty=str(row.get("difficulty", profile.difficulty)),
                )
            except Exception as exc:
                ver = {
                    "verified": False,
                    "risk_score": 1.0,
                    "escalate": True,
                    "reason": str(exc),
                    "failure_reason": "verify_exception",
                    "stage_results": {},
                }
            verification_by_provider[candidate.provider] = ver

        retrieval_strength = 0.0
        if retrieved:
            raw = sum(max(0.0, float(block.score)) for block in retrieved[:5]) / max(1, len(retrieved[:5]))
            retrieval_strength = max(0.0, min(1.0, raw))

        judge_results = self.judge.evaluate(
            candidates=candidates,
            verification_by_provider=verification_by_provider,
            provider_reliability=reliability,
            retrieval_strength=retrieval_strength,
            coherence_by_provider={},
            structure_by_provider={},
            process_reward_by_provider={},
        )
        judge_by_provider = {row.provider: row for row in judge_results}

        arena_inputs: List[Dict[str, Any]] = []
        for candidate in candidates:
            arena_inputs.append(
                {
                    "provider": candidate.provider,
                    "final_answer": str(candidate.final_answer),
                    "critic_score": float(judge_by_provider.get(candidate.provider).critic_score if candidate.provider in judge_by_provider else 0.5),
                    "deterministic_pass": bool(verification_by_provider.get(candidate.provider, {}).get("verified")),
                    "confidence": float(candidate.confidence),
                    "skill": float(reliability.get(candidate.provider, 0.5)),
                    "reasoning": str(candidate.reasoning),
                }
            )

        entropy = self.compute_entropy(arena_inputs)
        for provider in verification_by_provider:
            verification_by_provider[provider]["entropy"] = entropy

        arena_outcome = self.advanced_arena.run(responses=arena_inputs, entropy=entropy)
        posteriors = arena_outcome.get("posteriors", {})
        winner_provider = str(arena_outcome.get("winner", "") or "")
        if not winner_provider and posteriors:
            winner_provider = max(posteriors, key=posteriors.get)
        if not winner_provider and candidates:
            winner_provider = candidates[0].provider

        winner_candidate = next((candidate for candidate in candidates if candidate.provider == winner_provider), candidates[0])
        winner_verification = verification_by_provider.get(
            winner_provider,
            {"verified": False, "risk_score": 1.0, "escalate": True},
        )

        ranked_providers = sorted(
            [{"provider": provider, "score": float(score)} for provider, score in posteriors.items()],
            key=lambda row: row["score"],
            reverse=True,
        )
        disagreement = self._candidate_disagreement(arena_inputs)

        return {
            "question": question,
            "reasoning": str(winner_candidate.reasoning),
            "final_answer": str(winner_candidate.final_answer),
            "verification": winner_verification,
            "routing_decision": f"arena-only providers={','.join(providers)}",
            "escalate": bool(winner_verification.get("escalate", False)),
            "winner_provider": winner_provider,
            "profile": {
                "subject": str(row.get("subject", profile.subject)),
                "difficulty": str(row.get("difficulty", profile.difficulty)),
                "numeric": bool(profile.numeric),
                "multiConcept": bool(profile.multi_concept),
                "trapProbability": float(profile.trap_probability),
            },
            "arena": {
                "ranked_providers": ranked_providers,
                "judge_results": [
                    {
                        "provider": result.provider,
                        "score": float(result.score),
                        "risk": float(result.risk),
                        "rule_score": float(result.rule_score),
                        "critic_score": float(result.critic_score),
                        "verified": bool(result.verified),
                        "notes": list(result.notes),
                    }
                    for result in judge_results
                ],
                "bt_thetas": dict(arena_outcome.get("thetas", {})),
                "posteriors": dict(posteriors),
                "winner_margin": float(arena_outcome.get("winner_margin", 0.0)),
                "arena_confidence": float(arena_outcome.get("arena_confidence", 0.0)),
                "pairwise_confidence_margin": float(arena_outcome.get("pairwise", {}).get("confidence_margin", 0.0)),
                "uncertainty_adjusted_margin": float(arena_outcome.get("pairwise", {}).get("uncertainty_adjusted_margin", 0.0)),
                "disagreement": float(disagreement),
                "disagreement_case_count": len(arena_outcome.get("pairwise", {}).get("disagreement_cases", [])),
                "deterministic_dominance": any(bool(v.get("verified", False)) for v in verification_by_provider.values()),
                "entropy": float(arena_outcome.get("entropy", entropy)),
            },
            "retrieval": {
                "top_blocks": [
                    {"id": block.block_id, "title": block.title, "score": float(block.score), "source": block.source}
                    for block in retrieved[:5]
                ],
                "claim_support_score": 0.0,
            },
            "engine": {
                "name": "LALACORE_X_ARENA_ONLY",
                "version": "manual-runner-v1",
                "backward_compatible": True,
                "degraded_mode": False,
                "degraded_reason": None,
            },
            "ts": _utc_now(),
        }


async def process_pending_with_executor(
    *,
    feeder: FeederEngine,
    executor: ArenaOnlyExecutor,
    max_items: int,
    trigger: str,
) -> Dict[str, Any]:
    max_items = max(1, int(max_items))
    feeder._recover_stale_processing()

    budget_scale = float(feeder.token_guardian.replay_intensity_scale())
    scaled_max = max(1, int(round(max_items * budget_scale)))
    remaining_daily = max(0, feeder.daily_cap - feeder._processed_today_count())
    budget = min(scaled_max, remaining_daily)
    if budget <= 0:
        return {
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "skipped_daily_cap": True,
            "daily_cap": feeder.daily_cap,
            "remaining_daily": remaining_daily,
        }

    processed = 0
    completed = 0
    failed = 0
    start_ts = datetime.now(timezone.utc)

    while processed < budget:
        row = feeder._reserve_next_pending()
        if row is None:
            break

        processed += 1
        feeder_id = int(row.get("id", 0))
        try:
            result = await executor.solve(row)
            feeder._mark_completed(row, result)
            completed += 1
            feeder.state.checkpoint(
                "feeder",
                last_processed_id=feeder_id,
                last_processed_hash=str(row.get("item_hash")),
                last_checkpoint_ts=_utc_now(),
            )
        except Exception as exc:
            row = feeder._mark_failed(row, str(exc))
            if str(row.get("status")) == feeder.STATUS_FAILED:
                failed += 1

    duration_s = max(0.0, (datetime.now(timezone.utc) - start_ts).total_seconds())
    feeder.logger.event(
        "feeder_process_summary",
        {
            "trigger": str(trigger),
            "mode": "arena-only",
            "processed": processed,
            "completed": completed,
            "failed": failed,
            "duration_s": round(duration_s, 6),
            "budget": budget,
            "daily_cap": feeder.daily_cap,
        },
    )

    return {
        "mode": "arena-only",
        "processed": processed,
        "completed": completed,
        "failed": failed,
        "duration_s": round(duration_s, 6),
        "budget": budget,
        "daily_cap": feeder.daily_cap,
        "remaining_daily": max(0, feeder.daily_cap - feeder._processed_today_count()),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch manual question runner (JSON/CSV + queue statuses)")
    parser.add_argument("--queue-path", default="data/lc9/LC9_FEEDER_QUEUE.jsonl", help="Queue JSONL path")
    parser.add_argument("--training-cases-path", default="data/lc9/LC9_FEEDER_CASES.jsonl", help="Training cases JSONL path")
    parser.add_argument("--replay-cases-path", default="data/replay/feeder_cases.jsonl", help="Replay cases JSONL path")

    sub = parser.add_subparsers(dest="cmd", required=True)

    run_file = sub.add_parser("run-file", help="Load file, enqueue, optionally process")
    run_file.add_argument("--input", required=True, help="Input .json/.jsonl/.csv")
    run_file.add_argument("--mode", choices=("full-solver", "arena-only"), default="full-solver")
    run_file.add_argument("--max-items", type=int, default=20, help="Max queue items to process")
    run_file.add_argument("--source-tag", default="manual_batch", help="Source tag if missing per row")
    run_file.add_argument("--no-auto-classify", action="store_true", default=False, help="Disable auto subject/difficulty fill")
    run_file.add_argument("--enqueue-only", action="store_true", default=False, help="Only enqueue; skip processing")
    run_file.add_argument("--providers", default="mini,openrouter,groq", help="Arena-only provider list")
    run_file.add_argument("--status-limit", type=int, default=20, help="Recent status row limit")

    add = sub.add_parser("add", help="Add one question directly")
    add.add_argument("--question", required=True, help="Question text")
    add.add_argument("--subject", default="general", help="Subject")
    add.add_argument("--difficulty", default="unknown", help="Difficulty")
    add.add_argument("--concept-cluster", default="", help="Comma/pipe separated concept clusters")
    add.add_argument("--source-tag", default="manual_direct", help="Source tag")

    process = sub.add_parser("process", help="Process pending queue")
    process.add_argument("--mode", choices=("full-solver", "arena-only"), default="full-solver")
    process.add_argument("--max-items", type=int, default=20, help="Max queue items to process")
    process.add_argument("--providers", default="mini,openrouter,groq", help="Arena-only provider list")

    status = sub.add_parser("status", help="Show queue status")
    status.add_argument("--limit", type=int, default=20, help="Recent row limit")

    return parser


def _build_feeder(args: argparse.Namespace) -> FeederEngine:
    return FeederEngine(
        queue_path=str(args.queue_path),
        training_cases_path=str(args.training_cases_path),
        replay_cases_path=str(args.replay_cases_path),
    )


def _parse_provider_arg(value: str) -> List[str]:
    providers = []
    for part in str(value or "").split(","):
        p = part.strip()
        if p and p not in providers:
            providers.append(p)
    return providers


async def _process_queue(feeder: FeederEngine, *, mode: str, max_items: int, providers: Sequence[str], trigger: str) -> Dict[str, Any]:
    if mode == "full-solver":
        out = await feeder.process_pending(max_items=max_items, trigger=trigger)
        out["mode"] = "full-solver"
        return out

    executor = ArenaOnlyExecutor(providers=providers)
    return await process_pending_with_executor(
        feeder=feeder,
        executor=executor,
        max_items=max_items,
        trigger=trigger,
    )


async def main_async(args: argparse.Namespace) -> None:
    feeder = _build_feeder(args)

    if args.cmd == "add":
        payload = feeder.enqueue_question(
            question=str(args.question),
            subject=str(args.subject),
            difficulty=str(args.difficulty),
            concept_cluster=_split_csv_like(args.concept_cluster),
            source_tag=str(args.source_tag),
        )
        _print(payload)
        return

    if args.cmd == "status":
        _print(feeder.status(limit=max(1, int(args.limit))))
        return

    if args.cmd == "process":
        process_result = await _process_queue(
            feeder,
            mode=str(args.mode),
            max_items=max(1, int(args.max_items)),
            providers=_parse_provider_arg(args.providers),
            trigger="manual_runner_process",
        )
        _print(
            {
                "process": process_result,
                "status": feeder.status(limit=20),
            }
        )
        return

    if args.cmd == "run-file":
        input_path = Path(args.input).resolve()
        items = load_questions_file(
            input_path,
            auto_classify=(not bool(args.no_auto_classify)),
            default_source_tag=str(args.source_tag),
        )
        enqueue_result = enqueue_items(feeder, items)

        output: Dict[str, Any] = {
            "input_path": str(input_path),
            "enqueue": enqueue_result,
        }

        if not bool(args.enqueue_only):
            process_result = await _process_queue(
                feeder,
                mode=str(args.mode),
                max_items=max(1, int(args.max_items)),
                providers=_parse_provider_arg(args.providers),
                trigger="manual_runner_run_file",
            )
            output["process"] = process_result

        output["status"] = feeder.status(limit=max(1, int(args.status_limit)))
        _print(output)
        return

    raise RuntimeError(f"unknown command: {args.cmd}")


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
