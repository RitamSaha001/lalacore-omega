# LALACORE OMEGA / LALACORE X

Research-grade, domain-specialized reasoning engine for JEE-style solving with:

- Retrieval-first GraphRAG concept vault
- Multi-provider arena with iterative Bradley-Terry + log-space Bayesian fusion
- Deterministic verification 2.0
- DAG reasoning coherence scoring + deterministic dominance gating
- Confidence calibration + risk-aware escalation
- Autonomous failure replay + weekly evolution loop
- Self-healing telemetry
- Mini shadow evolution engine (EMA reliability, Brier drift, gated promotion)
- Backward-compatible API shape (`/solve`)

## Architecture

`Flutter / Apps Script (thin)` -> `FastAPI router` -> `LalaCore X Engine` -> `Arena + Verification + Calibration` -> `Weekly Zaggle loop`

### Engine layers

1. Problem classification (subject, difficulty, trap probability)
2. GraphRAG retrieval (concept vault + trap vault + claim checking)
3. Statistical routing (subject/difficulty EMA, weakness-aware penalties, auto-thresholds)
4. Advanced arena (O(N^2) pairwise BT + clone clustering + uncertainty-aware posterior)
5. Log-space Bayesian aggregator (stable softmax, collapse fallback, margin confidence)
6. Deterministic verification supremacy + reasoning graph coherence penalties
7. Mini evolution engine (shadow-only, replay queue, calibration drift monitoring)
8. Replay + telemetry (self-healing logs, disagreement/failure structured memory)

## Key paths

- Engine: `core/lalacore_x/`
- API entry: `app/main.py`, `app/routes.py`
- Solver entry: `core/solver.py`
- Verification: `verification/verifier.py`
- Weekly evolution: `core/lalacore_x/weekly.py`
- Mini distillation hub: `core/lalacore_x/mini_distillation.py`
- Weekly script: `scripts/weekly_replay.py`
- Zaggle dataset build: `app/training/dataset_builder.py`

## Run

```bash
./venv/bin/uvicorn app.main:app --reload
```

Solve:

```bash
curl -X POST http://127.0.0.1:8000/solve \
  -H "Content-Type: application/json" \
  -d '{"question":"What is 6 * 7?"}'
```

## Weekly evolution loop

```bash
./venv/bin/python scripts/weekly_replay.py
```

## Automation Orchestrator

Run full automation pipeline (feeder refresh + weekly evolution + replay + dataset distillation + ranking refresh):

```bash
./venv/bin/python scripts/run_automation.py --trigger manual --resume
```

Scheduled tick (runs only when due):

```bash
./venv/bin/python scripts/run_automation.py --scheduled --interval-days 7
```

## Feeder System (Manual Question Injection)

Add question to feeder queue:

```bash
./venv/bin/python scripts/feeder_cli.py add \
  --question "A block slides on a rough incline. Find acceleration." \
  --subject physics \
  --difficulty hard \
  --concept-cluster friction,newton-laws \
  --source-tag manual_batch
```

Process queued feeder items through the normal solve pipeline:

```bash
./venv/bin/python scripts/feeder_cli.py process --max-items 8
```

Check queue status:

```bash
./venv/bin/python scripts/feeder_cli.py status --limit 20
```

Equivalent API endpoints:

- `POST /ops/feeder/add`
- `POST /ops/feeder/process`
- `GET /ops/feeder/status`
- `POST /ops/automation/run-weekly`
- `POST /ops/automation/tick`

Outputs:

- `data/reports/weekly_evolution_report.json`
- `data/replay/weekly_replay.jsonl`
- `data/zaggle/prm_dataset.jsonl`
- `data/zaggle/dpo_pairs.jsonl`
- `data/zaggle/rlaif_feedback.jsonl`
- `data/zaggle/LC9_MINI_WEEKLY_DATASET.jsonl`

LC9 persistent memories:

- `data/lc9/LC9_MINI_DISAGREEMENT_MEMORY.jsonl`
- `data/lc9/LC9_MINI_TRAINING_DATASET.jsonl`
- `data/lc9/LC9_PROVIDER_PROMPT_ARCHIVE.jsonl`
- `data/lc9/LC9_MINI_SHADOW_LOGS.jsonl`
- `data/lc9/LC9_FEEDER_QUEUE.jsonl`
- `data/lc9/LC9_FEEDER_CASES.jsonl`
- `data/lc9/LC9_AUTOMATION_STATE.json`
- `data/lc9/LC9_AUTOMATION_LOGS.jsonl`

## Environment keys (free-tier aware)

- `OPENROUTER_KEYS`
- `GROQ_KEYS`
- `GEMINI_KEYS`
- `HF_KEYS`

Keys can be comma-separated; rotation and cooldown are automatic.

## Database migrations

- `001_arena_tables.sql`
- `002_reasoning_tables.sql`
- `003_core_orchestration_tables.sql`

## Compatibility

The `/solve` response keeps legacy fields (`reasoning`, `final_answer`, `verification`) and adds structured research metadata (`profile`, `arena`, `retrieval`, `engine`).
