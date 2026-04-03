# Main AI Engine 3.1.1 Validation

Generated: 2026-04-03

## Scope

- Main AI engine hardening only
- Excludes Atlas-specific planner surfaces
- Excludes study-material AI surfaces

## Engine Changes

- Fixed digit-permutation constraint parsing for odd/even digit-sum prompts.
- Fixed digit-permutation counting for `first digit > last digit`.
- Improved research-verifier answer-type matching for roots/list-style symbolic answers.
- Preserved deterministic symbolic solving for the corrected combinatorics path.

## Benchmark Summary

### 20-question JEE Advanced slice

- Before: `20/20` correct (`100.0%`)
- After: `20/20` correct (`100.0%`)
- Before mean latency: `16.182s`
- After mean latency: `18.157s`
- Before median latency: `10.930s`
- After median latency: `12.575s`

Interpretation:

- No regression on the broad 20-question validation slice.
- The added deterministic checks slightly increased average latency on this run, but accuracy remained perfect.

### Historical regression subset

These were previously wrong in the older 50-case benchmark corpus.

| Label | Prompt summary | Previous answer | New answer | Result |
|---|---|---:|---:|---|
| `C1` | 4-digit numbers, odd digit sum | `4536` | `2160` | fixed |
| `C2` | 4-digit numbers, even digit sum | `4536` | `2376` | fixed |
| `C4` | 4-digit numbers, first digit > last digit | `4536` | `2520` | fixed |

- Historical failing subset before: `0/3` correct (`0.0%`)
- Historical failing subset after: `3/3` correct (`100.0%`)

## Tests Run

- `./venv/bin/python -m unittest tests.test_problem_parser tests.test_contextual_math_combinatorics tests.test_hardening_layers tests.test_answer_quality_verifier tests.test_contextual_math_verifier tests.test_lalacore_entrypoint`
- `./venv/bin/python -m py_compile core/math/problem_parser.py core/math/contextual_math_solver.py core/lalacore_x/research_verifier.py tests/test_problem_parser.py tests/test_contextual_math_combinatorics.py tests/test_hardening_layers.py`

## Notes

- The benchmark harness exercises the real local `ai_chat` action pipeline.
- The 20-question benchmark did not include the historical failing combinatorics trio, so the targeted regression subset is the meaningful quality-gain signal for this pass.
