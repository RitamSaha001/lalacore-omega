# Complex Numbers and Quadratic Equations

Status: completed
Curated at: 2026-03-08T10:33:03+05:30
Raw rows reviewed: 80
Accepted rows: 5
Rejected rows: 75

Editorial policy for this chapter:
- Preserve the original mathematical problem.
- Allow only OCR-symbol restoration, missing fraction bars, missing modulus bars, and removal of book artifacts.
- Reject any row that would require inventing an expression, condition, option, or solution path.

Accepted rows:
- `manual_complex_001` from legacy `imp_q_112`: complete multi-correct question; answer key manually corrected to `A, C, D`.
- `manual_complex_002` from legacy `imp_q_92`: exact PYQ statement restored; numerical answer re-derived as `20`.
- `manual_complex_003` from legacy row `bankq_1772718576609`: restored `|z - 2 - 2i| <= 1`; numerical answer re-derived as `5`.
- `manual_complex_004` from legacy `imp_q_88`: restored condition `z != 1` and `z^2/(z - 1)`; correct option `A` re-derived.
- `manual_complex_005` from legacy `imp_q_65`: restored fraction `(2z - 3i)/(2z + i)` and MCQ options; correct option `C` re-derived.

Primary rejection reasons for the remaining 75 rows:
- standalone answer-key rows or answer-table fragments
- wrong-subject contamination (chemistry / physics leakage into the chapter)
- incomplete stems with missing core expressions
- options missing beyond safe recovery
- solution text contaminated by unrelated question content
- duplicate rows where the cleaner duplicate was already accepted

Important constraint carried forward:
- no row was accepted purely on bank confidence; every accepted row either survived direct algebra/geometry verification or had an exact statement recovery that was then re-verified manually.
