import asyncio
import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.data.local_app_data_service import LocalAppDataService


class AIGenerateScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = Path(tempfile.mkdtemp(prefix="lc_scope_"))
        self.service = LocalAppDataService(
            assessments_file=self._tmp / "assessments.json",
            materials_file=self._tmp / "materials.json",
            uploads_file=self._tmp / "uploads.json",
            ai_quizzes_file=self._tmp / "ai_generated_quizzes.json",
            results_file=self._tmp / "results.json",
            teacher_review_file=self._tmp / "teacher_review_queue.json",
            import_drafts_file=self._tmp / "import_drafts.json",
            import_question_bank_file=self._tmp / "import_question_bank.json",
        )
        self.service._jee_bank_x_file = self._tmp / "JEE_BANK_X.json"

    def test_generic_math_scope_is_not_rejected(self) -> None:
        question = self.service._question_from_chapter_template(
            idx=0,
            subject="Mathematics",
            concept_tags=["Mathematics", "Mathematics"],
            difficulty=3,
            trap_intensity="medium",
            cross_concept=False,
            seed_key="scope_generic_math",
        )
        ok = self.service._question_matches_requested_scope(
            question=question,
            subject="Mathematics",
            chapters=["Mathematics"],
            subtopics=["Mathematics"],
        )
        self.assertTrue(ok)

    def test_ensure_loaded_prefers_jee_bank_x_when_present(self) -> None:
        import_bank = [
            {
                "question_id": "legacy_1",
                "question_text": "Legacy bank row",
                "type": "MCQ_SINGLE",
                "options": ["1", "2", "3", "4"],
                "correct_answer": {"single": "A", "multiple": ["A"], "numerical": None},
            }
        ]
        jee_bank_x = [
            {
                "question_id": "x_1",
                "question_text": "JEE BANK X row",
                "type": "MCQ_SINGLE",
                "options": ["1", "2", "3", "4"],
                "correct_answer": {"single": "B", "multiple": ["B"], "numerical": None},
                "repair_status": "safe",
                "requires_human_review": False,
            }
        ]
        (self._tmp / "import_question_bank.json").write_text(
            json.dumps(import_bank),
            encoding="utf-8",
        )
        (self._tmp / "JEE_BANK_X.json").write_text(
            json.dumps(jee_bank_x),
            encoding="utf-8",
        )

        asyncio.run(self.service._ensure_loaded())

        self.assertEqual(len(self.service._import_question_bank), 1)
        self.assertEqual(self.service._import_question_bank[0].get("question_id"), "x_1")

    def test_ensure_loaded_reads_chunked_jee_bank_x_when_single_file_missing(self) -> None:
        import_bank = [
            {
                "question_id": "legacy_1",
                "question_text": "Legacy bank row",
                "type": "MCQ_SINGLE",
                "options": ["1", "2", "3", "4"],
                "correct_answer": {"single": "A", "multiple": ["A"], "numerical": None},
            }
        ]
        chunked_rows = [
            {
                "question_id": "x_1",
                "question_text": "JEE BANK X row 1",
                "type": "MCQ_SINGLE",
                "options": ["1", "2", "3", "4"],
                "correct_answer": {"single": "B", "multiple": ["B"], "numerical": None},
                "repair_status": "safe",
                "requires_human_review": False,
            },
            {
                "question_id": "x_2",
                "question_text": "JEE BANK X row 2",
                "type": "MCQ_SINGLE",
                "options": ["1", "2", "3", "4"],
                "correct_answer": {"single": "C", "multiple": ["C"], "numerical": None},
                "repair_status": "safe",
                "requires_human_review": False,
            },
        ]
        (self._tmp / "import_question_bank.json").write_text(
            json.dumps(import_bank),
            encoding="utf-8",
        )
        parts_dir = self._tmp / "JEE_BANK_X.json.parts"
        parts_dir.mkdir(parents=True, exist_ok=True)
        (parts_dir / "part-000.json").write_text(
            json.dumps(chunked_rows[:1]),
            encoding="utf-8",
        )
        (parts_dir / "part-001.json").write_text(
            json.dumps(chunked_rows[1:]),
            encoding="utf-8",
        )

        asyncio.run(self.service._ensure_loaded())

        self.assertEqual(
            [row.get("question_id") for row in self.service._import_question_bank],
            ["x_1", "x_2"],
        )

    def test_local_import_bank_row_can_be_used_as_exact_pyq_question(self) -> None:
        jee_bank_x = [
            {
                "question_id": "cx_1",
                "question_text": "If z = 3 + 4i, then |z| equals",
                "type": "MCQ_SINGLE",
                "subject": "Mathematics",
                "chapter": "Complex Numbers and Quadratic Equations",
                "chapter_tags": ["Complex Numbers and Quadratic Equations"],
                "options": [
                    {"label": "A", "text": "4"},
                    {"label": "B", "text": "5"},
                    {"label": "C", "text": "6"},
                    {"label": "D", "text": "7"},
                ],
                "correct_answer": {"single": "B", "multiple": ["B"], "numerical": None},
                "solution_explanation": "Use modulus formula sqrt(a^2+b^2).",
                "repair_status": "safe",
                "requires_human_review": False,
                "verification": {
                    "mathematical_consistency": True,
                    "answer_key_verified": True,
                },
                "topic": "limits",
            }
        ]
        (self._tmp / "JEE_BANK_X.json").write_text(
            json.dumps(jee_bank_x),
            encoding="utf-8",
        )

        asyncio.run(self.service._ensure_loaded())

        bank_row = jee_bank_x[0]
        self.assertTrue(
            self.service._question_bank_row_matches_scope(
                row=bank_row,
                subject="Mathematics",
                chapters=["Complex Numbers and Quadratic Equations"],
                subtopics=["Complex Numbers and Quadratic Equations"],
            )
        )

        source_row = {
            "source_provider": "local_pyq_import_bank",
            "source_origin": "teacher_import",
            "url": "local://question_bank/cx_1",
            "question_text": bank_row["question_text"],
            "question_stub": bank_row["question_text"],
            "snippet": bank_row["question_text"],
            "options": ["4", "5", "6", "7"],
            "answer_stub": "B",
            "solution_stub": bank_row["solution_explanation"],
            "bank_payload": dict(bank_row),
            "verification_safe": True,
            "quality_score": 1.0,
        }

        prepared = self.service._question_from_web_source(
            row=source_row,
            idx=0,
            subject="Mathematics",
            chapters=["Complex Numbers and Quadratic Equations"],
            subtopics=["Complex Numbers and Quadratic Equations"],
            minimum_reasoning_steps=2,
        )
        self.assertIsNotNone(prepared)
        self.assertEqual(prepared.get("question_type"), "MCQ_SINGLE")
        self.assertEqual(prepared.get("_correct_option"), "B")
        self.assertIn("Complex Numbers", " ".join(prepared.get("chapter_tags") or []))

    def test_binomial_plus_combinatorics_scope_accepts_combinatorics_item(self) -> None:
        question = self.service._question_from_chapter_template(
            idx=1,
            subject="Mathematics",
            concept_tags=["Inclusion exclusion", "Permutation and Combination"],
            difficulty=5,
            trap_intensity="high",
            cross_concept=True,
            seed_key="scope_binomial_mix",
        )
        domain = self.service._domain_key_from_context(
            subject="Mathematics",
            concept_tags=["Inclusion exclusion", "Permutation and Combination"],
        )
        ok = self.service._question_matches_requested_scope(
            question=question,
            subject="Mathematics",
            chapters=["Binomial Theorem", "Permutation and Combination"],
            subtopics=["Binomial expansion", "Inclusion exclusion", "Arrangements"],
        )
        self.assertEqual(domain, "math_combinatorics")
        self.assertTrue(ok)

    def test_pyq_fetch_records_diagnostics_when_search_returns_empty(self) -> None:
        empty_diag = {
            "query": "jee integration pyq",
            "providers": [
                {
                    "provider": "duckduckgo_html",
                    "result_count": 0,
                    "transport": "curl",
                    "error": "bot_challenge",
                    "block_reason": "bot_challenge",
                }
            ],
            "result_count": 0,
            "error_reason": "bot_challenge",
        }
        with patch.object(
            self.service,
            "_search_rows_with_provider_fallback",
            return_value=([], dict(empty_diag)),
        ):
            rows = self.service._fetch_pyq_web_snippets(
                subject="Mathematics",
                chapters=["Integration"],
                subtopics=["Definite Integral"],
                query_suffix="JEE PYQ hard question",
                limit=4,
                difficulty=5,
            )
        self.assertEqual(rows, [])
        diag = self.service._last_pyq_web_diagnostics
        self.assertGreater(diag.get("query_attempts", 0), 0)
        self.assertEqual(diag.get("queries_with_results"), 0)
        self.assertEqual(diag.get("web_error_reason"), "bot_challenge")

    def test_unwrap_search_result_link_decodes_bing_redirect(self) -> None:
        target = "https://jeeadv.ac.in/past_qps/2022_2_English.pdf"
        payload = base64.urlsafe_b64encode(target.encode("utf-8")).decode("utf-8").rstrip("=")
        wrapped = (
            "https://www.bing.com/ck/a?!&&p=abc&ptn=3&ver=2&hsh=4"
            f"&u=a1{payload}&ntb=1"
        )
        self.assertEqual(self.service._unwrap_search_result_link(wrapped), target)

    def test_fetch_web_text_clears_urlopen_error_after_curl_success(self) -> None:
        with patch.object(
            self.service,
            "_fetch_text_urlopen",
            side_effect=RuntimeError("dns"),
        ), patch.object(
            self.service,
            "_fetch_text_curl",
            return_value="<html><body>ok</body></html>",
        ):
            raw, diag = self.service._fetch_web_text(url="https://example.com")
        self.assertIn("ok", raw)
        self.assertEqual(diag.get("transport"), "curl")
        self.assertEqual(diag.get("error"), "")

    def test_extract_search_rows_from_html_brave_anchor_mode(self) -> None:
        html = """
<html><body>
<a href="https://jeeadv.ac.in/past_qps/2022_2_English.pdf">JEE Advanced 2022 Paper 2</a>
</body></html>
        """.strip()
        rows = self.service._extract_search_rows_from_html(
            raw_html=html,
            provider="brave_html",
            max_rows=5,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("url"), "https://jeeadv.ac.in/past_qps/2022_2_English.pdf")

    def test_extract_import_search_seeds_ignores_binary_pdf_like_text(self) -> None:
        noisy = "%PDF-1.5\n2 0 obj\n<</Type /Catalog>>\nstream\nx\\x00\\x01{}<><><><>\nendstream"
        seeds = self.service._extract_import_search_seeds(noisy, max_seeds=4)
        self.assertEqual(seeds, [])

    def test_search_rows_from_provider_uses_cache_after_first_fetch(self) -> None:
        html = (
            '<html><body><a href="https://jeeadv.ac.in/past_qps/2022_2_English.pdf">'
            "JEE 2022 Paper</a></body></html>"
        )
        with patch.object(
            self.service,
            "_fetch_web_text",
            return_value=(html, {"transport": "curl", "error": "", "block_reason": ""}),
        ) as fetch_mock:
            rows1, diag1 = self.service._search_rows_from_provider(
                query="JEE advanced paper",
                provider="brave_html",
                max_rows=5,
            )
            rows2, diag2 = self.service._search_rows_from_provider(
                query="JEE advanced paper",
                provider="brave_html",
                max_rows=5,
            )
        self.assertEqual(len(rows1), 1)
        self.assertEqual(len(rows2), 1)
        self.assertEqual(fetch_mock.call_count, 1)
        self.assertNotEqual(diag1.get("cached"), True)
        self.assertEqual(diag2.get("cached"), True)

    def test_extract_text_from_pdf_bytes_handles_missing_strings_binary(self) -> None:
        blob = b"%PDF-1.5\n1 0 obj\n<<>>\nendobj\n"
        with patch("app.data.local_app_data_service.shutil.which", return_value=None):
            text = self.service._extract_text_from_pdf_bytes(blob)
        self.assertEqual(text, "")

    def test_canonical_web_error_reason_prefers_dns_over_curl_unavailable(self) -> None:
        error = (
            "urlopen:URLError:[Errno 8] nodename nor servname provided, or not known;"
            "curl:RuntimeError:curl_unavailable"
        )
        reason = self.service._canonical_web_error_reason(error)
        self.assertEqual(reason, "dns_resolution_failed")

    def test_looks_like_image_blob_detects_png_signature(self) -> None:
        png_head = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0DIHDR"
        self.assertTrue(self.service._looks_like_image_blob(png_head, mime=""))


if __name__ == "__main__":
    unittest.main()
