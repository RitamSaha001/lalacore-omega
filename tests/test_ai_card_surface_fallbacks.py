import unittest
import sys
import types


def _install_stub(name: str, **attrs) -> None:
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module


_install_stub(
    "grading_engine",
    GradingError=Exception,
    GradingValidationError=Exception,
    evaluate_attempt=lambda *args, **kwargs: {},
)
_install_stub("question_repair_engine", QuestionRepairEngine=object)
_install_stub(
    "latex_sanitizer",
    QuestionStructureError=Exception,
    sanitize_latex=lambda value: value,
    sanitize_question_payload=lambda value: value,
    validate_question_structure=lambda value: value,
)
_install_stub("services")
_install_stub("services.atlas_incident_email_service", AtlasIncidentEmailService=object)
_install_stub("services.atlas_memory_service", AtlasMemoryService=object)
_install_stub("app.storage")
_install_stub("app.storage.sqlite_json_store", SQLiteJsonBlobStore=object)
_install_stub(
    "core.analytics_insight_engine",
    analyze_exam_entry=lambda *args, **kwargs: {},
    class_summary_entry=lambda *args, **kwargs: {},
    student_intelligence_entry=lambda *args, **kwargs: {},
    student_profile_entry=lambda *args, **kwargs: {},
)
_install_stub(
    "core.material_generation_engine",
    material_generation_entry=lambda *args, **kwargs: {},
)

from app.data.local_app_data_service import LocalAppDataService


class AiCardSurfaceFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LocalAppDataService.__new__(LocalAppDataService)
        self.service._str = LocalAppDataService._str.__get__(self.service, LocalAppDataService)

    def test_teacher_dashboard_fallback_uses_card_data(self) -> None:
        text = self.service._build_card_surface_fallback_answer(
            function="teacher_dashboard_review",
            card={
                "attention_students": [
                    {"name": "Railway Audit Student", "issue": "low accuracy in thermodynamics"}
                ],
                "recommended_focus": "Thermodynamics",
            },
        )

        self.assertIn("Railway Audit Student", text)
        self.assertIn("Thermodynamics", text)

    def test_study_material_grounding_includes_notes(self) -> None:
        text = self.service._build_ai_card_surface_grounding(
            function="study_material_chat",
            card={
                "title": "Rotational Motion Notes",
                "subject": "Physics",
                "chapter": "Rotational Motion",
                "material_notes": "Entropy, Carnot engine, torque, angular momentum.",
            },
        )

        self.assertIn("Rotational Motion", text)
        self.assertIn("torque", text)

    def test_generic_unresolved_answer_triggers_surface_fallback(self) -> None:
        self.assertTrue(
            self.service._should_use_card_surface_fallback(
                function="analytics_review",
                answer="[UNRESOLVED]",
                explanation="",
            )
        )


if __name__ == "__main__":
    unittest.main()
