import os
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

os.environ.setdefault("OTP_EMAIL_ENABLED", "false")

from app.main import app  # noqa: E402


class LiveClassesRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_live_token_and_session_endpoints(self) -> None:
        token = self.client.post(
            "/live/token",
            json={
                "class_id": "math_live_01",
                "user_id": "teacher_01",
                "display_name": "Dr Sharma",
                "role": "teacher",
                "title": "Definite Integration",
                "teacher_name": "Dr Sharma",
                "subject": "Mathematics",
                "topic": "Definite Integration",
            },
        )
        self.assertEqual(token.status_code, 200)
        body = token.json()
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertTrue(body.get("token"))

        session = self.client.get(
            "/class/session",
            params={
                "class_id": "math_live_01",
                "title": "Definite Integration",
                "teacher_name": "Dr Sharma",
                "subject": "Mathematics",
                "topic": "Definite Integration",
            },
        )
        self.assertEqual(session.status_code, 200)
        session_body = session.json()
        self.assertEqual(session_body.get("title"), "Definite Integration")
        self.assertEqual(session_body.get("teacher_name"), "Dr Sharma")

    def test_join_request_event_and_approval_flow(self) -> None:
        with self.client.websocket_connect(
            "/class/events?class_id=chem_live_01&user_id=teacher_01&token=test"
        ) as teacher_events:
            initial = teacher_events.receive_json()
            self.assertEqual(initial.get("type"), "waiting_room_snapshot")

            request = self.client.post(
                "/class/join_request",
                json={
                    "class_id": "chem_live_01",
                    "user_id": "student_01",
                    "user_name": "Ritam",
                    "role": "student",
                    "camera_enabled": True,
                    "mic_enabled": True,
                    "device_info": {"platform": "android"},
                },
            )
            self.assertEqual(request.status_code, 200)
            request_body = request.json()
            self.assertEqual(request_body.get("status"), "PENDING")

            join_event = teacher_events.receive_json()
            snapshot_event = teacher_events.receive_json()
            self.assertEqual(join_event.get("type"), "join_request_received")
            self.assertEqual(snapshot_event.get("type"), "waiting_room_snapshot")
            self.assertEqual(len(snapshot_event.get("requests", [])), 1)

            approve = self.client.post(
                "/class/admit",
                json={"class_id": "chem_live_01", "user_id": "student_01"},
            )
            self.assertEqual(approve.status_code, 200)
            self.assertEqual(approve.json().get("status"), "APPROVED")

            approved_event = teacher_events.receive_json()
            removed_event = teacher_events.receive_json()
            cleared_snapshot = teacher_events.receive_json()
            self.assertEqual(approved_event.get("type"), "join_approved")
            self.assertEqual(removed_event.get("type"), "join_request_removed")
            self.assertEqual(cleared_snapshot.get("type"), "waiting_room_snapshot")
            self.assertEqual(cleared_snapshot.get("requests"), [])

    def test_sync_and_fallback_signal_websockets(self) -> None:
        with self.client.websocket_connect(
            "/class/sync?class_id=phy_live_01&user_id=teacher_01&token=test"
        ) as teacher_sync, self.client.websocket_connect(
            "/class/sync?class_id=phy_live_01&user_id=student_01&token=test"
        ) as student_sync:
            teacher_sync.send_json(
                {
                    "type": "whiteboard_grant",
                    "class_id": "phy_live_01",
                    "sender_id": "teacher_01",
                    "target_user_id": "student_01",
                    "timestamp": "2026-03-08T00:00:00Z",
                    "metadata": {},
                }
            )
            sync_message = student_sync.receive_json()
            self.assertEqual(sync_message.get("type"), "whiteboard_grant")
            mirrored_grant = teacher_sync.receive_json()
            self.assertEqual(mirrored_grant.get("type"), "whiteboard_grant")
            state = self.client.get(
                "/class/state",
                params={"class_id": "phy_live_01", "user_id": "student_01"},
            )
            self.assertEqual(state.status_code, 200)
            self.assertTrue(state.json().get("whiteboard_access"))
            self.assertEqual(
                state.json().get("active_whiteboard_user_id"), "student_01"
            )
            self.assertEqual(state.json().get("whiteboard_surface_style"), "classic")

            teacher_sync.send_json(
                {
                    "type": "whiteboard_surface_changed",
                    "class_id": "phy_live_01",
                    "sender_id": "teacher_01",
                    "timestamp": "2026-03-08T00:00:00Z",
                    "metadata": {"surface": "document"},
                }
            )
            surface_update = student_sync.receive_json()
            self.assertEqual(surface_update.get("type"), "whiteboard_surface_changed")
            mirrored_surface = teacher_sync.receive_json()
            self.assertEqual(
                mirrored_surface.get("type"), "whiteboard_surface_changed"
            )

            stroke_payload = {
                "type": "whiteboard_stroke",
                "class_id": "phy_live_01",
                "sender_id": "student_01",
                "target_user_id": "student_01",
                "timestamp": "2026-03-08T00:00:01Z",
                "metadata": {
                    "points": [{"x": 0.1, "y": 0.2}, {"x": 0.7, "y": 0.8}],
                    "color": 4278190335,
                    "width": 3,
                },
            }
            student_sync.send_json(stroke_payload)
            mirrored = teacher_sync.receive_json()
            self.assertEqual(mirrored.get("type"), "whiteboard_stroke")
            echoed_stroke = student_sync.receive_json()
            self.assertEqual(echoed_stroke.get("type"), "whiteboard_stroke")

            state_after_stroke = self.client.get(
                "/class/state",
                params={"class_id": "phy_live_01", "user_id": "teacher_01"},
            )
            self.assertEqual(state_after_stroke.status_code, 200)
            self.assertEqual(
                len(state_after_stroke.json().get("whiteboard_strokes", [])),
                1,
            )
            self.assertEqual(
                state_after_stroke.json().get("whiteboard_surface_style"),
                "document",
            )

            teacher_sync.send_json(
                {
                    "type": "whiteboard_snapshot",
                    "class_id": "phy_live_01",
                    "sender_id": "teacher_01",
                    "timestamp": "2026-03-08T00:00:01Z",
                    "metadata": {
                        "surface": "document",
                        "strokes": [
                            {
                                "points": [{"x": 0.2, "y": 0.3}, {"x": 0.8, "y": 0.7}],
                                "color": 4278190335,
                                "width": 4,
                                "tool": "rectangle",
                            }
                        ],
                    },
                }
            )
            snapshot_echo = student_sync.receive_json()
            self.assertEqual(snapshot_echo.get("type"), "whiteboard_snapshot")
            state_after_snapshot = self.client.get(
                "/class/state",
                params={"class_id": "phy_live_01", "user_id": "teacher_01"},
            )
            self.assertEqual(state_after_snapshot.status_code, 200)
            strokes = state_after_snapshot.json().get("whiteboard_strokes", [])
            self.assertEqual(len(strokes), 1)
            self.assertEqual(strokes[0].get("tool"), "rectangle")

            teacher_sync.send_json(
                {
                    "type": "whiteboard_operation",
                    "class_id": "phy_live_01",
                    "sender_id": "teacher_01",
                    "timestamp": "2026-03-08T00:00:01Z",
                    "metadata": {
                        "operation": {
                            "id": "op_import_1",
                            "kind": "import_document",
                            "actor_id": "teacher_01",
                            "lamport": 7,
                            "timestamp": "2026-03-08T00:00:01Z",
                            "payload": {
                                "active_page_id": "doc_pg_1",
                                "pages": [
                                    {
                                        "id": "doc_pg_1",
                                        "document_id": "doc_1",
                                        "page_number": 1,
                                        "title": "Sheet 1",
                                        "source_label": "Worksheet",
                                        "background_data_url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9sM4P6gAAAAASUVORK5CYII=",
                                        "revision": 1,
                                        "width": 1,
                                        "height": 1,
                                    }
                                ],
                            },
                        }
                    },
                }
            )
            operation_echo = student_sync.receive_json()
            self.assertEqual(operation_echo.get("type"), "whiteboard_operation")
            state_after_operation = self.client.get(
                "/class/state",
                params={"class_id": "phy_live_01", "user_id": "teacher_01"},
            )
            self.assertEqual(state_after_operation.status_code, 200)
            operation_body = state_after_operation.json()
            self.assertEqual(operation_body.get("active_whiteboard_page_id"), "doc_pg_1")
            self.assertEqual(len(operation_body.get("whiteboard_document_pages", [])), 1)
            self.assertEqual(operation_body.get("whiteboard_clock"), 7)

            teacher_sync.send_json(
                {
                    "type": "whiteboard_clear",
                    "class_id": "phy_live_01",
                    "sender_id": "teacher_01",
                    "timestamp": "2026-03-08T00:00:02Z",
                    "metadata": {},
                }
            )
            cleared = student_sync.receive_json()
            self.assertEqual(cleared.get("type"), "whiteboard_clear")
            state_after_clear = self.client.get(
                "/class/state",
                params={"class_id": "phy_live_01", "user_id": "teacher_01"},
            )
            self.assertEqual(state_after_clear.status_code, 200)
            self.assertEqual(
                state_after_clear.json().get("whiteboard_strokes", []), []
            )

        with self.client.websocket_connect("/class/fallback_signal") as ws_a, self.client.websocket_connect(
            "/class/fallback_signal"
        ) as ws_b:
            ws_a.send_json(
                {
                    "type": "join",
                    "room": "phy_live_01",
                    "user_id": "teacher_01",
                    "token": "a",
                    "provider": "webrtc",
                }
            )
            self.assertEqual(ws_a.receive_json().get("type"), "ready")

            ws_b.send_json(
                {
                    "type": "join",
                    "room": "phy_live_01",
                    "user_id": "student_01",
                    "token": "b",
                    "provider": "webrtc",
                }
            )
            self.assertEqual(ws_b.receive_json().get("type"), "ready")
            self.assertEqual(ws_a.receive_json().get("type"), "peer_joined")

            ws_a.send_json(
                {
                    "type": "offer",
                    "room": "phy_live_01",
                    "user_id": "teacher_01",
                    "sdp": "offer",
                    "sdp_type": "offer",
                }
            )
            forwarded = ws_b.receive_json()
            self.assertEqual(forwarded.get("type"), "offer")
            self.assertEqual(forwarded.get("sdp"), "offer")

    def test_livekit_token_mode(self) -> None:
        previous_provider = os.environ.get("LIVE_CLASSES_RTC_PROVIDER")
        previous_key = os.environ.get("LIVEKIT_API_KEY")
        previous_secret = os.environ.get("LIVEKIT_API_SECRET")
        previous_url = os.environ.get("LIVEKIT_WS_URL")
        try:
            os.environ["LIVE_CLASSES_RTC_PROVIDER"] = "livekit"
            os.environ["LIVEKIT_API_KEY"] = "devkey"
            os.environ["LIVEKIT_API_SECRET"] = "secret"
            os.environ["LIVEKIT_WS_URL"] = "ws://localhost:7880"

            token = self.client.post(
                "/live/token",
                json={
                    "class_id": "math_live_02",
                    "user_id": "student_01",
                    "display_name": "Ritam",
                    "role": "student",
                },
            )
            self.assertEqual(token.status_code, 200)
            body = token.json()
            self.assertEqual(body.get("provider"), "livekit")
            self.assertEqual(body.get("ws_url"), "ws://localhost:7880")
            self.assertTrue(body.get("token"))
        finally:
            _restore_env("LIVE_CLASSES_RTC_PROVIDER", previous_provider)
            _restore_env("LIVEKIT_API_KEY", previous_key)
            _restore_env("LIVEKIT_API_SECRET", previous_secret)
            _restore_env("LIVEKIT_WS_URL", previous_url)

    def test_server_authoritative_control_state_updates(self) -> None:
        self.client.post(
            "/class/lock",
            json={"class_id": "math_live_controls", "locked": True},
        )
        self.client.post(
            "/class/chat",
            json={"class_id": "math_live_controls", "enabled": False},
        )
        self.client.post(
            "/class/waiting_room",
            json={"class_id": "math_live_controls", "enabled": False},
        )
        self.client.post(
            "/class/recording",
            json={"class_id": "math_live_controls", "enabled": True},
        )
        self.client.post(
            "/class/mute",
            json={
                "class_id": "math_live_controls",
                "user_id": "student_42",
                "muted": True,
            },
        )
        self.client.post(
            "/class/breakout/move",
            json={
                "class_id": "math_live_controls",
                "user_id": "student_42",
                "room_id": "breakout_2",
            },
        )
        self.client.post(
            "/class/whiteboard/access",
            json={
                "class_id": "math_live_controls",
                "user_id": "student_42",
                "enabled": True,
            },
        )

        session = self.client.get(
            "/class/session",
            params={"class_id": "math_live_controls"},
        )
        self.assertEqual(session.status_code, 200)
        session_body = session.json()
        self.assertTrue(session_body.get("meeting_locked"))
        self.assertFalse(session_body.get("chat_enabled"))
        self.assertFalse(session_body.get("waiting_room_enabled"))
        self.assertTrue(session_body.get("is_recording"))

        state = self.client.get(
            "/class/state",
            params={"class_id": "math_live_controls", "user_id": "student_42"},
        )
        self.assertEqual(state.status_code, 200)
        state_body = state.json()
        self.assertTrue(state_body.get("muted"))
        self.assertEqual(state_body.get("active_breakout_room_id"), "breakout_2")
        self.assertTrue(state_body.get("whiteboard_access"))

    def test_transcription_stream_accepts_connection_and_echoes_text_payloads(self) -> None:
        with self.client.websocket_connect("/transcription/stream?token=test") as ws:
            ws.send_json(
                {
                    "speaker_id": "teacher_01",
                    "speaker_name": "Dr Sharma",
                    "text": "Consider the limit as x tends to zero.",
                    "timestamp": "2026-03-12T10:00:00Z",
                    "confidence": 0.98,
                }
            )
            message = ws.receive_json()
            self.assertEqual(message.get("speaker_id"), "teacher_01")
            self.assertEqual(message.get("speaker_name"), "Dr Sharma")
            self.assertEqual(
                message.get("text"), "Consider the limit as x tends to zero."
            )
            self.assertEqual(message.get("timestamp"), "2026-03-12T10:00:00Z")

    def test_agent_route_returns_structured_plan(self) -> None:
        mocked_result = {
            "final_answer": """
            {
              "type": "multi_step_plan",
              "goal": "Find a tough PYQ and make it a 2 minute poll.",
              "plan_id": "plan_pyq_poll_1",
              "summary": "Prepared a poll and a reminder.",
              "teacher_notice": "Teacher can approve or stop execution.",
              "requires_confirmation": true,
              "needs_more_info": false,
              "follow_up_questions": [],
              "proposed_tools": ["create_poll", "set_reminder"],
              "steps": [
                {
                  "id": "step_1",
                  "tool": "create_poll",
                  "title": "Launch tough PYQ poll",
                  "detail": "Create a 2-minute question on hyperbola.",
                  "risk": "medium",
                  "requires_confirmation": true,
                  "args": {
                    "question": "Find the eccentricity of x^2/16 - y^2/9 = 1.",
                    "options": ["1", "5/4", "3/2", "2"],
                    "correct_index": 1,
                    "timer_seconds": 120
                  },
                  "depends_on": [],
                  "on_failure": {"strategy": "replan"}
                },
                {
                  "id": "step_2",
                  "tool": "set_reminder",
                  "title": "Remind teacher to end class",
                  "detail": "Trigger after 10 minutes.",
                  "risk": "low",
                  "requires_confirmation": false,
                  "args": {
                    "note": "End the class in 10 minutes",
                    "delay_minutes": 10
                  },
                  "depends_on": ["step_1"],
                  "on_failure": {"strategy": "retry"}
                }
              ]
            }
            """,
            "citations": [
                {"title": "PYQ source", "url": "https://example.com/pyq"}
            ],
            "web_retrieval": {"enabled": True, "context_injected": True},
            "sources_consulted": ["example.com"],
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Find a tough PYQ and make it a 2 minute poll.",
                    "context": {"class_metadata": {"topic": "Hyperbola"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(body.get("plan_id"), "plan_pyq_poll_1")
        self.assertEqual(body.get("summary"), "Prepared a poll and a reminder.")
        self.assertTrue(body.get("requires_confirmation"))
        self.assertEqual(len(body.get("steps", [])), 2)
        self.assertEqual(len(body.get("actions", [])), 2)
        self.assertEqual(body["actions"][0]["tool"], "create_poll")
        self.assertEqual(body["actions"][1]["tool"], "set_reminder")
        self.assertFalse(body.get("needs_more_info"))
        self.assertEqual(
            body.get("proposed_tools"), ["create_poll", "set_reminder"]
        )
        self.assertEqual(body.get("sources_consulted"), ["example.com"])

    def test_agent_route_can_request_follow_up_questions(self) -> None:
        mocked_result = {
            "final_answer": """
            {
              "type": "needs_more_info",
              "goal": "Schedule the next class.",
              "summary": "I can schedule the next class, but I still need the time.",
              "teacher_notice": "Reply in the same Atlas chat and I will continue.",
              "requires_confirmation": false,
              "needs_more_info": true,
              "follow_up_questions": [
                "When should I schedule the next class?",
                "What title should students see?"
              ],
              "proposed_tools": ["schedule_next_class"],
              "actions": []
            }
            """,
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Schedule the next class.",
                    "context": {"class_metadata": {"topic": "Hyperbola"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("needs_more_info"))
        self.assertEqual(
            body.get("follow_up_questions"),
            [
                "When should I schedule the next class?",
                "What title should students see?",
            ],
        )
        self.assertEqual(body.get("proposed_tools"), ["schedule_next_class"])
        self.assertEqual(body.get("actions"), [])

    def test_agent_route_can_request_recurring_schedule_details(self) -> None:
        mocked_result = {
            "final_answer": """
            {
              "summary": "Atlas can build a recurring class plan, but it needs the first time and repeat pattern.",
              "teacher_notice": "Reply in the same Atlas chat and I will continue.",
              "requires_confirmation": false,
              "needs_more_info": true,
              "follow_up_questions": [
                "When should the first class happen?",
                "How should it repeat?"
              ],
              "proposed_tools": ["create_recurring_class_plan"],
              "actions": []
            }
            """,
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Create a recurring class plan for this batch.",
                    "context": {"class_metadata": {"topic": "Hyperbola"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("needs_more_info"))
        self.assertEqual(
            body.get("proposed_tools"), ["create_recurring_class_plan"]
        )
        self.assertEqual(body.get("actions"), [])

    def test_agent_route_recovers_plan_from_unsafe_candidate_answer(self) -> None:
        mocked_result = {
            "final_answer": "Uncertain answer: verification failed under high risk. Please retry with a stronger model.",
            "unsafe_candidate_answer": """
            {
              "type": "multi_step_plan",
              "goal": "Admit all, mute all, and write Binomial Theorem",
              "plan_id": "live_agent_unsafe_1",
              "summary": "Admit waiting students, mute all, then write the heading on the board.",
              "steps": [
                {
                  "id": "step_1",
                  "tool": "approve_waiting_all",
                  "title": "Admit waiting students",
                  "detail": "Admit everyone currently waiting.",
                  "risk": "low",
                  "args": {}
                },
                {
                  "id": "step_2",
                  "tool": "mute_all",
                  "title": "Mute all students",
                  "detail": "Mute the room before writing on the board.",
                  "risk": "low",
                  "args": {},
                  "depends_on": ["step_1"]
                },
                {
                  "id": "step_3",
                  "tool": "draw_text_on_whiteboard",
                  "title": "Write board heading",
                  "detail": "Write Binomial Theorem on the board.",
                  "risk": "low",
                  "args": {"text": "Binomial Theorem"},
                  "depends_on": ["step_2"]
                }
              ]
            }
            """,
            "reasoning": "The candidate plan was preserved in unsafe_candidate_answer.",
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Admit all waiting students, mute all, and write Binomial Theorem on the board.",
                    "context": {"class_metadata": {"topic": "Binomial Theorem"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(body.get("steps", [])[0].get("tool"), "approve_waiting_all")
        self.assertEqual(body.get("steps", [])[2].get("tool"), "draw_text_on_whiteboard")

    def test_agent_route_recovers_plan_from_reasoning_tool_mentions(self) -> None:
        mocked_result = {
            "final_answer": '{"type":"multi_step_plan","goal":"Admit all, mute all, and write on the board",',
            "reasoning": (
                "Reasoning: We should use `approve_waiting_all` first, then `mute_all`, "
                "and finally `draw_text_on_whiteboard` to write Binomial Theorem on the board."
            ),
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Admit all waiting students, mute all, and write Binomial Theorem on the board.",
                    "context": {"class_metadata": {"topic": "Binomial Theorem"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(body.get("recovery_mode"), "tool_mentions_from_reasoning")
        self.assertEqual(body.get("steps", [])[0].get("tool"), "approve_waiting_all")
        self.assertEqual(body.get("steps", [])[1].get("tool"), "mute_all")
        self.assertEqual(body.get("steps", [])[2].get("tool"), "draw_text_on_whiteboard")
        self.assertEqual(
            body.get("steps", [])[2].get("args", {}).get("text"),
            "Binomial Theorem",
        )

    def test_agent_route_can_request_homework_details(self) -> None:
        mocked_result = {"final_answer": "{}"}
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Create a homework for this class.",
                    "context": {"class_metadata": {"topic": "Hyperbola"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("needs_more_info"))
        self.assertEqual(body.get("proposed_tools"), ["create_homework_assignment"])
        self.assertIn("Which chapter or topic should the homework cover?", body.get("follow_up_questions", []))

    def test_agent_route_can_request_revision_pack_details(self) -> None:
        mocked_result = {"final_answer": "{}"}
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Make a revision pack for this batch.",
                    "context": {"class_metadata": {"topic": "Hyperbola"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("needs_more_info"))
        self.assertEqual(body.get("proposed_tools"), ["create_revision_pack"])
        self.assertIn(
            "Which chapter or topic should the revision pack cover?",
            body.get("follow_up_questions", []),
        )

    def test_agent_route_understands_natural_classroom_sequence_request(self) -> None:
        mocked_result = {"final_answer": "{}", "reasoning": ""}
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Can you let everyone in, quiet the room, and then put Newton's laws of motion on the board?",
                    "context": {"class_metadata": {"topic": "Newton's Laws of Motion"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(
            [step.get("tool") for step in body.get("steps", [])],
            ["approve_waiting_all", "mute_all", "draw_text_on_whiteboard"],
        )
        self.assertEqual(
            body.get("steps", [])[2].get("args", {}).get("text"),
            "Newton's laws of motion",
        )
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_agent_route_understands_natural_schedule_request_needs_time(self) -> None:
        mocked_result = {"final_answer": "{}", "reasoning": ""}
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Can you line up the next class and ping the students before it?",
                    "context": {"class_metadata": {"topic": "Hyperbola"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("needs_more_info"))
        self.assertIn("schedule_next_class", body.get("proposed_tools", []))
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_agent_route_understands_blurry_video_audio_issue_as_diagnosis(self) -> None:
        mocked_result = {"final_answer": "{}", "reasoning": ""}
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/agent",
                json={
                    "instruction": "Atlas, Karthik says the video is blurry and the sound quality is bad.",
                    "context": {"class_metadata": {"topic": "Electrostatics"}},
                    "authority_level": "assist",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("type"), "single_action")
        self.assertEqual(body.get("tool"), "report_system_issue")
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")


def _restore_env(key: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
