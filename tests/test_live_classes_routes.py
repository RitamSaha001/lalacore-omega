import os
import unittest

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


def _restore_env(key: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
