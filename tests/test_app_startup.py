import asyncio
import os
import time
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app import routes as app_routes


class AppStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_does_not_block_on_atlas_warmup(self) -> None:
        import app.main as app_main

        async def _slow_warmup():
            await asyncio.sleep(0.2)
            return {"ok": True}

        with patch.dict(
            os.environ,
            {
                "ATLAS_MAINTENANCE_ENABLED": "0",
                "APP_UPDATE_CONFIRMATION_ENABLED": "0",
                "LC9_DISABLE_DISCOVERY": "1",
            },
            clear=False,
        ), patch("app.main.initialize_keys"), patch(
            "app.main.Database.init",
            new=AsyncMock(return_value=None),
        ), patch(
            "app.main.warm_atlas_runtime",
            new=AsyncMock(side_effect=_slow_warmup),
        ):
            started = time.perf_counter()
            await app_main.startup_event()
            elapsed = time.perf_counter() - started
            self.assertLess(elapsed, 0.1)
            task = app_main._atlas_runtime_warm_task
            self.assertIsNotNone(task)
            self.assertFalse(task.done())
            await app_main.shutdown_event()

    async def test_app_action_uses_forwarded_https_base_url(self) -> None:
        import app.main as app_main

        with patch.object(
            app_routes._APP_DATA,
            "handle_action",
            new=AsyncMock(return_value={"ok": True, "status": "SUCCESS"}),
        ) as mocked:
            client = TestClient(app_main.app)
            response = client.post(
                "/app/action",
                json={"action": "create_quiz"},
                headers={
                    "x-forwarded-proto": "https",
                    "x-forwarded-host": "lalacore-omega-production.up.railway.app",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = mocked.await_args.args[0]
        self.assertEqual(
            payload.get("_request_base_url"),
            "https://lalacore-omega-production.up.railway.app",
        )


if __name__ == "__main__":
    unittest.main()
