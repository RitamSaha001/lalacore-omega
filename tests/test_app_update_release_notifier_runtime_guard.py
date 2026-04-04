import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.automation.state_manager import AutomationStateManager
from services.app_update_release_notifier import AppUpdateReleaseNotifierService


class AppUpdateReleaseNotifierRuntimeGuardTests(unittest.TestCase):
    def test_scheduled_release_poll_is_disabled_outside_production_without_override(self) -> None:
        csv_text = (
            "enabled,app_id,channel,audience,platform,version,build_number,apk_url,force,message\n"
            "TRUE,lalacore_rebuild,stable,all,android,3.1.1,19,https://example.com/app.apk,FALSE,Fresh release\n"
        )

        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {
                "APP_ENV": "development",
                "NODE_ENV": "",
                "RAILWAY_ENVIRONMENT": "",
                "APP_UPDATE_CONFIRMATION_ENABLED": "1",
                "APP_UPDATE_CONFIRMATION_ALLOW_NON_PRODUCTION": "0",
            },
            clear=False,
        ):
            service = AppUpdateReleaseNotifierService(
                state=AutomationStateManager(
                    path=str(Path(tmp) / "LC9_AUTOMATION_STATE.json")
                ),
                fetcher=lambda url: csv_text,
                sheet_url="https://example.com/updates.csv",
            )

            result = asyncio.run(service.poll_for_new_releases(trigger="scheduled"))

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("status"), "DISABLED_NON_PRODUCTION")


if __name__ == "__main__":
    unittest.main()
