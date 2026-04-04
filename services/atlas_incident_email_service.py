from __future__ import annotations

import base64
import hashlib
import json
import os
import smtplib
import ssl
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any
from urllib.request import Request, urlopen

from core.automation.state_manager import AutomationStateManager
from services.email_branding import (
    EmailTheme,
    build_email_document,
    bullet_list,
    buttons,
    code_block,
    detail_rows,
    esc,
    metric_grid,
    nl2br,
    paragraph,
    pill,
    section,
)


class AtlasIncidentEmailService:
    """SMTP-backed support reporter reused by Atlas incident flows."""

    RELEASE_CONFIRMATION_SCOPE = "atlas_release_confirmation_mail"
    RELEASE_THEME = EmailTheme(
        accent="#2457ff",
        accent_soft="#eaf0ff",
        hero_from="#10214d",
        hero_to="#2d7eff",
        border="#d8e2ff",
    )
    OPERATOR_THEME = EmailTheme(
        accent="#5b3df5",
        accent_soft="#efeafd",
        hero_from="#1d1438",
        hero_to="#6e54ff",
        border="#e0d7ff",
    )
    ASSIGNMENT_THEME = EmailTheme(
        accent="#038b72",
        accent_soft="#e5fbf4",
        hero_from="#0f3d3c",
        hero_to="#19b28f",
        border="#d5f1ea",
    )
    ANALYTICS_THEME = EmailTheme(
        accent="#0d8abf",
        accent_soft="#e6f7ff",
        hero_from="#0a2338",
        hero_to="#1592cf",
        border="#d4edf9",
    )
    PERFORMANCE_THEME = EmailTheme(
        accent="#b56a00",
        accent_soft="#fff3dd",
        hero_from="#362012",
        hero_to="#de8c16",
        border="#f1dfbd",
    )
    INCIDENT_THEME = EmailTheme(
        accent="#d1495b",
        accent_soft="#fdeced",
        hero_from="#351621",
        hero_to="#d1495b",
        border="#f5d3d8",
    )

    def __init__(
        self,
        *,
        state: AutomationStateManager | None = None,
    ) -> None:
        self._state = state or AutomationStateManager(
            os.getenv(
                "ATLAS_RELEASE_CONFIRMATION_STATE_PATH",
                "data/lc9/LC9_AUTOMATION_STATE.json",
            )
        )

    def smtp_configured(self) -> bool:
        if self._apps_script_configured():
            return True
        sender = self._sender_email()
        password = self._sender_password()
        return bool(sender and password)

    def _from_name(self) -> str:
        return "God of Maths"

    def _runtime_is_production_like(self) -> bool:
        candidates = (
            os.getenv("APP_ENV", ""),
            os.getenv("NODE_ENV", ""),
            os.getenv("RAILWAY_ENVIRONMENT", ""),
        )
        return any(
            str(value or "").strip().lower() == "production"
            for value in candidates
        )

    def _release_confirmation_allowed(self) -> bool:
        if self._runtime_is_production_like():
            return True
        raw = os.getenv("ATLAS_RELEASE_CONFIRMATION_ALLOW_NON_PRODUCTION", "")
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def _release_confirmation_duplicates_allowed(self) -> bool:
        raw = os.getenv("ATLAS_RELEASE_CONFIRMATION_ALLOW_DUPLICATES", "")
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def _release_confirmation_sent_map(self) -> dict[str, str]:
        row = self._state.checkpoint_row(self.RELEASE_CONFIRMATION_SCOPE)
        raw_map = row.get("sent_map")
        if not isinstance(raw_map, dict):
            return {}
        normalized: dict[str, str] = {}
        for raw_key, raw_ts in raw_map.items():
            key = str(raw_key or "").strip()
            ts = str(raw_ts or "").strip()
            if key and ts:
                normalized[key] = ts
        return normalized

    def _release_confirmation_key(
        self,
        *,
        releases: list[dict[str, Any]],
        recipients: list[str],
        sheet_url: str,
    ) -> str:
        payload = {
            "sheet_url": str(sheet_url or "").strip(),
            "recipients": sorted(
                str(item or "").strip().lower()
                for item in recipients
                if str(item or "").strip()
            ),
            "releases": [
                dict(item)
                for item in releases
                if isinstance(item, dict)
            ],
        }
        encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _mark_release_confirmation_sent(self, confirmation_key: str) -> None:
        key = str(confirmation_key or "").strip()
        if not key:
            return
        sent_map = self._release_confirmation_sent_map()
        sent_map[key] = datetime.now(timezone.utc).isoformat()
        if len(sent_map) > 500:
            sent_map = dict(list(sent_map.items())[-500:])
        self._state.checkpoint(
            self.RELEASE_CONFIRMATION_SCOPE,
            sent_map=sent_map,
        )

    def _footer_html(self, *, purpose: str, theme: EmailTheme) -> str:
        return (
            f'<div style="margin:0 0 10px;color:{theme.footer_ink};font-size:12px;'
            'line-height:1.7;">'
            f"{esc(purpose)}"
            "</div>"
            f'<div style="color:{theme.footer_ink};font-size:12px;line-height:1.7;">'
            "Sent by God of Maths through the LalaCore communication system. "
            "Designed for clear learning workflows, calm updates, and dependable delivery."
            "</div>"
        )

    def _brand_orb(self, *, theme: EmailTheme, primary: str, secondary: str) -> str:
        return (
            '<div style="padding:18px;border-radius:24px;background:rgba(255,255,255,0.12);'
            'border:1px solid rgba(255,255,255,0.18);">'
            '<div style="width:148px;height:148px;margin:0 auto 14px;border-radius:50%;'
            f'background:radial-gradient(circle at 30% 30%, {primary} 0%, rgba(255,255,255,0.92) 28%, {secondary} 100%);'
            'box-shadow:0 18px 44px rgba(16,33,77,0.22);"></div>'
            '<div style="text-align:center;color:rgba(255,255,255,0.92);font-size:13px;line-height:1.6;'
            'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
            f'{pill("crafted mail", background="rgba(255,255,255,0.15)", color="#ffffff")}'
            "</div>"
            "</div>"
        )

    def _meta_aside(self, items: list[tuple[str, str]], *, theme: EmailTheme) -> str:
        inner = detail_rows(items)
        if not inner:
            return ""
        return (
            '<div style="padding:18px 18px 6px;border-radius:24px;background:rgba(255,255,255,0.12);'
            'border:1px solid rgba(255,255,255,0.18);backdrop-filter:blur(8px);">'
            f"{inner}"
            "</div>"
        )

    def send_incident_report(
        self,
        *,
        report: dict[str, Any],
        recipient: str | None = None,
    ) -> dict[str, Any]:
        recipients = self._recipient_list(recipient)
        to_email = ", ".join(recipients)
        issue_summary = str(report.get("issue_summary") or "Atlas incident").strip()
        severity = str(report.get("severity") or "medium").strip().upper()
        incident_id = str(report.get("incident_id") or "").strip()
        sender = self._sender_email()
        from_name = self._from_name()

        msg = EmailMessage()
        msg["Subject"] = f"[{severity}] Atlas incident - {issue_summary[:96]}"
        if sender:
            msg["From"] = f"{from_name} <{sender}>"
        msg["To"] = to_email
        reply_to = str(
            report.get("reply_to_email")
            or ((report.get("reporter") or {}).get("email") if isinstance(report.get("reporter"), dict) else "")
            or ""
        ).strip()
        if reply_to:
            msg["Reply-To"] = reply_to
        msg.set_content(self._build_text_body(report))
        msg.add_alternative(
            self._build_incident_report_html(report),
            subtype="html",
        )
        try:
            msg.add_attachment(
                json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8"),
                maintype="application",
                subtype="json",
                filename=f"{incident_id or 'atlas_incident'}.json",
            )
        except Exception:
            pass
        return self._send_message(
            msg,
            recipients=recipients,
            from_name_override=self._from_name(),
        )

    def send_release_confirmation(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        recipient: str | None = None,
        trigger: str = "scheduled",
        checked_at: str | None = None,
    ) -> dict[str, Any]:
        if not self._release_confirmation_allowed():
            return {
                "ok": True,
                "sent": False,
                "recipient": "",
                "recipients": [],
                "message": (
                    "Release confirmation skipped outside production-like runtime. "
                    "Set ATLAS_RELEASE_CONFIRMATION_ALLOW_NON_PRODUCTION=true to "
                    "override this guard intentionally."
                ),
            }
        recipients = self._recipient_list(recipient)
        to_email = ", ".join(recipients)
        if not recipients:
            return {
                "ok": True,
                "sent": False,
                "recipient": "",
                "recipients": [],
                "message": (
                    "Release confirmation skipped because support recipients "
                    "are not configured"
                ),
            }
        cleaned_releases = [
            dict(item)
            for item in releases
            if isinstance(item, dict)
        ]
        if not cleaned_releases:
            return {
                "ok": False,
                "sent": False,
                "recipient": to_email,
                "message": "No release rows were provided for confirmation mail",
            }
        confirmation_key = self._release_confirmation_key(
            releases=cleaned_releases,
            recipients=recipients,
            sheet_url=sheet_url,
        )
        if (
            confirmation_key
            and confirmation_key in self._release_confirmation_sent_map()
            and not self._release_confirmation_duplicates_allowed()
        ):
            return {
                "ok": True,
                "sent": False,
                "recipient": to_email,
                "recipients": recipients,
                "message": (
                    "Release confirmation skipped because this exact release batch "
                    "was already sent earlier."
                ),
            }
        version_bits: list[str] = []
        for release in cleaned_releases[:3]:
            version = str(release.get("version") or "").strip()
            build = str(release.get("build_number") or "").strip()
            audience = str(release.get("audience") or "").strip() or "all"
            label = version or "unknown-version"
            if build:
                label = f"{label}+{build}"
            version_bits.append(f"{audience}:{label}")
        subject_suffix = ", ".join(version_bits) if version_bits else "new release"
        if len(cleaned_releases) > 3:
            subject_suffix = f"{subject_suffix} (+{len(cleaned_releases) - 3} more)"
        msg = EmailMessage()
        msg["Subject"] = f"[RELEASE] Atlas app update published - {subject_suffix[:120]}"
        msg.set_content(
            self._build_release_confirmation_body(
                releases=cleaned_releases,
                sheet_url=sheet_url,
                trigger=trigger,
                checked_at=checked_at,
            )
        )
        msg.add_alternative(
            self._build_release_confirmation_html(
                releases=cleaned_releases,
                sheet_url=sheet_url,
                trigger=trigger,
                checked_at=checked_at,
            ),
            subtype="html",
        )
        manifest = {
            "trigger": trigger,
            "checked_at": checked_at or "",
            "sheet_url": sheet_url,
            "release_count": len(cleaned_releases),
            "releases": cleaned_releases,
        }
        try:
            msg.add_attachment(
                json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
                maintype="application",
                subtype="json",
                filename="atlas_release_confirmation.json",
            )
        except Exception:
            pass
        result = self._send_message(
            msg,
            recipients=recipients,
            from_name_override=self._from_name(),
        )
        if bool(result.get("ok")) and bool(result.get("sent")):
            self._mark_release_confirmation_sent(confirmation_key)
        return result

    def send_release_announcement(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        recipients: list[str] | None = None,
        trigger: str = "scheduled",
        checked_at: str | None = None,
    ) -> dict[str, Any]:
        target_recipients = [
            email
            for email in list(recipients or [])
            if str(email).strip()
        ]
        if not target_recipients:
            return {
                "ok": True,
                "sent": False,
                "message": "No signed-in user email recipients were available",
                "recipients": [],
                "sent_recipients": [],
                "failed_recipients": [],
                "sent_count": 0,
                "failed_count": 0,
                "no_deliverable_recipients": True,
            }
        cleaned_releases = [
            dict(item)
            for item in releases
            if isinstance(item, dict)
        ]
        if not cleaned_releases:
            return {
                "ok": False,
                "sent": False,
                "message": "No release rows were provided for announcement mail",
                "recipients": target_recipients,
            }

        subject_suffix = self._release_subject_suffix(cleaned_releases)
        failed_recipients: list[str] = []
        sent_recipients: list[str] = []
        last_message = ""

        for recipient in target_recipients:
            msg = EmailMessage()
            msg["Subject"] = f"LalaCore update available - {subject_suffix[:120]}"
            msg["To"] = str(recipient).strip()
            msg.set_content(
                self._build_release_announcement_body(
                    releases=cleaned_releases,
                    sheet_url=sheet_url,
                    trigger=trigger,
                    checked_at=checked_at,
                    recipient=str(recipient).strip(),
                )
            )
            msg.add_alternative(
                self._build_release_announcement_html(
                    releases=cleaned_releases,
                    sheet_url=sheet_url,
                    trigger=trigger,
                    checked_at=checked_at,
                    recipient=str(recipient).strip(),
                ),
                subtype="html",
            )
            manifest = {
                "trigger": trigger,
                "checked_at": checked_at or "",
                "sheet_url": sheet_url,
                "release_count": len(cleaned_releases),
                "releases": cleaned_releases,
                "recipient": str(recipient).strip(),
            }
            try:
                msg.add_attachment(
                    json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
                    maintype="application",
                    subtype="json",
                    filename="lalacore_release_announcement.json",
                )
            except Exception:
                pass
            result = self._send_message(
                msg,
                recipients=[str(recipient).strip()],
                from_name_override=self._from_name(),
            )
            last_message = str(result.get("message") or "")
            if bool(result.get("ok")):
                sent_recipients.append(str(recipient).strip())
            else:
                failed_recipients.append(str(recipient).strip())

        return {
            "ok": not failed_recipients and bool(sent_recipients),
            "sent": not failed_recipients and bool(sent_recipients),
            "message": last_message or "Release announcement completed",
            "recipients": target_recipients,
            "sent_recipients": sent_recipients,
            "failed_recipients": failed_recipients,
            "sent_count": len(sent_recipients),
            "failed_count": len(failed_recipients),
        }

    def send_assessment_report(
        self,
        *,
        report: dict[str, Any],
        recipient: str | None = None,
    ) -> dict[str, Any]:
        recipients = self._recipient_list(
            recipient
            or os.getenv("ATLAS_ASSESSMENT_REPORT_EMAIL_RECIPIENT", "").strip()
        )
        if not recipients:
            return {
                "ok": True,
                "sent": False,
                "recipient": "",
                "recipients": [],
                "message": (
                    "Assessment report skipped because recipients are not configured"
                ),
            }
        to_email = ", ".join(recipients)
        title = str(report.get("assessment_title") or "Assessment").strip()
        assessment_type = str(report.get("assessment_type") or "Assessment").strip()
        deadline = str(report.get("deadline") or "").strip()
        msg = EmailMessage()
        msg["Subject"] = f"[ASSESSMENT REPORT] {assessment_type} - {title[:96]}"
        msg["To"] = to_email
        msg.set_content(self._build_assessment_report_body(report))
        msg.add_alternative(
            self._build_assessment_report_html(report),
            subtype="html",
        )
        try:
            msg.add_attachment(
                json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8"),
                maintype="application",
                subtype="json",
                filename=f"assessment_report_{title[:32] or 'assessment'}.json",
            )
        except Exception:
            pass
        return self._send_message(
            msg,
            recipients=recipients,
            from_name_override=self._from_name(),
        )

    def send_assessment_submission_report(
        self,
        *,
        report: dict[str, Any],
        recipient: str | None = None,
    ) -> dict[str, Any]:
        recipients = self._recipient_list(
            recipient
            or os.getenv("ATLAS_ASSESSMENT_SUBMISSION_EMAIL_RECIPIENT", "").strip()
            or os.getenv("ATLAS_ASSESSMENT_REPORT_EMAIL_RECIPIENT", "").strip()
        )
        if not recipients:
            return {
                "ok": True,
                "sent": False,
                "recipient": "",
                "recipients": [],
                "message": (
                    "Assessment submission report skipped because recipients are not configured"
                ),
            }
        to_email = ", ".join(recipients)
        title = str(report.get("assessment_title") or "Assessment").strip()
        student_name = str(
            report.get("student_name")
            or report.get("student_id")
            or "Unknown student"
        ).strip()
        attempt_index = int(report.get("attempt_index") or 1)
        submission_kind = str(report.get("submission_kind") or "submission").strip()
        kind_label = (
            f"Reattempt #{attempt_index}"
            if submission_kind == "reattempt"
            else "First attempt"
        )
        msg = EmailMessage()
        msg["Subject"] = (
            f"[ASSESSMENT SUBMISSION] {title[:72]} - {student_name[:40]} - {kind_label}"
        )
        msg["To"] = to_email
        msg.set_content(self._build_assessment_submission_report_body(report))
        msg.add_alternative(
            self._build_assessment_submission_report_html(report),
            subtype="html",
        )
        try:
            msg.add_attachment(
                json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8"),
                maintype="application",
                subtype="json",
                filename=f"assessment_submission_{title[:32] or 'assessment'}.json",
            )
        except Exception:
            pass
        return self._send_message(
            msg,
            recipients=recipients,
            from_name_override=self._from_name(),
        )

    def send_assignment_announcement(
        self,
        *,
        report: dict[str, Any],
        recipient: str | None = None,
    ) -> dict[str, Any]:
        recipients = self._recipient_list(recipient)
        if not recipients:
            return {
                "ok": False,
                "sent": False,
                "message": "Student recipient is missing",
            }
        title = str(report.get("assessment_title") or "Assessment").strip()
        assessment_type = str(report.get("assessment_type") or "Assessment").strip()
        class_name = str(report.get("class_name") or "").strip()
        subject = str(report.get("subject") or "").strip()
        subject_tail = f" - {subject[:40]}" if subject else ""
        class_tail = f" [{class_name[:32]}]" if class_name else ""
        msg = EmailMessage()
        msg["Subject"] = (
            f"[NEW {assessment_type.upper()}] {title[:72]}{subject_tail}{class_tail}"
        )
        msg["To"] = ", ".join(recipients)
        msg.set_content(self._build_assignment_announcement_body(report))
        msg.add_alternative(
            self._build_assignment_announcement_html(report),
            subtype="html",
        )
        try:
            msg.add_attachment(
                json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8"),
                maintype="application",
                subtype="json",
                filename=f"assignment_announcement_{title[:32] or 'assessment'}.json",
            )
        except Exception:
            pass
        return self._send_message(
            msg,
            recipients=recipients,
            from_name_override=self._from_name(),
        )

    def _send_message(
        self,
        msg: EmailMessage,
        *,
        recipients: list[str],
        from_name_override: str,
    ) -> dict[str, Any]:
        recipient = ", ".join(recipients)
        if not recipients:
            return {
                "ok": False,
                "sent": False,
                "message": "Support recipient is missing",
            }
        apps_script_settings = self._apps_script_settings()
        smtp_settings = self._smtp_settings()
        if apps_script_settings["ok"]:
            apps_script_result = self._send_via_apps_script(
                msg,
                recipients=recipients,
                from_name_override=from_name_override,
                settings=apps_script_settings,
            )
            if bool(apps_script_result.get("ok")):
                return apps_script_result
            if not smtp_settings["ok"] or not self._allow_apps_script_smtp_fallback():
                return apps_script_result
        if not smtp_settings["ok"]:
            return {
                "ok": False,
                "sent": False,
                "recipient": recipient,
                "message": str(smtp_settings.get("message") or "SMTP sender credentials are not configured"),
            }
        sender = str(smtp_settings["sender"])
        smtp_host = str(smtp_settings["smtp_host"])
        smtp_port = int(smtp_settings["smtp_port"])
        smtp_security = str(smtp_settings["smtp_security"])
        password = str(smtp_settings["password"])
        if not msg.get("From"):
            msg["From"] = f"{from_name_override} <{sender}>"
        if not msg.get("To"):
            msg["To"] = recipient

        try:
            ssl_ctx = self._ssl_context()
        except Exception:
            ssl_ctx = ssl.create_default_context()
        try:
            if smtp_security in {"ssl", "smtps", "implicit_ssl"}:
                with smtplib.SMTP_SSL(
                    smtp_host,
                    smtp_port,
                    timeout=20,
                    context=ssl_ctx,
                ) as smtp:
                    smtp.ehlo()
                    smtp.login(sender, password)
                    smtp.send_message(msg, to_addrs=recipients)
            else:
                with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp:
                    smtp.ehlo()
                    if smtp_security not in {"none", "plain"}:
                        smtp.starttls(context=ssl_ctx)
                        smtp.ehlo()
                    smtp.login(sender, password)
                    smtp.send_message(msg, to_addrs=recipients)
            return {
                "ok": True,
                "sent": True,
                "recipient": recipient,
                "recipients": recipients,
                "message": "Support email sent",
            }
        except Exception as exc:
            return {
                "ok": False,
                "sent": False,
                "recipient": recipient,
                "recipients": recipients,
                "message": f"Support email send failed: {exc}",
            }

    def _send_via_apps_script(
        self,
        msg: EmailMessage,
        *,
        recipients: list[str],
        from_name_override: str,
        settings: dict[str, Any],
    ) -> dict[str, Any]:
        reply_to = (
            str(msg.get("Reply-To") or "").strip()
            or self._sender_email()
        )
        payload = {
            "secret": str(settings.get("shared_secret") or ""),
            "sender_name": from_name_override,
            "reply_to": reply_to,
            "subject": str(msg.get("Subject") or "").strip(),
            "recipients": recipients,
            "text_body": self._message_plain_text(msg),
            "html_body": self._message_html(msg),
            "attachments": self._message_attachments_payload(msg),
        }
        request = Request(
            str(settings["webhook_url"]),
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "LalaCoreAutoMail/1.0",
            },
            method="POST",
        )
        attempts = max(1, int(settings.get("retry_count") or 0) + 1)
        backoff_seconds = float(settings.get("retry_backoff_seconds") or 0.0)
        last_error: Exception | None = None
        for attempt_index in range(attempts):
            try:
                with urlopen(  # noqa: S310
                    request,
                    timeout=int(settings["timeout_seconds"]),
                    context=self._ssl_context(),
                ) as response:
                    body = response.read().decode("utf-8", errors="replace")
                decoded = json.loads(body) if body.strip() else {}
                success = bool(decoded.get("ok")) or str(decoded.get("status") or "").strip().lower() in {
                    "success",
                    "sent",
                }
                message = str(decoded.get("message") or "Apps Script mail sent").strip()
                if attempt_index > 0 and success:
                    message = f"{message} after retry {attempt_index}"
                return {
                    "ok": success,
                    "sent": success,
                    "recipient": ", ".join(recipients),
                    "recipients": recipients,
                    "message": message,
                    "transport": "apps_script",
                    "response": decoded,
                    "attempts": attempt_index + 1,
                }
            except Exception as exc:
                last_error = exc
                if attempt_index + 1 >= attempts:
                    break
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds * (attempt_index + 1))
        return {
            "ok": False,
            "sent": False,
            "recipient": ", ".join(recipients),
            "recipients": recipients,
            "message": f"Apps Script mail send failed: {last_error}",
            "transport": "apps_script",
            "attempts": attempts,
        }

    def _configured_support_recipient(self) -> str:
        return os.getenv("ATLAS_SUPPORT_EMAIL_RECIPIENT", "").strip()

    def _build_assessment_report_body(self, report: dict[str, Any]) -> str:
        weak_sections = report.get("weak_sections") or []
        top_students = report.get("top_students") or []
        reattempted_students = report.get("reattempted_students") or []
        lines = [
            "Assessment deadline report",
            "",
            f"Assessment: {report.get('assessment_title') or 'Assessment'}",
            f"Type: {report.get('assessment_type') or 'Assessment'}",
            f"Deadline: {report.get('deadline') or 'N/A'}",
            f"Generated at: {report.get('generated_at') or 'N/A'}",
            "",
            "Overview",
            f"- Total submissions: {report.get('total_submissions') or 0}",
            f"- Counted first attempts: {report.get('counted_first_attempts') or 0}",
            f"- Reattempt submissions: {report.get('reattempt_submissions') or 0}",
            f"- Unique students: {report.get('unique_students') or 0}",
            f"- First-attempt average: {report.get('first_attempt_average_pct') or 0}%",
            f"- First-attempt best: {report.get('first_attempt_best_pct') or 0}%",
            f"- First-attempt worst: {report.get('first_attempt_worst_pct') or 0}%",
        ]
        if reattempted_students:
            lines.extend(
                [
                    "",
                    "Students who reattempted",
                    *[f"- {str(name)}" for name in reattempted_students[:20]],
                ]
            )
        if weak_sections:
            lines.extend(
                [
                    "",
                    "Weak sections",
                    *[
                        f"- {item.get('section')}: {item.get('average_accuracy')}% average accuracy"
                        for item in weak_sections[:8]
                        if isinstance(item, dict)
                    ],
                ]
            )
        if top_students:
            lines.extend(
                [
                    "",
                    "Top first attempts",
                    *[
                        f"- {item.get('student_name') or item.get('student_id')}: {item.get('score_pct')}%"
                        for item in top_students[:10]
                        if isinstance(item, dict)
                    ],
                ]
            )
        lines.extend(
            [
                "",
                "The full structured report is attached as JSON.",
            ]
        )
        return "\n".join(lines)

    def _build_assessment_submission_report_body(
        self, report: dict[str, Any]
    ) -> str:
        section_accuracy = report.get("section_accuracy") or {}
        first_attempt_baseline = report.get("first_attempt_baseline") or {}
        answer_snapshot = report.get("answer_snapshot") or []
        submission_kind = str(report.get("submission_kind") or "submission").strip()
        attempt_index = int(report.get("attempt_index") or 1)
        total_attempts = int(report.get("total_attempts_for_quiz") or attempt_index)
        kind_line = (
            f"Reattempt #{attempt_index} of {max(total_attempts, attempt_index)}"
            if submission_kind == "reattempt"
            else "First attempt"
        )
        lines = [
            "Detailed assessment submission report",
            "",
            f"Assessment: {report.get('assessment_title') or 'Assessment'}",
            f"Type: {report.get('assessment_type') or 'Assessment'}",
            f"Submission kind: {kind_line}",
            f"Counts for ranking analytics: {'Yes' if report.get('counts_for_teacher_analytics') else 'No'}",
            f"Submitted at: {report.get('submitted_at') or 'N/A'}",
            f"Deadline: {report.get('deadline') or 'N/A'}",
            f"Submitted before deadline: {'Yes' if report.get('submitted_before_deadline') else 'No'}",
            "",
            "Student details",
            f"- Name: {report.get('student_name') or 'N/A'}",
            f"- Student ID: {report.get('student_id') or 'N/A'}",
            f"- Account ID: {report.get('account_id') or 'N/A'}",
            f"- Email: {report.get('student_email') or 'N/A'}",
            "",
            "Performance summary",
            f"- Score: {report.get('score') or 0} / {report.get('total') or 0}",
            f"- Score percent: {report.get('score_pct') or 0}%",
            f"- Correct: {report.get('correct') or 0}",
            f"- Wrong: {report.get('wrong') or 0}",
            f"- Skipped: {report.get('skipped') or 0}",
            f"- Attempted: {report.get('attempted') or 0} of {report.get('total_questions') or 0}",
            f"- Accuracy on attempted questions: {report.get('accuracy_pct') or 0}%",
            f"- Coverage: {report.get('coverage_pct') or 0}%",
            f"- Time taken: {report.get('total_time_seconds') or 0} seconds",
            f"- Scheduled duration: {report.get('duration_minutes') or 0} minutes",
        ]
        if report.get("subject") or report.get("chapters") or report.get("class_name"):
            lines.extend(
                [
                    "",
                    "Assessment context",
                    f"- Subject: {report.get('subject') or 'N/A'}",
                    f"- Chapter or topic: {report.get('chapters') or 'N/A'}",
                    f"- Class: {report.get('class_name') or 'N/A'}",
                ]
            )
        if isinstance(section_accuracy, dict) and section_accuracy:
            lines.extend(["", "Section accuracy"])
            for key, value in section_accuracy.items():
                lines.append(f"- {key}: {value}%")
        if isinstance(first_attempt_baseline, dict) and first_attempt_baseline:
            lines.extend(
                [
                    "",
                    "First-attempt baseline for comparison",
                    (
                        f"- Score: {first_attempt_baseline.get('score') or 0} / "
                        f"{first_attempt_baseline.get('total') or 0}"
                    ),
                    f"- Score percent: {first_attempt_baseline.get('score_pct') or 0}%",
                    f"- Submitted at: {first_attempt_baseline.get('submitted_at') or 'N/A'}",
                ]
            )
            delta_pct = first_attempt_baseline.get("delta_pct")
            if delta_pct not in (None, ""):
                lines.append(f"- Delta vs first attempt: {delta_pct}%")
        if isinstance(answer_snapshot, list) and answer_snapshot:
            lines.extend(["", "Answer snapshot"])
            for item in answer_snapshot[:20]:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    "- "
                    f"Q{item.get('question_index') or '?'}"
                    f" [{item.get('status') or 'unknown'}]"
                    f" Student: {item.get('student_answer') or 'Skipped'}"
                    f" | Correct: {item.get('correct_answer') or 'N/A'}"
                )
        lines.extend(["", "The full structured submission report is attached as JSON."])
        return "\n".join(lines)

    def _build_assignment_announcement_body(self, report: dict[str, Any]) -> str:
        title = str(report.get("assessment_title") or "Assessment").strip()
        assessment_type = str(report.get("assessment_type") or "Assessment").strip()
        question_count = int(report.get("question_count") or 0)
        total_marks = int(report.get("total_marks") or 0)
        duration_minutes = int(report.get("duration_minutes") or 0)
        start_at = str(report.get("start_at") or "N/A").strip() or "N/A"
        deadline = str(report.get("deadline") or "N/A").strip() or "N/A"
        subject = str(report.get("subject") or "N/A").strip() or "N/A"
        chapters = str(report.get("chapters") or "N/A").strip() or "N/A"
        class_name = str(report.get("class_name") or "N/A").strip() or "N/A"
        quiz_url = str(report.get("quiz_url") or "").strip()
        lines = [
            "A new assignment has been published for you in LalaCore.",
            "",
            f"Title: {title}",
            f"Type: {assessment_type}",
            f"Class: {class_name}",
            f"Subject: {subject}",
            f"Chapter or topic: {chapters}",
            f"Available from: {start_at}" if start_at != "N/A" else "Available from: Immediately",
            f"Deadline: {deadline}",
            f"Duration: {duration_minutes} minutes" if duration_minutes > 0 else "Duration: Flexible / not fixed",
            f"Total questions: {question_count}",
            f"Total marks: {total_marks}",
        ]
        if quiz_url:
            lines.extend(["", f"Assignment link: {quiz_url}"])
        lines.extend(
            [
                "",
                "Open the app to review the full paper, instructions, and analytics.",
                "If you sign in later on another device, LalaCore will keep this assignment available there as well.",
                "",
                "Sent automatically by God of Maths via LalaCore.",
            ]
        )
        return "\n".join(lines)

    def _recipient_list(self, recipient: str | None) -> list[str]:
        source = (
            self._configured_support_recipient()
            if recipient is None
            else str(recipient).strip()
        )
        if not source:
            return []
        recipients: list[str] = []
        for chunk in source.replace(";", ",").replace("\n", ",").split(","):
            email = chunk.strip()
            if email and email not in recipients:
                recipients.append(email)
        return recipients

    def _smtp_settings(self) -> dict[str, Any]:
        sender = self._sender_email()
        password = self._sender_password()
        smtp_host = (
            os.getenv("ATLAS_SUPPORT_SMTP_HOST", "").strip()
            or os.getenv("OTP_SMTP_HOST", "").strip()
            or "smtp.gmail.com"
        )
        smtp_port_raw = (
            os.getenv("ATLAS_SUPPORT_SMTP_PORT", "").strip()
            or os.getenv("OTP_SMTP_PORT", "").strip()
            or "587"
        )
        smtp_security = (
            os.getenv("ATLAS_SUPPORT_SMTP_SECURITY", "").strip().lower()
            or os.getenv("OTP_SMTP_SECURITY", "").strip().lower()
            or "tls"
        )
        if not sender or not password:
            return {
                "ok": False,
                "message": "SMTP sender credentials are not configured",
            }
        try:
            smtp_port = int(smtp_port_raw)
        except ValueError:
            return {
                "ok": False,
                "message": f"Invalid SMTP port: {smtp_port_raw}",
            }
        return {
            "ok": True,
            "sender": sender,
            "password": password,
            "smtp_host": smtp_host,
            "smtp_port": smtp_port,
            "smtp_security": smtp_security,
        }

    def _apps_script_configured(self) -> bool:
        return bool(str(os.getenv("ATLAS_AUTOMAIL_WEBHOOK_URL", "")).strip())

    def _apps_script_settings(self) -> dict[str, Any]:
        webhook_url = str(os.getenv("ATLAS_AUTOMAIL_WEBHOOK_URL", "")).strip()
        timeout_raw = str(
            os.getenv("ATLAS_AUTOMAIL_TIMEOUT_SECONDS", "40")
        ).strip() or "40"
        retry_raw = str(
            os.getenv("ATLAS_AUTOMAIL_RETRY_COUNT", "1")
        ).strip() or "1"
        retry_backoff_raw = str(
            os.getenv("ATLAS_AUTOMAIL_RETRY_BACKOFF_SECONDS", "1.5")
        ).strip() or "1.5"
        if not webhook_url:
            return {
                "ok": False,
                "message": "Apps Script webhook is not configured",
            }
        try:
            timeout_seconds = max(5, int(timeout_raw))
        except ValueError:
            return {
                "ok": False,
                "message": f"Invalid ATLAS_AUTOMAIL_TIMEOUT_SECONDS: {timeout_raw}",
            }
        try:
            retry_count = max(0, min(3, int(retry_raw)))
        except ValueError:
            return {
                "ok": False,
                "message": f"Invalid ATLAS_AUTOMAIL_RETRY_COUNT: {retry_raw}",
            }
        try:
            retry_backoff_seconds = max(0.0, min(10.0, float(retry_backoff_raw)))
        except ValueError:
            return {
                "ok": False,
                "message": (
                    "Invalid ATLAS_AUTOMAIL_RETRY_BACKOFF_SECONDS: "
                    f"{retry_backoff_raw}"
                ),
            }
        return {
            "ok": True,
            "webhook_url": webhook_url,
            "shared_secret": str(os.getenv("ATLAS_AUTOMAIL_SHARED_SECRET", "")).strip(),
            "timeout_seconds": timeout_seconds,
            "retry_count": retry_count,
            "retry_backoff_seconds": retry_backoff_seconds,
        }

    def _allow_apps_script_smtp_fallback(self) -> bool:
        raw = os.getenv("ATLAS_AUTOMAIL_ALLOW_SMTP_FALLBACK")
        if raw is None:
            return True
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def _ssl_context(self) -> ssl.SSLContext:
        try:
            import certifi  # type: ignore

            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            return ssl.create_default_context()

    def _message_plain_text(self, msg: EmailMessage) -> str:
        try:
            if msg.is_multipart():
                part = msg.get_body(preferencelist=("plain",))
                if part is not None:
                    return str(part.get_content())
            return str(msg.get_content())
        except Exception:
            return ""

    def _message_html(self, msg: EmailMessage) -> str:
        try:
            if msg.is_multipart():
                part = msg.get_body(preferencelist=("html",))
                if part is not None:
                    return str(part.get_content())
        except Exception:
            return ""
        return ""

    def _message_attachments_payload(self, msg: EmailMessage) -> list[dict[str, str]]:
        attachments: list[dict[str, str]] = []
        try:
            iterator = msg.iter_attachments()
        except Exception:
            iterator = []
        for part in iterator:
            try:
                payload = part.get_payload(decode=True) or b""
            except Exception:
                payload = b""
            attachments.append(
                {
                    "filename": str(part.get_filename() or "attachment.bin"),
                    "content_type": str(part.get_content_type() or "application/octet-stream"),
                    "base64": base64.b64encode(payload).decode("ascii"),
                }
            )
        return attachments

    def _sender_email(self) -> str:
        return (
            os.getenv("ATLAS_SUPPORT_SENDER_EMAIL", "").strip()
            or os.getenv("OTP_SENDER_EMAIL", "").strip()
            or os.getenv("FORGOT_OTP_SENDER_EMAIL", "").strip()
        )

    def _sender_password(self) -> str:
        return (
            os.getenv("ATLAS_SUPPORT_SENDER_PASSWORD", "").strip().replace(" ", "")
            or os.getenv("OTP_SENDER_PASSWORD", "").strip().replace(" ", "")
        )

    def _build_text_body(self, report: dict[str, Any]) -> str:
        root_causes = report.get("likely_root_causes")
        if isinstance(root_causes, list):
            root_cause_lines = "\n".join(
                f"- {str(item).strip()}"
                for item in root_causes
                if str(item).strip()
            )
        else:
            root_cause_lines = ""
        next_steps = report.get("next_steps")
        if isinstance(next_steps, list):
            next_step_lines = "\n".join(
                f"- {str(item).strip()}"
                for item in next_steps
                if str(item).strip()
            )
        else:
            next_step_lines = ""
        evidence = report.get("evidence")
        if isinstance(evidence, list):
            evidence_lines = "\n".join(
                f"- {str(item).strip()}"
                for item in evidence
                if str(item).strip()
            )
        else:
            evidence_lines = ""
        diagnostics = report.get("diagnostics")
        diagnostics_json = (
            json.dumps(diagnostics, indent=2, ensure_ascii=False)
            if isinstance(diagnostics, dict)
            else "{}"
        )
        diagnostics_before = report.get("diagnostics_before_repair")
        diagnostics_before_json = (
            json.dumps(diagnostics_before, indent=2, ensure_ascii=False)
            if isinstance(diagnostics_before, dict)
            else "{}"
        )
        reporter = report.get("reporter")
        reporter_block = (
            json.dumps(reporter, indent=2, ensure_ascii=False)
            if isinstance(reporter, dict)
            else "{}"
        )
        self_heal = report.get("self_heal")
        self_heal_block = (
            json.dumps(self_heal, indent=2, ensure_ascii=False)
            if isinstance(self_heal, dict)
            else "{}"
        )
        runtime_logs = report.get("runtime_logs")
        runtime_logs_json = (
            json.dumps(runtime_logs, indent=2, ensure_ascii=False)
            if isinstance(runtime_logs, dict)
            else "{}"
        )
        maintenance = report.get("maintenance")
        maintenance_json = (
            json.dumps(maintenance, indent=2, ensure_ascii=False)
            if isinstance(maintenance, dict)
            else "{}"
        )
        maintenance_audit = (
            maintenance.get("audit")
            if isinstance(maintenance, dict) and isinstance(maintenance.get("audit"), dict)
            else None
        )
        maintenance_audit_json = (
            json.dumps(maintenance_audit, indent=2, ensure_ascii=False)
            if isinstance(maintenance_audit, dict)
            else "{}"
        )
        plausible_layers = report.get("plausible_causes_by_layer")
        plausible_layers_json = (
            json.dumps(plausible_layers, indent=2, ensure_ascii=False)
            if isinstance(plausible_layers, dict)
            else "{}"
        )
        engineer_checklist = report.get("engineer_checklist")
        if isinstance(engineer_checklist, list):
            engineer_checklist_lines = "\n".join(
                f"- {str(item).strip()}"
                for item in engineer_checklist
                if str(item).strip()
            )
        else:
            engineer_checklist_lines = ""
        return "\n".join(
            [
                "Atlas incident report",
                "",
                f"Incident id: {str(report.get('incident_id') or '').strip()}",
                f"Severity: {str(report.get('severity') or 'medium').strip()}",
                f"Surface: {str(report.get('surface') or '').strip()}",
                f"Role: {str(report.get('role') or '').strip()}",
                "",
                "Issue summary:",
                str(report.get("issue_summary") or "").strip(),
                "",
                "Atlas summary:",
                str(report.get("summary") or "").strip(),
                "",
                "Impact assessment:",
                str(report.get("impact_assessment") or "").strip()
                or "Not provided",
                "",
                "Likely root causes:",
                root_cause_lines or "- Atlas could not isolate a dominant cause yet.",
                "",
                "Plausible causes by layer:",
                plausible_layers_json,
                "",
                "Evidence:",
                evidence_lines or "- No strong evidence list was generated.",
                "",
                "Self-heal attempt:",
                self_heal_block,
                "",
                "Next steps:",
                next_step_lines or "- No next-step list was generated.",
                "",
                "Engineer checklist:",
                engineer_checklist_lines or "- No engineer checklist was generated.",
                "",
                "Reporter:",
                reporter_block,
                "",
                "Maintenance metadata:",
                maintenance_json,
                "",
                "Maintenance deep audit:",
                maintenance_audit_json,
                "",
                "Diagnostics before repair:",
                diagnostics_before_json,
                "",
                "Diagnostics snapshot:",
                diagnostics_json,
                "",
                "Runtime logs excerpt:",
                runtime_logs_json,
            ]
        )

    def _incident_json_block(self, label: str, value: Any) -> str:
        if isinstance(value, dict):
            payload = json.dumps(value, indent=2, ensure_ascii=False)
        elif isinstance(value, list):
            payload = json.dumps(value, indent=2, ensure_ascii=False)
        else:
            payload = str(value or "").strip()
        if not payload:
            return ""
        return section(
            label,
            code_block(payload),
            accent=self.INCIDENT_THEME.accent,
            background="#fff8f8",
        )

    def _normalized_lines(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [line.strip() for line in value.splitlines() if line.strip()]
        return []

    def _display_url(self, *candidates: Any) -> str:
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text:
                return text
        return ""

    def _build_incident_report_html(self, report: dict[str, Any]) -> str:
        severity = str(report.get("severity") or "medium").strip().upper()
        likely_root_causes = self._normalized_lines(report.get("likely_root_causes"))
        next_steps = self._normalized_lines(report.get("next_steps"))
        evidence = self._normalized_lines(report.get("evidence"))
        reporter = report.get("reporter") if isinstance(report.get("reporter"), dict) else {}
        hero_aside = self._meta_aside(
            [
                ("Severity", severity or "MEDIUM"),
                ("Incident id", str(report.get("incident_id") or "Pending")),
                ("Surface", str(report.get("surface") or "General")),
            ],
            theme=self.INCIDENT_THEME,
        )
        body_html = "".join(
            [
                section(
                    "Issue summary",
                    "".join(
                        [
                            paragraph(
                                report.get("issue_summary") or "Atlas raised an incident without a short summary yet.",
                                size=18,
                            ),
                            paragraph(
                                report.get("summary") or "A deeper Atlas summary was not attached to this report.",
                                color=self.INCIDENT_THEME.muted,
                            ),
                        ]
                    ),
                    accent=self.INCIDENT_THEME.accent,
                    background="#fffaf8",
                ),
                metric_grid(
                    [
                        ("Role", str(report.get("role") or "N/A")),
                        ("Impact", str(report.get("impact_assessment") or "Needs review")),
                    ],
                    accent=self.INCIDENT_THEME.accent,
                    soft=self.INCIDENT_THEME.accent_soft,
                ),
                section(
                    "Likely root causes",
                    bullet_list(
                        likely_root_causes,
                        accent=self.INCIDENT_THEME.accent,
                        empty_message="Atlas could not isolate a dominant cause yet.",
                    ),
                    accent=self.INCIDENT_THEME.accent,
                    background="#ffffff",
                ),
                section(
                    "Evidence",
                    bullet_list(
                        evidence,
                        accent=self.INCIDENT_THEME.accent,
                        empty_message="No strong evidence list was generated.",
                    ),
                    accent=self.INCIDENT_THEME.accent,
                    background="#ffffff",
                ),
                section(
                    "Next steps",
                    bullet_list(
                        next_steps,
                        accent=self.INCIDENT_THEME.accent,
                        empty_message="No next-step list was generated.",
                    ),
                    accent=self.INCIDENT_THEME.accent,
                    background="#fffaf8",
                ),
                section(
                    "Reporter",
                    detail_rows(
                        [
                            ("Name", str(reporter.get("name") or "N/A")),
                            ("Email", str(reporter.get("email") or "N/A")),
                            ("Reply to", str(report.get("reply_to_email") or reporter.get("email") or "N/A")),
                        ]
                    ),
                    accent=self.INCIDENT_THEME.accent,
                    background="#ffffff",
                ),
                self._incident_json_block("Diagnostics snapshot", report.get("diagnostics")),
                self._incident_json_block(
                    "Diagnostics before repair",
                    report.get("diagnostics_before_repair"),
                ),
                self._incident_json_block("Runtime logs excerpt", report.get("runtime_logs")),
            ]
        )
        return build_email_document(
            theme=self.INCIDENT_THEME,
            eyebrow="operator incident",
            title="Atlas raised an incident",
            subtitle="A sharply formatted operator digest with the issue, impact, evidence, and next repair steps in one place.",
            body_html=body_html,
            footer_html=self._footer_html(
                purpose="Operator incident digest",
                theme=self.INCIDENT_THEME,
            ),
            preheader=f"{severity} incident: {str(report.get('issue_summary') or '').strip()}",
            hero_aside_html=hero_aside,
        )

    def _build_release_confirmation_body(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        trigger: str,
        checked_at: str | None,
    ) -> str:
        lines: list[str] = [
            "Atlas app update confirmation",
            "",
            f"Trigger: {trigger}",
            f"Checked at: {checked_at or ''}",
            f"Published sheet: {sheet_url}",
            f"Release rows detected: {len(releases)}",
            "",
        ]
        for idx, release in enumerate(releases, start=1):
            release_notes = str(release.get("release_notes") or "").strip()
            if release_notes:
                notes_block = "\n".join(
                    f"  - {line.strip()}"
                    for line in release_notes.splitlines()
                    if line.strip()
                )
            else:
                notes_block = "  - No release notes provided."
            lines.extend(
                [
                    f"Release {idx}",
                    f"  App id: {str(release.get('app_id') or '').strip()}",
                    f"  Channel: {str(release.get('channel') or '').strip()}",
                    f"  Audience: {str(release.get('audience') or '').strip()}",
                    f"  Platform: {str(release.get('platform') or '').strip()}",
                    f"  Version: {str(release.get('version') or '').strip()}",
                    f"  Build number: {str(release.get('build_number') or '').strip()}",
                    f"  Force update: {str(bool(release.get('force')))}",
                    f"  Android URL: {str(release.get('android_url') or release.get('apk_url') or '').strip()}",
                    f"  iOS URL: {str(release.get('ios_url') or '').strip()}",
                    f"  Generic URL: {str(release.get('download_url') or '').strip()}",
                    f"  Message: {str(release.get('message') or '').strip()}",
                    "  Release notes:",
                    notes_block,
                    "",
                ]
            )
        return "\n".join(lines)

    def _build_release_confirmation_html(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        trigger: str,
        checked_at: str | None,
    ) -> str:
        cards: list[str] = []
        for idx, release in enumerate(releases, start=1):
            notes = self._normalized_lines(release.get("release_notes"))
            cards.append(
                section(
                    f"Release {idx}",
                    "".join(
                        [
                            metric_grid(
                                [
                                    ("Version", str(release.get("version") or "N/A")),
                                    ("Build", str(release.get("build_number") or "N/A")),
                                    ("Audience", str(release.get("audience") or "all")),
                                    ("Platform", str(release.get("platform") or "all")),
                                ],
                                accent=self.OPERATOR_THEME.accent,
                                soft=self.OPERATOR_THEME.accent_soft,
                            ),
                            detail_rows(
                                [
                                    ("App id", str(release.get("app_id") or "lalacore_rebuild")),
                                    ("Channel", str(release.get("channel") or "stable")),
                                    ("Force update", "Yes" if bool(release.get("force")) else "No"),
                                    (
                                        "Message",
                                        str(release.get("message") or "No message was provided in the release row."),
                                    ),
                                    (
                                        "Android URL",
                                        self._display_url(release.get("android_url"), release.get("apk_url")) or "N/A",
                                    ),
                                    ("iOS URL", self._display_url(release.get("ios_url")) or "N/A"),
                                    ("Generic URL", self._display_url(release.get("download_url")) or "N/A"),
                                ]
                            ),
                            section(
                                "Patch notes",
                                bullet_list(
                                    notes,
                                    accent=self.OPERATOR_THEME.accent,
                                    empty_message="No release notes were provided.",
                                ),
                                accent=self.OPERATOR_THEME.accent,
                                background="#faf7ff",
                            ),
                        ]
                    ),
                    accent=self.OPERATOR_THEME.accent,
                    background="#ffffff",
                )
            )
        hero_aside = self._meta_aside(
            [
                ("Trigger", trigger or "scheduled"),
                ("Checked at", checked_at or "N/A"),
                ("Rows", str(len(releases))),
            ],
            theme=self.OPERATOR_THEME,
        )
        body_html = "".join(
            [
                section(
                    "Published feed",
                    detail_rows(
                        [
                            ("Sheet URL", sheet_url),
                            ("Release rows detected", str(len(releases))),
                        ]
                    ),
                    accent=self.OPERATOR_THEME.accent,
                    background="#faf7ff",
                ),
                "".join(cards),
            ]
        )
        return build_email_document(
            theme=self.OPERATOR_THEME,
            eyebrow="release control",
            title="Atlas published release metadata",
            subtitle="A clean operator confirmation for the exact release batch that entered the update pipeline.",
            body_html=body_html,
            footer_html=self._footer_html(
                purpose="Internal release confirmation",
                theme=self.OPERATOR_THEME,
            ),
            preheader=f"{len(releases)} release row(s) published from Atlas",
            hero_aside_html=hero_aside,
        )

    def _release_subject_suffix(self, releases: list[dict[str, Any]]) -> str:
        version_bits: list[str] = []
        for release in releases[:3]:
            version = str(release.get("version") or "").strip()
            build = str(release.get("build_number") or "").strip()
            audience = str(release.get("audience") or "").strip() or "all"
            label = version or "unknown-version"
            if build:
                label = f"{label}+{build}"
            version_bits.append(f"{audience}:{label}")
        subject_suffix = ", ".join(version_bits) if version_bits else "new release"
        if len(releases) > 3:
            subject_suffix = f"{subject_suffix} (+{len(releases) - 3} more)"
        return subject_suffix

    def _build_release_announcement_body(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        trigger: str,
        checked_at: str | None,
        recipient: str,
    ) -> str:
        lines: list[str] = [
            "A new LalaCore app update is available.",
            "",
            f"This release notice was sent to: {recipient}",
            f"Detected via: {trigger}",
            f"Checked at: {checked_at or ''}",
            f"Release sheet: {sheet_url}",
            "",
            "What to do",
            "- Open the app and follow the update prompt if one appears.",
            "- If the app requires an update, use the platform download link below.",
            "",
        ]
        for idx, release in enumerate(releases, start=1):
            release_notes = str(release.get("release_notes") or "").strip()
            if release_notes:
                notes_block = [
                    f"- {line.strip()}"
                    for line in release_notes.splitlines()
                    if line.strip()
                ]
            else:
                notes_block = ["- Detailed release notes were not provided in the sheet row."]
            download_links = [
                f"- Android: {str(release.get('android_url') or '').strip() or 'N/A'}",
                f"- iPhone/iPad: {str(release.get('ios_url') or '').strip() or 'N/A'}",
                f"- Generic link: {str(release.get('download_url') or '').strip() or 'N/A'}",
            ]
            lines.extend(
                [
                    f"Release {idx}",
                    f"- App: {str(release.get('app_id') or '').strip() or 'lalacore_rebuild'}",
                    f"- Channel: {str(release.get('channel') or '').strip() or 'stable'}",
                    f"- Audience: {str(release.get('audience') or '').strip() or 'all'}",
                    f"- Platform: {str(release.get('platform') or '').strip() or 'all'}",
                    f"- Version: {str(release.get('version') or '').strip() or 'N/A'}",
                    f"- Build number: {str(release.get('build_number') or '').strip() or 'N/A'}",
                    f"- Forced update: {'Yes' if bool(release.get('force')) else 'No'}",
                    f"- Minimum supported version: {str(release.get('min_supported_version') or '').strip() or 'N/A'}",
                    f"- Minimum supported build: {str(release.get('min_supported_build') or '').strip() or 'N/A'}",
                    f"- Update message: {str(release.get('message') or '').strip() or 'A new update is available.'}",
                    "Download links",
                    *download_links,
                    "Patch notes",
                    *notes_block,
                    "",
                ]
            )
        lines.extend(
            [
                "This email was sent automatically by God of Maths from the LalaCore release system.",
            ]
        )
        return "\n".join(lines)

    def _build_release_announcement_html(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        trigger: str,
        checked_at: str | None,
        recipient: str,
    ) -> str:
        cards: list[str] = []
        for idx, release in enumerate(releases, start=1):
            notes = self._normalized_lines(release.get("release_notes"))
            actions = [
                ("Download for Android", self._display_url(release.get("android_url"), release.get("apk_url"))),
                ("Download for iPhone or iPad", self._display_url(release.get("ios_url"))),
                ("Open update link", self._display_url(release.get("download_url"))),
            ]
            cards.append(
                section(
                    f"Release {idx}",
                    "".join(
                        [
                            metric_grid(
                                [
                                    ("Version", str(release.get("version") or "N/A")),
                                    ("Build", str(release.get("build_number") or "N/A")),
                                    ("Forced", "Yes" if bool(release.get("force")) else "No"),
                                    ("Platform", str(release.get("platform") or "all")),
                                ],
                                accent=self.RELEASE_THEME.accent,
                                soft=self.RELEASE_THEME.accent_soft,
                            ),
                            paragraph(
                                str(release.get("message") or "A new update is ready for your app experience."),
                                size=18,
                            ),
                            detail_rows(
                                [
                                    ("Channel", str(release.get("channel") or "stable")),
                                    ("Audience", str(release.get("audience") or "all")),
                                    (
                                        "Minimum supported version",
                                        str(release.get("min_supported_version") or "N/A"),
                                    ),
                                    (
                                        "Minimum supported build",
                                        str(release.get("min_supported_build") or "N/A"),
                                    ),
                                ]
                            ),
                            buttons(
                                actions,
                                accent=self.RELEASE_THEME.accent,
                                text_color=self.RELEASE_THEME.button_text,
                            ),
                            section(
                                "Patch notes",
                                bullet_list(
                                    notes,
                                    accent=self.RELEASE_THEME.accent,
                                    empty_message="Detailed release notes were not attached to this release row.",
                                ),
                                accent=self.RELEASE_THEME.accent,
                                background="#f8faff",
                            ),
                        ]
                    ),
                    accent=self.RELEASE_THEME.accent,
                    background="#ffffff",
                )
            )
        hero_aside = self._brand_orb(
            theme=self.RELEASE_THEME,
            primary="#7db7ff",
            secondary="#2457ff",
        )
        body_html = "".join(
            [
                section(
                    "Delivery details",
                    detail_rows(
                        [
                            ("Sent to", recipient),
                            ("Detected via", trigger),
                            ("Checked at", checked_at or "N/A"),
                            ("Release sheet", sheet_url),
                        ]
                    ),
                    accent=self.RELEASE_THEME.accent,
                    background="#f8faff",
                ),
                "".join(cards),
            ]
        )
        return build_email_document(
            theme=self.RELEASE_THEME,
            eyebrow="app update",
            title="A polished LalaCore update is ready",
            subtitle="A calmer, more premium release note built to help students and teachers update without friction or noise.",
            body_html=body_html,
            footer_html=self._footer_html(
                purpose="User-facing release announcement",
                theme=self.RELEASE_THEME,
            ),
            preheader=self._release_subject_suffix(releases),
            hero_aside_html=hero_aside,
        )

    def _build_assessment_report_html(self, report: dict[str, Any]) -> str:
        weak_sections = report.get("weak_sections") or []
        weak_section_lines = [
            f"{item.get('section')}: {item.get('average_accuracy')}% average accuracy"
            for item in weak_sections[:8]
            if isinstance(item, dict)
        ]
        top_students = report.get("top_students") or []
        top_student_lines = [
            f"{item.get('student_name') or item.get('student_id')}: {item.get('score_pct')}%"
            for item in top_students[:10]
            if isinstance(item, dict)
        ]
        reattempted_students = [
            str(item).strip()
            for item in (report.get("reattempted_students") or [])[:20]
            if str(item).strip()
        ]
        hero_aside = self._meta_aside(
            [
                ("Assessment", str(report.get("assessment_type") or "Assessment")),
                ("Deadline", str(report.get("deadline") or "N/A")),
                ("Generated", str(report.get("generated_at") or "N/A")),
            ],
            theme=self.ANALYTICS_THEME,
        )
        body_html = "".join(
            [
                metric_grid(
                    [
                        ("Total submissions", str(report.get("total_submissions") or 0)),
                        ("Unique students", str(report.get("unique_students") or 0)),
                        ("First-attempt average", f"{report.get('first_attempt_average_pct') or 0}%"),
                        ("Reattempts", str(report.get("reattempt_submissions") or 0)),
                    ],
                    accent=self.ANALYTICS_THEME.accent,
                    soft=self.ANALYTICS_THEME.accent_soft,
                ),
                section(
                    "Assessment snapshot",
                    detail_rows(
                        [
                            ("Title", str(report.get("assessment_title") or "Assessment")),
                            ("Type", str(report.get("assessment_type") or "Assessment")),
                            ("Deadline", str(report.get("deadline") or "N/A")),
                            ("First-attempt best", f"{report.get('first_attempt_best_pct') or 0}%"),
                            ("First-attempt worst", f"{report.get('first_attempt_worst_pct') or 0}%"),
                        ]
                    ),
                    accent=self.ANALYTICS_THEME.accent,
                    background="#f5fbff",
                ),
                section(
                    "Weak sections",
                    bullet_list(
                        weak_section_lines,
                        accent=self.ANALYTICS_THEME.accent,
                        empty_message="No weak section cluster was identified.",
                    ),
                    accent=self.ANALYTICS_THEME.accent,
                    background="#ffffff",
                ),
                section(
                    "Top first attempts",
                    bullet_list(
                        top_student_lines,
                        accent=self.ANALYTICS_THEME.accent,
                        empty_message="No top-attempt summary was available.",
                    ),
                    accent=self.ANALYTICS_THEME.accent,
                    background="#ffffff",
                ),
                section(
                    "Students who reattempted",
                    bullet_list(
                        reattempted_students,
                        accent=self.ANALYTICS_THEME.accent,
                        empty_message="No reattempts were recorded.",
                    ),
                    accent=self.ANALYTICS_THEME.accent,
                    background="#f5fbff",
                ),
            ]
        )
        return build_email_document(
            theme=self.ANALYTICS_THEME,
            eyebrow="teacher analytics",
            title="Assessment performance digest",
            subtitle="A premium analytics snapshot for deadlines, first-attempt quality, and student reattempt behaviour.",
            body_html=body_html,
            footer_html=self._footer_html(
                purpose="Assessment deadline report",
                theme=self.ANALYTICS_THEME,
            ),
            preheader=str(report.get("assessment_title") or "Assessment report"),
            hero_aside_html=hero_aside,
        )

    def _build_assessment_submission_report_html(self, report: dict[str, Any]) -> str:
        section_accuracy = report.get("section_accuracy") or {}
        first_attempt_baseline = report.get("first_attempt_baseline") or {}
        answer_snapshot = report.get("answer_snapshot") or []
        answer_lines = [
            (
                f"Q{item.get('question_index') or '?'} [{item.get('status') or 'unknown'}]: "
                f"Student {item.get('student_answer') or 'Skipped'} | "
                f"Correct {item.get('correct_answer') or 'N/A'}"
            )
            for item in answer_snapshot[:20]
            if isinstance(item, dict)
        ]
        section_lines = [
            f"{key}: {value}%"
            for key, value in section_accuracy.items()
        ] if isinstance(section_accuracy, dict) else []
        baseline_rows = (
            [
                ("Score", f"{first_attempt_baseline.get('score') or 0} / {first_attempt_baseline.get('total') or 0}"),
                ("Score percent", f"{first_attempt_baseline.get('score_pct') or 0}%"),
                ("Submitted at", str(first_attempt_baseline.get("submitted_at") or "N/A")),
                ("Delta vs first attempt", f"{first_attempt_baseline.get('delta_pct')}%"),
            ]
            if isinstance(first_attempt_baseline, dict) and first_attempt_baseline
            else []
        )
        hero_aside = self._meta_aside(
            [
                ("Student", str(report.get("student_name") or report.get("student_id") or "Unknown")),
                ("Attempt", str(report.get("attempt_index") or 1)),
                ("Counts for ranking", "Yes" if report.get("counts_for_teacher_analytics") else "No"),
            ],
            theme=self.PERFORMANCE_THEME,
        )
        body_html = "".join(
            [
                metric_grid(
                    [
                        ("Score", f"{report.get('score') or 0} / {report.get('total') or 0}"),
                        ("Score percent", f"{report.get('score_pct') or 0}%"),
                        ("Accuracy", f"{report.get('accuracy_pct') or 0}%"),
                        ("Coverage", f"{report.get('coverage_pct') or 0}%"),
                    ],
                    accent=self.PERFORMANCE_THEME.accent,
                    soft=self.PERFORMANCE_THEME.accent_soft,
                ),
                section(
                    "Student details",
                    detail_rows(
                        [
                            ("Name", str(report.get("student_name") or "N/A")),
                            ("Student ID", str(report.get("student_id") or "N/A")),
                            ("Account ID", str(report.get("account_id") or "N/A")),
                            ("Email", str(report.get("student_email") or "N/A")),
                        ]
                    ),
                    accent=self.PERFORMANCE_THEME.accent,
                    background="#fffaf2",
                ),
                section(
                    "Submission summary",
                    detail_rows(
                        [
                            ("Assessment", str(report.get("assessment_title") or "Assessment")),
                            ("Type", str(report.get("assessment_type") or "Assessment")),
                            ("Submission kind", str(report.get("submission_kind") or "submission")),
                            ("Submitted at", str(report.get("submitted_at") or "N/A")),
                            ("Deadline", str(report.get("deadline") or "N/A")),
                            ("Time taken", f"{report.get('total_time_seconds') or 0} seconds"),
                            ("Scheduled duration", f"{report.get('duration_minutes') or 0} minutes"),
                        ]
                    ),
                    accent=self.PERFORMANCE_THEME.accent,
                    background="#ffffff",
                ),
                section(
                    "Section accuracy",
                    bullet_list(
                        section_lines,
                        accent=self.PERFORMANCE_THEME.accent,
                        empty_message="No section-level breakdown was attached.",
                    ),
                    accent=self.PERFORMANCE_THEME.accent,
                    background="#ffffff",
                ),
                (
                    section(
                        "First-attempt baseline",
                        detail_rows(baseline_rows),
                        accent=self.PERFORMANCE_THEME.accent,
                        background="#fffaf2",
                    )
                    if baseline_rows
                    else ""
                ),
                section(
                    "Answer snapshot",
                    bullet_list(
                        answer_lines,
                        accent=self.PERFORMANCE_THEME.accent,
                        empty_message="No answer snapshot was attached.",
                    ),
                    accent=self.PERFORMANCE_THEME.accent,
                    background="#ffffff",
                ),
            ]
        )
        return build_email_document(
            theme=self.PERFORMANCE_THEME,
            eyebrow="submission intelligence",
            title="Assessment submission detail",
            subtitle="A tighter teacher-side performance email for scores, reattempt context, and answer review without digging into raw JSON first.",
            body_html=body_html,
            footer_html=self._footer_html(
                purpose="Assessment submission report",
                theme=self.PERFORMANCE_THEME,
            ),
            preheader=str(report.get("assessment_title") or "Assessment submission"),
            hero_aside_html=hero_aside,
        )

    def _build_assignment_announcement_html(self, report: dict[str, Any]) -> str:
        title = str(report.get("assessment_title") or "Assessment").strip()
        question_count = int(report.get("question_count") or 0)
        total_marks = int(report.get("total_marks") or 0)
        duration_minutes = int(report.get("duration_minutes") or 0)
        start_at = str(report.get("start_at") or "").strip()
        deadline = str(report.get("deadline") or "N/A").strip() or "N/A"
        quiz_url = str(report.get("quiz_url") or "").strip()
        hero_aside = self._brand_orb(
            theme=self.ASSIGNMENT_THEME,
            primary="#6ff0d8",
            secondary="#038b72",
        )
        body_html = "".join(
            [
                metric_grid(
                    [
                        ("Questions", str(question_count)),
                        ("Marks", str(total_marks)),
                        ("Duration", f"{duration_minutes} min" if duration_minutes > 0 else "Flexible"),
                        ("Deadline", deadline),
                    ],
                    accent=self.ASSIGNMENT_THEME.accent,
                    soft=self.ASSIGNMENT_THEME.accent_soft,
                ),
                section(
                    "Assignment brief",
                    detail_rows(
                        [
                            ("Title", title),
                            ("Type", str(report.get("assessment_type") or "Assessment")),
                            ("Class", str(report.get("class_name") or "N/A")),
                            ("Subject", str(report.get("subject") or "N/A")),
                            ("Chapter or topic", str(report.get("chapters") or "N/A")),
                            ("Available from", start_at or "Immediately"),
                            ("Deadline", deadline),
                        ]
                    ),
                    accent=self.ASSIGNMENT_THEME.accent,
                    background="#f5fffb",
                ),
                section(
                    "What to do next",
                    "".join(
                        [
                            paragraph(
                                "Open the app to review instructions, solve carefully, and track analytics after submission.",
                            ),
                            buttons(
                                [("Open assignment", quiz_url)],
                                accent=self.ASSIGNMENT_THEME.accent,
                                text_color=self.ASSIGNMENT_THEME.button_text,
                            ),
                        ]
                    ),
                    accent=self.ASSIGNMENT_THEME.accent,
                    background="#ffffff",
                ),
            ]
        )
        return build_email_document(
            theme=self.ASSIGNMENT_THEME,
            eyebrow="new assignment",
            title="A new task is ready in LalaCore",
            subtitle="A cleaner student-facing announcement with the essentials up front and a direct path back into the app.",
            body_html=body_html,
            footer_html=self._footer_html(
                purpose="Assignment announcement",
                theme=self.ASSIGNMENT_THEME,
            ),
            preheader=title,
            hero_aside_html=hero_aside,
        )
