from __future__ import annotations

import json
import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Any


class AtlasIncidentEmailService:
    """SMTP-backed support reporter reused by Atlas incident flows."""

    def __init__(self) -> None:
        self._default_recipient = self._configured_support_recipient()

    def smtp_configured(self) -> bool:
        sender = self._sender_email()
        password = self._sender_password()
        return bool(sender and password)

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
        from_name = (
            os.getenv("ATLAS_SUPPORT_FROM_NAME", "").strip()
            or os.getenv("OTP_FROM_NAME", "").strip()
            or "Atlas Health"
        )

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
            from_name_override=(
                os.getenv("ATLAS_SUPPORT_FROM_NAME", "").strip()
                or os.getenv("OTP_FROM_NAME", "").strip()
                or "Atlas Health"
            ),
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
        recipients = self._recipient_list(recipient)
        to_email = ", ".join(recipients)
        if not recipients:
            return {
                "ok": False,
                "sent": False,
                "message": "Support recipient is missing",
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
        return self._send_message(
            msg,
            recipients=recipients,
            from_name_override=(
                os.getenv("ATLAS_RELEASE_FROM_NAME", "").strip()
                or os.getenv("ATLAS_SUPPORT_FROM_NAME", "").strip()
                or os.getenv("OTP_FROM_NAME", "").strip()
                or "Atlas Release Watcher"
            ),
        )

    def _send_message(
        self,
        msg: EmailMessage,
        *,
        recipients: list[str],
        from_name_override: str,
    ) -> dict[str, Any]:
        smtp_settings = self._smtp_settings()
        recipient = ", ".join(recipients)
        if not recipients:
            return {
                "ok": False,
                "sent": False,
                "message": "Support recipient is missing",
            }
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

    def _configured_support_recipient(self) -> str:
        return (
            os.getenv("ATLAS_SUPPORT_EMAIL_RECIPIENT", "").strip()
            or "saharitam171@gmail.com"
        )

    def _recipient_list(self, recipient: str | None) -> list[str]:
        source = (recipient or self._default_recipient).strip()
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

    def _ssl_context(self) -> ssl.SSLContext:
        try:
            import certifi  # type: ignore

            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            return ssl.create_default_context()

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
