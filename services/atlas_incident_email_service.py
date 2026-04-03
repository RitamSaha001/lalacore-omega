from __future__ import annotations

import base64
import json
import os
import smtplib
import ssl
import time
from email.message import EmailMessage
from typing import Any
from urllib.request import Request, urlopen


class AtlasIncidentEmailService:
    """SMTP-backed support reporter reused by Atlas incident flows."""

    def __init__(self) -> None:
        pass

    def smtp_configured(self) -> bool:
        if self._apps_script_configured():
            return True
        sender = self._sender_email()
        password = self._sender_password()
        return bool(sender and password)

    def _from_name(self) -> str:
        return "God of Maths"

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
            from_name_override=self._from_name(),
        )

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
            or "sanny86@gmail.com"
        )
        to_email = ", ".join(recipients)
        title = str(report.get("assessment_title") or "Assessment").strip()
        assessment_type = str(report.get("assessment_type") or "Assessment").strip()
        deadline = str(report.get("deadline") or "").strip()
        msg = EmailMessage()
        msg["Subject"] = f"[ASSESSMENT REPORT] {assessment_type} - {title[:96]}"
        msg["To"] = to_email
        msg.set_content(self._build_assessment_report_body(report))
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
            or "sanny86@gmail.com"
        )
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
