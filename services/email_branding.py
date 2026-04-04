from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Iterable, Sequence


@dataclass(frozen=True)
class EmailTheme:
    accent: str
    accent_soft: str
    hero_from: str
    hero_to: str
    page_background: str = "#f2efe9"
    card_background: str = "#ffffff"
    ink: str = "#162033"
    muted: str = "#5d687b"
    border: str = "#dfe6ef"
    footer_ink: str = "#708099"
    button_text: str = "#ffffff"


def esc(value: object) -> str:
    return escape(str(value or ""))


def nl2br(value: object) -> str:
    text = esc(value)
    if not text:
        return ""
    return text.replace("\n", "<br />")


def paragraph(value: object, *, color: str = "#162033", size: int = 16) -> str:
    text = nl2br(value)
    if not text:
        return ""
    return (
        f'<p style="margin:0 0 14px;color:{color};font-size:{size}px;'
        'line-height:1.65;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,'
        'Helvetica,Arial,sans-serif;">'
        f"{text}</p>"
    )


def pill(text: object, *, background: str, color: str) -> str:
    if not str(text or "").strip():
        return ""
    return (
        f'<span style="display:inline-block;padding:8px 12px;border-radius:999px;'
        f'background:{background};color:{color};font-size:12px;font-weight:700;'
        'letter-spacing:0.08em;text-transform:uppercase;'
        'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
        f"{esc(text)}</span>"
    )


def section(title: object, body_html: str, *, accent: str, background: str = "#ffffff") -> str:
    heading = str(title or "").strip()
    return (
        f'<div style="margin:0 0 18px;padding:20px 22px;border-radius:22px;'
        f'background:{background};border:1px solid rgba(22,32,51,0.07);'
        'box-shadow:0 10px 34px rgba(15,23,42,0.05);">'
        f'<div style="margin:0 0 12px;color:{accent};font-size:12px;font-weight:700;'
        'letter-spacing:0.08em;text-transform:uppercase;'
        'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
        f"{esc(heading)}</div>"
        f"{body_html}"
        "</div>"
    )


def detail_rows(rows: Sequence[tuple[object, object]]) -> str:
    rendered: list[str] = []
    fallback_value = '<span style="color:#98a2b3;">N/A</span>'
    for label, value in rows:
        if not str(label or "").strip():
            continue
        rendered.append(
            "<tr>"
            '<td style="padding:8px 12px 8px 0;color:#6b778c;font-size:13px;'
            'font-weight:600;vertical-align:top;font-family:-apple-system,BlinkMacSystemFont,'
            'Segoe UI,Helvetica,Arial,sans-serif;">'
            f"{esc(label)}"
            "</td>"
            '<td style="padding:8px 0;color:#162033;font-size:14px;line-height:1.5;'
            'vertical-align:top;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,'
            'Helvetica,Arial,sans-serif;">'
            f"{nl2br(value) or fallback_value}"
            "</td>"
            "</tr>"
        )
    if not rendered:
        return ""
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;">'
        f"{''.join(rendered)}"
        "</table>"
    )


def metric_grid(metrics: Sequence[tuple[object, object]], *, accent: str, soft: str) -> str:
    cards: list[str] = []
    for label, value in metrics:
        if not str(label or "").strip():
            continue
        cards.append(
            '<div style="display:inline-block;vertical-align:top;width:calc(50% - 10px);'
            f'min-width:180px;margin:0 10px 10px 0;padding:16px 16px 14px;border-radius:18px;'
            f'background:{soft};border:1px solid rgba(22,32,51,0.06);">'
            f'<div style="margin:0 0 8px;color:{accent};font-size:12px;font-weight:700;'
            'letter-spacing:0.06em;text-transform:uppercase;font-family:-apple-system,'
            'BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
            f"{esc(label)}</div>"
            '<div style="color:#162033;font-size:24px;font-weight:800;line-height:1.2;'
            'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
            f"{esc(value)}"
            "</div>"
            "</div>"
        )
    if not cards:
        return ""
    return f'<div style="margin:0 0 8px;">{"".join(cards)}</div>'


def bullet_list(
    items: Iterable[object],
    *,
    accent: str,
    empty_message: str | None = None,
) -> str:
    rows: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        rows.append(
            "<tr>"
            f'<td style="padding:0 10px 12px 0;color:{accent};font-size:16px;vertical-align:top;">'
            "&#9679;"
            "</td>"
            '<td style="padding:0 0 12px;color:#223049;font-size:14px;line-height:1.6;'
            'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
            f"{nl2br(text)}"
            "</td>"
            "</tr>"
        )
    if not rows and empty_message:
        rows.append(
            "<tr>"
            f'<td style="padding:0 10px 0 0;color:{accent};font-size:16px;vertical-align:top;">'
            "&#9675;"
            "</td>"
            '<td style="padding:0;color:#6b778c;font-size:14px;line-height:1.6;'
            'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
            f"{esc(empty_message)}"
            "</td>"
            "</tr>"
        )
    if not rows:
        return ""
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;">'
        f"{''.join(rows)}"
        "</table>"
    )


def buttons(
    actions: Sequence[tuple[object, object]],
    *,
    accent: str,
    text_color: str = "#ffffff",
) -> str:
    rendered: list[str] = []
    for label, url in actions:
        if not str(label or "").strip() or not str(url or "").strip():
            continue
        rendered.append(
            f'<a href="{esc(url)}" '
            f'style="display:inline-block;margin:0 10px 10px 0;padding:14px 18px;'
            f'border-radius:14px;background:{accent};color:{text_color};text-decoration:none;'
            'font-size:14px;font-weight:700;letter-spacing:0.01em;'
            'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
            f"{esc(label)}</a>"
        )
    if not rendered:
        return ""
    return f'<div style="margin:4px 0 0;">{"".join(rendered)}</div>'


def code_block(value: object) -> str:
    text = esc(value)
    if not text:
        return ""
    return (
        '<pre style="margin:0;padding:16px 18px;border-radius:18px;overflow:auto;'
        'background:#101827;color:#e5edf8;font-size:12px;line-height:1.6;'
        'white-space:pre-wrap;word-break:break-word;'
        'font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;">'
        f"{text}</pre>"
    )


def build_email_document(
    *,
    theme: EmailTheme,
    eyebrow: str,
    title: str,
    subtitle: str,
    body_html: str,
    footer_html: str,
    preheader: str = "",
    hero_aside_html: str = "",
) -> str:
    preheader_html = (
        '<div style="display:none;max-height:0;overflow:hidden;opacity:0;'
        'mso-hide:all;font-size:1px;line-height:1px;color:transparent;">'
        f"{esc(preheader)}"
        "</div>"
        if preheader
        else ""
    )
    aside = (
        f'<td style="width:220px;padding:0 0 0 18px;vertical-align:top;">{hero_aside_html}</td>'
        if hero_aside_html
        else ""
    )
    hero_layout = (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0">'
        "<tr>"
        '<td style="vertical-align:top;">'
        f"{pill(eyebrow, background='rgba(255,255,255,0.18)', color='#ffffff')}"
        f'<div style="margin:18px 0 10px;color:#ffffff;font-size:38px;line-height:1.08;'
        'font-weight:800;letter-spacing:-0.03em;font-family:-apple-system,'
        'BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
        f"{esc(title)}</div>"
        f'<div style="max-width:380px;color:rgba(255,255,255,0.86);font-size:16px;line-height:1.65;'
        'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
        f"{nl2br(subtitle)}</div>"
        "</td>"
        f"{aside}"
        "</tr>"
        "</table>"
    )
    return (
        "<!DOCTYPE html>"
        '<html lang="en"><head><meta charset="utf-8" />'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0" />'
        "<title>LalaCore email</title></head>"
        f'<body style="margin:0;padding:0;background:{theme.page_background};">'
        f"{preheader_html}"
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'style="width:100%;background:{theme.page_background};border-collapse:collapse;">'
        "<tr><td align=\"center\" style=\"padding:28px 12px;\">"
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'style="width:100%;max-width:720px;border-collapse:separate;border-spacing:0;'
        f'background:{theme.card_background};border-radius:32px;overflow:hidden;'
        'box-shadow:0 16px 48px rgba(15,23,42,0.10);">'
        "<tr><td style=\"padding:0;\">"
        f'<div style="padding:34px 34px 30px;background:linear-gradient(135deg,{theme.hero_from} 0%,{theme.hero_to} 100%);">'
        '<div style="margin:0 0 22px;color:rgba(255,255,255,0.88);font-size:14px;font-weight:700;'
        'letter-spacing:0.04em;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
        "God of Maths x LalaCore</div>"
        f"{hero_layout}"
        "</div>"
        "</td></tr>"
        f'<tr><td style="padding:28px 28px 10px;">{body_html}</td></tr>'
        '<tr><td style="padding:8px 28px 30px;">'
        f'<div style="padding:22px 22px 0;border-top:1px solid {theme.border};'
        'color:#708099;font-size:12px;line-height:1.7;'
        'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;">'
        f"{footer_html}"
        "</div>"
        "</td></tr>"
        "</table>"
        "</td></tr></table>"
        "</body></html>"
    )
