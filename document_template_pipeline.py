from __future__ import annotations

import io
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any

from xml.sax.saxutils import escape as xml_escape

try:
    import qrcode
except Exception:  # pragma: no cover - optional until dependencies are installed
    qrcode = None

try:
    from qrbill import QRBill
except Exception:  # pragma: no cover - optional until dependencies are installed
    QRBill = None


logger = logging.getLogger(__name__)

try:
    from docxtpl import DocxTemplate, InlineImage
    from docx.shared import Mm
except Exception:  # pragma: no cover - optional until dependencies are installed
    DocxTemplate = None
    InlineImage = None
    Mm = None

try:
    import pypdf
except Exception:  # pragma: no cover - optional until dependencies are installed
    pypdf = None

try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover - optional until dependencies are installed
    fitz = None

try:
    from reportlab.lib.colors import black
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfgen import canvas
except Exception:  # pragma: no cover - optional until dependencies are installed
    black = None
    A4 = None
    ImageReader = None
    canvas = None

try:
    import win32com.client  # type: ignore
except Exception:  # pragma: no cover - optional until dependencies are installed
    win32com = None
else:
    win32com = win32com.client


TEMPLATE_FORMATS = {"docx", "pdf"}
OUTPUT_FORMATS = {"docx", "pdf"}


DEFAULT_PDF_LAYOUTS: dict[str, dict[str, dict[str, Any]]] = {
    "invoice": {
        "document_title": {"page": 1, "x": 40, "y": 800, "font_size": 18},
        "document_number": {"page": 1, "x": 430, "y": 800, "font_size": 11},
        "issue_date": {"page": 1, "x": 430, "y": 785, "font_size": 11},
        "recipient_name": {"page": 1, "x": 40, "y": 740, "font_size": 11},
        "recipient_address": {"page": 1, "x": 40, "y": 726, "font_size": 10},
        "issuer_name": {"page": 1, "x": 40, "y": 680, "font_size": 11},
        "issuer_address": {"page": 1, "x": 40, "y": 666, "font_size": 10},
        "summary_block": {"page": 1, "x": 40, "y": 540, "font_size": 10, "width": 500},
        "qr_code_image": {"page": 1, "x": 390, "y": 120, "w": 150, "h": 150},
    },
    "quotation": {
        "document_title": {"page": 1, "x": 40, "y": 800, "font_size": 18},
        "document_number": {"page": 1, "x": 430, "y": 800, "font_size": 11},
        "issue_date": {"page": 1, "x": 430, "y": 785, "font_size": 11},
        "recipient_name": {"page": 1, "x": 40, "y": 740, "font_size": 11},
        "recipient_address": {"page": 1, "x": 40, "y": 726, "font_size": 10},
        "summary_block": {"page": 1, "x": 40, "y": 560, "font_size": 10, "width": 500},
    },
    "ledger_journal": {
        "document_title": {"page": 1, "x": 40, "y": 800, "font_size": 18},
        "document_number": {"page": 1, "x": 430, "y": 800, "font_size": 11},
        "issue_date": {"page": 1, "x": 430, "y": 785, "font_size": 11},
        "summary_block": {"page": 1, "x": 40, "y": 740, "font_size": 9, "width": 520},
    },
}


def normalize_template_format(value: str) -> str:
    format_name = str(value or "").strip().lower()
    if format_name not in TEMPLATE_FORMATS:
        raise ValueError("template_format must be docx or pdf")
    return format_name


def normalize_output_format(value: str) -> str:
    format_name = str(value or "").strip().lower()
    if format_name not in OUTPUT_FORMATS:
        raise ValueError("output_format must be docx or pdf")
    return format_name


def _safe_filename(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "generated-document"
    cleaned: list[str] = []
    for character in text:
        if character.isalnum() or character in {"-", "_", "."}:
            cleaned.append(character)
        else:
            cleaned.append("-")
    return "".join(cleaned).strip("-_.") or "generated-document"


def _extract_explicit_output_filename(payload: dict[str, Any]) -> str:
    explicit_keys = [
        "output_filename",
        "filename",
        "file_name",
        "document_filename",
        "generated_filename",
        "output_name",
    ]
    for key in explicit_keys:
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _normalize_generation_date_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return date.today().isoformat()
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    return date.today().isoformat()


def _resolve_output_filename_stem(*, template_name: str, payload: dict[str, Any], context: dict[str, Any]) -> str:
    explicit_name = _extract_explicit_output_filename(payload)
    if explicit_name:
        return _safe_filename(Path(explicit_name).stem or explicit_name)

    template_stem = Path(str(template_name or "template")).stem or "template"
    generation_date = _normalize_generation_date_token(
        payload.get("generation_date")
        or context.get("generation_date")
        or payload.get("issue_date")
        or payload.get("date")
    )
    return _safe_filename(f"{template_stem}-{generation_date}")


def _stringify(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return "\n".join(_stringify(item) for item in value if _stringify(item))
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _flatten_lines(value: Any) -> list[str]:
    if value in (None, "", [], {}):
        return []
    if isinstance(value, list):
        lines: list[str] = []
        for item in value:
            lines.extend(_flatten_lines(item))
        return lines
    text = _stringify(value)
    return [line for line in text.splitlines() if line.strip()]


def _to_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("'", "").replace(" ", "")
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(",", "")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def _compute_line_item_totals(context: dict[str, Any]) -> None:
    items = context.get("line_items") if isinstance(context.get("line_items"), list) else []
    computed_items: list[dict[str, Any]] = []
    subtotal = 0.0
    tax_total = 0.0

    for item in items:
        row = dict(item) if isinstance(item, dict) else {}
        qty = _to_float_or_none(row.get("quantity"))
        unit_price = _to_float_or_none(row.get("unit_price"))
        amount = _to_float_or_none(row.get("amount"))

        if amount is None and qty is not None and unit_price is not None:
            amount = round(qty * unit_price, 2)
            row["amount"] = amount

        vat_rate = _to_float_or_none(row.get("vat_rate"))
        vat_amount = _to_float_or_none(row.get("vat_amount"))
        if vat_amount is None and amount is not None and vat_rate is not None:
            vat_amount = round(amount * vat_rate / 100.0, 2)
            row["vat_amount"] = vat_amount

        if amount is not None:
            subtotal += amount
        if vat_amount is not None:
            tax_total += vat_amount

        computed_items.append(row)

    context["line_items"] = computed_items

    if computed_items:
        if context.get("subtotal") in (None, ""):
            context["subtotal"] = round(subtotal, 2)
        if context.get("tax_total") in (None, ""):
            context["tax_total"] = round(tax_total, 2)
        if context.get("grand_total") in (None, ""):
            context["grand_total"] = round(subtotal + tax_total, 2)


def build_template_context(document_kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    context = dict(payload or {})
    kind = str(document_kind or context.get("document_kind") or "document").strip().lower() or "document"
    context["document_kind"] = kind
    context.setdefault("document_title", context.get("title") or context.get("name") or context.get("subject") or kind.title())
    context.setdefault("document_number", context.get("invoice_number") or context.get("quotation_number") or context.get("journal_number") or context.get("number") or "")
    context.setdefault("issue_date", context.get("invoice_date") or context.get("quotation_date") or context.get("document_date") or context.get("date") or "")
    context.setdefault("currency", context.get("currency") or "CHF")
    context.setdefault("line_items", context.get("line_items") or [])
    context.setdefault("notes", context.get("notes") or context.get("remarks") or "")
    context.setdefault("summary_block", context.get("summary_block") or context.get("body_text") or context.get("notes") or "")
    context.setdefault("recipient_name", context.get("recipient_name") or context.get("customer_name") or "")
    context.setdefault("recipient_address", context.get("recipient_address") or context.get("customer_address") or "")
    context.setdefault("issuer_name", context.get("issuer_name") or context.get("supplier_name") or context.get("seller_name") or "")
    context.setdefault("issuer_address", context.get("issuer_address") or context.get("supplier_address") or context.get("seller_address") or "")
    context.setdefault("subtotal", context.get("subtotal") or context.get("net_amount") or "")
    context.setdefault("tax_total", context.get("tax_total") or context.get("vat_amount") or context.get("tax_amount") or "")
    context.setdefault("grand_total", context.get("grand_total") or context.get("total_amount") or context.get("gross_amount") or "")
    _compute_line_item_totals(context)
    return context


def build_swiss_qr_bill_payload(qr_data: dict[str, Any], diagnostics_out: dict[str, Any] | None = None) -> str:
    payment = dict(qr_data or {})
    creditor = payment.get("creditor") if isinstance(payment.get("creditor"), dict) else {}
    debtor = payment.get("debtor") if isinstance(payment.get("debtor"), dict) else {}

    def _mask_account(value: str) -> str:
        text = str(value or "").strip().replace(" ", "")
        if len(text) <= 8:
            return text
        return f"{text[:4]}...{text[-4:]}"

    if QRBill is not None:
        try:
            account = str(payment.get("qr_iban") or payment.get("iban") or "").strip().replace(" ", "")

            def _to_addr(source: dict[str, Any], fallback_country: str = "CH") -> dict[str, Any]:
                name = str(source.get("name") or "").strip()
                street = str(source.get("street") or "").strip()
                house_num = str(source.get("house_no") or source.get("house_num") or "").strip()
                pcode = str(source.get("postal_code") or source.get("pcode") or "").strip()
                city = str(source.get("city") or "").strip()
                country = str(source.get("country") or fallback_country).strip() or fallback_country
                return {
                    "name": name,
                    "street": street,
                    "house_num": house_num,
                    "pcode": pcode,
                    "city": city,
                    "country": country,
                }

            creditor_addr = _to_addr(
                {
                    "name": creditor.get("name") or payment.get("creditor_name"),
                    "street": creditor.get("street") or payment.get("creditor_street"),
                    "house_no": creditor.get("house_no") or payment.get("creditor_house_no"),
                    "postal_code": creditor.get("postal_code") or payment.get("creditor_postal_code"),
                    "city": creditor.get("city") or payment.get("creditor_city"),
                    "country": creditor.get("country") or payment.get("creditor_country") or "CH",
                }
            )

            debtor_name = debtor.get("name") or payment.get("debtor_name")
            debtor_addr = None
            if str(debtor_name or "").strip():
                debtor_addr = _to_addr(
                    {
                        "name": debtor_name,
                        "street": debtor.get("street") or payment.get("debtor_street"),
                        "house_no": debtor.get("house_no") or payment.get("debtor_house_no"),
                        "postal_code": debtor.get("postal_code") or payment.get("debtor_postal_code"),
                        "city": debtor.get("city") or payment.get("debtor_city"),
                        "country": debtor.get("country") or payment.get("debtor_country") or "CH",
                    }
                )

            amount_value = payment.get("amount")
            amount_str = None
            if amount_value not in (None, ""):
                amount_str = f"{float(amount_value):.2f}"

            reference = str(payment.get("reference") or payment.get("qr_reference") or "").strip()
            additional_information = str(payment.get("additional_information") or payment.get("additional_info") or "").strip()
            if len(additional_information) > 140:
                additional_information = additional_information[:140]

            common_kwargs = {
                "account": account,
                "creditor": creditor_addr,
                "amount": amount_str,
                "currency": str(payment.get("currency") or "CHF").strip() or "CHF",
                "debtor": debtor_addr,
                "additional_information": additional_information,
                "language": "de",
                "top_line": False,
                "payment_line": False,
            }

            # First try with provided reference (QRR / SCOR / NON as validated by qrbill).
            try:
                bill = QRBill(
                    **common_kwargs,
                    reference_number=reference.replace(" ", "") if reference else None,
                )
                if isinstance(diagnostics_out, dict):
                    diagnostics_out.clear()
                    diagnostics_out.update(
                        {
                            "backend": "qrbill",
                            "mode": "direct",
                            "ref_type": str(getattr(bill, "ref_type", "") or "NON"),
                            "has_reference": bool(reference.strip()),
                            "account": _mask_account(account),
                        }
                    )
                logger.info(
                    "swiss_qr_payload_generated backend=qrbill mode=direct ref_type=%s has_reference=%s account=%s",
                    str(getattr(bill, "ref_type", "") or "NON"),
                    bool(reference.strip()),
                    _mask_account(account),
                )
                return bill.qr_data()
            except Exception as ref_error:
                # If reference/account combination is invalid for Swiss QR (e.g. QRR with non-QR IBAN),
                # retry with NON reference to still emit a standards-compliant payload.
                bill = QRBill(
                    **common_kwargs,
                    reference_number=None,
                )
                if isinstance(diagnostics_out, dict):
                    diagnostics_out.clear()
                    diagnostics_out.update(
                        {
                            "backend": "qrbill",
                            "mode": "fallback_non",
                            "ref_type": str(getattr(bill, "ref_type", "") or "NON"),
                            "has_reference": bool(reference.strip()),
                            "account": _mask_account(account),
                            "reason": str(ref_error),
                        }
                    )
                logger.warning(
                    "swiss_qr_payload_generated backend=qrbill mode=fallback_non reason=%s original_has_reference=%s account=%s",
                    str(ref_error),
                    bool(reference.strip()),
                    _mask_account(account),
                )
                return bill.qr_data()
        except Exception as qrbill_error:
            # Fall back to legacy serializer if qrbill validation fails for edge payloads.
            if isinstance(diagnostics_out, dict):
                diagnostics_out.clear()
                diagnostics_out.update(
                    {
                        "backend": "legacy",
                        "mode": "fallback_legacy",
                        "reason": str(qrbill_error),
                    }
                )
            logger.warning(
                "swiss_qr_payload_generated backend=legacy reason=qrbill_unavailable_or_invalid detail=%s",
                str(qrbill_error),
            )
            pass

    lines = [
        "SPC",
        "0200",
        "1",
        str(payment.get("qr_iban") or payment.get("iban") or "").strip(),
        str(creditor.get("name") or payment.get("creditor_name") or "").strip(),
        str(creditor.get("street") or payment.get("creditor_street") or "").strip(),
        str(creditor.get("house_no") or payment.get("creditor_house_no") or "").strip(),
        str(creditor.get("postal_code") or payment.get("creditor_postal_code") or "").strip(),
        str(creditor.get("city") or payment.get("creditor_city") or "").strip(),
        str(creditor.get("country") or payment.get("creditor_country") or "CH").strip() or "CH",
        "",
        f"{float(payment.get('amount')):.2f}" if payment.get("amount") not in (None, "") else "",
        str(payment.get("currency") or "CHF").strip() or "CHF",
        str(debtor.get("name") or payment.get("debtor_name") or "").strip(),
        str(debtor.get("street") or payment.get("debtor_street") or "").strip(),
        str(debtor.get("house_no") or payment.get("debtor_house_no") or "").strip(),
        str(debtor.get("postal_code") or payment.get("debtor_postal_code") or "").strip(),
        str(debtor.get("city") or payment.get("debtor_city") or "").strip(),
        str(debtor.get("country") or payment.get("debtor_country") or "").strip(),
        str(payment.get("reference_type") or ("QRR" if str(payment.get("reference") or payment.get("qr_reference") or "").strip() else "NON")).strip() or "NON",
        str(payment.get("reference") or payment.get("qr_reference") or "").strip(),
        str(payment.get("additional_information") or payment.get("additional_info") or "").strip(),
    ]
    while len(lines) < 34:
        lines.append("")
    if isinstance(diagnostics_out, dict):
        diagnostics_out.clear()
        diagnostics_out.update(
            {
                "backend": "legacy",
                "mode": "legacy_serializer",
            }
        )
    return "\n".join(lines[:34])


def _render_qr_png_bytes(payload_text: str) -> bytes:
    if qrcode is None:
        raise RuntimeError("qrcode is not installed; run install_document_generation.ps1 first")
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=8, border=0)
    qr.add_data(payload_text)
    qr.make(fit=True)
    image = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _normalize_field_label_candidates(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return out
    return []


def extract_pdf_text_spans(template_bytes: bytes) -> list[dict[str, Any]]:
    if pypdf is None:
        raise RuntimeError("pypdf is not installed; run install_document_generation.ps1 first")
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(template_bytes))
    spans: list[dict[str, Any]] = []

    for page_index, page in enumerate(reader.pages, start=1):
        page_spans: list[dict[str, Any]] = []

        def _visitor(text: str, cm: Any, tm: Any, font_dict: Any, font_size: float) -> None:
            raw = str(text or "")
            normalized = raw.strip()
            if not normalized:
                return
            x = float(tm[4]) if len(tm) > 4 else 0.0
            y = float(tm[5]) if len(tm) > 5 else 0.0
            base_font = ""
            if isinstance(font_dict, dict):
                base_font = str(font_dict.get("/BaseFont") or "").strip()
            page_spans.append(
                {
                    "page": page_index,
                    "x": x,
                    "y": y,
                    "font_size": float(font_size or 10.0),
                    "font_name": base_font,
                    "text": normalized,
                }
            )

        page.extract_text(visitor_text=_visitor)
        spans.extend(page_spans)

    return spans


def infer_pdf_rewrite_layout(
    *,
    template_bytes: bytes,
    payload: dict[str, Any],
    template_payload: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    template_root = dict(template_payload or {})
    field_rules = template_root.get("field_rules") if isinstance(template_root.get("field_rules"), dict) else {}
    field_labels = field_rules.get("field_labels") if isinstance(field_rules.get("field_labels"), dict) else {}
    if not field_labels:
        return {}

    spans = extract_pdf_text_spans(template_bytes)
    layout: dict[str, dict[str, Any]] = {}

    for field_name, raw_labels in field_labels.items():
        value = payload.get(field_name)
        if value in (None, "", [], {}):
            continue

        labels = _normalize_field_label_candidates(raw_labels)
        if not labels:
            continue

        label_span = None
        matched_label = ""
        for candidate in labels:
            candidate_lower = candidate.lower()
            for span in spans:
                text_lower = str(span.get("text") or "").lower()
                if text_lower == candidate_lower or text_lower.startswith(candidate_lower) or candidate_lower in text_lower:
                    label_span = span
                    matched_label = candidate
                    break
            if label_span is not None:
                break

        if label_span is None:
            continue

        same_line_candidates = [
            span
            for span in spans
            if int(span.get("page") or 0) == int(label_span.get("page") or 0)
            and float(span.get("x") or 0.0) > float(label_span.get("x") or 0.0)
            and abs(float(span.get("y") or 0.0) - float(label_span.get("y") or 0.0)) <= 4.0
        ]
        same_line_candidates.sort(key=lambda span: float(span.get("x") or 0.0))
        value_span = same_line_candidates[0] if same_line_candidates else None

        font_size = float((value_span or label_span).get("font_size") or 10.0)
        font_name = str((value_span or label_span).get("font_name") or "").strip()
        x = float((value_span or label_span).get("x") or 0.0)
        y = float((value_span or label_span).get("y") or 0.0)
        old_text = str((value_span or {}).get("text") or "")
        if not value_span:
            approx_offset = max(len(matched_label) * font_size * 0.45, 40.0)
            x = float(label_span.get("x") or 0.0) + approx_offset
            y = float(label_span.get("y") or 0.0)

        estimated_width = max((len(old_text) if old_text else 12) * font_size * 0.58, 48.0)
        erase_padding_below = max(font_size * 0.02, 0.2)
        erase_height = max(font_size * 0.66, 8.0)
        layout[str(field_name)] = {
            "page": int((value_span or label_span).get("page") or 1),
            "x": round(x, 1),
            "y": round(y, 1),
            "font_size": round(font_size, 1),
            "font_name": font_name,
            "sample_text": old_text,
            "erase_box": {
                "x": round(x - 2.0, 1),
                "y": round(y - erase_padding_below, 1),
                "w": round(estimated_width + 6.0, 1),
                "h": round(erase_height, 1),
            },
        }

    return layout


def _normalize_qr_region_label_config(template_payload: dict[str, Any] | None = None) -> dict[str, list[str]]:
    field_rules = template_payload.get("field_rules") if isinstance((template_payload or {}).get("field_rules"), dict) else {}
    config = field_rules.get("qr_region_labels") if isinstance(field_rules.get("qr_region_labels"), dict) else {}
    defaults = {
        "top": ["Empfangsschein", "Zahlteil", "Konto / Zahlbar an"],
        "bottom": ["Annahmestelle"],
        "account": ["Konto / Zahlbar an"],
        "reference": ["Referenz"],
        "debtor": ["Zahlbar durch"],
        "amount": ["Betrag"],
        "currency": ["Währung"],
    }
    out: dict[str, list[str]] = {}
    for key, values in defaults.items():
        override = config.get(key)
        candidates = _normalize_field_label_candidates(override) if override is not None else []
        out[key] = candidates or values
    return out


def _find_matching_spans(spans: list[dict[str, Any]], labels: list[str]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    normalized_labels = [str(label or "").strip().lower() for label in labels if str(label or "").strip()]
    for span in spans:
        text_lower = str(span.get("text") or "").strip().lower()
        if not text_lower:
            continue
        if any(text_lower == label or text_lower.startswith(label) or label in text_lower for label in normalized_labels):
            matches.append(span)
    return matches


def _format_swiss_reference_visual(reference_value: Any) -> str:
    reference_visual = str(reference_value or "").strip()
    ref_digits = "".join(ch for ch in reference_visual if ch.isdigit())
    if len(ref_digits) == 27:
        return f"{ref_digits[:2]} {ref_digits[2:7]} {ref_digits[7:12]} {ref_digits[12:17]} {ref_digits[17:22]} {ref_digits[22:27]}"
    return reference_visual


def _build_span_overwrite_spec(
    span: dict[str, Any],
    width_padding: float = 2.0,
    expected_text: str = "",
    max_erase_w: float | None = None,
    include_erase_box: bool = True,
    draw_erase_box: bool = True,
    explicit_erase_box: dict[str, Any] | None = None,
) -> dict[str, Any]:
    x = float(span.get("x") or 0.0)
    y = float(span.get("y") or 0.0)
    font_size = float(span.get("font_size") or 10.0)
    sample_text = str(span.get("text") or "")
    width_chars = max(len(sample_text), len(str(expected_text or "")))
    estimated_width = max((width_chars if width_chars else 8) * font_size * 0.58, 34.0)
    erase_padding_below = max(font_size * 0.01, 0.1)
    erase_height = max(font_size * 0.62, 7.0)
    if isinstance(max_erase_w, (int, float)) and max_erase_w > 0:
        estimated_width = min(estimated_width, float(max_erase_w))
    spec = {
        "page": int(span.get("page") or 1),
        "x": round(x, 1),
        "y": round(y, 1),
        "font_size": round(font_size, 1),
        "font_name": str(span.get("font_name") or "").strip(),
        "sample_text": sample_text,
    }
    if include_erase_box:
        if isinstance(explicit_erase_box, dict):
            spec["erase_box"] = {
                "x": round(float(explicit_erase_box.get("x") or x), 1),
                "y": round(float(explicit_erase_box.get("y") or (y - erase_padding_below)), 1),
                "w": round(float(explicit_erase_box.get("w") or (estimated_width + width_padding)), 1),
                "h": round(float(explicit_erase_box.get("h") or erase_height), 1),
            }
        else:
            spec["erase_box"] = {
                "x": round(x, 1),
                "y": round(y - erase_padding_below, 1),
                "w": round(estimated_width + width_padding, 1),
                "h": round(erase_height, 1),
            }
        spec["draw_erase_box"] = bool(draw_erase_box)
    return spec


def infer_pdf_qr_bill_overwrite_layout(
    *,
    template_bytes: bytes,
    qr_data: dict[str, Any],
    template_payload: dict[str, Any] | None = None,
    qr_bill_spec: dict[str, Any] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    spans = extract_pdf_text_spans(template_bytes)
    if not spans or not isinstance(qr_data, dict):
        return {}, {}

    labels = _normalize_qr_region_label_config(template_payload)
    region = qr_bill_spec if isinstance(qr_bill_spec, dict) else infer_pdf_qr_bill_region(template_bytes, template_payload)
    if not isinstance(region, dict):
        return {}, {}

    page = int(region.get("page") or 1)
    region_x = float(region.get("x") or 0.0)
    region_y = float(region.get("y") or 0.0)
    region_w = float(region.get("w") or 0.0)
    region_h = float(region.get("h") or 0.0)
    top_y = region_y + region_h
    region_mid_x = region_x + (region_w / 2.0)

    page_spans = [
        span
        for span in spans
        if int(span.get("page") or 0) == page
        and region_x <= float(span.get("x") or 0.0) <= (region_x + region_w)
        and region_y <= float(span.get("y") or 0.0) <= top_y
    ]
    if not page_spans:
        return {}, {}

    layout: dict[str, dict[str, Any]] = {}
    values: dict[str, Any] = {}

    precise_rects: list[Any] = []
    page_height = 0.0
    fitz_page = None
    if fitz is not None:
        try:
            doc = fitz.open(stream=template_bytes, filetype="pdf")
            if 0 <= (page - 1) < len(doc):
                fitz_page = doc[page - 1]
                page_height = float(fitz_page.rect.height)
        except Exception:
            fitz_page = None
            page_height = 0.0

    reference_value = _format_swiss_reference_visual(qr_data.get("reference") or qr_data.get("qr_reference"))
    if reference_value:
        reference_spans = _find_matching_spans(page_spans, labels["reference"])
        reference_pattern = re.compile(r"\d{2}(?:\s\d{5}){4}\s\d{5}")
        all_reference_candidates = [
            span
            for span in page_spans
            if reference_pattern.search(str(span.get("text") or ""))
        ]
        reference_targets: list[dict[str, Any]] = []

        # Prefer one value span per detected "Referenz" label (left and right blocks).
        for label_span in sorted(reference_spans, key=lambda span: float(span.get("x") or 0.0)):
            label_x = float(label_span.get("x") or 0.0)
            label_y = float(label_span.get("y") or 0.0)
            label_is_left = label_x < region_mid_x
            candidates_for_label = [
                span
                for span in all_reference_candidates
                if float(span.get("x") or 0.0) >= (label_x - 4.0)
                and abs(float(span.get("y") or 0.0) - (label_y - 14.0)) <= 18.0
                and ((float(span.get("x") or 0.0) < region_mid_x) == label_is_left)
            ]
            if not candidates_for_label:
                continue
            target = min(
                candidates_for_label,
                key=lambda span: (
                    abs(float(span.get("y") or 0.0) - (label_y - 14.0)),
                    abs(float(span.get("x") or 0.0) - label_x),
                ),
            )
            reference_targets.append(target)

        if not reference_targets and all_reference_candidates:
            reference_targets = sorted(all_reference_candidates, key=lambda span: float(span.get("x") or 0.0))

        if reference_targets:
            unique_targets: list[dict[str, Any]] = []
            seen: set[tuple[int, int]] = set()
            for span in reference_targets:
                key = (int(round(float(span.get("x") or 0.0))), int(round(float(span.get("y") or 0.0))))
                if key in seen:
                    continue
                seen.add(key)
                unique_targets.append(span)

            for index, target_span in enumerate(unique_targets, start=1):
                field_key = f"qr_reference_value_{index}"
                erase_x = float(target_span.get("x") or 0.0)
                if float(target_span.get("x") or 0.0) < region_mid_x:
                    # Left receipt column: keep erase box inside the left half.
                    max_right = region_mid_x - 18.0
                else:
                    # Right payment column: stay inside region right boundary.
                    max_right = region_x + region_w - 12.0
                max_erase_w = max(22.0, max_right - erase_x)

                explicit_erase_box = None
                old_text = str(target_span.get("text") or "").strip()
                if fitz_page is not None and page_height > 0 and old_text:
                    try:
                        rects = fitz_page.search_for(old_text)
                    except Exception:
                        rects = []
                    if rects:
                        target_x = float(target_span.get("x") or 0.0)
                        target_y = float(target_span.get("y") or 0.0)
                        best = min(
                            rects,
                            key=lambda r: abs(float(r.x0) - target_x) + abs((page_height - float(r.y1)) - target_y),
                        )
                        x0 = float(best.x0)
                        y0 = page_height - float(best.y1)
                        w0 = float(best.x1) - float(best.x0)
                        h0 = float(best.y1) - float(best.y0)
                        w0 = min(max(w0, 10.0), max_erase_w)
                        explicit_erase_box = {
                            "x": x0,
                            "y": y0,
                            "w": w0,
                            "h": max(h0, 7.0),
                        }

                layout[field_key] = _build_span_overwrite_spec(
                    target_span,
                    width_padding=2.0,
                    expected_text=reference_value,
                    max_erase_w=max_erase_w,
                    include_erase_box=True,
                    draw_erase_box=False,
                    explicit_erase_box=explicit_erase_box,
                )
                values[field_key] = reference_value

    if fitz_page is not None:
        try:
            fitz_page.parent.close()
        except Exception:
            pass

    return layout, values


def _infer_qr_code_image_spec_from_qr_bill_region(
    template_bytes: bytes,
    region_spec: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(region_spec, dict):
        return None
    x = float(region_spec.get("x") or 0.0)
    y = float(region_spec.get("y") or 0.0)
    w = float(region_spec.get("w") or 0.0)
    h = float(region_spec.get("h") or 0.0)
    if w <= 0 or h <= 0:
        return None

    # Prefer the exact existing QR image bounds from the template so we replace
    # on the same side/location and avoid visual overlap with the old code.
    if fitz is not None:
        try:
            doc = fitz.open(stream=template_bytes, filetype="pdf")
            page_idx = int(region_spec.get("page") or 1) - 1
            if 0 <= page_idx < len(doc):
                page = doc[page_idx]
                page_height = float(page.rect.height)
                region_left = x
                region_right = x + w
                region_bottom = y
                region_top = y + h
                split_x = x + (w * 0.33)
                expected_qr_x = split_x + 14.0
                expected_qr_size = min(max(w * 0.22, 80.0), 120.0)

                text_dict = page.get_text("dict")
                candidates: list[tuple[float, float, float, float]] = []
                for block in text_dict.get("blocks", []):
                    if int(block.get("type") or 0) != 1:
                        continue
                    bbox = block.get("bbox")
                    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
                        continue
                    x0 = float(bbox[0])
                    y0_top = float(bbox[1])
                    x1 = float(bbox[2])
                    y1_top = float(bbox[3])
                    y0 = page_height - y1_top
                    y1 = page_height - y0_top
                    iw = max(0.0, x1 - x0)
                    ih = max(0.0, y1 - y0)
                    if iw <= 24.0 or ih <= 24.0:
                        continue
                    # Keep images that are inside slip region and at/near the payment-part QR lane.
                    if x0 < region_left or x1 > region_right:
                        continue
                    if y0 < region_bottom or y1 > region_top:
                        continue
                    if x1 < (split_x - 4.0):
                        continue
                    candidates.append((x0, y0, iw, ih))

                if candidates:
                    # Choose image closest to expected QR lane/size while favoring larger blocks.
                    best = min(
                        candidates,
                        key=lambda item: (
                            abs(item[0] - expected_qr_x)
                            + abs(item[2] - expected_qr_size)
                            + abs(item[3] - expected_qr_size)
                            - (0.02 * (item[2] * item[3]))
                        ),
                    )
                    qr_x, qr_y, qr_w, qr_h = best
                    doc.close()
                    return {
                        "page": int(region_spec.get("page") or 1),
                        "x": round(qr_x, 1),
                        "y": round(qr_y, 1),
                        "w": round(qr_w, 1),
                        "h": round(qr_h, 1),
                        "erase_box": {
                            "x": round(qr_x, 1),
                            "y": round(qr_y, 1),
                            "w": round(qr_w, 1),
                            "h": round(qr_h, 1),
                        },
                        "draw_erase_box": False,
                    }
            doc.close()
        except Exception:
            pass

    split_x = x + (w * 0.33)
    top_extension = max(10.0, min(16.0, h * 0.07))
    bg_top = y + h + top_extension
    header_line_y = bg_top - 32.0
    content_top = header_line_y - 16.0
    qr_size = min(max(w * 0.22, 80.0), 120.0)
    qr_x = split_x + 14.0
    qr_y = max(y + 90.0, content_top - qr_size)

    return {
        "page": int(region_spec.get("page") or 1),
        "x": round(qr_x, 1),
        "y": round(qr_y, 1),
        "w": round(qr_size, 1),
        "h": round(qr_size, 1),
        # Redact only the QR square from template, then draw the new QR image.
        "erase_box": {
            "x": round(qr_x, 1),
            "y": round(qr_y, 1),
            "w": round(qr_size, 1),
            "h": round(qr_size, 1),
        },
        # Template redaction handles clearing; avoid overlaying a large white rect.
        "draw_erase_box": False,
    }


def infer_pdf_qr_bill_region(template_bytes: bytes, template_payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if pypdf is None:
        raise RuntimeError("pypdf is not installed; run install_document_generation.ps1 first")
    from pypdf import PdfReader

    spans = extract_pdf_text_spans(template_bytes)
    if not spans:
        return None

    labels = _normalize_qr_region_label_config(template_payload)
    top_matches = _find_matching_spans(spans, labels["top"])
    bottom_matches = _find_matching_spans(spans, labels["bottom"])
    if not top_matches or not bottom_matches:
        return None

    page = int(top_matches[0].get("page") or 1)
    page_spans = [span for span in spans if int(span.get("page") or 0) == page]
    if not page_spans:
        return None

    reader = PdfReader(io.BytesIO(template_bytes))
    pdf_page = reader.pages[page - 1]
    page_width = float(pdf_page.mediabox.width)

    top_y = max(float(span.get("y") or 0.0) for span in top_matches) + 6.0
    bottom_y = min(float(span.get("y") or 0.0) for span in bottom_matches) - 8.0
    left_x = max(min(float(span.get("x") or 0.0) for span in top_matches) - 8.0, 8.0)
    right_x = page_width - 8.0
    width = max(right_x - left_x, 100.0)
    height = max(top_y - bottom_y, 80.0)

    return {
        "page": page,
        "x": round(left_x, 1),
        "y": round(bottom_y, 1),
        "w": round(width, 1),
        "h": round(height, 1),
    }


def infer_qr_data_from_pdf_text(template_bytes: bytes, template_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    spans = extract_pdf_text_spans(template_bytes)
    if not spans:
        return {}

    labels = _normalize_qr_region_label_config(template_payload)
    region = infer_pdf_qr_bill_region(template_bytes, template_payload)
    if not region:
        return {}

    page = int(region.get("page") or 1)
    region_x = float(region.get("x") or 0.0)
    region_y = float(region.get("y") or 0.0)
    region_w = float(region.get("w") or 0.0)
    region_h = float(region.get("h") or 0.0)
    region_mid_x = region_x + (region_w / 2.0)
    top_y = region_y + region_h

    page_spans = [
        span for span in spans
        if int(span.get("page") or 0) == page
        and region_x <= float(span.get("x") or 0.0) <= (region_x + region_w)
        and region_y <= float(span.get("y") or 0.0) <= top_y
    ]
    page_spans.sort(key=lambda span: (-float(span.get("y") or 0.0), float(span.get("x") or 0.0)))

    result: dict[str, Any] = {}

    iban_pattern = re.compile(r"CH\d{2}(?:\s?\d){5,}")
    reference_pattern = re.compile(r"\d{2}(?:\s\d{5}){4}\s\d{5}")
    amount_pattern = re.compile(r"\d+[\.,]\d{2}")

    account_spans = _find_matching_spans(page_spans, labels["account"])
    reference_spans = _find_matching_spans(page_spans, labels["reference"])
    debtor_spans = _find_matching_spans(page_spans, labels["debtor"])
    amount_spans = _find_matching_spans(page_spans, labels["amount"])
    currency_spans = _find_matching_spans(page_spans, labels["currency"])

    right_account_label = max(account_spans, key=lambda span: float(span.get("x") or 0.0), default=None)
    right_reference_label = max(reference_spans, key=lambda span: float(span.get("x") or 0.0), default=None)
    right_debtor_label = max(debtor_spans, key=lambda span: float(span.get("x") or 0.0), default=None)

    if right_account_label and right_reference_label:
        creditor_lines = [
            span for span in page_spans
            if float(span.get("x") or 0.0) >= max(float(right_account_label.get("x") or 0.0) - 5.0, region_mid_x)
            and float(right_reference_label.get("y") or 0.0) < float(span.get("y") or 0.0) < float(right_account_label.get("y") or 0.0)
        ]
        creditor_lines.sort(key=lambda span: -float(span.get("y") or 0.0))
        creditor_values = [str(span.get("text") or "").strip() for span in creditor_lines if str(span.get("text") or "").strip()]
        for value in creditor_values:
            match = iban_pattern.search(value)
            if match and not result.get("qr_iban"):
                result["qr_iban"] = re.sub(r"\s+", " ", match.group(0)).strip()
                continue
            if not result.get("creditor_name"):
                result["creditor_name"] = value
            elif re.search(r"\b\d{4}\b", value) and not result.get("creditor_postal_code"):
                postal_match = re.search(r"\b(\d{4})\b\s+(.*)$", value)
                if postal_match:
                    result["creditor_postal_code"] = postal_match.group(1)
                    result["creditor_city"] = postal_match.group(2).strip()
                else:
                    result["creditor_street"] = value
            elif not result.get("creditor_street"):
                result["creditor_street"] = value

    if right_reference_label and right_debtor_label:
        reference_lines = [
            span for span in page_spans
            if float(span.get("x") or 0.0) >= max(float(right_reference_label.get("x") or 0.0) - 5.0, region_mid_x)
            and float(right_debtor_label.get("y") or 0.0) < float(span.get("y") or 0.0) < float(right_reference_label.get("y") or 0.0)
        ]
        reference_lines.sort(key=lambda span: -float(span.get("y") or 0.0))
        for span in reference_lines:
            match = reference_pattern.search(str(span.get("text") or ""))
            if match:
                result["reference"] = match.group(0)
                result.setdefault("reference_type", "QRR")
                break

    if right_debtor_label:
        max_amount_y = max((float(span.get("y") or 0.0) for span in amount_spans), default=region_y)
        debtor_lines = [
            span for span in page_spans
            if float(span.get("x") or 0.0) >= max(float(right_debtor_label.get("x") or 0.0) - 5.0, region_mid_x)
            and max_amount_y < float(span.get("y") or 0.0) < float(right_debtor_label.get("y") or 0.0)
        ]
        debtor_lines.sort(key=lambda span: -float(span.get("y") or 0.0))
        debtor_values = [str(span.get("text") or "").strip() for span in debtor_lines if str(span.get("text") or "").strip()]
        if debtor_values:
            result.setdefault("debtor_name", debtor_values[0])
        for value in debtor_values[1:]:
            if re.search(r"\b\d{4}\b", value) and not result.get("debtor_postal_code"):
                postal_match = re.search(r"\b(\d{4})\b\s+(.*)$", value)
                if postal_match:
                    result["debtor_postal_code"] = postal_match.group(1)
                    result["debtor_city"] = postal_match.group(2).strip()
            elif not result.get("debtor_street"):
                result["debtor_street"] = value

    numeric_amounts: list[tuple[float, str]] = []
    for span in page_spans:
        text = str(span.get("text") or "")
        for match in amount_pattern.findall(text):
            numeric_amounts.append((float(span.get("y") or 0.0), match))
    if numeric_amounts and not result.get("amount"):
        amount_text = sorted(numeric_amounts, key=lambda item: item[0])[0][1]
        result["amount"] = amount_text.replace(",", ".")

    for span in page_spans:
        text = str(span.get("text") or "").strip().upper()
        if text in {"CHF", "EUR"}:
            result.setdefault("currency", text)
            break

    info_label_y = None
    info_lines: list[tuple[float, str]] = []
    for span in page_spans:
        text = str(span.get("text") or "").strip()
        if not text:
            continue
        lower = text.lower()
        if "zusatzliche information" in lower or "zus\u00e4tzliche information" in lower:
            info_label_y = float(span.get("y") or 0.0)
            continue
        if info_label_y is not None and float(span.get("y") or 0.0) < info_label_y and float(span.get("y") or 0.0) > (info_label_y - 44.0):
            info_lines.append((float(span.get("y") or 0.0), text))

    if info_lines:
        info_lines.sort(key=lambda item: -item[0])
        joined = "\n".join(line for _, line in info_lines if line)
        if joined.strip():
            result.setdefault("additional_information", joined.strip())

    result.setdefault("creditor_country", "CH")
    result.setdefault("debtor_country", "CH")
    return result


def _draw_swiss_qr_bill_region(pdf_canvas: Any, region_spec: dict[str, Any], qr_data: dict[str, Any], qr_png_bytes: bytes) -> None:
    x = float(region_spec.get("x") or 0)
    y = float(region_spec.get("y") or 0)
    w = float(region_spec.get("w") or 0)
    h = float(region_spec.get("h") or 0)
    if w <= 0 or h <= 0:
        return

    top_extension = max(10.0, min(16.0, h * 0.07))
    bottom_extension = max(34.0, min(56.0, h * 0.2))
    bg_top = y + h + top_extension
    bg_bottom = max(0.0, y - bottom_extension)

    pdf_canvas.setFillColorRGB(1, 1, 1)
    pdf_canvas.rect(x, bg_bottom, w, bg_top - bg_bottom, stroke=0, fill=1)
    pdf_canvas.setFillColor(black)

    # Repaint the top band to remove any residual original separators/vertical stubs,
    # then redraw a clean frame line at the intended alignment.
    pdf_canvas.setFillColorRGB(1, 1, 1)
    pdf_canvas.rect(x, bg_top - 4, w, 8, stroke=0, fill=1)
    pdf_canvas.setFillColor(black)
    pdf_canvas.setLineWidth(0.6)
    pdf_canvas.line(x, bg_top, x + w, bg_top)

    pdf_canvas.setLineWidth(0.6)
    split_x = x + (w * 0.33)
    pdf_canvas.line(split_x, bg_bottom, split_x, bg_top)

    title_y = bg_top - 22
    header_line_y = bg_top - 32
    content_top = header_line_y - 16
    amount_line_y = y + 34

    pdf_canvas.setDash(3, 2)
    pdf_canvas.line(x + 2, header_line_y, x + w - 2, header_line_y)
    pdf_canvas.setDash()
    pdf_canvas.setLineWidth(0.3)
    pdf_canvas.line(x + 4, amount_line_y, split_x - 4, amount_line_y)
    pdf_canvas.line(split_x + 4, amount_line_y, x + w - 4, amount_line_y)

    pdf_canvas.setFont("Helvetica-Bold", 10)
    pdf_canvas.drawString(x + 10, title_y, "Empfangsschein")
    pdf_canvas.drawString(split_x + 10, title_y, "Zahlteil")

    qr_size = min(max(w * 0.22, 80), 120)
    qr_x = split_x + 14
    qr_y = max(y + 90, content_top - qr_size)
    pdf_canvas.drawImage(ImageReader(io.BytesIO(qr_png_bytes)), qr_x, qr_y, width=qr_size, height=qr_size, mask="auto")

    creditor_lines = [
        str(qr_data.get("qr_iban") or qr_data.get("iban") or "").strip(),
        str(qr_data.get("creditor_name") or "").strip(),
        str(qr_data.get("creditor_street") or "").strip(),
        " ".join(part for part in [str(qr_data.get("creditor_postal_code") or "").strip(), str(qr_data.get("creditor_city") or "").strip()] if part),
    ]
    debtor_lines = [
        str(qr_data.get("debtor_name") or "").strip(),
        str(qr_data.get("debtor_street") or "").strip(),
        " ".join(part for part in [str(qr_data.get("debtor_postal_code") or "").strip(), str(qr_data.get("debtor_city") or "").strip()] if part),
    ]
    left_debtor_compact = [item for item in [
        debtor_lines[0] if len(debtor_lines) > 0 else "",
        " ".join(part for part in [debtor_lines[1] if len(debtor_lines) > 1 else "", debtor_lines[2] if len(debtor_lines) > 2 else ""] if part),
    ] if item]
    reference_visual = _format_swiss_reference_visual(qr_data.get("reference") or qr_data.get("qr_reference"))
    amount_text = str(qr_data.get("amount") or "").strip()
    currency_text = str(qr_data.get("currency") or "CHF").strip() or "CHF"
    additional_information = str(qr_data.get("additional_information") or qr_data.get("additional_info") or "").strip()

    def _draw_lines(start_x: float, start_y: float, title: str, lines: list[str], title_size: int = 9, max_lines: int = 4) -> float:
        current_y = start_y
        pdf_canvas.setFont("Helvetica-Bold", title_size)
        pdf_canvas.drawString(start_x, current_y, title)
        current_y -= title_size + 4
        pdf_canvas.setFont("Helvetica", 9)
        for line in [item for item in lines if item][: max_lines]:
            pdf_canvas.drawString(start_x, current_y, line[:80])
            current_y -= 11
        return current_y

    left_col_x = x + 10
    right_col_x = split_x + qr_size + 22
    left_top_y = content_top
    right_top_y = content_top

    _draw_lines(left_col_x, left_top_y, "Konto / Zahlbar an", creditor_lines, max_lines=4)
    _draw_lines(left_col_x, y + 76, "Zahlbar durch", left_debtor_compact or debtor_lines, max_lines=2)
    left_reference_title_y = amount_line_y + 12
    left_reference_value_y = amount_line_y + 2
    pdf_canvas.setFont("Helvetica-Bold", 8)
    pdf_canvas.drawString(left_col_x, left_reference_title_y, "Referenz")
    pdf_canvas.setFont("Helvetica", 9)
    pdf_canvas.drawString(left_col_x, left_reference_value_y, reference_visual)

    _draw_lines(right_col_x, right_top_y, "Konto / Zahlbar an", creditor_lines, max_lines=4)
    ref_y = _draw_lines(right_col_x, max(qr_y - 18, y + 128), "Referenz", [reference_visual], title_size=8, max_lines=2)
    _draw_lines(right_col_x, max(ref_y - 6, y + 76), "Zahlbar durch", debtor_lines, max_lines=3)

    if additional_information:
        info_lines = [line.strip() for line in additional_information.splitlines() if line.strip()]
        if info_lines:
            _draw_lines(right_col_x, y + 56, "Zusatzliche Informationen", info_lines[:2], title_size=8, max_lines=2)

    left_currency_x = x + 10
    left_amount_x = x + 86
    right_currency_x = split_x + 10
    right_amount_x = split_x + 86

    pdf_canvas.setFont("Helvetica-Bold", 9)
    pdf_canvas.drawString(left_currency_x, y + 24, "Währung")
    pdf_canvas.drawString(left_amount_x, y + 24, "Betrag")
    pdf_canvas.drawString(right_currency_x, y + 24, "Währung")
    pdf_canvas.drawString(right_amount_x, y + 24, "Betrag")
    pdf_canvas.setFont("Helvetica", 11)
    pdf_canvas.drawString(left_currency_x, y + 10, currency_text)
    pdf_canvas.drawString(left_amount_x, y + 10, amount_text)
    pdf_canvas.drawString(right_currency_x, y + 10, currency_text)
    pdf_canvas.drawString(right_amount_x, y + 10, amount_text)
    pdf_canvas.setFont("Helvetica", 8)
    left_center_x = x + ((split_x - x) / 2.0)
    pdf_canvas.drawCentredString(left_center_x, y + 2, "Annahmestelle")


def _resolve_overlay_font_name(font_hint: str, fallback: str = "Helvetica") -> str:
    hint = str(font_hint or "").strip().lower()
    if not hint:
        return fallback
    if "oblique" in hint or "italic" in hint:
        return "Helvetica-Oblique"
    return "Helvetica"


def _format_value_like_sample(value: Any, sample_text: str) -> str:
    raw = _stringify(value)
    sample = str(sample_text or "").strip()
    if not raw or not sample:
        return raw

    if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", sample):
        text = str(raw).strip()
        try:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                parsed = datetime.strptime(text, "%Y-%m-%d")
                return parsed.strftime("%d.%m.%Y")
            if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", text):
                return text
        except Exception:
            return text

    return raw


def _collect_pdf_redaction_regions(
    layout: dict[str, Any],
    qr_bill_spec: dict[str, Any] | None = None,
    include_qr_bill_region: bool = False,
) -> list[dict[str, Any]]:
    regions: list[dict[str, Any]] = []
    for spec in (layout or {}).values():
        if not isinstance(spec, dict):
            continue
        erase_box = spec.get("erase_box") if isinstance(spec.get("erase_box"), dict) else None
        if not erase_box:
            continue
        regions.append(
            {
                "page": int(spec.get("page") or 1),
                "x": float(erase_box.get("x") or 0),
                "y": float(erase_box.get("y") or 0),
                "w": float(erase_box.get("w") or 0),
                "h": float(erase_box.get("h") or 0),
            }
        )
    if include_qr_bill_region and isinstance(qr_bill_spec, dict):
        regions.append(
            {
                "page": int(qr_bill_spec.get("page") or 1),
                "x": float(qr_bill_spec.get("x") or 0),
                "y": float(qr_bill_spec.get("y") or 0),
                "w": float(qr_bill_spec.get("w") or 0),
                "h": float(qr_bill_spec.get("h") or 0),
            }
        )
    return [region for region in regions if region["w"] > 0 and region["h"] > 0]


def _redact_pdf_template_bytes(template_bytes: bytes, regions: list[dict[str, Any]]) -> bytes:
    if fitz is None or not regions:
        return template_bytes

    document = fitz.open(stream=template_bytes, filetype="pdf")
    try:
        pending_pages: set[int] = set()
        for region in regions:
            page_index = int(region.get("page") or 1) - 1
            if page_index < 0 or page_index >= len(document):
                continue
            page = document[page_index]
            page_height = float(page.rect.height)
            x = float(region.get("x") or 0.0)
            y = float(region.get("y") or 0.0)
            w = float(region.get("w") or 0.0)
            h = float(region.get("h") or 0.0)
            rect = fitz.Rect(x, page_height - (y + h), x + w, page_height - y)
            page.add_redact_annot(rect, fill=(1, 1, 1))
            pending_pages.add(page_index)

        for page_index in sorted(pending_pages):
            document[page_index].apply_redactions()

        return document.tobytes(garbage=3, deflate=True)
    finally:
        document.close()


def _render_docx_template_bytes(template_bytes: bytes, context: dict[str, Any], qr_data: dict[str, Any] | None = None) -> bytes:
    if DocxTemplate is None:
        raise RuntimeError("docxtpl is not installed; run install_document_generation.ps1 first")

    with tempfile.TemporaryDirectory() as temp_dir:
        template_path = Path(temp_dir) / "template.docx"
        template_path.write_bytes(template_bytes)
        template = DocxTemplate(str(template_path))
        render_context = dict(context)
        if qr_data:
            payload_text = build_swiss_qr_bill_payload(qr_data)
            render_context["qr_bill_payload"] = payload_text
            render_context["qr_bill_text"] = render_context.get("qr_bill_text") or payload_text
            qr_png = _render_qr_png_bytes(payload_text)
            qr_path = Path(temp_dir) / "qr.png"
            qr_path.write_bytes(qr_png)
            if InlineImage is not None and Mm is not None:
                render_context["qr_code_image"] = InlineImage(template, str(qr_path), width=Mm(45))
                render_context["qr_code"] = render_context["qr_code_image"]
                render_context["qr_bill_image"] = render_context["qr_code_image"]
        template.render(render_context, autoescape=True)
        output = io.BytesIO()
        template.save(output)
        return output.getvalue()


def _convert_docx_to_pdf_bytes(docx_bytes: bytes) -> bytes:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        docx_path = temp_dir_path / "rendered.docx"
        pdf_path = temp_dir_path / "rendered.pdf"
        docx_path.write_bytes(docx_bytes)

        if win32com is not None:
            word = None
            document = None
            try:
                word = win32com.Dispatch("Word.Application")
                word.Visible = False
                document = word.Documents.Open(str(docx_path), ReadOnly=1)
                document.ExportAsFixedFormat(str(pdf_path), 17)
            finally:
                if document is not None:
                    document.Close(False)
                if word is not None:
                    word.Quit()
            if pdf_path.exists():
                return pdf_path.read_bytes()

        soffice = shutil.which("soffice") or shutil.which("libreoffice")
        if soffice:
            subprocess.run(
                [soffice, "--headless", "--convert-to", "pdf", "--outdir", str(temp_dir_path), str(docx_path)],
                check=True,
                capture_output=True,
            )
            if pdf_path.exists():
                return pdf_path.read_bytes()

        raise RuntimeError(
            "Unable to convert DOCX to PDF. Install Microsoft Word, LibreOffice, or provide a PDF template layout."
        )


def _draw_overlay_page(
    page_width: float,
    page_height: float,
    context: dict[str, Any],
    layout: dict[str, Any],
    qr_png_bytes: bytes | None = None,
    qr_bill_spec: dict[str, Any] | None = None,
    qr_data: dict[str, Any] | None = None,
) -> bytes:
    if canvas is None:
        raise RuntimeError("reportlab is not installed; run install_document_generation.ps1 first")
    buffer = io.BytesIO()
    pdf_canvas = canvas.Canvas(buffer, pagesize=(page_width, page_height))
    for field_name, spec in layout.items():
        if not isinstance(spec, dict):
            continue
        page = int(spec.get("page") or 1)
        if page != 1:
            continue
        x = float(spec.get("x") or 0)
        y = float(spec.get("y") or 0)
        size = float(spec.get("font_size") or 10)
        font_name = _resolve_overlay_font_name(spec.get("font_name") or "", fallback="Helvetica")
        sample_text = str(spec.get("sample_text") or "")
        width = float(spec.get("width") or 0)
        value = context.get(field_name)
        if value in (None, "", [], {}):
            continue
        erase_box = spec.get("erase_box") if isinstance(spec.get("erase_box"), dict) else None
        draw_erase_box = bool(spec.get("draw_erase_box", True))
        if erase_box and draw_erase_box:
            erase_x = float(erase_box.get("x") or x)
            erase_y = float(erase_box.get("y") or y)
            erase_w = float(erase_box.get("w") or 0)
            erase_h = float(erase_box.get("h") or max(size + 4, 12))
            if erase_w > 0 and erase_h > 0:
                pdf_canvas.setFillColorRGB(1, 1, 1)
                pdf_canvas.rect(erase_x, erase_y, erase_w, erase_h, stroke=0, fill=1)
        text_lines = _flatten_lines(value)
        if sample_text and len(text_lines) <= 1:
            formatted = _format_value_like_sample(value, sample_text)
            text_lines = _flatten_lines(formatted)
        pdf_canvas.setFillColor(black)
        if isinstance(spec.get("erase_box"), dict):
            size = max(size * 0.86, 6.0)
        pdf_canvas.setFont(font_name, size)
        if not text_lines:
            pdf_canvas.drawString(x, y, _stringify(value))
            continue
        for line_index, line in enumerate(text_lines):
            if width:
                pdf_canvas.drawString(x, y - (line_index * (size + 2)), line[: max(1, int(width / max(size * 0.5, 1)))])
            else:
                pdf_canvas.drawString(x, y - (line_index * (size + 2)), line)

    qr_spec = layout.get("qr_code_image") if isinstance(layout.get("qr_code_image"), dict) else None
    if qr_spec and qr_png_bytes:
        x = float(qr_spec.get("x") or 0)
        y = float(qr_spec.get("y") or 0)
        w = float(qr_spec.get("w") or 150)
        h = float(qr_spec.get("h") or 150)
        pdf_canvas.drawImage(ImageReader(io.BytesIO(qr_png_bytes)), x, y, width=w, height=h, mask="auto")

    if qr_bill_spec and qr_png_bytes and isinstance(qr_data, dict):
        _draw_swiss_qr_bill_region(pdf_canvas, qr_bill_spec, qr_data, qr_png_bytes)

    pdf_canvas.showPage()
    pdf_canvas.save()
    return buffer.getvalue()


def _render_pdf_summary_fallback(context: dict[str, Any], document_kind: str, qr_png_bytes: bytes | None = None) -> bytes:
    if canvas is None:
        raise RuntimeError("reportlab is not installed; run install_document_generation.ps1 first")
    from reportlab.lib.pagesizes import A4

    buffer = io.BytesIO()
    pdf_canvas = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 50
    pdf_canvas.setFont("Helvetica-Bold", 18)
    pdf_canvas.drawString(40, y, _stringify(context.get("document_title") or document_kind.title()))
    y -= 28
    pdf_canvas.setFont("Helvetica", 10)
    for key in ["document_number", "issue_date", "recipient_name", "recipient_address", "issuer_name", "issuer_address", "currency", "subtotal", "tax_total", "grand_total"]:
        value = _stringify(context.get(key))
        if value:
            for line in value.splitlines():
                pdf_canvas.drawString(40, y, f"{key}: {line}")
                y -= 14
    summary_lines = _flatten_lines(context.get("summary_block"))
    if summary_lines:
        pdf_canvas.setFont("Helvetica-Bold", 11)
        pdf_canvas.drawString(40, y - 4, "Summary")
        y -= 18
        pdf_canvas.setFont("Helvetica", 10)
        for line in summary_lines:
            pdf_canvas.drawString(40, y, line)
            y -= 12
    if qr_png_bytes:
        pdf_canvas.drawImage(ImageReader(io.BytesIO(qr_png_bytes)), width - 210, 60, width=160, height=160, mask="auto")
    pdf_canvas.showPage()
    pdf_canvas.save()
    return buffer.getvalue()


def _overlay_pdf_template_bytes(
    template_bytes: bytes,
    context: dict[str, Any],
    layout: dict[str, Any],
    qr_png_bytes: bytes | None = None,
    qr_bill_spec: dict[str, Any] | None = None,
    qr_data: dict[str, Any] | None = None,
) -> bytes:
    if pypdf is None:
        raise RuntimeError("pypdf is not installed; run install_document_generation.ps1 first")
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(io.BytesIO(template_bytes))
    writer = PdfWriter()
    page_count = len(reader.pages)
    layout_by_page: dict[int, dict[str, Any]] = {}
    for field_name, spec in layout.items():
        if not isinstance(spec, dict):
            continue
        page = int(spec.get("page") or 1)
        layout_by_page.setdefault(page, {})[field_name] = spec

    for page_index, page in enumerate(reader.pages, start=1):
        media_box = page.mediabox
        page_width = float(media_box.width)
        page_height = float(media_box.height)
        overlay_layout = layout_by_page.get(page_index, {})
        page_qr_bill_spec = qr_bill_spec if page_index == int((qr_bill_spec or {}).get("page") or 1) else None
        if overlay_layout or (page_index == 1 and qr_png_bytes) or page_qr_bill_spec:
            overlay_pdf = _draw_overlay_page(
                page_width,
                page_height,
                context,
                overlay_layout,
                qr_png_bytes if page_index == 1 else None,
                page_qr_bill_spec,
                qr_data,
            )
            overlay_reader = PdfReader(io.BytesIO(overlay_pdf))
            page.merge_page(overlay_reader.pages[0])
        writer.add_page(page)

    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def render_template_document(
    *,
    template_bytes: bytes,
    template_format: str,
    output_format: str,
    document_kind: str,
    payload: dict[str, Any],
    qr_data: dict[str, Any] | None = None,
    layout: dict[str, Any] | None = None,
    render_mode: str = "overlay",
    template_name: str = "template",
) -> tuple[bytes, str, str]:
    template_format_name = normalize_template_format(template_format)
    output_format_name = normalize_output_format(output_format)
    normalized_render_mode = str(render_mode or "overlay").strip().lower() or "overlay"
    context = build_template_context(document_kind, payload)
    qr_png_bytes = _render_qr_png_bytes(build_swiss_qr_bill_payload(qr_data)) if qr_data else None
    filename_stem = _resolve_output_filename_stem(template_name=template_name, payload=payload, context=context)
    filename = f"{filename_stem}.{output_format_name}"

    if template_format_name == "docx":
        rendered_docx = _render_docx_template_bytes(template_bytes, context, qr_data=qr_data)
        if output_format_name == "docx":
            return rendered_docx, filename, "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        try:
            pdf_bytes = _convert_docx_to_pdf_bytes(rendered_docx)
            return pdf_bytes, filename, "application/pdf"
        except Exception:
            return _render_pdf_summary_fallback(context, document_kind, qr_png_bytes), filename, "application/pdf"

    if output_format_name != "pdf":
        raise ValueError("PDF templates can only be rendered to PDF")

    explicit_layout = dict(layout) if isinstance(layout, dict) else {}
    qr_bill_spec = explicit_layout.get("qr_bill_region") if isinstance(explicit_layout.get("qr_bill_region"), dict) else None
    qr_bill_redraw_enabled = bool(explicit_layout.get("qr_bill_redraw") or explicit_layout.get("redraw_qr_bill"))
    if normalized_render_mode == "preserve_template":
        effective_layout = dict(explicit_layout)
        if isinstance(qr_data, dict) and qr_data:
            qr_image_spec = effective_layout.get("qr_code_image") if isinstance(effective_layout.get("qr_code_image"), dict) else None
            if qr_image_spec is None:
                inferred_qr_image_spec = _infer_qr_code_image_spec_from_qr_bill_region(template_bytes, qr_bill_spec)
                if inferred_qr_image_spec:
                    effective_layout["qr_code_image"] = inferred_qr_image_spec
        qr_overwrite_layout, qr_overwrite_values = infer_pdf_qr_bill_overwrite_layout(
            template_bytes=template_bytes,
            qr_data=qr_data if isinstance(qr_data, dict) else {},
            template_payload=payload,
            qr_bill_spec=qr_bill_spec,
        )
        if qr_overwrite_values:
            context.update(qr_overwrite_values)
        if qr_overwrite_layout:
            merged_layout = dict(qr_overwrite_layout)
            merged_layout.update(effective_layout)
            effective_layout = merged_layout
        # In preserve-template mode, default to one-to-one template copy and
        # only overwrite explicit fields in place. Full QR bill redraw is opt-in.
        keep_qr_bytes = isinstance(effective_layout.get("qr_code_image"), dict) or qr_bill_redraw_enabled
        qr_overlay_bytes = qr_png_bytes if keep_qr_bytes else None
    else:
        effective_layout = dict(DEFAULT_PDF_LAYOUTS.get(str(document_kind or "").strip().lower(), {}))
        if explicit_layout:
            effective_layout.update(explicit_layout)
        qr_overlay_bytes = qr_png_bytes

    render_template_bytes = template_bytes
    if normalized_render_mode == "preserve_template":
        redact_regions = _collect_pdf_redaction_regions(
            effective_layout,
            qr_bill_spec,
            include_qr_bill_region=qr_bill_redraw_enabled,
        )
        render_template_bytes = _redact_pdf_template_bytes(template_bytes, redact_regions)

    pdf_bytes = _overlay_pdf_template_bytes(
        render_template_bytes,
        context,
        effective_layout,
        qr_overlay_bytes,
        qr_bill_spec if qr_bill_redraw_enabled else None,
        qr_data,
    )
    return pdf_bytes, filename, "application/pdf"
