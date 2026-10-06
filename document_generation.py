from __future__ import annotations

import csv
import io
import json
import textwrap
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape as xml_escape

import psycopg2
from psycopg2.extras import RealDictCursor


CSV_MEDIA_TYPE = "text/csv; charset=utf-8"
DOCX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
PDF_MEDIA_TYPE = "application/pdf"

EXPORT_MEDIA_TYPES = {
    "csv": CSV_MEDIA_TYPE,
    "docx": DOCX_MEDIA_TYPE,
    "pdf": PDF_MEDIA_TYPE,
}


def normalize_export_format(export_format: str) -> str:
    format_name = str(export_format or "").strip().lower()
    if format_name == "excel":
        format_name = "csv"
    if format_name not in EXPORT_MEDIA_TYPES:
        raise ValueError("export_format must be csv, docx, or pdf")
    return format_name


def _safe_filename(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "document-export"
    cleaned = []
    for character in text:
        if character.isalnum() or character in {"-", "_", "."}:
            cleaned.append(character)
        else:
            cleaned.append("-")
    name = "".join(cleaned).strip("-_.")
    return name or "document-export"


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def load_document_export_payload(
    connection: psycopg2.extensions.connection,
    doc_id: int,
) -> dict[str, Any]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT doc_id, doc_key, doc_path, doc_cat, doc_type, doc_date, doc_desc, doc_theme,
                   keyword_text, identifiers_kv, metadata, status, valid_from, valid_until, entry_date
            FROM document
            WHERE doc_id = %s
            """,
            (doc_id,),
        )
        document = cursor.fetchone()
        if document is None:
            raise KeyError(f"document {doc_id} not found")

        cursor.execute(
            """
            SELECT
                p.part_id,
                p.part_key,
                p.metadata,
                p.entry_date,
                c.class_name AS part_class_name,
                o.object_name,
                o.canonical_full_name,
                o.class_name AS object_class_name,
                r.relationship_name,
                r.relationship_cat,
                r.src_object_id,
                r.tar_object_id
            FROM part p
            LEFT JOIN object_class c ON c.class_id = p.class_id
            LEFT JOIN object_instance o ON o.object_id = p.object_id
            LEFT JOIN object_relationship r ON r.relationship_id = p.relationship_id
            WHERE p.doc_id = %s
            ORDER BY p.part_key, p.part_id
            """,
            (doc_id,),
        )
        parts = cursor.fetchall()

    return {
        "document": dict(document),
        "parts": [dict(part) for part in parts],
    }


def _document_title(document: dict[str, Any]) -> str:
    return str(document.get("doc_key") or document.get("doc_path") or f"document-{document.get('doc_id') or 'export'}").strip()


def _document_file_stem(document: dict[str, Any]) -> str:
    return _safe_filename(_document_title(document))


def _document_lines(document: dict[str, Any], parts: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    lines.append(f"Document Export: {_document_title(document)}")
    lines.append("")
    lines.append("Document")
    document_fields = [
        ("doc_id", document.get("doc_id")),
        ("doc_key", document.get("doc_key")),
        ("doc_path", document.get("doc_path")),
        ("doc_cat", document.get("doc_cat")),
        ("doc_type", document.get("doc_type")),
        ("doc_date", document.get("doc_date")),
        ("doc_desc", document.get("doc_desc")),
        ("doc_theme", document.get("doc_theme")),
        ("keyword_text", document.get("keyword_text")),
        ("status", document.get("status")),
        ("valid_from", document.get("valid_from")),
        ("valid_until", document.get("valid_until")),
        ("entry_date", document.get("entry_date")),
    ]
    for field_name, value in document_fields:
        text = _json_text(value)
        if text != "":
            lines.append(f"- {field_name}: {text}")

    identifiers = _json_text(document.get("identifiers_kv") or {})
    if identifiers:
        lines.append("- identifiers_kv:")
        lines.extend(f"  {line}" for line in identifiers.splitlines())

    metadata = _json_text(document.get("metadata") or {})
    if metadata:
        lines.append("- metadata:")
        lines.extend(f"  {line}" for line in metadata.splitlines())

    lines.append("")
    lines.append("Parts")
    if not parts:
        lines.append("- none")
        return lines

    for index, part in enumerate(parts, start=1):
        lines.append(
            f"{index}. part_key={_json_text(part.get('part_key'))} class={_json_text(part.get('part_class_name'))} "
            f"object={_json_text(part.get('object_name'))} relationship={_json_text(part.get('relationship_name'))}"
        )
        if part.get("canonical_full_name"):
            lines.append(f"   canonical_full_name: {_json_text(part.get('canonical_full_name'))}")
        if part.get("relationship_cat"):
            lines.append(f"   relationship_cat: {_json_text(part.get('relationship_cat'))}")
        part_metadata = _json_text(part.get("metadata") or {})
        if part_metadata:
            lines.append("   metadata:")
            lines.extend(f"     {line}" for line in part_metadata.splitlines())

    return lines


def render_document_export_csv(document: dict[str, Any], parts: list[dict[str, Any]]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "section",
        "item_index",
        "field",
        "value",
        "part_key",
        "part_class",
        "object_name",
        "canonical_full_name",
        "relationship_name",
        "relationship_cat",
        "metadata_json",
    ])

    document_fields = [
        ("doc_id", document.get("doc_id")),
        ("doc_key", document.get("doc_key")),
        ("doc_path", document.get("doc_path")),
        ("doc_cat", document.get("doc_cat")),
        ("doc_type", document.get("doc_type")),
        ("doc_date", document.get("doc_date")),
        ("doc_desc", document.get("doc_desc")),
        ("doc_theme", document.get("doc_theme")),
        ("keyword_text", document.get("keyword_text")),
        ("status", document.get("status")),
        ("valid_from", document.get("valid_from")),
        ("valid_until", document.get("valid_until")),
        ("entry_date", document.get("entry_date")),
        ("identifiers_kv", document.get("identifiers_kv") or {}),
        ("metadata", document.get("metadata") or {}),
    ]
    for index, (field_name, value) in enumerate(document_fields, start=1):
        writer.writerow(["document", index, field_name, _json_text(value), "", "", "", "", "", "", ""])

    for index, part in enumerate(parts, start=1):
        writer.writerow([
            "part",
            index,
            "",
            "",
            _json_text(part.get("part_key")),
            _json_text(part.get("part_class_name")),
            _json_text(part.get("object_name")),
            _json_text(part.get("canonical_full_name")),
            _json_text(part.get("relationship_name")),
            _json_text(part.get("relationship_cat")),
            _json_text(part.get("metadata") or {}),
        ])

    return output.getvalue().encode("utf-8-sig")


def _sanitize_pdf_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _wrap_pdf_lines(lines: list[str], width: int = 92) -> list[str]:
    wrapped: list[str] = []
    for line in lines:
        clean = _sanitize_pdf_text(line)
        if not clean:
            wrapped.append("")
            continue
        chunks = textwrap.wrap(clean, width=width, replace_whitespace=False, drop_whitespace=False)
        wrapped.extend(chunks or [""])
    return wrapped


def _build_simple_pdf(lines: list[str], title: str) -> bytes:
    page_width = 612
    page_height = 792
    left_margin = 48
    top_margin = 742
    font_size = 10
    line_height = 13
    lines_per_page = max(1, int((top_margin - 48) / line_height))
    pages = [lines[index:index + lines_per_page] for index in range(0, len(lines), lines_per_page)] or [[]]

    objects: list[bytes] = []
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    page_refs = [f"{index} 0 R" for index in range(3, 3 + len(pages))]
    objects.append(f"<< /Type /Pages /Kids [{' '.join(page_refs)}] /Count {len(pages)} >>".encode("ascii"))

    font_obj_num = 3 + len(pages)
    for page_number, page_lines in enumerate(pages, start=3):
        content_obj_num = page_number + len(pages)
        stream_lines = ["BT", f"/F1 {font_size} Tf", f"1 0 0 1 {left_margin} {top_margin} Tm"]
        for line_index, line in enumerate(page_lines):
            if line_index > 0:
                stream_lines.append("T*")
            stream_lines.append(f"({_sanitize_pdf_text(line)}) Tj")
        stream_lines.append("ET")
        stream_bytes = "\n".join(stream_lines).encode("latin-1", "ignore")
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_width} {page_height}] "
                f"/Resources << /Font << /F1 {font_obj_num} 0 R >> >> /Contents {content_obj_num} 0 R >>"
            ).encode("ascii")
        )
        objects.append(f"<< /Length {len(stream_bytes)} >>\nstream\n".encode("ascii") + stream_bytes + b"\nendstream")

    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    output = io.BytesIO()
    output.write(b"%PDF-1.4\n")
    offsets = [0]
    for index, body in enumerate(objects, start=1):
        offsets.append(output.tell())
        output.write(f"{index} 0 obj\n".encode("ascii"))
        output.write(body)
        output.write(b"\nendobj\n")

    xref_position = output.tell()
    output.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    output.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.write(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.write(
        (
            "trailer\n"
            f"<< /Size {len(objects) + 1} /Root 1 0 R /Info << /Title ({_sanitize_pdf_text(title)}) >> >>\n"
            f"startxref\n{xref_position}\n%%EOF\n"
        ).encode("ascii")
    )
    return output.getvalue()


def _build_simple_docx(lines: list[str], title: str) -> bytes:
    def paragraph_xml(text: str) -> str:
        return (
            "<w:p><w:r><w:t xml:space=\"preserve\">"
            f"{xml_escape(text)}"
            "</w:t></w:r></w:p>"
        )

    body = "".join(paragraph_xml(line) if line else "<w:p/>" for line in lines)
    document_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<w:document xmlns:w=\"http://schemas.openxmlformats.org/wordprocessingml/2006/main\">"
        f"<w:body>{body}<w:sectPr><w:pgSz w:w=\"12240\" w:h=\"15840\"/></w:sectPr></w:body>"
        "</w:document>"
    )
    core_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" "
        "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" "
        "xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
        f"<dc:title>{xml_escape(title)}</dc:title>"
        f"<dc:creator>IDMS</dc:creator>"
        f"<cp:lastModifiedBy>IDMS</cp:lastModifiedBy>"
        f"<dcterms:created xsi:type=\"dcterms:W3CDTF\">{datetime.now(timezone.utc).isoformat()}</dcterms:created>"
        f"<dcterms:modified xsi:type=\"dcterms:W3CDTF\">{datetime.now(timezone.utc).isoformat()}</dcterms:modified>"
        "</cp:coreProperties>"
    )
    app_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" "
        "xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">"
        "<Application>IDMS</Application></Properties>"
    )
    content_types = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
        "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
        "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
        "<Override PartName=\"/word/document.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml\"/>"
        "<Override PartName=\"/docProps/core.xml\" ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>"
        "<Override PartName=\"/docProps/app.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>"
        "</Types>"
    )
    rels = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"word/document.xml\"/>"
        "</Relationships>"
    )

    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", rels)
        archive.writestr("docProps/core.xml", core_xml)
        archive.writestr("docProps/app.xml", app_xml)
        archive.writestr("word/document.xml", document_xml)
    return output.getvalue()


def render_document_export(
    document: dict[str, Any],
    parts: list[dict[str, Any]],
    export_format: str,
) -> tuple[bytes, str, str]:
    format_name = normalize_export_format(export_format)
    title = _document_title(document)
    filename = f"{_document_file_stem(document)}.{format_name}"
    if format_name == "csv":
        return render_document_export_csv(document, parts), filename, CSV_MEDIA_TYPE

    lines = _wrap_pdf_lines(_document_lines(document, parts))
    if format_name == "docx":
        return _build_simple_docx(lines, title), filename, DOCX_MEDIA_TYPE
    return _build_simple_pdf(lines, title), filename, PDF_MEDIA_TYPE