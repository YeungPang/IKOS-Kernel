import base64
import io
import logging
import multiprocessing
import os
import time
import requests
from pathlib import Path
from dotenv import load_dotenv
from pypdf import PdfReader

import nest_asyncio

from llama_cloud_services import LlamaParse
from llama_index.core import SimpleDirectoryReader

nest_asyncio.apply()
try:
    from PIL import Image
except Exception:  # pragma: no cover - optional dependency
    Image = None

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

LOGGER = logging.getLogger("idms.md_gen")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_URL = os.getenv("IDMS_OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/") + "/chat/completions"
OPENROUTER_VISION_MODEL = os.getenv("IDMS_OPENROUTER_MD_VISION_MODEL", "openai/gpt-4o")
OPENROUTER_TEXT_MODEL = os.getenv("IDMS_OPENROUTER_MD_TEXT_MODEL", "openai/gpt-4o-mini")
INGESTION_MARKDOWN_RULE = os.getenv("IDMS_INGESTION_MARKDOWN_RULE", "force_pparser").strip().lower()

LLAMA_CLOUD_API_KEY = os.getenv("LLAMA_CLOUD_API_KEY", "")

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff"}
OFFICE_TEXT_EXTENSIONS = {".docx", ".xlsx", ".xlsm"}
PDF_EXTENSION = ".pdf"

_MD_SYSTEM_PROMPT = (
    "You are a document conversion engine. "
    "Convert the provided document into clean, structured Markdown. "
    "Preserve all text content, headings, tables, lists, numbers, dates, and identifiers faithfully. "
    "Do not summarise or omit content. Do not hallucinate missing content. "
    "Output only the Markdown text with no additional commentary."
)

mparser = LlamaParse(
    result_type="markdown"  # "markdown" and "text" are available  
)
pparser = LlamaParse(
    result_type="markdown",  # "markdown" and "text" are available
    premium_mode=True
)

LLAMAPARSE_TIMEOUT_SECONDS = int(str(os.getenv("IDMS_LLAMAPARSE_TIMEOUT_SECONDS", "420")).strip() or "420")


def _is_truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _should_force_pparser(parser_rule_override: str | None = None) -> bool:
    """Ingestion rule: force premium parser for markdown generation.

    This is enabled by default because complex tables are parsed more reliably
    through the premium parser path.
    """
    effective_rule = str(parser_rule_override or INGESTION_MARKDOWN_RULE or "").strip().lower()
    if effective_rule in {"force_pparser", "pparser_only", "complex_tables"}:
        return True
    if effective_rule in {"mparser", "standard", "default"}:
        return False
    return _is_truthy(os.getenv("IDMS_FORCE_PPARSER", ""))


def _select_markdown_parser(parser_rule_override: str | None = None) -> tuple[LlamaParse, str]:
    """Return parser and label according to ingestion markdown rule."""
    if _should_force_pparser(parser_rule_override=parser_rule_override):
        return pparser, "pparser"
    return mparser, "mparser"


def _load_data_with_retries(
    parser: LlamaParse,
    path: str,
    parser_name: str,
    extra_retries: int = 3,
) -> list[str]:
    """Call parser.load_data with retries.

    extra_retries=3 means up to 4 total attempts.
    """
    attempts = 1 + max(0, int(extra_retries))
    last_exc: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return _load_data_with_timeout(path=path, parser_name=parser_name, timeout_seconds=LLAMAPARSE_TIMEOUT_SECONDS)
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            # Small linear backoff to handle transient LlamaParse/API instability.
            sleep_seconds = attempt
            LOGGER.warning(
                "md_gen %s failed attempt=%d/%d path=%s error=%s; retrying in %ss",
                parser_name,
                attempt,
                attempts,
                path,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"md_gen {parser_name} failed after {attempts} attempts for path={path}: {last_exc}"
    ) from last_exc


def _parser_load_texts_worker(path: str, parser_name: str, result_queue: multiprocessing.Queue) -> None:
    """Worker process for LlamaParse to allow hard timeout termination."""
    try:
        use_premium = str(parser_name or "").strip().lower() == "pparser"
        parser = LlamaParse(result_type="markdown", premium_mode=use_premium)
        docs = parser.load_data(path)
        texts = [str(getattr(doc, "text", "") or "") for doc in docs]
        result_queue.put({"ok": True, "texts": texts})
    except Exception as exc:  # pragma: no cover - worker isolation path
        result_queue.put({"ok": False, "error": str(exc)})


def _load_data_with_timeout(path: str, parser_name: str, timeout_seconds: int) -> list[str]:
    """Run LlamaParse in isolated process and terminate if timeout is exceeded."""
    safe_timeout = max(1, int(timeout_seconds or 0))
    ctx = multiprocessing.get_context("spawn")
    result_queue: multiprocessing.Queue = ctx.Queue()
    proc = ctx.Process(target=_parser_load_texts_worker, args=(path, parser_name, result_queue), daemon=True)
    proc.start()
    proc.join(timeout=safe_timeout)

    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=5)
        raise TimeoutError(
            f"md_gen {parser_name} timed out after {safe_timeout}s for path={path}; worker terminated"
        )

    if proc.exitcode not in (0, None) and result_queue.empty():
        raise RuntimeError(f"md_gen {parser_name} worker exited with code {proc.exitcode} for path={path}")

    if result_queue.empty():
        return []

    payload = result_queue.get()
    if not isinstance(payload, dict):
        raise RuntimeError(f"md_gen {parser_name} worker returned unexpected payload type")
    if not payload.get("ok"):
        raise RuntimeError(f"md_gen {parser_name} worker failed for path={path}: {payload.get('error')}")
    return [str(item or "") for item in (payload.get("texts") or [])]

def is_pdf_image_based(path: str) -> bool:
    """Return True if the PDF is likely image-based (scanned) with no extractable text layer."""
    try:
        reader = PdfReader(path)
        text = ""
        for page in reader.pages[:3]:
            text += page.extract_text() or ""
        return len(text.strip()) == 0
    except Exception:
        return True


def _image_ext_to_mime(ext: str) -> str:
    return {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".bmp": "image/bmp",
        ".tif": "image/tiff",
        ".tiff": "image/tiff",
    }.get(ext, "image/jpeg")


def _extract_pdf_page_images(path: str) -> list[str]:
    """Extract embedded scan images from a PDF as data: URIs."""
    data_uris: list[str] = []
    try:
        reader = PdfReader(path)
        for page in reader.pages:
            try:
                for img in page.images:
                    raw = bytes(img.data) if img.data else b""
                    if not raw:
                        continue
                    mime = "image/jpeg" if raw[:2] == b"\xff\xd8" else (
                        "image/png" if raw[:8] == b"\x89PNG\r\n\x1a\n" else "image/jpeg"
                    )

                    # Keep OCR requests under API payload limits for large scans.
                    if Image is not None and len(raw) > 2_500_000:
                        try:
                            with Image.open(io.BytesIO(raw)) as img:
                                img = img.convert("RGB")
                                max_dim = 1800
                                w, h = img.size
                                scale = min(max_dim / float(max(w, 1)), max_dim / float(max(h, 1)), 1.0)
                                if scale < 1.0:
                                    img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))))
                                out = io.BytesIO()
                                img.save(out, format="JPEG", quality=75, optimize=True)
                                raw = out.getvalue()
                                mime = "image/jpeg"
                        except Exception:
                            LOGGER.debug("md_gen: image downscale failed; using original bytes", exc_info=True)

                    b64 = base64.b64encode(raw).decode("ascii")
                    data_uris.append(f"data:{mime};base64,{b64}")
            except Exception:
                continue
    except Exception:
        LOGGER.exception("Failed to extract page images from PDF %s", path)
    return data_uris


def _call_openrouter(payload: dict) -> str:
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY is not set")
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    response = requests.post(OPENROUTER_URL, json=payload, headers=headers, timeout=120)
    if not response.ok:
        body = response.text[:2000]
        raise RuntimeError(f"OpenRouter HTTP {response.status_code}: {body}")
    return response.json()["choices"][0]["message"]["content"]


def generate_markdown_from_image_file(path: str) -> str:
    text_chunks = _load_data_with_retries(pparser, path, "pparser", extra_retries=3)
    mdt = ""
    LOGGER.info("md_gen vision call premium parser path=%s", path)
    for chunk in text_chunks:
        mdt += chunk + "\n"
    return mdt.strip()
    # """Generate Markdown from a standalone image file using the vision model."""
    # ext = Path(path).suffix.lower()
    # mime = _image_ext_to_mime(ext)
    # with open(path, "rb") as f:
    #     b64 = base64.b64encode(f.read()).decode("ascii")
    # data_uri = f"data:{mime};base64,{b64}"
    # payload = {
    #     "model": OPENROUTER_VISION_MODEL,
    #     "messages": [
    #         {"role": "system", "content": _MD_SYSTEM_PROMPT},
    #         {"role": "user", "content": [
    #             {"type": "image_url", "image_url": {"url": data_uri}},
    #             {"type": "text", "text": "Convert this document image into Markdown."},
    #         ]},
    #     ],
    #     "temperature": 0,
    # }
    # LOGGER.info("md_gen vision call model=%s path=%s", OPENROUTER_VISION_MODEL, path)
    # return _call_openrouter(payload)


def generate_markdown_from_scanned_pdf(path: str) -> str:
    return generate_markdown_from_image_file(path)
    # """Generate Markdown from a scanned (image-only) PDF via the vision model."""
    # images = _extract_pdf_page_images(path)
    # if not images:
    #     LOGGER.warning("md_gen: no page images extractable from scanned PDF %s", path)
    #     return ""
    # LOGGER.info("md_gen scanned PDF vision call model=%s pages=%d path=%s", OPENROUTER_VISION_MODEL, len(images), path)

    # # Use one request per page to avoid oversized payloads and provider limits.
    # page_markdown: list[str] = []
    # for i, data_uri in enumerate(images, start=1):
    #     payload = {
    #         "model": OPENROUTER_VISION_MODEL,
    #         "messages": [
    #             {"role": "system", "content": _MD_SYSTEM_PROMPT},
    #             {
    #                 "role": "user",
    #                 "content": [
    #                     {"type": "text", "text": f"Convert page {i} of this scanned PDF into Markdown."},
    #                     {"type": "image_url", "image_url": {"url": data_uri}},
    #                 ],
    #             },
    #         ],
    #         "temperature": 0,
    #     }
    #     page_md = _call_openrouter(payload).strip()
    #     if page_md:
    #         page_markdown.append(f"<!-- page:{i} -->\n{page_md}")

    # return "\n\n".join(page_markdown)


def generate_markdown_from_text_pdf(path: str, text: str) -> str:
    return file_to_markdown_openrouter(path)
    # """Generate Markdown from a text-layer PDF using the text model."""
    # payload = {
    #     "model": OPENROUTER_TEXT_MODEL,
    #     "messages": [
    #         {"role": "system", "content": _MD_SYSTEM_PROMPT},
    #         {"role": "user", "content": f"Convert this document into Markdown.\n\nDocument text:\n{text}"},
    #     ],
    #     "temperature": 0,
    # }
    # LOGGER.info("md_gen text PDF call model=%s path=%s", OPENROUTER_TEXT_MODEL, path)
    # return _call_openrouter(payload)


def file_to_markdown_openrouter(path: str, parser_rule_override: str | None = None) -> str:
    parser, parser_name = _select_markdown_parser(parser_rule_override=parser_rule_override)
    effective_rule = str(parser_rule_override or INGESTION_MARKDOWN_RULE or "").strip().lower() or INGESTION_MARKDOWN_RULE
    text_chunks = _load_data_with_retries(parser, path, parser_name, extra_retries=3)
    mdt = ""
    LOGGER.info(
        "md_gen markdown generation parser=%s ingestion_rule=%s path=%s",
        parser_name,
        effective_rule,
        path,
    )
    for chunk in text_chunks:
        mdt += chunk + "\n"
    return mdt.strip()
    # """Convert a PDF, image, or Office document into Markdown.

    # Routing:
    # - Image file          → vision model (image_url data URI)
    # - Scanned PDF         → vision model (per-page image extraction)
    # - Text-layer PDF      → text model (extracted text in prompt)
    # - Office file (.docx) → handled upstream by extract_local_text; not needed here
    # """
    # if not OPENROUTER_API_KEY:
    #     raise RuntimeError("OPENROUTER_API_KEY is not set")

    # ext = Path(path).suffix.lower()

    # if ext in IMAGE_EXTENSIONS:
    #     return generate_markdown_from_image_file(path)

    # if ext == PDF_EXTENSION:
    #     if is_pdf_image_based(path):
    #         return generate_markdown_from_scanned_pdf(path)
    #     reader = PdfReader(path)
    #     text = "\n\n".join(page.extract_text() or "" for page in reader.pages).strip()
    #     return generate_markdown_from_text_pdf(path, text)

    # raise ValueError(f"file_to_markdown_openrouter: unsupported file type {ext}")


# Example usage:
if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "example.pdf"
    print(file_to_markdown_openrouter(target))
