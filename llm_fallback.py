from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable
from urllib import error as urlerror
from urllib import request as urlrequest

from ikos_config import (
	ENABLE_OPENROUTER_FALLBACK,
	OPENROUTER_API_KEY,
	OPENROUTER_BASE_URL,
	OPENROUTER_COMPLEX_MODEL,
	OPENROUTER_EMBEDDING_MODEL,
	OPENROUTER_MODEL,
	OPENROUTER_SIMPLE_MODEL,
	OPENROUTER_TIMEOUT_SECONDS,
)


LOGGER = logging.getLogger("idms.llm")


@dataclass
class TextResponse:
	text: str


@dataclass
class EmbeddingItem:
	values: list[float]


@dataclass
class EmbeddingResponse:
	embeddings: list[EmbeddingItem]


def is_quota_exhausted_error(exc: Exception) -> bool:
	text = str(exc or "").upper()
	return "RESOURCE_EXHAUSTED" in text or " 429 " in f" {text} "


def openrouter_generate_with_vision(
	*,
	model: str,
	prompt: str,
	images_data_uris: list[str],
	temperature: float = 0.0,
) -> str:
	"""Send a multimodal (text + images) request to an OpenRouter vision-capable model.

	Each entry in images_data_uris must be a data URI:
	    data:image/jpeg;base64,<base64-encoded-bytes>
	"""
	if not OPENROUTER_API_KEY:
		raise RuntimeError("OPENROUTER_API_KEY is not configured")

	content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
	for data_uri in images_data_uris:
		content.append({"type": "image_url", "image_url": {"url": data_uri}})

	payload = {
		"model": model,
		"messages": [{"role": "user", "content": content}],
		"temperature": float(temperature),
	}
	data = _openrouter_post("chat/completions", payload)
	choices = data.get("choices") if isinstance(data, dict) else None
	if not isinstance(choices, list) or not choices:
		raise RuntimeError("OpenRouter vision response missing choices")
	message = choices[0].get("message") if isinstance(choices[0], dict) else None
	text_content = message.get("content") if isinstance(message, dict) else None
	text = str(text_content or "").strip()
	if not text:
		raise RuntimeError("OpenRouter vision response content is empty")
	return text


def _openrouter_post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
	if not OPENROUTER_API_KEY:
		raise RuntimeError("OPENROUTER_API_KEY is not configured")

	body = json.dumps(payload).encode("utf-8")
	endpoint = f"{OPENROUTER_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
	req = urlrequest.Request(
		endpoint,
		data=body,
		headers={
			"Authorization": f"Bearer {OPENROUTER_API_KEY}",
			"Content-Type": "application/json",
		},
		method="POST",
	)

	try:
		with urlrequest.urlopen(req, timeout=OPENROUTER_TIMEOUT_SECONDS) as resp:
			raw = resp.read().decode("utf-8", errors="replace")
	except urlerror.HTTPError as exc:
		details = exc.read().decode("utf-8", errors="replace")
		raise RuntimeError(f"OpenRouter HTTP {exc.code}: {details}") from exc
	except Exception as exc:
		raise RuntimeError(f"OpenRouter request failed: {exc}") from exc

	data = json.loads(raw)
	if not isinstance(data, dict):
		raise RuntimeError("OpenRouter returned non-JSON response")
	return data


def _coerce_contents_to_text(contents: Any) -> str | None:
	if isinstance(contents, str):
		return contents
	if isinstance(contents, list):
		text_parts: list[str] = []
		for item in contents:
			if isinstance(item, str):
				text_parts.append(item)
				continue
			# Non-string content (e.g. Part.from_uri/media payload) is not provider-agnostic.
			return None
		return "\n\n".join(part for part in text_parts if part)
	return None


def _openrouter_generate_text(
	*,
	model: str,
	contents: Any,
	temperature: float,
	response_format: dict[str, Any] | None = None,
	openrouter_model: str | None = None,
) -> str:
	if not OPENROUTER_API_KEY:
		raise RuntimeError("OPENROUTER_API_KEY is not configured")

	prompt_text = _coerce_contents_to_text(contents)
	if not prompt_text:
		raise RuntimeError("OpenRouter fallback supports text-only contents")

	payload = {
		"model": str(openrouter_model or OPENROUTER_MODEL or model),
		"messages": [{"role": "user", "content": prompt_text}],
		"temperature": float(temperature),
			"max_tokens": 4096,
	}
	if isinstance(response_format, dict) and response_format:
		payload["response_format"] = response_format
	data = _openrouter_post("chat/completions", payload)
	choices = data.get("choices") if isinstance(data, dict) else None
	if not isinstance(choices, list) or not choices:
		raise RuntimeError("OpenRouter response missing choices")
		finish_reason = choices[0].get("finish_reason") if isinstance(choices[0], dict) else None
		if str(finish_reason or "").lower() == "length":
			raise RuntimeError("OpenRouter response was truncated due to output length")
	message = choices[0].get("message") if isinstance(choices[0], dict) else None
	content = message.get("content") if isinstance(message, dict) else None
	text = str(content or "").strip()
	if not text:
		raise RuntimeError("OpenRouter response content is empty")
	return text


def _openrouter_embed_texts(*, model: str, texts: list[str]) -> EmbeddingResponse:
	payload = {
		"model": str(model or OPENROUTER_EMBEDDING_MODEL),
		"input": texts,
	}
	data = _openrouter_post("embeddings", payload)
	items = data.get("data") if isinstance(data, dict) else None
	if not isinstance(items, list) or not items:
		raise RuntimeError("OpenRouter embedding response missing data")

	embeddings: list[EmbeddingItem] = []
	for item in items:
		emb = item.get("embedding") if isinstance(item, dict) else None
		if not isinstance(emb, list):
			raise RuntimeError("OpenRouter embedding item missing embedding vector")
		embeddings.append(EmbeddingItem(values=[float(x) for x in emb]))
	return EmbeddingResponse(embeddings=embeddings)


class _OpenRouterModels:
	def generate_content(
		self,
		*,
		model: str,
		contents: Any,
		temperature: float = 0.0,
		config: Any | None = None,
		**_: Any,
	) -> TextResponse:
		if config is not None:
			try:
				temperature = float(getattr(config, "temperature", temperature) or temperature)
			except Exception:
				pass
		text = _openrouter_generate_text(
			model=model,
			contents=contents,
			temperature=temperature,
			openrouter_model=model,
		)
		return TextResponse(text=text)

	def embed_content(self, *, model: str, contents: Any, **_: Any) -> EmbeddingResponse:
		if isinstance(contents, str):
			texts = [contents]
		elif isinstance(contents, list):
			texts = [str(item) for item in contents if str(item or "").strip()]
		else:
			texts = []

		if not texts:
			raise RuntimeError("OpenRouter embeddings require non-empty text content")

		return _openrouter_embed_texts(model=model, texts=texts)


class OpenRouterClient:
	def __init__(self) -> None:
		self.models = _OpenRouterModels()


_OPENROUTER_CLIENT: OpenRouterClient | None = None


def get_openrouter_client() -> OpenRouterClient | None:
	global _OPENROUTER_CLIENT
	if not OPENROUTER_API_KEY:
		return None
	if _OPENROUTER_CLIENT is None:
		_OPENROUTER_CLIENT = OpenRouterClient()
	return _OPENROUTER_CLIENT


def generate_content_with_openrouter_fallback(
	*,
	primary_call: Callable[[], Any],
	model: str,
	contents: Any,
	temperature: float = 0.0,
	response_format: dict[str, Any] | None = None,
	call_name: str = "unknown",
	complexity: str = "complex",
) -> Any:
	"""Call the text-generation model via OpenRouter only."""
	del primary_call
	del complexity
	if ENABLE_OPENROUTER_FALLBACK and OPENROUTER_API_KEY:
		# Honor the model requested by the caller. This prevents hidden model
		# overrides (for example, forcing OPENROUTER_COMPLEX_MODEL for
		# "complex" calls) and keeps billing/observability aligned with logs.
		chosen_openrouter_model = str(model or "").strip() or OPENROUTER_MODEL
		try:
			fallback_text = _openrouter_generate_text(
				model=model,
				contents=contents,
				temperature=temperature,
				response_format=response_format,
				openrouter_model=chosen_openrouter_model,
			)
			return TextResponse(text=fallback_text)
		except Exception as or_exc:
			raise RuntimeError(f"OpenRouter call failed for {call_name}: {or_exc}") from or_exc

	raise RuntimeError("OpenRouter is disabled or OPENROUTER_API_KEY is missing")
