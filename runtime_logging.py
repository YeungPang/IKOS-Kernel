from __future__ import annotations

import logging
import os
from datetime import datetime
from logging import Handler
from pathlib import Path
from threading import RLock
from typing import Any


_CONFIGURED = False


class DailyDatedFileHandler(Handler):
    def __init__(self, log_dir: str | Path, prefix: str = "log") -> None:
        super().__init__()
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.prefix = str(prefix or "log")
        self._lock = RLock()
        self._current_path: Path | None = None
        self._stream: Any | None = None

    def _target_path(self) -> Path:
        today = datetime.now().strftime("%Y-%m-%d")
        return self.log_dir / f"{self.prefix}-{today}.log"

    def _ensure_stream(self) -> None:
        target_path = self._target_path()
        if self._stream is not None and self._current_path == target_path:
            return

        if self._stream is not None:
            try:
                self._stream.close()
            except Exception:
                pass

        self._stream = target_path.open("a", encoding="utf-8")
        self._current_path = target_path

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            with self._lock:
                self._ensure_stream()
                assert self._stream is not None
                self._stream.write(message + "\n")
                self._stream.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        with self._lock:
            if self._stream is not None:
                try:
                    self._stream.close()
                finally:
                    self._stream = None
                    self._current_path = None
        super().close()


def configure_logging(level: int | str | None = None) -> logging.Logger:
    global _CONFIGURED
    root = logging.getLogger()
    resolved_level = level if isinstance(level, int) else getattr(logging, str(level or os.getenv("IDMS_LOG_LEVEL", "INFO")).upper(), logging.INFO)
    root.setLevel(resolved_level)

    if _CONFIGURED:
        return root

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    log_dir = Path(os.getenv("IDMS_LOG_DIR", "log"))

    file_handler = DailyDatedFileHandler(log_dir=log_dir, prefix="log")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    _CONFIGURED = True
    return root