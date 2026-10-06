from __future__ import annotations

import argparse

import uvicorn

from ikos_api_server.application import app


def _build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Run IDMS API server.")
	parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
	parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
	parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
	parser.add_argument(
		"--graceful-timeout",
		type=int,
		default=0,
		help="Uvicorn graceful shutdown timeout in seconds (default: 0 for immediate Ctrl+C stop)",
	)
	return parser


def main() -> int:
	args = _build_parser().parse_args()
	uvicorn.run(
		"ikos_api:app",
		host=args.host,
		port=args.port,
		reload=bool(args.reload),
		timeout_graceful_shutdown=int(args.graceful_timeout),
	)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

