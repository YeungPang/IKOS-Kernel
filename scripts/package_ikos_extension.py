"""Validate and build an IKOS extension manifest from a development folder.

This command packages declarative JSON/SOLF assets only. It never reads database
credentials, connects to PostgreSQL, installs an extension, or promotes a release.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import extension_packages


def _read_asset_file(root: Path, relative_name: Any) -> str:
    if not isinstance(relative_name, str) or not relative_name.strip():
        raise ValueError("asset file reference must be a non-empty relative path")
    candidate = Path(relative_name)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("asset file reference must stay inside the source folder")
    source = root / candidate
    if source.is_symlink():
        raise ValueError("symbolic links are not accepted as package assets")
    resolved = source.resolve(strict=True)
    if resolved.parent != root and root not in resolved.parents:
        raise ValueError("asset file resolves outside the source folder")
    if not resolved.is_file() or resolved.suffix.lower() != ".solf":
        raise ValueError("script_file/clause_file must reference a regular .solf file")
    if resolved.stat().st_size > 1_000_000:
        raise ValueError("SOLF asset exceeds the 1 MB limit")
    return resolved.read_text(encoding="utf-8")


def build_manifest(source_dir: Path) -> dict[str, Any]:
    root = source_dir.expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError("source path must be an extension folder")
    manifest_path = root / "extension.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("extension folder must contain a regular extension.json")
    if manifest_path.stat().st_size > extension_packages.MAX_PACKAGE_BYTES:
        raise ValueError("extension.json exceeds the 2 MB limit")
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("extension.json must contain valid UTF-8 JSON") from exc
    if not isinstance(data, dict) or not isinstance(data.get("assets"), list):
        raise ValueError("extension.json must be an object with an assets array")

    for asset in data["assets"]:
        if not isinstance(asset, dict) or not isinstance(asset.get("payload"), dict):
            raise ValueError("every asset must contain an object payload")
        payload = asset["payload"]
        kind = asset.get("kind")
        if kind == "solf_script" and "script_file" in payload:
            if "script" in payload:
                raise ValueError("use either payload.script or payload.script_file, not both")
            payload["script"] = _read_asset_file(root, payload.pop("script_file"))
        elif kind == "solf_clause" and "clause_file" in payload:
            if "clause_body" in payload:
                raise ValueError("use either payload.clause_body or payload.clause_file, not both")
            payload["clause_body"] = _read_asset_file(root, payload.pop("clause_file"))

    try:
        parsed = extension_packages.ExtensionManifest.model_validate(data)
    except Exception as exc:
        raise ValueError(f"extension manifest validation failed: {exc}") from exc
    return extension_packages.canonical_manifest(parsed)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_dir", type=Path, help="Folder containing extension.json and optional assets/")
    parser.add_argument("--output", type=Path, default=None, help="Output JSON path; default: <source>/dist/<key>-<version>.json")
    parser.add_argument("--check", action="store_true", help="Validate and print the package hash without writing an artifact")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        manifest = build_manifest(args.source_dir)
        digest = extension_packages.manifest_sha256(manifest)
        artifact_name = f"{manifest['extension_key']}-{manifest['version']}.json"
        output_path = args.output or (args.source_dir.resolve() / "dist" / artifact_name)
        if not args.check:
            resolved_output = output_path.expanduser().resolve()
            resolved_root = args.source_dir.expanduser().resolve(strict=True)
            if resolved_output == resolved_root or resolved_root in resolved_output.parents:
                # Keep generated output out of the editable source tree by default,
                # but permit its conventional dist/ child for local packaging.
                relative = resolved_output.relative_to(resolved_root)
                if not relative.parts or relative.parts[0] != "dist":
                    raise ValueError("output inside the source folder must be under dist/")
            resolved_output.parent.mkdir(parents=True, exist_ok=True)
            temporary = resolved_output.with_suffix(resolved_output.suffix + ".tmp")
            temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            os.replace(temporary, resolved_output)
            print(f"Package artifact: {resolved_output}")
        print(f"Extension: {manifest['extension_key']}@{manifest['version']}")
        print(f"Assets: {len(manifest['assets'])}")
        print(f"SHA-256: {digest}")
        print("Validation: passed")
        return 0
    except (OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
