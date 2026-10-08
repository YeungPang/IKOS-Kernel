"""One bundled SOLF program, composed without duplicating baseline definitions.

Only the two repository-owned entry points receive compatibility handling.
Explicit application scripts retain ordinary path/text loading semantics.
"""
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROGRAM_PATH = ROOT / "solf_script.txt"
BASELINE_PATH = ROOT / "kernel_script.txt"
BASELINE_MARKER = "# IKOS-BUNDLED-BASELINE: kernel_script.txt"


def expand_bundled_program(script: str) -> str:
    """Expand the fixed baseline marker, not arbitrary includes/imports."""
    first, _, remainder = script.partition("\n")
    if first.rstrip("\r") == BASELINE_MARKER:
        return BASELINE_PATH.read_text(encoding="utf-8") + "\n\n" + remainder
    return script


def read_program(file_path: str | Path | None = None) -> str:
    """Read the complete bundle; bare legacy names are independent of cwd.

An explicit path outside this repository is never replaced with the bundle.
The repository baseline filename is a compatibility entry point, not a copy.
"""
    path = PROGRAM_PATH if file_path is None else Path(file_path).expanduser()
    if path in (Path("solf_script.txt"), Path("kernel_script.txt")):
        path = PROGRAM_PATH
    elif path.resolve() in (PROGRAM_PATH, BASELINE_PATH):
        path = PROGRAM_PATH
    text = path.read_text(encoding="utf-8")
    if path.resolve() == PROGRAM_PATH:
        return expand_bundled_program(text)
    return text