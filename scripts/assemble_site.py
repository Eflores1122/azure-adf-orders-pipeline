#!/usr/bin/env python3
"""Copy the static site and sample CSVs into the GitHub Pages artifact."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import validate_adf  # noqa: E402


def assemble(root: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(root / "site", dest)
    sample_dest = dest / "samples"
    sample_dest.mkdir(parents=True, exist_ok=True)
    files = []
    for path in sorted((root / "samples").glob("*.csv")):
        shutil.copy2(path, sample_dest / path.name)
        _header, rows = validate_adf.explain_csv(path)
        files.append(
            {
                "name": path.name,
                "good": sum(1 for row in rows if row["verdict"] == "good"),
                "reject": sum(1 for row in rows if row["verdict"] == "reject"),
                "rows": [
                    {"line": row["line"], "verdict": row["verdict"], "reason": row["reason"]}
                    for row in rows
                ],
            }
        )
    payload = {"source": "scripts/validate_adf.py explain_csv", "files": files}
    (dest / "measured.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    assemble(ROOT, ROOT / "_site")


if __name__ == "__main__":
    main()
