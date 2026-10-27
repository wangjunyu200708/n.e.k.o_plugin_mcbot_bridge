"""Build a source plugin archive using an explicit allowlist, excluding all local state."""
from __future__ import annotations

import hashlib
import json
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILES = (
    "__init__.py", "client.py", "service.py", "prompts.py", "plugin.toml",
    "config.schema.json", "pyproject.toml", "README.md", "STATUS.md", "LICENSE",
    "docs/BRIDGE-RELIABILITY-2026-10-25.md",
)


def main() -> None:
    version = tomllib.loads((ROOT / "plugin.toml").read_text(encoding="utf-8"))["plugin"]["version"]
    output = ROOT / "dist" / f"mcbot_bridge-{version}.zip"
    output.parent.mkdir(parents=True, exist_ok=True)
    files = [ROOT / name for name in FILES]
    files.extend(sorted((ROOT / "i18n").glob("*.json")))
    files.extend(sorted((ROOT / "surfaces").glob("*.tsx")))
    manifest = {}
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source in files:
            name = source.relative_to(ROOT).as_posix()
            content = source.read_bytes()
            archive.writestr(name, content)
            manifest[name] = hashlib.sha256(content).hexdigest()
        archive.writestr("PACKAGE-MANIFEST.json", json.dumps(manifest, indent=2) + "\n")
    print(f"{output.name}: {output.stat().st_size} bytes")
    print(f"SHA256 {hashlib.sha256(output.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    main()
