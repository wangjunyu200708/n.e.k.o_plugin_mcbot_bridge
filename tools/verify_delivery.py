"""Verify local delivery artifacts with the host's real package inspector."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from pathlib import Path


def fingerprint(path: Path) -> dict[str, object]:
    return {
        "name": path.name,
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=Path, required=True)
    parser.add_argument("--mod", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    version = tomllib.loads((root / "plugin.toml").read_text(encoding="utf-8"))["plugin"]["version"]
    package = root / f"dist/mcbot_bridge-{version}.neko-plugin"
    jar = args.mod.resolve() / "build/libs/mcbot-0.1.0.jar"
    core = args.mod.resolve() / "agent-core/build/libs/agent-core-0.1.0.jar"
    sys.path.insert(0, str(args.host.resolve()))
    from plugin.core.packaged_metadata import compute_source_sha256
    from plugin.neko_plugin_cli.core import inspect_package
    from plugin.sdk.plugin.llm_tool import LLM_TOOL_META_ATTR

    inspected = inspect_package(package)
    assert inspected.package_id == "mcbot_bridge"
    assert inspected.version == version
    assert inspected.payload_hash_verified
    with tempfile.TemporaryDirectory(prefix="delivery-check-", dir=root / "build") as temp:
        with zipfile.ZipFile(package) as archive:
            assert archive.testzip() is None
            names = archive.namelist()
            forbidden = {"data", "tests", "tools", "node_modules", "__pycache__"}
            for name in names:
                parts = Path(name).parts
                assert not forbidden.intersection(parts), name
                assert Path(name).name not in {".env", ".lock", "bridge.token", "client.json"}, name
            # The official inspector above checks traversal and archive layout first.
            archive.extractall(temp)
        plugin_dir = Path(temp) / "payload/plugins/mcbot_bridge"
        metadata = json.loads((plugin_dir / "plugin.meta.json").read_text(encoding="utf-8"))
        assert metadata["source_sha256"] == compute_source_sha256(plugin_dir)
        assert {entry["id"] for entry in metadata["entries"]} == {
            "mcbot_task", "mcbot_answer", "mcbot_status", "mcbot_reload_config", "mcbot_stop",
        }
        assert all(entry["metadata"]["result_kind"] == "event" for entry in metadata["entries"])
        task_entry = next(entry for entry in metadata["entries"] if entry["id"] == "mcbot_task")
        assert task_entry["input_schema"]["required"] == ["text"]
        assert (plugin_dir / "surfaces/mcbot.tsx").is_file()
        # Import the packaged facade with the actual SDK, without instantiating
        # a plugin or invoking its startup lifecycle or any game endpoint.
        spec = importlib.util.spec_from_file_location(
            "plugin.plugins.mcbot_bridge", plugin_dir / "__init__.py",
            submodule_search_locations=[str(plugin_dir)],
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        tools = []
        assert getattr(module.McbotBridgePlugin.mcbot_task, LLM_TOOL_META_ATTR, None) is None
        for suffix in ("ask", "answer", "cancel"):
            method = getattr(module.McbotBridgePlugin, "mcbot_" + suffix)
            meta = getattr(method, LLM_TOOL_META_ATTR)
            payload = meta.to_ipc_payload(plugin_id="mcbot_bridge")
            assert payload["type"] == "LLM_TOOL_REGISTER"
            assert payload["parameters"]["type"] == "object"
            tools.append(meta.name)
        # -I -S exclude the environment's site packages. Every HTTP dependency
        # must resolve from the shipped vendor tree; no requests are made.
        subprocess.run([
            sys.executable, "-I", "-S", "-B", "-c",
            "import sys; sys.path.insert(0, sys.argv[1]); "
            "import httpx, httpcore, anyio, certifi, h11, idna; "
            "from pathlib import Path; "
            "assert all(Path(m.__file__).is_relative_to(Path(sys.argv[1])) "
            "for m in (httpx, httpcore, anyio, certifi, h11, idna)); "
            "client = httpx.Client(trust_env=False); client.close()",
            str(plugin_dir / "vendor"),
        ], check=True)

    with zipfile.ZipFile(jar) as archive:
        assert archive.testzip() is None
        fabric = json.loads(archive.read("fabric.mod.json"))
        assert fabric["id"] == "mcbot" and fabric["version"] == "0.1.0"
        embedded = archive.read("META-INF/jars/agent-core-0.1.0.jar")
        assert embedded == core.read_bytes()
        with zipfile.ZipFile(io.BytesIO(embedded)) as inner:
            assert inner.testzip() is None

    report = {
        "package": fingerprint(package),
        "mod": fingerprint(jar),
        "embedded_core": fingerprint(core),
        "official_payload_hash_verified": True,
        "packaged_metadata_source_verified": True,
        "actual_sdk_tools": tools,
        "isolated_vendor_import_verified": True,
        "real_game_or_host_started": False,
    }
    output = root / f"build/delivery-verification-{version}-20261025.json"
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
