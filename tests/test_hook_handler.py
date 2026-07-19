"""Tests for the generated in-gateway handler (the spooler), issue #4.

The handler runs inside the Hermes gateway process. These load the actual
generated ``handler.py`` by path (as Hermes' loader does) and drive its
``handle(event_type, context)`` to assert the on-disk contract the drain
depends on: one newline-terminated JSON line per event with
``event_type``/``context``/``captured_at``; that it NEVER raises (even on an
unserializable context or an unwritable spool); the over-cap drop; and that
failures are logged to the Bridge-side error log.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

from hermes_flight_recorder.collector.hook import ERRLOG_FILENAME, SPOOL_FILENAME
from hermes_flight_recorder.collector.hook.install import install_hook


def load_handler(tmp_path: Path, monkeypatch, bridge_home: Path | None = None) -> ModuleType:
    """Install the hook and import the generated handler.py by path."""
    bridge = bridge_home or (tmp_path / "bridge")
    hook_dir = install_hook(tmp_path / "hermes", bridge)
    # Force the handler onto our bridge home regardless of any ambient env.
    monkeypatch.setenv("BRIDGE_HOME", str(bridge))
    spec = importlib.util.spec_from_file_location("gen_handler", hook_dir / "handler.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_spool(bridge: Path) -> list[dict]:
    lines = (bridge / SPOOL_FILENAME).read_text().splitlines()
    return [json.loads(line) for line in lines]


def test_handle_appends_one_json_line_per_event(tmp_path: Path, monkeypatch) -> None:
    bridge = tmp_path / "bridge"
    handler = load_handler(tmp_path, monkeypatch, bridge)
    handler.handle("session:start", {"session_id": "s1", "session_key": "k1"})
    handler.handle("agent:start", {"session_id": "s1", "message": "hi"})

    records = read_spool(bridge)
    assert [r["event_type"] for r in records] == ["session:start", "agent:start"]
    assert records[0]["context"] == {"session_id": "s1", "session_key": "k1"}
    assert isinstance(records[1]["captured_at"], (int, float))


def test_spool_lines_are_newline_terminated(tmp_path: Path, monkeypatch) -> None:
    bridge = tmp_path / "bridge"
    handler = load_handler(tmp_path, monkeypatch, bridge)
    handler.handle("gateway:startup", {"platforms": ["cli"]})
    raw = (bridge / SPOOL_FILENAME).read_bytes()
    assert raw.endswith(b"\n")


def test_handle_never_raises_on_unserializable_context(tmp_path: Path, monkeypatch) -> None:
    bridge = tmp_path / "bridge"
    handler = load_handler(tmp_path, monkeypatch, bridge)
    # A bare object is not JSON-serializable; default=str must keep it from
    # raising into the gateway, and the line must still be written.
    handler.handle("agent:end", {"session_id": "s1", "obj": object()})
    records = read_spool(bridge)
    assert records[0]["event_type"] == "agent:end"
    assert isinstance(records[0]["context"]["obj"], str)


def test_handle_never_raises_when_spool_path_unwritable(tmp_path: Path, monkeypatch) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    # Make the spool path a directory so the append open() fails.
    (bridge / SPOOL_FILENAME).mkdir()
    handler = load_handler(tmp_path, monkeypatch, bridge)
    handler.handle("session:start", {"session_id": "s1"})  # must not raise
    # The failure is recorded Bridge-side.
    errlog = bridge / ERRLOG_FILENAME
    assert errlog.exists() and "handler error" in errlog.read_text()


def test_over_cap_drops_event_and_logs(tmp_path: Path, monkeypatch) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    spool = bridge / SPOOL_FILENAME
    spool.write_bytes(b"x" * 4096)  # pre-fill past the tiny cap we set below
    handler = load_handler(tmp_path, monkeypatch, bridge)
    monkeypatch.setattr(handler, "_MAX_SPOOL_BYTES", 1024)
    handler.handle("agent:start", {"session_id": "s1", "message": "hi"})
    # Nothing appended (still just the pre-fill), and the drop is logged.
    assert spool.read_bytes() == b"x" * 4096
    assert "dropped agent:start" in (bridge / ERRLOG_FILENAME).read_text()


def test_runtime_bridge_home_env_overrides_baked(tmp_path: Path, monkeypatch) -> None:
    # Install baking one home, then point BRIDGE_HOME at another at runtime.
    hook_dir = install_hook(tmp_path / "hermes", tmp_path / "baked")
    other = tmp_path / "other"
    monkeypatch.setenv("BRIDGE_HOME", str(other))
    spec = importlib.util.spec_from_file_location("gen_handler2", hook_dir / "handler.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.handle("gateway:startup", {"platforms": []})
    assert (other / SPOOL_FILENAME).exists()
    assert not (tmp_path / "baked" / SPOOL_FILENAME).exists()
