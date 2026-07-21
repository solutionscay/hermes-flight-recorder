"""Tests for installing the in-gateway hook package (issue #4).

``install_hook`` writes ``$HERMES_HOME/hooks/hermes-flight-recorder/`` with a
``HOOK.yaml`` manifest and a generated, standard-library-only ``handler.py``.
These assert the manifest contract Hermes' loader requires (``name``,
``description``, a non-empty ``events`` list), that the generated handler is
valid Python exposing a module-level ``handle``, the force/idempotency guard,
that the Flight Recorder home is baked in (with a ``$SC_HERMES_FLIGHT_RECORDER_HOME`` runtime override),
and the invariant that the hook package is the ONLY thing written under the
Hermes home.
"""

from __future__ import annotations

import ast
import py_compile
from pathlib import Path

import pytest

from hermes_flight_recorder.collector.hook import HOOK_EVENTS
from hermes_flight_recorder.collector.hook.install import install_hook, render_handler


def test_install_writes_manifest_and_handler(tmp_path: Path) -> None:
    hook_dir = install_hook(tmp_path / "hermes", tmp_path / "bridge")
    assert hook_dir == tmp_path / "hermes" / "hooks" / "hermes-flight-recorder"
    assert sorted(p.name for p in hook_dir.iterdir()) == ["HOOK.yaml", "handler.py"]


def test_manifest_declares_name_description_and_all_events(tmp_path: Path) -> None:
    hook_dir = install_hook(tmp_path / "hermes", tmp_path / "bridge")
    text = (hook_dir / "HOOK.yaml").read_text()
    # Hermes' loader requires name + a non-empty events list; parse leniently.
    assert "name: hermes-flight-recorder" in text
    assert "description:" in text
    listed = [line.split("- ", 1)[1].strip() for line in text.splitlines() if line.strip().startswith("- ")]
    assert listed == list(HOOK_EVENTS)


def test_generated_handler_compiles_and_exposes_handle(tmp_path: Path) -> None:
    hook_dir = install_hook(tmp_path / "hermes", tmp_path / "bridge")
    handler = hook_dir / "handler.py"
    py_compile.compile(str(handler), doraise=True)  # raises on a syntax error
    tree = ast.parse(handler.read_text())
    funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    assert "handle" in funcs


def test_handler_is_stdlib_only(tmp_path: Path) -> None:
    """The in-gateway handler must not import the Flight Recorder package or crypto."""
    hook_dir = install_hook(tmp_path / "hermes", tmp_path / "bridge")
    tree = ast.parse((hook_dir / "handler.py").read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported == {"json", "os", "time"}
    assert "hermes_flight_recorder" not in imported
    assert "cryptography" not in imported


def test_flight_recorder_home_is_baked_in_resolved(tmp_path: Path) -> None:
    src = render_handler(tmp_path / "bridge")
    expected = str((tmp_path / "bridge").resolve())
    assert expected in src
    assert "SC_HERMES_FLIGHT_RECORDER_HOME" in src
    assert "BRIDGE" + "_HOME" not in src


def test_reinstall_without_force_raises(tmp_path: Path) -> None:
    install_hook(tmp_path / "hermes", tmp_path / "bridge")
    with pytest.raises(FileExistsError):
        install_hook(tmp_path / "hermes", tmp_path / "bridge")


def test_reinstall_with_force_rewrites(tmp_path: Path) -> None:
    hook_dir = install_hook(tmp_path / "hermes", tmp_path / "bridge-old")
    hook_dir = install_hook(tmp_path / "hermes", tmp_path / "bridge-new", force=True)
    assert str((tmp_path / "bridge-new").resolve()) in (hook_dir / "handler.py").read_text()


def test_hook_is_the_only_write_under_hermes_home(tmp_path: Path) -> None:
    hermes = tmp_path / "hermes"
    install_hook(hermes, tmp_path / "bridge")
    files = {p.relative_to(hermes).as_posix() for p in hermes.rglob("*") if p.is_file()}
    assert files == {
        "hooks/hermes-flight-recorder/HOOK.yaml",
        "hooks/hermes-flight-recorder/handler.py",
    }
