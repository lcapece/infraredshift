from __future__ import annotations

import base64
import hashlib

from tools.build_fat_py import render_launcher as render_fat_launcher
from tools.build_text_py import render_launcher as render_text_launcher


def _load_launcher(source: str) -> dict[str, object]:
    namespace: dict[str, object] = {"__name__": "launcher_test"}
    exec(compile(source, "<generated-launcher>", "exec"), namespace)
    return namespace


def test_plaintext_launcher_repairs_tampered_materialized_source(tmp_path, monkeypatch) -> None:
    expected = "VALUE = 'trusted'\n"
    namespace = _load_launcher(
        render_text_launcher({"analyzer/__init__.py": expected}, {})
    )
    monkeypatch.setattr(namespace["tempfile"], "tempdir", str(tmp_path))

    root = namespace["_materialize_sources"]()
    source = root / "analyzer" / "__init__.py"
    source.write_text("VALUE = 'tampered'\n", encoding="utf-8")

    repaired = namespace["_materialize_sources"]()

    assert repaired == root
    assert source.read_text(encoding="utf-8") == expected
    assert (root / ".complete").read_text(encoding="ascii") == namespace["_source_digest"]()


def test_fat_launcher_repairs_tampered_cached_bundle(tmp_path, monkeypatch) -> None:
    payload = b"PK\x03\x04trusted-test-payload"
    namespace = _load_launcher(render_fat_launcher(base64.b64encode(payload).decode("ascii")))
    monkeypatch.setattr(namespace["tempfile"], "tempdir", str(tmp_path))

    path = namespace["_bundle_zip_path"]()
    path.write_bytes(b"tampered")

    repaired = namespace["_bundle_zip_path"]()

    assert repaired == path
    assert repaired.read_bytes() == payload
    assert hashlib.sha256(repaired.read_bytes()).digest() == hashlib.sha256(payload).digest()

