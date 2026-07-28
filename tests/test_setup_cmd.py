from __future__ import annotations

from pathlib import Path


def test_cmd_python_version_check_does_not_pass_caret_to_python() -> None:
    script = (Path(__file__).resolve().parents[1] / "setup_new_machine.cmd").read_text(
        encoding="utf-8"
    )

    assert "sys.version_info >= (3,9)" in script
    assert "sys.version_info ^>=" not in script
    assert "powershell" not in script.lower()


def test_setup_prefers_email_safe_txt_launcher() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "setup_new_machine.cmd").read_text(encoding="utf-8")
    # Infraredshift.txt is the branded email-safe launcher; legacy aliases remain.
    assert "Infraredshift.txt" in script
    assert "Infraredshift" in script
    assert script.index("Infraredshift.txt") < script.index("Infraredshift.py")
