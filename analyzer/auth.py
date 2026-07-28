"""Local access gate for Infraredshift.

There are deliberately no compiled-in credentials.  The first user creates a
local access code and PIN, which are stored only as salted PBKDF2-SHA256
verifiers in the per-user application directory.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from pathlib import Path

from .runtime_paths import resolve_runtime_paths

_ITERATIONS = 240_000
_MIN_ACCESS_CODE_LENGTH = 6
_MIN_PIN_LENGTH = 4
_MAX_PIN_LENGTH = 12


def auth_path() -> Path:
    return resolve_runtime_paths().auth_file


def _hash(value: str, salt: str) -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        (value or "").encode("utf-8"),
        bytes.fromhex(salt),
        _ITERATIONS,
    )
    return digest.hex()


def validate_new_credentials(access_code: str, pin: str) -> None:
    """Reject weak or malformed first-run credentials before touching disk."""
    access_code = str(access_code or "").strip()
    pin = str(pin or "").strip()
    if len(access_code) < _MIN_ACCESS_CODE_LENGTH:
        raise ValueError(f"Access code must contain at least {_MIN_ACCESS_CODE_LENGTH} characters.")
    if not (_MIN_PIN_LENGTH <= len(pin) <= _MAX_PIN_LENGTH) or not pin.isdecimal():
        raise ValueError(
            f"PIN must contain {_MIN_PIN_LENGTH} to {_MAX_PIN_LENGTH} digits."
        )
    if access_code == pin:
        raise ValueError("Access code and PIN must be different.")


def save_credentials(access_code: str, pin: str, path: str | os.PathLike | None = None) -> Path:
    validate_new_credentials(access_code, pin)
    target = Path(path) if path else auth_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    access_salt = secrets.token_hex(16)
    pin_salt = secrets.token_hex(16)
    payload = {
        "version": 1,
        "kdf": "PBKDF2-HMAC-SHA256",
        "iterations": _ITERATIONS,
        "access_salt": access_salt,
        "access_hash": _hash(access_code, access_salt),
        "pin_salt": pin_salt,
        "pin_hash": _hash(pin, pin_salt),
    }
    tmp = target.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2))
        handle.flush()
        os.fsync(handle.fileno())
    if not verify_credentials(access_code, pin, tmp):
        tmp.unlink(missing_ok=True)
        raise OSError("Local access verifier failed validation before promotion.")
    os.replace(tmp, target)
    return target


def credentials_exist(path: str | os.PathLike | None = None) -> bool:
    target = Path(path) if path else auth_path()
    return target.is_file()


def verify_credentials(access_code: str, pin: str, path: str | os.PathLike | None = None) -> bool:
    target = Path(path) if path else auth_path()
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return False
        # Existing installations wrote the same PBKDF2 verifier before the
        # descriptive ``kdf`` field was added.  Accept that exact legacy
        # shape so an upgrade cannot lock out the user's encrypted .secrets.
        if data.get("version") != 1 or data.get("kdf") not in {None, "PBKDF2-HMAC-SHA256"}:
            return False
        # Never accept an attacker-controlled work factor.  Existing files
        # must match the build's reviewed KDF policy exactly.
        if data.get("iterations") != _ITERATIONS:
            return False
        access_salt = str(data["access_salt"])
        pin_salt = str(data["pin_salt"])
        if len(access_salt) != 32 or len(pin_salt) != 32:
            return False
        bytes.fromhex(access_salt)
        bytes.fromhex(pin_salt)
        access_hash = str(data["access_hash"])
        pin_hash = str(data["pin_hash"])
        if len(access_hash) != 64 or len(pin_hash) != 64:
            return False
        access_ok = hmac.compare_digest(
            _hash(access_code, access_salt),
            access_hash,
        )
        pin_ok = hmac.compare_digest(
            _hash(pin, pin_salt),
            pin_hash,
        )
        return access_ok and pin_ok
    except Exception:
        return False
