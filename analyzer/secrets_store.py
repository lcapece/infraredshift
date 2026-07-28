"""Windows-user-bound Redshift credential storage with GUI access gating."""
from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sys
import threading
from typing import Mapping

from .runtime_paths import resolve_runtime_paths


FORMAT = "databasix-encrypted-secrets"
VERSION = 3
ALGORITHM = "Windows-DPAPI-current-user"
LEGACY_VERSION = 2
LEGACY_ALGORITHM = "Windows-DPAPI-current-user+DataBasix-credential-entropy"
_SECRET_KEY = re.compile(
    r"^REDSHIFT_(?:HOST|USER|PASSWORD)$|"
    r"^REDSHIFT_PRODUCER_(?:HOST|USER|PASSWORD)$|"
    r"^REDSHIFT_CONSUMER_\d+_(?:HOST|USER|PASSWORD)$"
)
_SESSION_VALUES: dict[str, str] = {}
_SESSION_LOCK = threading.RLock()
_MAX_SECRETS_FILE_BYTES = 1_000_000
_GATE_ITERATIONS = 120_000


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


_DPAPI_PROTOTYPED = False


def _dpapi_functions():
    """crypt32/kernel32 with declared prototypes.

    Without argtypes/restype, ctypes assumes C int for every parameter and
    return value, which truncates 64-bit pointers (LocalFree on a truncated
    pbData is heap corruption waiting to happen).
    """
    global _DPAPI_PROTOTYPED
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    if not _DPAPI_PROTOTYPED:
        blob_p = ctypes.POINTER(_DATA_BLOB)
        crypt32.CryptProtectData.argtypes = [
            blob_p, wintypes.LPCWSTR, blob_p, ctypes.c_void_p,
            ctypes.c_void_p, wintypes.DWORD, blob_p,
        ]
        crypt32.CryptProtectData.restype = wintypes.BOOL
        crypt32.CryptUnprotectData.argtypes = [
            blob_p, ctypes.POINTER(wintypes.LPWSTR), blob_p, ctypes.c_void_p,
            ctypes.c_void_p, wintypes.DWORD, blob_p,
        ]
        crypt32.CryptUnprotectData.restype = wintypes.BOOL
        kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        kernel32.LocalFree.restype = ctypes.c_void_p
        _DPAPI_PROTOTYPED = True
    return crypt32, kernel32


def is_secret_key(key: str) -> bool:
    return bool(_SECRET_KEY.fullmatch(str(key or "").strip().upper()))


def secrets_candidates(explicit_path: str | os.PathLike | None = None) -> tuple[Path, ...]:
    if explicit_path:
        return (Path(explicit_path).expanduser(),)
    paths = resolve_runtime_paths()
    candidates: list[Path] = [
        paths.config_dir / ".secrets",
        paths.install_dir / ".secrets",
        Path(__file__).resolve().parents[1] / ".secrets",
    ]
    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path.absolute())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return tuple(unique)


def active_secrets_path() -> Path:
    """Per-user destination for all new or migrated encrypted credentials."""
    candidates = secrets_candidates()
    return candidates[0]


def existing_secrets_path() -> Path | None:
    """Find a per-user file first, then a legacy beside-launcher file."""
    return next((path for path in secrets_candidates() if path.is_file()), None)


def encrypted_secrets_exist(path: str | os.PathLike | None = None) -> bool:
    """Identify an encrypted credential file without attempting to decrypt it."""
    target = Path(path) if path is not None else (existing_secrets_path() or active_secrets_path())
    if not target.is_file() or target.stat().st_size > _MAX_SECRETS_FILE_BYTES:
        return False
    try:
        payload = json.loads(target.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and payload.get("format") == FORMAT


def validate_first_run_secrets_state() -> None:
    """Ensure first-run setup cannot orphan or overwrite existing credentials."""
    target = existing_secrets_path() or active_secrets_path()
    if not target.is_file():
        return
    if encrypted_secrets_exist(target):
        raise ValueError(
            "Encrypted Local Credentials already exist but the local access verifier is missing. "
            f"To prevent credential loss, Infraredshift will not overwrite {target}. Restore auth.json "
            "from this Windows user profile or have an administrator preserve and reset the credential file."
        )
    if target.stat().st_size > _MAX_SECRETS_FILE_BYTES:
        raise ValueError("The existing Local Credentials file is too large to migrate safely.")
    # A supported legacy plaintext file may be migrated after the new access
    # verifier is safely created. Validate its shape first so setup cannot
    # leave auth.json created beside an unreadable credential file.
    _parse_plaintext(target.read_text(encoding="utf-8-sig"), str(target))


def _credential_entropy(access_code: str, pin: str) -> bytes:
    material = (str(access_code).strip() + "\0" + str(pin).strip()).encode("utf-8")
    return hashlib.sha256(b"DataBasix-secrets-v2\0" + material).digest()


def _blob(data: bytes) -> tuple[_DATA_BLOB, ctypes.Array]:
    buffer = ctypes.create_string_buffer(data, max(1, len(data)))
    blob = _DATA_BLOB(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
    return blob, buffer


def _dpapi_protect(cleartext: bytes, entropy: bytes | None = None) -> bytes:
    if os.name != "nt":
        raise RuntimeError("Infraredshift .secrets encryption requires Windows DPAPI.")
    crypt32, kernel32 = _dpapi_functions()
    source, source_buffer = _blob(cleartext)
    extra = extra_buffer = None
    if entropy:
        extra, extra_buffer = _blob(entropy)
    output = _DATA_BLOB()
    if not crypt32.CryptProtectData(
        ctypes.byref(source),
        "Infraredshift Redshift credentials",
        ctypes.byref(extra) if extra is not None else None,
        None,
        None,
        0x1,  # CRYPTPROTECT_UI_FORBIDDEN
        ctypes.byref(output),
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        if output.pbData:
            kernel32.LocalFree(ctypes.cast(output.pbData, ctypes.c_void_p))
        del source_buffer, extra_buffer


def _dpapi_unprotect(ciphertext: bytes, entropy: bytes | None = None) -> bytes:
    if os.name != "nt":
        raise RuntimeError("Infraredshift .secrets encryption requires Windows DPAPI.")
    crypt32, kernel32 = _dpapi_functions()
    source, source_buffer = _blob(ciphertext)
    extra = extra_buffer = None
    if entropy:
        extra, extra_buffer = _blob(entropy)
    output = _DATA_BLOB()
    description = wintypes.LPWSTR()
    if not crypt32.CryptUnprotectData(
        ctypes.byref(source),
        ctypes.byref(description),
        ctypes.byref(extra) if extra is not None else None,
        None,
        None,
        0x1,
        ctypes.byref(output),
    ):
        raise ValueError("The Windows user or Infraredshift credentials could not unlock this .secrets file.")
    try:
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        if output.pbData:
            kernel32.LocalFree(ctypes.cast(output.pbData, ctypes.c_void_p))
        # Windows allocates the description string too; it must be freed.
        description_pointer = ctypes.cast(description, ctypes.c_void_p)
        if description_pointer.value:
            kernel32.LocalFree(description_pointer)
        del source_buffer, extra_buffer


def _clean_values(values: Mapping[str, str]) -> dict[str, str]:
    return {
        str(key).strip().upper(): str(value)
        for key, value in values.items()
        if is_secret_key(str(key))
    }


def _gate_hash(access_code: str, pin: str, salt: str) -> str:
    material = (str(access_code).strip() + "\0" + str(pin).strip()).encode("utf-8")
    return hashlib.pbkdf2_hmac(
        "sha256", material, bytes.fromhex(salt), _GATE_ITERATIONS
    ).hex()


def _serialize(values: Mapping[str, str], access_code: str, pin: str) -> bytes:
    clean = {
        str(key).strip().upper(): str(value)
        for key, value in values.items()
        if is_secret_key(str(key))
    }
    salt = secrets.token_hex(16)
    envelope = {
        "values": clean,
        "gate": {
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": _GATE_ITERATIONS,
            "salt": salt,
            "hash": _gate_hash(access_code, pin, salt),
        },
    }
    return json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")


def encrypt_values(values: Mapping[str, str], access_code: str, pin: str) -> str:
    # v3 is DPAPI-current-user only so a scheduled loader running under the
    # same Windows identity can decrypt unattended.  Access code/PIN remain a
    # GUI authorization gate for viewing and editing, not cryptographic
    # material needed by Task Scheduler.
    ciphertext = _dpapi_protect(_serialize(values, access_code, pin))
    return json.dumps(
        {
            "format": FORMAT,
            "version": VERSION,
            "algorithm": ALGORITHM,
            "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        },
        indent=2,
    ) + "\n"


def _decrypt_payload(
    payload: Mapping[str, object],
    access_code: str,
    pin: str,
    *,
    enforce_gate: bool = True,
) -> dict[str, str]:
    if not isinstance(payload, Mapping):
        raise ValueError("The .secrets file is not a recognized Infraredshift encrypted credential file.")
    if set(payload) != {"format", "version", "algorithm", "ciphertext"}:
        raise ValueError("The .secrets file contains unsupported or missing fields.")
    version = payload.get("version")
    algorithm = payload.get("algorithm")
    if payload.get("format") != FORMAT or (version, algorithm) not in {
        (VERSION, ALGORITHM),
        (LEGACY_VERSION, LEGACY_ALGORITHM),
    }:
        raise ValueError("The .secrets file is not a recognized Infraredshift encrypted credential file.")
    encoded = payload.get("ciphertext")
    if not isinstance(encoded, str) or not encoded or len(encoded) > _MAX_SECRETS_FILE_BYTES:
        raise ValueError("The .secrets ciphertext is invalid.")
    try:
        ciphertext = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError("The .secrets ciphertext is invalid.") from exc
    entropy = _credential_entropy(access_code, pin) if version == LEGACY_VERSION else None
    clear = _dpapi_unprotect(ciphertext, entropy)
    if len(clear) > _MAX_SECRETS_FILE_BYTES:
        raise ValueError("The decrypted .secrets payload is too large.")
    decoded = json.loads(clear.decode("utf-8"))
    if version == VERSION:
        if not isinstance(decoded, dict) or set(decoded) != {"values", "gate"}:
            raise ValueError("The decrypted .secrets payload has an invalid v3 envelope.")
        gate = decoded.get("gate")
        if not isinstance(gate, dict) or set(gate) != {"kdf", "iterations", "salt", "hash"}:
            raise ValueError("The decrypted .secrets credential gate is invalid.")
        if gate.get("kdf") != "PBKDF2-HMAC-SHA256" or gate.get("iterations") != _GATE_ITERATIONS:
            raise ValueError("The decrypted .secrets credential gate policy is invalid.")
        salt = str(gate.get("salt") or "")
        expected = str(gate.get("hash") or "")
        if len(salt) != 32 or len(expected) != 64:
            raise ValueError("The decrypted .secrets credential gate is invalid.")
        try:
            bytes.fromhex(salt)
        except ValueError as exc:
            raise ValueError("The decrypted .secrets credential gate is invalid.") from exc
        if enforce_gate and not secrets.compare_digest(
            _gate_hash(access_code, pin, salt), expected
        ):
            raise ValueError("The Infraredshift access code or PIN could not unlock this .secrets file.")
        values = decoded.get("values")
    else:
        values = decoded
    if not isinstance(values, dict) or any(not is_secret_key(key) for key in values):
        raise ValueError("The decrypted .secrets payload contains unsupported keys.")
    return {str(key): str(value) for key, value in values.items()}


def read_encrypted_secrets(path: str | os.PathLike, access_code: str, pin: str) -> dict[str, str]:
    target = Path(path)
    if target.stat().st_size > _MAX_SECRETS_FILE_BYTES:
        raise ValueError("The .secrets file is too large.")
    payload = json.loads(target.read_text(encoding="utf-8-sig"))
    return _decrypt_payload(payload, access_code, pin)


def write_encrypted_secrets(
    path: str | os.PathLike,
    values: Mapping[str, str],
    access_code: str,
    pin: str,
) -> Path:
    unsupported = sorted(str(key) for key in values if not is_secret_key(str(key)))
    if unsupported:
        raise ValueError(
            "Unsupported key(s) in encrypted Local Credentials: " + ", ".join(unsupported)
        )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    rendered = encrypt_values(values, access_code, pin)
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    # Fail closed if storage was truncated or corrupted during the write.
    if read_encrypted_secrets(target, access_code, pin) != {
        str(key).strip().upper(): str(value)
        for key, value in values.items()
        if is_secret_key(str(key))
    }:
        raise OSError("Encrypted credential verification failed after write.")
    return target


def _parse_plaintext(text: str, source: str) -> dict[str, str]:
    from .ingest_redshift import parse_dotenv_text

    try:
        values = parse_dotenv_text(text, source=source)
    except SystemExit as exc:
        raise ValueError(str(exc)) from exc
    unsupported = sorted(key for key in values if not is_secret_key(key))
    if unsupported:
        raise ValueError(
            "Only server address, username, and password keys belong in .secrets: "
            + ", ".join(unsupported)
        )
    return values


def _remove_secret_lines(text: str) -> str:
    output: list[str] = []
    for raw in str(text).splitlines():
        match = re.match(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=", raw)
        if match and is_secret_key(match.group(1)):
            continue
        output.append(raw)
    return "\n".join(output).rstrip() + "\n"


def unlock_secrets_session(access_code: str, pin: str) -> dict[str, str]:
    """Unlock connection values and migrate old plaintext .env secrets."""
    global _SESSION_VALUES
    from .ingest_redshift import dotenv_candidates, parse_dotenv

    env_paths = dotenv_candidates(None)
    env_path = next((path for path in env_paths if path.is_file()), env_paths[0])
    source_secret_path = existing_secrets_path()
    secret_path = source_secret_path or active_secrets_path()
    destination_secret_path = active_secrets_path()
    values: dict[str, str] = {}
    rewrite_secrets = False

    if secret_path.is_file():
        raw = secret_path.read_text(encoding="utf-8-sig")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            values.update(_parse_plaintext(raw, str(secret_path)))
            rewrite_secrets = True
        else:
            values.update(_decrypt_payload(payload, access_code, pin))
            if payload.get("version") == LEGACY_VERSION:
                rewrite_secrets = True
        if secret_path.resolve(strict=False) != destination_secret_path.resolve(strict=False):
            rewrite_secrets = True

    cleaned_env: str | None = None
    if env_path.is_file():
        env_values = parse_dotenv(env_path)
        migrated = {name: value for name, value in env_values.items() if is_secret_key(name)}
        if migrated:
            # An existing encrypted value is authoritative.  A stale
            # plaintext file beside the launcher must never override it.
            for name, value in migrated.items():
                values.setdefault(name, value)
            rewrite_secrets = True
            cleaned_env = _remove_secret_lines(env_path.read_text(encoding="utf-8-sig"))

    if values and (rewrite_secrets or not destination_secret_path.is_file()):
        write_encrypted_secrets(destination_secret_path, values, access_code, pin)
        if (
            source_secret_path is not None
            and source_secret_path.resolve(strict=False)
            != destination_secret_path.resolve(strict=False)
        ):
            try:
                source_secret_path.unlink()
            except OSError as exc:
                # The verified per-user file is authoritative. A locked-down
                # read-only install may retain the obsolete DPAPI ciphertext,
                # but it is never selected ahead of the per-user copy again.
                # Say so, instead of silently leaving two files behind.
                print(
                    f"Infraredshift: could not remove the migrated secrets file "
                    f"{source_secret_path} ({exc}); the per-user copy is authoritative.",
                    file=sys.stderr,
                )

    # Remove legacy plaintext only after the encrypted file has been written
    # and read back successfully.  A failed migration must never destroy the
    # only credential copy.
    if cleaned_env is not None:
        temporary_env = env_path.with_name(env_path.name + ".tmp")
        with temporary_env.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(cleaned_env)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_env, env_path)

    with _SESSION_LOCK:
        _SESSION_VALUES = dict(values)
    return dict(values)


def load_session_secrets_into_environment() -> dict[str, str]:
    """Compatibility shim: return secrets without copying them to ``os.environ``.

    Environment variables are inherited by child processes and routinely end
    up in diagnostics.  Callers must use :func:`session_secret` instead.
    """
    return session_secrets()


def unlock_scheduled_secrets_session(
    path: str | os.PathLike | None = None,
) -> dict[str, str]:
    """Unlock v3 credentials for an unattended same-user loader process.

    This function never accepts credentials from command-line arguments and
    never exports decrypted values to ``os.environ``.  Legacy v2 files remain
    login-gated and are upgraded to v3 after the next successful GUI login.
    """
    global _SESSION_VALUES
    target = Path(path) if path is not None else active_secrets_path()
    if not target.is_file():
        raise FileNotFoundError(
            "No encrypted Infraredshift Local Credentials were found for this Windows user."
        )
    if target.stat().st_size > _MAX_SECRETS_FILE_BYTES:
        raise ValueError("The .secrets file is too large.")
    payload = json.loads(target.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or payload.get("version") != VERSION:
        raise ValueError(
            "Scheduled loading requires v3 Local Credentials. Open Infraredshift and sign in once "
            "to migrate the existing credential file."
        )
    values = _decrypt_payload(payload, "", "", enforce_gate=False)
    with _SESSION_LOCK:
        _SESSION_VALUES = dict(values)
    return dict(values)


def session_secrets() -> dict[str, str]:
    with _SESSION_LOCK:
        return dict(_SESSION_VALUES)


def session_secret(name: str, default: str | None = None) -> str | None:
    key = str(name or "").strip().upper()
    with _SESSION_LOCK:
        return _SESSION_VALUES.get(key, default)


def set_session_secret(name: str, value: str) -> None:
    """Set one validated credential in memory without environment export."""
    key = str(name or "").strip().upper()
    if not is_secret_key(key):
        raise ValueError(f"Unsupported Local Credentials key {key!r}.")
    secret = str(value or "")
    if not secret:
        raise ValueError(f"Local Credentials value for {key} cannot be blank.")
    with _SESSION_LOCK:
        _SESSION_VALUES[key] = secret


def clear_session_secrets() -> None:
    global _SESSION_VALUES
    with _SESSION_LOCK:
        _SESSION_VALUES.clear()
        _SESSION_VALUES = {}


def redact_sensitive_text(value: object) -> str:
    """Return an exception/log string with credentials and URL auth removed."""
    text = str(value)
    with _SESSION_LOCK:
        known = tuple(_SESSION_VALUES.values())
    for secret in sorted((item for item in known if item), key=len, reverse=True):
        text = text.replace(secret, "<redacted>")
    text = re.sub(
        r"(?i)(\b(?:password|passwd|pwd|user(?:name)?)\s*[=:]\s*)([^\s,;]+)",
        r"\1<redacted>",
        text,
    )
    text = re.sub(r"(?i)(://[^\s:/@]+:)[^\s@/]+@", r"\1<redacted>@", text)
    return text


def plaintext_editor_text(values: Mapping[str, str]) -> str:
    lines = [
        "# Infraredshift encrypted connection credentials",
        "# Only HOST, USER, and PASSWORD values are permitted.",
        "",
    ]
    for key in sorted(values):
        if is_secret_key(key):
            lines.append(f"{key}={values[key]}")
    return "\n".join(lines).rstrip() + "\n"


def parse_editor_text(text: str) -> dict[str, str]:
    return _parse_plaintext(text, "encrypted credential editor")
