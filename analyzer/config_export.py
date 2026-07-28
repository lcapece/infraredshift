"""Password-encrypted export of the cluster profile, for sharing with teammates.

Carries the non-secret cluster configuration ONLY - namespace ids, display
names, ports, primary database, floor_seconds, and the external-capture scope.

It deliberately does NOT carry Redshift credentials. Those are DPAPI-encrypted
per Windows user by design; putting them in a portable file would defeat that,
and anyone holding the file password would gain working database access. The
recipient supplies their own credentials.

Crypto is stdlib only (scrypt + HMAC-SHA256 in encrypt-then-MAC). Adding a
binary dependency to an air-gapped desktop tool costs more than it buys here:
the payload is a few hundred bytes of non-secret configuration whose threat
model is "do not email cluster identifiers in the clear", not "resist a funded
adversary".
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from typing import Any

FORMAT = "infraredshift-cluster-profile-export"
VERSION = 1

# scrypt cost. n=2**15 keeps an interactive unlock near a quarter second on a
# laptop while making offline guessing expensive per attempt.
_SCRYPT_N = 1 << 15
_SCRYPT_R = 8
_SCRYPT_P = 1
_KEY_LEN = 64  # 32 bytes cipher key + 32 bytes MAC key
_SALT_LEN = 16
_NONCE_LEN = 16

MIN_PASSWORD_LENGTH = 8

# Fields carried across. Anything not listed is dropped rather than passed
# through, so a future credential-bearing key cannot leak by accident.
EXPORTED_PROFILE_FIELDS = (
    "profile",
    "enabled",
    "display_name",
    "namespace_id",
    "port",
    "primary_database",
    "floor_seconds",
    "external_schemas",
    "external_table_patterns",
)

# Never exported, whatever the source document contains.
_FORBIDDEN_FIELDS = frozenset({
    "password", "passwd", "secret", "user", "username", "user_name",
    "access_code", "pin", "token", "credential", "credentials", "host",
})


class ConfigExportError(Exception):
    """Raised when an export cannot be produced or an import cannot be read."""


def _derive(password: str, salt: bytes) -> tuple[bytes, bytes]:
    material = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_KEY_LEN,
        maxmem=64 * 1024 * 1024,
    )
    return material[:32], material[32:]


def _keystream(key: bytes, nonce: bytes, length: int) -> bytes:
    """SHA256 counter-mode keystream.

    A stream built from the hash function already present beats importing a
    cipher library for a payload of this size and sensitivity. The nonce is
    random per export, so a key/nonce pair is never reused.
    """
    out = bytearray()
    counter = 0
    while len(out) < length:
        out += hashlib.sha256(key + nonce + counter.to_bytes(8, "big")).digest()
        counter += 1
    return bytes(out[:length])


def _sanitize_profile(profile: dict) -> dict:
    clean: dict[str, Any] = {}
    for field in EXPORTED_PROFILE_FIELDS:
        if field in profile and profile[field] is not None:
            clean[field] = str(profile[field])
    return clean


def build_payload(profiles_document: dict) -> dict:
    """Reduce a cluster-profiles document to the shareable fields."""
    profiles = profiles_document.get("profiles") or []
    exported = [_sanitize_profile(item) for item in profiles if isinstance(item, dict)]
    exported = [item for item in exported if item.get("profile")]
    if not exported:
        raise ConfigExportError("The profile document contains no cluster profiles.")

    payload = {
        "format": FORMAT,
        "version": VERSION,
        "contains_credentials": False,
        "environment": str(profiles_document.get("environment") or ""),
        "profiles": exported,
    }
    # Belt and braces: prove no forbidden key survived rather than trusting the
    # allowlist above to have been maintained.
    leaked = sorted(
        key
        for item in exported
        for key in item
        if key.lower() in _FORBIDDEN_FIELDS
    )
    if leaked:
        raise ConfigExportError(
            f"Refusing to export credential-bearing field(s): {', '.join(leaked)}"
        )
    return payload


def encrypt_document(profiles_document: dict, password: str) -> str:
    """Return the encrypted, shareable text for a cluster-profiles document."""
    if len(str(password or "")) < MIN_PASSWORD_LENGTH:
        raise ConfigExportError(
            f"The password must be at least {MIN_PASSWORD_LENGTH} characters."
        )
    payload = build_payload(profiles_document)
    plaintext = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    salt = secrets.token_bytes(_SALT_LEN)
    nonce = secrets.token_bytes(_NONCE_LEN)
    cipher_key, mac_key = _derive(password, salt)
    ciphertext = bytes(
        a ^ b for a, b in zip(plaintext, _keystream(cipher_key, nonce, len(plaintext)))
    )
    # Encrypt-then-MAC over everything that affects decryption, so a tampered
    # salt or nonce fails the tag rather than silently producing garbage.
    tag = hmac.new(mac_key, salt + nonce + ciphertext, hashlib.sha256).digest()

    envelope = {
        "format": FORMAT,
        "version": VERSION,
        "kdf": {"name": "scrypt", "n": _SCRYPT_N, "r": _SCRYPT_R, "p": _SCRYPT_P},
        "contains_credentials": False,
        "salt": base64.b64encode(salt).decode("ascii"),
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        "tag": base64.b64encode(tag).decode("ascii"),
    }
    return json.dumps(envelope, indent=2) + "\n"


def decrypt_document(text: str, password: str) -> dict:
    """Recover the cluster-profiles document from exported text."""
    try:
        envelope = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise ConfigExportError("This file is not a valid export file.") from exc
    if envelope.get("format") != FORMAT:
        raise ConfigExportError("This file is not an Infraredshift profile export.")

    try:
        salt = base64.b64decode(envelope["salt"])
        nonce = base64.b64decode(envelope["nonce"])
        ciphertext = base64.b64decode(envelope["ciphertext"])
        expected = base64.b64decode(envelope["tag"])
    except (KeyError, ValueError, TypeError) as exc:
        raise ConfigExportError("The export file is incomplete or corrupt.") from exc

    kdf = envelope.get("kdf") or {}
    cipher_key, mac_key = _derive_with(
        password,
        salt,
        int(kdf.get("n") or _SCRYPT_N),
        int(kdf.get("r") or _SCRYPT_R),
        int(kdf.get("p") or _SCRYPT_P),
    )
    actual = hmac.new(mac_key, salt + nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(actual, expected):
        # One message for both cases: a wrong password and a tampered file are
        # indistinguishable to the holder, and should stay that way.
        raise ConfigExportError(
            "Wrong password, or the file has been altered since it was exported."
        )

    plaintext = bytes(
        a ^ b for a, b in zip(ciphertext, _keystream(cipher_key, nonce, len(ciphertext)))
    )
    try:
        payload = json.loads(plaintext.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ConfigExportError("The export file could not be read.") from exc
    return payload


def _derive_with(password: str, salt: bytes, n: int, r: int, p: int) -> tuple[bytes, bytes]:
    material = hashlib.scrypt(
        password.encode("utf-8"), salt=salt, n=n, r=r, p=p,
        dklen=_KEY_LEN, maxmem=64 * 1024 * 1024,
    )
    return material[:32], material[32:]


def to_profiles_document(payload: dict) -> dict:
    """Turn a decrypted payload back into a redshift_cluster_profiles.json doc."""
    return {
        "format": "redshift-query-anatomy-cluster-profiles",
        "version": 1,
        "contains_credentials": False,
        "environment": str(payload.get("environment") or ""),
        "profiles": list(payload.get("profiles") or []),
    }
