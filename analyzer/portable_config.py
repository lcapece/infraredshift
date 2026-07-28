"""Portable non-secret Redshift cluster profile configuration."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re

from .runtime_paths import (
    PORTABLE_PROFILE_FILENAME,
    configuration_candidates,
    resolve_runtime_paths,
)


PORTABLE_FILENAME = PORTABLE_PROFILE_FILENAME
ACTIVE_PROFILE_PREFIXES_ENV = "INFRAREDSHIFT_ACTIVE_PROFILE_PREFIXES"
PROFILE_FIELDS = (
    "ENABLED",
    "DISPLAY_NAME",
    "NAMESPACE_ID",
    "PORT",
    "PRIMARY_DATABASE",
    # Per-cluster sys_query_history runtime floor in seconds. Optional; when
    # omitted the load's --floor-seconds default applies.
    "FLOOR_SECONDS",
)
LEGACY_PRODUCER_FIELDS = {
    "ENABLED": ("REDSHIFT_ENABLED",),
    "DISPLAY_NAME": ("REDSHIFT_FRIENDLY", "REDSHIFT_ENV"),
    "NAMESPACE_ID": ("REDSHIFT_NAMESPACE", "REDSHIFT_NAMESPACE_ID"),
    "PORT": ("REDSHIFT_PORT",),
    "PRIMARY_DATABASE": ("REDSHIFT_PRIMARY_DATABASE", "REDSHIFT_DATABASE"),
}


def _profile_prefix_from_key(key: object) -> str:
    match = re.match(
        r"^(REDSHIFT_(?:PRODUCER|CONSUMER_\d+))_",
        str(key).strip().upper(),
    )
    return match.group(1) if match else ""


def active_profile_prefixes(
    environment: os._Environ[str] | dict[str, str] | None = None,
) -> tuple[str, ...] | None:
    """Return the portable manifest's exact profile membership, when loaded.

    ``None`` means no portable manifest has been applied and legacy
    environment/secret discovery remains available. An empty tuple means a
    manifest was loaded with no profiles.
    """
    source = environment if environment is not None else os.environ
    if ACTIVE_PROFILE_PREFIXES_ENV not in source:
        return None
    raw = str(source.get(ACTIVE_PROFILE_PREFIXES_ENV) or "")
    result: list[str] = []
    for value in raw.split(","):
        prefix = value.strip().upper()
        if prefix == "REDSHIFT_PRODUCER" or re.fullmatch(
            r"REDSHIFT_CONSUMER_\d+", prefix
        ):
            if prefix not in result:
                result.append(prefix)
    return tuple(result)


def apply_portable_environment(
    values: dict[str, str],
    environment: os._Environ[str] | dict[str, str] | None = None,
) -> tuple[str, ...]:
    """Apply portable identity as the authoritative profile manifest.

    Credentials remain in the protected local secret store. The manifest
    controls which profile prefixes are active and its display names and
    enabled flags override stale non-secret values from an older ``.env``.
    """
    target = environment if environment is not None else os.environ
    prefixes: list[str] = []
    for key in values:
        prefix = _profile_prefix_from_key(key)
        if prefix and prefix not in prefixes:
            prefixes.append(prefix)
    target[ACTIVE_PROFILE_PREFIXES_ENV] = ",".join(prefixes)
    for key, value in values.items():
        target[str(key)] = str(value)
    return tuple(prefixes)


def profile_prefixes(environment: dict[str, str] | None = None) -> tuple[str, ...]:
    source = environment if environment is not None else os.environ
    ordinals = {
        int(match.group(1))
        for key in source
        if (match := re.match(r"^REDSHIFT_CONSUMER_(\d+)_", str(key).upper()))
    }
    # Preserve the original seven editable slots while accepting any larger
    # numbered consumer profile present in configuration.
    ordinals.update(range(1, 8))
    return ("REDSHIFT_PRODUCER",) + tuple(f"REDSHIFT_CONSUMER_{number}" for number in sorted(ordinals))


def portable_config_candidates(
    environment: os._Environ[str] | dict[str, str] | None = None,
    *,
    include_legacy_cwd: bool = True,
) -> tuple[Path, ...]:
    return configuration_candidates(
        PORTABLE_FILENAME,
        environment=environment,
        include_legacy_cwd=include_legacy_cwd,
    )


def default_portable_config_path(
    environment: os._Environ[str] | dict[str, str] | None = None,
) -> Path:
    """Team-shareable profile location beside the installed launcher."""
    return resolve_runtime_paths(environment).portable_profile_file


def _lookup_profile_field(source: dict[str, str], prefix: str, field: str) -> str:
    value = str(source.get(f"{prefix}_{field}", "") or "").strip()
    if value:
        return value
    if prefix == "REDSHIFT_PRODUCER":
        for legacy_key in LEGACY_PRODUCER_FIELDS.get(field, ()):
            legacy = str(source.get(legacy_key, "") or "").strip()
            if legacy:
                return legacy
    return ""


_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_PLACEHOLDER_RE = re.compile(r"^(REPLACE|CHANGE|YOUR|TODO|XXX)[-_]", re.IGNORECASE)


def _validate_namespace_id(prefix: str, value: str) -> None:
    """Reject namespace ids that cannot be real.

    Two failures have been observed in the field, both silent and both costly:

    * the shipped template value (``REPLACE-PRODUCER-NAMESPACE-ID``) left
      unedited, and
    * a UUID pasted twice, producing a 72-character string.

    In each case the loader accepted the value and stored every row under it.
    The data was perfectly good, but the analysis scope filters on the real
    namespace, so whole clusters showed zero rows while their data sat right
    there. Failing loudly here converts hours of confusion into one clear
    message.

    Deliberately narrow: it rejects only values that are provably wrong. Short
    labels are legitimate (single-cluster installs where the id is ``producer``,
    Serverless workgroup names, test fixtures), so anything that is neither a
    placeholder nor a doubled UUID is allowed through. A validator that rejects
    valid configurations is worse than none, because it blocks working setups.
    """
    text = value.strip()
    if _PLACEHOLDER_RE.match(text):
        raise ValueError(
            f"{prefix}_NAMESPACE_ID is still the template placeholder {text!r}. "
            "Replace it with the cluster's real namespace id "
            "(run: SELECT current_namespace;)."
        )
    # A UUID pasted twice: 72 characters, second half identical to the first.
    if len(text) == 72 and _UUID_RE.match(text[:36]) and text[:36] == text[36:]:
        raise ValueError(
            f"{prefix}_NAMESPACE_ID contains the same UUID twice "
            f"({len(text)} characters). Use only the first 36: {text[:36]!r}."
        )


def _effective_enabled(source: dict[str, str], prefix: str, profile_fields: dict[str, str]) -> str:
    """Match GUI semantics: missing ENABLED defaults to on when the profile is configured.

    The Settings checkboxes show checked when host/namespace is present even if
    the ENABLED key was never written. Portable JSON must not export blank/null
    for that case — write an explicit true/false so handoff kits match the GUI.
    """
    raw = _lookup_profile_field(source, prefix, "ENABLED")
    if raw:
        return "true" if raw.lower() in {"1", "true", "yes", "y", "on", "checked"} else "false"
    # Configured = any non-secret identity present (host is secret and not in
    # the portable file; namespace / name / port / database still count).
    configured = any(
        str(profile_fields.get(name) or "").strip()
        for name in ("display_name", "namespace_id", "port", "primary_database")
    )
    # Also treat a secret host / legacy host as configured when exporting from
    # a full process environment (GUI export path).
    host = str(source.get(f"{prefix}_HOST") or (source.get("REDSHIFT_HOST") if prefix == "REDSHIFT_PRODUCER" else "") or "").strip()
    if host:
        configured = True
    return "true" if configured else "false"


def export_portable_config(path: str | os.PathLike, environment: dict[str, str] | None = None) -> Path:
    source = environment if environment is not None else os.environ
    # Normalize to plain dict so missing keys behave consistently.
    source = {str(key): str(value) for key, value in dict(source).items()}
    profiles: list[dict[str, str]] = []
    for prefix in profile_prefixes(source):
        profile = {"profile": prefix}
        for field in PROFILE_FIELDS:
            if field == "ENABLED":
                continue
            profile[field.lower()] = _lookup_profile_field(source, prefix, field)
        profile["enabled"] = _effective_enabled(source, prefix, profile)
        if any(profile.get(field.lower()) for field in PROFILE_FIELDS):
            profiles.append(profile)
    payload = {
        "format": "redshift-query-anatomy-cluster-profiles",
        "version": 1,
        "contains_credentials": False,
        "environment": str(source.get("REDSHIFT_ENV") or ""),
        "profiles": profiles,
    }
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2))
        handle.flush()
        os.fsync(handle.fileno())
    read_portable_environment(temporary)
    os.replace(temporary, target)
    return target


def read_portable_environment(path: str | os.PathLike) -> dict[str, str]:
    target = Path(path)
    if target.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("Portable cluster-profile file is unexpectedly large.")
    payload = json.loads(target.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("Unrecognized portable cluster-profile file.")
    if payload.get("format") != "redshift-query-anatomy-cluster-profiles" or payload.get("version") != 1:
        raise ValueError("Unrecognized portable cluster-profile file.")
    # Version-1 files created before this marker was added remain readable,
    # but an affirmative credential declaration is never accepted.
    if payload.get("contains_credentials") not in {None, False}:
        raise ValueError("Portable cluster-profile file cannot contain credentials.")
    _reject_credential_fields(payload)
    result: dict[str, str] = {}
    if payload.get("environment") is not None:
        environment = _safe_value("environment", payload.get("environment") or "", maximum=32)
        if environment and environment.upper() not in {"PROD", "QA", "DEV"}:
            raise ValueError("Portable environment must be PROD, QA, or DEV.")
        result["REDSHIFT_ENV"] = environment.upper()
    profiles = payload.get("profiles") or []
    if not isinstance(profiles, list):
        raise ValueError("Portable cluster profiles must be a list.")
    seen_profiles: set[str] = set()
    for profile in profiles:
        if not isinstance(profile, dict):
            raise ValueError("Every portable cluster profile must be a JSON object.")
        allowed = {"profile"} | {field.lower() for field in PROFILE_FIELDS}
        unknown = set(profile) - allowed
        if unknown:
            raise ValueError("Unsupported portable profile field(s): " + ", ".join(sorted(unknown)))
        prefix = str(profile.get("profile") or "").strip().upper()
        if prefix != "REDSHIFT_PRODUCER" and not re.fullmatch(r"REDSHIFT_CONSUMER_\d+", prefix):
            raise ValueError(f"Unsupported portable cluster profile {prefix!r}.")
        if prefix in seen_profiles:
            raise ValueError(f"Duplicate portable cluster profile {prefix!r}.")
        seen_profiles.add(prefix)
        for field in PROFILE_FIELDS:
            value = profile.get(field.lower())
            if value is not None:
                clean = _safe_value(field.lower(), value)
                if field == "ENABLED" and clean and clean.lower() not in {"true", "false", "1", "0", "yes", "no"}:
                    raise ValueError(f"{prefix}_ENABLED must be true or false.")
                if field == "PORT" and clean:
                    if not clean.isdecimal() or not (1 <= int(clean) <= 65535):
                        raise ValueError(f"{prefix}_PORT must be between 1 and 65535.")
                if field == "FLOOR_SECONDS" and clean:
                    try:
                        seconds = float(clean)
                    except ValueError:
                        raise ValueError(f"{prefix}_FLOOR_SECONDS must be a number of seconds.")
                    if not (0 <= seconds <= 86400):
                        raise ValueError(f"{prefix}_FLOOR_SECONDS must be between 0 and 86400.")
                if field == "NAMESPACE_ID" and clean:
                    _validate_namespace_id(prefix, clean)
                result[f"{prefix}_{field}"] = clean
    # Defense in depth: these keys are never accepted even if a hand-edited
    # portable file attempts to introduce them.
    return {
        key: value
        for key, value in result.items()
        if not key.endswith("_USER") and not key.endswith("_PASSWORD")
    }


def _safe_value(name: str, value: object, *, maximum: int = 2048) -> str:
    if not isinstance(value, (str, int, float, bool)):
        raise ValueError(f"Portable profile field {name!r} must be a scalar value.")
    clean = str(value).strip()
    if len(clean) > maximum or any(char in clean for char in ("\0", "\r", "\n")):
        raise ValueError(f"Portable profile field {name!r} contains an invalid value.")
    return clean


def _reject_credential_fields(value: object, path: str = "root") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            name = str(key).strip().lower()
            if name in {
                "user", "username", "password", "passwd", "pwd",
                "credential", "credentials", "secret", "secrets",
            }:
                raise ValueError(f"Credential field {path}.{key} is forbidden in portable configuration.")
            _reject_credential_fields(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_credential_fields(child, f"{path}[{index}]")
