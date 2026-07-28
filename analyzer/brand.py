"""Product naming for Infraredshift.

User-facing strings and email-safe launcher names live here so renames stay
consistent. Crypto salts / secret format ids intentionally stay on their
historical values in secrets_store so existing .secrets files still decrypt.

Renamed from DataBa6ix. Three things deliberately did NOT change, because
changing them would strand an existing install:
  * the DPAPI szDataDescr in secrets_store (a display label, but paired with
    files already on disk),
  * LEGACY_APP_STATE_FOLDERS, which still lists the old folder so settings and
    credentials are found after an upgrade,
  * LEGACY_LAUNCHER_NAMES, so old kits keep launching.
"""
from __future__ import annotations

# Short brand shown in the window title, docs, and messages.
PRODUCT_NAME = "Infraredshift"
PRODUCT_NAME_UPPER = "INFRAREDSHIFT"
PRODUCT_NAME_LOWER = "infraredshift"

# Logo asset filenames under analyzer/assets/
LOGO_WORDMARK = "databa6ix_logo.png"
LOGO_MARK = "databa6ix_mark.png"
LOGO_STARTUP_BANNER = "databa6ix_startup_banner.png"
# Vector wordmark. SVG rather than PNG so it stays sharp at any panel width,
# and it is 2.8 KB against the 1.06 MB PNG it replaces.
LOGO_LOGIN = "infraredshift-logo.svg"
# The login mark is a wide banner (1600x420, ~3.81:1), not the near-square
# 1.50:1 shape it replaced - see _LOGIN_LOGO_WIDTH_RATIO in login_dialog.
LOGO_LOGIN_ASPECT = 1600 / 420

# Windows launcher file names (email-safe twin uses .txt).
LAUNCHER_BASENAME = "Infraredshift"
LAUNCHER_PY = f"{LAUNCHER_BASENAME}.py"
LAUNCHER_TXT = f"{LAUNCHER_BASENAME}.txt"

# Legacy launcher names still accepted by setup scripts (older kits shipped
# these; keep recognizing them so an existing install keeps working).
LEGACY_LAUNCHER_NAMES = (
    "DataBa6ix.py", "DataBa6ix.txt",
    "Databas6ix.py", "Databas6ix.txt", "databa6ix.py",
    "redshift_analyzer_text.py", "redshift_analyzer_text.txt",
)
LEGACY_LAUNCHER_TEXT_TXT = "redshift_analyzer_text.txt"
LEGACY_LAUNCHER_TEXT_PY = "redshift_analyzer_text.py"
LEGACY_LAUNCHER_FAT_TXT = "redshift_analyzer_fat.txt"
LEGACY_LAUNCHER_FAT_PY = "redshift_analyzer_fat.py"

# Subtitle under the brand mark.
PRODUCT_TAGLINE = "Physical Design Intelligence"
WINDOW_TITLE = f"{PRODUCT_NAME} — {PRODUCT_TAGLINE}"

# Per-user state folder under %LOCALAPPDATA% (new installs).
APP_STATE_FOLDER = "Infraredshift"
# Prior product folders; still accepted so upgrades keep settings/secrets.
# DataBa6ix must stay first - an existing install has its saved credentials and
# settings there, and dropping it would silently strand them.
LEGACY_APP_STATE_FOLDERS = (
    "DataBa6ix",
    "Databa6ix",
    "RedshiftQueryAnatomy",
    "DataBasix",
    "Databasix",
)

# Where the DuckDB warehouse lives, under the user profile.
# ~/Infraredshift/data for a new install; ~/RQP/data is still adopted when it
# already holds a warehouse, so an existing corporate deployment keeps its
# captured data without being told to move anything.
DATA_PARTS = ("Infraredshift", "data")
LEGACY_DATA_PARTS = ("RQP", "data")

PORTABLE_PROFILE_FILENAME = "redshift_cluster_profiles.json"

LOADER_PROG = "infraredshift_loader"
LOADER_EVENT_PREFIX = "INFRAREDSHIFT_EVENT"
