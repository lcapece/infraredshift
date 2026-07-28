# Databa6ix - New Windows Machine

## Required files

Copy these files into one folder on the destination computer.

**Email-safe delivery (preferred):** corporate mail filters often block `.py`
attachments. Ship the `.txt` launcher instead — it is the same Python program.

- `Databas6ix.txt` - desktop application (**email-safe; preferred**)
- `redshift_analyzer_requirements.txt` - Python dependencies
- `setup_new_machine.cmd` - Command Prompt dependency and empty-schema setup
  (also available as `setup_new_machine_cmd.txt` if `.cmd` is filtered)
- `redshift_cluster_profiles.json` - portable non-secret cluster configuration exported from Settings

Optional twins (local/dev only; not required for email handoff):

- `Databas6ix.py` - identical to the `.txt` launcher
- `redshift_analyzer_fat.txt` / `.py` - smaller zip-embedded launcher

The application contains the recoverable general loader, including the optional
external-table final stage. Do not distribute the old standalone runner or
focused external loader as part of a normal installation.

The `.env` and portable JSON contain only non-secret configuration and may be
transported. Do not copy `.secrets`; each teammate creates encrypted Local
Credentials for their own Windows account inside Databa6ix.

The portable JSON is specifically designed to travel with the application. It
contains cluster friendly names, namespace IDs, ports, database entry points,
and enabled/disabled selections. Every enabled cluster requires
its actual namespace ID. Accessible database lists are discovered from
`SVV_REDSHIFT_DATABASES` separately for each cluster during cycle loading, restricted to `database_type = 'local'`. The file never contains a Redshift username
address, username, or password. Before copying the application, use **Settings -> Data Sources ->
Export Portable Configuration**. Keep the resulting JSON beside the application
on the destination computer; it loads automatically.

## Platform and dependencies

- Windows 10 or Windows 11, 64-bit
- 64-bit CPython 3.12 recommended; Python 3.9 or newer is supported by the loader
- PySide6
- pandas
- NumPy
- DuckDB Python package
- Amazon Redshift Connector for Python
- sqlglot
- RapidFuzz
- Network/DNS access from the laptop to each enabled Redshift endpoint and port
- A Redshift account authorized to read the required SYS/SVV/PG metadata views

DuckDB CLI, a DuckDB Windows service, Java, and a JDBC driver are not required
for native Redshift Connector mode.

Optional JDBC mode additionally requires Java, the Redshift JDBC JAR,
`JayDeBeApi`, and `JPype1`.

## Setup from Command Prompt

Open **Command Prompt** in the copied application folder and run:

```bat
setup_new_machine.cmd
```

After setup, launch the application. First run asks the user to create and
confirm a local access code and PIN; there are no built-in credentials. Open
**Settings -> Data Sources -> Edit Local Credentials** and enter each cluster's
server address, username, and password. Databa6ix stores them in a per-user,
Windows-DPAPI encrypted `.secrets` file outside the application folder.

Equivalent manual commands:

```bat
python -m pip install --user -r .\redshift_analyzer_requirements.txt
mkdir "%USERPROFILE%\RQP\data"
python .\Databas6ix.txt --index-duckdb --duckdb-path "%USERPROFILE%\RQP\data\redshift.duckdb"
```

## How DuckDB is created

DuckDB is embedded in the Python `duckdb` package. There is no separate server
or executable to install. On the first application connection:

1. `duckdb.connect(path)` creates the `.duckdb` file if it is absent.
2. `DuckDBStore.install_schema()` creates every metadata and captured-data table.
3. Missing columns are added for application upgrades.
4. Legacy rows without a namespace are assigned the producer namespace.
5. Analytics views are created or replaced from the application schema bundle.
6. Performance indexes are created by the setup command or rebuilt from Settings.

The default transferable data path is:

```text
C:\Users\<WindowsUser>\RQP\data\redshift.duckdb
```

The Data Loader may also create a new DuckDB file at `REDSHIFT_DUCKDB_PATH`.
It loads into `*_tmp` staging tables, checkpoints every completed table, and
keeps live tables unchanged until **Review Complete — Promote**. Opening the
application installs or upgrades analytics views automatically.

## Existing database versus new database

- To preserve existing telemetry, copy `redshift.duckdb` and its relevant
  backup files to the same data folder. The app upgrades its schema in place.
- For a clean installation, do not copy a database. Run the setup script to
  create an empty schema, configure clusters inside the authenticated app, and
  use **Topology -> Open Data Loader -> Start Safe Load**.

## Unattended daily loading

After the user has signed in once and saved Local Credentials, Windows Task
Scheduler can run the same loader used by the GUI under that Windows account:

```bat
python "C:\Path\To\Databas6ix.txt" --loader refresh --duckdb-path "%USERPROFILE%\RQP\data\redshift.duckdb" --promote --external-timeout-action skip --json-events
```

Use **Run only when user is logged on** or **Run whether user is logged on or
not** with the same Windows identity. No password appears in the task command.
The OS lock prevents simultaneous GUI and scheduled loads; a rerun resumes
completed table checkpoints.

## Local files

- Application data: `%USERPROFILE%\RQP\data\redshift.duckdb`
- Settings, sign-in verifier, and encrypted credentials: `%LOCALAPPDATA%\RedshiftQueryAnatomy\`
- Demonstration-safe non-secret configuration: `.env` beside the launcher
- Portable non-secret cluster profiles: `redshift_cluster_profiles.json` beside the launcher
- Extracted single-file application source: the Windows temporary directory;
  this is regenerated from the packaged launcher content and is not the data store

