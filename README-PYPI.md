# Infraredshift

Amazon Redshift workload triage. It captures SYS/SVV telemetry from your
producer and consumer clusters into a local DuckDB file, groups queries that
repeat with different literals into one pattern, and ranks physical-design
fixes by the total time they actually cost you.

The point is triage. A cluster running fifteen thousand slow queries does not
have fifteen thousand problems — it usually has a few dozen query *shapes* run
over and over. Infraredshift finds the shapes, totals their real cost, and
tells you which ones are worth fixing first.

Everything runs locally. Your SQL, your telemetry, and your credentials never
leave the machine.

---

## Install

```bash
pip install infraredshift
```

To capture live from Redshift, add the connector:

```bash
pip install "infraredshift[redshift]"
```

On Windows/ARM, where the native connector has no wheel, use the JDBC path:

```bash
pip install "infraredshift[jdbc]"
```

Python 3.10+.

**No executables are installed.** The wheel is pure Python (`py3-none-any`) and
contains no `.exe`, `.dll` or compiled extension of any kind. Deliberately, it
also installs no console-script shims — on Windows pip generates those as
`.exe` files (the same mechanism behind `pip.exe`), drops them in a directory
that is often not on `PATH`, and warns about it. On a locked-down laptop an
unexplained `.exe` is a question you have to answer, so there are none.

Everything runs as a module:

```bash
python -m analyzer                  # the desktop app
python -m analyzer.loader           # the loader CLI
python -m analyzer.ingest_redshift  # direct capture
```

---

## First run

```bash
python -m analyzer
```

The app opens on a local sign-in screen. **This is not a Redshift login** — it
creates an access code and PIN that encrypt your Redshift credentials at rest
on this machine, using Windows DPAPI scoped to your Windows user. Nobody else
signing in to this machine can decrypt them, and they are never written to a
`.env` file or exported into environment variables.

There is nothing to configure before this step. If you have no data yet, the
app opens with empty screens and tells you what to load.

### Getting data in

You need three things: a cluster profile, credentials, and a load.

**1. Describe your clusters.** Create `redshift_cluster_profiles.json` next to
the app, or point at one with `REDSHIFT_ANALYZER_PROFILE_PATH`:

```json
{
  "format": "redshift-query-anatomy-cluster-profiles",
  "version": 1,
  "profiles": [
    {
      "profile": "REDSHIFT_PRODUCER",
      "display_name": "Producer",
      "namespace_id": "your-producer-namespace-uuid",
      "port": "5439",
      "primary_database": "dev",
      "floor_seconds": "300"
    },
    {
      "profile": "REDSHIFT_CONSUMER_1",
      "display_name": "Reporting",
      "namespace_id": "your-consumer-namespace-uuid",
      "port": "5439",
      "primary_database": "dev",
      "floor_seconds": "30"
    }
  ]
}
```

`floor_seconds` is the capture threshold for that cluster — queries faster than
this are not captured. A busy producer earns a high floor (300s); a quieter
consumer can afford 30s. This file holds **no credentials**, so it is safe to
share with teammates.

**2. Enter credentials** in the app under Local Credentials. They are encrypted
immediately.

**3. Load.** Use the Data Loader tab, or:

```bash
python -m analyzer.loader refresh --days 7
```

The first load is the slow one. After that, loads are incremental.

**No cluster handy?** The app ships a mock warehouse, so you can open every
screen and see real-shaped data before connecting anything.

---

## What you get

**Workload Triage** — one bubble per repeat pattern. Horizontal axis is how
often it runs, vertical is what you choose (runtime, rows, spill). Big bubble
in the upper right is where your time is going. Each pattern carries a verdict:
fix the query, fix the tables, or leave it alone.

**Table Heat Map** — every table as a square, coloured by distribution and sort
health. Missing DISTKEY, unsorted, stale statistics. Spectrum tables get their
own view coloured by whether a partition key exists.

**Query Decomposer** — takes one slow query and splits it into the stages it
actually runs, so you can see which join or scan is the expensive one.

**Behind Views** — the base tables a query really touches once views are
expanded, which is usually more than the SQL suggests.

---

## Commands

| Command | Does |
|---|---|
| `python -m analyzer` | Open the desktop app |
| `python -m analyzer.loader refresh --days 7` | Incremental capture |
| `python -m analyzer.ingest_redshift --external-tables` | Spectrum/external metadata (opt-in) |

### Large Spectrum catalogs

`SVV_EXTERNAL_COLUMNS` returns **one row per column**, so a catalog with
millions of external tables is tens of millions of rows. If yours is large,
restrict the capture in the producer profile:

```json
"external_schemas": "curated, analytics",
"external_table_patterns": "fact_*, dim_*"
```

The filter runs on Redshift, so the rows never cross the wire. `*` and `?` are
the wildcards; a literal `_` in a table name means itself.

---

## Privacy

Analysis is local. The app makes no outbound calls except to your own Redshift
clusters. Captured SQL, telemetry, and credentials stay in your DuckDB file and
your DPAPI-encrypted secrets file on this machine.

---

## License

MIT. See [LICENSE](https://github.com/lcapece/redshift/blob/main/LICENSE).
