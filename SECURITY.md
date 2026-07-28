# Security Policy

## Reporting a vulnerability

Please report security issues privately via
[GitHub Security Advisories](https://github.com/lcapece/infraredshift/security/advisories/new)
rather than opening a public issue.

Expect an acknowledgement within a few days. If the report is valid, a fix and
an advisory will follow; you will be credited unless you prefer otherwise.

## Automated scanning

Every push and pull request, plus a weekly schedule, runs:

| Scan | Tool | What it covers |
|---|---|---|
| Dependency CVEs | [`pip-audit`](https://github.com/pypa/pip-audit) | Declared dependencies against the [PyPI Advisory Database](https://github.com/pypa/advisory-database) |
| Static analysis | [CodeQL](https://codeql.github.com/) (`security-extended`) | Injection, path traversal, unsafe deserialization, and similar defects |
| Secret detection | [Gitleaks](https://github.com/gitleaks/gitleaks) | Credentials committed to the repository or its history |

The weekly run matters more than it looks: a CVE can be published against a
dependency long after you last changed it.

## Threat model

Understanding what this application does bounds most questions:

- **It runs on an analyst's desktop.** No service, no listener, no port.
- **Its only network connection is read-only, to your own Redshift clusters.**
  Every statement it issues is a `SELECT` against `SYS_*` / `SVV_*` /
  `PG_VIEWS` catalogs. It has no code path that reads a business table.
- **No data leaves the machine.** No telemetry, no update check, no AI or
  vendor endpoint.
- **Credentials are encrypted at rest** with Windows DPAPI, scoped to one
  Windows user, and never written to `.env` or environment variables.

The practical consequence: the usual web-application attack surface —
authentication bypass, injection through a request, SSRF, session handling —
largely does not exist here, because there is no server and no untrusted input
path. The realistic risks are dependency CVEs (scanned above) and a user being
tricked into opening a malicious `.duckdb` or config file.

## Handling captured SQL

Capturing `SYS_QUERY_TEXT` means the local DuckDB file stores **your SQL
statements**, which can embed literal values in predicates. That file is not
encrypted. Treat it with the same care as any extract of production query logs,
and note that copying it to a teammate copies that content.

The encrypted config export (**DuckDB Tools → Export Cluster Config**)
deliberately carries cluster identity and settings only — never credentials and
never query text.

## Supported versions

Fixes land on the latest release. This is early-stage software; there is no
long-term support branch yet.
