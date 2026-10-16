# Scheduled Data Pipeline With a Database and Failure Alerts

A small, complete example of the kind of system that has to keep running on its own: it pulls
several sources on a schedule, stores what it finds in a database without ever duplicating a
record, writes a status page you can read in the morning — and when a source breaks, it says so
loudly instead of failing quietly.

The interesting part is not the fetching. It is what happens on the bad day: retries, a recorded
attempt count, a structured alert, and a non-zero exit code so whatever runs the schedule
actually notifies a human.

## What it handles

- **Multi-source ingest from config, not code** — `sources/feeds.json` lists the sources, their id
  field and their retry budget; adding a source is a JSON edit.
- **Retry with exponential backoff** — a source is only marked failed after its configured attempts.
- **Row-level validation** — rows missing their id or name are skipped and counted, and a source that
  returns mostly unusable rows is treated as a failure rather than ingested.
- **Idempotent storage** — every write is keyed on `(source_id, external_id)` plus a payload hash, so
  running the pipeline twice changes nothing the second time.
- **Cross-source duplicates are made visible** — records are matched on a normalized name+city key;
  both rows are kept, the match is written to a `merges` table, and the record is flagged, so a wrong
  match can be undone instead of being lost.
- **Per-run bookkeeping** — `runs` and `feed_runs` record what each run did, source by source,
  including how many attempts each source needed.
- **Failure alerting** — a structured JSON line in `output/alerts.log` plus a non-zero exit code
  (`0` = all sources ok, `2` = at least one failure, `3` = every source failed).
- **A status page written on every run** — recent runs, records held per source, and how many
  duplicates were flagged.
- **Offline rehearsal mode** — the pipeline can be run and verified against local fixtures, so the
  failure path can be demonstrated without touching anyone's server.

## Run it

```bash
python runner.py --offline                              # rehearsal against local fixtures (default)
python runner.py --online                               # fetch the URLs configured in sources/feeds.json
python runner.py --offline --inject-failure feed_b       # rehearse the alert path
python verify_run.py                                     # re-read the database and check every number
```

Optional flags: `--db <path>`, `--feeds <path>`, `--fixtures <dir>`, `--output <dir>`, `--retries <n>`.
Standard library only — no third-party packages, no network needed for the rehearsal.

## Output

| Path | Contents |
|---|---|
| `db/pipeline.db` | SQLite database: `records`, `runs`, `feed_runs`, `merges` |
| `output/run_summary.json` | machine-readable summary of the last run (per-source status and counts) |
| `output/status_report.md` | the human-readable status page |
| `output/alerts.log` | one JSON line per source that failed after its retries |

### Verified runs (2026-10-16, Windows, Python 3.12)

| Run | Command | Result |
|---|---|---|
| First ingest | `python runner.py --offline` | `feeds_ok=3 seen=14 new=14 unchanged=0 invalid=1 duplicates=2`, exit 0 |
| Second ingest (idempotency) | `python runner.py --offline` | `new=0 updated=0 unchanged=14`, exit 0 — nothing was duplicated |
| Failure rehearsal | `python runner.py --offline --inject-failure feed_b` | `status=partial feeds_ok=2/3`, alert written, exit 2 |
| Reconciliation | `python verify_run.py` | `all checks passed (12 checks)` — database re-read and compared against the fixtures |

The reconciliation script recomputes from the fixtures and the database itself; it does not read the
run summary. It checks that the record count matches the fixtures, that no `(source, external id)`
pair is duplicated, that the last successful run accounted for exactly as many rows as it saw, that
every feed recorded its attempts, and that every duplicate decision is consistent.

## Design notes

- **Why the exit code matters.** A pipeline that logs an error and still exits 0 trains everyone to
  ignore it. The non-zero code is the contract with the scheduler: run 0 means "nothing to do",
  run 2 means "something needs a person".
- **Why both rows survive a duplicate match.** Merging automatically is how lists quietly lose
  records. Keeping both rows plus a merge entry means the decision is auditable and reversible.
- **Why the fixtures are local.** The failure path is the part clients cannot see in a demo, so it is
  made deliberately reachable: `--inject-failure` fails a source on every attempt, which is how the
  retry counter and the alert were verified.

## Running it on a schedule with n8n

`n8n/workflow.json` is an importable starting point: a Schedule Trigger (daily 06:00) → Execute
Command (`python runner.py --online`) → on failure, a Slack alert carrying the exit code and stderr.
Import it, set the working directory to this folder, and attach your Slack credential.

The workflow file is authored, not executed inside n8n as part of this repository — the part that is
tested and reconciled here is the Python pipeline, and the alert contract it exposes (alerts.log plus
a non-zero exit code) is what any scheduler can consume. cron and Windows Task Scheduler work the
same way.

## Pointing it at Postgres / Supabase

`db/schema_postgres.sql` is the same model on Postgres, which is what Supabase runs. Create the
tables there, put the connection string in an environment variable, and swap the storage layer —
`pipeline/store.py` is deliberately the only file that talks to the database, so the SQLite
implementation here and a Postgres one are interchangeable. The SQLite path is what the verified
runs above use.

## What is deliberately not included

- No third-party packages: no `requests`, no ORM, no queue library. Portability first.
- No scraping of anyone else's site: the practice sources are local fixtures, and online mode expects
  a JSON endpoint you are allowed to call.
- No credentials anywhere in the code or the fixtures; anything secret belongs in environment
  variables.
