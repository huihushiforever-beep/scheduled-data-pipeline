# Pipeline status

Generated: 2026-10-16 10:32:45  
Records held: **10**  
Cross-source duplicates flagged: **1** (merge decisions logged: 1)

## Recent runs

| run | started | status | feeds ok/failed | new | updated | unchanged | invalid | alert |
|---|---|---|---|---|---|---|---|---|
| RUN-20261016-023244-36e8 | 2026-10-16T02:32:44+00:00 | partial | 2/1 | 10 | 0 | 0 | 0 | 1 source(s) failed - alerter fired |

## Records by source

| source | records | flagged as duplicate |
|---|---|---|
| feed_a | 6 | 0 |
| feed_c | 4 | 1 |

## If something looks wrong

1. A run marked `partial` means at least one source failed after its retries - read the alert line
   for that run and check the source before the next scheduled run.
2. A source that keeps returning fewer records than usual usually means its page or feed changed;
   that is what the per-source counts above are for.
3. A record flagged as a duplicate keeps both rows and a merge entry, so a wrong match can be undone
   without losing the original.
