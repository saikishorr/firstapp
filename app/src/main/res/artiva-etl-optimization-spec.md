# Artiva Multi-Vertical ETL Optimization Spec

**Author:** Karthik
**Date:** July 25, 2026
**Status:** Draft for review

## 1. Problem Statement

- Current ETL for 1 vertical: 6-7 hours full pull across 8 tables (15-18M+ rows)
- Refresh cadence required: every 24 hours
- Scope expanding to 5 verticals total (4 new + 1 existing), one of which is larger than the current vertical
- At current sequential, full-pull performance, 5 verticals will not fit inside the 24-hour window
- Source and staging databases are on **separate SQL Server instances** (no shared resource contention)
- No reliable `modified_at`/`updated_at` timestamp exists on **source** tables — native timestamp-based incremental pull is not possible
- Staging tables **do** have a timestamp column, usable for tracking when a row was last written
- Hardware scaling is explicitly out of scope for this optimization pass

## 2. Goals

1. Determine, per table, whether SQL Server Change Data Capture (CDC) is actually available and enabled — don't assume
2. For CDC-eligible tables: consume only changed rows via CDC change tables
3. For non-CDC tables: fall back to full-pull + row-hash diff to avoid writing unchanged rows
4. Parallelize table pulls on the source side, independent of staging writes
5. Keep the design safe for production — pulls must not degrade the live Artiva app on the source instance

## 3. Step 1 — CDC Availability Check

CDC must be enabled at both the **database level** and the **table level**. Both checks are required; database-level enablement does not imply any given table is tracked.

### 3.1 Database-level check

```sql
SELECT name AS database_name, is_cdc_enabled
FROM sys.databases
WHERE name = 'YourArtivaDatabaseName';
```

- `is_cdc_enabled = 0` → CDC has never been enabled on this database. Requires `sysadmin`/`db_owner` to enable via `sys.sp_cdc_enable_db`. This may not be something you control on the Artiva source — confirm with whoever owns that instance before assuming it's an option.
- `is_cdc_enabled = 1` → proceed to table-level check.

### 3.2 Table-level check (per table you pull)

```sql
SELECT
    t.name AS table_name,
    t.is_tracked_by_cdc
FROM sys.tables t
WHERE t.name IN ('Table1','Table2','Table3','Table4','Table5','Table6','Table7','Table8');
```

- `is_tracked_by_cdc = 1` → table is CDC-enabled, safe to query change tables
- `is_tracked_by_cdc = 0` → table is not tracked; falls back to hash-diff strategy (Section 5)

### 3.3 Detailed capture instance info (for tables that ARE tracked)

```sql
EXEC sys.sp_cdc_help_change_data_capture;
```

Returns capture instance name, capture/cleanup job status, and column list per tracked table — needed to know what change table to query and whether the capture job is actually running (a table can be marked tracked but have a disabled/failed capture job, which silently stops producing changes).

### 3.4 Capture/cleanup job health check

```sql
SELECT name, enabled, [type]
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobsteps s ON j.job_id = s.job_id
WHERE name LIKE 'cdc.%';
```

If disabled or erroring, CDC data will silently go stale — this should be part of ongoing monitoring, not just a one-time check.

### 3.5 Decision logic

```
FOR each of the 8 tables (× 5 verticals):
    IF database.is_cdc_enabled == 0:
        → route ALL tables in this vertical's source DB to hash-diff (Section 5)
    ELSE IF table.is_tracked_by_cdc == 1 AND capture job is enabled/healthy:
        → route this table to CDC pull (Section 4)
    ELSE:
        → route this table to hash-diff (Section 5)
```

This check should run at the start of every ETL cycle (cheap, metadata-only query) rather than being hardcoded once — capture jobs can be disabled or fail independently of your ETL, and you want to detect that automatically rather than silently under-syncing.

## 4. CDC Pull Path (where available)

For tables where CDC is confirmed enabled and healthy:

1. Track the last processed LSN (Log Sequence Number) per table in a small control table in staging, e.g. `etl_cdc_watermark(table_name, vertical, last_lsn, updated_at)`.
2. Each run, get the current max LSN:
   ```sql
   SELECT sys.fn_cdc_get_max_lsn() AS max_lsn;
   ```
3. Pull only the delta since the last watermark:
   ```sql
   SELECT *
   FROM cdc.fn_cdc_get_all_changes_<capture_instance>(
       @from_lsn, @to_lsn, 'all'
   );
   ```
4. Apply changes to staging via `MERGE` (insert/update/delete based on `__$operation` column CDC provides — 1=delete, 2=insert, 3=update before, 4=update after).
5. Update the watermark table to `@to_lsn` only after a successful, committed write.

**Why this matters for your numbers:** CDC-tracked tables skip the full 15-20M row read entirely on steady-state runs — you're only reading what actually changed since last LSN, typically a small fraction of the table. This is the biggest possible win, but only applies to tables where it's actually available and healthy.

## 5. Hash-Diff Fallback Path (no CDC / CDC unavailable)

For tables without CDC (expected default given no source timestamps and unconfirmed CDC availability):

1. Full read from source (unavoidable — no way to filter server-side without CDC or timestamps).
2. Compute a row hash per pulled row over the relevant business columns:
   ```sql
   -- Computed either in source SQL or in the pull query itself
   SELECT *,
          HASHBYTES('MD5',
              CONCAT(Col1, '|', Col2, '|', Col3, '|', ColN)
          ) AS row_hash
   FROM SourceTable;
   ```
3. Load pulled rows into a staging **temp/landing table** (bulk insert, not row-by-row).
4. `MERGE` from landing table into the real staging table, comparing `row_hash`:
   ```sql
   MERGE INTO staging.TargetTable AS tgt
   USING landing.TargetTable AS src
   ON tgt.primary_key = src.primary_key
   WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
       UPDATE SET
           tgt.col1 = src.col1,
           tgt.col2 = src.col2,
           tgt.row_hash = src.row_hash,
           tgt.updated_at = SYSUTCDATETIME()
   WHEN NOT MATCHED BY TARGET THEN
       INSERT (primary_key, col1, col2, row_hash, updated_at)
       VALUES (src.primary_key, src.col1, src.col2, src.row_hash, SYSUTCDATETIME())
   WHEN NOT MATCHED BY SOURCE THEN
       -- optional: handle deletes if needed
       DELETE;
   ```
5. Only rows with a changed hash trigger an actual write — the staging `updated_at` timestamp naturally reflects only real changes.

**Why this matters:** read cost stays the same as today, but write cost (typically the dominant cost with row-by-row ORM writes) drops to just the delta. In collections data, most accounts don't change day-to-day, so this alone can meaningfully cut runtime even without CDC.

## 6. Parallelization (applies to both paths)

- Pull each table on the source side concurrently (separate connections/threads/Celery tasks), not sequentially — reads are I/O-wait dominated, so concurrent pulls overlap instead of queuing.
- Start with a conservative concurrency cap (e.g., 4 concurrent table pulls) and increase incrementally while monitoring:
  - Source SQL Server CPU/connection pool usage
  - Any live-app latency degradation on the source Artiva instance
- Because source and staging are separate instances, parallel reads and parallel `MERGE` writes do not contend with each other — both can be tuned independently to their own instance's ceiling.
- Once per-vertical pull time is optimized, the same concurrency model extends to running **multiple verticals' pull+merge groups in parallel**, up to whatever combined ceiling testing reveals on each instance.

## 7. Rollout Plan

1. Run CDC availability check (Section 3) against source instance — get a definitive table-by-table map of CDC-eligible vs. not.
2. Build the hash-diff + `MERGE` path first (Section 5) since it applies universally regardless of CDC availability, and measure its impact alone.
3. If any tables are CDC-eligible, build the CDC path (Section 4) for those specific tables and compare steady-state runtime against hash-diff for the same table.
4. Add parallel table pulls (Section 6), testing concurrency levels 2 → 4 → 8 and measuring total pull time at each step to find the real ceiling.
5. Onboard new verticals one at a time behind this pipeline, re-running the CDC check for each new source (CDC availability may differ per vertical/database).
6. Add capture job health monitoring (Section 3.4) as an ongoing check, not just a one-time gate.

## 8. Open Questions (Part 1)

- Do you have `sysadmin`/`db_owner` access on the source instance to run `sys.sp_cdc_enable_db` if CDC is not currently enabled at the database level?
- Are there delete scenarios in source data that need to be reflected in staging (relevant to the `WHEN NOT MATCHED BY SOURCE` clause in Section 5)?
- What's the current write mechanism in the .exe/Celery pipeline — ORM row-by-row, or already bulk? This determines how much of the hash-diff win is "free" vs. requires also switching to bulk `MERGE`.

---

# Part 2 — Letter Eligibility Count Mismatch (COS → Python)

## 9. Problem Statement

- Ad-hoc letter eligibility count feature: Artiva COS script mapped line-by-line to Python
- Rules span two levels: client-level and letter-code-level
- Resulting eligible account counts consistently **do not match** Artiva's counts
- Pattern observed: Python's count is **majority lower** than Artiva's, with a small number of cases where it was **higher**

## 10. Diagnosis — Why "Mostly Lower, Occasionally Higher" Points to Two Separate Bugs

A systematic undercount plus isolated overcounts is rarely one root cause — it's usually:

- **One or two broad over-restrictive conditions** affecting most letter codes/clients (drives the majority-lower pattern), and
- **A separate, isolated missing-exclusion bug** affecting only specific client/letter-code combinations (drives the rare overcounts)

These should be diagnosed and fixed independently rather than assumed to share a cause.

## 11. Likely Causes — Majority Lower

- **Match-type mismatch**: a COS partial/contains-style comparison (`'=`) translated as an exact match in Python, quietly excluding accounts that should qualify
- **Join type mismatch**: a `LEFT JOIN` in the original logic implemented as `INNER JOIN` in the Python/SQLAlchemy pull, silently dropping accounts with NULLs on the joined side
- **NULL handling divergence**: COS treating "undefined" as eligible/neutral, while Python's `is None` check routes those accounts into an exclusion branch instead
- **Date/aging boundary off-by-one**: e.g. "days since last letter >= 30" implemented as `> 30`, cutting out a full day's worth of eligible accounts every run

## 12. Likely Causes — Rare Higher Cases

- **Client-level override precedence not replicated**: if client-level rules are meant to override or narrow letter-level rules in Artiva, but the Python implementation applies both as independent AND conditions, specific client/letter-code accounts can slip through a path that should have been excluded
- Concentrated in a small number of clients/letter codes — consistent with a precedence bug rather than a systemic rule error

## 13. Isolation Method

1. Pick one letter code + client combination where the gap is large
2. Pull the **actual eligible account ID lists** from both Artiva and the Python implementation — not just counts
3. Diff the two ID lists
4. For accounts missing from the Python output: trace each through the mapped rule logic step by step to identify which condition(s) they fail that they shouldn't
5. For accounts present only in the Python output (the overcount cases): trace which exclusion rule(s) should have applied but didn't — check client-level override precedence first

## 14. Open Questions (Part 2)

- Confirmed: gap is majority lower, with rare higher cases — treat as two separate bugs (Section 10)
- Is there access to pull actual eligible account ID lists from Artiva (not just aggregate counts), to enable the diff-based isolation method in Section 13?
- Does client-level rule logic in the original COS script override, narrow, or independently AND with letter-level rules? This needs to be confirmed against the source script before assuming the override-precedence theory (Section 12) is the cause.
