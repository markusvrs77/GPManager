# gpcopy v2 — Copy Modes Design Spec

**Date:** 2026-07-21 · **Status:** Implemented (same day, branch `claude/gpm-new-features-plan-909316`)

## Purpose

Extend gpcopy beyond full/date copy with two routine prod→test refresh modes and
make every mode schedulable via the scheduler subsystem (see
`2026-07-21-scheduler-cron-design.md`).

## Copy modes (final set)

| mode | mechanism | when to use |
|---|---|---|
| **full** | include-table-file, existing `run_gpcopy_job` | first load / небольшие таблицы |
| **date** | include-table-json `WHERE col >= from AND < to` (+ relative windows in scheduler) | партиционированные по дате факты |
| **increment** *(new)* | dest `max(watermark_column)` → copy only `WHERE col > watermark`, `--append`; no watermark → full backfill | append-only логи/факты |
| **partition_diff** *(new)* | leaf partitions via `pg_inherits` CTE; copy partitions where `COUNT(*)` differs source↔dest or missing in dest; `--truncate` | большие партиц. таблицы с точечными изменениями |
| **sync** | existing key-upsert (`gpcopy_sync.py`: staging + INSERT/UPDATE/DELETE by keys, md5 hash) | изменяемые таблицы, нужны update/delete |

Decisions (brainstorm 2026-07-21): increment = **both** watermark-append (new)
and key-upsert (existing) as separate modes; "partition differs" = **row count**
(cheap, covers most cases; md5-checksum rejected as full-scan-expensive).

## Implementation map

- `modules/gpcopy_increment.py` — `build_increment_items` (pure, tested:
  watermark None→full, literal escaping incl. SQL injection), `get_dest_watermark`,
  `run_gpcopy_increment_job`.
- `modules/gpcopy_partition.py` — `list_leaf_partitions` (CTE reused from
  `reorganize.py`), `classify_partition_diff` (pure: copy_missing/copy_changed/skip),
  `diff_partitions`, `run_gpcopy_partition_diff_job`.
- API: `/api/gpcopy/increment/{preview,start}`,
  `/api/gpcopy/partition-diff/{preview,start}`.
- Scheduler: job_types `gpcopy_increment`, `gpcopy_partition_diff` registered in
  `JOB_RUNNERS`; on `/schedules` — dropdown options + watermark + dest-connection
  fields; on `/gpcopy` — "Increment / Partition-diff" card with Preview/Start and
  inline "Запланировать" (name + cron → `POST /api/schedules`).

## Follow-ups

- Row-count validation gate after copy; partition-diff `job_items` per partition
  with individual statuses; multi-table partition-diff preview in UI (now first
  selected table); md5 checksum as opt-in precision mode.
