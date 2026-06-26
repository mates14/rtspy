# Integer overflow in GRB database tables

## Problem

Einstein Probe trigger IDs (e.g. `11900781319`) exceed the range of a 32-bit
signed integer (INT4 max = 2 147 483 647). The `grb` and `grb_gcn` tables store
`grb_id` as `INTEGER`, so any attempt to insert a large EP trigger ID causes
PostgreSQL to throw `integer out of range` and the target is never created.

Observed in the log on 2026-06-26:

```
KAFKA I Einstein Probe WXT alert: 11900781319
KAFKA I EINSTEIN_PROBE 11900781319: creating target for follow-up
KAFKA E Failed to create GRB target 53363: integer out of range
```

## Workaround applied (commit cecf81f)

`_convert_grb_id_to_int()` in `grbd.py` now keeps only the last 9 decimal
digits when the value exceeds INT4 range:

```python
if val > 2_147_483_647:
    val = val % 1_000_000_000
```

`11900781319 % 1_000_000_000 = 900781319` — still recognisable from the tail
of the original ID, and fits comfortably in INT4.

This avoids any database schema change and is safe for RTS2, which has its own
assumptions about the `targets` / `grb` table types.

## Correct long-term solution

Migrate `grb_id` columns to `BIGINT` (INT8) on all database instances:

```sql
BEGIN;
ALTER TABLE grb     ALTER COLUMN grb_id TYPE BIGINT;
ALTER TABLE grb_gcn ALTER COLUMN grb_id TYPE BIGINT;
ALTER TABLE grb_gcn ALTER COLUMN packet  TYPE BIGINT[];
COMMIT;
```

`grb_gcn.packet` is an integer array whose slot 0 also holds `grb_id_int`, so
it needs the same treatment.

**Before doing this**, verify that RTS2 itself does not hardcode INT4 for these
columns in its own C++ schema or queries — if it does, the migration will break
RTS2's GRB handling. Once confirmed safe, apply on every observatory PC running
the database, then remove the `% 1_000_000_000` truncation from `grbd.py`.
