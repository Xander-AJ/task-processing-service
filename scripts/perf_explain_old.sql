-- perf_explain_old.sql
--
-- BASELINE snapshot of the PRE-LATERAL fair-claim query (global ROW_NUMBER window
-- over every eligible row). Kept only so the performance deep-dive can show the old
-- and new plans side-by-side without checking out an old commit. This is NOT the
-- production query anymore — see perf_explain.sql for the current one
-- (LATERAL top-K per company, matching claim_tasks() in
-- app/services/worker_service.py).
--
-- Bound params inlined as literals: :now -> now(),
-- :stale_before -> now() - interval '5 minutes', :batch_size -> 10.
--
-- track_io_timing: on so BUFFERS reports I/O *time*, not just block counts;
-- otherwise the I/O-bound vs memory-resident question can't be answered from the
-- plan. Session-scoped SET — no permanent change to the database.

SET track_io_timing = on;

\echo ===== WARMUP (discard this plan) =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at) AS rn,
           created_at
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
candidates AS (
    SELECT id, rn, created_at FROM ranked WHERE rn <= 10
)
SELECT t.*
FROM tasks t
JOIN candidates c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;

\echo ===== RUN 1 =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at) AS rn,
           created_at
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
candidates AS (
    SELECT id, rn, created_at FROM ranked WHERE rn <= 10
)
SELECT t.*
FROM tasks t
JOIN candidates c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;

\echo ===== RUN 2 =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at) AS rn,
           created_at
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
candidates AS (
    SELECT id, rn, created_at FROM ranked WHERE rn <= 10
)
SELECT t.*
FROM tasks t
JOIN candidates c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;

\echo ===== RUN 3 =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at) AS rn,
           created_at
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
candidates AS (
    SELECT id, rn, created_at FROM ranked WHERE rn <= 10
)
SELECT t.*
FROM tasks t
JOIN candidates c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;
