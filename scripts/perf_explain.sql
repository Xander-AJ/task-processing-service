-- perf_explain.sql
--
-- Snapshot of the production fair-claim query, copied verbatim from claim_tasks()
-- in app/services/worker_service.py (same CTE, same outer eligibility predicate,
-- same ORDER BY, same FOR UPDATE ... SKIP LOCKED, same LIMIT). Bound parameters are
-- inlined as literals for offline EXPLAIN: :now -> now(),
-- :stale_before -> now() - interval '5 minutes', :batch_size -> 10.
--
-- IMPORTANT: if the production SQL in claim_tasks() changes, update this file in the
-- SAME commit — an EXPLAIN of a stale query measures nothing useful.
--
-- track_io_timing: turned on below because BUFFERS alone reports block counts but no
-- I/O *time*. Without it, "is the query I/O-bound or memory-resident?" cannot be
-- answered from the plan (I/O Timings lines simply won't appear). This is a
-- session-scoped SET — it makes no permanent change to the database.

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
