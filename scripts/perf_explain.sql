-- perf_explain.sql
--
-- Snapshot of the production fair-claim query, copied verbatim from claim_tasks()
-- in app/services/worker_service.py (LATERAL top-K per eligible company, same outer
-- eligibility re-check, same ORDER BY, same FOR UPDATE ... SKIP LOCKED, same LIMIT).
-- Bound parameters are inlined as literals for offline EXPLAIN: :now -> now(),
-- :stale_before -> now() - interval '5 minutes', :per_company_cap -> 10,
-- :batch_size -> 10.
--
-- IMPORTANT: if the production SQL in claim_tasks() changes, update this file in the
-- SAME commit — an EXPLAIN of a stale query measures nothing useful. (See
-- scripts/perf_explain_old.sql for the previous ROW_NUMBER baseline.)
--
-- track_io_timing: turned on below because BUFFERS alone reports block counts but no
-- I/O *time*. Without it, "is the query I/O-bound or memory-resident?" cannot be
-- answered from the plan. Session-scoped SET — no permanent change to the database.

SET track_io_timing = on;

\echo ===== WARMUP (discard this plan) =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH eligible_companies AS (
    SELECT DISTINCT company_id
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
per_company_top AS (
    SELECT ec.company_id, top.id, top.created_at,
           ROW_NUMBER() OVER (PARTITION BY ec.company_id
                              ORDER BY top.created_at) AS rn
    FROM eligible_companies ec
    CROSS JOIN LATERAL (
        SELECT id, created_at
        FROM tasks
        WHERE company_id = ec.company_id
          AND ((status = 'pending'    AND run_after <= now())
            OR (status = 'processing' AND locked_at < now() - interval '5 minutes'))
        ORDER BY created_at
        LIMIT 10
    ) top
)
SELECT t.*
FROM tasks t
JOIN per_company_top c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;

\echo ===== RUN 1 =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH eligible_companies AS (
    SELECT DISTINCT company_id
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
per_company_top AS (
    SELECT ec.company_id, top.id, top.created_at,
           ROW_NUMBER() OVER (PARTITION BY ec.company_id
                              ORDER BY top.created_at) AS rn
    FROM eligible_companies ec
    CROSS JOIN LATERAL (
        SELECT id, created_at
        FROM tasks
        WHERE company_id = ec.company_id
          AND ((status = 'pending'    AND run_after <= now())
            OR (status = 'processing' AND locked_at < now() - interval '5 minutes'))
        ORDER BY created_at
        LIMIT 10
    ) top
)
SELECT t.*
FROM tasks t
JOIN per_company_top c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;

\echo ===== RUN 2 =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH eligible_companies AS (
    SELECT DISTINCT company_id
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
per_company_top AS (
    SELECT ec.company_id, top.id, top.created_at,
           ROW_NUMBER() OVER (PARTITION BY ec.company_id
                              ORDER BY top.created_at) AS rn
    FROM eligible_companies ec
    CROSS JOIN LATERAL (
        SELECT id, created_at
        FROM tasks
        WHERE company_id = ec.company_id
          AND ((status = 'pending'    AND run_after <= now())
            OR (status = 'processing' AND locked_at < now() - interval '5 minutes'))
        ORDER BY created_at
        LIMIT 10
    ) top
)
SELECT t.*
FROM tasks t
JOIN per_company_top c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;

\echo ===== RUN 3 =====
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH eligible_companies AS (
    SELECT DISTINCT company_id
    FROM tasks
    WHERE (status = 'pending'    AND run_after <= now())
       OR (status = 'processing' AND locked_at < now() - interval '5 minutes')
),
per_company_top AS (
    SELECT ec.company_id, top.id, top.created_at,
           ROW_NUMBER() OVER (PARTITION BY ec.company_id
                              ORDER BY top.created_at) AS rn
    FROM eligible_companies ec
    CROSS JOIN LATERAL (
        SELECT id, created_at
        FROM tasks
        WHERE company_id = ec.company_id
          AND ((status = 'pending'    AND run_after <= now())
            OR (status = 'processing' AND locked_at < now() - interval '5 minutes'))
        ORDER BY created_at
        LIMIT 10
    ) top
)
SELECT t.*
FROM tasks t
JOIN per_company_top c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= now())
   OR (t.status = 'processing' AND t.locked_at < now() - interval '5 minutes')
ORDER BY c.rn, c.created_at
FOR UPDATE OF t SKIP LOCKED
LIMIT 10;
