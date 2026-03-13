-- benchmark.sql
-- Compares ordered_bitmap_scan vs regular PostgreSQL query
-- Run: cargo make benchmark

\timing on
\pset pager off

------------------------------------------------------------
-- 1. Setup: Create table and indexes
------------------------------------------------------------
DROP TABLE IF EXISTS bench CASCADE;
CREATE TABLE bench (
    id serial PRIMARY KEY,
    created_at timestamptz NOT NULL,
    body text NOT NULL,
    body_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
);
CREATE INDEX idx_bench_created ON bench (created_at);
CREATE INDEX idx_bench_tsv ON bench USING gin (body_tsv);

------------------------------------------------------------
-- 2. Insert 10,000 rows (deterministic 1-in-10 match)
------------------------------------------------------------
INSERT INTO bench (created_at, body)
SELECT '2024-01-01'::timestamptz + (i || ' minutes')::interval,
       CASE i % 10
           WHEN 0 THEN 'the monkey jumps up the tree and runs away'
           WHEN 1 THEN 'hello world from postgresql extension development'
           WHEN 2 THEN 'database indexing strategies for large tables'
           WHEN 3 THEN 'concurrent transactions and isolation levels'
           WHEN 4 THEN 'query planning and execution in postgresql'
           WHEN 5 THEN 'partitioning large tables for better performance'
           WHEN 6 THEN 'vacuum and autovacuum tuning guide today'
           WHEN 7 THEN 'replication streaming logical physical setup'
           WHEN 8 THEN 'monitoring slow queries with statistics views'
           WHEN 9 THEN 'connection pooling configuration and setup'
       END
FROM generate_series(1, 25000) AS i;

INSERT INTO bench (created_at, body)
SELECT '2024-01-02'::timestamptz + (i || ' minutes')::interval,
       CASE i % 10
           WHEN 0 THEN 'the quick brown fox jumps over the lazy dog'
           WHEN 1 THEN 'hello world from postgresql extension development'
           WHEN 2 THEN 'database indexing strategies for large tables'
           WHEN 3 THEN 'concurrent transactions and isolation levels'
           WHEN 4 THEN 'query planning and execution in postgresql'
           WHEN 5 THEN 'partitioning large tables for better performance'
           WHEN 6 THEN 'vacuum and autovacuum tuning guide today'
           WHEN 7 THEN 'replication streaming logical physical setup'
           WHEN 8 THEN 'monitoring slow queries with statistics views'
           WHEN 9 THEN 'connection pooling configuration and setup'
       END
FROM generate_series(1, 25000) AS i;

------------------------------------------------------------
-- 3. Load extension and analyze
------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS bitmap_index_filter;
ANALYZE bench;

------------------------------------------------------------
-- 4. Test Case 1: LIMIT 10 — correctness comparison
------------------------------------------------------------
\echo ''
\echo '==== TEST CASE 1: LIMIT 10 ===='
\echo ''


CREATE TEMP TABLE regular_10 AS
SELECT ctid::text AS tid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 10;

CREATE TEMP TABLE obs_10 AS
SELECT ordered_bitmap_scan::text AS tid FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 10
);

\echo '--- Mismatches (should be 0 rows) ---'
SELECT 'LIMIT 10 MISMATCH' AS error, * FROM (
    (TABLE regular_10 EXCEPT TABLE obs_10)
    UNION ALL
    (TABLE obs_10 EXCEPT TABLE regular_10)
) diff;

------------------------------------------------------------
-- 5. Test Case 2: LIMIT 100 — correctness comparison
------------------------------------------------------------
\echo ''
\echo '==== TEST CASE 2: LIMIT 100 ===='
\echo ''

CREATE TEMP TABLE regular_100 AS
SELECT ctid::text AS tid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 100;

CREATE TEMP TABLE obs_100 AS
SELECT ordered_bitmap_scan::text AS tid FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 100
);

\echo '--- Mismatches (should be 0 rows) ---'
SELECT 'LIMIT 100 MISMATCH' AS error, * FROM (
    (TABLE regular_100 EXCEPT TABLE obs_100)
    UNION ALL
    (TABLE obs_100 EXCEPT TABLE regular_100)
) diff;

------------------------------------------------------------
-- 6. Test Case 3: LIMIT 1000 — correctness comparison
------------------------------------------------------------
\echo ''
\echo '==== TEST CASE 3: LIMIT 1000 ===='
\echo ''

CREATE TEMP TABLE regular_1000 AS
SELECT ctid::text AS tid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 1000;

CREATE TEMP TABLE obs_1000 AS
SELECT ordered_bitmap_scan::text AS tid FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 1000
);

\echo '--- Mismatches (should be 0 rows) ---'
SELECT 'LIMIT 1000 MISMATCH' AS error, * FROM (
    (TABLE regular_1000 EXCEPT TABLE obs_1000)
    UNION ALL
    (TABLE obs_1000 EXCEPT TABLE regular_1000)
) diff;

------------------------------------------------------------
-- 7. Buffer comparison: EXPLAIN (ANALYZE, BUFFERS)
------------------------------------------------------------
\echo ''
\echo '==== BUFFER COMPARISON: LIMIT 10 ===='
\echo ''

\echo '--- Regular query (LIMIT 10) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT ctid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 10;

\echo ''
\echo '--- ordered_bitmap_scan (LIMIT 10, report_buffers=true) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 10, true
);

\echo ''
\echo '==== BUFFER COMPARISON: LIMIT 100 ===='
\echo ''

\echo '--- Regular query (LIMIT 100) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT ctid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 100;

\echo ''
\echo '--- ordered_bitmap_scan (LIMIT 100, report_buffers=true) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 100, true
);

\echo ''
\echo '==== BUFFER COMPARISON: LIMIT 1000 ===='
\echo ''

\echo '--- Regular query (LIMIT 1000) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT ctid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 1000;

\echo ''
\echo '--- ordered_bitmap_scan (LIMIT 1000, report_buffers=true) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 1000, true
);

\echo ''
\echo '==== EXACT BITMAP BENCHMARK COMPLETE ===='

------------------------------------------------------------
-- 8. Lossy bitmap setup: insert 800K rows with scattered matches (1-in-20)
------------------------------------------------------------
\echo ''
\echo '==== LOSSY BITMAP TESTS ===='
\echo ''

INSERT INTO bench (created_at, body)
SELECT '2025-01-01'::timestamptz + (i || ' seconds')::interval,
       CASE WHEN i % 200 = 0
           THEN 'the quick brown fox jumps over the lazy dog'
           ELSE 'hello world from postgresql extension development'
       END
FROM generate_series(1, 800000) AS i;

ANALYZE bench;

SET work_mem = '64kB';

------------------------------------------------------------
-- 9. Lossy correctness: LIMIT 100
------------------------------------------------------------
\echo ''
\echo '==== LOSSY TEST CASE 1: LIMIT 100 ===='
\echo ''

CREATE TEMP TABLE lossy_regular_100 AS
SELECT ctid::text AS tid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 100;

CREATE TEMP TABLE lossy_obs_100 AS
SELECT ordered_bitmap_scan::text AS tid FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 100
);

\echo '--- Mismatches (should be 0 rows) ---'
SELECT 'LOSSY LIMIT 100 MISMATCH' AS error, * FROM (
    (TABLE lossy_regular_100 EXCEPT TABLE lossy_obs_100)
    UNION ALL
    (TABLE lossy_obs_100 EXCEPT TABLE lossy_regular_100)
) diff;

------------------------------------------------------------
-- 10. Lossy correctness: LIMIT 1000
------------------------------------------------------------
\echo ''
\echo '==== LOSSY TEST CASE 2: LIMIT 1000 ===='
\echo ''

CREATE TEMP TABLE lossy_regular_1000 AS
SELECT ctid::text AS tid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 1000;

CREATE TEMP TABLE lossy_obs_1000 AS
SELECT ordered_bitmap_scan::text AS tid FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 1000
);

\echo '--- Mismatches (should be 0 rows) ---'
SELECT 'LOSSY LIMIT 1000 MISMATCH' AS error, * FROM (
    (TABLE lossy_regular_1000 EXCEPT TABLE lossy_obs_1000)
    UNION ALL
    (TABLE lossy_obs_1000 EXCEPT TABLE lossy_regular_1000)
) diff;

------------------------------------------------------------
-- 11. Lossy buffer comparison: EXPLAIN (ANALYZE, BUFFERS)
------------------------------------------------------------

\echo ''
\echo '==== LOSSY BUFFER COMPARISON: LIMIT 10 ===='
\echo ''

\echo '--- Regular query (LIMIT 10, work_mem=64kB) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT ctid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 10;

\echo ''
\echo '--- ordered_bitmap_scan (LIMIT 10, work_mem=64kB, report_buffers=true) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 10, true
);

RESET work_mem;

\echo ''
\echo '==== BENCHMARK COMPLETE (exact + lossy) ===='


\echo ''
\echo '==== LOSSY BUFFER COMPARISON: LIMIT 100 ===='
\echo ''

\echo '--- Regular query (LIMIT 100, work_mem=64kB) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT ctid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 100;

\echo ''
\echo '--- ordered_bitmap_scan (LIMIT 100, work_mem=64kB, report_buffers=true) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 100, true
);

RESET work_mem;

\echo ''
\echo '==== BENCHMARK COMPLETE (exact + lossy) ===='

\echo ''
\echo '==== LOSSY BUFFER COMPARISON: LIMIT 1000 ===='
\echo ''

\echo '--- Regular query (LIMIT 1000, work_mem=64kB) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT ctid FROM bench
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at
LIMIT 1000;

\echo ''
\echo '--- ordered_bitmap_scan (LIMIT 1000, work_mem=64kB, report_buffers=true) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM ordered_bitmap_scan(
    'bench', 'idx_bench_created', 'idx_bench_tsv',
    '@@', 'fox & brown', 'forward', 1000, true
);

RESET work_mem;

\echo ''
\echo '==== BENCHMARK COMPLETE (exact + lossy) ===='

