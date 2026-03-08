-- test_lossy.sql
-- Tests that lossy bitmap recheck produces correct results
-- Run: cargo make test-lossy

\pset pager off

------------------------------------------------------------
-- 1. Setup: Create table and indexes
------------------------------------------------------------
DROP TABLE IF EXISTS test_lossy CASCADE;
CREATE TABLE test_lossy (
    id serial PRIMARY KEY,
    created_at timestamptz NOT NULL,
    body text NOT NULL,
    body_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
);
CREATE INDEX idx_tl_created ON test_lossy (created_at);
CREATE INDEX idx_tl_tsv ON test_lossy USING gin (body_tsv);

------------------------------------------------------------
-- 2. Insert 10,000 rows (~3,333 matches)
------------------------------------------------------------
INSERT INTO test_lossy (created_at, body)
SELECT '2024-01-01'::timestamptz + (i || ' seconds')::interval,
       CASE WHEN i % 3 = 0 THEN 'the quick brown fox jumps over the lazy dog'
            WHEN i % 3 = 1 THEN 'hello world from postgresql extension'
            ELSE 'random text about weather patterns'
       END
FROM generate_series(1, 10000) AS i;

CREATE EXTENSION IF NOT EXISTS bitmap_index_filter;
ANALYZE test_lossy;

------------------------------------------------------------
-- 3. Test with default work_mem (exact bitmap)
------------------------------------------------------------
\echo ''
\echo '==== TEST 1: Default work_mem (exact bitmap) ===='

CREATE TEMP TABLE exact_regular AS
SELECT ctid::text AS tid FROM test_lossy
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at;

CREATE TEMP TABLE exact_obs AS
SELECT ordered_bitmap_scan::text AS tid FROM ordered_bitmap_scan(
    'test_lossy', 'idx_tl_created', 'idx_tl_tsv',
    '@@', 'fox & brown', 'forward', 10000
);

SELECT count(*) AS regular_count FROM exact_regular;
SELECT count(*) AS obs_count FROM exact_obs;

\echo '--- Mismatches (should be 0 rows) ---'
SELECT 'EXACT MISMATCH' AS error, * FROM (
    (TABLE exact_regular EXCEPT TABLE exact_obs)
    UNION ALL
    (TABLE exact_obs EXCEPT TABLE exact_regular)
) diff;

------------------------------------------------------------
-- 4. Test with tiny work_mem (forces lossy bitmap)
------------------------------------------------------------
\echo ''
\echo '==== TEST 2: work_mem=64kB (lossy bitmap) ===='

SET work_mem = '64kB';

CREATE TEMP TABLE lossy_regular AS
SELECT ctid::text AS tid FROM test_lossy
WHERE body_tsv @@ to_tsquery('english', 'fox & brown')
ORDER BY created_at;

CREATE TEMP TABLE lossy_obs AS
SELECT ordered_bitmap_scan::text AS tid FROM ordered_bitmap_scan(
    'test_lossy', 'idx_tl_created', 'idx_tl_tsv',
    '@@', 'fox & brown', 'forward', 10000
);

SELECT count(*) AS regular_count FROM lossy_regular;
SELECT count(*) AS obs_count FROM lossy_obs;

\echo '--- Mismatches (should be 0 rows) ---'
SELECT 'LOSSY MISMATCH' AS error, * FROM (
    (TABLE lossy_regular EXCEPT TABLE lossy_obs)
    UNION ALL
    (TABLE lossy_obs EXCEPT TABLE lossy_regular)
) diff;

RESET work_mem;

\echo ''
\echo '==== LOSSY BITMAP TEST COMPLETE ===='
