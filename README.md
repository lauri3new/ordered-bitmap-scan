# bitmap-index-filter

A proof of concept for a new type of Scan, an index scan than uses a bitmap for filtering rather than going to the heap for filtering. This repo is a proof of concept - a PostgreSQL 18 extension that combines a B-tree index with a bitmap filter index (GIN/GiST) to return ordered results.

## The problem

PostgreSQL can use multiple indexes together via bitmap scans, but bitmap scans don't support ordering. This creates a tradeoff when you need results ordered by one index and filtered by another.

Say you want the 10 most recent articles matching a full-text search query. Postgres has two strategies:

1. **Bitmap scan on the GIN index** — finds all matching rows and then heap-fetch every match, sort them by `created_at`, and return the top 10. If there are many matching rows then this can be expensive.

2. **B-tree index scan on `created_at`** — walks rows in order and checks each one against the text search condition. If there are many non-matching rows then it can be expensive finding 10 hits.

The proposal here adds a sort of imbetween that *may* be preferable over these two approaches depending on data distribution.

## The approach

`ordered_bitmap_scan`

1. Build a TID bitmap from the filter index (GIN/GiST) — the same structure Postgres builds internally for bitmap scans
2. Walk the B-tree (driving index) in order
3. For each TID from the B-tree, check it against the bitmap — O(1) lookup
4. Return matching TIDs up to the limit, already in order

When `work_mem` is too low to hold exact page-level bitmaps, Postgres falls back to lossy bitmaps (tracking pages instead of individual tuples). The function handles this by fetching heap tuples for lossy pages and rechecking the filter condition.

## Usage

```sql
SELECT t.created_at, t.body
FROM ordered_bitmap_scan(
    'articles',              -- table
    'idx_articles_created',  -- B-tree driving index (controls ordering)
    'idx_articles_tsv',      -- GIN/GiST filter index
    '@@',                    -- filter operator
    'fox & brown',           -- filter value (cast to appropriate type internally)
    'forward',               -- scan direction: 'forward' or 'backward'
    10                       -- limit
) AS obs
JOIN articles t ON t.ctid = obs.ordered_bitmap_scan;
```

The function returns TIDs (`ItemPointerData`), so you join back to the table on `ctid` to get full rows.

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `table_name` | — | Table to scan |
| `driving_index` | — | B-tree index that determines result order |
| `filter_index` | — | GIN or GiST index for filtering |
| `filter_op` | — | Operator as string (e.g. `'@@'`) |
| `filter_value` | — | Right-hand operand for the filter |
| `driving_direction` | `'forward'` | `'forward'` or `'backward'` |
| `fetch_limit` | `100` | Max rows to return |
| `report_buffers` | `false` | Print buffer usage via `elog` |

## Benchmark

The benchmark (`sql/benchmark.sql`) sets up a scenario designed to highlight the difference:

- 50K base rows + 800K additional scattered rows
- ~1-in-200 match rate for the text search filter
- `work_mem = '64kB'` to force lossy bitmaps

It compares buffer hits and execution time between a standard query (`WHERE body_tsv @@ ... ORDER BY created_at LIMIT N`) and `ordered_bitmap_scan` at various limit values (10, 100, 1000). `ordered_bitmap_scan` uses less buffers than either index scan or bitmap scan for the data distribution used in the benchmark.

Run it:

```sh
cargo make benchmark
```

## Potential applications

The text search case is just one instance of a general pattern — any time you need ordered results filtered by a non-B-tree index:

- **pgvector HNSW/IVFFlat** — nearest K vectors that also satisfy some ordering constraint (e.g. most recent similar documents)
- **GiST spatial indexes** — events within a geographic boundary, ordered by timestamp
- **GIN array/JSONB indexes** — rows matching array containment or JSONB path conditions, ordered by a B-tree column

The operator is currently hardcoded to resolve `tsvector` types, but the approach should in theory generalize to any index type that produces a bitmap scan.

## Setup

Requires: Rust, [cargo-make](https://github.com/sagiegurari/cargo-make), Docker.

```sh
# Build and start PostgreSQL 18 with the extension
cargo make build && cargo make up

# Open a psql session
cargo make psql

# Run lossy bitmap correctness tests
cargo make test-lossy

# Run the benchmark
cargo make benchmark

# Stop and clean up
cargo make clean
```

## Limitations

- Proof of concept — not production-ready
- Only supports the tsvector `@@` operator currently (type resolution is hardcoded)
- Returns TIDs, not full rows — requires a `JOIN` on `ctid`
- Requires PostgreSQL 18 and pgrx 0.17
