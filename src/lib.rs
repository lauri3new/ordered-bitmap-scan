use pgrx::prelude::*;
use pgrx::itemptr::{item_pointer_get_both, item_pointer_to_u64};
use pgrx::pg_sys::AsPgCStr;
use pgrx::PgRelation;
use std::collections::HashSet;

pg_module_magic!();

unsafe fn snapshot_buffers() -> pg_sys::BufferUsage {
    std::ptr::addr_of!(pg_sys::pgBufferUsage).read()
}

fn report_buffer_delta(label: &str, before: &pg_sys::BufferUsage, after: &pg_sys::BufferUsage) {
    let mut delta: pg_sys::BufferUsage = unsafe { std::mem::zeroed() };
    unsafe {
        pg_sys::BufferUsageAccumDiff(
            &mut delta,
            after as *const pg_sys::BufferUsage as *mut pg_sys::BufferUsage,
            before as *const pg_sys::BufferUsage as *mut pg_sys::BufferUsage,
        );
    }
    notice!(
        "{}: shared hit={} read={}, local hit={} read={}",
        label,
        delta.shared_blks_hit,
        delta.shared_blks_read,
        delta.local_blks_hit,
        delta.local_blks_read,
    );
}

#[pg_extern]
fn ordered_bitmap_scan(
    table_name: &str,
    driving_index: &str,
    filter_index: &str,
    filter_op: &str,
    filter_value: &str,
    driving_direction: default!(&str, "'forward'"),
    fetch_limit: default!(i64, 100),
    report_buffers: default!(bool, false),
) -> SetOfIterator<'static, pg_sys::ItemPointerData> {
    let limit = fetch_limit.max(0) as usize;

    // Step 1: Resolve names to OIDs, validate, and open relations
    let table_rel = PgRelation::open_with_name_and_share_lock(table_name)
        .unwrap_or_else(|_| error!("table '{}' does not exist", table_name));
    let table_oid = table_rel.oid();

    let driving_rel = PgRelation::open_with_name_and_share_lock(driving_index)
        .unwrap_or_else(|_| error!("index '{}' does not exist", driving_index));
    let driving_oid = driving_rel.oid();

    let filter_rel = PgRelation::open_with_name_and_share_lock(filter_index)
        .unwrap_or_else(|_| error!("index '{}' does not exist", filter_index));
    let filter_oid = filter_rel.oid();

    // Validate indexes belong to the table
    unsafe {
        let driving_indrelid = (*(*driving_rel.as_ptr()).rd_index).indrelid;
        if driving_indrelid != table_oid {
            error!(
                "index '{}' does not belong to table '{}'",
                driving_index, table_name
            );
        }

        let filter_indrelid = (*(*filter_rel.as_ptr()).rd_index).indrelid;
        if filter_indrelid != table_oid {
            error!(
                "index '{}' does not belong to table '{}'",
                filter_index, table_name
            );
        }
    }

    // Validate index types
    const BTREE_AM_OID: pg_sys::Oid = pg_sys::Oid::from_u32(403);
    const GIN_AM_OID: pg_sys::Oid = pg_sys::Oid::from_u32(2742);
    const GIST_AM_OID: pg_sys::Oid = pg_sys::Oid::from_u32(783);

    unsafe {
        let driving_am = (*driving_rel.as_ptr()).rd_rel.as_ref().unwrap().relam;
        if driving_am != BTREE_AM_OID {
            error!("driving index '{}' must be a B-tree index", driving_index);
        }

        let filter_am = (*filter_rel.as_ptr()).rd_rel.as_ref().unwrap().relam;
        if filter_am != GIN_AM_OID && filter_am != GIST_AM_OID {
            error!(
                "filter index '{}' must be a GIN or GiST index",
                filter_index
            );
        }
    }

    // Drop PgRelation wrappers before opening raw handles (they call RelationClose on drop)
    drop(table_rel);
    drop(driving_rel);
    drop(filter_rel);

    unsafe {
        let heap_rel = pg_sys::table_open(table_oid, pg_sys::AccessShareLock as i32);
        let driving_idx = pg_sys::index_open(driving_oid, pg_sys::AccessShareLock as i32);
        let filter_idx = pg_sys::index_open(filter_oid, pg_sys::AccessShareLock as i32);
        let snapshot = pg_sys::GetActiveSnapshot();

        // Step 2: Build scan key for filter index
        let opfamily_oid = *(*filter_idx).rd_opfamily.add(0);
        let _opcintype = *(*filter_idx).rd_opcintype.add(0);

        // Build operator name as a pg List of String nodes
        let op_cstr = filter_op.as_pg_cstr();
        let op_string_node = pg_sys::makeString(op_cstr);
        let cell = pg_sys::ListCell {
            ptr_value: op_string_node as *mut std::ffi::c_void,
        };
        let name_list = pg_sys::list_make1_impl(pg_sys::NodeTag::T_String, cell);

        let tsvector_oid = pg_sys::TSVECTOROID;
        let tsquery_oid = pg_sys::TSQUERYOID;

        let op_oid = pg_sys::OpernameGetOprid(name_list, tsvector_oid, tsquery_oid);
        if op_oid == pg_sys::InvalidOid {
            error!("operator '{}' not found", filter_op);
        }

        let strategy = pg_sys::get_op_opfamily_strategy(op_oid, opfamily_oid);
        if strategy == 0 {
            error!(
                "operator '{}' is not a member of the index's operator family",
                filter_op
            );
        }

        let regproc = pg_sys::get_opcode(op_oid);

        let mut left_type: pg_sys::Oid = pg_sys::InvalidOid;
        let mut right_type: pg_sys::Oid = pg_sys::InvalidOid;
        pg_sys::op_input_types(op_oid, &mut left_type, &mut right_type);

        let mut typinput: pg_sys::Oid = pg_sys::InvalidOid;
        let mut typioparam: pg_sys::Oid = pg_sys::InvalidOid;
        pg_sys::getTypeInputInfo(right_type, &mut typinput, &mut typioparam);

        let value_cstr = filter_value.as_pg_cstr();
        let datum = pg_sys::OidInputFunctionCall(typinput, value_cstr, typioparam, -1);

        let mut scan_key: pg_sys::ScanKeyData = std::mem::zeroed();
        pg_sys::ScanKeyInit(
            &mut scan_key,
            1, // first attribute
            strategy as u16,
            regproc,
            datum,
        );

        // Step 3: Build TID bitmap from filter index
        let buf_before_filter = if report_buffers { Some(snapshot_buffers()) } else { None };

        let tbm = pg_sys::tbm_create(
            (pg_sys::work_mem as usize) * 1024,
            std::ptr::null_mut(),
        );

        let bitmap_scan = pg_sys::index_beginscan_bitmap(
            filter_idx,
            snapshot,
            std::ptr::null_mut(), // no instrumentation
            1,                    // nkeys
        );
        pg_sys::index_rescan(
            bitmap_scan,
            &mut scan_key as *mut pg_sys::ScanKeyData,
            1,
            std::ptr::null_mut(),
            0,
        );
        pg_sys::index_getbitmap(bitmap_scan, tbm);
        pg_sys::index_endscan(bitmap_scan);

        if let Some(ref before) = buf_before_filter {
            let after = snapshot_buffers();
            report_buffer_delta("Filter index scan", before, &after);
        }

        // Capture the filter index's attribute number for lossy recheck
        let filter_attnum = *(*(*filter_idx).rd_index).indkey.values.as_ptr().add(0); // i16 (AttrNumber)

        // Step 4: Convert bitmap to HashSet for O(1) lookup
        let mut tid_set: HashSet<u64> = HashSet::new();
        let mut lossy_blocks: HashSet<pg_sys::BlockNumber> = HashSet::new();

        let iterator = pg_sys::tbm_begin_private_iterate(tbm);
        let mut result: pg_sys::TBMIterateResult = std::mem::zeroed();
        let mut offset_buf: [pg_sys::OffsetNumber; 512] = [0; 512];

        while pg_sys::tbm_private_iterate(iterator, &mut result) {
            if result.lossy {
                lossy_blocks.insert(result.blockno);
            } else {
                let noffsets = pg_sys::tbm_extract_page_tuple(
                    &mut result,
                    offset_buf.as_mut_ptr(),
                    512,
                );
                for i in 0..noffsets as usize {
                    let key = ((result.blockno as u64) << 32) | (offset_buf[i] as u64);
                    tid_set.insert(key);
                }
            }
        }
        pg_sys::tbm_end_private_iterate(iterator);
        pg_sys::tbm_free(tbm);

        // Create a TupleTableSlot for heap fetches (only needed for lossy blocks)
        let slot = if lossy_blocks.is_empty() {
            std::ptr::null_mut()
        } else {
            pg_sys::MakeSingleTupleTableSlot(
                (*heap_rel).rd_att,
                &pg_sys::TTSOpsBufferHeapTuple as *const _ as *mut _,
            )
        };

        // Step 5: Walk B-tree in order, probe bitmap
        let direction = match driving_direction.to_lowercase().as_str() {
            "forward" => pg_sys::ScanDirection::ForwardScanDirection,
            "backward" => pg_sys::ScanDirection::BackwardScanDirection,
            _ => error!("driving_direction must be 'forward' or 'backward'"),
        };

        let buf_before_driving = if report_buffers { Some(snapshot_buffers()) } else { None };

        let btree_scan = pg_sys::index_beginscan(
            heap_rel,
            driving_idx,
            snapshot,
            std::ptr::null_mut(), // no instrumentation
            0,                    // nkeys
            0,                    // norderbys
        );
        pg_sys::index_rescan(
            btree_scan,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            0,
        );

        let mut results: Vec<pg_sys::ItemPointerData> = Vec::with_capacity(limit);
        loop {
            let tid_ptr = pg_sys::index_getnext_tid(btree_scan, direction);
            if tid_ptr.is_null() {
                break;
            }
            let tid = *tid_ptr;
            let key = item_pointer_to_u64(tid);
            let (blockno, _offno) = item_pointer_get_both(tid);

            if tid_set.contains(&key) {
                // Exact bitmap match — no heap access needed
                results.push(tid);
                if results.len() >= limit {
                    break;
                }
            } else if lossy_blocks.contains(&blockno) {
                // Lossy page — must fetch heap tuple and recheck filter condition
                if pg_sys::index_fetch_heap(btree_scan, slot) {
                    pg_sys::slot_getsomeattrs_int(slot, filter_attnum as i32);
                    let col_datum = (*slot).tts_values.add(filter_attnum as usize - 1).read();
                    let col_isnull = (*slot).tts_isnull.add(filter_attnum as usize - 1).read();

                    if !col_isnull {
                        let result_datum = pg_sys::OidFunctionCall2Coll(
                            regproc,
                            pg_sys::InvalidOid,
                            col_datum,
                            datum,
                        );
                        if result_datum != pg_sys::Datum::from(0usize) {
                            results.push(tid);
                            if results.len() >= limit {
                                break;
                            }
                        }
                    }
                }
            }
        }
        pg_sys::index_endscan(btree_scan);

        if !slot.is_null() {
            pg_sys::ExecDropSingleTupleTableSlot(slot);
        }

        if let Some(ref before) = buf_before_driving {
            let after = snapshot_buffers();
            report_buffer_delta("Driving index scan", before, &after);
        }

        if report_buffers {
            notice!(
                "ordered_bitmap_scan: {} matching TIDs returned (limit {})",
                results.len(),
                limit,
            );
        }

        // Step 6: Cleanup and return
        pg_sys::index_close(driving_idx, pg_sys::AccessShareLock as i32);
        pg_sys::index_close(filter_idx, pg_sys::AccessShareLock as i32);
        pg_sys::table_close(heap_rel, pg_sys::AccessShareLock as i32);

        SetOfIterator::new(results)
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_ordered_bitmap_scan_basic() {
        // Create test table and indexes
        Spi::run(
            "CREATE TABLE test_obs (
                id serial PRIMARY KEY,
                created_at timestamptz NOT NULL DEFAULT now(),
                body text NOT NULL,
                body_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
            )",
        )
        .unwrap();
        Spi::run("CREATE INDEX idx_test_created ON test_obs (created_at)").unwrap();
        Spi::run("CREATE INDEX idx_test_tsv ON test_obs USING gin (body_tsv)").unwrap();

        Spi::run(
            "INSERT INTO test_obs (created_at, body)
             SELECT '2024-01-01'::timestamptz + (i || ' minutes')::interval,
                    CASE WHEN i % 3 = 0 THEN 'the quick brown fox jumps over the lazy dog'
                         WHEN i % 3 = 1 THEN 'hello world from postgresql extension'
                         ELSE 'random text about weather patterns'
                    END
             FROM generate_series(1, 300) AS i",
        )
        .unwrap();

        // Our function should return results
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM ordered_bitmap_scan(
                'test_obs', 'idx_test_created', 'idx_test_tsv',
                '@@', 'fox & brown', 'forward', 10
            )",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 10);
    }

    #[pg_test]
    fn test_ordered_bitmap_scan_with_buffer_reporting() {
        Spi::run(
            "CREATE TABLE test_obs_buf (
                id serial PRIMARY KEY,
                created_at timestamptz NOT NULL DEFAULT now(),
                body text NOT NULL,
                body_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
            )",
        )
        .unwrap();
        Spi::run("CREATE INDEX idx_buf_created ON test_obs_buf (created_at)").unwrap();
        Spi::run("CREATE INDEX idx_buf_tsv ON test_obs_buf USING gin (body_tsv)").unwrap();

        Spi::run(
            "INSERT INTO test_obs_buf (created_at, body)
             SELECT '2024-01-01'::timestamptz + (i || ' minutes')::interval,
                    CASE WHEN i % 3 = 0 THEN 'the quick brown fox jumps over the lazy dog'
                         WHEN i % 3 = 1 THEN 'hello world from postgresql extension'
                         ELSE 'random text about weather patterns'
                    END
             FROM generate_series(1, 300) AS i",
        )
        .unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM ordered_bitmap_scan(
                'test_obs_buf', 'idx_buf_created', 'idx_buf_tsv',
                '@@', 'fox & brown', 'forward', 10, true
            )",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 10);
    }

    #[pg_test]
    fn test_ordered_bitmap_scan_lossy() {
        // Force lossy bitmaps by setting work_mem very low
        Spi::run("SET work_mem = '64kB'").unwrap();

        Spi::run(
            "CREATE TABLE test_obs_lossy (
                id serial PRIMARY KEY,
                created_at timestamptz NOT NULL DEFAULT now(),
                body text NOT NULL,
                body_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
            )",
        )
        .unwrap();
        Spi::run("CREATE INDEX idx_lossy_created ON test_obs_lossy (created_at)").unwrap();
        Spi::run("CREATE INDEX idx_lossy_tsv ON test_obs_lossy USING gin (body_tsv)").unwrap();

        // Insert enough rows so the bitmap exceeds 64kB and goes lossy
        // ~3,333 matches out of 10,000 rows
        Spi::run(
            "INSERT INTO test_obs_lossy (created_at, body)
             SELECT '2024-01-01'::timestamptz + (i || ' seconds')::interval,
                    CASE WHEN i % 3 = 0 THEN 'the quick brown fox jumps over the lazy dog'
                         WHEN i % 3 = 1 THEN 'hello world from postgresql extension'
                         ELSE 'random text about weather patterns'
                    END
             FROM generate_series(1, 10000) AS i",
        )
        .unwrap();

        // Get the true count from a regular query
        let expected = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_obs_lossy WHERE body_tsv @@ to_tsquery('english', 'fox & brown')",
        )
        .unwrap()
        .unwrap();

        // Get the count from ordered_bitmap_scan with a high limit
        let actual = Spi::get_one::<i64>(
            "SELECT count(*) FROM ordered_bitmap_scan(
                'test_obs_lossy', 'idx_lossy_created', 'idx_lossy_tsv',
                '@@', 'fox & brown', 'forward', 10000
            )",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            actual, expected,
            "lossy bitmap recheck: ordered_bitmap_scan returned {} rows but expected {}",
            actual, expected
        );
    }
}
