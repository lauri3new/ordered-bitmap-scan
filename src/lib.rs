use pgrx::prelude::*;
use pgrx::itemptr::{item_pointer_get_both, item_pointer_to_u64};
use pgrx::pg_sys::AsPgCStr;
use pgrx::PgRelation;
use std::collections::HashSet;

pg_module_magic!();

#[pg_extern]
fn ordered_bitmap_scan(
    table_name: &str,
    driving_index: &str,
    filter_index: &str,
    filter_op: &str,
    filter_value: &str,
    driving_direction: default!(&str, "'forward'"),
    fetch_limit: default!(i64, 100),
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
        let opcintype = *(*filter_idx).rd_opcintype.add(0);

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

        // Step 5: Walk B-tree in order, probe bitmap
        let direction = match driving_direction.to_lowercase().as_str() {
            "forward" => pg_sys::ScanDirection::ForwardScanDirection,
            "backward" => pg_sys::ScanDirection::BackwardScanDirection,
            _ => error!("driving_direction must be 'forward' or 'backward'"),
        };

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

            if tid_set.contains(&key) || lossy_blocks.contains(&blockno) {
                results.push(tid);
                if results.len() >= limit {
                    break;
                }
            }
        }
        pg_sys::index_endscan(btree_scan);

        // Step 6: Cleanup and return
        pg_sys::index_close(driving_idx, pg_sys::AccessShareLock as i32);
        pg_sys::index_close(filter_idx, pg_sys::AccessShareLock as i32);
        pg_sys::table_close(heap_rel, pg_sys::AccessShareLock as i32);

        SetOfIterator::new(results)
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
}
