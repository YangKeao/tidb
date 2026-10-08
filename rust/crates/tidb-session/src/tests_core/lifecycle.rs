//! A session from SQL strings alone, and the statement routing that gets
//! each kind to its executor -- Go `pkg/session`'s `ExecuteStmt`.

use crate::tests_support::*;
use crate::*;
use std::sync::Arc;

#[derive(Default)]
struct NoopMemStateRecorder;

impl tidb_util::memory::RecordMemState for NoopMemStateRecorder {
    fn load(&self) -> Result<Option<tidb_util::memory::RuntimeMemStateV1>, String> {
        Ok(None)
    }

    fn store(&self, _: &tidb_util::memory::RuntimeMemStateV1) -> Result<(), String> {
        Ok(())
    }
}

fn test_mem_arbitrator() -> Arc<tidb_util::memory::MemArbitrator> {
    let arbitrator =
        tidb_util::memory::MemArbitrator::new(1024, 4, 3, 0, Box::new(NoopMemStateRecorder));
    assert!(arbitrator.auto_run(
        tidb_util::memory::MemArbitratorActions::default(),
        tidb_util::memory::DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
        4,
        tidb_util::memory::DEF_TASK_TICK_DUR,
    ));
    arbitrator.set_work_mode(tidb_util::memory::ArbitratorWorkMode::Standard);
    arbitrator
}

#[test]
fn server_spill_authority_reaches_every_statement_context() {
    let path = std::env::temp_dir().join(format!(
        "tidb-session-spill-authority-{}",
        std::process::id()
    ));
    let storage = Arc::new(
        tidb_util::spill_storage::SpillStorage::open(tidb_util::spill_storage::SpillStorageSpec {
            path: path.clone(),
            quota_bytes: -1,
            encryption: tidb_util::spill_storage::SpillEncryptionMethod::Aes128Ctr,
        })
        .unwrap(),
    );
    let mut session = Session::new();
    session.set_spill_storage(Arc::clone(&storage));

    for context in [
        session.statement_context(false),
        session.statement_context(true),
    ] {
        let inherited = context.statement_memory().spill_storage();
        assert_eq!(inherited.path(), path);
        assert_eq!(
            inherited.encryption(),
            tidb_util::spill_storage::SpillEncryptionMethod::Aes128Ctr
        );
    }

    drop(session);
    drop(storage);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn long_data_uses_live_query_quota_and_releases_session_bytes() {
    let mut session = Session::new();
    session.set_connection_id(42);
    session.run("SET @@tidb_mem_quota_query = 1024").unwrap();

    assert!(session.try_consume_long_data(600));
    assert_eq!(session.session_memory_bytes_consumed(), 600);
    assert!(
        !session.try_consume_long_data(424),
        "reaching the quota is refused"
    );
    assert_eq!(session.session_memory_bytes_consumed(), 600);

    session.release_long_data(600);
    assert_eq!(session.session_memory_bytes_consumed(), 0);
    assert!(session.try_consume_long_data(1023));
    session.release_long_data(1023);
    assert_eq!(session.session_memory_bytes_consumed(), 0);
}

#[test]
fn statement_contexts_keep_one_session_memory_root() {
    let session = Session::new();
    let first = session.statement_context(false).statement_memory();
    let second = session.statement_context(true).statement_memory();

    assert!(Arc::ptr_eq(
        first.session_tracker(),
        second.session_tracker()
    ));
    assert!(!Arc::ptr_eq(first.stmt_tracker(), second.stmt_tracker()));

    let retained = first.operator_tracker(917);
    retained.consume(128);
    assert_eq!(
        second.bytes_consumed(),
        128,
        "a retained result from the preceding statement remains under this connection's quota"
    );
    retained.consume(-128);
}

#[test]
fn statement_context_maps_session_arbitrator_variables_to_its_root_pool() {
    let arbitrator = test_mem_arbitrator();
    let mut session = Session::new();
    session.set_connection_id(211);
    session.set_mem_arbitrator(Arc::clone(&arbitrator));
    session
        .vars
        .set_system(
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_QUERY_RESERVED,
            "64".to_owned(),
        )
        .unwrap();

    let reserved = session.statement_context(false).statement_memory();
    let pool = arbitrator
        .find_root_pool(211)
        .entry
        .expect("a session statement must reserve an arbitrated root pool");
    assert!(pool.pool().capacity() >= 64);
    reserved.finish_statement();

    session
        .vars
        .set_system(
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_WAIT_AVERSE,
            "nolimit".to_owned(),
        )
        .unwrap();
    let bypass = session.statement_context(true).statement_memory();
    bypass.operator_tracker(11).consume(8);
    assert_eq!(
        pool.pool().capacity(),
        0,
        "nolimit must not register or reserve a new global-arbitrator budget"
    );
    bypass.finish_statement();
    assert!(arbitrator.stop());
}

#[test]
fn apply_cache_quota_reaches_query_and_dml_statement_contexts() {
    let mut session = Session::new();
    assert_eq!(
        session.statement_context(false).apply_cache_capacity(),
        tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE
    );

    session
        .run("SET @@tidb_mem_quota_apply_cache = 12345")
        .unwrap();
    assert_eq!(
        session.statement_context(false).apply_cache_capacity(),
        12345
    );
    assert_eq!(
        session.statement_context(true).apply_cache_capacity(),
        12345
    );
}

#[test]
fn optimizer_cost_variables_reach_the_statement_snapshot() {
    let mut session = Session::new();
    for (name, value) in [
        ("tidb_executor_concurrency", "13"),
        ("tidb_hashagg_partial_concurrency", "-1"),
        ("tidb_hashagg_final_concurrency", "7"),
        ("tidb_hash_join_concurrency", "-1"),
        ("tidb_projection_concurrency", "-1"),
        ("tidb_index_lookup_join_concurrency", "7"),
        ("tidb_distsql_scan_concurrency", "19"),
        ("tidb_index_join_batch_size", "123"),
        ("tidb_opt_hash_join_cost_factor", "2.5"),
        ("tidb_opt_merge_join_cost_factor", "0.5"),
        ("tidb_opt_sort_cost_factor", "3.25"),
    ] {
        session.vars.set_system(name, value.to_owned()).unwrap();
    }

    let ctx = session.statement_context(false);
    let env = ctx.optimizer_cost_env();
    assert_eq!(env.session.hash_join_concurrency, 13.0);
    assert_eq!(env.session.projection_concurrency, 13.0);
    assert_eq!(env.session.index_lookup_join_concurrency, 7.0);
    assert_eq!(env.session.distsql_scan_concurrency, 19.0);
    assert_eq!(env.session.index_join_batch_size, 123.0);
    assert_eq!(ctx.hashagg_concurrency(), (13, 7));
    assert_eq!(env.cost_factors.hash_join, 2.5);
    assert_eq!(env.cost_factors.merge_join, 0.5);
    assert_eq!(env.cost_factors.sort, 3.25);
}

#[test]
fn advanced_join_reorder_switch_reaches_every_statement_context() {
    let mut session = Session::new();
    assert!(session.statement_context(false).advanced_join_reorder());
    assert!(session.statement_context(true).advanced_join_reorder());

    session
        .run("SET @@tidb_opt_enable_advanced_join_reorder = OFF")
        .unwrap();
    assert!(!session.statement_context(false).advanced_join_reorder());
    assert!(!session.statement_context(true).advanced_join_reorder());

    session
        .run("SET @@tidb_opt_enable_advanced_join_reorder = ON")
        .unwrap();
    assert!(session.statement_context(false).advanced_join_reorder());
}

#[test]
fn either_plan_replayer_capture_switch_enables_statement_statistics_capture() {
    let mut session = Session::new();
    assert!(session
        .statement_context(false)
        .plan_replayer_capture_enabled());

    session
        .run("SET @@tidb_enable_plan_replayer_capture = OFF")
        .unwrap();
    assert!(!session
        .statement_context(false)
        .plan_replayer_capture_enabled());

    session
        .run("SET GLOBAL tidb_enable_historical_stats = ON")
        .unwrap();
    session
        .run("SET @@tidb_enable_plan_replayer_continuous_capture = ON")
        .unwrap();
    assert!(session
        .statement_context(false)
        .plan_replayer_capture_enabled());
}

/// A whole session lifecycle from SQL strings alone: DDL, writes, reads.
#[test]
fn session_runs_a_sql_lifecycle() {
    let mut session = Session::new();
    assert_eq!(
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap(),
        StmtResult::Done(true)
    );
    assert_eq!(
        session
            .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap(),
        StmtResult::Affected(3)
    );
    assert_eq!(
        session
            .run("SELECT a + b FROM t WHERE a >= 2 ORDER BY a DESC LIMIT 1")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(33)]])
    );
    // A second table coexists in the same catalog.
    session.run("CREATE TABLE u (x BIGINT)").unwrap();
    session.run("INSERT INTO u VALUES (42)").unwrap();
    assert_eq!(
        session.run("SELECT x FROM u").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(42)]])
    );
}

/// One connection sends everything through one door: the transaction
/// controls, `SET`, and `SHOW VARIABLES` all answer from `run` now.
///
/// Checked against captured TiDB output: the columns are
/// `Variable_name` and `Value`, the LIKE pattern filters, and a SET is
/// visible to the next SHOW.
#[test]
fn run_routes_session_statements() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();

    // The transaction controls answer through `run`.
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(row_text(session.run("SELECT a FROM t")), [["1"]]);
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO t VALUES (2)").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(row_text(session.run("SELECT a FROM t")), [["1"]]);

    // So does SET.
    session.run("SET autocommit = 0").unwrap();

    // Captured: SHOW VARIABLES reports Variable_name/Value, filtered.
    match session
        .run_with_columns("SHOW VARIABLES LIKE 'autocommit'")
        .unwrap()
    {
        StmtOutput::Rows { columns, rows } => {
            assert_eq!(
                columns
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                ["Variable_name", "Value"]
            );
            assert_eq!(rows.len(), 1);
            assert_eq!(datum_text(&rows[0][0]).unwrap(), "autocommit");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    // Captured: sql_mode reports the session's own value.
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'sql_mode'")),
        [[
            "sql_mode".to_owned(),
            session.vars().get_system("sql_mode").unwrap()
        ]]
    );
    // Captured: a wildcard pattern matches a prefix family.
    let matched = row_text(session.run("SHOW VARIABLES LIKE 'max_allowed%'"));
    assert!(
        matched.iter().any(|row| row[0] == "max_allowed_packet"),
        "{matched:?}"
    );
    // A SET is visible to the next SHOW.
    session.run("SET autocommit = 1").unwrap();
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'autocommit'"))[0][1],
        session.vars().get_system("autocommit").unwrap()
    );

    // Captured: the scoped spellings a JDBC client sends read the same
    // session value here.
    assert_eq!(
        row_text(session.run("SELECT @@session.autocommit, @@global.autocommit")).len(),
        1
    );

    // Captured: the WHERE form filters the same virtual rows, including
    // over the Value column and with a case-insensitive column name.
    assert_eq!(
        row_text(session.run("SHOW VARIABLES WHERE variable_name = 'autocommit'"))[0][0],
        "autocommit"
    );
    let pair =
        row_text(session.run("SHOW VARIABLES WHERE Variable_name IN ('autocommit','sql_mode')"));
    assert_eq!(pair.len(), 2, "{pair:?}");
    assert_eq!(pair[0][0], "autocommit");
    assert_eq!(pair[1][0], "sql_mode");
    let both =
        row_text(session.run("SHOW VARIABLES WHERE value = 'ON' AND variable_name LIKE 'auto%'"));
    assert!(both.iter().any(|row| row[0] == "autocommit"), "{both:?}");
}

#[test]
fn parser_only_dml_modifiers_do_not_change_the_write() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT)").unwrap();
    // Go parses QUICK but neither its planner nor executor reads it.
    assert_eq!(
        session.run("DELETE QUICK FROM t").unwrap(),
        StmtResult::Affected(0)
    );
    // RETURNING is likewise parsed and silently ignored, so the insert lands
    // with a plain OK rather than a result set.
    assert_eq!(
        session
            .run("INSERT INTO t (a) VALUES (1) RETURNING a")
            .unwrap(),
        StmtResult::Affected(1)
    );
}

#[test]
fn compatibility_only_alter_specs_leave_the_table_unchanged() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();

    for sql in [
        "ALTER TABLE t LOCK = EXCLUSIVE",
        "ALTER TABLE t DISABLE KEYS",
        "ALTER TABLE t ENABLE KEYS",
    ] {
        assert_eq!(session.run(sql).unwrap(), StmtResult::Affected(0), "{sql}");
    }
    session
        .run("ALTER TABLE t LOCK = NONE, ADD COLUMN b BIGINT DEFAULT 9")
        .unwrap();

    session.run("INSERT INTO t (a) VALUES (7)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b FROM t")),
        vec![vec!["7", "9"]]
    );
}

#[test]
fn alter_validation_specs_warn_and_do_not_change_the_table() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();

    for (sql, message) in [
        (
            "ALTER TABLE t WITH VALIDATION",
            "ALTER TABLE WITH VALIDATION is currently unsupported",
        ),
        (
            "ALTER TABLE t WITHOUT VALIDATION",
            "ALTER TABLE WITHOUT VALIDATION is currently unsupported",
        ),
    ] {
        assert_eq!(session.run(sql).unwrap(), StmtResult::Affected(0), "{sql}");
        assert_eq!(warnings_of(&session), vec![(8200, message.to_owned())]);
    }
    session
        .run(
            "ALTER TABLE t WITHOUT VALIDATION, WITH VALIDATION, \
             ADD COLUMN b BIGINT DEFAULT 12",
        )
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![
            (
                8200,
                "ALTER TABLE WITHOUT VALIDATION is currently unsupported".to_owned(),
            ),
            (
                8200,
                "ALTER TABLE WITH VALIDATION is currently unsupported".to_owned(),
            ),
        ]
    );

    session.run("INSERT INTO t (a) VALUES (11)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b FROM t")),
        vec![vec!["11", "12"]]
    );
}

/// UPDATE and DELETE run through the session like any other write, and
/// report their affected-row counts.
#[test]
fn update_and_delete_through_the_session() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    assert_eq!(
        session.run("UPDATE t SET a = a * 10 WHERE a > 1").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        session.run("DELETE FROM t WHERE a >= 20").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    // Both are classified as writes, so the wire answers with an OK packet.
    assert_eq!(
        session.statement_kind("UPDATE t SET a = 1").unwrap(),
        StmtKind::Write
    );
    assert_eq!(
        session.statement_kind("DELETE FROM t").unwrap(),
        StmtKind::Write
    );
}

#[test]
fn ready_value_sql_columns_use_lane_cache_for_null_empty_binary_and_utf8() {
    let mut session = Session::new();
    // The serial projection lane owns and reuses its operation workers.
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE ascii_one_slot (id INT PRIMARY KEY, v VARBINARY(8))")
        .unwrap();
    session
        .run("INSERT INTO ascii_one_slot VALUES (1,NULL),(2,X''),(3,X'FF'),(4,X'C3A9'),(5,X'E4B8AD'),(6,X'41')")
        .unwrap();

    // v is a stored column, not a foldable literal. Multibyte values answer the
    // first encoded byte (195/228), not the Unicode code point (233/20013).
    for _ in 0..2 {
        let output = session
            .run_with_columns("SELECT ASCII(v) FROM ascii_one_slot ORDER BY id")
            .unwrap();
        let StmtOutput::Rows { rows, .. } = output else {
            panic!("expected SQL ASCII rows")
        };
        assert_eq!(
            rows,
            vec![
                vec![Datum::Null],
                vec![Datum::Int(0)],
                vec![Datum::Int(255)],
                vec![Datum::Int(195)],
                vec![Datum::Int(228)],
                vec![Datum::Int(65)],
            ]
        );
    }
}



























#[test]
fn ready_value_lane_cache_mixed_string_ops_on_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_string_ops (id INT PRIMARY KEY, v VARBINARY(8), h VARBINARY(8))")
        .unwrap();
    session
        .run(
            "INSERT INTO shared_string_ops VALUES \
              (1,NULL,NULL),(2,X'',X''),(3,X'20FF2020','F'),\
              (4,X'2020C3A92020','c3a9'),(5,X'202020','ABC'),\
              (6,X'41','0G'),(7,X'FF',X'FF')",
        )
        .unwrap();

    // Stored columns prevent constant folding. Alternating integer/byte results
    // exercises op replacement in one slot; OCTET_LENGTH shares LENGTH's op.
    let output = session
        .run_with_columns(
            "SELECT LENGTH(v), LTRIM(v), BIT_LENGTH(v), RTRIM(v), \
             OCTET_LENGTH(v), UNHEX(h) FROM shared_string_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { rows, .. } = output else {
        panic!("expected mixed shared-kernel SQL rows")
    };
    // Chunk row materialization uses SetString plus the declared collation for
    // VARBINARY/BLOB (tidb-chunk row.rs), not the pre-chunk Datum::Bytes variant.
    let binary_string =
        |bytes: Vec<u8>| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 6],
            vec![
                Datum::Int(0),
                binary_string(vec![]),
                Datum::Int(0),
                binary_string(vec![]),
                Datum::Int(0),
                binary_string(vec![]),
            ],
            vec![
                Datum::Int(4),
                binary_string(vec![0xff, b' ', b' ']),
                Datum::Int(32),
                binary_string(vec![b' ', 0xff]),
                Datum::Int(4),
                binary_string(vec![0x0f]),
            ],
            vec![
                Datum::Int(6),
                binary_string(vec![0xc3, 0xa9, b' ', b' ']),
                Datum::Int(48),
                binary_string(vec![b' ', b' ', 0xc3, 0xa9]),
                Datum::Int(6),
                binary_string(vec![0xc3, 0xa9]),
            ],
            vec![
                Datum::Int(3),
                binary_string(vec![]),
                Datum::Int(24),
                binary_string(vec![]),
                Datum::Int(3),
                binary_string(vec![0x0a, 0xbc]),
            ],
            vec![
                Datum::Int(1),
                binary_string(b"A".to_vec()),
                Datum::Int(8),
                binary_string(b"A".to_vec()),
                Datum::Int(1),
                Datum::Null,
            ],
            vec![
                Datum::Int(1),
                binary_string(vec![0xff]),
                Datum::Int(8),
                binary_string(vec![0xff]),
                Datum::Int(1),
                Datum::Null,
            ],
        ]
    );
}



#[test]
fn ready_value_lane_cache_crc_reverse_char_length_quote_sql_values() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_text_ops (id INT PRIMARY KEY, c VARBINARY(16), \
             b VARBINARY(16), t VARCHAR(16) CHARACTER SET utf8mb4, q VARBINARY(16))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_text_ops VALUES \
             (1,NULL,NULL,NULL,NULL),(2,X'',X'','',X''),\
             (3,'123456789',X'C3A9E4B8AD','é中',X'275C001AFF')",
        )
        .unwrap();

    // Dynamic stored columns distinguish binary byte reversal/counting from
    // UTF-8 character semantics. HEX inspects payloads without confusing Chunk
    // SetString materialization with the pre-chunk Datum::Bytes representation.
    let output = session
        .run_with_columns(
            "SELECT CRC32(c), HEX(REVERSE(b)), HEX(REVERSE(t)), CHAR_LENGTH(b), \
             CHARACTER_LENGTH(t), CHAR_LENGTH(t), CHARACTER_LENGTH(b), \
             HEX(QUOTE(q)), QUOTE(q) FROM shared_text_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected mixed CRC/string SQL rows")
    };
    let payloads: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row[..8].iter().map(cell_text).collect())
        .collect();
    assert_eq!(
        payloads,
        [
            ["NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "4E554C4C"],
            ["0", "", "", "0", "0", "0", "0", "2727"],
            [
                "3421780262",
                "ADB8E4A9C3",
                "E4B8ADC3A9",
                "5",
                "2",
                "2",
                "5",
                "275C275C5C5C305C5AEFBFBD27",
            ],
        ]
    );
    // The existing rewriter/result_type.rs declares CRC32 as signed LongLong.
    // Preserve that SQL/Chunk contract; the raw evaluator's UInt packing is
    // checked separately by next_bytes_dispatch_ rather than changing inference.
    assert!(!columns[0].1.is_unsigned());
    assert_eq!(rows[1][0], Datum::Int(0));
    assert_eq!(rows[2][0], Datum::Int(3_421_780_262));
    // QUOTE(NULL) is the four-byte string NULL, not SQL NULL. The binary
    // argument's declared collation survives the ordinary Chunk boundary.
    match &rows[0][8] {
        Datum::String(value) => {
            assert_eq!(value.bytes(), b"NULL");
            assert_eq!(value.collation(), tidb_datatype::Collation::Binary);
        }
        other => panic!("QUOTE(NULL column) must be a materialized string: {other:?}"),
    }
    // The last HEX value also pins quote escapes for apostrophe, backslash,
    // NUL and Ctrl-Z, plus the existing single-FF -> U+FFFD normalization.
    // Exhaustive malformed-sequence and PB/legacy cases belong to D's tests.
}



#[test]
fn ready_value_lane_cache_hex_bin_left_right_replace_sql_values() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_shape_ops (id INT PRIMARY KEY, u BIGINT UNSIGNED, \
             k BIT(16), b VARBINARY(16), t VARCHAR(16) CHARACTER SET utf8mb4, \
             n BIGINT, f VARBINARY(8), r VARBINARY(8))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_shape_ops VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,0,0,X'','',0,X'','X'),\
             (3,9223372036854775808,65,X'C3A9E4B8AD','é中',1,X'C3A9',X'FF'),\
             (4,18446744073709551615,256,X'C3A9E4B8AD','é中',-1,X'',X'FF'),\
             (5,1,1,'ababa','ababa',99,'aba','Z')",
        )
        .unwrap();

    // Every expression consumes a stored column. One slot switches between
    // Int(bits), Bytes, BytesInt and Bytes3, including nested migrated HEX.
    // A declared BIT column takes HEX's integer signature, dropping width zeros.
    let output = session
        .run_with_columns(
            "SELECT HEX(u), HEX(k), HEX(b), BIN(u), HEX(LEFT(b,n)), HEX(RIGHT(b,n)), \
             HEX(LEFT(t,n)), HEX(RIGHT(t,n)), HEX(REPLACE(b,f,r)), \
             HEX(REPLACE(t,'é','X')) FROM shared_shape_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { rows, .. } = output else {
        panic!("expected mixed input-shape SQL rows")
    };
    let payloads: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(cell_text).collect())
        .collect();
    let high_bit = format!("1{}", "0".repeat(63));
    let all_bits = "1".repeat(64);
    assert_eq!(
        payloads,
        [
            ["NULL"; 10],
            ["0", "0", "", "0", "", "", "", "", "", ""],
            [
                "8000000000000000",
                "41",
                "C3A9E4B8AD",
                high_bit.as_str(),
                "C3",
                "AD",
                "C3A9",
                "E4B8AD",
                "FFE4B8AD",
                "58E4B8AD",
            ],
            [
                "FFFFFFFFFFFFFFFF",
                "100",
                "C3A9E4B8AD",
                all_bits.as_str(),
                "",
                "",
                "",
                "",
                "C3A9E4B8AD",
                "58E4B8AD",
            ],
            [
                "1",
                "1",
                "6162616261",
                "1",
                "6162616261",
                "6162616261",
                "6162616261",
                "6162616261",
                "5A6261",
                "6162616261",
            ],
        ]
    );
}



#[test]
fn ready_value_lane_cache_bitwise_sql_values_and_original_metadata() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_bit_ops (id INT PRIMARY KEY, u BIGINT UNSIGNED, s BIGINT, n BIGINT)")
        .unwrap();
    session
        .run(
            "INSERT INTO shared_bit_ops VALUES \
             (1,NULL,NULL,NULL),(2,0,0,0),(3,9223372036854775808,-1,0),\
             (4,18446744073709551615,-9223372036854775808,63),\
             (5,1,-1,64),(6,9223372036854775808,0,-1)",
        )
        .unwrap();

    // Actual operator syntax plus the named BIT_COUNT call, all on columns.
    // The same one-slot root switches between nullable Int and Int2 recipes.
    let output = session
        .run_with_columns(
            "SELECT u & s, u | s, u ^ s, ~s, u << n, u >> n, \
             BIT_COUNT(s), BIT_COUNT(u) FROM shared_bit_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected bitwise SQL rows")
    };
    assert_eq!(columns.len(), 8);
    // These are the unchanged builtin_op/result_type contracts: six bitwise
    // expressions are unsigned LongLong; BIT_COUNT is signed, with flen 2.
    for (_, field_type) in &columns[..6] {
        assert!(field_type.is_unsigned());
    }
    for (_, field_type) in &columns[6..] {
        assert!(!field_type.is_unsigned());
    }
    let high = 0x8000_0000_0000_0000_u64;
    let low = 0x7fff_ffff_ffff_ffff_u64;
    let all = u64::MAX;
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 8],
            vec![
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::UInt(all),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::Int(0),
                Datum::Int(0),
            ],
            vec![
                Datum::UInt(high),
                Datum::UInt(all),
                Datum::UInt(low),
                Datum::UInt(0),
                Datum::UInt(high),
                Datum::UInt(high),
                Datum::Int(64),
                Datum::Int(1),
            ],
            vec![
                Datum::UInt(high),
                Datum::UInt(all),
                Datum::UInt(low),
                Datum::UInt(low),
                Datum::UInt(high),
                Datum::UInt(1),
                Datum::Int(1),
                Datum::Int(64),
            ],
            vec![
                Datum::UInt(1),
                Datum::UInt(all),
                Datum::UInt(all - 1),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::Int(64),
                Datum::Int(1),
            ],
            vec![
                Datum::UInt(0),
                Datum::UInt(high),
                Datum::UInt(high),
                Datum::UInt(all),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::Int(0),
                Datum::Int(1),
            ],
        ]
    );
}



#[test]
fn ready_value_lane_cache_predicate_sql_values_and_null_truth_table() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_pred_ops (id INT PRIMARY KEY, v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO shared_pred_ops VALUES (1,NULL),(2,0),(3,2),(4,-3)")
        .unwrap();

    // IS [NOT] UNKNOWN is already parsed as IS [NOT] NULL. The internal
    // ISTRUE_WITH_NULL name has no ordinary SQL return-type arm, so its direct
    // entry remains a native/PB test responsibility rather than a new spelling.
    let cases: [(&str, [Option<i64>; 4]); 13] = [
        ("NOT v", [None, Some(1), Some(0), Some(0)]),
        ("!v", [None, Some(1), Some(0), Some(0)]),
        ("ISNULL(v)", [Some(1), Some(0), Some(0), Some(0)]),
        ("v IS NULL", [Some(1), Some(0), Some(0), Some(0)]),
        ("v IS NOT NULL", [Some(0), Some(1), Some(1), Some(1)]),
        ("ISTRUE(v)", [Some(0), Some(0), Some(1), Some(1)]),
        ("ISFALSE(v)", [Some(0), Some(1), Some(0), Some(0)]),
        ("v IS TRUE", [Some(0), Some(0), Some(1), Some(1)]),
        ("v IS NOT TRUE", [Some(1), Some(1), Some(0), Some(0)]),
        ("v IS FALSE", [Some(0), Some(1), Some(0), Some(0)]),
        ("v IS NOT FALSE", [Some(1), Some(0), Some(1), Some(1)]),
        ("v IS UNKNOWN", [Some(1), Some(0), Some(0), Some(0)]),
        ("v IS NOT UNKNOWN", [Some(0), Some(1), Some(1), Some(1)]),
    ];
    let projection = cases
        .iter()
        .map(|(expression, _)| *expression)
        .collect::<Vec<_>>()
        .join(", ");
    let output = session
        .run_with_columns(&format!(
            "SELECT {projection} FROM shared_pred_ops ORDER BY id"
        ))
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected predicate SQL rows")
    };
    assert_eq!(columns.len(), cases.len());
    assert_eq!(rows.len(), 4);
    for row in &rows {
        assert_eq!(row.len(), cases.len());
    }
    for (column_index, (expression, expected)) in cases.iter().enumerate() {
        // The existing SQL descriptors are signed boolean integers, unlike
        // the unsigned bitwise results in the preceding batch.
        assert!(!columns[column_index].1.is_unsigned(), "{expression}");
        for (row_index, &value) in expected.iter().enumerate() {
            assert_eq!(
                rows[row_index][column_index],
                value.map_or(Datum::Null, Datum::Int),
                "{expression}, fixture id {}",
                row_index + 1
            );
        }
    }
}



#[test]
fn ready_value_lane_cache_md5_sha_sha1_sql_binary_and_text_values() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_hash_ops (id INT PRIMARY KEY, b VARBINARY(16), \
             t VARCHAR(16) CHARACTER SET utf8mb4)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_hash_ops VALUES \
             (1,NULL,NULL),(2,X'',''),(3,X'616263','abc'),\
             (4,X'FF','é中'),(5,X'006100FF','A')",
        )
        .unwrap();

    let output = session
        .run_with_columns(
            "SELECT MD5(b), SHA(b), SHA1(b), MD5(t), SHA(t), SHA1(t) \
             FROM shared_hash_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected hash SQL rows")
    };
    assert_eq!(columns.len(), 6);

    // Independent Python hashlib.md5/sha1 constants over these literal bytes,
    // not fixtures recorded from the TiKV kernels. SHA and SHA1 share an
    // algorithm, but both SQL spellings must consume the actual column bytes.
    let empty = (
        "d41d8cd98f00b204e9800998ecf8427e",
        "da39a3ee5e6b4b0d3255bfef95601890afd80709",
    );
    let abc = (
        "900150983cd24fb0d6963f7d28e17f72",
        "a9993e364706816aba3e25717850c26c9cd0d89d",
    );
    let ff = (
        "00594fd4f42ba43fc1ca0427a0576295",
        "85e53271e14006f0265921d02d4d736cdc580b0b",
    );
    let utf8 = (
        "f06fd4f6fa9601b2b8d793ff0f2e65f1",
        "57f448f2872fa262f717efb9545e3ee4fe30e3a1",
    );
    let embedded_nul = (
        "09894eaf397901c43b954e0452edb2d7",
        "8c6f623d8416a0d6efdbe626b413b8a849346932",
    );
    let upper_a = (
        "7fc56270e7a70fa81a5935b72eacbe29",
        "6dcd4ce23d88e2ee9568ba546c007c63d9131c1b",
    );
    let expected = [
        ["NULL"; 6],
        [empty.0, empty.1, empty.1, empty.0, empty.1, empty.1],
        [abc.0, abc.1, abc.1, abc.0, abc.1, abc.1],
        [ff.0, ff.1, ff.1, utf8.0, utf8.1, utf8.1],
        [
            embedded_nul.0,
            embedded_nul.1,
            embedded_nul.1,
            upper_a.0,
            upper_a.1,
            upper_a.1,
        ],
    ];
    assert_eq!(rows.len(), expected.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected).enumerate() {
        assert_eq!(row.len(), expected.len());
        for (column_index, (value, expected)) in row.iter().zip(expected).enumerate() {
            assert_eq!(
                crate::tests_support::cell_text(value),
                expected,
                "hash fixture id {}, column {column_index}",
                row_index + 1
            );
        }
    }
}



#[test]
fn ready_value_lane_cache_logical_sql_full_three_valued_table() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_logic_ops (id INT PRIMARY KEY, a BIGINT, b BIGINT)")
        .unwrap();
    session
        .run(
            "INSERT INTO shared_logic_ops VALUES \
             (1,NULL,NULL),(2,NULL,0),(3,NULL,-3),\
             (4,0,NULL),(5,0,0),(6,0,-3),\
             (7,2,NULL),(8,2,0),(9,2,-3)",
        )
        .unwrap();

    // Stored nullable operands keep the complete 3x3 truth table live at
    // evaluation; 2 and -3 also cover nonzero values other than boolean 1.
    let output = session
        .run_with_columns("SELECT a AND b, a OR b, a XOR b FROM shared_logic_ops ORDER BY id")
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected logical SQL rows")
    };
    assert_eq!(columns.len(), 3);
    // All three retain their existing signed SQL boolean-integer descriptors.
    for (_, field_type) in &columns {
        assert!(!field_type.is_unsigned());
    }
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null, Datum::Null, Datum::Null],
            vec![Datum::Int(0), Datum::Null, Datum::Null],
            vec![Datum::Null, Datum::Int(1), Datum::Null],
            vec![Datum::Int(0), Datum::Null, Datum::Null],
            vec![Datum::Int(0), Datum::Int(0), Datum::Int(0)],
            vec![Datum::Int(0), Datum::Int(1), Datum::Int(1)],
            vec![Datum::Null, Datum::Int(1), Datum::Null],
            vec![Datum::Int(0), Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(0)],
        ]
    );
}



#[test]
fn ready_value_lane_cache_inet_sql_nullable_text_integer_and_binary_values() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_inet_ops (id INT PRIMARY KEY, t VARCHAR(64), \
             n BIGINT UNSIGNED, b VARBINARY(16))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_inet_ops VALUES \
             (1,NULL,NULL,NULL),\
             (2,'127.0.0.1',2130706433,X'7F000001'),\
             (3,'255.255.255.255',4294967295,X'FFFFFFFF'),\
             (4,'2001:db8::1',4294967296,X'20010DB8000000000000000000000001'),\
             (5,'::ffff:1.2.3.4',16909060,X'00000000000000000000FFFF01020304'),\
             (6,'not-an-ip',18446744073709551615,X'010203'),\
             (7,'',0,X'')",
        )
        .unwrap();

    let output = session
        .run_with_columns(
            "SELECT INET_ATON(t), INET_NTOA(n), INET6_ATON(t), INET6_NTOA(b) \
             FROM shared_inet_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected INET SQL rows")
    };
    assert_eq!(columns.len(), 4);
    // Existing inference declares unsigned ATON and binary 6_ATON. As in
    // the earlier byte tests, chunk materialization gives a binary String.
    assert!(columns[0].1.is_unsigned());
    assert_eq!(columns[2].1.collation(), tidb_datatype::Collation::Binary);
    let binary_string =
        |bytes: Vec<u8>| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);

    // Standard address constants and fixed network-order bytes, not results
    // recorded from the new engine. The two NTOA columns carry textual output.
    let expected: [(Option<u64>, &str, Option<Vec<u8>>, &str); 7] = [
        (None, "NULL", None, "NULL"),
        (
            Some(2_130_706_433),
            "127.0.0.1",
            Some(vec![127, 0, 0, 1]),
            "127.0.0.1",
        ),
        (
            Some(4_294_967_295),
            "255.255.255.255",
            Some(vec![0xff; 4]),
            "255.255.255.255",
        ),
        (
            None,
            "NULL",
            Some(vec![
                0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            ]),
            "2001:db8::1",
        ),
        (
            None,
            "1.2.3.4",
            Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            "::ffff:1.2.3.4",
        ),
        (None, "NULL", None, "NULL"),
        (None, "0.0.0.0", None, "NULL"),
    ];
    assert_eq!(rows.len(), expected.len());
    for (row_index, (row, (aton, ntoa, aton6, ntoa6))) in rows.iter().zip(expected).enumerate() {
        assert_eq!(row.len(), 4);
        assert_eq!(
            row[0],
            aton.map_or(Datum::Null, Datum::UInt),
            "id {}",
            row_index + 1
        );
        assert_eq!(
            crate::tests_support::cell_text(&row[1]),
            ntoa,
            "id {}",
            row_index + 1
        );
        assert_eq!(
            row[2],
            aton6.map_or(Datum::Null, binary_string),
            "id {}",
            row_index + 1
        );
        assert_eq!(
            crate::tests_support::cell_text(&row[3]),
            ntoa6,
            "id {}",
            row_index + 1
        );
    }
}



#[test]
fn ready_value_lane_cache_math_real_sql_analytical_values_and_metadata() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_math_ops (id INT PRIMARY KEY, v DOUBLE, \
             deg DOUBLE, rad DOUBLE)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_math_ops VALUES \
             (1,NULL,NULL,NULL),(2,0,0,0),\
             (3,1,180,3.141592653589793),(4,-1,-180,-3.141592653589793),\
             (5,4,90,1.5707963267948966),(6,-4,360,6.283185307179586)",
        )
        .unwrap();

    // Every target reads a stored nullable column, including the angle inputs.
    // These are analytical endpoints and square roots, not recorded outputs.
    let output = session
        .run_with_columns(
            "SELECT ASIN(v), ACOS(v), SQRT(v), SIGN(v), RADIANS(deg), DEGREES(rad) \
             FROM shared_math_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected real math SQL rows")
    };
    assert_eq!(columns.len(), 6);
    // Preserve the existing inference: SIGN is signed Int, all others Real.
    for (column_index, (_, field_type)) in columns.iter().enumerate() {
        let expected_type = if column_index == 3 {
            tidb_datatype::EvalType::Int
        } else {
            tidb_datatype::EvalType::Real
        };
        assert_eq!(
            field_type.eval_type(),
            expected_type,
            "column {column_index}"
        );
        assert!(!field_type.is_unsigned(), "column {column_index}");
    }
    let pi = std::f64::consts::PI;
    let half_pi = std::f64::consts::FRAC_PI_2;
    let expected = vec![
        vec![Datum::Null; 6],
        vec![
            Datum::Real(0.0),
            Datum::Real(half_pi),
            Datum::Real(0.0),
            Datum::Int(0),
            Datum::Real(0.0),
            Datum::Real(0.0),
        ],
        vec![
            Datum::Real(half_pi),
            Datum::Real(0.0),
            Datum::Real(1.0),
            Datum::Int(1),
            Datum::Real(pi),
            Datum::Real(180.0),
        ],
        vec![
            Datum::Real(-half_pi),
            Datum::Real(pi),
            Datum::Null,
            Datum::Int(-1),
            Datum::Real(-pi),
            Datum::Real(-180.0),
        ],
        vec![
            Datum::Null,
            Datum::Null,
            Datum::Real(2.0),
            Datum::Int(1),
            Datum::Real(half_pi),
            Datum::Real(90.0),
        ],
        vec![
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Int(-1),
            Datum::Real(2.0 * pi),
            Datum::Real(360.0),
        ],
    ];
    assert_eq!(rows.len(), expected.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected).enumerate() {
        assert_eq!(row.len(), expected.len());
        for (column_index, (value, expected)) in row.iter().zip(expected).enumerate() {
            match expected {
                Datum::Real(expected) => {
                    let Datum::Real(actual) = value else {
                        panic!(
                            "math id {}, column {column_index}: expected Real, got {value:?}",
                            row_index + 1
                        )
                    };
                    let tolerance = 1e-12 * expected.abs().max(1.0);
                    assert!(
                        actual.is_finite() && (*actual - expected).abs() <= tolerance,
                        "math id {}, column {column_index}: {actual} != {expected}",
                        row_index + 1
                    );
                }
                expected => assert_eq!(
                    value,
                    &expected,
                    "math id {}, column {column_index}",
                    row_index + 1
                ),
            }
        }
    }
}



#[test]
fn ready_value_lane_cache_pi_and_ip_predicate_sql_values_and_metadata() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_ip_pred_ops (id INT PRIMARY KEY, \
             t VARCHAR(64), b VARBINARY(16))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_ip_pred_ops VALUES \
             (1,NULL,NULL),\
             (2,'192.168.0.1',X'000000000000000000000000C0A80001'),\
             (3,'001.002.0003.000004',X'00000000000000000000FFFF01020304'),\
             (4,'2001:db8::1',X'20010DB8000000000000000000000001'),\
             (5,'::ffff:1.2.3.4',X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),\
             (6,'1..2.3',X'000102'),(7,'0000.00.0.000',X'')",
        )
        .unwrap();

    let output = session
        .run_with_columns(
            "SELECT PI(), IS_IPV4(t), IS_IPV6(t), IS_IPV4_COMPAT(b), IS_IPV4_MAPPED(b) \
             FROM shared_ip_pred_ops ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected PI and IP predicate SQL rows")
    };
    assert_eq!(columns.len(), 5);
    // PI may legally fold before row execution. Only its SQL value and
    // descriptor are asserted here; native tests own true NoArgs admission.
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Double);
    assert_eq!(columns[0].1.flen(), 8);
    assert_eq!(columns[0].1.decimal(), 6);
    assert!(!columns[0].1.is_unsigned());
    for (column_index, (_, field_type)) in columns.iter().enumerate().skip(1) {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::Int);
        assert_eq!(field_type.flen(), 1);
        assert!(!field_type.is_unsigned(), "column {column_index}");
        assert!(field_type.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }

    // The four native predicates propagate NULL. Leading-zero decimal
    // components are accepted, but an original empty component remains invalid.
    // Binary fixtures distinguish compatible, mapped and nonmatching prefixes.
    let expected: [[Option<i64>; 4]; 7] = [
        [None, None, None, None],
        [Some(1), Some(0), Some(1), Some(0)],
        [Some(1), Some(0), Some(0), Some(1)],
        [Some(0), Some(1), Some(0), Some(0)],
        [Some(0), Some(1), Some(0), Some(0)],
        [Some(0), Some(0), Some(0), Some(0)],
        [Some(1), Some(0), Some(0), Some(0)],
    ];
    assert_eq!(rows.len(), expected.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected).enumerate() {
        assert_eq!(row.len(), 5);
        assert_eq!(row[0], Datum::Real(std::f64::consts::PI));
        for (column_index, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                row[column_index + 1],
                expected.map_or(Datum::Null, Datum::Int),
                "IP predicate fixture id {}, predicate {column_index}",
                row_index + 1
            );
        }
    }
}



#[test]
fn ready_value_lane_cache_packet_string_sql_values_metadata_and_warnings() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_packet_ops (id INT PRIMARY KEY, n BIGINT, \
             t VARCHAR(16), b VARBINARY(1024), e VARCHAR(2048))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_packet_ops VALUES \
             (1,NULL,NULL,NULL,NULL),(2,0,'',X'',''),\
             (3,2,'ab',X'616263','YWJj'),(4,-1,'é',X'FF00','/wA='),\
             (5,2,'中',X'00',' Y Q = = '),(6,1,'x',X'41','!!!!')",
        )
        .unwrap();
    // Small Rust-built input fixtures, never nested calls to the SQL targets.
    // 1368 raw Base64 bytes estimate to 1026 decoded bytes BEFORE whitespace
    // removal, although the cleaned YQ== payload would decode to just one byte.
    let padded_base64 = format!("{}YQ==", " ".repeat(1364));
    session
        .run(&format!(
            "INSERT INTO shared_packet_ops VALUES \
             (7,1,'a','{}','YQ=='),(8,1025,'ab','{}','{}')",
            "a".repeat(58),
            "a".repeat(768),
            padded_base64,
        ))
        .unwrap();
    // SQL SET SESSION is read-only, and SET GLOBAL does not update this
    // session. Reuse the existing validated internal SessionVars test setup.
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    assert_eq!(session.max_allowed_packet(), 1024);

    let output = session
        .run_with_columns(
            "SELECT SPACE(n), REPEAT(t,n), TO_BASE64(b), FROM_BASE64(e) \
             FROM shared_packet_ops WHERE id<8 ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = output else {
        panic!("expected packet string SQL rows")
    };
    assert_eq!(columns.len(), 4);
    for (column_index, (_, field_type)) in columns.iter().enumerate() {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
        if column_index == 3 {
            assert_eq!(field_type.collation(), tidb_datatype::Collation::Binary);
        } else {
            assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
        }
    }
    // Each aaa triple is YWFh. The final a starts a new line after exactly
    // 76 encoded columns; no encoder under test supplies this expected value.
    let wrapped = format!("{}\nYQ==", "YWFh".repeat(19));
    let expected_text: [[Option<&str>; 3]; 7] = [
        [None; 3],
        [Some(""); 3],
        [Some("  "), Some("abab"), Some("YWJj")],
        [Some(""), Some(""), Some("/wA=")],
        [Some("  "), Some("中中"), Some("AA==")],
        [Some(" "), Some("x"), Some("QQ==")],
        [Some(" "), Some("a"), Some(wrapped.as_str())],
    ];
    let binary_string =
        |bytes: Vec<u8>| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);
    let expected_binary = [
        Datum::Null,
        binary_string(vec![]),
        binary_string(b"abc".to_vec()),
        binary_string(vec![0xff, 0x00]),
        binary_string(b"a".to_vec()),
        Datum::Null,
        binary_string(b"a".to_vec()),
    ];
    assert_eq!(rows.len(), expected_text.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected_text).enumerate() {
        assert_eq!(row.len(), 4);
        for (column_index, expected) in expected.into_iter().enumerate() {
            let value = &row[column_index];
            match expected {
                None => assert_eq!(value, &Datum::Null),
                Some(expected) => {
                    assert!(matches!(value, Datum::String(_)));
                    assert_ne!(value.collation(), Some(tidb_datatype::Collation::Binary));
                    assert_eq!(
                        crate::tests_support::cell_text(value),
                        expected,
                        "packet string fixture id {}, column {column_index}",
                        row_index + 1
                    );
                }
            }
        }
        assert_eq!(row[3], expected_binary[row_index]);
    }
    assert!(warnings_of(&session).is_empty());

    // Each direct target independently suppresses an over-packet result. TO's
    // 768-byte input needs 1024 Base64 characters PLUS 13 newline bytes.
    for (expression, function) in [
        ("SPACE(n)", "space"),
        ("REPEAT(t,n)", "repeat"),
        ("TO_BASE64(b)", "to_base64"),
        ("FROM_BASE64(e)", "from_base64"),
    ] {
        let sql = format!("SELECT {expression} FROM shared_packet_ops WHERE id=8");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected packet-suppressed SQL rows: {sql}")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        assert_eq!(
            session.warnings(),
            &[SqlWarning {
                level: WarningLevel::Warning,
                code: 1301,
                message: format!(
                    "Result of {function}() was larger than max_allowed_packet (1024) - truncated"
                ),
            }],
            "{sql}"
        );
    }
}



#[test]
fn ready_value_lane_cache_case_sha2_ord_sql_values_and_metadata() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_case_hash_ord (id INT PRIMARY KEY, \
             t VARCHAR(16) CHARSET utf8mb4, b VARBINARY(16), \
             l VARCHAR(16) CHARSET latin1, h VARBINARY(8), bits INT)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_case_hash_ord VALUES \
             (1,NULL,NULL,NULL,NULL,NULL),(2,'',X'','',X'',256),\
             (3,'你İßΣ',X'E4BDA0','é',X'616263',0),\
             (4,'Ab',X'4162FF',0xE28241,X'616263',256),\
             (5,'A',X'61','Z',X'616263',123)",
        )
        .unwrap();

    // Existing latin1 storage is a byte-preserving UTF-8 alias, not a write
    // transcode to ISO-8859-1. Confirm both the declared argument charsets and
    // actual bytes before testing case conversion or ORD's charset boundary.
    let StmtOutput::Rows {
        columns: input_columns,
        rows: input_rows,
    } = session
        .run_with_columns("SELECT t,b,l FROM shared_case_hash_ord ORDER BY id")
        .unwrap()
    else {
        panic!("expected stored case/ORD inputs")
    };
    assert_eq!(input_rows.len(), 5);
    assert_eq!(input_columns[0].1.charset_name(), "utf8mb4");
    assert_eq!(input_columns[1].1.charset_name(), "binary");
    assert_eq!(input_columns[2].1.charset_name(), "latin1");
    assert_eq!(input_rows[2][0].to_bytes().unwrap(), "你İßΣ".as_bytes());
    assert_eq!(input_rows[2][1].to_bytes().unwrap(), vec![0xe4, 0xbd, 0xa0]);
    assert_eq!(input_rows[2][2].to_bytes().unwrap(), vec![0xc3, 0xa9]);
    assert_eq!(input_rows[3][2].to_bytes().unwrap(), vec![0xe2, 0x82, b'A']);

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT LOWER(t), UPPER(t), LCASE(t), UCASE(t), LOWER(b), UPPER(b), \
             LOWER(l), UPPER(l), SHA2(h,bits), ORD(b), ORD(t), ORD(l) \
             FROM shared_case_hash_ord ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected case/SHA2/ORD SQL rows")
    };
    assert_eq!(columns.len(), 12);
    for (column_index, (_, field_type)) in columns.iter().enumerate() {
        if column_index >= 9 {
            assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::Int);
            assert_eq!(field_type.flen(), 10);
            assert!(!field_type.is_unsigned());
        } else {
            assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
            assert_eq!(field_type.flen(), if column_index == 8 { 128 } else { 16 });
            if matches!(column_index, 4 | 5) {
                assert_eq!(field_type.collation(), tidb_datatype::Collation::Binary);
            } else {
                assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
            }
        }
    }
    // Published SHA-256 vectors, never recorded from the implementation.
    // The stored length 0 aliases 256; 123 is silent NULL, not warning 1583.
    let empty_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let abc_sha256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    let expected_text: [[Option<&str>; 5]; 5] = [
        [None; 5],
        [Some(""), Some(""), Some(""), Some(""), Some(empty_sha256)],
        [
            Some("你ißσ"),
            Some("你İßΣ"),
            Some("é"),
            Some("É"),
            Some(abc_sha256),
        ],
        [
            Some("ab"),
            Some("AB"),
            Some("\u{fffd}\u{fffd}a"),
            Some("\u{fffd}\u{fffd}A"),
            Some(abc_sha256),
        ],
        [Some("a"), Some("A"), Some("z"), Some("Z"), None],
    ];
    let binary_string =
        |bytes: Vec<u8>| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);
    let expected_binary = [
        Datum::Null,
        binary_string(vec![]),
        binary_string(vec![0xe4, 0xbd, 0xa0]),
        binary_string(vec![b'A', b'b', 0xff]),
        binary_string(vec![b'a']),
    ];
    // ORD uses arg0.charset, not its integer return field's binary collation.
    // latin1's no-op encode plus one-byte peek makes stored C3A9 yield 195.
    let expected_ord: [[Option<i64>; 3]; 5] = [
        [None; 3],
        [Some(0); 3],
        [Some(228), Some(14_990_752), Some(195)],
        [Some(65), Some(65), Some(226)],
        [Some(97), Some(65), Some(90)],
    ];
    assert_eq!(rows.len(), expected_text.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected_text).enumerate() {
        assert_eq!(row.len(), 12);
        for (column_index, expected) in [0, 1, 6, 7, 8].into_iter().zip(expected) {
            let value = &row[column_index];
            match expected {
                None => assert_eq!(value, &Datum::Null),
                Some(expected) => {
                    assert!(matches!(value, Datum::String(_)));
                    assert_ne!(value.collation(), Some(tidb_datatype::Collation::Binary));
                    assert_eq!(
                        crate::tests_support::cell_text(value),
                        expected,
                        "case/SHA2 fixture id {}, column {column_index}",
                        row_index + 1
                    );
                }
            }
        }
        assert_eq!(row[2], row[0], "LCASE alias, id {}", row_index + 1);
        assert_eq!(row[3], row[1], "UCASE alias, id {}", row_index + 1);
        assert_eq!(row[4], expected_binary[row_index]);
        assert_eq!(row[5], expected_binary[row_index]);
        for (column_index, expected) in expected_ord[row_index].into_iter().enumerate() {
            assert_eq!(
                row[9 + column_index],
                expected.map_or(Datum::Null, Datum::Int),
                "ORD fixture id {}, argument {column_index}",
                row_index + 1
            );
        }
    }
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_trim_split_pad_sql_values_metadata_and_packet_policy() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_trim_split_pad (id INT PRIMARY KEY, t VARCHAR(16), \
             b VARBINARY(16), r VARCHAR(8), d VARCHAR(8), c BIGINT, \
             u BIGINT UNSIGNED, n BIGINT, p VARCHAR(8), q VARBINARY(8))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_trim_split_pad VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,'ababa',X'20FF20','aba','ba',1,NULL,7,'',X''),\
             (3,'aaa',X'6142','a','aa',-1,NULL,3,'',X''),\
             (4,'你a',X'FF41','你','你',-9223372036854775808,18446744073709551615,4,'x',X'5A'),\
             (5,NULL,NULL,NULL,NULL,NULL,NULL,-1,'x',X'78'),\
             (6,'ab',X'6162',NULL,NULL,NULL,NULL,257,'x',X'78')",
        )
        .unwrap();
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    assert_eq!(session.max_allowed_packet(), 1024);

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT TRIM(t), TRIM(BOTH r FROM t), TRIM(LEADING r FROM t), \
             TRIM(TRAILING r FROM t), TRIM(b), SUBSTRING_INDEX(t,d,c), \
             LPAD(t,n,p), RPAD(t,n,p), LPAD(b,n,p), RPAD(t,n,q) \
             FROM shared_trim_split_pad WHERE id<5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected TRIM/SUBSTRING_INDEX/PAD SQL rows")
    };
    assert_eq!(columns.len(), 10);
    // Expr::Trim keeps its unsized result width. The outer collation pass
    // still derives TRIM from arg0 and PAD from both string arguments (0, 2).
    for (column_index, (_, field_type)) in columns.iter().enumerate() {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
        let expected_flen = match column_index {
            0..=4 => tidb_datatype::UNSPECIFIED_LENGTH,
            5 => 16,
            _ => 16_777_216,
        };
        assert_eq!(field_type.flen(), expected_flen, "column {column_index}");
        if column_index == 4 || column_index >= 8 {
            assert_eq!(field_type.collation(), tidb_datatype::Collation::Binary);
        } else {
            assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
        }
    }
    let expected_text: [[Option<&str>; 7]; 4] = [
        [None; 7],
        [
            Some("ababa"),
            Some("ba"),
            Some("ba"),
            Some("ab"),
            Some("a"),
            Some(""),
            Some(""),
        ],
        [
            Some("aaa"),
            Some(""),
            Some(""),
            Some(""),
            Some("a"),
            Some("aaa"),
            Some("aaa"),
        ],
        [
            Some("你a"),
            Some("a"),
            Some("a"),
            Some("你a"),
            Some("你a"),
            Some("xx你a"),
            Some("你axx"),
        ],
    ];
    let expected_raw_trim: [Option<&[u8]>; 4] = [None, Some(b"\xff"), Some(b"aB"), Some(b"\xffA")];
    let binary_string =
        |bytes: Vec<u8>| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);
    let expected_binary_pad = [
        [Datum::Null, Datum::Null],
        [binary_string(vec![]), binary_string(vec![])],
        [binary_string(vec![]), binary_string(b"aaa".to_vec())],
        [
            binary_string(b"xx\xffA".to_vec()),
            binary_string("你a".as_bytes().to_vec()),
        ],
    ];
    assert_eq!(rows.len(), expected_text.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected_text).enumerate() {
        assert_eq!(row.len(), 10);
        for (column_index, expected) in [0, 1, 2, 3, 5, 6, 7].into_iter().zip(expected) {
            let value = &row[column_index];
            match expected {
                None => assert_eq!(value, &Datum::Null),
                Some(expected) => {
                    assert!(matches!(value, Datum::String(_)));
                    assert_ne!(value.collation(), Some(tidb_datatype::Collation::Binary));
                    assert_eq!(
                        crate::tests_support::cell_text(value),
                        expected,
                        "trim/split/pad fixture id {}, column {column_index}",
                        row_index + 1
                    );
                }
            }
        }
        match expected_raw_trim[row_index] {
            None => assert_eq!(row[4], Datum::Null),
            Some(expected) => {
                assert!(matches!(&row[4], Datum::String(_)));
                assert_eq!(row[4].collation(), Some(tidb_datatype::Collation::Binary));
                assert_eq!(row[4].to_bytes().unwrap(), expected);
            }
        }
        assert_eq!(row[8], expected_binary_pad[row_index][0]);
        assert_eq!(row[9], expected_binary_pad[row_index][1]);
    }
    assert!(warnings_of(&session).is_empty());

    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT c,u,SUBSTRING_INDEX(t,d,u) FROM shared_trim_split_pad WHERE id=4")
        .unwrap()
    else {
        panic!("expected stored extreme split counts")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int(i64::MIN));
    assert_eq!(rows[0][1], Datum::UInt(u64::MAX));
    assert_eq!(crate::tests_support::cell_text(&rows[0][2]), "你a");
    assert!(warnings_of(&session).is_empty());

    // NULL source does not skip the packet check, and negative lengths warn
    // before the range rejection. Positive text length 257 also exceeds 1024
    // under the original n*4 estimate, before source/pad string coercion.
    for (function, id) in [("LPAD", 5), ("RPAD", 5), ("LPAD", 6), ("RPAD", 6)] {
        let sql = format!("SELECT {function}(t,n,p) FROM shared_trim_split_pad WHERE id={id}");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected packet-suppressed padding rows")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        assert_eq!(
            session.warnings(),
            &[SqlWarning {
                level: WarningLevel::Warning,
                code: 1301,
                message: format!(
                    "Result of {}() was larger than max_allowed_packet (1024) - truncated",
                    function.to_ascii_lowercase()
                ),
            }],
            "{sql}"
        );
    }
    // Either binary operand selects n bytes, not n*4: both 257-byte results
    // fit. These tiny buffers also distinguish source-binary from pad-binary.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT LPAD(b,n,p), RPAD(t,n,q) FROM shared_trim_split_pad WHERE id=6")
        .unwrap()
    else {
        panic!("expected allowed binary padding rows")
    };
    let left = [vec![b'x'; 255], b"ab".to_vec()].concat();
    let right = [b"ab".to_vec(), vec![b'x'; 255]].concat();
    assert_eq!(rows, vec![vec![binary_string(left), binary_string(right)]]);
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_log_pow_length_insert_sql_values_metadata_and_warnings() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_log_pow_length_insert (id INT PRIMARY KEY, \
             x DOUBLE, base DOUBLE, e DOUBLE, z VARBINARY(8), zt VARCHAR(8), \
             t VARCHAR(16), b VARBINARY(16), pos BIGINT, n BIGINT, \
             r VARCHAR(1100), rb VARBINARY(8), rt VARCHAR(8) CHARSET latin1)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_log_pow_length_insert VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,1,2,2,X'','','abc',X'614263',2,1,'X',X'FF','X'),\
             (3,4,2,0.5,X'FFFFFFFF00','AAAAA','中a',X'FF61',2,1,'X',X'FF',0xFF)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_log_pow_length_insert (id,x,base,z) VALUES \
             (4,0,2,X'00'),(5,2,1,X'0000'),\
             (6,NULL,NULL,X'000000'),(7,NULL,NULL,X'00000000')",
        )
        .unwrap();
    // The replacement is 1026 bytes but only 342 characters. The non-NULL
    // INSERT result is 1028 bytes; the NULL source must not read packet policy.
    let replacement = "中".repeat(342);
    session
        .run(&format!(
            "INSERT INTO shared_log_pow_length_insert (id,t,pos,n,r) VALUES \
             (8,'abc',2,1,'{replacement}'),(9,NULL,2,1,'{replacement}')"
        ))
        .unwrap();
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    assert_eq!(session.max_allowed_packet(), 1024);

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT LN(x), LOG(x), LOG(base,x), LOG2(x), POW(x,e), POWER(x,e), \
             UNCOMPRESSED_LENGTH(z), UNCOMPRESSED_LENGTH(zt), \
             INSERT(t,pos,n,r), INSERT(b,pos,n,r), INSERT(t,pos,n,rb) \
             FROM shared_log_pow_length_insert WHERE id<4 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected log/pow/length/insert SQL rows")
    };
    assert_eq!(columns.len(), 11);
    for (column_index, (_, field_type)) in columns.iter().enumerate() {
        match column_index {
            0..=5 => {
                assert_eq!(field_type.code(), tidb_datatype::FieldTypeCode::Double);
                assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::Real);
                assert_eq!(field_type.flen(), 23);
                assert_eq!(field_type.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
                assert!(!field_type.is_unsigned());
            }
            6..=7 => {
                assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::Int);
                assert_eq!(field_type.flen(), 10);
                assert!(!field_type.is_unsigned());
            }
            _ => {
                assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
                assert_eq!(field_type.flen(), 16_777_216);
                // Outer derivation aggregates INSERT's arguments 0 and 3.
                if column_index == 8 {
                    assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
                } else {
                    assert_eq!(field_type.collation(), tidb_datatype::Collation::Binary);
                }
            }
        }
    }
    let ln_four = 2.0 * std::f64::consts::LN_2;
    let expected_real = [
        [None; 6],
        [
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(1.0),
            Some(1.0),
        ],
        [
            Some(ln_four),
            Some(ln_four),
            Some(2.0),
            Some(2.0),
            Some(2.0),
            Some(2.0),
        ],
    ];
    let expected_lengths = [
        [Datum::Null, Datum::Null],
        [Datum::Int(0), Datum::Int(0)],
        // Both are raw LE headers, not validated zlib streams. All 32 bits
        // survive, with no packet rejection based on the advertised length.
        [Datum::Int(4_294_967_295), Datum::Int(1_094_795_585)],
    ];
    let expected_text = [None, Some("aXc"), Some("中X")];
    let binary_string =
        |bytes: Vec<u8>| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);
    let expected_binary = [
        [Datum::Null, Datum::Null],
        [
            binary_string(b"aXc".to_vec()),
            binary_string(b"a\xffc".to_vec()),
        ],
        [
            binary_string(b"\xffX".to_vec()),
            binary_string(vec![0xe4, 0xff, 0xad, b'a']),
        ],
    ];
    assert_eq!(rows.len(), 3);
    for (row_index, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), 11);
        for (column_index, expected) in expected_real[row_index].into_iter().enumerate() {
            match expected {
                None => assert_eq!(row[column_index], Datum::Null),
                Some(expected) => {
                    let Datum::Real(actual) = &row[column_index] else {
                        panic!(
                            "expected native Real at id {}, column {column_index}",
                            row_index + 1
                        )
                    };
                    assert!(
                        (*actual - expected).abs() < 1e-12,
                        "id {}, column {column_index}: {actual} != {expected}",
                        row_index + 1
                    );
                }
            }
        }
        assert_eq!(row[4], row[5], "POW/POWER aliases");
        assert_eq!(row[6], expected_lengths[row_index][0]);
        assert_eq!(row[7], expected_lengths[row_index][1]);
        match expected_text[row_index] {
            None => assert_eq!(row[8], Datum::Null),
            Some(expected) => {
                assert!(matches!(&row[8], Datum::String(_)));
                assert_ne!(row[8].collation(), Some(tidb_datatype::Collation::Binary));
                assert_eq!(crate::tests_support::cell_text(&row[8]), expected);
            }
        }
        assert_eq!(row[9], expected_binary[row_index][0]);
        assert_eq!(row[10], expected_binary[row_index][1]);
    }
    assert!(warnings_of(&session).is_empty());

    // A text-signature replacement is raw too: only the source is normalized
    // to Go runes. Existing latin1 storage lets the SQL fixture retain FF.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT rt,INSERT(t,pos,n,rt) FROM shared_log_pow_length_insert WHERE id=3",
        )
        .unwrap()
    else {
        panic!("expected raw text replacement rows")
    };
    assert_eq!(columns[0].1.charset_name(), "latin1");
    assert_eq!(columns[1].1.eval_type(), tidb_datatype::EvalType::String);
    assert_eq!(columns[1].1.flen(), 16_777_216);
    assert_ne!(columns[1].1.collation(), tidb_datatype::Collation::Binary);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_bytes().unwrap(), vec![0xff]);
    assert!(matches!(&rows[0][1], Datum::String(_)));
    assert_ne!(
        rows[0][1].collation(),
        Some(tidb_datatype::Collation::Binary)
    );
    assert_eq!(rows[0][1].to_bytes().unwrap(), vec![0xe4, 0xb8, 0xad, 0xff]);
    assert!(warnings_of(&session).is_empty());

    for (expression, id) in [
        ("LN(x)", 4),
        ("LOG(x)", 4),
        ("LOG(base,x)", 4),
        ("LOG2(x)", 4),
        ("LOG(base,x)", 5),
    ] {
        let sql = format!("SELECT {expression} FROM shared_log_pow_length_insert WHERE id={id}");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected invalid logarithm rows")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        assert_eq!(
            session.warnings(),
            &[SqlWarning {
                level: WarningLevel::Warning,
                code: 3020,
                message: "Invalid argument for logarithm".to_owned(),
            }],
            "{sql}"
        );
    }
    let mysql = session
        .run_with_columns("SELECT POW(-2, 0.5)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(mysql.code, 1690);
    assert_eq!(mysql.state, *b"22003");
    assert_eq!(
        mysql.message,
        "DOUBLE value is out of range in 'pow(-2, 0.5)'"
    );

    for id in 4..=7 {
        let sql = format!(
            "SELECT UNCOMPRESSED_LENGTH(z) FROM shared_log_pow_length_insert WHERE id={id}"
        );
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected short uncompressed-length rows")
        };
        assert_eq!(rows, vec![vec![Datum::Int(0)]], "{sql}");
        assert_eq!(
            session.warnings(),
            &[SqlWarning {
                level: WarningLevel::Warning,
                code: 1259,
                message: "ZLIB: Input data corrupted".to_owned(),
            }],
            "{sql}"
        );
    }
    for id in [8, 9] {
        let sql =
            format!("SELECT INSERT(t,pos,n,r) FROM shared_log_pow_length_insert WHERE id={id}");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected INSERT packet-policy rows")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        if id == 8 {
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1301,
                    message: "Result of insert() was larger than \
                              max_allowed_packet (1024) - truncated"
                        .to_owned(),
                }]
            );
        } else {
            assert!(warnings_of(&session).is_empty());
        }
    }
}



#[test]
fn ready_value_lane_cache_substring_dispatch_sql_values_metadata_and_diagnostics() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_substring_dispatch (id INT PRIMARY KEY, \
             t VARCHAR(16), b VARBINARY(16), p BIGINT, n BIGINT, \
             u BIGINT UNSIGNED, s VARCHAR(8), l VARCHAR(16) CHARSET latin1)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_substring_dispatch (id,t,b,p,n) VALUES \
             (1,NULL,NULL,NULL,NULL),(3,'abcd',X'61626364',0,1),\
             (4,'abcd',X'61626364',-2,2),(5,'abcd',X'61626364',2,0),\
             (6,'abcd',X'61626364',2,-1),(7,'abcd',X'61626364',10,2),\
             (8,'abcd',X'61626364',2,9223372036854775807),\
             (9,'abcd',X'61626364',2,NULL)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_substring_dispatch VALUES \
             (2,'中ab',X'E4B8AD6162',2,1,18446744073709551615,'bad',0xE28241)",
        )
        .unwrap();

    let assert_text = |value: &Datum, expected: Option<&str>| match expected {
        None => assert_eq!(value, &Datum::Null),
        Some(expected) => {
            assert!(matches!(value, Datum::String(_)));
            assert_ne!(value.collation(), Some(tidb_datatype::Collation::Binary));
            assert_eq!(crate::tests_support::cell_text(value), expected);
        }
    };
    let binary_string =
        |bytes: Vec<u8>| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT SUBSTRING(t,p), SUBSTRING(t,p,n), SUBSTRING(b,p), SUBSTRING(b,p,n) \
             FROM shared_substring_dispatch ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected SUBSTRING dispatch rows")
    };
    assert_eq!(columns.len(), 4);
    for (column_index, (_, field_type)) in columns.iter().enumerate() {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
        assert_eq!(field_type.flen(), 16);
        // Both arities inherit arg0's width and outer-derived collation.
        if column_index < 2 {
            assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
        } else {
            assert_eq!(field_type.collation(), tidb_datatype::Collation::Binary);
        }
    }
    let expected_text = [
        [None, None],
        [Some("ab"), Some("a")],
        [Some(""), Some("")],
        [Some("cd"), Some("cd")],
        [Some("bcd"), Some("")],
        [Some("bcd"), Some("")],
        [Some(""), Some("")],
        // The native three-argument start+length overflows. Two arguments do
        // NOT mean "synthesize i64::MAX length": they still return the tail.
        [Some("bcd"), Some("")],
        [Some("bcd"), None],
    ];
    assert_eq!(rows.len(), expected_text.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected_text).enumerate() {
        assert_eq!(row.len(), 4);
        for (column_index, expected) in expected.into_iter().enumerate() {
            assert_text(&row[column_index], expected);
            let raw = if row_index == 1 {
                if column_index == 0 {
                    binary_string(vec![0xb8, 0xad, b'a', b'b'])
                } else {
                    binary_string(vec![0xb8])
                }
            } else {
                // All other non-NULL fixtures are the same ASCII bytes in
                // both columns, so their byte slices have these same bytes.
                expected.map_or(Datum::Null, |value| {
                    binary_string(value.as_bytes().to_vec())
                })
            };
            assert_eq!(
                row[2 + column_index],
                raw,
                "id {}, column {column_index}",
                row_index + 1
            );
        }
    }
    assert!(warnings_of(&session).is_empty());

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT SUBSTR(t,p), SUBSTR(t,p,n), MID(t,p), MID(t,p,n) \
             FROM shared_substring_dispatch WHERE id=2",
        )
        .unwrap()
    else {
        panic!("expected SUBSTR/MID alias rows")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 4);
    assert_eq!(columns.len(), 4);
    for ((value, expected), (_, field_type)) in
        rows[0].iter().zip(["ab", "a", "ab", "a"]).zip(columns)
    {
        assert_text(value, Some(expected));
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
        assert_eq!(field_type.flen(), 16);
        assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
    }
    assert!(warnings_of(&session).is_empty());
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT u,SUBSTRING(t,u),SUBSTRING(t,u,n),SUBSTRING(t,p,u) \
             FROM shared_substring_dispatch WHERE id=2",
        )
        .unwrap()
    else {
        panic!("expected stored UInt substring counts")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 4);
    assert_eq!(rows[0][0], Datum::UInt(u64::MAX));
    // Native ETInt keeps the UInt bits: MAX is position/length -1.
    for (value, expected) in rows[0][1..].iter().zip(["b", "b", ""]) {
        assert_text(value, Some(expected));
    }
    assert!(warnings_of(&session).is_empty());

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT l,SUBSTRING(l,p),SUBSTRING(l,p,n) FROM shared_substring_dispatch WHERE id=2",
        )
        .unwrap()
    else {
        panic!("expected malformed text substring rows")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(columns[0].1.charset_name(), "latin1");
    assert_eq!(rows[0][0].to_bytes().unwrap(), vec![0xe2, 0x82, b'A']);
    // Go normalizes EACH bad byte before character slicing; Rust's grouped
    // replacement would incorrectly move A into the second character slot.
    assert_text(&rows[0][1], Some("\u{fffd}A"));
    assert_text(&rows[0][2], Some("\u{fffd}"));
    for (_, field_type) in &columns[1..] {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
        assert_eq!(field_type.flen(), 16);
        assert_eq!(field_type.charset_name(), "latin1");
    }
    assert!(warnings_of(&session).is_empty());

    // The old string2 dispatcher coerces TWO arguments under NoColumns;
    // adding a real execution scope must not also enable its discarded 1292.
    for (expression, warns) in [("SUBSTRING(t,s)", false), ("SUBSTRING(t,s,n)", true)] {
        let sql = format!("SELECT {expression} FROM shared_substring_dispatch WHERE id=2");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected substring position-coercion rows")
        };
        assert_eq!(rows.len(), 1);
        assert_text(&rows[0][0], Some(""));
        if warns {
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1292,
                    message: "Truncated incorrect INTEGER value: 'bad'".to_owned(),
                }]
            );
        } else {
            assert!(warnings_of(&session).is_empty());
        }
    }
}



#[test]
fn ready_value_lane_cache_collated_search_set_dispatch_sql_values_metadata_and_cache() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_collated_search_set (id INT PRIMARY KEY, \
             h VARCHAR(16) CHARSET utf8mb4 COLLATE utf8mb4_general_ci, \
             n VARCHAR(16) CHARSET utf8mb4 COLLATE utf8mb4_general_ci, \
             hb VARBINARY(16), nb VARBINARY(16), p BIGINT, \
             f VARCHAR(16) CHARSET utf8mb4 COLLATE utf8mb4_general_ci, \
             v VARCHAR(32) CHARSET utf8mb4 COLLATE utf8mb4_general_ci)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_collated_search_set VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,'é','e',X'C3A9',X'65',1,' ','  , , ,'),\
             (3,'B,b,b','b',X'422C622C62',X'62',2,'b','B,b,b'),\
             (4,'','',X'',X'',1,'',''),\
             (5,'ẞ','s',X'E1BA9E',X'73',1,'a','a,a')",
        )
        .unwrap();

    // One shared root, column-driven calls: no constant folding of a family.
    // INSTR reverses LOCATE's operands; POSITION has its own SQL grammar but
    // rewrites to the same two-argument LOCATE. p is only 1/2 (or NULL), not
    // the old LOCATE3 position-minus-one overflow domain.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT STRCMP(n,h), STRCMP(nb,hb), \
             LOCATE(n,h), LOCATE(n,h,p), LOCATE(nb,hb), \
             INSTR(h,n), INSTR(hb,nb), POSITION(n IN h), POSITION(nb IN hb), \
             FIND_IN_SET(f,v), FIND_IN_SET(nb,hb), FIND_IN_SET(f,'  , , ,') \
             FROM shared_collated_search_set ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected collated search/set dispatch rows")
    };
    let widths = [2, 2, 20, 20, 20, 11, 11, 20, 20, 3, 3, 3];
    assert_eq!(columns.len(), widths.len());
    for (index, ((_, field_type), width)) in columns.iter().zip(widths).enumerate() {
        assert_eq!(field_type.code(), tidb_datatype::FieldTypeCode::LongLong);
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::Int);
        assert_eq!(field_type.flen(), width);
        assert_eq!(field_type.decimal(), 0);
        assert!(!field_type.is_unsigned());
        // The numeric result still carries the derived comparison collation.
        let collation = if matches!(index, 1 | 4 | 6 | 8 | 10) {
            tidb_datatype::Collation::Binary
        } else {
            tidb_datatype::Collation::Utf8Mb4GeneralCi
        };
        assert_eq!(field_type.collation(), collation, "column {index}");
    }
    let expected = [
        None,
        // Native general_ci finds e in é; binary does not. FIND_IN_SET uses
        // NoPad keys: one space first matches field TWO, not the leading two
        // spaces. The literal-list expression selects the context cache path.
        Some([0, -1, 1, 1, 0, 1, 0, 1, 0, 2, 0, 2]),
        // general_ci's B/b duplicate keeps position 1; binary keeps the first
        // exact b at position 2. LOCATE3 starts at the stored position 2.
        Some([-1, 1, 1, 3, 3, 1, 3, 1, 3, 1, 2, 0]),
        // Empty dynamic lists answer 0, whereas the cached nonempty list has
        // its first empty member at 4. Its earlier one-space members stay put.
        Some([0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 4]),
        // GENERAL_CI_PLANE_1E keeps U+1E9E's weight, but native LOCATE3 first
        // uses Go simple-lower (U+1E9E -> U+00DF), whose general_ci weight is S.
        // Thus s in ẞ is 0 with two args and 1 with three args, even at pos 1.
        Some([-1, -1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0]),
    ];
    assert_eq!(rows.len(), expected.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected).enumerate() {
        assert_eq!(row.len(), widths.len());
        for (column_index, value) in row.iter().enumerate() {
            let expected = expected.map_or(Datum::Null, |values| Datum::Int(values[column_index]));
            assert_eq!(
                value,
                &expected,
                "id {}, column {column_index}",
                row_index + 1
            );
        }
    }
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_variadic_oct_elt_dispatch_sql_values_and_metadata() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_variadic_oct_elt (id INT PRIMARY KEY, \
             o VARCHAR(32) CHARSET utf8mb4, u BIGINT UNSIGNED, k BIGINT, \
             t VARCHAR(8) CHARSET utf8mb4, b VARBINARY(8), s VARCHAR(1) CHARSET utf8mb4)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_variadic_oct_elt VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,'\u{00a0}8',18446744073709551615,5,'中',X'FF00','|'),\
             (3,'',0,6,'',X'',''),(4,'   ',8,0,'A',X'41','|'),\
             (5,'8',16,1,NULL,NULL,'|')",
        )
        .unwrap();

    // Six actual CONCAT/CONCAT_WS arguments and six ELT candidates, with a
    // stored selector reaching candidates 5 and 6. No SQL child-laziness claim:
    // ELT's existing SQL argument evaluation remains eager.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT u, OCT(o), OCT(u), \
             CONCAT(t,'a','b','c','d',t), CONCAT(b,'a','b','c','d',b), \
             CONCAT_WS(s,t,'','x','',t), CONCAT_WS(s,b,'','x','',b), \
             ELT(k,t,'2','3','4','5','6'), ELT(k,b,'2','3','4','5','6') \
             FROM shared_variadic_oct_elt ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected variadic/OCT/ELT dispatch rows")
    };
    assert_eq!(columns.len(), 9);
    assert!(columns[0].1.is_unsigned());
    let widths = [64, 64, 20, 20, 21, 21, 8, 8];
    for (index, ((_, field_type), width)) in columns[1..].iter().zip(widths).enumerate() {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
        assert_eq!(field_type.flen(), width, "column {}", index + 1);
        assert_eq!(field_type.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
        if matches!(index, 3 | 5 | 7) {
            assert_eq!(field_type.collation(), tidb_datatype::Collation::Binary);
            assert_eq!(field_type.charset_name(), "binary");
        } else {
            assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
            assert_eq!(field_type.charset_name(), "utf8mb4");
        }
    }
    let expected_text = [
        [None, None, None, None, None],
        // OCT trims the Unicode NBSP, not only ASCII whitespace; the other
        // signature renders every bit of the stored UInt64 without clamping.
        [
            Some("10"),
            Some("1777777777777777777777"),
            Some("中abcd中"),
            Some("中||x||中"),
            Some("5"),
        ],
        // OCT's original empty string is NULL, while UInt(0) renders "0".
        [None, Some("0"), Some("abcd"), Some("x"), Some("6")],
        // Whitespace is nonempty before trimming, so OCT returns "0". ELT(0)
        // is invalid, not the first candidate.
        [Some("0"), Some("10"), Some("AabcdA"), Some("A||x||A"), None],
        // CONCAT propagates a NULL argument; WS skips it but keeps each empty
        // field, giving |x|. ELT selects the stored NULL first candidate.
        [Some("10"), Some("20"), None, Some("|x|"), None],
    ];
    let expected_binary: [[Option<&[u8]>; 3]; 5] = [
        [None, None, None],
        [
            Some(b"\xff\0abcd\xff\0"),
            Some(b"\xff\0||x||\xff\0"),
            Some(b"5"),
        ],
        [Some(b"abcd"), Some(b"x"), Some(b"6")],
        [Some(b"AabcdA"), Some(b"A||x||A"), None],
        [None, Some(b"|x|"), None],
    ];
    assert_eq!(rows.len(), expected_text.len());
    assert_eq!(rows[1][0], Datum::UInt(u64::MAX));
    for (row_index, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), columns.len());
        for (column, expected) in [1, 2, 3, 5, 7].into_iter().zip(expected_text[row_index]) {
            let value = &row[column];
            if let Some(expected) = expected {
                assert!(matches!(value, Datum::String(_)));
                assert_ne!(value.collation(), Some(tidb_datatype::Collation::Binary));
                assert_eq!(
                    crate::tests_support::cell_text(value),
                    expected,
                    "id {}, column {column}",
                    row_index + 1
                );
            } else {
                assert_eq!(value, &Datum::Null, "id {}, column {column}", row_index + 1);
            }
        }
        // A binary candidate makes ELT binary even when another (text)
        // candidate is selected. CONCAT/WS must also retain raw FF/00 bytes.
        for (column, expected) in [4, 6, 8].into_iter().zip(expected_binary[row_index]) {
            let expected = expected.map_or(Datum::Null, |bytes| {
                Datum::new_collation_string(bytes.to_vec(), tidb_datatype::Collation::Binary)
            });
            assert_eq!(
                row[column],
                expected,
                "id {}, column {column}",
                row_index + 1
            );
        }
    }
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_field_make_export_dispatch_sql_values_and_metadata() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_field_make_export (id INT PRIMARY KEY, n BIGINT, \
             u BIGINT UNSIGNED, f VARCHAR(8) CHARSET utf8mb4 COLLATE utf8mb4_general_ci, \
             fb VARBINARY(8), a VARCHAR(8) CHARSET utf8mb4, z VARCHAR(8) CHARSET utf8mb4, c BIGINT)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_field_make_export VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,9007199254740993,9007199254740993,'b',X'62','Y','N',0),\
             (3,1,32,'',X'','A','',6),(4,33,33,NULL,NULL,NULL,'Z',64),\
             (5,-1,9223372036854775808,'?',X'3F','Y','N',65)",
        )
        .unwrap();

    // FIELD has five/six candidates and MAKE_SET has six, never the old
    // greater-than-64 shift-panic domain. These are value/metadata checks, not
    // lazy SQL-child assertions: the existing SQL children are eager.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT n,u, FIELD(n,9007199254740992,1,2,3,u), \
             FIELD(n,9007199254740992e0,1,2,3,u), \
             FIELD(f,'a','B','c','d','b',''), FIELD(fb,'a','B','c','d','b',''), \
             MAKE_SET(u,a,'2','3','4','5',z), MAKE_SET(u,fb,'2','3','4','5',z), \
             EXPORT_SET(u,a,z), EXPORT_SET(u,a,z,''), EXPORT_SET(u,a,z,'',c) \
             FROM shared_field_make_export ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected FIELD/MAKE_SET/EXPORT_SET dispatch rows")
    };
    assert_eq!(columns.len(), 11);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[1][0], Datum::Int(9_007_199_254_740_993));
    assert_eq!(rows[1][1], Datum::UInt(9_007_199_254_740_993));
    assert_eq!(rows[4][1], Datum::UInt(1_u64 << 63));
    assert!(!columns[0].1.is_unsigned());
    assert!(columns[1].1.is_unsigned());
    for (index, (_, field_type)) in columns[2..6].iter().enumerate() {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::Int);
        assert_eq!(field_type.flen(), 20);
        assert_eq!(field_type.decimal(), 0);
        assert!(!field_type.is_unsigned());
        let collation = if index == 2 {
            tidb_datatype::Collation::Utf8Mb4GeneralCi
        } else {
            tidb_datatype::Collation::Binary
        };
        assert_eq!(field_type.collation(), collation);
    }
    for (index, ((_, field_type), width)) in columns[6..]
        .iter()
        .zip([25, 25, 2300, 2048, 2048])
        .enumerate()
    {
        assert_eq!(field_type.eval_type(), tidb_datatype::EvalType::String);
        assert_eq!(field_type.flen(), width);
        assert_eq!(field_type.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
        if index == 1 {
            assert_eq!(field_type.collation(), tidb_datatype::Collation::Binary);
        } else {
            assert_ne!(field_type.collation(), tidb_datatype::Collation::Binary);
            assert_eq!(field_type.charset_name(), "utf8mb4");
        }
    }
    let assert_text = |value: &Datum, expected: Option<&str>| match expected {
        None => assert_eq!(value, &Datum::Null),
        Some(expected) => {
            assert!(matches!(value, Datum::String(_)));
            assert_ne!(value.collation(), Some(tidb_datatype::Collation::Binary));
            assert_eq!(crate::tests_support::cell_text(value), expected);
        }
    };
    let expected_field = [
        [0, 0, 0, 0], // FIELD(NULL, ...) is 0, not NULL.
        // Exact Int/UInt comparison reaches candidate 5 above 2^53, while the
        // REAL signature rounds onto candidate 1. general_ci finds B before b.
        [5, 1, 2, 5],
        [2, 2, 6, 6],
        [5, 5, 0, 0],
        [0, 0, 0, 0],
    ];
    let expected_make = [None, Some("Y"), Some(""), Some("Z"), Some("")];
    let expected_make_binary = [None, Some("b"), Some(""), Some("Z"), Some("")];
    // Small explicit fixtures, at most 127 output bytes. 2^53+1 has just these
    // two on entries. Bit 5 selects the sixth entry in the next row.
    let mut high_parts = ["N"; 64];
    high_parts[0] = "Y";
    high_parts[53] = "Y";
    let mut sixth_parts = [""; 64];
    sixth_parts[5] = "A";
    let expected_export = [
        [None, None, None],
        [
            Some(high_parts.join(",")),
            Some(high_parts.join("")),
            Some(String::new()),
        ],
        [
            Some(sixth_parts.join(",")),
            Some("A".to_owned()),
            Some("A".to_owned()),
        ],
        [None, None, None],
        // The old native test is signed `(bits & (1 << i)) > 0`: bit 63 is
        // therefore OFF, even though set. count 65 clamps to the same 64 entries.
        [
            Some(["N"; 64].join(",")),
            Some("N".repeat(64)),
            Some("N".repeat(64)),
        ],
    ];
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), columns.len());
        for (value, expected) in row[2..6].iter().zip(expected_field[index]) {
            assert_eq!(value, &Datum::Int(expected), "id {}", index + 1);
        }
        // A selected NULL is skipped; a selected empty sixth candidate is
        // retained. Bit 63 alone selects none of these six candidates.
        assert_text(&row[6], expected_make[index]);
        let binary = expected_make_binary[index].map_or(Datum::Null, |value: &str| {
            Datum::new_collation_string(value.as_bytes().to_vec(), tidb_datatype::Collation::Binary)
        });
        assert_eq!(row[7], binary, "id {}", index + 1);
        for (value, expected) in row[8..].iter().zip(&expected_export[index]) {
            assert_text(value, expected.as_deref());
        }
    }
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_abs_round_decimal_dispatch_sql_values_metadata_and_overflow() {
    use tidb_datatype::FieldTypeCode::{Double, LongLong, NewDecimal};

    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_abs_round_decimal (id INT PRIMARY KEY, i BIGINT, \
             u BIGINT UNSIGNED, d DECIMAL(10,3), w DECIMAL(24,3), r DOUBLE, k BIGINT)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_abs_round_decimal VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,9007199254740993,9007199254740993,2.500,2.500,2.5e0,0),\
             (3,-15,18446744073709551610,-1.255,9007199254740993.125,-2.5e0,-1),\
             (4,42,42,1.234,1.234,1.25e0,4),\
             (5,-9223372036854775808,NULL,NULL,NULL,NULL,NULL)",
        )
        .unwrap();

    // The MIN row is reserved for the separate ABS diagnostic below. Small
    // declared-wide decimals select the decimal CEIL/FLOOR result domain too;
    // the native SQL oracle, not a wire signature, fixes these result types.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT ABS(i),ABS(u),ABS(d),CEIL(d),CEILING(w),FLOOR(w), \
             ROUND(i),ROUND(i,0),ROUND(u,k),ROUND(d,k),ROUND(r), \
             TRUNCATE(u,k),TRUNCATE(d,k) \
             FROM shared_abs_round_decimal WHERE id<5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected native numeric/decimal dispatch rows")
    };
    let unspecified = tidb_datatype::UNSPECIFIED_LENGTH;
    let expected_metadata = [
        (LongLong, 20, 0, false),
        (LongLong, 20, 0, true),
        (NewDecimal, 10, 3, false),
        (LongLong, 20, 0, false),
        (NewDecimal, 24, 0, false),
        (NewDecimal, 24, 0, false),
        (LongLong, 20, 0, false),
        (LongLong, 20, 0, false),
        (LongLong, 20, 0, true),
        (NewDecimal, 10, 3, false),
        (Double, unspecified, unspecified, false),
        (LongLong, 20, 0, true),
        (NewDecimal, 10, 3, false),
    ];
    assert_eq!(columns.len(), expected_metadata.len());
    for ((_, field), (code, flen, decimal, unsigned)) in columns.iter().zip(expected_metadata) {
        assert_eq!(field.code(), code);
        assert_eq!(field.flen(), flen);
        assert_eq!(field.decimal(), decimal);
        assert_eq!(field.is_unsigned(), unsigned);
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert_eq!(field.charset_name(), "binary");
    }
    let expected_rows = [
        ["NULL"; 13],
        // ROUND(i) preserves 2^53+1; ROUND(i,0) and ROUND(u,0) take the old
        // f64 round trip. Decimal ties go away from zero, Real ties to even.
        [
            "9007199254740993",
            "9007199254740993",
            "2.500",
            "3",
            "3",
            "2",
            "9007199254740993",
            "9007199254740992",
            "9007199254740992",
            "3",
            "2",
            "9007199254740993",
            "2",
        ],
        // UInt ...610 is read as signed -6 by ROUND, hence unsigned ...606.
        // TRUNCATE uses exact uint division. FLOOR(w) cannot pass through f64.
        [
            "15",
            "18446744073709551610",
            "1.255",
            "-1",
            "9007199254740994",
            "9007199254740993",
            "-15",
            "-15",
            "18446744073709551606",
            "0",
            "-2",
            "18446744073709551610",
            "0",
        ],
        // A row's requested scale 4 is capped by the declared result scale 3.
        [
            "42", "42", "1.234", "2", "2", "1", "42", "42", "42", "1.234", "1", "42", "1.234",
        ],
    ];
    assert_eq!(rows.len(), expected_rows.len());
    for (row_index, (row, expected)) in rows.iter().zip(expected_rows).enumerate() {
        assert_eq!(row.len(), columns.len());
        for (column, (value, expected)) in row.iter().zip(expected).enumerate() {
            if expected == "NULL" {
                assert_eq!(value, &Datum::Null, "id {}, column {column}", row_index + 1);
                continue;
            }
            match columns[column].1.eval_type() {
                tidb_datatype::EvalType::Int if columns[column].1.is_unsigned() => {
                    assert_eq!(value, &Datum::UInt(expected.parse().unwrap()));
                }
                tidb_datatype::EvalType::Int => {
                    assert_eq!(value, &Datum::Int(expected.parse().unwrap()));
                }
                tidb_datatype::EvalType::Real => {
                    assert_eq!(value, &Datum::Real(expected.parse().unwrap()));
                }
                tidb_datatype::EvalType::Decimal => {
                    let Datum::Decimal(decimal) = value else {
                        panic!(
                            "expected Decimal at id {}, column {column}: {value:?}",
                            row_index + 1
                        )
                    };
                    assert_eq!(decimal.to_string(), expected);
                    // Runtime result scale need not equal FieldType.decimal:
                    // the dynamic-scale columns declare 3 but return scale 0
                    // for k=0/-1, rather than padding every result to 3 places.
                    let scale = expected
                        .split_once('.')
                        .map_or(0, |(_, fraction)| fraction.len() as u32);
                    assert_eq!(decimal.scale(), scale);
                }
                other => panic!("unexpected numeric result domain: {other:?}"),
            }
        }
    }
    assert!(warnings_of(&session).is_empty());

    // A stored operand cannot constant-fold. The SQL diagnostic renders the
    // original qualified column, not the row's MIN datum or a wire code name.
    let mysql = session
        .run_with_columns("SELECT ABS(i) FROM shared_abs_round_decimal WHERE id=5")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(mysql.code, 1690);
    assert_eq!(mysql.state, *b"22003");
    assert_eq!(
        mysql.message,
        "BIGINT value is out of range in 'abs(test.shared_abs_round_decimal.i)'"
    );
    assert!(mysql.is_from_evaluation());
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_char_conv_dispatch_sql_values_metadata_and_diagnostics() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_char_conv (id INT PRIMARY KEY, n BIGINT, \
             v VARCHAR(32) CHARSET utf8mb4, f BIGINT, t BIGINT, u BIGINT UNSIGNED, \
             bad BIGINT, ov VARCHAR(32) CHARSET utf8mb4)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_char_conv VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,14989485,'18446744073709551615',10,-10,18446744073709551606,255,'-18446744073709551616'),\
             (3,0,'',2,16,2,NULL,NULL),\
             (4,-1,'18446744073709551615',-10,16,18446744073709551606,NULL,NULL),\
             (5,4294967361,'-18446744073709551615',10,-16,NULL,NULL,NULL)",
        )
        .unwrap();

    // Stored bases prevent folding the nine-byte binary literal. CONV must
    // preserve that literal through its native base-2 -> from -> to stages,
    // rather than treating it as VARCHAR bytes or a first-eight-byte integer.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT CHAR(n),CHAR(n,n,n,n,n),CONV(v,f,t),CONV(v,u,t), \
             CONV(0x000000000000000020,f,t) FROM shared_char_conv ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected CHAR/CONV dispatch rows")
    };
    assert_eq!(columns.len(), 5);
    assert_eq!(rows.len(), 5);
    for (index, ((_, field), width)) in columns.iter().zip([4, 20, 64, 64, 64]).enumerate() {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
        assert_eq!(field.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
        assert_eq!(field.flen(), width);
        if index < 2 {
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        } else {
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        }
    }
    let char_bytes = [
        Vec::new(), // All numeric NULLs are skipped: empty, not NULL.
        "中".as_bytes().to_vec(),
        vec![0],
        vec![0xff; 4],
        // Old CHAR shifts the full i64 at most four times. Trimming a u32
        // first would incorrectly turn 2^32+65 into just 41 instead of 00000041.
        vec![0, 0, 0, b'A'],
    ];
    let expected_conv = [
        [None, None, None],
        // Unsigned MAX wraps negative for -toBase; UInt base ...606 is raw -10
        // and instead selects the signed fromBase clamp to i64::MAX.
        [Some("-1"), Some("9223372036854775807"), Some("32")],
        [Some("0"), Some("0"), Some("20")],
        [
            Some("7FFFFFFFFFFFFFFF"),
            Some("7FFFFFFFFFFFFFFF"),
            Some("20"),
        ],
        // -u64::MAX wraps to positive 1: recompute sign after wrapping.
        [Some("1"), None, Some("20")],
    ];
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), columns.len());
        for (column, copies) in [(0, 1), (1, 5)] {
            assert_eq!(
                row[column],
                Datum::new_collation_string(
                    char_bytes[index].repeat(copies),
                    tidb_datatype::Collation::Binary
                ),
                "id {}, CHAR arity {copies}",
                index + 1
            );
        }
        for (value, expected) in row[2..].iter().zip(expected_conv[index]) {
            let expected = expected.map_or(Datum::Null, |text| {
                Datum::new_collation_string(
                    text.as_bytes().to_vec(),
                    tidb_datatype::Collation::Utf8Mb4Bin,
                )
            });
            assert_eq!(value, &expected, "id {}", index + 1);
        }
    }
    assert!(warnings_of(&session).is_empty());

    // Exclude the negative integer's invalid UTF-8 from this normal projection.
    // Five numeric arguments remain five; the charset sentinel is not a sixth.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT CHAR(n,n,n,n,n USING utf8) FROM shared_char_conv WHERE id<>4 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected UTF-8 CHAR dispatch rows")
    };
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].1.flen(), 20);
    assert_eq!(columns[0].1.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
    assert_eq!(columns[0].1.charset_name(), "utf8");
    assert_eq!(columns[0].1.collation(), tidb_datatype::Collation::Utf8Bin);
    assert_eq!(rows.len(), 4);
    for (row, index) in rows.iter().zip([0, 1, 2, 4]) {
        assert_eq!(
            row,
            &vec![Datum::new_collation_string(
                char_bytes[index].repeat(5),
                tidb_datatype::Collation::Utf8Bin
            )]
        );
    }
    assert!(warnings_of(&session).is_empty());

    // Native digit overflow is 1690, not the legacy NULL result. The original
    // sign is removed, but the complete overflowing digit string is preserved.
    let mysql = session
        .run_with_columns("SELECT CONV(ov,f,t) FROM shared_char_conv WHERE id=2")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(mysql.code, 1690);
    assert_eq!(mysql.state, *b"22003");
    assert_eq!(
        mysql.message,
        "BIGINT UNSIGNED value is out of range in '18446744073709551616'"
    );
    assert!(mysql.is_from_evaluation());
    assert!(warnings_of(&session).is_empty());

    // One stored bad byte after a valid prefix, in both modes. The existing
    // decoder trims at FF in permissive mode; it does not replace FF with '?'.
    // No claim of lazy SQL children or changed charset-lookup precedence.
    for (mode, expected) in [("STRICT_TRANS_TABLES", None), ("", Some("中"))] {
        session.run(&format!("SET sql_mode='{mode}'")).unwrap();
        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("SELECT CHAR(n,bad USING utf8) FROM shared_char_conv WHERE id=2")
            .unwrap()
        else {
            panic!("expected invalid UTF-8 CHAR result")
        };
        assert_eq!(columns[0].1.flen(), 8);
        assert_eq!(columns[0].1.charset_name(), "utf8");
        assert_eq!(columns[0].1.collation(), tidb_datatype::Collation::Utf8Bin);
        let expected = expected.map_or(Datum::Null, |text| {
            Datum::new_collation_string(text.as_bytes().to_vec(), tidb_datatype::Collation::Utf8Bin)
        });
        assert_eq!(rows, vec![vec![expected]], "sql_mode={mode}");
        assert_eq!(
            warnings_of(&session),
            vec![(1300, "Invalid utf8mb4 character string: 'FF'".to_owned())],
            "sql_mode={mode}"
        );
    }
}



#[test]
fn ready_value_lane_cache_go_trig_dispatch_sql_bits_metadata_and_overflow() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_go_trig (id INT PRIMARY KEY, x DOUBLE, y DOUBLE)")
        .unwrap();
    session
        .run(
            "INSERT INTO shared_go_trig VALUES \
             (1,NULL,1e0),(2,-1e0,1e0),(3,1e0,1e0),(4,0.5e0,NULL),(5,0e0,0e0)",
        )
        .unwrap();

    // Three copied sin/cos/tan vectors from math_fn/go_trig.rs's existing
    // Go-bit goldens, not stdlib calls or calls back into the shared kernel.
    // COT/ATAN read y=1 or NULL so their exact old source goldens suffice;
    // the two-argument spellings read (x,y) and cover either nullable operand.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT SIN(x),COS(x),TAN(x),COT(y),ATAN(y),ATAN(x,y),ATAN2(x,y) \
             FROM shared_go_trig WHERE id<5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected Go trig dispatch rows")
    };
    assert_eq!(columns.len(), 7);
    assert_eq!(rows.len(), 4);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::Double);
        assert_eq!(field.flen(), 23);
        assert_eq!(field.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
        assert!(!field.is_unsigned());
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
    }
    // Existing tests/math.rs source vectors pin these decimal round-trip
    // literals. In particular Go COT(1) ends in 3308, not libm's 3306.
    let cot_one = 0.6420926159343308_f64.to_bits();
    let atan_one = 0.7853981633974483_f64.to_bits();
    let atan_minus_one = (-0.7853981633974483_f64).to_bits();
    let expected_bits: [[Option<u64>; 7]; 4] = [
        [None, None, None, Some(cot_one), Some(atan_one), None, None],
        [
            Some(0xbfeaed548f090cee),
            Some(0x3fe14a280fb5068c),
            Some(0xbff8eb245cbee3a5),
            Some(cot_one),
            Some(atan_one),
            Some(atan_minus_one),
            Some(atan_minus_one),
        ],
        [
            Some(0x3feaed548f090cee),
            Some(0x3fe14a280fb5068c),
            Some(0x3ff8eb245cbee3a5),
            Some(cot_one),
            Some(atan_one),
            Some(atan_one),
            Some(atan_one),
        ],
        [
            Some(0x3fdeaee8744b05f0),
            Some(0x3fec1528065b7d50),
            Some(0x3fe17b4f5bf3474a),
            None,
            None,
            None,
            None,
        ],
    ];
    for (row_index, (row, expected)) in rows.iter().zip(expected_bits).enumerate() {
        assert_eq!(row.len(), columns.len());
        for (column, (value, bits)) in row.iter().zip(expected).enumerate() {
            match (value, bits) {
                (Datum::Null, None) => {}
                (Datum::Real(actual), Some(bits)) => {
                    assert_eq!(
                        actual.to_bits(),
                        bits,
                        "id {}, column {column}",
                        row_index + 1
                    );
                }
                other => panic!(
                    "unexpected Go trig cell: id {}, column {column}: {other:?}",
                    row_index + 1
                ),
            }
        }
    }
    assert!(warnings_of(&session).is_empty());

    // Keep the fifth row out of the normal projection. The frontend's finite
    // policy and existing scalar renderer attach the original column name,
    // not the value spelling "cot(0)", to COT's DOUBLE overflow.
    let mysql = session
        .run_with_columns("SELECT COT(x) FROM shared_go_trig WHERE id=5")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(mysql.code, 1690);
    assert_eq!(mysql.state, *b"22003");
    assert_eq!(
        mysql.message,
        "DOUBLE value is out of range in 'cot(test.shared_go_trig.x)'"
    );
    assert!(mysql.is_from_evaluation());
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_exp_log10_dispatch_sql_bits_metadata_and_diagnostics() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_exp_log10 (id INT PRIMARY KEY, x DOUBLE, n DOUBLE, \
             d DOUBLE, s VARCHAR(16))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_exp_log10 VALUES \
             (1,NULL,NULL,NULL,NULL),(2,1.5e0,100e0,NULL,'2020-01-01'),\
             (3,0e0,100e0,0e0,NULL),(4,0e0,NULL,-1e0,NULL)",
        )
        .unwrap();

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT EXP(x),LOG10(n) FROM shared_exp_log10 ORDER BY id")
        .unwrap()
    else {
        panic!("expected native EXP/LOG10 dispatch rows")
    };
    assert_eq!(columns.len(), 2);
    assert_eq!(rows.len(), 4);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::Double);
        assert_eq!(field.flen(), 23);
        assert_eq!(field.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
        assert!(!field.is_unsigned());
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
    }
    // Preserve the original Go EXP(1.5) golden, not libm's neighboring value.
    // These are fixed round-trip literals, never shared-kernel/stdlib oracles.
    let expected_bits: [[Option<u64>; 2]; 4] = [
        [None, None],
        [
            Some(4.481689070338065_f64.to_bits()),
            Some(2.0_f64.to_bits()),
        ],
        [Some(1.0_f64.to_bits()), Some(2.0_f64.to_bits())],
        [Some(1.0_f64.to_bits()), None],
    ];
    for (row_index, (row, expected)) in rows.iter().zip(expected_bits).enumerate() {
        assert_eq!(row.len(), 2);
        for (column, (value, bits)) in row.iter().zip(expected).enumerate() {
            match (value, bits) {
                (Datum::Null, None) => {}
                (Datum::Real(actual), Some(bits)) => {
                    assert_eq!(
                        actual.to_bits(),
                        bits,
                        "id {}, column {column}",
                        row_index + 1
                    );
                }
                other => panic!(
                    "unexpected EXP/LOG10 cell: id {}, column {column}: {other:?}",
                    row_index + 1
                ),
            }
        }
    }
    assert!(warnings_of(&session).is_empty());

    // Both zero and negative LOG10 inputs are NULL plus the original 3020.
    // Keep their warning-bearing projection separate from ordinary values.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT LOG10(d) FROM shared_exp_log10 WHERE id>=3 ORDER BY id")
        .unwrap()
    else {
        panic!("expected LOG10 domain rows")
    };
    assert_eq!(rows, vec![vec![Datum::Null], vec![Datum::Null]]);
    assert_eq!(
        warnings_of(&session),
        vec![(3020, "Invalid argument for logarithm".to_owned()); 2]
    );

    // EXP formats the evaluated 2020 after its one ETReal coercion warning.
    // Unlike COT, this error must not name the source column or raw date text.
    let mysql = session
        .run_with_columns("SELECT EXP(s) FROM shared_exp_log10 WHERE id=2")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(mysql.code, 1690);
    assert_eq!(mysql.state, *b"22003");
    assert_eq!(mysql.message, "DOUBLE value is out of range in 'exp(2020)'");
    assert!(mysql.is_from_evaluation());
    assert_eq!(
        session.warnings(),
        &[SqlWarning {
            level: WarningLevel::Warning,
            code: 1292,
            message: "Truncated incorrect DOUBLE value: '2020-01-01'".to_owned(),
        }]
    );
}



#[test]
fn ready_value_lane_cache_compress_uncompress_dispatch_sql_bytes_metadata_and_diagnostics() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_compress_uncompress (id INT PRIMARY KEY, \
             v VARBINARY(16), f VARBINARY(64))",
        )
        .unwrap();
    // Fixed pre-migration go_flate/crypto source vectors, not a compression
    // generator or a new shared-kernel result used as its own expected value.
    let go_hello = "0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D";
    let wire_hello = "0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D";
    let wrong_len = "02000000789CCB48CDC9C95728CF2FCA4901001A0B045D";
    // The Go frame with exactly its last checksum byte removed. Decoding all
    // eleven plaintext bytes is insufficient without a complete zlib stream.
    let truncated = "0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B04";
    session
        .run(&format!(
            "INSERT INTO shared_compress_uncompress VALUES \
             (1,NULL,NULL),(2,x'',x''),\
             (3,x'68656C6C6F20776F726C64',x'{go_hello}'),\
             (4,x'00FF20',x'{wire_hello}'),\
             (5,NULL,x'0B0000001234'),(6,NULL,x'{wrong_len}'),(7,NULL,x'{truncated}')"
        ))
        .unwrap();

    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT HEX(COMPRESS(v)),HEX(UNCOMPRESS(f)),HEX(UNCOMPRESS(COMPRESS(v))) \
             FROM shared_compress_uncompress WHERE id<5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected compression byte rows")
    };
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| row.len() == 3));
    assert!(rows[0].iter().all(Datum::is_null));
    for value in &rows[1] {
        assert_eq!(cell_text(value), "");
    }
    let hello_hex = "68656C6C6F20776F726C64";
    assert_eq!(cell_text(&rows[2][0]), go_hello);
    assert_eq!(cell_text(&rows[2][1]), hello_hex);
    assert_eq!(cell_text(&rows[2][2]), hello_hex);
    assert_eq!(cell_text(&rows[3][1]), hello_hex);
    assert_eq!(cell_text(&rows[3][2]), "00FF20");
    // This raw-byte case is deliberately NOT a full independent compressed
    // golden: framing plus roundtrip are correlated checks. 00 FF 20 has an
    // Adler low byte 20, so the existing framing rule appends the trailing dot.
    let raw_compressed = cell_text(&rows[3][0]);
    assert!(raw_compressed.starts_with("03000000"));
    assert!(raw_compressed.ends_with("202E"));
    assert!(warnings_of(&session).is_empty());

    // Query the bodies separately: HEX's own return type cannot establish
    // COMPRESS's 16+13 bound or UNCOMPRESS's promoted maximum-width BLOB type.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT COMPRESS(v),UNCOMPRESS(f) FROM shared_compress_uncompress WHERE id=3",
        )
        .unwrap()
    else {
        panic!("expected compression metadata row")
    };
    assert_eq!(columns.len(), 2);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    for ((_, field), (code, width)) in columns.iter().zip([
        (tidb_datatype::FieldTypeCode::VarString, 29),
        (tidb_datatype::FieldTypeCode::LongBlob, 16_777_216),
    ]) {
        assert_eq!(field.code(), code);
        assert_eq!(field.flen(), width);
        assert_eq!(field.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(field.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
        assert!(!field.is_unsigned());
    }
    assert!(warnings_of(&session).is_empty());

    // These are independent stored frames, not malformed outputs generated by
    // the new encoder. All diagnostics follow a computed decoder disposition.
    for (id, code, message) in [
        (5, 1259, "ZLIB: Input data corrupted"),
        (6, 1258, "ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)"),
        (7, 1259, "ZLIB: Input data corrupted"),
    ] {
        let sql = format!("SELECT UNCOMPRESS(f) FROM shared_compress_uncompress WHERE id={id}");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected UNCOMPRESS diagnostic row: {sql}")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        assert_eq!(
            session.warnings(),
            &[SqlWarning {
                level: WarningLevel::Warning,
                code,
                message: message.to_owned(),
            }],
            "{sql}"
        );
    }
}



#[test]
fn ready_value_lane_cache_json_report_dispatch_sql_values_metadata_and_errors() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_json_report (id INT PRIMARY KEY, \
             v VARCHAR(64) CHARSET utf8mb4, n BIGINT, d DATE, b VARBINARY(1))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_json_report VALUES \
             (1,NULL,NULL,NULL,NULL),(2,'null',NULL,NULL,NULL),\
             (3,'{\"a\":[{\"b\":1}],\"a\":2,\"c\":[0]}',42,'2020-01-01',x'FF'),\
             (4,'9223372036854775807',NULL,NULL,NULL),\
             (5,'a',NULL,NULL,NULL),(6,'',NULL,NULL,NULL)",
        )
        .unwrap();

    // Native document parsing retains the signed i64 maximum and duplicate
    // keys are last-wins: the overwritten {"b":1} branch must not add depth.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT JSON_VALID(v),JSON_TYPE(v),JSON_DEPTH(v) \
             FROM shared_json_report WHERE id<5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected JSON report rows")
    };
    assert_eq!(columns.len(), 3);
    for (index, (_, field)) in columns.iter().enumerate() {
        assert!(!field.is_unsigned());
        if index == 1 {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
            assert_eq!(field.flen(), tidb_datatype::UNSPECIFIED_LENGTH);
            assert_eq!(field.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        } else {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert_eq!(field.flen(), 20);
            assert_eq!(field.decimal(), 0);
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
            assert_eq!(
                field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN),
                index == 0
            );
        }
    }
    let text = |value: &str| {
        Datum::new_collation_string(
            value.as_bytes().to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )
    };
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null, Datum::Null, Datum::Null],
            vec![Datum::Int(1), text("NULL"), Datum::Int(1)],
            vec![Datum::Int(1), text("OBJECT"), Datum::Int(3)],
            vec![Datum::Int(1), text("INTEGER"), Datum::Int(1)],
        ]
    );
    assert!(warnings_of(&session).is_empty());

    // Stored DATE -> typed JSON preserves TYPE=DATE; a text roundtrip would
    // incorrectly answer STRING. DEPTH deliberately uses its original Display
    // path. VALID's typed/Other/raw-invalid-UTF8 signatures stay distinct.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT JSON_VALID(CAST(d AS JSON)),JSON_TYPE(CAST(d AS JSON)), \
             JSON_DEPTH(CAST(d AS JSON)),JSON_VALID(n),JSON_DEPTH(n),JSON_VALID(b) \
             FROM shared_json_report WHERE id=3",
        )
        .unwrap()
    else {
        panic!("expected typed and non-document JSON report row")
    };
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(1),
            text("DATE"),
            Datum::Int(1),
            Datum::Int(0),
            Datum::Int(1),
            Datum::Int(0)
        ]]
    );
    assert!(warnings_of(&session).is_empty());

    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT JSON_VALID(v) FROM shared_json_report WHERE id>=5 ORDER BY id")
        .unwrap()
    else {
        panic!("expected invalid and empty JSON validity rows")
    };
    assert_eq!(rows, vec![vec![Datum::Int(0)], vec![Datum::Int(0)]]);
    assert!(warnings_of(&session).is_empty());

    // The original JsonError variants own these exact messages. Neither a new
    // parser's detailed error nor an admission failure may replace them here.
    for (id, message) in [
        (
            5,
            "Invalid JSON text: The document root must not be followed by other values.",
        ),
        (6, "Invalid JSON text: The document is empty"),
    ] {
        for expression in ["JSON_TYPE(v)", "JSON_DEPTH(v)"] {
            let sql = format!("SELECT {expression} FROM shared_json_report WHERE id={id}");
            let mysql = session
                .run_with_columns(&sql)
                .expect_err(&sql)
                .to_mysql_error();
            assert_eq!(mysql.code, 3140, "{sql}");
            assert_eq!(mysql.state, *b"22032", "{sql}");
            assert_eq!(mysql.message, message, "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}



#[test]
fn ready_value_lane_cache_json_storage_quote_dispatch_sql_values_metadata_and_errors() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_json_storage_quote (id INT PRIMARY KEY, \
             v VARCHAR(64) CHARSET utf8mb4)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_json_storage_quote VALUES \
             (1,NULL),(2,'null'),(3,'[1,true,\"x\"]'),\
             (4,'[{\"a\":{\"a\":1},\"b\":2}]'),(5,'a'),(6,''),\
             (7,x'070B3C3E26E280A8E280A9')",
        )
        .unwrap();

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT JSON_STORAGE_FREE(v),JSON_STORAGE_SIZE(v),JSON_QUOTE(v) \
             FROM shared_json_storage_quote WHERE id<5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected JSON storage and quote rows")
    };
    assert_eq!(columns.len(), 3);
    for (index, (_, field)) in columns.iter().enumerate() {
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
        if index == 2 {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
            assert_eq!(field.flen(), tidb_datatype::UNSPECIFIED_LENGTH);
            assert_eq!(field.decimal(), tidb_datatype::UNSPECIFIED_LENGTH);
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        } else {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert_eq!(field.flen(), 20);
            assert_eq!(field.decimal(), 0);
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        }
    }
    let text = |value: &str| {
        Datum::new_collation_string(
            value.as_bytes().to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )
    };
    // 82 is the unchanged nested-object source fixture. The mixed array's 34
    // is the old layout: root tag 1 + header 8 + entries 3*5 + number 8 + string
    // length/payload 2; true is inline. No new encoder supplies these expected sizes.
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null, Datum::Null, Datum::Null],
            vec![Datum::Int(0), Datum::Int(2), text(r#""null""#)],
            vec![Datum::Int(0), Datum::Int(34), text(r#""[1,true,\"x\"]""#)],
            vec![
                Datum::Int(0),
                Datum::Int(82),
                text(r#""[{\"a\":{\"a\":1},\"b\":2}]""#)
            ],
        ]
    );
    assert!(warnings_of(&session).is_empty());

    // The stored bytes contain actual BEL/VT, not SQL backslash sequences.
    // Keep the old serde JSON rules independently: \u0007/\u000b (not wire
    // \a/\v), with HTML and U+2028/U+2029 left as their original UTF-8 bytes.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT HEX(v),HEX(JSON_QUOTE(v)) FROM shared_json_storage_quote WHERE id=7",
        )
        .unwrap()
    else {
        panic!("expected JSON quote control-byte row")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(cell_text(&rows[0][0]), "070B3C3E26E280A8E280A9");
    assert_eq!(
        cell_text(&rows[0][1]),
        "225C75303030375C75303030623C3E26E280A8E280A922"
    );
    assert!(warnings_of(&session).is_empty());

    // Admitted storage calls parse both malformed and empty documents, even
    // though every successful STORAGE_FREE result is zero. Keep the original
    // JsonError variants, not a new parser's detail or a fabricated NULL.
    for (id, message) in [
        (
            5,
            "Invalid JSON text: The document root must not be followed by other values.",
        ),
        (6, "Invalid JSON text: The document is empty"),
    ] {
        for expression in ["JSON_STORAGE_FREE(v)", "JSON_STORAGE_SIZE(v)"] {
            let sql = format!("SELECT {expression} FROM shared_json_storage_quote WHERE id={id}");
            let mysql = session
                .run_with_columns(&sql)
                .expect_err(&sql)
                .to_mysql_error();
            assert_eq!(mysql.code, 3140, "{sql}");
            assert_eq!(mysql.state, *b"22032", "{sql}");
            assert_eq!(mysql.message, message, "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}



#[test]
fn ready_value_lane_cache_date_fields_dispatch_sql_values_metadata_and_coercion() {
    let mut session = Session::new();
    session
        .run("SET sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'")
        .unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_date_fields (id INT PRIMARY KEY, \
             d DATETIME, s VARCHAR(32), n BIGINT)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_date_fields VALUES (1,NULL,NULL,NULL),\
             (3,'2024-02-29 12:34:56','not-a-date',20240315),\
             (4,'2021-12-31 23:59:59',NULL,NULL)",
        )
        .unwrap();
    // The recorded INSERT IGNORE mechanism stores a typed zero. Do not use a
    // read-path string CAST here: NO_ZERO_DATE would turn that into SQL NULL.
    session
        .run("INSERT IGNORE INTO shared_date_fields VALUES (2,0,'0000-00-00',NULL)")
        .unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].level, WarningLevel::Warning);
    assert_eq!(session.warnings()[0].code, 1292);
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT d FROM shared_date_fields WHERE id=2")
        .unwrap()
    else {
        panic!("expected a stored zero DATETIME")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
    assert!(matches!(&rows[0][0], Datum::Time(time) if time.is_zero()));
    assert_eq!(cell_text(&rows[0][0]), "0000-00-00 00:00:00");
    // Only ordinary setup SQL cleared the INSERT warning. No evaluation
    // warnings are manually drained or suppressed after installing the policy.
    assert!(warnings_of(&session).is_empty());

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT YEAR(d),MONTH(d),DAYOFMONTH(d),DAY(d),QUARTER(d) \
             FROM shared_date_fields ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected stored date-field rows")
    };
    assert_eq!(columns.len(), 5);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
        assert_eq!(field.flen(), 20);
        assert_eq!(field.decimal(), 0);
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 5],
            vec![Datum::Int(0); 5],
            vec![
                Datum::Int(2024),
                Datum::Int(2),
                Datum::Int(29),
                Datum::Int(29),
                Datum::Int(1)
            ],
            vec![
                Datum::Int(2021),
                Datum::Int(12),
                Datum::Int(31),
                Datum::Int(31),
                Datum::Int(4)
            ],
        ]
    );
    assert!(warnings_of(&session).is_empty());

    // A numeric source keeps its original packed-date cast, rather than
    // becoming a string or being interpreted as a native calendar field.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT YEAR(n),MONTH(n),DAY(n),QUARTER(n) FROM shared_date_fields WHERE id=3",
        )
        .unwrap()
    else {
        panic!("expected numeric-source date fields")
    };
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(2024),
            Datum::Int(3),
            Datum::Int(15),
            Datum::Int(1)
        ]]
    );
    assert!(warnings_of(&session).is_empty());

    // The string zero differs from the stored typed zero above. The original
    // NO_ZERO_DATE cast warning renders the parsed zero at MaxFsp, not the
    // source spelling; the bad-string warning instead retains its raw text.
    for (expression, id, message) in [
        ("YEAR(s)", 3, "Incorrect datetime value: 'not-a-date'"),
        (
            "MONTH(s)",
            2,
            "Incorrect datetime value: '0000-00-00 00:00:00.000000'",
        ),
    ] {
        let sql = format!("SELECT {expression} FROM shared_date_fields WHERE id={id}");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected cast-to-NULL date-field row: {sql}")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        assert_eq!(
            session.warnings(),
            &[SqlWarning {
                level: WarningLevel::Warning,
                code: 1292,
                message: message.to_owned(),
            }],
            "{sql}"
        );
    }
}



#[test]
fn ready_value_lane_cache_hms_dispatch_sql_values_metadata_and_native_text_policy() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_hms (id INT PRIMARY KEY, \
             v VARCHAR(32), n BIGINT, t TIME(1))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_hms VALUES (1,NULL,NULL,NULL),\
             (2,'900:30:15',NULL,NULL),(3,'-12:34:56.9',-103045,'-12:34:56.9'),\
             (4,'2024-01-15',NULL,NULL),(5,'2024-01-15 10:30:45',NULL,NULL),\
             (6,'12:60:00',NULL,NULL)",
        )
        .unwrap();
    assert!(warnings_of(&session).is_empty());

    // Original native text rules, not an ETDuration cast or the legacy nanos
    // adapter: overflowing hours clamp the WHOLE clock; fractions do not round;
    // a bare date decodes its leading digits as HHMMSS rather than midnight.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT HOUR(v),MINUTE(v),SECOND(v) FROM shared_hms ORDER BY id")
        .unwrap()
    else {
        panic!("expected native HMS text rows")
    };
    assert_eq!(columns.len(), 3);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
        assert_eq!(field.flen(), 20);
        assert_eq!(field.decimal(), 0);
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 3],
            vec![Datum::Int(838), Datum::Int(59), Datum::Int(59)],
            vec![Datum::Int(12), Datum::Int(34), Datum::Int(56)],
            vec![Datum::Int(0), Datum::Int(20), Datum::Int(24)],
            vec![Datum::Int(10), Datum::Int(30), Datum::Int(45)],
            vec![Datum::Null; 3],
        ]
    );
    // Invalid minute text produces NULL without a cast warning. Adding a
    // duration cast before admission would change both this and the clamp row.
    assert!(warnings_of(&session).is_empty());

    // The numeric source uses its signed decimal text. A legal typed TIME(1)
    // retains the original Duration Display/FSP -> text parse path, including
    // the negative sign and discarded (not rounded) fractional second.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT HOUR(n),MINUTE(n),SECOND(n),HOUR(t),MINUTE(t),SECOND(t) \
             FROM shared_hms WHERE id=3",
        )
        .unwrap()
    else {
        panic!("expected numeric and typed-duration HMS row")
    };
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(10),
            Datum::Int(30),
            Datum::Int(45),
            Datum::Int(12),
            Datum::Int(34),
            Datum::Int(56)
        ]]
    );
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_monthname_time_to_sec_sql_values_metadata_and_warnings() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_monthname_ttsec (id INT PRIMARY KEY, d VARCHAR(32), s VARCHAR(32), n BIGINT)").unwrap();
    session
        .run(
            "INSERT INTO shared_monthname_ttsec VALUES (1,NULL,NULL,NULL),\
         (2,'2017-12-01','-02:00:05.999',20005),(3,'2000-01-01','900:00:00',NULL),\
         (4,'2011-11-11','junk',NULL),(5,'2017-00-01','',NULL),\
         (6,'not-a-date','2017-12-01 02:00:05',NULL)",
        )
        .unwrap();
    // Original independent parsers: month-zero is not a raw MONTH lookup;
    // duration overflow is NULL, junk/empty are zero, and fractions never round.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT MONTHNAME(d),TIME_TO_SEC(s) FROM shared_monthname_ttsec ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected month-name and duration rows")
    };
    assert_eq!(columns.len(), 2);
    for ((_, field), (code, flen, decimal)) in columns.iter().zip([
        (tidb_datatype::FieldTypeCode::VarString, -1, -1),
        (tidb_datatype::FieldTypeCode::LongLong, 20, 0),
    ]) {
        assert_eq!(
            (field.code(), field.flen(), field.decimal()),
            (code, flen, decimal)
        );
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    assert_eq!(columns[0].1.charset_name(), "utf8mb4");
    assert_eq!(
        columns[0].1.collation(),
        tidb_datatype::Collation::Utf8Mb4Bin
    );
    assert_eq!(columns[1].1.charset_name(), "binary");
    assert_eq!(columns[1].1.collation(), tidb_datatype::Collation::Binary);
    let text = |s: &str| {
        Datum::new_collation_string(s.as_bytes().to_vec(), tidb_datatype::Collation::Utf8Mb4Bin)
    };
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null, Datum::Null],
            vec![text("December"), Datum::Int(-7205)],
            vec![text("January"), Datum::Null],
            vec![text("November"), Datum::Int(0)],
            vec![Datum::Null, Datum::Int(0)],
            vec![Datum::Null, Datum::Int(7205)],
        ]
    );
    // Only the bad DATE's original ETDatetime cast warns. A zero-in-date
    // reaches the native validator; TIME_TO_SEC does not add an ETDuration cast.
    assert_eq!(
        session.warnings(),
        &[SqlWarning {
            level: WarningLevel::Warning,
            code: 1292,
            message: "Incorrect datetime value: 'not-a-date'".to_owned(),
        }]
    );
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT TIME_TO_SEC(n) FROM shared_monthname_ttsec WHERE id=2")
        .unwrap()
    else {
        panic!("expected compact numeric duration row")
    };
    assert_eq!(rows, vec![vec![Datum::Int(7205)]]);
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_period_get_format_sql_values_metadata_and_errors() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_period_format (id INT PRIMARY KEY, \
             p BIGINT, d BIGINT, a BIGINT, b BIGINT, loc VARCHAR(16))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_period_format VALUES (1,0,NULL,NULL,0,NULL),\
             (2,201611,2,201701,201611,'USA'),(3,1611,3,201702,1611,'eur'),\
             (4,7011,3,197102,7011,'unknown'),(5,201611,-13,201510,201611,'INTERNAL'),\
             (6,0,3,0,201611,'unknown')",
        )
        .unwrap();
    // All arithmetic answers are fixed old period vectors. In row one, a
    // NULL companion must win over invalid period zero in either position.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT PERIOD_ADD(p,d),PERIOD_DIFF(a,b),GET_FORMAT(DATE,loc) \
             FROM shared_period_format WHERE id<=5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected period and format rows")
    };
    assert_eq!(columns.len(), 3);
    for (index, (_, field)) in columns.iter().enumerate() {
        let expected = if index == 2 {
            (
                tidb_datatype::FieldTypeCode::VarString,
                17,
                -1,
                "utf8mb4",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        } else {
            (
                tidb_datatype::FieldTypeCode::LongLong,
                20,
                0,
                "binary",
                tidb_datatype::Collation::Binary,
            )
        };
        assert_eq!(
            (
                field.code(),
                field.flen(),
                field.decimal(),
                field.charset_name(),
                field.collation()
            ),
            expected
        );
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let text = |s: &str| {
        Datum::new_collation_string(s.as_bytes().to_vec(), tidb_datatype::Collation::Utf8Mb4Bin)
    };
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 3],
            vec![Datum::Int(201701), Datum::Int(2), text("%m.%d.%Y")],
            vec![Datum::Int(201702), Datum::Int(3), text("%d.%m.%Y")],
            vec![Datum::Int(197102), Datum::Int(3), text("")],
            vec![Datum::Int(201510), Datum::Int(-13), text("%Y%m%d")],
        ]
    );
    assert!(warnings_of(&session).is_empty());
    // With both coerced operands present, validation belongs to the worker.
    // Preserve the original classed error, not an adapter refusal or SQL NULL.
    for (expression, message) in [
        ("PERIOD_ADD(p,d)", "Incorrect arguments to period_add"),
        ("PERIOD_DIFF(a,b)", "Incorrect arguments to period_diff"),
    ] {
        let sql = format!("SELECT {expression} FROM shared_period_format WHERE id=6");
        let mysql = session
            .run_with_columns(&sql)
            .expect_err(&sql)
            .to_mysql_error();
        assert_eq!(mysql.code, 1210, "{sql}");
        assert_eq!(mysql.message, message, "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        assert!(warnings_of(&session).is_empty(), "{sql}");
    }
}



#[test]
fn ready_value_lane_cache_weekday_dayname_sql_values_metadata_and_year_zero() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_weekday_dayname (id INT PRIMARY KEY, d VARCHAR(32))")
        .unwrap();
    session
        .run(
            "INSERT INTO shared_weekday_dayname VALUES (1,NULL),(2,'2017-12-01'),\
             (3,'0000-01-01'),(4,'2000-02-29'),(5,'2017-00-01'),(6,'2017-01-00')",
        )
        .unwrap();
    // Preserve the existing ETDatetime cast. Its legal year zero is not a
    // zero date; zero month/day survive the read cast but fail full validation.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT DAYOFWEEK(d),WEEKDAY(d),DAYOFYEAR(d),DAYNAME(d) \
             FROM shared_weekday_dayname ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected weekday and day-name rows")
    };
    assert_eq!(columns.len(), 4);
    for (index, (_, field)) in columns.iter().enumerate() {
        let expected = if index == 3 {
            (
                tidb_datatype::FieldTypeCode::VarString,
                -1,
                -1,
                "utf8mb4",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        } else {
            (
                tidb_datatype::FieldTypeCode::LongLong,
                20,
                0,
                "binary",
                tidb_datatype::Collation::Binary,
            )
        };
        assert_eq!(
            (
                field.code(),
                field.flen(),
                field.decimal(),
                field.charset_name(),
                field.collation()
            ),
            expected
        );
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let text = |s: &str| {
        Datum::new_collation_string(s.as_bytes().to_vec(), tidb_datatype::Collation::Utf8Mb4Bin)
    };
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 4],
            vec![
                Datum::Int(6),
                Datum::Int(4),
                Datum::Int(335),
                text("Friday")
            ],
            vec![
                Datum::Int(7),
                Datum::Int(5),
                Datum::Int(1),
                text("Saturday")
            ],
            vec![
                Datum::Int(3),
                Datum::Int(1),
                Datum::Int(60),
                text("Tuesday")
            ],
            vec![Datum::Null; 4],
            vec![Datum::Null; 4],
        ]
    );
    assert!(warnings_of(&session).is_empty());
    // Years 0 and 2000 share this weekday. Check the actual cast datum too,
    // rather than accidentally accepting a century-pivoted year as evidence.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT CAST(d AS DATETIME) FROM shared_weekday_dayname WHERE id=3")
        .unwrap()
    else {
        panic!("expected legal year-zero datetime")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
    assert!(matches!(&rows[0][0], Datum::Time(time) if time.core_time().year() == 0));
    assert_eq!(cell_text(&rows[0][0]), "0000-01-01 00:00:00");
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_date_serial_tso_logical_sql_values_and_metadata() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_date_serial (id INT PRIMARY KEY, \
             l VARCHAR(32), r VARCHAR(32), d VARCHAR(32), s VARCHAR(32), n BIGINT)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_date_serial VALUES (1,NULL,NULL,NULL,NULL,NULL),\
             (2,'2004-05-21','2004:01:02','2007-10-07 00:00:59','2009-11-29 13:43:32',452605852463012352),\
             (3,'2008-12-31 23:59:59.000001','2008-12-30 01:01:01.000002','0000-01-01','0000-01-01',262144),\
             (4,'0000-03-01','0000-02-29','1998-10-00','1998-00-11',0),\
             (5,'1010-11-30 23:59:59','2010-12-31','2008-10-07','2009-11-29',-1),\
             (6,NULL,NULL,NULL,NULL,262143)",
        )
        .unwrap();
    // Text DATEDIFF is civil-date arithmetic and ignores the clock. TO_DAYS
    // and TO_SECONDS use strict datetime parsing and MySQL's day-number epoch;
    // year-zero Feb 29 -> Mar 1 is civil one day, not the legacy raw-core zero.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT DATEDIFF(l,r),TO_DAYS(d),TO_SECONDS(s),TIDB_PARSE_TSO_LOGICAL(n) \
             FROM shared_date_serial ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected date-serial and logical-TSO rows")
    };
    assert_eq!(columns.len(), 4);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
        assert_eq!(field.flen(), 20);
        assert_eq!(field.decimal(), 0);
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 4],
            vec![
                Datum::Int(140),
                Datum::Int(733321),
                Datum::Int(63_426_721_412),
                Datum::Int(137728)
            ],
            vec![
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(86400),
                Datum::Int(0)
            ],
            vec![Datum::Int(1), Datum::Null, Datum::Null, Datum::Null],
            vec![
                Datum::Int(-365274),
                Datum::Int(733687),
                Datum::Int(63_426_672_000),
                Datum::Null
            ],
            vec![Datum::Null, Datum::Null, Datum::Null, Datum::Int(262143)],
        ]
    );
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_lane_cache_week_modes_sql_values_metadata_and_probe_order() {
    let mut session = Session::new();
    session
        .run(
            "SET tidb_executor_concurrency=1, tidb_projection_concurrency=1, default_week_format=1",
        )
        .unwrap();
    session
        .run("CREATE TABLE shared_week_modes (id INT PRIMARY KEY, d VARCHAR(32), m VARBINARY(1))")
        .unwrap();
    session
        .run(
            "INSERT INTO shared_week_modes VALUES (1,NULL,x'FF'),(2,'2008-02-20',NULL),\
         (3,'2000-01-01',NULL),(4,'2016-00-05',x'FF'),(5,'0000-01-01','3')",
        )
        .unwrap();
    // One slot is sufficient only if the first probe lease is released before
    // mode preparation and the final week worker. Invalid dates never read m.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT WEEK(d),WEEK(d,m),WEEKOFYEAR(d),YEARWEEK(d),YEARWEEK(d,m) \
         FROM shared_week_modes WHERE id<=4 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected week-mode rows")
    };
    assert_eq!(columns.len(), 5);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
        assert_eq!(field.flen(), 20);
        assert_eq!(field.decimal(), 0);
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 5],
            vec![
                Datum::Int(8),
                Datum::Int(7),
                Datum::Int(8),
                Datum::Int(200807),
                Datum::Int(200807)
            ],
            vec![
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(52),
                Datum::Int(199952),
                Datum::Int(199952)
            ],
            vec![Datum::Null; 5],
        ]
    );
    assert!(warnings_of(&session).is_empty());
    session.run("SET default_week_format=0").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT WEEK(d) FROM shared_week_modes WHERE id=2")
        .unwrap()
    else {
        panic!("expected changed default mode")
    };
    assert_eq!(rows, vec![vec![Datum::Int(7)]]);
    assert!(warnings_of(&session).is_empty());
    // Only the ISO mode here has the negative year; do not infer the sentinel
    // from year zero alone or use the session default for YEARWEEK.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT YEARWEEK(d,m) FROM shared_week_modes WHERE id=5")
        .unwrap()
    else {
        panic!("expected negative week-year sentinel")
    };
    assert_eq!(rows, vec![vec![Datum::Int(4_294_967_295)]]);
    assert!(warnings_of(&session).is_empty());
}

#[test]
fn ready_value_lane_cache_password_sm3_sql_values_metadata_and_deprecation() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_password_sm3 (id INT PRIMARY KEY, v VARCHAR(3))")
        .unwrap();
    session
        .run("INSERT INTO shared_password_sm3 VALUES (1,NULL),(2,''),(3,'abc')")
        .unwrap();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT PASSWORD(v),SM3(v) FROM shared_password_sm3 ORDER BY id")
        .unwrap()
    else {
        panic!("expected native password and SM3 rows")
    };
    assert_eq!(columns.len(), 2);
    for ((_, field), flen) in columns.iter().zip([41, 40]) {
        // SM3's source metadata is 40 even though its digest has 64 hex digits.
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
        assert_eq!(field.flen(), flen);
        assert_eq!(field.decimal(), -1);
        assert_eq!(field.charset_name(), "utf8mb4");
        assert_eq!(field.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let text = |s: &str| {
        Datum::new_collation_string(s.as_bytes().to_vec(), tidb_datatype::Collation::Utf8Mb4Bin)
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Datum::Null; 2]);
    assert_eq!(rows[1].len(), 2);
    assert_eq!(rows[1][0], text(""));
    // The old fixtures pin abc, not empty SM3: this is a shape assertion only,
    // not a newly recorded independent digest golden.
    let empty_digest = cell_text(&rows[1][1]);
    assert_eq!(empty_digest.len(), 64);
    assert!(empty_digest
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(
        rows[2],
        vec![
            text("*0D3CED9BEC10A777AEC23CCC353A8C08A633045E"),
            text("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0"),
        ]
    );
    assert_eq!(session.warnings().len(), 3);
    for warning in session.warnings() {
        assert_eq!(
            warning,
            &SqlWarning {
                level: WarningLevel::Warning,
                code: 1681,
                message: "PASSWORD is deprecated and will be removed in a future release."
                    .to_owned(),
            }
        );
    }
}



#[test]
fn ready_value_lane_cache_make_date_from_days_sql_typed_dates_and_metadata() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("SET sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'")
        .unwrap();
    session.run("CREATE TABLE shared_date_constructors (id INT PRIMARY KEY, y BIGINT, d BIGINT, n BIGINT)").unwrap();
    session
        .run(
            "INSERT INTO shared_date_constructors VALUES (1,NULL,NULL,NULL),\
         (2,69,1,734927),(3,70,1,365),(4,2024,60,3652425),\
         (5,-1,1,3652499),(6,10000,1,3652500)",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT MAKEDATE(y,d),FROM_DAYS(n) FROM shared_date_constructors ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected typed date constructors")
    };
    assert_eq!(columns.len(), 2);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::Date);
        assert_eq!((field.flen(), field.decimal()), (10, 0));
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let expected = [
        ["NULL", "NULL"],
        ["2069-01-01", "2012-02-29"],
        ["1970-01-01", "0000-00-00"],
        ["2024-02-29", "NULL"],
        ["NULL", "NULL"],
        ["NULL", "0000-00-00"],
    ];
    assert_eq!(rows.len(), expected.len());
    for (row, expected) in rows.iter().zip(expected) {
        assert_eq!(row.len(), 2);
        for (value, expected) in row.iter().zip(expected) {
            assert!(matches!(value, Datum::Null | Datum::Time(_)));
            assert_eq!(cell_text(value), expected);
        }
    }
    // Both sides of the exceptional NULL band retain actual typed zero dates,
    // even under NO_ZERO_DATE; reparsing a zero string would change the result.
    for row in [2, 5] {
        assert!(matches!(&rows[row][1], Datum::Time(time) if time.is_zero()));
    }
    assert!(warnings_of(&session).is_empty());
}

#[test]
fn ready_value_lane_cache_make_time_sec_to_time_sql_typed_durations_and_warnings() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_time_constructors (id INT PRIMARY KEY, h BIGINT, m BIGINT, \
         s DECIMAL(6,3), n DECIMAL(12,2), r DOUBLE, u BIGINT UNSIGNED, i BIGINT, \
         t VARCHAR(16), b VARBINARY(1))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_time_constructors VALUES \
         (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'abc',x'FF'),\
         (2,1,2,3.456,1.25,86401.54321,18446744073709551615,0,'123x',NULL),\
         (3,1000,1,1.000,3864000.00,-3864000,NULL,NULL,NULL,NULL),\
         (4,12,60,0.000,NULL,NULL,NULL,NULL,NULL,NULL),\
         (5,12,15,60.000,NULL,NULL,NULL,NULL,NULL,NULL)",
        )
        .unwrap();
    // MAKETIME's total-seconds call must release the one slot before its
    // independent FSP/formatter call; all outputs still pass the old TIME cast.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT MAKETIME(h,m,s),SEC_TO_TIME(n),SEC_TO_TIME(r) \
         FROM shared_time_constructors ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected typed duration constructors")
    };
    assert_eq!(columns.len(), 3);
    for ((_, field), (flen, fsp)) in columns.iter().zip([(14, 3), (13, 2), (17, 6)]) {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::Duration);
        assert_eq!((field.flen(), field.decimal()), (flen, fsp));
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let expected = [
        ["NULL", "NULL", "NULL"],
        ["01:02:03.456", "00:00:01.25", "24:00:01.543210"],
        ["838:59:59.000", "838:59:59.00", "-838:59:59.000000"],
        ["NULL", "NULL", "NULL"],
        ["NULL", "NULL", "NULL"],
    ];
    assert_eq!(rows.len(), expected.len());
    for (row, expected) in rows.iter().zip(expected) {
        assert_eq!(row.len(), 3);
        for (value, expected) in row.iter().zip(expected) {
            assert!(matches!(value, Datum::Null | Datum::Duration(_)));
            assert_eq!(cell_text(value), expected);
        }
    }
    assert!(warnings_of(&session).is_empty());
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT MAKETIME(u,i,i) FROM shared_time_constructors WHERE id=2")
        .unwrap()
    else {
        panic!("expected unsigned-hour clamp")
    };
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Duration);
    assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (10, 0));
    assert!(matches!(&rows[0][0], Datum::Duration(_)));
    assert_eq!(cell_text(&rows[0][0]), "838:59:59");
    assert!(warnings_of(&session).is_empty());
    // Original number_arg parses the whole string: a numeric prefix plus junk
    // warns and becomes zero, while invalid UTF-8 silently becomes zero.
    for (expression, id, expected, warning) in [
        (
            "MAKETIME(h,m,t)",
            1,
            "NULL",
            Some("Truncated incorrect DOUBLE value: 'abc'"),
        ),
        (
            "SEC_TO_TIME(t)",
            2,
            "00:00:00.000000",
            Some("Truncated incorrect DOUBLE value: '123x'"),
        ),
        ("SEC_TO_TIME(b)", 1, "00:00:00.000000", None),
    ] {
        let sql = format!("SELECT {expression} FROM shared_time_constructors WHERE id={id}");
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected duration coercion row: {sql}")
        };
        assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Duration);
        assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (17, 6));
        assert!(matches!(&rows[0][0], Datum::Null | Datum::Duration(_)));
        assert_eq!(cell_text(&rows[0][0]), expected, "{sql}");
        if let Some(message) = warning {
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1292,
                    message: message.to_owned(),
                }],
                "{sql}"
            );
        } else {
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}









#[test]
fn ready_value_uuid_translate_uuid_values_metadata_and_diagnostics() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_uuid_values (id INT PRIMARY KEY, u VARCHAR(64), b VARBINARY(16), f VARCHAR(8))").unwrap();
    session
        .run(
            "INSERT INTO shared_uuid_values VALUES (1,NULL,NULL,'a'),\
         (2,'5f13f854-d74a-11f0-9b7a-0ae0156bd76b',NULL,NULL),\
         (3,'1f0e48c1-7860-69cc-9b3f-35f89c103d4d',NULL,NULL),\
         (4,'019b1440-87b7-7380-ab00-ce413e795004',NULL,NULL),\
         (5,'a3e3b4a1-ea6d-471e-9860-8303a8b261f6',NULL,NULL),\
         (6,'6ccd780cbaba102695645b8c656024db',x'6CCD780CBABA102695645B8C656024DB','0'),\
         (7,'{99a9ad03-5298-11ec-8f5c-00ff90147ac3*',NULL,NULL),\
         (8,'urn:uuid:99a9ad03-5298-11ec-8f5c-00ff90147ac3',NULL,NULL),\
         (9,'abc','1','a'),(10,' 6ccd780c-baba-1026-9564-5b8c656024db',NULL,'a')",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = session.run_with_columns(
        "SELECT IS_UUID(u),UUID_VERSION(u),UUID_TIMESTAMP(u) FROM shared_uuid_values WHERE id<=6 ORDER BY id",
    ).unwrap() else { panic!("expected UUID scalar rows") };
    assert_eq!(columns.len(), 3);
    for ((_, field), (code, flen, scale)) in columns.iter().zip([
        (tidb_datatype::FieldTypeCode::LongLong, 1, 0),
        (tidb_datatype::FieldTypeCode::LongLong, 10, 0),
        (tidb_datatype::FieldTypeCode::NewDecimal, 18, 6),
    ]) {
        assert_eq!(field.code(), code);
        assert_eq!((field.flen(), field.decimal()), (flen, scale));
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    // Source-owned TestUUIDTimestamp decimals, including the pre-1970 v1.
    let expected = [
        ["NULL", "NULL", "NULL"],
        ["1", "1", "1765537487.118139"],
        ["1", "6", "1766995078.970004"],
        ["1", "7", "1765571332.023000"],
        ["1", "4", "NULL"],
        ["1", "1", "-11129156903.290674"],
    ];
    assert_eq!(rows.len(), expected.len());
    for (row, expected) in rows.iter().zip(expected) {
        assert_eq!(row.len(), 3);
        for (value, expected) in row.iter().zip(expected) {
            assert_eq!(cell_text(value), expected);
        }
        assert!(matches!(&row[2], Datum::Null | Datum::Decimal(_)));
    }
    assert!(warnings_of(&session).is_empty());
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT IS_UUID(u) FROM shared_uuid_values WHERE id>=7 ORDER BY id")
        .unwrap()
    else {
        panic!("expected UUID parse-shape rows")
    };
    assert_eq!(
        rows,
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(1)],
            vec![Datum::Int(0)],
            vec![Datum::Int(0)]
        ]
    );
    assert!(warnings_of(&session).is_empty());
    // Each UUID_TO_BIN row uses two complete sequential calls (probe + swap),
    // so these four projections cost six worker calls, not four. One slot must
    // suffice; do not manufacture an inverse-swap roundtrip as the oracle.
    let StmtOutput::Rows { columns, rows } = session.run_with_columns(
        "SELECT UUID_TO_BIN(u,f),UUID_TO_BIN(u,1),BIN_TO_UUID(b,f),BIN_TO_UUID(b,1) FROM shared_uuid_values WHERE id=6",
    ).unwrap() else { panic!("expected raw UUID conversions") };
    assert_eq!(columns.len(), 4);
    for (index, (_, field)) in columns.iter().enumerate() {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
        assert_eq!(
            (field.flen(), field.decimal()),
            (if index < 2 { 16 } else { 32 }, 0)
        );
        assert_eq!(
            field.charset_name(),
            if index < 2 { "binary" } else { "utf8mb4" }
        );
        assert_eq!(
            field.collation(),
            if index < 2 {
                tidb_datatype::Collation::Binary
            } else {
                tidb_datatype::Collation::Utf8Mb4Bin
            }
        );
    }
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 4);
    assert_eq!(
        rows[0][0].to_bytes().unwrap(),
        vec![
            0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60,
            0x24, 0xdb
        ]
    );
    assert_eq!(
        rows[0][1].to_bytes().unwrap(),
        vec![
            0x10, 0x26, 0xba, 0xba, 0x6c, 0xcd, 0x78, 0x0c, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60,
            0x24, 0xdb
        ]
    );
    assert_eq!(
        cell_text(&rows[0][2]),
        "6ccd780c-baba-1026-9564-5b8c656024db"
    );
    assert_eq!(
        cell_text(&rows[0][3]),
        "baba1026-780c-6ccd-9564-5b8c656024db"
    );
    assert!(warnings_of(&session).is_empty());
    let flag_warning = SqlWarning {
        level: WarningLevel::Warning,
        code: 1292,
        message: "Truncated incorrect INTEGER value: 'a'".to_owned(),
    };
    for (expression, warned) in [("UUID_TO_BIN(u,f)", false), ("BIN_TO_UUID(b,f)", true)] {
        let StmtOutput::Rows { rows, .. } = session
            .run_with_columns(&format!(
                "SELECT {expression} FROM shared_uuid_values WHERE id=1",
            ))
            .unwrap()
        else {
            panic!("expected nullable UUID conversion")
        };
        assert_eq!(rows, vec![vec![Datum::Null]]);
        if warned {
            assert_eq!(session.warnings(), std::slice::from_ref(&flag_warning));
        } else {
            assert!(warnings_of(&session).is_empty());
        }
    }
    for (expression, id, reason) in [
        ("UUID_VERSION(u)", 9, "invalid UUID for UUID_VERSION"),
        ("UUID_TIMESTAMP(u)", 9, "invalid UUID for UUID_TIMESTAMP"),
        ("UUID_TO_BIN(u,f)", 9, "invalid UUID for UUID_TO_BIN"),
        ("UUID_TO_BIN(u,f)", 10, "invalid UUID_TO_BIN whitespace"),
    ] {
        let sql = format!("SELECT {expression} FROM shared_uuid_values WHERE id={id}");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        assert!(
            matches!(&error, DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Unsupported(message))) if *message == reason),
            "{sql}: {error:?}"
        );
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105);
        assert_eq!(mysql.state, *b"HY000");
        assert!(mysql.is_from_evaluation());
        assert!(warnings_of(&session).is_empty(), "{sql}");
    }
    let error = session
        .run_with_columns("SELECT BIN_TO_UUID(b,f) FROM shared_uuid_values WHERE id=9")
        .expect_err("bad binary UUID length");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1411);
    assert_eq!(
        mysql.message,
        "Incorrect string value: '1' for function bin_to_uuid"
    );
    assert!(mysql.is_from_evaluation());
    assert_eq!(session.warnings(), std::slice::from_ref(&flag_warning));
}

#[test]
fn ready_value_uuid_translate_byte_rune_results_and_null_demand() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run(
        "CREATE TABLE shared_translate_values (id INT PRIMARY KEY, s VARCHAR(16), f VARCHAR(16), \
         t VARCHAR(16), bs VARBINARY(16), bf VARBINARY(16), bt VARBINARY(16), l VARCHAR(8) CHARSET latin1)",
    ).unwrap();
    session
        .run(
            "INSERT INTO shared_translate_values VALUES \
         (1,'中文','中','ab',x'E4B8AD',x'E4B8AD','x',0xFF),\
         (2,'aaa','aa','xy','aaa','aa','xy',0xFF),\
         (3,'hello','lo','L','hello','lo','L',0xFF),\
         (4,NULL,'a','b',NULL,x'FF',x'FF',0xFF)",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = session.run_with_columns(
        "SELECT TRANSLATE(s,f,t),TRANSLATE(s,bf,t),TRANSLATE(bs,f,bt) FROM shared_translate_values ORDER BY id",
    ).unwrap() else { panic!("expected byte/rune TRANSLATE rows") };
    assert_eq!(columns.len(), 3);
    for (index, (_, field)) in columns.iter().enumerate() {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
        assert_eq!((field.flen(), field.decimal()), (16, -1));
        assert_eq!(
            field.charset_name(),
            if index == 2 { "binary" } else { "utf8mb4" }
        );
        assert_eq!(
            field.collation(),
            if index == 2 {
                tidb_datatype::Collation::Binary
            } else {
                tidb_datatype::Collation::Utf8Mb4Bin
            }
        );
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let expected = [
        ["a文", "ab文", "x"],
        ["xxx", "xxx", "xxx"],
        ["heLL", "heLL", "heLL"],
    ];
    assert_eq!(rows.len(), 4);
    for (row, expected) in rows[..3].iter().zip(expected) {
        assert_eq!(row.len(), 3);
        for (value, expected) in row.iter().zip(expected) {
            assert_eq!(value.to_bytes().unwrap(), expected.as_bytes());
        }
    }
    assert_eq!(rows[3], vec![Datum::Null; 3]);
    assert!(warnings_of(&session).is_empty());
    // Existing latin1 storage keeps FF without turning it into a binary-string
    // argument. This witnesses the original UTF-8 coercion's lazy demand.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT l,TRANSLATE(s,l,t) FROM shared_translate_values WHERE id=4")
        .unwrap()
    else {
        panic!("expected undemanded invalid UTF-8 layout")
    };
    assert_eq!(columns[0].1.charset_name(), "latin1");
    assert_eq!(rows[0][0].to_bytes().unwrap(), vec![0xff]);
    assert_eq!(rows[0][1], Datum::Null);
    assert!(warnings_of(&session).is_empty());
    let error = session
        .run_with_columns("SELECT TRANSLATE(s,l,t) FROM shared_translate_values WHERE id=2")
        .expect_err("non-NULL source demands the bad UTF-8 argument");
    assert!(matches!(
        &error,
        DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Unsupported("invalid UTF-8 string datum")
        ))
    ));
    assert_eq!(error.to_mysql_error().code, 1105);
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_crypt_hash_format_stream_direction_values_and_metadata() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_crypt_stream (id INT PRIMARY KEY, d VARCHAR(32), c VARBINARY(32), p VARBINARY(32))").unwrap();
    session
        .run(
            "INSERT INTO shared_crypt_stream VALUES (1,NULL,NULL,x'FF'),\
         (2,'pingcap',x'2C35B5A4ADF391','1234567890123456'),(3,'',x'',''),\
         (4,'pingcap',x'CE5C02A5010010','密匙'),(5,'data',x'0001',NULL)",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT ENCODE(c,p),DECODE(d,p) FROM shared_crypt_stream ORDER BY id")
        .unwrap()
    else {
        panic!("expected original stream-cipher directions")
    };
    assert_eq!(columns.len(), 2);
    for (_, field) in &columns {
        assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
        assert_eq!((field.flen(), field.decimal()), (32, -1));
        assert_eq!(field.charset_name(), "utf8mb4");
        assert_eq!(field.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let raw_text = |bytes: &[u8]| {
        Datum::new_collation_string(bytes.to_vec(), tidb_datatype::Collation::Utf8Mb4Bin)
    };
    // Old TestSQLDecode supplies the ciphertext; ENCODE is the inverse here.
    // Binary input/output bytes do not change the connection-charset tag.
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 2],
            vec![
                raw_text(b"pingcap"),
                raw_text(&[0x2c, 0x35, 0xb5, 0xa4, 0xad, 0xf3, 0x91])
            ],
            vec![raw_text(b""), raw_text(b"")],
            vec![
                raw_text(b"pingcap"),
                raw_text(&[0xce, 0x5c, 0x02, 0xa5, 0x01, 0x00, 0x10])
            ],
            vec![Datum::Null; 2],
        ]
    );
    // Passwords are raw bytes, so FF is not an invented UTF-8 error witness.
    // Byte preparation selects the worker before capability/guard/admission.
    assert!(warnings_of(&session).is_empty());
}

#[test]
fn ready_value_crypt_hash_format_numeric_values_metadata_and_coercion() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_hash_format (id INT PRIMARY KEY, h BIGINT UNSIGNED, b DOUBLE, \
         n DOUBLE, s VARCHAR(32), v VECTOR(3))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_hash_format VALUES (1,NULL,NULL,NULL,NULL,NULL),\
         (2,1,2048,2000,'abc',NULL),(3,0,75295729,898787877,'1e9999',NULL),\
         (4,18446744073709551615,-18446644073709551615.0,-9999999991,'18446744073709551615',NULL),\
         (5,30375298039,287952852482075252752429875.0,4827524825702572425242552.0,NULL,NULL),\
         (6,NULL,NULL,NULL,'-0.0',NULL),(7,NULL,NULL,NULL,NULL,'[1,2,3]'),\
         (8,NULL,1023,999,NULL,NULL),(9,NULL,1024,1000,NULL,NULL)",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT TIDB_SHARD(h),VITESS_HASH(h),FORMAT_BYTES(b),FORMAT_NANO_TIME(n) \
         FROM shared_hash_format WHERE id<=5 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected hash and scaled-format rows")
    };
    assert_eq!(columns.len(), 4);
    for (index, (_, field)) in columns.iter().enumerate() {
        if index < 2 {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert_eq!(
                (field.flen(), field.decimal()),
                (if index == 0 { 4 } else { 20 }, 0)
            );
            assert!(field.is_unsigned());
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation(), tidb_datatype::Collation::Binary);
        } else {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::VarString);
            assert_eq!((field.flen(), field.decimal()), (-1, -1));
            assert!(!field.is_unsigned());
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        }
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    let text = |value: &str| {
        Datum::new_collation_string(
            value.as_bytes().to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )
    };
    // The shard 51 is hand-derived from the low byte of the old frozen Vitess
    // vector 031265661E5F1133, not recorded from the new worker/provider.
    assert_eq!(
        rows,
        vec![
            vec![Datum::Null; 4],
            vec![
                Datum::UInt(214),
                Datum::UInt(1_615_456_034_434_468_822),
                text("2.00 KiB"),
                text("2.00 us")
            ],
            vec![
                Datum::UInt(167),
                Datum::UInt(10_134_873_677_816_210_343),
                text("71.81 MiB"),
                text("898.79 ms")
            ],
            vec![
                Datum::UInt(81),
                Datum::UInt(3_843_066_582_818_235_473),
                text("-16.00 EiB"),
                text("-10.00 s")
            ],
            vec![
                Datum::UInt(51),
                Datum::UInt(221_350_820_965_191_987),
                text("2.50e+08 EiB"),
                text("5.59e+10 d")
            ],
        ]
    );
    assert!(warnings_of(&session).is_empty());
    // Hand-derived boundary literals from the old >=1024 / >=1000 scale
    // selection, not a newly recorded formatter oracle.
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT FORMAT_BYTES(b),FORMAT_NANO_TIME(n) FROM shared_hash_format WHERE id>=8 ORDER BY id",
    ).unwrap() else { panic!("expected base-unit and first-scale edges") };
    assert_eq!(
        rows,
        vec![
            vec![text("1023 bytes"), text("999 ns")],
            vec![text("1.00 KiB"), text("1.00 us")],
        ]
    );
    assert!(warnings_of(&session).is_empty());
    // Source-derived combination: a textual u64::MAX takes the old signed
    // complement cast, so it reuses the fixed UInt(u64::MAX) hash above.
    for (expression, id, expected, code, message) in [
        (
            "TIDB_SHARD(s),VITESS_HASH(s)",
            2,
            ["167", "10134873677816210343"],
            1292,
            "Truncated incorrect INTEGER value: 'abc'",
        ),
        (
            "TIDB_SHARD(s),VITESS_HASH(s)",
            4,
            ["81", "3843066582818235473"],
            8030,
            "Cast to signed converted positive out-of-range integer to its negative complement",
        ),
        (
            "FORMAT_BYTES(s),FORMAT_NANO_TIME(s)",
            2,
            ["0 bytes", "0 ns"],
            1292,
            "Truncated incorrect DOUBLE value: 'abc'",
        ),
        (
            "FORMAT_BYTES(s),FORMAT_NANO_TIME(s)",
            3,
            ["1.56e+290 EiB", "2.08e+294 d"],
            1292,
            "Truncated incorrect DOUBLE value: '1e9999'",
        ),
    ] {
        let sql = format!("SELECT {expression} FROM shared_hash_format WHERE id={id}");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected source coercion row: {sql}")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 2);
        for (value, expected) in rows[0].iter().zip(expected) {
            assert_eq!(cell_text(value), expected, "{sql}");
        }
        assert_eq!(session.warnings().len(), 2, "{sql}");
        for warning in session.warnings() {
            assert_eq!(
                warning,
                &SqlWarning {
                    level: WarningLevel::Warning,
                    code,
                    message: message.to_owned()
                },
                "{sql}"
            );
        }
    }
    // A stored string plus the original ETReal cast witnesses the negative-zero
    // input, rather than assuming an SQL numeric literal retained its sign.
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT CAST(s AS DOUBLE),FORMAT_BYTES(s),FORMAT_NANO_TIME(s) FROM shared_hash_format WHERE id=6",
    ).unwrap() else { panic!("expected negative-zero formatting") };
    let Datum::Real(value) = &rows[0][0] else {
        panic!("expected ETReal witness")
    };
    assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(rows[0][1], text("0 bytes"));
    assert_eq!(rows[0][2], text("0 ns"));
    assert!(warnings_of(&session).is_empty());
    for expression in ["FORMAT_BYTES(v)", "FORMAT_NANO_TIME(v)"] {
        let sql = format!("SELECT {expression} FROM shared_hash_format WHERE id=7");
        let error = session
            .run_with_columns(&sql)
            .expect_err("a vector has no ETReal reading");
        assert!(matches!(
            &error,
            DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::Unsupported("numeric argument conversion")
            ))
        ));
        assert_eq!(error.to_mysql_error().code, 1105);
        assert!(warnings_of(&session).is_empty());
    }
}



#[test]
fn ready_value_vector_stored_values_metadata_and_native_formatting() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_vector_values (id INT PRIMARY KEY, a VECTOR, b VECTOR, t VARCHAR(64))").unwrap();
    session
        .run(
            "INSERT INTO shared_vector_values VALUES (1,NULL,NULL,NULL),\
         (2,'[3,4]','[0,0]','[1.1,2.2]'),(3,'[1,0]','[-1,0]','[]'),\
         (4,'[-0,0]','[1,0]','[-0,1e-8,1e10]'),(5,'[3e38]','[-3e38]',NULL)",
        )
        .unwrap();
    let StmtOutput::Rows { columns, rows } = session.run_with_columns(
        "SELECT VEC_AS_TEXT(a),VEC_FROM_TEXT(t),VEC_DIMS(a),VEC_L1_DISTANCE(a,b),\
         VEC_L2_DISTANCE(a,b),VEC_NEGATIVE_INNER_PRODUCT(a,b),VEC_COSINE_DISTANCE(a,b),VEC_L2_NORM(a) \
         FROM shared_vector_values WHERE id<=4 ORDER BY id",
    ).unwrap() else { panic!("expected typed vector SQL rows") };
    assert_eq!(columns.len(), 8);
    for (index, (_, field)) in columns.iter().enumerate() {
        let (code, flen, scale) = match index {
            0 => (tidb_datatype::FieldTypeCode::VarString, -1, -1),
            1 => (tidb_datatype::FieldTypeCode::VectorFloat32, -1, -1),
            2 => (tidb_datatype::FieldTypeCode::LongLong, 20, 0),
            _ => (tidb_datatype::FieldTypeCode::Double, -1, -1),
        };
        assert_eq!(field.code(), code);
        assert_eq!((field.flen(), field.decimal()), (flen, scale));
        assert_eq!(
            field.charset_name(),
            if index == 0 { "utf8mb4" } else { "binary" }
        );
        assert_eq!(
            field.collation(),
            if index == 0 {
                tidb_datatype::Collation::Utf8Mb4Bin
            } else {
                tidb_datatype::Collation::Binary
            }
        );
        assert!(!field.is_unsigned());
        assert!(!field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN));
    }
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec![Datum::Null; 8]);
    // The old [3,4] distance/norm fixture anchors 5. The other elementary
    // metrics and signed-zero combinations below are explicitly hand-derived
    // from the original sequential loops, not a recorded provider oracle.
    for (row, (text, elements, metrics)) in rows[1..].iter().zip([
        (
            "[3,4]",
            vec![1.1_f32, 2.2],
            [Some(7.0_f64), Some(5.0), Some(-0.0), None, Some(5.0)],
        ),
        (
            "[1,0]",
            vec![],
            [Some(2.0), Some(2.0), Some(1.0), Some(2.0), Some(1.0)],
        ),
        (
            "[-0,0]",
            vec![-0.0, 1e-8, 1e10],
            [Some(1.0), Some(1.0), Some(-0.0), None, Some(0.0)],
        ),
    ]) {
        assert_eq!(row.len(), 8);
        assert_eq!(
            row[0],
            Datum::new_collation_string(
                text.as_bytes().to_vec(),
                tidb_datatype::Collation::Utf8Mb4Bin
            )
        );
        let Datum::VectorFloat32(value) = &row[1] else {
            panic!("FROM_TEXT must return a real vector datum")
        };
        assert_eq!(value.elements(), elements.as_slice());
        assert_eq!(row[2], Datum::Int(2));
        for (actual, expected) in row[3..].iter().zip(metrics) {
            match (actual, expected) {
                (Datum::Null, None) => {}
                (Datum::Real(actual), Some(expected)) => {
                    assert_eq!(actual.to_bits(), expected.to_bits())
                }
                other => panic!("wrong typed vector metric: {other:?}"),
            }
        }
    }
    let Datum::VectorFloat32(value) = &rows[1][1] else {
        unreachable!()
    };
    // Exact old test_vector_endianess bytes, not parser-generated expected data.
    assert_eq!(
        value.serialize(),
        [2, 0, 0, 0, 0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0x0c, 0x40]
    );
    let Datum::VectorFloat32(value) = &rows[3][1] else {
        unreachable!()
    };
    assert_eq!(value.elements()[0].to_bits(), (-0.0_f32).to_bits());
    assert!(warnings_of(&session).is_empty());
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT VEC_AS_TEXT(VEC_FROM_TEXT(t)) FROM shared_vector_values WHERE id=4",
        )
        .unwrap()
    else {
        panic!("expected fixed native vector text")
    };
    // Hand-derived fixed-shortest spellings: no JSON/scientific reformatter.
    assert_eq!(cell_text(&rows[0][0]), "[-0,0.00000001,10000000000]");
    assert!(warnings_of(&session).is_empty());
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT VEC_L1_DISTANCE(a,b),VEC_L2_DISTANCE(a,b),VEC_NEGATIVE_INNER_PRODUCT(a,b),\
         VEC_COSINE_DISTANCE(a,b),VEC_L2_NORM(a) FROM shared_vector_values WHERE id=5",
        )
        .unwrap()
    else {
        panic!("expected finite-input overflow boundary")
    };
    // Hand-derived from accepted finite f32 inputs: three f32 loops overflow,
    // cosine computes NaN and becomes NULL, but native norm accumulates in f64.
    // No unsupported NaN/Inf SQL input is manufactured.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 5);
    for value in &rows[0][..3] {
        let Datum::Real(value) = value else {
            panic!("infinity must remain a real result")
        };
        assert!(value.is_infinite() && value.is_sign_positive());
    }
    assert_eq!(rows[0][3], Datum::Null);
    let Datum::Real(norm) = &rows[0][4] else {
        panic!("norm must remain a real result")
    };
    assert!(norm.is_finite() && *norm > 1e38);
    assert!(warnings_of(&session).is_empty());
}

#[test]
fn ready_value_vector_sql_errors_preserve_left_first_demand() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_vector_errors (id INT PRIMARY KEY, l VARCHAR(64), r VARCHAR(64), b VARBINARY(64))").unwrap();
    session
        .run(
            "INSERT INTO shared_vector_errors VALUES (1,NULL,'abc',x'FF'),\
         (2,'abc',NULL,'abc'),(3,'[1]','[1,2]','[-1e39,1e39]')",
        )
        .unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT VEC_L1_DISTANCE(l,r),VEC_L2_DISTANCE(l,r),VEC_NEGATIVE_INNER_PRODUCT(l,r),\
         VEC_COSINE_DISTANCE(l,r) FROM shared_vector_errors WHERE id=1",
        )
        .unwrap()
    else {
        panic!("left NULL must not coerce the malformed right vector")
    };
    assert_eq!(rows, vec![vec![Datum::Null; 4]]);
    assert!(warnings_of(&session).is_empty());
    for (expression, id, message) in [
        (
            "VEC_FROM_TEXT(b)",
            1,
            "invalid utf-8 sequence of 1 bytes from index 0",
        ),
        ("VEC_FROM_TEXT(b)", 2, "Invalid vector text: abc"),
        (
            "VEC_FROM_TEXT(b)",
            3,
            "value -1e+39 out of range for float32",
        ),
        (
            "VEC_L1_DISTANCE(l,r)",
            3,
            "vectors have different dimensions: 1 and 2",
        ),
        (
            "VEC_L2_DISTANCE(l,r)",
            3,
            "vectors have different dimensions: 1 and 2",
        ),
        (
            "VEC_NEGATIVE_INNER_PRODUCT(l,r)",
            3,
            "vectors have different dimensions: 1 and 2",
        ),
        (
            "VEC_COSINE_DISTANCE(l,r)",
            3,
            "vectors have different dimensions: 1 and 2",
        ),
        // A right NULL must not suppress the original left conversion error.
        ("VEC_L1_DISTANCE(l,r)", 2, "Invalid vector text: abc"),
    ] {
        let sql = format!("SELECT {expression} FROM shared_vector_errors WHERE id={id}");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        assert!(
            matches!(&error, DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Vector(actual))) if actual == message),
            "{sql}: {error:?}"
        );
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert_eq!(mysql.message, message, "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        assert!(warnings_of(&session).is_empty(), "{sql}");
    }
}



#[test]
fn ready_value_regexp_sql_values_metadata_and_demand_order() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_regexp_sql (id INT PRIMARY KEY, s VARCHAR(32), p VARCHAR(32), \
         repl VARCHAR(8), pos INT, occ INT, flags VARCHAR(8))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_regexp_sql VALUES (1,'abc abd','ab.','X',1,2,''),\
         (2,'你好啊','好','的',2,1,''),(3,'seafood fool','foo(.?)','z\\\\12',3,0,'')",
        )
        .unwrap();
    // Literal and per-row column patterns share one pool, not one regex cache.
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT REGEXP_LIKE(s,'^a'),REGEXP_LIKE(s,p,flags),\
         REGEXP_SUBSTR(s,'ab.',1,2),REGEXP_SUBSTR(s,p,pos,occ,flags),\
         REGEXP_INSTR(s,'好',2),REGEXP_INSTR(s,p,pos,occ,0,flags),\
         REGEXP_REPLACE(s,'ab.','X'),REGEXP_REPLACE(s,p,repl,pos,occ,flags) \
         FROM shared_regexp_sql ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected named regexp SQL rows")
    };
    assert_eq!(columns.len(), 8);
    for (index, (_, field)) in columns.iter().enumerate() {
        let is_like = index < 2;
        let is_int = is_like || matches!(index, 4 | 5);
        assert_eq!(
            field.code(),
            if is_int {
                tidb_datatype::FieldTypeCode::LongLong
            } else {
                tidb_datatype::FieldTypeCode::VarString
            }
        );
        assert_eq!(
            field.flen(),
            if is_like {
                1
            } else if is_int {
                20
            } else {
                32
            }
        );
        assert_eq!(field.decimal(), if is_int { 0 } else { -1 });
        // Original collation derivation stamps the string arguments' collation
        // on every regex result FieldType, including LIKE/INSTR's integer types.
        assert_eq!(field.charset_name(), "utf8mb4");
        assert_eq!(field.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        assert!(!field.is_unsigned());
        assert_eq!(
            field.has_flag(tidb_datatype::FieldTypeFlags::IS_BOOLEAN),
            is_like
        );
    }
    // Original Unicode/occurrence and single-digit z\12 fixtures anchor values.
    // These cross-function rows (including food/INSTR=4 and the retained 啊)
    // are hand-derived combinations, not newly recorded worker output.
    let expected = [
        ["1", "1", "abd", "abd", "0", "5", "X X", "abc X"],
        ["0", "1", "NULL", "好", "2", "2", "你好啊", "你的啊"],
        [
            "0",
            "1",
            "NULL",
            "food",
            "0",
            "4",
            "seafood fool",
            "seazd2 zl2",
        ],
    ];
    assert_eq!(rows.len(), expected.len());
    for (row, expected) in rows.iter().zip(expected) {
        assert_eq!(row.len(), 8);
        for (value, expected) in row.iter().zip(expected) {
            assert_eq!(cell_text(value), expected);
        }
        for index in [0, 1, 4, 5] {
            assert!(matches!(&row[index], Datum::Int(_)));
        }
    }
    assert!(warnings_of(&session).is_empty());
    // Constant NULL flags are mixed with stored columns. In the three
    // positional functions the original flag-NULL exit precedes trimming.
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT REGEXP_LIKE(s,p,NULL),REGEXP_SUBSTR(s,p,0,1,NULL),\
         REGEXP_INSTR(s,p,0,1,0,NULL),REGEXP_REPLACE(s,p,repl,0,0,NULL) \
         FROM shared_regexp_sql WHERE id=1",
        )
        .unwrap()
    else {
        panic!("expected original late-NULL argument order")
    };
    assert_eq!(rows, vec![vec![Datum::Null; 4]]);
    assert!(warnings_of(&session).is_empty());
    // Combined-error cases are source-order assertions: position validation
    // precedes compilation, but INSTR's return_option precedes late flags.
    for (expression, message) in [
        ("REGEXP_LIKE(s,'(')", "invalid regular expression pattern"),
        ("REGEXP_SUBSTR(s,'')", "empty regular expression pattern"),
        ("REGEXP_REPLACE(s,p,repl,1,0,'p')", "Invalid match type"),
        (
            "REGEXP_INSTR(s,p,0,1,2,NULL)",
            "Incorrect arguments to regexp_instr: return_option must be 1 or 0",
        ),
        (
            "REGEXP_SUBSTR(s,'(',0)",
            "Index out of bounds in regular expression search",
        ),
        (
            "REGEXP_INSTR(s,'(',0,1,0)",
            "Index out of bounds in regular expression search",
        ),
        (
            "REGEXP_REPLACE(s,'(',repl,0)",
            "Index out of bounds in regular expression search",
        ),
    ] {
        let sql = format!("SELECT {expression} FROM shared_regexp_sql WHERE id=1");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        assert!(
            matches!(&error, DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Unsupported(actual))) if *actual == message),
            "{sql}: {error:?}"
        );
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert_eq!(mysql.message, message, "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        assert!(warnings_of(&session).is_empty(), "{sql}");
    }
    // A failed pattern must not poison a later expression's statement/cache.
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT REGEXP_LIKE(s,'ab.'),REGEXP_REPLACE(s,'ab.','X',1,2) FROM shared_regexp_sql WHERE id=1",
    ).unwrap() else { panic!("expected healthy pool after regexp errors") };
    assert_eq!(rows[0][0], Datum::Int(1));
    assert_eq!(cell_text(&rows[0][1]), "abc X");
    assert!(warnings_of(&session).is_empty());
}

#[test]
fn invalid_constant_regexp_is_compiled_only_in_a_demanded_control_branch() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT IF(1,7,REGEXP_LIKE('x','(')),\
             CASE WHEN 1 THEN 8 ELSE REGEXP_LIKE('x','(') END,\
             COALESCE(9,REGEXP_LIKE('x','(')),\
             IFNULL(10,REGEXP_LIKE('x','('))",
        )
        .unwrap()
    else {
        panic!("expected one strict-control row")
    };
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(7),
            Datum::Int(8),
            Datum::Int(9),
            Datum::Int(10)
        ]]
    );
    let error = session
        .run_with_columns("SELECT IF(0,7,REGEXP_LIKE('x','('))")
        .expect_err("demanded invalid constant pattern must fail");
    assert!(
        matches!(&error, DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Unsupported(message)
        )) if *message == "invalid regular expression pattern"),
        "{error:?}"
    );
}



#[test]
fn ready_value_like_ilike_sql_columns_cache_unicode_escape_and_nulls() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_like_sql (id INT PRIMARY KEY, s VARCHAR(16), p VARCHAR(16))")
        .unwrap();
    session.run("INSERT INTO shared_like_sql VALUES (1,'ABC','a%'),(2,'ü','Ü'),(3,'%','A%'),(4,NULL,'a%'),(5,'a',NULL)").unwrap();
    // Dynamic patterns and context-cached literal patterns use the same actual
    // statement pool. ILIKE only lowers ASCII; collated LIKE also folds ü/Ü.
    for _ in 0..2 {
        let StmtOutput::Rows { rows, .. } = session
            .run_with_columns(
                "SELECT s LIKE p,s ILIKE p,s NOT ILIKE p,s LIKE 'a%',s ILIKE 'a%',\
             s COLLATE utf8mb4_general_ci LIKE p FROM shared_like_sql ORDER BY id",
            )
            .unwrap()
        else {
            panic!("expected LIKE/ILIKE rows")
        };
        let expected = [
            ["0", "1", "0", "0", "1", "1"],
            ["0", "0", "1", "0", "0", "1"],
            ["0", "0", "1", "0", "0", "0"],
            ["NULL", "NULL", "NULL", "NULL", "NULL", "NULL"],
            ["NULL", "NULL", "NULL", "1", "1", "NULL"],
        ];
        assert_eq!(rows.len(), expected.len());
        for (row, expected) in rows.iter().zip(expected) {
            assert_eq!(row.len(), expected.len());
            for (value, expected) in row.iter().zip(expected) {
                assert_eq!(cell_text(value), expected);
            }
        }
        assert!(warnings_of(&session).is_empty());
    }
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT s LIKE p ESCAPE 'A',s ILIKE p ESCAPE 'A',s NOT ILIKE 'A%' ESCAPE 'A' \
         FROM shared_like_sql WHERE id=3",
        )
        .unwrap()
    else {
        panic!("expected alphabetic escape rows")
    };
    assert_eq!(
        rows,
        vec![vec![Datum::Int(1), Datum::Int(1), Datum::Int(0)]]
    );
    assert!(warnings_of(&session).is_empty());
    for sql in [
        "SHOW TABLES LIKE 'shared_like_sql'",
        "SHOW TABLES WHERE Tables_in_test LIKE 'shared_like_sql'",
    ] {
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(sql).unwrap() else {
            panic!("expected matching SHOW candidate")
        };
        assert_eq!(rows, vec![vec![Datum::Bytes(b"shared_like_sql".to_vec())]]);
        assert!(warnings_of(&session).is_empty());
    }
}





#[test]
fn shared_scalar_datum_preserves_json_float_and_hybrid_numeric_consumers() {
    let mut session = Session::new();
    session.run("CREATE TABLE shared_scalar_datum_sql (j JSON, e ENUM('x','word'), s SET('a','b','c'), b BIT(4))").unwrap();
    session
        .run("INSERT INTO shared_scalar_datum_sql VALUES ('2.5','word','a,c',b'0011')")
        .unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns(
                // Numeric JSON Display has no quotes. JSON-string conversion
                // distinctions are tested directly, not inferred from SQL lowering.
                "SELECT CAST(j AS DOUBLE),e+0e0,s+0e0,CAST(b AS DOUBLE) FROM shared_scalar_datum_sql",
            )
            .unwrap()
        else {
            panic!("expected scalar datum consumers")
        };
        assert_eq!(
            rows,
            vec![vec![
                Datum::Real(2.5),
                Datum::Real(2.0),
                Datum::Real(5.0),
                Datum::Real(3.0)
            ]]
        );
        assert_eq!(columns.len(), 4);
        for (_, field) in columns {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::Double);
        }
        assert!(warnings_of(&session).is_empty());
    }
}

#[test]
fn shared_decimal_context_preserves_temporal_arguments_and_fractional_projection() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE shared_decimal_context_sql (dt DATETIME(3), tm TIME(3), d DECIMAL(10,4))",
        )
        .unwrap();
    session.run("INSERT INTO shared_decimal_context_sql VALUES ('2024-02-03 04:05:06.125','11:22:33.125',1.0000)").unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(
            "SELECT dt+0.001,tm+0.001,CAST(d/3 AS DECIMAL(10,4)) FROM shared_decimal_context_sql"
        ).unwrap() else { panic!("expected decimal context rows") };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
        assert_eq!(columns.len(), 3);
        for (index, expected) in ["20240203040506.126", "112233.126", "0.3333"]
            .into_iter()
            .enumerate()
        {
            assert!(matches!(rows[0][index], Datum::Decimal(_)));
            assert_eq!(rows[0][index].sql_string().unwrap(), expected);
            assert_eq!(
                columns[index].1.code(),
                tidb_datatype::FieldTypeCode::NewDecimal
            );
        }
        // Division uses source scale4 plus the default increment4. The existing
        // DECIMAL CAST warns when narrowing changes that scale8 value.
        assert_eq!(
            warnings_of(&session),
            vec![(
                1292,
                "Truncated incorrect DECIMAL value: '0.33333333'".to_owned()
            )]
        );
    }
}

#[test]
fn shared_integer_argument_preserves_json_prefix_hybrid_and_unsigned_source_rules() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session.run("CREATE TABLE shared_arg_integer_sql (j JSON, e ENUM('other','word'), s SET('a','b','c'), u DOUBLE UNSIGNED)").unwrap();
    session
        .run("INSERT INTO shared_arg_integer_sql VALUES ('3.5','word','a,c',1.0)")
        .unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(
            "SELECT MAKE_SET(j,'a','b','c'),MAKE_SET(e,'a','b','c'),MAKE_SET(s,'a','b','c'),TRUNCATE(12.34,u) FROM shared_arg_integer_sql"
        ).unwrap() else { panic!("expected integer argument rows") };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 4);
        assert_eq!(rows[0][0], Datum::new_string("a,b"));
        assert_eq!(rows[0][1], Datum::new_string("b"));
        assert_eq!(rows[0][2], Datum::new_string("a,c"));
        assert_eq!(rows[0][3].sql_string().unwrap(), "12.3");
        assert_eq!(columns.len(), 4);
        assert_eq!(
            columns[3].1.code(),
            tidb_datatype::FieldTypeCode::NewDecimal
        );
        assert_eq!(
            warnings_of(&session),
            vec![(1292, "Truncated incorrect INTEGER value: '3.5'".to_owned())]
        );
    }
}

#[test]
fn shared_decimal_datum_preserves_unsigned_hybrid_bit_and_json_consumers() {
    use tidb_datatype::FieldTypeCode;
    let mut session = Session::new();
    session.run("CREATE TABLE shared_decimal_datum_sql (e ENUM('other','word'), s SET('a','b','c'), b BIT(64), j JSON)").unwrap();
    session.run("INSERT INTO shared_decimal_datum_sql VALUES ('word','a,c',x'ffffffffffffffff','18446744073709551615')").unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(
            "SELECT CAST(e AS UNSIGNED),CAST(s AS UNSIGNED),CAST(b AS UNSIGNED),CAST(j AS UNSIGNED) FROM shared_decimal_datum_sql"
        ).unwrap() else { panic!("expected unsigned decimal consumer rows") };
        assert_eq!(
            rows,
            vec![vec![
                Datum::UInt(2),
                Datum::UInt(5),
                Datum::UInt(u64::MAX),
                Datum::UInt(u64::MAX)
            ]]
        );
        assert_eq!(columns.len(), 4);
        for (_, field) in columns {
            assert_eq!(field.code(), FieldTypeCode::LongLong);
            assert!(field.is_unsigned());
            assert_eq!((field.flen(), field.decimal()), (20, 0));
        }
        assert!(warnings_of(&session).is_empty());
    }
}

#[test]
fn shared_argument_string_preserves_binary_names_float_display_and_widths() {
    let mut session = Session::new();
    session.run("CREATE TABLE shared_arg_string_sql (b BIT(8), e ENUM('other','word'), f DOUBLE, d DECIMAL(10,2), r VARBINARY(2))").unwrap();
    session
        .run("INSERT INTO shared_arg_string_sql VALUES (b'11111111','word',1e30,1.20,x'ff00')")
        .unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(
            "SELECT HEX(LTRIM(b)),QUOTE(e),CONCAT(f),CONCAT(d),HEX(CONCAT(r)),CONCAT(NULL) FROM shared_arg_string_sql"
        ).unwrap() else { panic!("expected argument string rows") };
        assert_eq!(
            rows,
            vec![vec![
                Datum::new_string("FF"),
                Datum::new_string("'word'"),
                Datum::new_string("1000000000000000000000000000000"),
                Datum::new_string("1.20"),
                Datum::new_string("FF00"),
                Datum::Null,
            ]]
        );
        assert_eq!(columns.len(), 6);
        assert_eq!(columns[2].1.flen(), 370);
        assert_eq!(columns[3].1.flen(), 13);
        assert!(warnings_of(&session).is_empty());
    }
}

#[test]
fn shared_datetime_controller_preserves_source_parsers_date_clock_and_diagnostics() {
    use tidb_datatype::{FieldTypeCode, TimeType};
    let mut session = Session::new();
    session
        .run("SET sql_mode='', time_zone='America/Los_Angeles'")
        .unwrap();
    session.run("CREATE TABLE shared_datetime_control_sql (s VARCHAR(64), n DECIMAL(12,4), u BIGINT UNSIGNED, bad VARCHAR(16), edge VARCHAR(64))").unwrap();
    session.run("INSERT INTO shared_datetime_control_sql VALUES ('2024-02-29 12:34:56.123456',121212.1111,18446744073709551615,'bad','2011-03-13 01:59:59.9999999')").unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(
            "SELECT CAST(s AS DATE),CAST(n AS DATETIME(3)),CAST(u AS DATETIME),CAST(bad AS DATETIME),CAST(edge AS DATETIME) FROM shared_datetime_control_sql"
        ).unwrap() else { panic!("expected datetime controller rows") };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 5);
        for (index, kind, fsp, text) in [
            (0, TimeType::Date, 0, "2024-02-29"),
            (1, TimeType::DateTime, 3, "2012-12-12 00:00:00.000"),
            (4, TimeType::DateTime, 0, "2011-03-13 03:00:00"),
        ] {
            let Datum::Time(time) = &rows[0][index] else {
                panic!("expected temporal value")
            };
            assert_eq!((time.kind(), time.fsp()), (kind, fsp));
            assert_eq!(time.to_string(), text);
        }
        assert_eq!(rows[0][2], Datum::Null);
        assert_eq!(rows[0][3], Datum::Null);
        assert_eq!(columns.len(), 5);
        for (index, (_, field)) in columns.iter().enumerate() {
            assert_eq!(
                field.code(),
                if index == 0 {
                    FieldTypeCode::Date
                } else {
                    FieldTypeCode::Datetime
                }
            );
            assert_eq!(field.decimal(), if index == 1 { 3 } else { 0 });
        }
        assert_eq!(
            warnings_of(&session),
            vec![
                (1292, "Incorrect time value: '-1'".to_owned()),
                (1292, "Incorrect datetime value: 'bad'".to_owned()),
            ]
        );
    }
}

#[test]
fn shared_year_controller_preserves_clock_date_prefix_and_unsigned_fallback() {
    use tidb_datatype::FieldTypeCode;
    let mut session = Session::new();
    session.run("SET sql_mode='', time_zone='+00:00'").unwrap();
    session.run("SET timestamp=1609459200").unwrap();
    session.run("CREATE TABLE shared_year_control_sql (d TIME(3), date_text VARCHAR(32), prefix_text VARCHAR(32), u BIGINT UNSIGNED)").unwrap();
    session.run("INSERT INTO shared_year_control_sql VALUES ('00:20:12.250','2024-01-02','42tail',18446744073709551615)").unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (zone, year) in [("+00:00", 2021), ("-01:00", 2020)] {
            session.run(&format!("SET time_zone='{zone}'")).unwrap();
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(
                "SELECT CAST(d AS YEAR),CAST(date_text AS YEAR),CAST(prefix_text AS YEAR),CAST(u AS YEAR),CAST(NULL AS YEAR) FROM shared_year_control_sql"
            ).unwrap() else { panic!("expected YEAR controller rows") };
            assert_eq!(
                rows,
                vec![vec![
                    Datum::Int(year),
                    Datum::Int(2024),
                    Datum::Int(42),
                    Datum::Int(-1),
                    Datum::Null
                ]]
            );
            assert_eq!(columns.len(), 5);
            for (_, field) in columns {
                assert_eq!(field.code(), FieldTypeCode::Year);
                assert_eq!((field.flen(), field.decimal()), (4, 0));
                assert!(!field.is_unsigned());
            }
            assert!(warnings_of(&session).is_empty());
        }
    }
}

#[test]
fn native_signed_datum_preserves_hybrid_ordinals_and_temporal_carry_in_sql() {
    use tidb_datatype::FieldTypeCode;
    let mut session = Session::new();
    session.run("SET sql_mode='' ").unwrap();
    session.run("SET time_zone='America/Los_Angeles'").unwrap();
    session.run("CREATE TABLE native_signed_datum_sql (e ENUM('other','word'), s SET('a','b','c'), t DATETIME(6), d TIME(6))").unwrap();
    session.run("INSERT INTO native_signed_datum_sql VALUES ('word','a,c','2011-03-13 01:59:59.999999','11:59:59.999999')").unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(
            "SELECT CAST(e AS YEAR),CAST(s AS YEAR),CAST(t AS SIGNED),CAST(d AS SIGNED) FROM native_signed_datum_sql"
        ).unwrap() else { panic!("expected signed conversion rows") };
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(2),
                Datum::Int(5),
                Datum::Int(20110313030000),
                Datum::Int(120000)
            ]]
        );
        assert_eq!(columns.len(), 4);
        for (index, (_, field)) in columns.iter().enumerate() {
            assert_eq!(
                field.code(),
                if index < 2 {
                    FieldTypeCode::Year
                } else {
                    FieldTypeCode::LongLong
                }
            );
            assert!(!field.is_unsigned());
            assert_eq!(field.decimal(), 0);
        }
        assert!(warnings_of(&session).is_empty());
    }
}

#[test]
fn native_temporal_calendar_preserves_statement_clock_year_fields_and_dst_boundaries() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags, TimeType};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session.run("SET timestamp=1609459200").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_temporal_calendar_sql (small_time TIME(3), negative_time TIME(3), y YEAR, zero_y YEAR, dst_edge DATETIME(6), gap_dt DATETIME)")
        .unwrap();
    session
        .run("INSERT INTO native_temporal_calendar_sql VALUES ('00:20:12.250','-01:00:00.125',2024,0,'2011-03-13 01:59:59.999999','2024-03-10 02:30:00')")
        .unwrap();
    session
        .run("CREATE TABLE native_temporal_gap_sql (ts TIMESTAMP)")
        .unwrap();
    // The actual SET timestamp API fixes 2021-01-01 00:00:00 UTC. YEAR uses
    // the normal false-concat mode; DATETIME's existing duration controller
    // uses now()'s fixed offset. Neither is a numeric rendering of 00:20:12.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        session.run("SET sql_mode=''").unwrap();
        for (zone, small_year, small_datetime, negative_datetime) in [
            (
                "+00:00",
                2021,
                "2021-01-01 00:20:12.250",
                "2020-12-31 22:59:59.875",
            ),
            (
                "-01:00",
                2020,
                "2020-12-31 00:20:12.250",
                "2020-12-30 22:59:59.875",
            ),
        ] {
            session.run(&format!("SET time_zone='{zone}'")).unwrap();
            let sql = "SELECT CAST(small_time AS YEAR),CAST(small_time AS DATETIME(3)),CAST(negative_time AS YEAR),CAST(negative_time AS DATETIME(3)) FROM native_temporal_calendar_sql";
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(sql).unwrap() else {
                panic!("expected statement-clock temporal rows")
            };
            assert_eq!(columns.len(), 4);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 4);
            for (index, year) in [(0, small_year), (2, 2020)] {
                let field = &columns[index].1;
                assert_eq!(field.code(), FieldTypeCode::Year);
                assert_eq!((field.flen(), field.decimal()), (4, 0));
                assert_eq!(
                    rows[0][index],
                    Datum::Int(year),
                    "{zone}/{vectorized}/{index}"
                );
            }
            for (index, expected) in [(1, small_datetime), (3, negative_datetime)] {
                let field = &columns[index].1;
                assert_eq!(field.code(), FieldTypeCode::Datetime);
                assert_eq!((field.flen(), field.decimal()), (23, 3));
                let Datum::Time(value) = &rows[0][index] else {
                    panic!("lost clock/calendar Time carrier: {zone}/{vectorized}/{index}")
                };
                assert_eq!(value.kind(), TimeType::DateTime);
                assert_eq!(value.fsp(), 3);
                assert_eq!(value.to_string(), expected, "{zone}/{vectorized}/{index}");
            }
            for (_, field) in &columns {
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
                assert!(field.has_flag(FieldTypeFlags::BINARY));
                assert!(!field.is_unsigned());
            }
            assert!(warnings_of(&session).is_empty(), "{zone}/{vectorized}");
        }
        session.run("SET time_zone='America/Los_Angeles'").unwrap();
        let sql = "SELECT CAST(y AS DATETIME),CAST(zero_y AS DATE),CAST(dst_edge AS DATETIME) FROM native_temporal_calendar_sql";
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(sql).unwrap() else {
            panic!("expected YEAR-field and DST-round rows")
        };
        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
        // ParseTimeFromYear injects only the year field; zero has DATE kind.
        // The target choices match those native kinds, without pretending an
        // early-return YEAR cast repairs target-kind mismatches. The third
        // value rounds across PST->PDT, re-resolving the zone at the new instant.
        for (index, (code, flen, kind, expected)) in [
            (
                FieldTypeCode::Datetime,
                19,
                TimeType::DateTime,
                "2024-00-00 00:00:00",
            ),
            (FieldTypeCode::Date, 10, TimeType::Date, "0000-00-00"),
            (
                FieldTypeCode::Datetime,
                19,
                TimeType::DateTime,
                "2011-03-13 03:00:00",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let field = &columns[index].1;
            assert_eq!(field.code(), code);
            assert_eq!((field.flen(), field.decimal()), (flen, 0));
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
            assert!(field.has_flag(FieldTypeFlags::BINARY));
            assert!(!field.is_unsigned());
            let Datum::Time(value) = &rows[0][index] else {
                panic!("lost YEAR/DST Time carrier: {vectorized}/{index}")
            };
            assert_eq!(value.kind(), kind);
            assert_eq!(value.fsp(), 0);
            assert_eq!(value.to_string(), expected, "{vectorized}/{index}");
        }
        assert!(warnings_of(&session).is_empty());
        // A typed DATETIME column, not a string parser, reaches convert_kind's
        // TIMESTAMP-gap adjustment. Strict INSERT exposes its diagnostic and
        // rejects the row. Do not invent a non-strict storage oracle: the
        // existing adjusted result resets to DATETIME/FSP0, and tablecodec's
        // UTC conversion depends on the native kind, not just column metadata.
        session.run("SET sql_mode='STRICT_ALL_TABLES'").unwrap();
        let error = session
            .run("INSERT INTO native_temporal_gap_sql SELECT gap_dt FROM native_temporal_calendar_sql")
            .expect_err("typed gap conversion must fail the strict write");
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1292);
        assert_eq!(mysql.state, *b"22007");
        assert_eq!(
            mysql.message,
            "Incorrect timestamp value: '2024-03-10 02:30:00' for column 'ts' at row 1"
        );
        assert_eq!(
            warnings_of(&session),
            vec![(
                1292,
                "Incorrect timestamp value: '2024-03-10 02:30:00' for column 'ts' at row 1"
                    .to_owned()
            )]
        );
        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("SELECT ts FROM native_temporal_gap_sql")
            .unwrap()
        else {
            panic!("expected empty gap-write table")
        };
        assert_eq!(columns.len(), 1);
        let field = &columns[0].1;
        assert_eq!(field.code(), FieldTypeCode::Timestamp);
        assert_eq!((field.flen(), field.decimal()), (19, 0));
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation_name(), "binary");
        assert!(field.has_flag(FieldTypeFlags::BINARY));
        assert!(!field.is_unsigned());
        assert!(
            rows.is_empty(),
            "strict write persisted a row: {vectorized}"
        );
        assert!(warnings_of(&session).is_empty());
    }
    // Eight SELECTs (22 cells, including two zero-row SELECTs) plus two strict
    // INSERT error probes. Normal SQL cannot toggle the duration YEAR concat
    // flag; true-concat/reorg and raw gap-kind details remain unit coverage.
    // YEAR/DATE controllers and zero-date diagnostics are unchanged, and no
    // whole-CAST/new-Head/zero-slot or synthetic-clock claim is made.
}

#[test]
fn native_duration_control_preserves_sql_source_rounding_overflow_and_storage() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_duration_control_sql (i BIGINT, u BIGINT UNSIGNED, r DOUBLE, d DECIMAL(12,3), s VARCHAR(32), overflow_i BIGINT, overflow_r DOUBLE, overflow_d DECIMAL(12,3), overflow_s VARCHAR(32), prefix_s VARCHAR(32), negative_tie TIME(6), negative_past TIME(6), negative_text VARCHAR(32), datetime_i BIGINT, datetime_s VARCHAR(32))")
        .unwrap();
    session
        .run("INSERT INTO native_duration_control_sql VALUES (125959,18446744073709551615,125959.125,125959.125,'125959.125',8390000,8390000,8390000.000,'8390000','1x','-00:00:00.001500','-00:00:00.001501','-00:00:00.001500',20240315020304,'240315020304')")
        .unwrap();
    session
        .run("CREATE TABLE native_duration_storage_sql (from_number TIME(3), from_text TIME(3), rounded TIME(3))")
        .unwrap();
    session
        .run(
            "INSERT INTO native_duration_storage_sql VALUES (8390000,'8390000','-00:00:00.001500')",
        )
        .unwrap();
    // The datatype write target retains its clamped value; non-strict DML
    // decorates the two overflow events as column-scoped 1264 warnings.
    assert_eq!(
        warnings_of(&session),
        vec![
            (
                1264,
                "Out of range value for column 'from_number' at row 1".to_owned()
            ),
            (
                1264,
                "Out of range value for column 'from_text' at row 1".to_owned()
            ),
        ]
    );
    type Cell = Option<(i64, &'static str)>;
    let cases: [(&str, &[Cell], &[(u16, &str)]); 4] = [
        (
            "SELECT CAST(i AS TIME(3)),CAST(u AS TIME(3)),CAST(r AS TIME(3)),CAST(d AS TIME(3)),CAST(s AS TIME(3)) FROM native_duration_control_sql",
            &[
                Some((46_799_000_000_000, "12:59:59.000")),
                Some((-1_000_000_000, "-00:00:01.000")),
                Some((46_799_125_000_000, "12:59:59.125")),
                Some((46_799_125_000_000, "12:59:59.125")),
                Some((46_799_125_000_000, "12:59:59.125")),
            ],
            &[],
        ),
        (
            "SELECT CAST(overflow_i AS TIME(3)),CAST(overflow_r AS TIME(3)),CAST(overflow_d AS TIME(3)),CAST(overflow_s AS TIME(3)),CAST(prefix_s AS TIME(3)) FROM native_duration_control_sql",
            &[
                None, None, None,
                Some((3_020_399_000_000_000, "838:59:59.000")),
                Some((1_000_000_000, "00:00:01.000")),
            ],
            &[
                (1292, "Truncated incorrect time value: '8390000'"),
                (1292, "Truncated incorrect time value: '8390000'"),
                (1292, "Truncated incorrect time value: '8390000.000'"),
                (1292, "Truncated incorrect time value: '8390000'"),
                (1292, "Truncated incorrect time value: '1x'"),
            ],
        ),
        (
            "SELECT CAST(negative_tie AS TIME(3)),CAST(negative_past AS TIME(3)),CAST(negative_text AS TIME(3)),CAST(datetime_i AS TIME(3)),CAST(datetime_s AS TIME(3)) FROM native_duration_control_sql",
            &[
                Some((-1_000_000, "-00:00:00.001")),
                Some((-2_000_000, "-00:00:00.002")),
                Some((-2_000_000, "-00:00:00.002")),
                Some((7_384_000_000_000, "02:03:04.000")),
                Some((7_384_000_000_000, "02:03:04.000")),
            ],
            &[],
        ),
        (
            "SELECT from_number,from_text,rounded FROM native_duration_storage_sql",
            &[
                Some((3_020_399_000_000_000, "838:59:59.000")),
                Some((3_020_399_000_000_000, "838:59:59.000")),
                Some((-2_000_000, "-00:00:00.002")),
            ],
            &[],
        ),
    ];
    // Integer TIME casts use the low-64-bit ETInt representation (UInt::MAX
    // becomes -1), not the datatype write target's numeric stringification.
    // Numeric cast errors discard the clamp as NULL; text keeps best effort.
    // Raw TIME rounding resolves a negative half toward positive infinity,
    // whereas text first rounds the positive fraction, then applies its sign.
    // Both the >=1e10 numeric datetime fallback and the 12-digit text-first
    // datetime route keep only the clock. These are fixed source-derived
    // nanoseconds/text literals, not outputs of a duration parser/constructor.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(sql, expected, warnings) in &cases {
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(sql).unwrap() else {
                panic!("expected duration control/storage rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected) in expected.iter().enumerate() {
                let field = &columns[index].1;
                assert_eq!(field.code(), FieldTypeCode::Duration, "{sql}/{index}");
                assert_eq!((field.flen(), field.decimal()), (14, 3), "{sql}/{index}");
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
                assert!(field.has_flag(FieldTypeFlags::BINARY));
                assert!(!field.is_unsigned());
                if let Some((nanoseconds, text)) = expected {
                    let Datum::Duration(value) = &rows[0][index] else {
                        panic!("lost duration carrier: {sql}/{vectorized}/{index}")
                    };
                    assert_eq!(
                        value.nanoseconds(),
                        *nanoseconds,
                        "{sql}/{vectorized}/{index}"
                    );
                    assert_eq!(value.fsp(), 3, "{sql}/{vectorized}/{index}");
                    assert_eq!(value.to_string(), *text, "{sql}/{vectorized}/{index}");
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert_eq!(
                warnings_of(&session),
                warnings
                    .iter()
                    .map(|&(code, message)| (code, message.to_owned()))
                    .collect::<Vec<_>>(),
                "{sql}/{vectorized}"
            );
        }
    }
    // Eight SELECTs: ordinary duration casts and the actual DML TIME target.
    // Argument/computed wrappers, strict-error upgrades and warning byte caps
    // retain separate unit coverage; no new Head/zero-slot claim is made.
}

#[test]
fn native_json_source_policy_preserves_year_unsigned_and_hybrid_name_modes() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_json_source_sql (y YEAR, u BIGINT UNSIGNED, e_one ENUM('1','true','bad'), e_true ENUM('1','true','bad'), e_bad ENUM('1','true','bad'), s_one SET('1','true'), s_true SET('1','true'), s_multi SET('1','true'), jnum JSON, jstr JSON, jbad JSON, jmulti JSON)")
        .unwrap();
    session
        .run(r#"INSERT INTO native_json_source_sql VALUES (2024,18446744073709551615,'1','true','bad','1','true','1,true','1','"1"','"bad"','"1,true"')"#)
        .unwrap();
    let expected_json: &[(u8, &[u8])] = &[
        (0x0a, &[0xe8, 7, 0, 0, 0, 0, 0, 0]),
        (0x0a, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        (0x09, &[1, 0, 0, 0, 0, 0, 0, 0]),
        (0x04, &[1]),
        (0x09, &[1, 0, 0, 0, 0, 0, 0, 0]),
        (0x04, &[1]),
    ];
    // The unchanged numeric entrypoint presents UInt::MAX as Int(-1); JSON
    // source preparation restores the unsigned carrier from the actual column
    // metadata. YEAR is likewise unsigned here, not a synthetic flag fixture.
    // ENUM/SET remain ETString: their names become Bytes, not ordinals/masks.
    // In particular, the name 'true' parses as JSON true, not integer 2.
    // Bytes alone cannot imply binary opaque data: these sources retain their
    // non-binary utf8mb4 field metadata at the typed expression boundary.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let sql = "SELECT CAST(y AS JSON),CAST(u AS JSON),CAST(e_one AS JSON),CAST(e_true AS JSON),CAST(s_one AS JSON),CAST(s_true AS JSON) FROM native_json_source_sql";
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(sql).unwrap() else {
            panic!("expected JSON source-policy cast rows")
        };
        assert_eq!(columns.len(), expected_json.len());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), expected_json.len());
        for (index, (tag, payload)) in expected_json.iter().enumerate() {
            let field = &columns[index].1;
            assert_eq!(field.code(), FieldTypeCode::Json);
            assert_eq!((field.flen(), field.decimal()), (4_194_304, 0));
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation_name(), "utf8mb4_bin");
            assert!(field.has_flag(FieldTypeFlags::BINARY));
            assert!(field.has_flag(FieldTypeFlags::PARSE_TO_JSON));
            assert!(!field.is_unsigned());
            let Datum::Json(value) = &rows[0][index] else {
                panic!("lost typed JSON source-policy result: {vectorized}/{index}")
            };
            assert_eq!(value.type_code(), *tag, "vectorized={vectorized}/{index}");
            assert_eq!(value.value(), *payload, "vectorized={vectorized}/{index}");
        }
        assert!(warnings_of(&session).is_empty());
        // Nonconstant two-item lists retain typed IN, whose JSON target has
        // ParseToJSONFlag cleared. The same hybrid names are now string values,
        // including names which would be invalid JSON documents on their own.
        for (projection, expected) in [
            (
                "jnum IN(e_one,e_one),jstr IN(e_one,e_one),jnum IN(s_one,s_one),jstr IN(s_one,s_one)",
                vec![Datum::Int(0), Datum::Int(1), Datum::Int(0), Datum::Int(1)],
            ),
            (
                "jbad IN(e_bad,e_bad),jmulti IN(s_multi,s_multi)",
                vec![Datum::Int(1), Datum::Int(1)],
            ),
        ] {
            let sql = format!("SELECT {projection} FROM native_json_source_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected hybrid JSON value-policy IN rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            for (_, field) in &columns {
                assert_eq!(field.code(), FieldTypeCode::LongLong);
                assert_eq!((field.flen(), field.decimal()), (1, 0));
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
                assert!(field.has_flag(FieldTypeFlags::IS_BOOLEAN));
                assert!(!field.is_unsigned());
            }
            assert_eq!(rows, vec![expected], "{sql}/{vectorized}");
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }
    }
    // The names accepted in VALUE mode above fail the explicit DOCUMENT cast.
    // SET's canonical multi-name string is '1,true', not its integer mask 3.
    for source in ["e_bad", "s_multi"] {
        let sql = format!("SELECT CAST({source} AS JSON) FROM native_json_source_sql");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        assert!(matches!(
            &error,
            DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::Json(tidb_executor::JsonError::InvalidText)
            ))
        ));
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 3140);
        assert_eq!(mysql.state, *b"22032");
        assert_eq!(
            mysql.message,
            "Invalid JSON text: The document root must not be followed by other values."
        );
        assert!(mysql.is_from_evaluation());
        assert!(warnings_of(&session).is_empty(), "{sql}");
    }
    // Six successful SELECTs across both modes plus two document-error
    // controls. All expected JSON tags/bytes are literals, not parser output.
    // Missing metadata/vector-before-child, raw helper values and datatype
    // surrogate repair retain their separate unit/previous-SQL coverage.
}

#[test]
fn native_json_coercion_preserves_sql_boolean_bit_document_and_value_policies() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_json_coercion_sql (i BIGINT, u BIGINT UNSIGNED, f FLOAT, d DECIMAL(6,2), dt DATETIME(3), tm TIME(3), b BINARY(3), vb VARBINARY(3), bits BIT(3), jnum JSON, jstr JSON, jnull JSON, doc JSON, s VARCHAR(16), n VARCHAR(16))")
        .unwrap();
    session
        .run(r#"INSERT INTO native_json_coercion_sql VALUES (1,1,0.1,1.25,'2024-01-02 03:04:05.006','00:00:01.250','ab','ab',b'101','1','"1"','null','{}','1',NULL)"#)
        .unwrap();
    type JsonCell = Option<(u8, &'static [u8])>;
    let cases: [(&str, &[JsonCell]); 4] = [
        (
            "CAST(i=1 AS JSON),CAST(i AS JSON),CAST(u AS JSON)",
            &[
                Some((0x04, &[1])),
                Some((0x09, &[1, 0, 0, 0, 0, 0, 0, 0])),
                Some((0x0a, &[1, 0, 0, 0, 0, 0, 0, 0])),
            ],
        ),
        (
            "CAST(bits AS JSON),CAST(b'101' AS JSON),CAST(x'aabb' AS JSON),CAST(b AS JSON),CAST(vb AS JSON)",
            &[
                Some((0x0a, &[5, 0, 0, 0, 0, 0, 0, 0])),
                Some((0x0d, &[253, 1, 5])),
                Some((0x0d, &[253, 2, 0xaa, 0xbb])),
                Some((0x0d, &[254, 3, b'a', b'b', 0])),
                Some((0x0d, &[15, 2, b'a', b'b'])),
            ],
        ),
        (
            "CAST(f AS JSON),CAST(d AS JSON),CAST(dt AS JSON),CAST(tm AS JSON),CAST(jnull AS JSON),CAST(n AS JSON)",
            &[
                Some((0x0b, &[0, 0, 0, 0xa0, 0x99, 0x99, 0xb9, 0x3f])),
                Some((0x0b, &[0, 0, 0, 0, 0, 0, 0xf4, 0x3f])),
                Some((0x0f, &[0, 0x77, 1, 5, 0x31, 0x44, 0xa0, 0x1f])),
                Some((0x11, &[0x80, 0x7c, 0x81, 0x4a, 0, 0, 0, 0, 6, 0, 0, 0])),
                Some((0x04, &[0])),
                None,
            ],
        ),
        (
            "CAST(s AS JSON),JSON_ARRAY(s,NULL,i=1),JSON_SET(doc,'$.x',s)",
            &[
                Some((0x09, &[1, 0, 0, 0, 0, 0, 0, 0])),
                // ["1", null, true]: three entries, string at 23, literals
                // inline, total payload size 25. No parser-built expectation.
                Some((0x03, &[
                    3, 0, 0, 0, 25, 0, 0, 0,
                    12, 23, 0, 0, 0, 4, 0, 0, 0, 0, 4, 1, 0, 0, 0,
                    1, b'1',
                ])),
                // {"x": "1"}: key at 19 and value string at 20, size 22.
                Some((0x01, &[
                    1, 0, 0, 0, 22, 0, 0, 0,
                    19, 0, 0, 0, 1, 0, 12, 20, 0, 0, 0,
                    b'x', 1, b'1',
                ])),
            ],
        ),
    ];
    // These are expression-controller consumers, with the real source flags.
    // DDL gives BIT its implicit UNSIGNED flag. ScalarFunction first converts
    // the hybrid column to Int(5), then restores UInt from that source flag;
    // this does NOT claim that a raw Datum::Bit helper becomes a JSON number.
    // The two binary literals instead produce opaque type253 payloads.
    // Likewise, SQL FLOAT has already narrowed to f32 and the typed entrypoint
    // widens it to Real before this controller: the fixed double payload is
    // 0.10000000149011612, not evidence for a raw Float32 helper input.
    // Temporal FSP=6, binary source codes/padding, boolean flags and SQL NULL
    // are controller choices; typed constructors only encode the chosen value.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (case_index, &(projection, expected)) in cases.iter().enumerate() {
            let sql = format!("SELECT {projection} FROM native_json_coercion_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected shared JSON coercion rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected) in expected.iter().enumerate() {
                let field = &columns[index].1;
                let cast = case_index != 3 || index == 0;
                assert_eq!(field.code(), FieldTypeCode::Json);
                assert_eq!(
                    (field.flen(), field.decimal()),
                    if cast {
                        (4_194_304, 0)
                    } else {
                        (16_777_216, -1)
                    }
                );
                assert_eq!(
                    field.charset_name(),
                    if cast { "utf8mb4" } else { "binary" }
                );
                assert_eq!(
                    field.collation_name(),
                    if cast { "utf8mb4_bin" } else { "binary" }
                );
                assert!(field.has_flag(FieldTypeFlags::BINARY));
                assert_eq!(field.has_flag(FieldTypeFlags::PARSE_TO_JSON), cast);
                assert!(!field.is_unsigned());
                if let Some((tag, payload)) = expected {
                    let Datum::Json(value) = &rows[0][index] else {
                        panic!("lost typed JSON carrier: {sql}/{index}")
                    };
                    assert_eq!(value.type_code(), *tag, "{sql}/{vectorized}/{index}");
                    assert_eq!(value.value(), *payload, "{sql}/{vectorized}/{index}");
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }
        // Two nonconstant candidates avoid single-IN's equality rewrite.
        // The wrapper disables ParseToJSONFlag: SQL s='1' is a JSON string
        // value here, unlike the explicit document CAST in the fourth probe.
        let sql = "SELECT jnum IN(s,s),jstr IN(s,s) FROM native_json_coercion_sql";
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(sql).unwrap() else {
            panic!("expected typed JSON value-policy IN rows")
        };
        assert_eq!(columns.len(), 2);
        for (_, field) in &columns {
            assert_eq!(field.code(), FieldTypeCode::LongLong);
            assert_eq!((field.flen(), field.decimal()), (1, 0));
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
            assert!(field.has_flag(FieldTypeFlags::IS_BOOLEAN));
            assert!(!field.is_unsigned());
        }
        assert_eq!(
            rows,
            vec![vec![Datum::Int(0), Datum::Int(1)]],
            "vectorized={vectorized}"
        );
        assert!(warnings_of(&session).is_empty());
    }
    // Ten successful SELECTs; strict malformed/surrogate document failures
    // remain covered by the unchanged preceding rounds. No raw Float32/NaN,
    // minimum-duration/unknown-metadata, new Head or zero-slot claim is made.
}

#[test]
fn native_json_parse_preserves_storage_surrogates_and_strict_expression_boundary() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_json_parse_sql (j_hi JSON, s_hi VARCHAR(64), j_lo JSON, s_lo VARCHAR(64), j_pair JSON, s_pair VARCHAR(64), j_nested JSON)")
        .unwrap();
    // Raw Rust strings preserve TWO SQL backslashes. With sql_mode='', the
    // SQL lexer consumes each pair and passes ONE backslash to the JSON text.
    // Identical literal bytes feed JSON's write cast and VARCHAR storage.
    session
        .run(r#"INSERT INTO native_json_parse_sql VALUES ('"\\ud800"','"\\ud800"','"\\udc00"','"\\udc00"','"\\ud83d\\ude00"','"\\ud83d\\ude00"','{"z":[1,1.25,null],"a":"\\ud800"}')"#)
        .unwrap();
    enum Expected {
        Text(&'static str),
        Json(u8, &'static [u8]),
    }
    // Source layout: count/size header, five-byte value entries, then payloads.
    // The three-item array has inline NULL, signed-int64 1 and double 1.25.
    let array: &'static [u8] = &[
        3, 0, 0, 0, 39, 0, 0, 0, 9, 23, 0, 0, 0, 11, 31, 0, 0, 0, 4, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0xf4, 0x3f,
    ];
    // Two sorted keys a/z: header 8 + key entries 12 + value entries 10;
    // key bytes at 30/31, replacement string at 32, child array at 36.
    let object: &'static [u8] = &[
        2, 0, 0, 0, 75, 0, 0, 0, 30, 0, 0, 0, 1, 0, 31, 0, 0, 0, 1, 0, 12, 32, 0, 0, 0, 3, 36, 0,
        0, 0, b'a', b'z', 3, 0xef, 0xbf, 0xbd, 3, 0, 0, 0, 39, 0, 0, 0, 9, 23, 0, 0, 0, 11, 31, 0,
        0, 0, 4, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf4, 0x3f,
    ];
    let cases: [(&str, &[Expected]); 4] = [
        (
            r#"SELECT HEX('"\\ud800"'),HEX(s_hi),HEX(s_lo),HEX(s_pair) FROM native_json_parse_sql"#,
            &[
                Expected::Text("225C756438303022"),
                Expected::Text("225C756438303022"),
                Expected::Text("225C756463303022"),
                Expected::Text("225C75643833645C756465303022"),
            ],
        ),
        (
            "SELECT JSON_TYPE(j_hi),HEX(JSON_UNQUOTE(j_hi)),JSON_TYPE(j_lo),HEX(JSON_UNQUOTE(j_lo)),JSON_TYPE(j_pair),HEX(JSON_UNQUOTE(j_pair)) FROM native_json_parse_sql",
            &[
                Expected::Text("STRING"), Expected::Text("EFBFBD"),
                Expected::Text("STRING"), Expected::Text("EFBFBD"),
                Expected::Text("STRING"), Expected::Text("F09F9880"),
            ],
        ),
        (
            "SELECT CAST(j_hi AS JSON),CAST(j_lo AS JSON),CAST(j_pair AS JSON),CAST(s_pair AS JSON) FROM native_json_parse_sql",
            &[
                Expected::Json(0x0c, &[3, 0xef, 0xbf, 0xbd]),
                Expected::Json(0x0c, &[3, 0xef, 0xbf, 0xbd]),
                Expected::Json(0x0c, &[4, 0xf0, 0x9f, 0x98, 0x80]),
                Expected::Json(0x0c, &[4, 0xf0, 0x9f, 0x98, 0x80]),
            ],
        ),
        (
            "SELECT CAST(j_nested AS JSON),JSON_EXTRACT(j_nested,'$.a'),JSON_EXTRACT(j_nested,'$.z'),JSON_EXTRACT(j_nested,'$.z[0]'),JSON_EXTRACT(j_nested,'$.z[1]'),JSON_EXTRACT(j_nested,'$.z[2]') FROM native_json_parse_sql",
            &[
                Expected::Json(0x01, object),
                Expected::Json(0x0c, &[3, 0xef, 0xbf, 0xbd]),
                Expected::Json(0x03, array),
                Expected::Json(0x09, &[1, 0, 0, 0, 0, 0, 0, 0]),
                Expected::Json(0x0b, &[0, 0, 0, 0, 0, 0, 0xf4, 0x3f]),
                Expected::Json(0x04, &[0]),
            ],
        ),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (case_index, &(sql, expected)) in cases.iter().enumerate() {
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(sql).unwrap() else {
                panic!("expected stored JSON parser rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected) in expected.iter().enumerate() {
                let field = &columns[index].1;
                let cast_json = case_index == 2 || (case_index == 3 && index == 0);
                let extract_json = case_index == 3 && index != 0;
                let (code, flen, decimal) = if cast_json {
                    (FieldTypeCode::Json, 4_194_304, 0)
                } else if extract_json {
                    (FieldTypeCode::Json, 16_777_216, -1)
                } else {
                    // HEX's string width is argument flen * 8. JSON_TYPE and
                    // JSON_UNQUOTE retain unspecified width in this resolver.
                    let flen = if case_index == 0 {
                        if index == 0 {
                            64
                        } else {
                            512
                        }
                    } else {
                        -1
                    };
                    (FieldTypeCode::VarString, flen, -1)
                };
                assert_eq!(field.code(), code, "{sql}/{index}");
                assert_eq!(
                    (field.flen(), field.decimal()),
                    (flen, decimal),
                    "{sql}/{index}"
                );
                assert_eq!(
                    field.charset_name(),
                    if extract_json { "binary" } else { "utf8mb4" }
                );
                assert_eq!(
                    field.collation_name(),
                    if extract_json {
                        "binary"
                    } else {
                        "utf8mb4_bin"
                    }
                );
                assert_eq!(
                    field.has_flag(FieldTypeFlags::BINARY),
                    cast_json || extract_json
                );
                assert_eq!(field.has_flag(FieldTypeFlags::PARSE_TO_JSON), cast_json);
                assert!(!field.is_unsigned());
                match expected {
                    Expected::Text(text) => assert_eq!(
                        cell_text(&rows[0][index]),
                        *text,
                        "{sql}/{vectorized}/{index}"
                    ),
                    Expected::Json(tag, payload) => {
                        let Datum::Json(value) = &rows[0][index] else {
                            panic!("lost binary JSON carrier: {sql}/{index}")
                        };
                        assert_eq!(value.type_code(), *tag, "{sql}/{vectorized}/{index}");
                        assert_eq!(value.value(), *payload, "{sql}/{vectorized}/{index}");
                    }
                }
            }
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }
    }
    // The SAME original VARCHAR bytes remain lone surrogate escapes. The
    // unchanged expression controller uses its strict document parser, not
    // the datatype write path's sanitizer/retry. Already-JSON clones above
    // never reparse those original escapes. Two controls, one per source.
    for source in ["s_hi", "s_lo"] {
        let sql = format!("SELECT CAST({source} AS JSON) FROM native_json_parse_sql");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        assert!(matches!(
            &error,
            DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::Json(tidb_executor::JsonError::InvalidText)
            ))
        ));
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 3140);
        assert_eq!(mysql.state, *b"22032");
        assert_eq!(
            mysql.message,
            "Invalid JSON text: The document root must not be followed by other values."
        );
        assert!(mysql.is_from_evaluation());
        assert!(warnings_of(&session).is_empty(), "{sql}");
    }
    // No expected value is built by a JSON parser/constructor. This is not
    // an expression-selector migration or exhaustive Unicode/depth/key-limit
    // evidence; retry/trailing/global-sanitizer unit domains remain separate.
}

#[test]
fn native_json_construction_preserves_sql_tags_opaque_temporals_and_value_coercion() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_json_construction_sql (b BINARY(3), vb VARBINARY(3), u BIGINT UNSIGNED, d DECIMAL(6,2), dt DATETIME(3), tm TIME(3), jnum JSON, jstr JSON, jnull JSON, sqlnull JSON, s VARCHAR(16), bad_text VARCHAR(16))")
        .unwrap();
    session
        .run(r#"INSERT INTO native_json_construction_sql VALUES ('ab','ab',1,1.25,'2024-01-02 03:04:05.006','00:00:01.250','1','"1"','null',NULL,'1','not-json')"#)
        .unwrap();
    type JsonCell = Option<(u8, &'static [u8], &'static str)>;
    let cases: [(&str, &[JsonCell]); 3] = [
        (
            "CAST(b AS JSON),CAST(vb AS JSON),CAST(u AS JSON),CAST(d AS JSON)",
            &[
                Some((0x0d, &[254, 3, b'a', b'b', 0], r#""base64:type254:YWIA""#)),
                Some((0x0d, &[15, 2, b'a', b'b'], r#""base64:type15:YWI=""#)),
                Some((0x0a, &[1, 0, 0, 0, 0, 0, 0, 0], "1")),
                Some((0x0b, &[0, 0, 0, 0, 0, 0, 0xf4, 0x3f], "1.25")),
            ],
        ),
        (
            "CAST(dt AS JSON),CAST(tm AS JSON)",
            &[
                Some((0x0f, &[0x00, 0x77, 0x01, 0x05, 0x31, 0x44, 0xa0, 0x1f], r#""2024-01-02 03:04:05.006000""#)),
                Some((0x11, &[0x80, 0x7c, 0x81, 0x4a, 0, 0, 0, 0, 6, 0, 0, 0], r#""00:00:01.250000""#)),
            ],
        ),
        (
            "CAST(jnum AS JSON),CAST(jstr AS JSON),CAST(jnull AS JSON),CAST(sqlnull AS JSON),CAST(s AS JSON)",
            &[
                Some((0x09, &[1, 0, 0, 0, 0, 0, 0, 0], "1")),
                Some((0x0c, &[1, b'1'], r#""1""#)),
                Some((0x04, &[0], "null")),
                None,
                Some((0x09, &[1, 0, 0, 0, 0, 0, 0, 0], "1")),
            ],
        ),
    ];
    // Fixed source-layout literals, not BinaryJSON::parse/from_typed_value
    // expected values: opaque = field code + uvarint length + raw bytes;
    // numeric scalars are LE payloads. DATETIME's core is
    // (2024<<50)|(1<<46)|(2<<41)|(3<<36)|(4<<30)|(5<<24)|(6000<<4)
    // = 0x1fa0443105017700. Duration is LE 1_250_000_000 ns + LE u32 FSP 6.
    // The unchanged expression controller sets temporal FSP=6 before the
    // datatype constructor, preserves JSON clones, and guards SQL NULL.
    // This is datatype-construction consumer evidence, not a migration of
    // builtin_ext/json/value.rs's CAST/document/value selector.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(projection, expected) in &cases {
            let sql = format!("SELECT {projection} FROM native_json_construction_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected typed JSON construction rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected) in expected.iter().enumerate() {
                let field = &columns[index].1;
                assert_eq!(field.code(), FieldTypeCode::Json);
                assert_eq!((field.flen(), field.decimal()), (4_194_304, 0));
                assert_eq!(field.charset_name(), "utf8mb4");
                assert_eq!(field.collation_name(), "utf8mb4_bin");
                assert!(field.has_flag(FieldTypeFlags::BINARY));
                assert!(field.has_flag(FieldTypeFlags::PARSE_TO_JSON));
                assert!(!field.is_unsigned());
                if let Some((tag, payload, text)) = expected {
                    let Datum::Json(value) = &rows[0][index] else {
                        panic!("lost typed JSON carrier: {sql}/{index}")
                    };
                    assert_eq!(value.type_code(), *tag, "{sql}/{vectorized}/{index}");
                    assert_eq!(value.value(), *payload, "{sql}/{vectorized}/{index}");
                    assert_eq!(value.to_string(), *text, "{sql}/{vectorized}/{index}");
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }
        // A one-item IN rewrites to equality. Two nonconstant column entries
        // retain typed IN (constant deduplication cannot remove them), whose
        // JSON wrappers use VALUE coercion rather than parsing s='1' as JSON.
        let sql = "SELECT jnum IN(s,s),jnum IN(u,u),jstr IN(s,s),jstr IN(u,u) FROM native_json_construction_sql";
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(sql).unwrap() else {
            panic!("expected typed JSON IN rows")
        };
        assert_eq!(columns.len(), 4);
        for (_, field) in &columns {
            assert_eq!(field.code(), FieldTypeCode::LongLong);
            assert_eq!((field.flen(), field.decimal()), (1, 0));
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
            assert!(field.has_flag(FieldTypeFlags::IS_BOOLEAN));
            assert!(!field.is_unsigned());
        }
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(0),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(0)
            ]],
            "vectorized={vectorized}"
        );
        assert!(warnings_of(&session).is_empty());
    }
    // One unchanged expression-control failure, outside the dual-mode matrix.
    let error = session
        .run_with_columns("SELECT CAST(bad_text AS JSON) FROM native_json_construction_sql")
        .unwrap_err();
    assert!(matches!(
        &error,
        DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Json(tidb_executor::JsonError::InvalidText)
        ))
    ));
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 3140);
    assert_eq!(mysql.state, *b"22032");
    assert_eq!(
        mysql.message,
        "Invalid JSON text: The document root must not be followed by other values."
    );
    assert!(mysql.is_from_evaluation());
    assert!(warnings_of(&session).is_empty());
    // Raw Float32/nonfinite/invalid binary/FSP/depth/key-limit unit domains,
    // new worker Heads and zero-slot/whole-JSON-family credit are not claimed.
}

#[test]
fn native_vector_cast_policy_preserves_stored_domains_dimensions_and_typed_errors() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_vector_cast_policy_sql (s VARCHAR(32), valid_bytes VARBINARY(32), v VECTOR(2), null_text VARCHAR(32), null_vector VECTOR(2), bad_bytes VARBINARY(8), bad_text VARCHAR(32), i BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO native_vector_cast_policy_sql VALUES ('[1,2.5]',0x5B312C322E355D,'[3,4]',NULL,NULL,0xFF,'not-a-vector',7)")
        .unwrap();
    let cases: [(&str, &[Option<&[f32]>], &[i64]); 2] = [
        (
            "CAST(s AS VECTOR),CAST(s AS VECTOR(2)),CAST(valid_bytes AS VECTOR(2))",
            &[Some(&[1.0, 2.5]), Some(&[1.0, 2.5]), Some(&[1.0, 2.5])],
            &[-1, 2, 2],
        ),
        (
            "CAST(v AS VECTOR),CAST(v AS VECTOR(2)),CAST(null_text AS VECTOR(3)),CAST(null_vector AS VECTOR(3))",
            &[Some(&[3.0, 4.0]), Some(&[3.0, 4.0]), None, None],
            &[-1, 2, 3, 3],
        ),
    ];
    // Two positive projections in each mode. The NULL VECTOR(2) source is
    // deliberately cast to VECTOR(3): the original SQL NULL guard precedes
    // dimension enforcement. Expected elements are fixed literals, never
    // produced by the vector parser being migrated.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(projection, expected, dimensions) in &cases {
            let sql = format!("SELECT {projection} FROM native_vector_cast_policy_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected stored vector cast rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected) in expected.iter().enumerate() {
                let field = &columns[index].1;
                assert_eq!(field.code(), FieldTypeCode::VectorFloat32);
                // CAST's parser type sets decimal=0 and binary charset, but
                // unlike many other casts it does NOT add BinaryFlag.
                assert_eq!((field.flen(), field.decimal()), (dimensions[index], 0));
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
                assert!(!field.has_flag(FieldTypeFlags::BINARY));
                assert!(!field.is_unsigned());
                if let Some(expected) = expected {
                    let Datum::VectorFloat32(value) = &rows[0][index] else {
                        panic!("VECTOR result lost its native carrier: {sql}/{index}")
                    };
                    assert_eq!(value.elements().len(), expected.len());
                    for (actual, expected) in value.elements().iter().zip(expected.iter()) {
                        assert_eq!(
                            actual.to_bits(),
                            expected.to_bits(),
                            "{sql}/{vectorized}/{index}"
                        );
                    }
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }
    }
    // Four separate failures, not an error Cartesian product: together with
    // the four successful SELECTs above, this is eight SQL probes. A binary
    // column can be materialized as a collation-bearing String, so this tests
    // strict byte payload decoding, not a forced Datum::Bytes enum variant.
    for (vectorized, projection, expected) in [
        (
            0,
            "CAST(v AS VECTOR(3))",
            "vector has 2 dimensions, does not fit VECTOR(3)",
        ),
        (
            1,
            "CAST(bad_bytes AS VECTOR(2))",
            "invalid utf-8 sequence of 1 bytes from index 0",
        ),
        (
            0,
            "CAST(bad_text AS VECTOR)",
            "Invalid vector text: not-a-vector",
        ),
        (1, "CAST(i AS VECTOR)", "cannot cast from bigint to vector"),
    ] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let sql = format!("SELECT {projection} FROM native_vector_cast_policy_sql");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        match &error {
            DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::Vector(message),
            )) => assert_eq!(message, expected, "{sql}/{vectorized}"),
            other => panic!("changed vector cast error tier: {sql}: {other:?}"),
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105);
        assert_eq!(mysql.state, *b"HY000");
        assert_eq!(mysql.message, expected);
        assert!(mysql.is_from_evaluation());
        // Evaluation-origin 1105 does not append its own error warning row.
        assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
    }
    // No new Head/zero-slot, JSON-cast, global dimension cap, mutable-NaN
    // clone validation, or unreachable SQL source-type-name claim is made.
}

#[test]
fn native_datum_sql_string_preserves_stored_cast_formatting_and_exact_payloads() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_datum_string_sql (small_r DOUBLE, large_r DOUBLE, f FLOAT, d DECIMAL(10,4), calendar_date DATE, dt DATETIME(6), tm TIME(3), j_string JSON, j_array JSON, j_jsonnull JSON, j_sqlnull JSON, text_value VARCHAR(16), raw_bytes VARBINARY(8))")
        .unwrap();
    session
        .run(r#"INSERT INTO native_datum_string_sql VALUES (1e-7,1e20,0.1,12.3400,'2024-02-29','2024-02-29 01:02:03.120000','-26:07:08.125','"x"','[1,2]','null',NULL,'你好\0x',0xFF0041)"#)
        .unwrap();
    let cases: [(&str, &[Option<&[u8]>], &[bool]); 4] = [
        (
            "CAST(small_r AS CHAR),CAST(large_r AS CHAR),CAST(f AS CHAR),CAST(d AS CHAR)",
            &[Some(b"0.0000001"), Some(b"100000000000000000000"), Some(b"0.1"), Some(b"12.3400")],
            &[false, false, false, false],
        ),
        (
            "CAST(calendar_date AS CHAR),CAST(dt AS BINARY),CAST(tm AS CHAR)",
            &[Some(b"2024-02-29"), Some(b"2024-02-29 01:02:03.120000"), Some(b"-26:07:08.125")],
            &[false, true, false],
        ),
        (
            "CAST(j_string AS CHAR),CAST(j_array AS BINARY),CAST(j_jsonnull AS CHAR),CAST(j_sqlnull AS CHAR)",
            &[Some(b"\"x\""), Some(b"[1, 2]"), Some(b"null"), None],
            &[false, true, false, false],
        ),
        (
            "CAST(text_value AS CHAR),CAST(text_value AS BINARY),CAST(raw_bytes AS BINARY)",
            &[Some("你好\0x".as_bytes()), Some("你好\0x".as_bytes()), Some(&[0xFF, 0, b'A'])],
            &[false, true, true],
        ),
    ];
    // Real/Float32/Decimal/temporal/JSON are Other inputs to the existing
    // CHAR/BINARY bridge, which invokes the actual Datum::sql_string callback.
    // Real uses shortest FIXED f64 rendering, while a FLOAT column is already
    // narrowed and sql_bytes formats it at f32 precision: 0.1, not the wider
    // 0.10000000149011612 text used by a promoted-f64 decimal conversion.
    // Decimal and temporal display retain visible scale; JSON retains quotes
    // and its array separators. SQL NULL is the cast guard, not evidence for
    // the standalone sql_bytes(NULL) empty-byte result.
    // The binary raw-payload controls may bypass semantic stringification;
    // they guard the unchanged byte path, not invalid UTF-8 sql_string success.
    // No minus-zero storage assumption, sentinel, noncanonical temporal,
    // new CAST target, runtime Head or zero-slot claim is made here.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(projection, expected, binary) in &cases {
            let sql = format!("SELECT {projection} FROM native_datum_string_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected stored SQL-string consumer rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected) in expected.iter().enumerate() {
                let field = &columns[index].1;
                assert_eq!(field.code(), FieldTypeCode::VarString, "{sql}/{index}");
                assert_eq!((field.flen(), field.decimal()), (-1, -1), "{sql}/{index}");
                assert_eq!(
                    field.charset_name(),
                    if binary[index] { "binary" } else { "utf8mb4" }
                );
                assert_eq!(
                    field.collation_name(),
                    if binary[index] {
                        "binary"
                    } else {
                        "utf8mb4_bin"
                    }
                );
                assert_eq!(field.has_flag(FieldTypeFlags::BINARY), binary[index]);
                assert_eq!(field.is_binary_string(), binary[index]);
                assert!(!field.is_unsigned());
                if let Some(expected) = expected {
                    // String-family typed finish leaves the payload intact;
                    // row materialization may restore the target collation.
                    let actual = match &rows[0][index] {
                        Datum::String(value) => value.bytes(),
                        Datum::Bytes(value) => value.as_slice(),
                        other => panic!("changed string carrier: {sql}/{index}: {other:?}"),
                    };
                    assert_eq!(actual, *expected, "{sql}/{vectorized}/{index}");
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }
    }
}

#[test]
fn native_string_cast_policy_preserves_sql_year_bytes_decode_order_and_packet_padding() {
    use tidb_datatype::{FieldTypeCode as C, FieldTypeFlags};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_string_cast_policy_sql (y YEAR, i BIGINT, unicode_text VARCHAR(16), raw_bytes VARBINARY(8), gbk_bad VARBINARY(8), short_bytes VARBINARY(8), wide_bytes VARBINARY(1100), equal_bytes VARBINARY(1100))")
        .unwrap();
    session
        .run(&format!("INSERT INTO native_string_cast_policy_sql VALUES (0,0,'你好x',0x00FF,0x414281,'ab','{}','{}')", "a".repeat(1050), "b".repeat(1025)))
        .unwrap();
    // SQL SET SESSION is read-only for this variable. Reuse the existing
    // validated SessionVars fixture path, after inserting the larger rows.
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    assert_eq!(session.max_allowed_packet(), 1024);
    type Cell = (C, i64, &'static str, &'static str, Option<Vec<u8>>);
    let cases: [(&str, Vec<Cell>, &[(u16, &str)]); 5] = [
        (
            "CAST(y AS CHAR),CAST(i AS CHAR),CAST(y AS BINARY),CAST(y AS CHAR(8) CHARACTER SET binary),CAST(y AS BINARY(6))",
            vec![
                (C::VarString, -1, "utf8mb4", "utf8mb4_bin", Some(b"0000".to_vec())),
                (C::VarString, -1, "utf8mb4", "utf8mb4_bin", Some(b"0".to_vec())),
                (C::VarString, -1, "binary", "binary", Some(b"0000".to_vec())),
                (C::VarString, 8, "binary", "binary", Some(b"0".to_vec())),
                (C::String, 6, "binary", "binary", Some(b"0000\0\0".to_vec())),
            ],
            &[],
        ),
        (
            "CAST(unicode_text AS CHAR(2)),CAST(unicode_text AS BINARY(5)),CAST(unicode_text AS CHAR(5) CHARACTER SET binary)",
            vec![
                (C::VarString, 2, "utf8mb4", "utf8mb4_bin", Some("你好".as_bytes().to_vec())),
                (C::String, 5, "binary", "binary", Some(vec![0xE4, 0xBD, 0xA0, 0xE5, 0xA5])),
                (C::VarString, 5, "binary", "binary", Some(vec![0xE4, 0xBD, 0xA0, 0xE5, 0xA5])),
            ],
            &[
                (1406, "Data Too Long, field len 2, data len 3"),
                (1406, "Data Too Long, field len 5, data len 7"),
                (1406, "Data Too Long, field len 5, data len 7"),
            ],
        ),
        (
            "CAST(raw_bytes AS BINARY(4)),CAST(raw_bytes AS BINARY),CAST(0x00FF AS BINARY(4))",
            vec![
                (C::String, 4, "binary", "binary", Some(vec![0, 255, 0, 0])),
                (C::VarString, -1, "binary", "binary", Some(vec![0, 255])),
                (C::String, 4, "binary", "binary", Some(vec![0, 255, 0, 0])),
            ],
            &[],
        ),
        (
            "CAST(gbk_bad AS CHAR(1) CHARACTER SET gbk)",
            vec![(C::VarString, 1, "gbk", "gbk_chinese_ci", Some(b"A".to_vec()))],
            &[
                (3854, "Cannot convert string '414281' from binary to gbk"),
                (1406, "Data Too Long, field len 1, data len 2"),
            ],
        ),
        (
            "CAST(short_bytes AS BINARY(1025)),CAST(wide_bytes AS BINARY(1025)),CAST(equal_bytes AS BINARY(1025)),CAST(short_bytes AS BINARY)",
            vec![
                (C::String, 1025, "binary", "binary", None),
                (C::String, 1025, "binary", "binary", Some(vec![b'a'; 1025])),
                (C::String, 1025, "binary", "binary", Some(vec![b'b'; 1025])),
                (C::VarString, -1, "binary", "binary", Some(b"ab".to_vec())),
            ],
            &[
                (1301, "Result of cast_as_binary() was larger than max_allowed_packet (1024) - truncated"),
                (1406, "Data Too Long, field len 1025, data len 1050"),
            ],
        ),
    ];
    // SQL canonicalizes the binary charset before reconstructing CastType;
    // this is not evidence for the raw SDK's case-sensitive charset input.
    // CHAR ... binary has neither YEAR rendering nor BINARY's NUL padding.
    // Explicit SQL casts retain unspecified width; they do not take the
    // separate build_cast_function's argument-based string-width inference.
    // String/Bytes already share the String eval family, unlike R113 FLOAT:
    // ScalarFunction's typed finish does not re-truncate/re-encode these.
    // Chunk materialization may rebuild a collation-bearing String datum, so
    // compare its bytes, including invalid UTF-8 and NULs, not display text.
    // The raw literal case is a build-time cast consumer; other sources are
    // stored columns. No new Head, zero-slot or whole-CAST claim is made.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (projection, expected, warnings) in &cases {
            let sql = format!("SELECT {projection} FROM native_string_cast_policy_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected CHAR/BINARY policy rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, (code, flen, charset, collation, bytes)) in expected.iter().enumerate() {
                let field = &columns[index].1;
                assert_eq!(field.code(), *code, "{sql}/{index}");
                assert_eq!(
                    (field.flen(), field.decimal()),
                    (*flen, -1),
                    "{sql}/{index}"
                );
                assert_eq!(field.charset_name(), *charset, "{sql}/{index}");
                assert_eq!(field.collation_name(), *collation, "{sql}/{index}");
                assert_eq!(field.has_flag(FieldTypeFlags::BINARY), *charset == "binary");
                assert_eq!(field.is_binary_string(), *collation == "binary");
                assert!(!field.is_unsigned());
                if let Some(expected_bytes) = bytes {
                    let actual = match &rows[0][index] {
                        Datum::String(value) => value.bytes(),
                        Datum::Bytes(value) => value.as_slice(),
                        other => panic!("changed string result carrier: {sql}/{index}: {other:?}"),
                    };
                    assert_eq!(
                        actual,
                        expected_bytes.as_slice(),
                        "{sql}/{vectorized}/{index}"
                    );
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert_eq!(
                warnings_of(&session),
                warnings
                    .iter()
                    .map(|&(code, message)| (code, message.to_owned()))
                    .collect::<Vec<_>>(),
                "{sql}/{vectorized}"
            );
        }
    }
}

#[test]
fn native_float_cast_policy_preserves_sql_source_parsing_narrowing_and_error_order() {
    use tidb_datatype::FieldTypeCode;

    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_float_cast_policy_sql (r DOUBLE, f FLOAT, d DECIMAL(6,2), s VARCHAR(32), huge_r DOUBLE, null_text VARCHAR(32), prefix_text VARCHAR(32), bare_e VARCHAR(32), nul_text VARCHAR(32), empty_text VARCHAR(32), invalid_bytes VARBINARY(8), j_number JSON, j_string JSON, j_bool JSON, j_null JSON, overflow_text VARCHAR(32), float_overflow_text VARCHAR(32))")
        .unwrap();
    session
        .run(r#"INSERT INTO native_float_cast_policy_sql VALUES (0.1,0.1,12.50,'0.1',1e300,NULL,' 12.5tail ','5e','\0 12','',0x31FF,'12.5','"12.5"','true','null','1e999x','1e300x')"#)
        .unwrap();
    // Ordinary DOUBLE/FLOAT policy, not a new runtime Head or admission gate.
    // Each ScalarFunction completes its own typed finish before its parent
    // evaluates: same_eval_family rejects Real under a Float result type,
    // and datum_convert materializes Float32 via an f32 cast. Thus even a
    // string's FLOAT result is narrowed before the outer DOUBLE sees it;
    // direct DOUBLE parsing is the contrasting 0.1 result. The controller's
    // earlier Other-source non-narrowing rule needs expression-unit evidence,
    // not a SQL claim that this outer cast can bypass the inner typed finish.
    let cases: [(&str, &[Option<f64>], &[bool], &[(u16, &str)]); 4] = [
        (
            "CAST(CAST(r AS FLOAT) AS DOUBLE),CAST(CAST(s AS FLOAT) AS DOUBLE),CAST(f AS DOUBLE),CAST(d AS DOUBLE),CAST(CAST(huge_r AS FLOAT) AS DOUBLE),CAST(null_text AS DOUBLE),CAST(null_text AS FLOAT),CAST(s AS DOUBLE)",
            &[Some(0.10000000149011612), Some(0.10000000149011612), Some(0.10000000149011612), Some(12.5), Some(0.0), None, None, Some(0.1)],
            &[false, false, false, false, false, false, true, false],
            &[],
        ),
        (
            "CAST(prefix_text AS DOUBLE),CAST(bare_e AS DOUBLE),CAST(nul_text AS DOUBLE),CAST(empty_text AS DOUBLE),CAST(invalid_bytes AS DOUBLE)",
            &[Some(12.5), Some(5.0), Some(0.0), Some(0.0), Some(1.0)],
            &[false, false, false, false, false],
            &[
                (1292, "Truncated incorrect DOUBLE value: '12.5tail'"),
                (1292, "Truncated incorrect DOUBLE value: ''"),
                (1292, "Truncated incorrect DOUBLE value: '1�'"),
            ],
        ),
        (
            "CAST(j_number AS DOUBLE),CAST(j_string AS DOUBLE),CAST(j_bool AS DOUBLE),CAST(j_null AS DOUBLE)",
            &[Some(12.5), Some(0.0), Some(0.0), Some(0.0)],
            &[false, false, false, false],
            &[
                (1292, "Truncated incorrect FLOAT value: '\"12.5\"'"),
                (1292, "Truncated incorrect FLOAT value: 'true'"),
                (1292, "Truncated incorrect FLOAT value: 'null'"),
            ],
        ),
        (
            "CAST(overflow_text AS DOUBLE)",
            &[Some(f64::MAX)],
            &[false],
            &[(1292, "Truncated incorrect DOUBLE value: '1e999x'")],
        ),
    ];
    // valid_float_prefix accepts a final bare e quietly; a leading NUL has
    // no numeric prefix and also caps the warning's displayed subject to "".
    // Bytes are lossy UTF-8 here, unlike the separate value-only helper.
    // Every JSON source is reparsed from Display JSON: the quotes around a
    // JSON string remain, and even true/null warn with FLOAT, not DOUBLE.
    // Prefix plus range diagnostics still become ONE handle_truncate call.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(projection, expected, float_fields, warnings) in &cases {
            let sql = format!("SELECT {projection} FROM native_float_cast_policy_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected floating cast-policy rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected) in expected.iter().enumerate() {
                let field = &columns[index].1;
                let (code, flen) = if float_fields[index] {
                    (FieldTypeCode::Float, 12)
                } else {
                    (FieldTypeCode::Double, 22)
                };
                assert_eq!(field.code(), code, "{sql}/{index}");
                assert_eq!((field.flen(), field.decimal()), (flen, -1));
                assert!(!field.is_unsigned());
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
                if let Some(expected) = expected {
                    let Datum::Real(actual) = &rows[0][index] else {
                        panic!("DOUBLE result changed carrier: {sql}/{index}")
                    };
                    assert_eq!(
                        actual.to_bits(),
                        expected.to_bits(),
                        "{sql}/{vectorized}/{index}"
                    );
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert_eq!(
                warnings_of(&session),
                warnings
                    .iter()
                    .map(|&(code, message)| (code, message.to_owned()))
                    .collect::<Vec<_>>(),
                "{sql}/{vectorized}"
            );
        }
        let sql = "SELECT CAST(float_overflow_text AS FLOAT) FROM native_float_cast_policy_sql";
        let error = session
            .run_with_columns(sql)
            .expect_err("text FLOAT must check its range after parsing");
        match &error {
            DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::ConstantFloatCastOverflow { value },
            )) => assert_eq!(value, "1e+300"),
            other => panic!("expected source-specific FLOAT overflow: {other:?}"),
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1690);
        assert_eq!(mysql.state, *b"22003");
        assert_eq!(mysql.message, "constant 1e+300 overflows float");
        assert!(mysql.is_from_evaluation());
        // SELECT's warning-level truncate callback precedes the range error.
        // finish_statement_state excludes evaluation-origin 1690 from its
        // appended error rows, so only the original DOUBLE warning remains.
        // Error-level callback short-circuiting is an expression-unit concern.
        assert_eq!(
            warnings_of(&session),
            vec![(
                1292,
                "Truncated incorrect DOUBLE value: '1e300x'".to_owned()
            )],
            "{sql}/{vectorized}"
        );
    }
}

#[test]
fn native_integer_cast_policy_preserves_sql_domains_complements_and_warning_order() {
    use tidb_datatype::FieldTypeCode;

    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session.run("SET sql_mode=''").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_integer_cast_policy_sql (i BIGINT, u BIGINT UNSIGNED, d DECIMAL(4,1), r DOUBLE, f FLOAT, wide_text VARCHAR(32), negative_text VARCHAR(32), dirty_negative VARCHAR(32), dirty_wide VARCHAR(32), negative_d DECIMAL(4,1), negative_r DOUBLE, tiny_d DECIMAL(4,1), dt DATETIME(6), tm TIME(6), j JSON, j_array JSON, null_text VARCHAR(32))")
        .unwrap();
    session
        .run("INSERT INTO native_integer_cast_policy_sql VALUES (-5,18446744073709551615,2.5,2.5,2.5,'18446744073709551615','-5','-5x','18446744073709551615x',-1.5,-1.5,-0.4,'2024-01-31 23:59:59.500000','11:59:59.500000','12.5','[]',NULL)")
        .unwrap();
    // The integer cast control policy is native/shared; the already-admitted
    // REAL-to-UNSIGNED worker remains its own unchanged child. These normal
    // one-slot statements make no new Head, zero-slot or facade-count claim.
    let cases: [(&str, Vec<Datum>, &[bool], &[(u16, &str)]); 5] = [
        (
            "CAST(i AS UNSIGNED),CAST(u AS SIGNED),CAST(d AS SIGNED),CAST(r AS SIGNED),CAST(f AS SIGNED)",
            vec![Datum::UInt(18446744073709551611), Datum::Int(-1), Datum::Int(3), Datum::Int(2), Datum::Int(2)],
            &[true, false, false, false, false],
            &[],
        ),
        (
            "CAST(wide_text AS SIGNED),CAST(negative_text AS UNSIGNED),CAST(dirty_negative AS UNSIGNED),CAST(dirty_wide AS SIGNED)",
            vec![Datum::Int(-1), Datum::UInt(18446744073709551611), Datum::UInt(18446744073709551611), Datum::Int(-1)],
            &[false, true, true, false],
            &[
                (8030, "Cast to signed converted positive out-of-range integer to its negative complement"),
                (8031, "Cast to unsigned converted negative integer to it's positive complement"),
                (1292, "Truncated incorrect INTEGER value: '-5x'"),
                (1292, "Truncated incorrect INTEGER value: '18446744073709551615x'"),
            ],
        ),
        (
            "CAST(negative_d AS UNSIGNED),CAST(negative_r AS UNSIGNED),CAST(tiny_d AS UNSIGNED)",
            vec![Datum::UInt(0), Datum::UInt(18446744073709551614), Datum::UInt(0)],
            &[true, true, true],
            &[
                (1292, "Truncated incorrect DECIMAL value: '-1.5'"),
                (1690, "constant -2 overflows bigint"),
            ],
        ),
        (
            "CAST(dt AS SIGNED),CAST(tm AS UNSIGNED),CAST(j AS SIGNED),CAST(j AS UNSIGNED)",
            vec![Datum::Int(20240201000000), Datum::UInt(120000), Datum::Int(12), Datum::UInt(13)],
            &[false, true, false, true],
            &[],
        ),
        (
            "CAST(j_array AS UNSIGNED),CAST(null_text AS SIGNED),CAST(null_text AS UNSIGNED)",
            vec![Datum::UInt(0), Datum::Null, Datum::Null],
            &[true, false, true],
            &[(1292, "Truncated incorrect INTEGER value: '[]'")],
        ),
    ];
    // Decimal rounds half-up, whereas both the direct REAL signed path and
    // Float32's Datum::to_i64_in fallback use ties-to-even. Do not infer an
    // away-from-zero result from the stale convert_float_to_int comment.
    // JSON signed uses that float-to-int conversion, but JSON unsigned uses
    // Other -> to_decimal -> round_to_u64, hence the observable 12 versus 13.
    // Temporal sources round their clock fields before numeric rendering.
    // Malformed integer strings retain only 1292, not an additional 8030/31.
    // SQL does not manufacture the separate AST UnsignedInUnion entrypoint;
    // its original expression-level guards remain the appropriate evidence.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (projection, expected, unsigned, warnings) in &cases {
            let sql = format!("SELECT {projection} FROM native_integer_cast_policy_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected integer cast-policy rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}/{vectorized}");
            assert_eq!(rows, vec![expected.clone()], "{sql}/{vectorized}");
            for ((_, field), unsigned) in columns.iter().zip(unsigned.iter()) {
                assert_eq!(field.code(), FieldTypeCode::LongLong);
                assert_eq!((field.flen(), field.decimal()), (20, 0));
                assert_eq!(field.is_unsigned(), *unsigned);
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
            }
            assert_eq!(
                warnings_of(&session),
                warnings
                    .iter()
                    .map(|&(code, message)| (code, message.to_owned()))
                    .collect::<Vec<_>>(),
                "{sql}/{vectorized}"
            );
        }
    }
}

#[test]
fn native_decimal_cast_policy_preserves_sql_source_domains_and_warning_order() {
    use tidb_datatype::FieldTypeCode;

    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_decimal_cast_policy_sql (i BIGINT, u BIGINT UNSIGNED, r DOUBLE, f FLOAT, j_number JSON, j_prefix JSON, j_jsonnull JSON, j_sqlnull JSON, huge_text VARCHAR(32), d DECIMAL(8,2))")
        .unwrap();
    session
        .run(r#"INSERT INTO native_decimal_cast_policy_sql VALUES (-7,18446744073709551615,0.1,0.1,'12.5','"7.50tail"','null',NULL,'1e300',1.25)"#)
        .unwrap();
    // Ordinary DECIMAL cast policy only: no new Head, zero-slot, PB, UNION,
    // or whole-CAST credit. The previous digit-parser test separately guards
    // raw-value parsing versus Unicode-trimmed warning parsing.
    let cases: [(&str, &[Option<&str>], &[(i64, i64)], &[(u16, &str)]); 4] = [
        (
            "CAST(i AS DECIMAL(8,2)),CAST(u AS DECIMAL(22,2)),CAST(r AS DECIMAL(20,17)),CAST(f AS DECIMAL(20,17)),CAST(d AS DECIMAL(8,3))",
            &[Some("-7.00"), Some("18446744073709551615.00"), Some("0.10000000000000000"), Some("0.10000000149011612"), Some("1.250")],
            &[(8, 2), (22, 2), (20, 17), (20, 17), (8, 3)],
            &[],
        ),
        (
            "CAST(j_number AS DECIMAL(8,2)),CAST(j_prefix AS DECIMAL(8,2)),CAST(j_jsonnull AS DECIMAL(8,2)),CAST(j_sqlnull AS DECIMAL(8,2))",
            &[Some("12.50"), Some("7.50"), Some("0.00"), None],
            &[(8, 2), (8, 2), (8, 2), (8, 2)],
            &[],
        ),
        (
            "CAST(huge_text AS DECIMAL(5,2))",
            &[Some("999.99")],
            &[(5, 2)],
            &[
                (1690, "%s value is out of range in '%s'"),
                (1690, "DECIMAL value is out of range in '(5, 2)'"),
            ],
        ),
        (
            "d IN ('1.25','2.75'),d='1.25'",
            &[Some("1"), Some("1")],
            &[],
            &[],
        ),
    ];
    // DOUBLE keeps Rust f64 Display before decimal_prefix; FLOAT follows
    // Datum::to_decimal's f32 narrowing and promoted-f64 shortest formatting.
    // The nearest f32 to 0.1 promotes to 0.10000000149011612, not f64's 0.1.
    // JSON uses the Other fallback, whose Converted event is intentionally
    // discarded: neither the JSON string suffix nor JSON null adds 1292.
    // The bounded 1e300 parse reports its raw overflow BEFORE the target
    // precision clamp's formatted overflow. Do not merge these two events.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(projection, expected, shapes, warnings) in &cases {
            let sql = format!("SELECT {projection} FROM native_decimal_cast_policy_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected ordinary DECIMAL cast-policy rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}");
            assert_eq!(rows.len(), 1, "{sql}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}");
            for (index, expected_text) in expected.iter().enumerate() {
                let field = &columns[index].1;
                assert!(!field.is_unsigned());
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
                if shapes.is_empty() {
                    // Decimal-column versus strict string constants selects
                    // WrapWithCastAsDecimal. Its build-time unspecified-scale
                    // conversion must retain 1.25 before fixing literal shape.
                    // This is not a new runtime unspecified-scale SQL syntax.
                    assert_eq!(field.code(), FieldTypeCode::LongLong);
                    assert_eq!(rows[0][index], Datum::Int(1), "{sql}/{vectorized}");
                    continue;
                }
                let shape = shapes[index];
                assert_eq!(field.code(), FieldTypeCode::NewDecimal);
                assert_eq!((field.flen(), field.decimal()), shape, "{sql}/{index}");
                if let Some(expected_text) = expected_text {
                    let Datum::Decimal(decimal) = &rows[0][index] else {
                        panic!("changed DECIMAL cast carrier: {sql}/{index}")
                    };
                    assert_eq!(
                        decimal.to_string(),
                        *expected_text,
                        "{sql}/{vectorized}/{index}"
                    );
                    assert_eq!(decimal.scale(), shape.1 as u32);
                    assert_eq!(decimal.declared_shape(), Some(shape));
                } else {
                    assert_eq!(rows[0][index], Datum::Null, "{sql}/{vectorized}/{index}");
                }
            }
            assert_eq!(
                warnings_of(&session),
                warnings
                    .iter()
                    .map(|&(code, message)| (code, message.to_owned()))
                    .collect::<Vec<_>>(),
                "{sql}/{vectorized}"
            );
        }
    }
}

#[test]
fn native_decimal_digit_parsing_preserves_sql_cast_diagnostics_scale_and_storage() {
    use tidb_datatype::FieldTypeCode;

    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_decimal_digits_sql (leading_text VARCHAR(32), negative_zero VARCHAR(32), positive_exponent VARCHAR(32), negative_exponent VARCHAR(32), prefix_text VARCHAR(32), invalid_text VARCHAR(32), round_text VARCHAR(32), overflow_text VARCHAR(32), stored_d DECIMAL(12,4), newline_text VARCHAR(32), tab_text VARCHAR(32))")
        .unwrap();
    session
        .run("INSERT INTO native_decimal_digits_sql VALUES ('  +00012.3400','-000.000','1.25e3','1.25e-2',' 12.50tail ','oops','9.995','123456','0000012.3400','\n1.25','\t1.25')")
        .unwrap();
    assert!(warnings_of(&session).is_empty());
    // String CAST sources call Decimal::parse_mysql for their diagnostic
    // and value, then retain the existing precision/scale production policy.
    // REAL's separate native decimal_prefix pre-parser is not exercised or
    // claimed migrated. This is not a new C4/Head or whole-CAST-family gate.
    let cases: [(&str, &[(&str, i64, i64)], &[(u16, &str)], bool); 5] = [
        (
            "CAST(leading_text AS DECIMAL(10,4)),CAST(negative_zero AS DECIMAL(6,3)),CAST(positive_exponent AS DECIMAL(12,3)),CAST(negative_exponent AS DECIMAL(12,5))",
            &[("12.3400", 10, 4), ("0.000", 6, 3), ("1250.000", 12, 3), ("0.01250", 12, 5)],
            &[],
            true,
        ),
        (
            "CAST(prefix_text AS DECIMAL(8,2)),CAST(invalid_text AS DECIMAL(8,2))",
            &[("12.50", 8, 2), ("0.00", 8, 2)],
            &[
                (1292, "Truncated incorrect DECIMAL value: '12.50tail'"),
                (1292, "Truncated incorrect DECIMAL value: 'oops'"),
            ],
            true,
        ),
        (
            "CAST(round_text AS DECIMAL(5,2)),CAST(overflow_text AS DECIMAL(5,2))",
            &[("10.00", 5, 2), ("999.99", 5, 2)],
            &[
                (1292, "Truncated incorrect DECIMAL value: '9.995'"),
                (1690, "DECIMAL value is out of range in '(5, 2)'"),
            ],
            true,
        ),
        (
            "stored_d,stored_d+0.0060,00012.3400",
            &[("12.3400", 12, 4), ("12.3460", 13, 4), ("12.3400", 8, 4)],
            &[],
            false,
        ),
        (
            "CAST(newline_text AS DECIMAL(8,2)),CAST(tab_text AS DECIMAL(8,2))",
            &[("0.00", 8, 2), ("1.25", 8, 2)],
            &[],
            true,
        ),
    ];
    // Five projections in each executor mode: ten SELECTs, no extreme
    // exponent allocation, no new admission or fixed-word-parser claim.
    // Stored-column scale and literal normalization remain observable:
    // decimal_literal_type uses rendered length + 1, hence 12.3400 has
    // metadata (8,4), not a width based on its leading zeroes.
    // Preserve the two parses: report_decimal_input_truncation uses trim(),
    // but actual parse_mysql trims only spaces/tabs. A leading LF therefore
    // produces zero WITHOUT a warning; TAB produces 1.25. Reusing the first
    // parse's value for actual conversion would silently change this rule.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(projection, expected, warnings, explicit_casts) in &cases {
            let sql = format!("SELECT {projection} FROM native_decimal_digits_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected decimal digit-parser consumer rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len(), "{sql}/{vectorized}");
            assert_eq!(rows.len(), 1, "{sql}/{vectorized}");
            assert_eq!(rows[0].len(), expected.len(), "{sql}/{vectorized}");
            for (index, &(text, flen, scale)) in expected.iter().enumerate() {
                let field = &columns[index].1;
                assert_eq!(field.code(), FieldTypeCode::NewDecimal, "{sql}/{index}");
                assert_eq!(
                    (field.flen(), field.decimal()),
                    (flen, scale),
                    "{sql}/{index}"
                );
                assert!(!field.is_unsigned());
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
                let Datum::Decimal(decimal) = &rows[0][index] else {
                    panic!("decimal parser changed the SQL value carrier: {sql}/{index}")
                };
                assert_eq!(decimal.to_string(), text, "{sql}/{vectorized}/{index}");
                assert_eq!(decimal.scale(), scale as u32, "{sql}/{index}");
                if explicit_casts {
                    assert_eq!(
                        decimal.declared_shape(),
                        Some((flen, scale)),
                        "{sql}/{index}"
                    );
                }
            }
            assert_eq!(
                warnings_of(&session),
                warnings
                    .iter()
                    .map(|&(code, message)| (code, message.to_owned()))
                    .collect::<Vec<_>>(),
                "{sql}/{vectorized}"
            );
        }
    }
}

#[test]
fn native_in_policy_preserves_sql_numeric_cache_prepared_and_row_rewrite_paths() {
    use tidb_datatype::FieldTypeCode;

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
        .unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_in_policy_sql (i BIGINT, i_eq BIGINT, i_miss BIGINT, null_i BIGINT, r DOUBLE, r_eq DOUBLE, r_miss DOUBLE, null_r DOUBLE, d DECIMAL(8,2), d_other DECIMAL(8,2), d_later DECIMAL(8,2), s VARCHAR(16) COLLATE utf8mb4_general_ci, sn VARCHAR(16) COLLATE utf8mb4_general_ci, bad_text VARCHAR(16))")
        .unwrap();
    session
        .run("INSERT INTO native_in_policy_sql VALUES (2,2,9,NULL,2.5,2.5,7.5,NULL,1.50,2.50,3.50,'alpha',NULL,'bad')")
        .unwrap();
    let plan = row_text(session.run("EXPLAIN SELECT i IN (i_eq,i_miss),s IN ('ALPHA','beta'),r IN (r_eq,bad_text),(i,r) IN ((i_eq,r_eq),(i_miss,r_miss)) FROM native_in_policy_sql"))
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_lowercase();
    assert!(
        plan.matches("in(").count() >= 3,
        "numeric, string-cache and late-cast expressions must retain IN: {plan}"
    );
    assert!(
        plan.contains("eq(") && plan.contains("or("),
        "SQL row IN is deliberately equality/OR rewrite coverage: {plan}"
    );
    // This pure control-plane migration keeps the original Eq workers and
    // facade demand. A strict string-cache hit can require no Eq worker at
    // all, so there is deliberately no novel Head or zero-slot assertion.
    let cases: [(&str, &[Option<i64>], bool, bool); 4] = [
        (
            "i IN (i_eq,i_miss),r IN (r_eq,r_miss),d IN (d_other,d_later),i IN (i_miss,null_i)",
            &[Some(1), Some(1), Some(0), None],
            false,
            false,
        ),
        (
            "s IN ('ALPHA','beta'),s IN ('alpha ','beta'),s IN ('omega',NULL),sn IN ('ALPHA','beta')",
            &[Some(1), Some(1), None, None],
            false,
            false,
        ),
        (
            "(i,r) IN ((i_eq,r_eq),(i_miss,r_miss)),(i,r) IN ((i_eq,r_miss),(i_miss,r_eq)),(i,r) IN ((i_eq,null_r),(i_miss,r_eq))",
            &[Some(1), Some(0), None],
            false,
            true,
        ),
        (
            "r IN (r_eq,bad_text)",
            &[Some(1)],
            true,
            false,
        ),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(projection, expected, warns, row_rewrite) in &cases {
            let sql = format!("SELECT {projection} FROM native_in_policy_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected remaining-IN SQL rows: {sql}")
            };
            assert_eq!(columns.len(), expected.len());
            assert_eq!(
                rows,
                vec![expected
                    .iter()
                    .map(|value| value.map_or(Datum::Null, Datum::Int))
                    .collect::<Vec<_>>()],
                "{sql}/{vectorized}"
            );
            for (_, field) in &columns {
                assert_eq!(field.code(), FieldTypeCode::LongLong);
                assert!(!field.is_unsigned());
                if !row_rewrite {
                    assert_eq!((field.flen(), field.decimal()), (1, 0));
                }
            }
            // The generic IN cursor still visits a later candidate after a
            // match. SQL's generated real cast owns this warning; it is not
            // evidence that a raw mixed-datum comparer emitted the warning.
            let expected_warnings = if warns {
                vec![(1292, "Truncated incorrect DOUBLE value: 'bad'".to_owned())]
            } else {
                Vec::new()
            };
            assert_eq!(
                warnings_of(&session),
                expected_warnings,
                "{sql}/{vectorized}"
            );
            // The row projections above test SQL Eq/AND/OR rewriting only,
            // not the manually constructed AST-row IN SDK entrypoint.
        }
    }

    // Prepare ONCE and execute the same retained statement three times. The
    // immutable 'fixed' key and has-NULL state coexist with a dynamic marker;
    // rebinding must not turn its first value into a permanent cached key.
    // This proves named prepared-statement reuse, not a physical-plan cache
    // hit or private hash-table length/capacity (covered by original tests).
    session
        .run("PREPARE native_in_rebind FROM 'SELECT s IN (''fixed'', ?, NULL) FROM native_in_policy_sql'")
        .unwrap();
    for (binding, expected) in [
        ("SET @native_in_key='ALPHA'", Some(1)),
        ("SET @native_in_key='omega'", None),
        ("SET @native_in_key='alpha '", Some(1)),
    ] {
        session.run(binding).unwrap();
        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("EXECUTE native_in_rebind USING @native_in_key")
            .unwrap()
        else {
            panic!("expected reused prepared IN rows")
        };
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].1.code(), FieldTypeCode::LongLong);
        assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (1, 0));
        assert_eq!(
            rows,
            vec![vec![expected.map_or(Datum::Null, Datum::Int)]],
            "{binding}"
        );
        // Any plan-cache eligibility diagnostic belongs to PREPARE/EXECUTE,
        // not this IN policy. String membership itself must not truncate.
        assert!(
            warnings_of(&session).iter().all(|(code, _)| *code != 1292),
            "{binding}"
        );
    }
    session.run("DEALLOCATE PREPARE native_in_rebind").unwrap();
}









#[test]
fn native_extremum_policy_preserves_numeric_winner_scale_promotion_and_global_null() {
    use tidb_datatype::FieldTypeCode;

    // Pure head/domain/winner policy consumers, not a new C4 runtime family.
    // Keep the native reducers and their SQL type/materialization contracts;
    // no zero-slot claim and no manufactured NaN SQL input belong here.
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_extremum_policy_sql (i BIGINT, d DECIMAL(10,3), u BIGINT UNSIGNED, r DOUBLE, n BIGINT)")
        .unwrap();
    session
        .run(
            "INSERT INTO native_extremum_policy_sql VALUES (-5,2.500,9223372036854775808,150,NULL)",
        )
        .unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("SELECT LEAST(i,d),GREATEST(i,d),GREATEST(u,i),LEAST(u,i),LEAST(r,d,i) FROM native_extremum_policy_sql")
            .unwrap()
        else {
            panic!("expected stored numeric extrema")
        };
        assert_eq!(columns.len(), 5);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 5);
        // Nonconstant decimal winners keep the winning argument's OWN scale,
        // although both i/d result headers aggregate to DECIMAL(23,3).
        // The mixed-sign BIGINT pair promotes to DECIMAL(21,0), preserving
        // the exact unsigned value rather than rounding it through f64.
        for (index, text, scale, flen, decimal) in [
            (0, "-5", 0, 23, 3),
            (1, "2.500", 3, 23, 3),
            (2, "9223372036854775808", 0, 21, 0),
            (3, "-5", 0, 21, 0),
        ] {
            let field = &columns[index].1;
            assert_eq!(field.code(), FieldTypeCode::NewDecimal);
            assert_eq!((field.flen(), field.decimal()), (flen, decimal));
            let Datum::Decimal(value) = &rows[0][index] else {
                panic!("expected decimal field {index}/mode {vectorized}")
            };
            assert_eq!(value.scale(), scale, "field {index}/mode {vectorized}");
            assert_eq!(value.to_string(), text, "field {index}/mode {vectorized}");
        }
        // The integer is the winner, but a REAL anywhere in the argument
        // list promotes the result. DOUBLE's declared width/scale is 22/-1.
        assert_eq!(columns[4].1.code(), FieldTypeCode::Double);
        assert_eq!((columns[4].1.flen(), columns[4].1.decimal()), (22, -1));
        assert!(matches!(&rows[0][4], Datum::Real(_)));
        assert_eq!(rows[0][4], Datum::Real(-5.0), "mode {vectorized}");
        for (_, field) in &columns {
            assert!(!field.is_unsigned());
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
        }
        assert!(warnings_of(&session).is_empty(), "mode {vectorized}");

        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("SELECT LEAST(1,2.5,3.25),GREATEST(3,2.55,1),GREATEST(i,d,n),LEAST(n,u,i) FROM native_extremum_policy_sql")
            .unwrap()
        else {
            panic!("expected constant-scale and global-NULL extrema")
        };
        assert_eq!(columns.len(), 4);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 4);
        // Fully constant calls instead use the maximum argument scale. The
        // literal type widths and merged scale give both headers 5/2, which
        // constant folding preserves rather than deriving from the winner.
        for (index, text) in [(0, "1.00"), (1, "3.00")] {
            let Datum::Decimal(value) = &rows[0][index] else {
                panic!("expected folded decimal field {index}/mode {vectorized}")
            };
            assert_eq!(value.scale(), 2);
            assert_eq!(value.to_string(), text, "field {index}/mode {vectorized}");
        }
        assert_eq!(rows[0][2], Datum::Null, "late NULL/mode {vectorized}");
        assert_eq!(rows[0][3], Datum::Null, "first NULL/mode {vectorized}");
        for (index, (flen, decimal)) in [(5, 2), (5, 2), (23, 3), (21, 0)].into_iter().enumerate() {
            let field = &columns[index].1;
            assert_eq!(field.code(), FieldTypeCode::NewDecimal);
            assert_eq!(
                (field.flen(), field.decimal()),
                (flen, decimal),
                "field {index}/mode {vectorized}"
            );
            assert!(!field.is_unsigned());
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
        }
        assert!(warnings_of(&session).is_empty(), "mode {vectorized}");
    }
}



#[test]
fn native_duration_helpers_preserve_stored_cast_and_extract_source_policy() {
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    // M0 shared parser/unit-extraction consumers only. EXTRACT still owns
    // its original native orchestration and source-type policy; there is no
    // new family/profile claim and deliberately no zero-slot root matrix.
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_duration_helpers_sql (round_text VARCHAR(32), bad_packed BIGINT, negative_time TIME(6), day_text VARCHAR(32), datetime_text VARCHAR(32), typed_datetime DATETIME)")
        .unwrap();
    session
        .run("INSERT INTO native_duration_helpers_sql VALUES ('12:59:59.9876',126060,'-25:03:04.123456','1 02:03:04','2024-03-15 02:03:04','2024-03-15 02:03:04')")
        .unwrap();
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("SELECT CAST(round_text AS TIME(3)), EXTRACT(HOUR FROM negative_time), EXTRACT(DAY_HOUR FROM day_text), EXTRACT(DAY_SECOND FROM datetime_text), EXTRACT(DAY_SECOND FROM typed_datetime) FROM native_duration_helpers_sql")
            .unwrap()
        else {
            panic!("expected duration helper consumer rows")
        };
        assert_eq!(columns.len(), 5, "mode {vectorized}");
        assert_eq!(rows.len(), 1, "mode {vectorized}");
        assert_eq!(rows[0].len(), 5, "mode {vectorized}");
        let time_type = &columns[0].1;
        assert_eq!(time_type.code(), FieldTypeCode::Duration);
        assert_eq!((time_type.flen(), time_type.decimal()), (14, 3));
        assert!(time_type.has_flag(FieldTypeFlags::BINARY));
        assert!(matches!(&rows[0][0], Datum::Duration(_)));
        assert_eq!(cell_text(&rows[0][0]), "12:59:59.988", "mode {vectorized}");
        // Typed TIME uses signed duration extraction. The mixed DAY_* text
        // route first parses a duration, preferring a positive-year datetime
        // only when its clock agrees. Typed DATETIME takes that route directly.
        for (index, expected) in [(1, -25), (2, 26), (3, 15_020_304), (4, 15_020_304)] {
            let field = &columns[index].1;
            assert_eq!(field.code(), FieldTypeCode::LongLong);
            assert_eq!((field.flen(), field.decimal()), (20, 0));
            assert!(!field.is_unsigned());
            assert_eq!(
                rows[0][index],
                Datum::Int(expected),
                "field {index}/mode {vectorized}"
            );
        }
        for (_, field) in &columns {
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
        }
        // RoundedToScale is successful CAST conversion, not a truncation
        // diagnostic. No mixed-source parse above is malformed or clamped.
        assert!(warnings_of(&session).is_empty(), "mode {vectorized}");

        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("SELECT CAST(bad_packed AS TIME) FROM native_duration_helpers_sql")
            .unwrap()
        else {
            panic!("expected invalid packed-duration rows")
        };
        assert_eq!(columns.len(), 1);
        let field = &columns[0].1;
        assert_eq!(field.code(), FieldTypeCode::Duration);
        assert_eq!((field.flen(), field.decimal()), (10, 0));
        assert!(field.has_flag(FieldTypeFlags::BINARY));
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation_name(), "binary");
        // The numeric CAST signature keeps its existing NULL-on-invalid
        // policy; it must not inherit a string-source retained-value policy.
        assert_eq!(rows, vec![vec![Datum::Null]], "mode {vectorized}");
        assert_eq!(
            warnings_of(&session),
            vec![(1292, "Truncated incorrect time value: '126060'".to_owned())],
            "mode {vectorized}"
        );
    }
}











#[test]
fn native_type_helpers_preserve_decimal_cast_values_and_float_diagnostics() {
    use tidb_datatype::FieldTypeCode;

    // M2 type-helper consumers only: no new C4 profile, zero-pool proof,
    // CAST/extrema family credit, or claim about the separate legacy Ryu
    // formatter. These cases use the existing normal runtime policy.
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE native_type_helpers_sql (d_positive DECIMAL(8,0), d_negative DECIMAL(8,0), d_carry DECIMAL(6,3), d_zero DECIMAL(3,0), d_pad DECIMAL(4,1), d_ordinary DECIMAL(7,2), f_small FLOAT, r_negative DOUBLE, r_huge DOUBLE)")
        .unwrap();
    session
        .run("INSERT INTO native_type_helpers_sql VALUES (123456,-123456,9.995,0,1.5,123.45,2.5,-1.5,1e300)")
        .unwrap();
    // cast.rs applies cast_to_precision and then report_decimal_production:
    // overflow suppresses truncation, whose text otherwise retains the
    // ORIGINAL decimal. No diagnostic is invented by the new helper.
    let decimal_cases = [
        (
            "CAST(d_positive AS DECIMAL(5,2))",
            (5, 2),
            "999.99",
            Some((1690, "DECIMAL value is out of range in '(5, 2)'")),
        ),
        (
            "CAST(d_negative AS DECIMAL(5,2))",
            (5, 2),
            "-999.99",
            Some((1690, "DECIMAL value is out of range in '(5, 2)'")),
        ),
        (
            "CAST(d_carry AS DECIMAL(5,2))",
            (5, 2),
            "10.00",
            Some((1292, "Truncated incorrect DECIMAL value: '9.995'")),
        ),
        (
            "CAST(d_carry AS DECIMAL(3,2))",
            (3, 2),
            "9.99",
            Some((1690, "DECIMAL value is out of range in '(3, 2)'")),
        ),
        ("CAST(d_zero AS DECIMAL(5,2))", (5, 2), "0.00", None),
        ("CAST(d_pad AS DECIMAL(8,4))", (8, 4), "1.5000", None),
        ("CAST(d_ordinary AS DECIMAL(8,2))", (8, 2), "123.45", None),
        // Float32's cast fallback calls Datum::to_decimal, then
        // MyDecimal::from_float64 -> format_float_g_shortest. Unlike the
        // DOUBLE cast's separate to_string arm, this is a value consumer of
        // the migrated formatter as well as the precision helper.
        ("CAST(f_small AS DECIMAL(5,2))", (5, 2), "2.50", None),
    ];
    let diagnostic_cases = [
        (
            "CAST(r_negative AS UNSIGNED)",
            true,
            "18446744073709551614",
            "constant -2 overflows bigint",
        ),
        (
            "CAST(r_huge AS SIGNED)",
            false,
            "9223372036854775807",
            "constant 1e+300 overflows bigint",
        ),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (expression, shape, expected, warning) in decimal_cases {
            let sql = format!("SELECT {expression} FROM native_type_helpers_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected decimal helper consumer rows: {sql}")
            };
            assert_eq!(columns.len(), 1, "{sql}/{vectorized}");
            let field = &columns[0].1;
            assert_eq!(
                field.code(),
                FieldTypeCode::NewDecimal,
                "{sql}/{vectorized}"
            );
            assert_eq!((field.flen(), field.decimal()), shape, "{sql}/{vectorized}");
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
            assert_eq!(rows.len(), 1, "{sql}/{vectorized}");
            assert_eq!(rows[0].len(), 1, "{sql}/{vectorized}");
            let Datum::Decimal(decimal) = &rows[0][0] else {
                panic!(
                    "decimal helper changed SQL carrier: {sql}: {:?}",
                    rows[0][0]
                )
            };
            assert_eq!(decimal.declared_shape(), Some(shape), "{sql}/{vectorized}");
            assert_eq!(decimal.to_string(), expected, "{sql}/{vectorized}");
            let expected_warnings = warning
                .map(|(code, message)| vec![(code, message.to_owned())])
                .unwrap_or_default();
            assert_eq!(
                warnings_of(&session),
                expected_warnings,
                "{sql}/{vectorized}"
            );
        }
        for (expression, unsigned, expected, warning) in diagnostic_cases {
            let sql = format!("SELECT {expression} FROM native_type_helpers_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected floating diagnostic consumer rows: {sql}")
            };
            assert_eq!(columns.len(), 1, "{sql}/{vectorized}");
            let field = &columns[0].1;
            assert_eq!(field.code(), FieldTypeCode::LongLong, "{sql}/{vectorized}");
            assert_eq!((field.flen(), field.decimal()), (20, 0));
            assert_eq!(field.is_unsigned(), unsigned);
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
            assert_eq!(rows.len(), 1, "{sql}/{vectorized}");
            assert_eq!(rows[0].len(), 1, "{sql}/{vectorized}");
            assert!(
                matches!(
                    (unsigned, &rows[0][0]),
                    (true, Datum::UInt(_)) | (false, Datum::Int(_))
                ),
                "unexpected diagnostic consumer carrier: {sql}: {:?}",
                rows[0][0]
            );
            assert_eq!(cell_text(&rows[0][0]), expected, "{sql}/{vectorized}");
            assert_eq!(
                warnings_of(&session),
                vec![(1690, warning.to_owned())],
                "{sql}/{vectorized}"
            );
        }
    }
}























#[test]
fn ready_value_temporal_literals_preserve_rewrite_folding_types_modes_and_zones() {
    use tidb_datatype::{FieldTypeCode, TimeType};

    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_literal_broadcast (s VARCHAR(32))")
        .unwrap();
    session
        .run("INSERT INTO shared_literal_broadcast VALUES ('2024-01-01'),('2024-01-02')")
        .unwrap();
    // These are rewrite-time constants broadcast over two rows, NOT dynamic
    // literal arguments or per-row worker admission. The original wrappers
    // still use NoColumns: this does not claim statement-owner/M6 closure.
    // Fixed values come from time_literal.rs's original tests and the existing
    // types/time.result fixture, not from a second implementation as oracle.
    let cases = [
        (
            "DATE '2024-01-01'",
            "UTC",
            "",
            "2024-01-01",
            FieldTypeCode::Date,
            10,
            0,
        ),
        (
            "TIMESTAMP '2024-01-01 14:00:00.010'",
            "UTC",
            "",
            "2024-01-01 14:00:00.010",
            FieldTypeCode::Datetime,
            23,
            3,
        ),
        (
            "{ts '2024-01-01 14:00:00+02:00'}",
            "America/Los_Angeles",
            "",
            "2024-01-01 04:00:00",
            FieldTypeCode::Datetime,
            19,
            0,
        ),
        (
            "TIMESTAMP '2011-03-13 01:59:59.9999999'",
            "America/Los_Angeles",
            "",
            "2011-03-13 03:00:00.000000",
            FieldTypeCode::Datetime,
            26,
            6,
        ),
        (
            "{ts '2011-11-06 01:59:59.9999999'}",
            "America/Los_Angeles",
            "",
            "2011-11-06 01:00:00.000000",
            FieldTypeCode::Datetime,
            26,
            6,
        ),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (literal, zone, modes, expected, code, flen, decimal) in cases {
            session.run(&format!("SET time_zone='{zone}'")).unwrap();
            session.run(&format!("SET sql_mode='{modes}'")).unwrap();
            let sql = format!("SELECT {literal} FROM shared_literal_broadcast");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected folded temporal literal rows: {sql}")
            };
            assert_eq!(columns.len(), 1, "{sql}/{vectorized}");
            assert_eq!(columns[0].1.code(), code, "{sql}/{vectorized}");
            assert_eq!(
                (columns[0].1.flen(), columns[0].1.decimal()),
                (flen, decimal),
                "{sql}/{vectorized}"
            );
            assert_eq!(rows.len(), 2, "{sql}/{vectorized}");
            for row in &rows {
                assert_eq!(row.len(), 1);
                assert_eq!(cell_text(&row[0]), expected, "{sql}/{vectorized}");
                let Datum::Time(time) = &row[0] else {
                    panic!("folded literal lost its native temporal cell: {sql}")
                };
                let kind = if code == FieldTypeCode::Date {
                    TimeType::Date
                } else {
                    TimeType::DateTime
                };
                assert_eq!(time.kind(), kind, "{sql}/{vectorized}");
                assert_eq!(i64::from(time.fsp()), decimal, "{sql}/{vectorized}");
            }
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }

        session.run("SET time_zone='UTC'").unwrap();
        // These literal failures are hard errors, unlike CAST's warning/NULL
        // policy. Keep the raw literal and the date/datetime diagnostic class.
        // Existing PlanScopeResolver forwards the zone but not date_modes(),
        // inheriting ColumnResolver's TIDB_DEFAULT_SQL_MODE (true,true,false).
        // The first three cases deliberately document that old table-projection
        // limitation, not Go mode parity. Direct root tests cover actual custom
        // mode bits; this migration must not silently repair planner forwarding.
        for (literal, modes, code, state, message) in [
            (
                "{d '2007-10-00'}",
                "NO_ZERO_DATE",
                1292,
                *b"22007",
                "Incorrect date value: '2007-10-00'",
            ),
            (
                "DATE '0000-00-00'",
                "NO_ZERO_IN_DATE",
                1292,
                *b"22007",
                "Incorrect date value: '0000-00-00'",
            ),
            (
                "{d '2017-2-31'}",
                "ALLOW_INVALID_DATES",
                1292,
                *b"22007",
                "Incorrect datetime value: '2017-2-31'",
            ),
            (
                "DATE '0000-00-00'",
                "NO_ZERO_DATE",
                1292,
                *b"22007",
                "Incorrect date value: '0000-00-00'",
            ),
            (
                "{d '2007-10-00'}",
                "NO_ZERO_IN_DATE",
                1292,
                *b"22007",
                "Incorrect date value: '2007-10-00'",
            ),
            (
                "DATE '2017-2-31'",
                "",
                1292,
                *b"22007",
                "Incorrect datetime value: '2017-2-31'",
            ),
            (
                "{d '2024-01-01 01:12:31'}",
                "",
                1292,
                *b"22007",
                "Incorrect date value: '2024-01-01 01:12:31'",
            ),
            (
                "TIMESTAMP '2024-01-01'",
                "",
                1525,
                *b"HY000",
                "Incorrect datetime value: '2024-01-01'",
            ),
            (
                "{ts '2024-01-01 14:00:00+14:01'}",
                "",
                1292,
                *b"22007",
                "Incorrect datetime value: '2024-01-01 14:00:00+14:01'",
            ),
        ] {
            session.run(&format!("SET sql_mode='{modes}'")).unwrap();
            let sql = format!("SELECT {literal} FROM shared_literal_broadcast");
            let mysql = session
                .run_with_columns(&sql)
                .expect_err(&sql)
                .to_mysql_error();
            assert_eq!(mysql.code, code, "{sql}/{vectorized}");
            assert_eq!(mysql.state, state, "{sql}/{vectorized}");
            assert_eq!(mysql.message, message, "{sql}/{vectorized}");
        }
        // ODBC's grammar accepts a full expression, but literal_text requires
        // a rewritten Constant. A column must not become a new runtime route.
        let sql = "SELECT {ts s} FROM shared_literal_broadcast";
        let mysql = session
            .run_with_columns(sql)
            .expect_err(sql)
            .to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}/{vectorized}");
        assert!(
            mysql
                .message
                .contains("a temporal literal whose argument is not constant"),
            "{sql}/{vectorized}: {}",
            mysql.message
        );
    }
}

#[test]
fn ready_value_json_search_preserves_native_patterns_paths_and_string_results() {
    use tidb_datatype::{Collation, FieldTypeCode};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
        .unwrap();
    session.run("SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_json_search_sql (doc_main VARCHAR(256), doc_escape VARCHAR(256), doc_keys VARCHAR(256), doc_unicode VARCHAR(64), doc_trailing VARCHAR(64), mode_one VARCHAR(8), mode_all VARCHAR(8), mode_bad VARCHAR(8), pattern_text VARCHAR(16), pattern_escaped VARCHAR(16), pattern_unicode_escape VARCHAR(16), pattern_unicode VARCHAR(16), pattern_x VARCHAR(4), pattern_slash VARCHAR(4), pattern_miss VARCHAR(8), esc_unicode VARCHAR(4), esc_empty VARCHAR(4), esc_null VARCHAR(4), path_star VARCHAR(32), path_wild VARCHAR(32), path_recursive VARCHAR(32), path_a VARCHAR(32), path_array VARCHAR(32), path_root VARCHAR(32), path_bad VARCHAR(32), null_text VARCHAR(64))").unwrap();
    // Raw SQL plus NO_BACKSLASH_ESCAPES makes the stored JSON and pattern
    // backslashes explicit, without a CHAR/CAST/function child in any probe.
    session.run(r#"INSERT INTO shared_json_search_sql VALUES ('["abc", [{"k":"10"}, "def"], {"x":"abc"}, {"y":"bcd"}]','["abc", [{"k":"10"}, "def"], {"x":"ab%d"}, {"y":"abcd"}]','{"*":"x","a":{"a":"x","b":"x"},"n":7,"t":true}','["é中","É中"]','["\\x"]','OnE','AlL','wrong','abc','ab\%d','ab中%d','é_','x','\','ghi','中','',NULL,'$."*"','$.*','$**.a','$.a','$[0].a','$','not_a_path',NULL)"#).unwrap();
    // Fixed native search.rs / original TestJSONSearch values. These are JSON
    // path TEXT cells, not Datum::Json and not another evaluator as an oracle.
    let cases = [
        ("doc_main,mode_one,pattern_text", Some(r#""$[0]""#)),
        (
            "doc_main,mode_all,pattern_text",
            Some(r#"["$[0]", "$[2].x"]"#),
        ),
        ("doc_escape,mode_all,pattern_escaped", Some(r#""$[2].x""#)),
        (
            "doc_escape,mode_all,pattern_escaped,esc_null",
            Some(r#""$[2].x""#),
        ),
        (
            "doc_escape,mode_all,pattern_escaped,esc_empty",
            Some(r#""$[2].x""#),
        ),
        (
            "doc_escape,mode_all,pattern_unicode_escape,esc_unicode",
            Some(r#""$[2].x""#),
        ),
        // Unicode scalar '_' and case-sensitive matching, despite a CI
        // connection collation. JSON_SEARCH never asks the LIKE collation.
        ("doc_unicode,mode_all,pattern_unicode", Some(r#""$[0]""#)),
        (
            "doc_keys,mode_all,pattern_x,esc_null,path_star",
            Some(r#""$.\"*\"""#),
        ),
        (
            "doc_keys,mode_all,pattern_x,esc_null,path_wild",
            Some(r#"["$.\"*\"", "$.a.a", "$.a.b"]"#),
        ),
        (
            "doc_keys,mode_all,pattern_x,esc_null,path_recursive,path_a",
            Some(r#"["$.a.a", "$.a.b"]"#),
        ),
        ("doc_keys,mode_all,pattern_x,esc_null,path_array", None),
        // Preserve the native trailing-escape behavior: it matches the
        // current escape character without requiring the remaining 'x' end.
        ("doc_trailing,mode_all,pattern_slash", Some(r#""$[0]""#)),
        ("doc_main,mode_all,pattern_miss", None),
        ("null_text,mode_all,pattern_text", None),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (args, expected) in cases {
            let sql = format!("SELECT JSON_SEARCH({args}) FROM shared_json_search_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected JSON_SEARCH rows: {sql}")
            };
            let expected = expected
                .map(|text| Datum::new_collation_string(text, Collation::Utf8Mb4GeneralCi))
                .unwrap_or(Datum::Null);
            assert_eq!(rows, vec![vec![expected]], "{sql}/{vectorized}");
            assert_eq!(columns.len(), 1);
            let field = &columns[0].1;
            assert_eq!(field.code(), FieldTypeCode::VarString);
            // text() uses FieldType::new(VarString), whose noninteger
            // dimensions remain unspecified; the separate default lookup
            // is not applied by that constructor.
            assert_eq!((field.flen(), field.decimal()), (-1, -1));
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation_name(), "utf8mb4_general_ci");
            assert!(warnings_of(&session).is_empty(), "{sql}/{vectorized}");
        }
    }
    // Mode validation uses native 3150, not JSON_CONTAINS_PATH's 3154.
    // Every path is parsed before one-mode can stop at its first matching leaf.
    // The existing EXEC-tier mapper uses 1105 for column-sourced InvalidPath;
    // the nominal 3143 belongs to the separate planning/folding conversion.
    for (args, code) in [
        ("doc_main,mode_bad,pattern_text", 3150),
        (
            "doc_main,mode_one,pattern_text,esc_null,path_root,path_bad",
            1105,
        ),
    ] {
        let sql = format!("SELECT JSON_SEARCH({args}) FROM shared_json_search_sql");
        let mysql = session
            .run_with_columns(&sql)
            .expect_err(&sql)
            .to_mysql_error();
        assert_eq!(mysql.code, code, "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
    }
}



#[test]
fn ready_value_timestampadd_preserves_calendar_rounding_nulls_and_diagnostics() {
    use tidb_datatype::{Collation, FieldTypeCode};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_timestampadd_sql (one BIGINT, tiny_amount DECIMAL(12,10), half_amount DECIMAL(2,1), null_amount BIGINT, jan DATETIME, leap_day DATETIME, base DATETIME, zero_date DATETIME, max_date DATETIME)").unwrap();
    session.run("INSERT INTO shared_timestampadd_sql VALUES (1,0.0000099999,1.5,NULL,'2024-01-31 00:00:00','2020-02-29 00:00:00','1995-05-01 00:00:00','0000-00-00 00:00:00','9999-12-31 23:59:59')").unwrap();
    // Bare units are syntax, not column expressions. Already-typed DATETIME
    // operands pass through the argument cast layer, preserving the leaf's
    // own invalid-base diagnostic rather than substituting a VARCHAR cast.
    let cases = [
        ("MONTH,one,jan", Some("2024-02-29 00:00:00"), None),
        ("QUARTER,one,jan", Some("2024-05-01 00:00:00"), None),
        ("YEAR,one,leap_day", Some("2021-03-01 00:00:00"), None),
        (
            "SECOND,tiny_amount,base",
            Some("1995-05-01 00:00:00.000009"),
            None,
        ),
        ("MINUTE,half_amount,base", Some("1995-05-01 00:02:00"), None),
        ("DAY,null_amount,base", None, None),
        // The grammar admits DAY_SECOND, but the worker does not implement
        // it. Invalid date validation precedes that unknown-unit refusal.
        (
            "DAY_SECOND,one,zero_date",
            None,
            Some("Incorrect datetime value: '0000-00-00 00:00:00'"),
        ),
        // Original day-number inversion reaches year 10000 here, not its
        // later all-zero sentinel. The diagnostic preserves these raw fields.
        (
            "SECOND,one,max_date",
            None,
            Some("Incorrect time value: '{10000 1 1 0 0 0 0}'"),
        ),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (args, expected, warning) in cases {
            let sql = format!("SELECT TIMESTAMPADD({args}) FROM shared_timestampadd_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected TIMESTAMPADD rows: {sql}")
            };
            let expected = expected
                .map(|text| Datum::new_collation_string(text, Collation::Utf8Mb4Bin))
                .unwrap_or(Datum::Null);
            assert_eq!(rows, vec![vec![expected]], "{sql}/{vectorized}");
            assert_eq!(columns.len(), 1);
            let field = &columns[0].1;
            assert_eq!(field.code(), FieldTypeCode::VarString);
            // text() uses FieldType::new(VarString), leaving both unspecified;
            // the separate default-length lookup is not applied here.
            assert_eq!((field.flen(), field.decimal()), (-1, -1));
            assert_eq!(field.charset_name(), "utf8mb4");
            assert_eq!(field.collation_name(), "utf8mb4_bin");
            let expected_warnings = warning
                .map(|message| vec![(1292, message.to_owned())])
                .unwrap_or_default();
            assert_eq!(
                warnings_of(&session),
                expected_warnings,
                "{sql}/{vectorized}"
            );
        }
    }
    let error = session
        .run_with_columns("SELECT TIMESTAMPADD(DAY_SECOND,one,base) FROM shared_timestampadd_sql")
        .expect_err("unsupported unit after a valid date");
    assert!(matches!(
        error,
        DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Unsupported("TIMESTAMPADD unit")
        ))
    ));
}



#[test]
fn ready_value_addtime_subtime_preserve_static_kinds_fsp_and_constant_row_split() {
    use tidb_datatype::{Collation, FieldTypeCode, MySqlDuration, Time, TimeType};

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_add_sub_time_sql (dt DATETIME(3), date_val DATE, delta TIME(6), dur TIME(6), delta_text VARCHAR(40), dur_delta VARCHAR(40), s_dur VARCHAR(40), s_delta VARCHAR(40), s_dt VARCHAR(40), bad VARCHAR(40), n VARCHAR(40))").unwrap();
    session.run("INSERT INTO shared_add_sub_time_sql VALUES ('2024-11-01 00:00:00.000','2024-11-01','12:00:01.341300','03:00:00.999999','12:00:01.341300','02:00:00.999998','01:00:00.000001','02:00:00.000001','2020-01-01 10:00:00','xxcvadfgasd',NULL)").unwrap();
    let text = |value: &str| Datum::new_collation_string(value, Collation::Utf8Mb4Bin);
    // DATETIME + DURATION's column body preserves the left value FSP (3),
    // although the declared result metadata takes max(3, 6). The existing
    // postcast infers FSP from the computed text; chunk Time cells retain it.
    let cases = [
        (
            "dt,delta",
            vec![
                Datum::Time(
                    Time::from_date_checked(2024, 11, 1, 12, 0, 1, 341_000, TimeType::DateTime, 3)
                        .unwrap(),
                ),
                Datum::Time(
                    Time::from_date_checked(
                        2024,
                        10,
                        31,
                        11,
                        59,
                        58,
                        658_000,
                        TimeType::DateTime,
                        3,
                    )
                    .unwrap(),
                ),
            ],
            FieldTypeCode::Datetime,
            26,
            6,
            None,
        ),
        (
            "date_val,delta_text",
            vec![
                text("2024-11-01 12:00:01.341300"),
                text("2024-10-31 11:59:58.658700"),
            ],
            FieldTypeCode::String,
            26,
            -1,
            None,
        ),
        (
            "dur,dur_delta",
            vec![
                Datum::Duration(MySqlDuration::from_nanoseconds(18_001_999_997_000, 6).unwrap()),
                Datum::Duration(MySqlDuration::from_nanoseconds(3_600_000_001_000, 6).unwrap()),
            ],
            FieldTypeCode::Duration,
            17,
            6,
            None,
        ),
        (
            "s_dur,s_delta",
            vec![text("03:00:00.000002"), text("-01:00:00")],
            FieldTypeCode::String,
            26,
            -1,
            None,
        ),
        (
            "s_dt,s_dt",
            vec![text("2020-01-01 20:00:00"), text("2020-01-01 00:00:00")],
            FieldTypeCode::String,
            26,
            -1,
            None,
        ),
        // The static DATETIME right argument wins even over a malformed left.
        (
            "bad,dt",
            vec![Datum::Null, Datum::Null],
            FieldTypeCode::String,
            26,
            -1,
            None,
        ),
        (
            "s_dur,bad",
            vec![Datum::Null, Datum::Null],
            FieldTypeCode::String,
            26,
            -1,
            Some("Truncated incorrect time value: 'xxcvadfgasd'"),
        ),
        (
            "n,s_delta",
            vec![Datum::Null, Datum::Null],
            FieldTypeCode::String,
            26,
            -1,
            None,
        ),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for (args, expected, code, flen, decimal, warning) in &cases {
            let sql =
                format!("SELECT ADDTIME({args}),SUBTIME({args}) FROM shared_add_sub_time_sql");
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected ADDTIME/SUBTIME rows: {sql}")
            };
            assert_eq!(rows, vec![expected.clone()], "{sql}/{vectorized}");
            assert_eq!(columns.len(), 2);
            for (_, field) in &columns {
                assert_eq!(field.code(), *code, "{sql}");
                assert_eq!((field.flen(), field.decimal()), (*flen, *decimal), "{sql}");
                if *code == FieldTypeCode::String {
                    assert_eq!(field.charset_name(), "utf8mb4");
                    assert_eq!(field.collation_name(), "utf8mb4_bin");
                } else {
                    assert_eq!(field.charset_name(), "binary");
                    assert_eq!(field.collation_name(), "binary");
                }
            }
            let expected_warnings = warning
                .map(|message| vec![(1292, message.to_owned()), (1292, message.to_owned())])
                .unwrap_or_default();
            assert_eq!(
                warnings_of(&session),
                expected_warnings,
                "{sql}/{vectorized}"
            );
        }
        // row_path means all-constant arguments, NOT the session vector flag.
        // Only constant ADDTIME has the trailing-dash guard; SUBTIME does not.
        let StmtOutput::Rows { rows, .. } = session.run_with_columns("SELECT ADDTIME('2020-01-01 10:00:00','2020-01-01 10:00:00'),SUBTIME('2020-01-01 10:00:00','2020-01-01 10:00:00')").unwrap() else {
            panic!("expected constant ADDTIME/SUBTIME rows")
        };
        assert_eq!(
            rows,
            vec![vec![Datum::Null, text("2020-01-01 00:00:00")]],
            "vectorized={vectorized}"
        );
        assert!(warnings_of(&session).is_empty());
    }
}



#[test]
fn ready_value_time_microsecond_preserve_sql_duration_shapes_and_parse_diagnostics() {
    use tidb_datatype::{FieldTypeCode, MySqlDuration};

    let mut session = Session::new();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_time_microsecond_sql (good_time TIME(6), negative_time TIME(6), day_text VARCHAR(40), compact_text VARCHAR(40), bad_text VARCHAR(40), tail_text VARCHAR(40), over_text VARCHAR(40), null_text VARCHAR(40))").unwrap();
    session.run("INSERT INTO shared_time_microsecond_sql VALUES ('12:34:56.123456','-00:00:00.123456','1 12:34:56.123456','20171231235959.9999999','2011-11-11 10:10:10.11.12','12:34:56tail','839:00:00',NULL)").unwrap();

    // TIME's native leaf returns text, but the SQL scalar boundary converts it
    // to the declared Duration type. TIME(6) columns retain six fraction digits;
    // VARCHAR columns declare decimal=0, so their TIME result is rounded to FSP0.
    // MICROSECOND reads the original parsed fraction, including its positive
    // magnitude for a negative duration. Compact datetime rounding carries into
    // 2018-01-01 before the clock fields are extracted, hence midnight here.
    let cases = [
        (
            "good_time",
            Some(45_296_123_456_000_i64),
            6_i64,
            Some(123_456_i64),
            None,
        ),
        ("negative_time", Some(-123_456_000), 6, Some(123_456), None),
        (
            "day_text",
            Some(131_696_000_000_000),
            0,
            Some(123_456),
            None,
        ),
        ("compact_text", Some(0), 0, Some(0), None),
        (
            "bad_text",
            Some(0),
            0,
            None,
            Some("Truncated incorrect time value: '2011-11-11 10:10:10.11.12'"),
        ),
        (
            "tail_text",
            Some(0),
            0,
            None,
            Some("Truncated incorrect time value: '12:34:56tail'"),
        ),
        (
            "over_text",
            Some(0),
            0,
            None,
            Some("Truncated incorrect time value: '839:00:00'"),
        ),
        ("null_text", None, 0, None, None),
    ];
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        for &(column, nanos, fsp, micros, warning) in &cases {
            let sql = format!(
                "SELECT TIME({column}),MICROSECOND({column}) FROM shared_time_microsecond_sql"
            );
            let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
                panic!("expected TIME/MICROSECOND rows: {sql}")
            };
            let expected_time = nanos
                .map(|value| Datum::Duration(MySqlDuration::from_nanoseconds(value, fsp).unwrap()))
                .unwrap_or(Datum::Null);
            let expected_micros = micros.map(Datum::Int).unwrap_or(Datum::Null);
            assert_eq!(
                rows,
                vec![vec![expected_time, expected_micros]],
                "{sql}/{vectorized}"
            );
            assert_eq!(columns.len(), 2);
            let time_type = &columns[0].1;
            assert_eq!(time_type.code(), FieldTypeCode::Duration);
            assert_eq!(
                (time_type.flen(), time_type.decimal()),
                (if fsp == 6 { 17 } else { 10 }, fsp)
            );
            assert!(time_type.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
            let micro_type = &columns[1].1;
            assert_eq!(micro_type.code(), FieldTypeCode::LongLong);
            assert_eq!((micro_type.flen(), micro_type.decimal()), (20, 0));
            assert!(!micro_type.is_unsigned());
            for (_, field) in &columns {
                assert_eq!(field.charset_name(), "binary");
                assert_eq!(field.collation_name(), "binary");
            }
            // TIME contributes exactly one 1292 for a parse error; MICROSECOND
            // suppresses that error and contributes no second diagnostic.
            let expected_warnings = warning
                .map(|message| vec![(1292, message.to_owned())])
                .unwrap_or_default();
            assert_eq!(
                warnings_of(&session),
                expected_warnings,
                "{sql}/{vectorized}"
            );
        }
    }
}



#[test]
fn ready_value_decimal_div_preserves_fast_bounded_unsigned_and_null_values() {
    let mut session = Session::new();
    session
        .run("SET sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'")
        .unwrap();
    session.run("SET div_precision_increment=4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_decimal_div_sql (f DECIMAL(10,2), fd DECIMAL(10,2), b DECIMAL(20,4), bd DECIMAL(20,4), u DECIMAL(22,2) UNSIGNED, ud DECIMAL(22,2), neg DECIMAL(10,2), eleven DECIMAL(10,2), small_neg DECIMAL(10,2), ueleven DECIMAL(10,2) UNSIGNED, n DECIMAL(10,2), z DECIMAL(10,2))").unwrap();
    session.run("INSERT INTO shared_decimal_div_sql VALUES (11.01,1.10,0.3000,0.1000,18446744073709551615.00,1.50,-13.00,11.00,-1.00,11.00,NULL,0.00)").unwrap();

    // Matching storage scales 2 fit the existing i128 fast policy. Scale 4
    // deliberately selects bounded DecimalDiv instead; both operands are
    // already DECIMAL columns, so no other arithmetic/cast worker intervenes.
    // These expected integers are literal source vectors, not another DIV call.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns("SELECT f DIV fd,b DIV bd,u DIV ud,neg DIV eleven,small_neg DIV ueleven,n DIV fd,f DIV n,f DIV z FROM shared_decimal_div_sql").unwrap() else {
            panic!("expected decimal DIV rows")
        };
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(10),
                Datum::Int(3),
                Datum::UInt(12_297_829_382_473_034_410),
                Datum::Int(-1),
                Datum::UInt(0),
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]],
            "vectorized={vectorized}"
        );
        assert_eq!(columns.len(), 8);
        for ((_, field), unsigned) in columns
            .iter()
            .zip([false, false, true, false, true, false, false, false])
        {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert_eq!((field.flen(), field.decimal()), (20, 0));
            assert_eq!(field.is_unsigned(), unsigned);
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
        }
        // SELECT retains its warning policy even under this strict SQL mode;
        // only the non-NULL zero divisor warns, not either NULL operand case.
        assert_eq!(
            warnings_of(&session),
            vec![(1365, "Division by 0".to_owned())],
            "vectorized={vectorized}"
        );
    }
}



#[test]
fn ready_value_integer_div_preserves_signedness_nulls_and_query_diagnostics() {
    let mut session = Session::new();
    session
        .run("SET sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'")
        .unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_integer_div_sql (a BIGINT, b BIGINT, u BIGINT UNSIGNED, v BIGINT UNSIGNED, umax BIGINT UNSIGNED, uone BIGINT UNSIGNED, neg BIGINT, neg_one BIGINT, neg_two BIGINT, n BIGINT, z BIGINT, min_i BIGINT)").unwrap();
    session.run("INSERT INTO shared_integer_div_sql VALUES (13,11,13,11,18446744073709551615,1,-13,-1,-2,NULL,0,-9223372036854775808)").unwrap();

    // Only the existing integer DIV slice: SS/US/SU/UU, including present
    // UINT64_MAX bits and negative quotients truncated toward zero. Decimal DIV
    // remains a separate native path and is not claimed by these SQL probes.
    for vectorized in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_vectorized_expression={vectorized}"
            ))
            .unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns("SELECT a DIV b,u DIV b,a DIV v,umax DIV uone,neg DIV b,uone DIV neg_two,neg_one DIV v,n DIV b,a DIV n,a DIV z FROM shared_integer_div_sql").unwrap() else {
            panic!("expected integer DIV rows")
        };
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(1),
                Datum::UInt(1),
                Datum::UInt(1),
                Datum::UInt(18_446_744_073_709_551_615),
                Datum::Int(-1),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]],
            "vectorized={vectorized}"
        );
        assert_eq!(columns.len(), 10);
        for ((_, field), unsigned) in columns.iter().zip([
            false, true, true, true, false, true, true, false, false, false,
        ]) {
            assert_eq!(field.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert_eq!((field.flen(), field.decimal()), (20, 0));
            assert_eq!(field.is_unsigned(), unsigned);
            assert_eq!(field.charset_name(), "binary");
            assert_eq!(field.collation_name(), "binary");
        }
        // A SELECT warns even in strict mode. NULL operands do not add a
        // diagnostic; only the final, non-NULL dividend / zero divisor does.
        assert_eq!(
            warnings_of(&session),
            vec![(1365, "Division by 0".to_owned())],
            "vectorized={vectorized}"
        );
    }

    // One ordered diagnostic witness, with direct operands and one row. The
    // integer overflow caller renders an operand tuple, not decimal DIV text.
    let error = session
        .run_with_columns("SELECT a DIV z,min_i DIV neg_one FROM shared_integer_div_sql")
        .expect_err("signed DIV overflow");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1690);
    assert_eq!(
        mysql.message,
        "BIGINT value is out of range in '(-9223372036854775808, -1)'"
    );
    assert!(mysql.is_from_evaluation());
    assert_eq!(
        warnings_of(&session),
        vec![(1365, "Division by 0".to_owned())]
    );
}



#[test]
fn ready_value_tso_timediff_preserve_native_values_metadata_and_zone() {
    use tidb_datatype::{FieldTypeCode, MySqlDuration, Time, TimeType};

    let mut session = Session::new();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_tso_timediff_sql (tso BIGINT, tso_text VARCHAR(30), one_tso BIGINT, zero_tso BIGINT, negative_tso BIGINT, null_tso BIGINT, a DATETIME(3), b DATETIME(3), x TIME, y TIME, nd DATETIME(3))").unwrap();
    session.run("INSERT INTO shared_tso_timediff_sql VALUES (404411537129996288,'404411537129996288',1,0,-1,NULL,'2024-01-02 00:00:00.123','2024-01-01 23:59:59.120','10:10:10','10:09:00',NULL)").unwrap();

    // Original tidb_parse_tso_source integration pins: the SQL return type is
    // DATETIME(0), flen 10, while the actual native Time deliberately has FSP 6.
    // Chunk Time materialization retains that value precision, unlike Duration.
    let timestamp = Datum::Time(
        Time::from_date_checked(2018, 11, 20, 9, 53, 4, 877_000, TimeType::DateTime, 6).unwrap(),
    );
    let StmtOutput::Rows { columns, rows } = session.run_with_columns("SELECT TIDB_PARSE_TSO(tso),TIDB_PARSE_TSO(tso_text),TIDB_PARSE_TSO(one_tso),TIDB_PARSE_TSO(zero_tso),TIDB_PARSE_TSO(negative_tso),TIDB_PARSE_TSO(null_tso) FROM shared_tso_timediff_sql").unwrap() else {
        panic!("expected TSO rows")
    };
    assert_eq!(
        rows,
        vec![vec![
            timestamp.clone(),
            timestamp,
            Datum::Time(
                Time::from_date_checked(1970, 1, 1, 0, 0, 0, 0, TimeType::DateTime, 6).unwrap()
            ),
            Datum::Null,
            Datum::Null,
            Datum::Null
        ]]
    );
    assert_eq!(columns.len(), 6);
    for (_, field) in &columns {
        assert_eq!(field.code(), FieldTypeCode::Datetime);
        assert_eq!((field.flen(), field.decimal()), (10, 0));
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation_name(), "binary");
    }
    assert!(warnings_of(&session).is_empty());

    // The fixed 1.003-second boundary is the existing temporal_types SQL
    // vector; the 70-second TIME pair is the original time_diff_source_vectors
    // row. Typed columns make the declared FSP explicit, without SQL CASTs.
    let StmtOutput::Rows { columns, rows } = session.run_with_columns("SELECT TIMEDIFF(a,b),TIMEDIFF(b,a),TIMEDIFF(x,y),TIMEDIFF(a,x),TIMEDIFF(nd,b),TIMEDIFF(a,nd) FROM shared_tso_timediff_sql").unwrap() else {
        panic!("expected TIMEDIFF rows")
    };
    assert_eq!(
        rows,
        vec![vec![
            Datum::Duration(MySqlDuration::from_nanoseconds(1_003_000_000, 3).unwrap()),
            Datum::Duration(MySqlDuration::from_nanoseconds(-1_003_000_000, 3).unwrap()),
            Datum::Duration(MySqlDuration::from_nanoseconds(70_000_000_000, 0).unwrap()),
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ]]
    );
    assert_eq!(columns.len(), 6);
    for ((_, field), shape) in
        columns
            .iter()
            .zip([(14, 3), (14, 3), (10, 0), (14, 3), (14, 3), (14, 3)])
    {
        assert_eq!(field.code(), FieldTypeCode::Duration);
        assert_eq!((field.flen(), field.decimal()), shape);
        assert_eq!(field.charset_name(), "binary");
        assert_eq!(field.collation_name(), "binary");
    }
    assert!(warnings_of(&session).is_empty());

    session.run("SET time_zone='+08:00'").unwrap();
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns("SELECT TIDB_PARSE_TSO(tso) FROM shared_tso_timediff_sql")
        .unwrap()
    else {
        panic!("expected zoned TSO row")
    };
    assert_eq!(
        rows,
        vec![vec![Datum::Time(
            Time::from_date_checked(2018, 11, 20, 17, 53, 4, 877_000, TimeType::DateTime, 6)
                .unwrap()
        )]]
    );
    assert_eq!(columns[0].1.code(), FieldTypeCode::Datetime);
    assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (10, 0));
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_identity_values_preserve_types_labels_and_name_const_gate() {
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, Decimal, FieldTypeCode, MySqlDuration, MysqlEnum,
        MysqlSet, Time, TimeType,
    };

    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_identity_sql (i BIGINT, u BIGINT UNSIGNED, r DOUBLE, f FLOAT, dec_value DECIMAL(8,3), s VARCHAR(8) COLLATE utf8mb4_general_ci, b VARBINARY(3), dt DATETIME(6), tm TIME(6), j JSON, en ENUM('a','b') COLLATE utf8mb4_bin, st SET('a','b') COLLATE utf8mb4_bin, bits BIT(8), nullable_value INT)").unwrap();
    session.run("INSERT INTO shared_identity_sql VALUES (-153,18446744073709551615,3.1415926,1.5,123.123,'TiDB',X'00ff80','2024-01-02 03:04:05.600000','12:34:56.700000','{\"a\":1}','b','a,b',b'00000001',NULL)").unwrap();

    // Fixed identity vectors, not answers obtained from another evaluator.
    // Existing SQL boundary: generic post-derivation overwrites these string
    // result types with the connection collation, after identity type cloning
    // (collation_derive::default_collation/apply_derived_collation). This is an
    // existing gap from Go's final type clone, not a compatibility fix here.
    // Chunk materialization stamps that result collation and the declared
    // decimal shape; SDK tests separately pin the worker's exact Datum metadata.
    let expected = vec![
        Datum::Int(-153),
        Datum::UInt(u64::MAX),
        Datum::Real(3.1415926),
        Datum::Float32(1.5),
        Datum::Decimal(Decimal::from_literal("123.123").with_declared_shape(8, 3)),
        Datum::new_collation_string(b"TiDB".to_vec(), Collation::Utf8Mb4Bin),
        Datum::new_collation_string(vec![0, 0xff, 0x80], Collation::Utf8Mb4Bin),
        Datum::Time(
            Time::from_date_checked(2024, 1, 2, 3, 4, 5, 600_000, TimeType::DateTime, 6).unwrap(),
        ),
        Datum::Duration(MySqlDuration::new(12, 34, 56, 700_000, 6).unwrap()),
        Datum::Json(BinaryJSON::parse(r#"{"a":1}"#).unwrap()),
        Datum::new_enum(MysqlEnum::new("b", 2), Collation::Utf8Mb4Bin),
        Datum::new_set(MysqlSet::new("a,b", 3), Collation::Utf8Mb4Bin),
        Datum::Bit(BinaryLiteral::from(vec![1])),
        Datum::Null,
    ];
    let StmtOutput::Rows { columns, rows } = session.run_with_columns("SELECT aNy_VaLuE(i),ANY_VALUE(u),ANY_VALUE(r),ANY_VALUE(f),ANY_VALUE(dec_value),ANY_VALUE(s) AS kept_text,ANY_VALUE(b),ANY_VALUE(dt),ANY_VALUE(tm),ANY_VALUE(j),ANY_VALUE(en),ANY_VALUE(st),ANY_VALUE(bits),ANY_VALUE(nullable_value) FROM shared_identity_sql").unwrap() else {
        panic!("expected typed ANY_VALUE row")
    };
    assert_eq!(rows, vec![expected.clone()]);
    let codes = [
        FieldTypeCode::LongLong,
        FieldTypeCode::LongLong,
        FieldTypeCode::Double,
        FieldTypeCode::Float,
        FieldTypeCode::NewDecimal,
        FieldTypeCode::Varchar,
        FieldTypeCode::Varchar,
        FieldTypeCode::Datetime,
        FieldTypeCode::Duration,
        FieldTypeCode::Json,
        FieldTypeCode::Enum,
        FieldTypeCode::Set,
        FieldTypeCode::Bit,
        FieldTypeCode::Long,
    ];
    assert_eq!(columns.len(), codes.len());
    for ((_, field), code) in columns.iter().zip(codes) {
        assert_eq!(field.code(), code);
    }
    assert!(!columns[0].1.is_unsigned());
    assert!(columns[1].1.is_unsigned());
    assert_eq!((columns[4].1.flen(), columns[4].1.decimal()), (8, 3));
    assert_eq!(columns[5].0, "kept_text");
    assert_eq!(columns[5].1.charset_name(), "utf8mb4");
    assert_eq!(columns[5].1.collation_name(), "utf8mb4_bin");
    assert_eq!(columns[6].1.charset_name(), "utf8mb4");
    assert_eq!(columns[6].1.collation_name(), "utf8mb4_bin");
    assert_eq!(columns[6].1.flen(), 3);
    assert_eq!(columns[7].1.decimal(), 6);
    assert_eq!(columns[8].1.decimal(), 6);
    for index in [10, 11] {
        assert_eq!(
            columns[index]
                .1
                .elems_snapshot()
                .iter()
                .map(|element| element.as_bytes().to_vec())
                .collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(columns[index].1.collation_name(), "utf8mb4_bin");
    }
    assert_eq!(columns[12].1.flen(), 8);
    assert!(warnings_of(&session).is_empty());

    // NAME_CONST allows a top-level unary value. Unary plus is erased by the
    // existing rewriter, leaving a typed column and no cast/arithmetic worker.
    // Its first literal names the result; an explicit alias takes precedence.
    let StmtOutput::Rows { columns, rows } = session.run_with_columns("SELECT nAmE_cOnSt('named_signed',+i),NAME_CONST('named_binary',+b),NAME_CONST('named_decimal',+dec_value),NAME_CONST('named_time',+dt),NAME_CONST('named_json',+j),NAME_CONST('named_null',+nullable_value) AS renamed FROM shared_identity_sql").unwrap() else {
        panic!("expected typed NAME_CONST row")
    };
    assert_eq!(
        rows,
        vec![vec![
            expected[0].clone(),
            expected[6].clone(),
            expected[4].clone(),
            expected[7].clone(),
            expected[9].clone(),
            expected[13].clone()
        ]]
    );
    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "named_signed",
            "named_binary",
            "named_decimal",
            "named_time",
            "named_json",
            "renamed"
        ]
    );
    for ((_, field), code) in columns.iter().zip([
        FieldTypeCode::LongLong,
        FieldTypeCode::Varchar,
        FieldTypeCode::NewDecimal,
        FieldTypeCode::Datetime,
        FieldTypeCode::Json,
        FieldTypeCode::Long,
    ]) {
        assert_eq!(field.code(), code);
    }
    assert_eq!(columns[1].1.charset_name(), "utf8mb4");
    assert_eq!(columns[1].1.collation_name(), "utf8mb4_bin");
    assert_eq!(columns[1].1.flen(), 3);
    assert_eq!((columns[2].1.flen(), columns[2].1.decimal()), (8, 3));
    assert_eq!(columns[3].1.decimal(), 6);
    assert!(warnings_of(&session).is_empty());

    // Preserve the existing source-shape gate, rather than expanding it to
    // arbitrary column/function expressions to make worker tests convenient.
    for expression in [
        "NAME_CONST('bad',i)",
        "NAME_CONST(i,1)",
        "NAME_CONST('bad',1+1)",
    ] {
        let sql = format!("SELECT {expression} FROM shared_identity_sql");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1210, "{sql}");
        assert_eq!(mysql.message, "Incorrect arguments to NAME_CONST", "{sql}");
    }
}



#[test]
fn ready_value_weight_string_format_preserve_typed_columns_padding_locales_and_warnings() {
    // Chunk string cells carry their declared collation; inspect the payload
    // directly, including invalid UTF-8 binary prefixes, without HEX or a
    // collation/key provider used to manufacture the expected answer.
    fn payload(value: &Datum) -> Option<&[u8]> {
        match value {
            Datum::String(value) => Some(value.bytes()),
            Datum::Bytes(value) => Some(value.as_slice()),
            Datum::Null => None,
            other => panic!("expected string bytes or SQL NULL: {other:?}"),
        }
    }
    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_weight_format_sql (s VARCHAR(8) COLLATE utf8mb4_bin, npad VARCHAR(8) COLLATE utf8mb4_0900_bin, ci VARCHAR(8) COLLATE utf8mb4_general_ci, uni VARCHAR(8) COLLATE utf8mb4_bin, ns VARCHAR(8), i INT, n DECIMAL(20,3), p INT, de VARCHAR(16), india VARCHAR(16), unknown_locale VARCHAR(16), null_locale VARCHAR(16), nn DECIMAL(20,3), pn INT, neg DECIMAL(2,1), zp INT)").unwrap();
    session.run("INSERT INTO shared_weight_format_sql VALUES ('ab','ab','A','中文',NULL,7,1234567.891,2,'de_DE','en_IN','not_REAL',NULL,NULL,NULL,-2.5,0)").unwrap();
    // Literal byte vectors from the original WEIGHT_STRING source tests.
    // Typed numeric AS BINARY overrides the numeric NULL signature; the AST
    // value-only policy is different and must not be imported into this path.
    let weight_cases: [(&str, Option<&[u8]>, Option<&str>); 12] = [
        ("WEIGHT_STRING(s)", Some(&[0x61, 0x62]), None),
        ("WEIGHT_STRING(s AS CHAR(1))", Some(&[0x61]), None),
        ("WEIGHT_STRING(s AS CHAR(4))", Some(&[0x61, 0x62]), None),
        (
            "WEIGHT_STRING(s AS BINARY(4))",
            Some(&[0x61, 0x62, 0, 0]),
            None,
        ),
        (
            "WEIGHT_STRING(npad AS CHAR(4))",
            Some(&[0x61, 0x62, 0x20, 0x20]),
            None,
        ),
        ("WEIGHT_STRING(ci)", Some(&[0, 0x41]), None),
        (
            "WEIGHT_STRING(uni AS CHAR(1))",
            Some(&[0xe4, 0xb8, 0xad]),
            None,
        ),
        (
            "WEIGHT_STRING(uni AS BINARY(1))",
            Some(&[0xe4]),
            Some("Truncated incorrect BINARY(1) value: '中文'"),
        ),
        ("WEIGHT_STRING(i)", None, None),
        ("WEIGHT_STRING(i AS CHAR(2))", None, None),
        ("WEIGHT_STRING(i AS BINARY(2))", Some(&[0x37, 0]), None),
        ("WEIGHT_STRING(ns)", None, None),
    ];
    for (expression, expected, warning) in weight_cases {
        let sql = format!("SELECT {expression} FROM shared_weight_format_sql");
        let StmtOutput::Rows { columns, rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected weight rows: {sql}")
        };
        assert_eq!(rows.len(), 1, "{sql}");
        assert_eq!(rows[0].len(), 1, "{sql}");
        assert_eq!(payload(&rows[0][0]), expected, "{sql}");
        assert_eq!(
            columns[0].1.code(),
            tidb_datatype::FieldTypeCode::VarString,
            "{sql}"
        );
        assert_eq!(columns[0].1.charset_name(), "binary", "{sql}");
        assert_eq!(
            columns[0].1.collation(),
            tidb_datatype::Collation::Binary,
            "{sql}"
        );
        let expected_warnings = warning
            .map(|message| vec![(1292, message.to_owned())])
            .unwrap_or_default();
        assert_eq!(warnings_of(&session), expected_warnings, "{sql}");
    }
    // Original locale/rounding literals. A NULL locale is not a NULL result;
    // NULL number or precision does suppress the locale warning entirely.
    let format_cases: [(&str, Option<&str>, Option<&str>); 8] = [
        ("FORMAT(n,p)", Some("1,234,567.89"), None),
        ("FORMAT(n,p,de)", Some("1.234.567,89"), None),
        ("FORMAT(n,p,india)", Some("12,34,567.89"), None),
        (
            "FORMAT(n,p,unknown_locale)",
            Some("1,234,567.89"),
            Some("Unknown locale: 'not_REAL'"),
        ),
        (
            "FORMAT(n,p,null_locale)",
            Some("1,234,567.89"),
            Some("Unknown locale: 'NULL'"),
        ),
        ("FORMAT(nn,p,unknown_locale)", None, None),
        ("FORMAT(n,pn,null_locale)", None, None),
        ("FORMAT(neg,zp)", Some("-3"), None),
    ];
    for (expression, expected, warning) in format_cases {
        let sql = format!("SELECT {expression} FROM shared_weight_format_sql");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected FORMAT rows: {sql}")
        };
        assert_eq!(rows.len(), 1, "{sql}");
        assert_eq!(rows[0].len(), 1, "{sql}");
        assert_eq!(payload(&rows[0][0]), expected.map(str::as_bytes), "{sql}");
        let expected_warnings = warning
            .map(|message| vec![(1649, message.to_owned())])
            .unwrap_or_default();
        assert_eq!(warnings_of(&session), expected_warnings, "{sql}");
    }
}



#[test]
fn ready_value_date_preserves_typed_casts_zero_modes_and_metadata() {
    let mut session = Session::new();
    session.run("SET time_zone='+00:00'").unwrap();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_date_sql (dt DATETIME(6), d DATE, ts TIMESTAMP(6), txt VARCHAR(40), num BIGINT, nd DATETIME, z DATETIME, p DATETIME)").unwrap();
    session.run("INSERT INTO shared_date_sql VALUES ('2024-03-05 14:30:45.123456','2024-02-29','2024-03-04 23:30:00','2024-02-29 23:59:59.654321',20240315123045,NULL,'0000-00-00 00:00:00','2024-00-05 12:34:56')").unwrap();
    // DATE's original ETDatetime adaptation passes temporal/NULL values
    // through; VARCHAR and BIGINT exercise its existing implicit casts.
    let StmtOutput::Rows { columns, rows, .. } = session.run_with_columns(
        "SELECT DATE(dt),DATE(d),DATE(ts),DATE(txt),DATE(num),DATE(nd),DATE(z),DATE(p) FROM shared_date_sql",
    ).unwrap() else { panic!("expected typed DATE rows") };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].iter().map(cell_text).collect::<Vec<_>>(),
        vec![
            "2024-03-05",
            "2024-02-29",
            "2024-03-04",
            "2024-02-29",
            "2024-03-15",
            "NULL",
            "0000-00-00",
            "2024-00-05",
        ]
    );
    for (index, (_, field)) in columns.iter().enumerate() {
        assert_eq!(
            field.code(),
            tidb_datatype::FieldTypeCode::Date,
            "column {index}"
        );
        assert_eq!((field.flen(), field.decimal()), (10, 0), "column {index}");
        if index == 5 {
            assert_eq!(rows[0][index], Datum::Null);
        } else {
            assert!(matches!(rows[0][index], Datum::Time(_)), "column {index}");
        }
    }
    assert!(warnings_of(&session).is_empty());
    // Stored typed values avoid cast warnings: these two diagnostics belong
    // to DATE's own mode validator and retain the original, untrimmed clock.
    session
        .run("SET sql_mode='NO_ZERO_DATE,NO_ZERO_IN_DATE'")
        .unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT DATE(z),DATE(p) FROM shared_date_sql")
        .unwrap()
    else {
        panic!("expected soft DATE mode failures")
    };
    assert_eq!(rows, vec![vec![Datum::Null, Datum::Null]]);
    assert_eq!(
        warnings_of(&session),
        vec![
            (
                1292,
                "Incorrect datetime value: '0000-00-00 00:00:00'".to_owned()
            ),
            (
                1292,
                "Incorrect datetime value: '2024-00-05 12:34:56'".to_owned()
            ),
        ]
    );
}



#[test]
fn ready_value_clock_now_date_sysdate_preserves_pinned_context_and_types() {
    use tidb_datatype::FieldTypeCode::{Date, Datetime};

    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("SET time_zone='+08:00'").unwrap();
    session.run("SET timestamp=1700000000.654321").unwrap();
    session.run("SET tidb_sysdate_is_now=ON").unwrap();
    // The source f64 timestamp split gives 654320955ns. NOW and aliased
    // SYSDATE truncate to .654320, not UTC_TIMESTAMP's .654321 rounding.
    // The fixed +08 offset moves the local date into November 15.
    let StmtOutput::Rows { columns, rows, .. } = session.run_with_columns(
        "SELECT NOW(),NOW(3),NOW(6),CURRENT_TIMESTAMP(6),LOCALTIME(3),LOCALTIMESTAMP(6),CURDATE(),CURRENT_DATE(),SYSDATE(),SYSDATE(3),SYSDATE(6)",
    ).unwrap() else { panic!("expected pinned local-clock rows") };
    let time_text = |value: &Datum| match value {
        Datum::Time(value) => value.to_string(),
        other => panic!("local clock lost its native Time domain: {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].iter().map(&time_text).collect::<Vec<_>>(),
        vec![
            "2023-11-15 06:13:20",
            "2023-11-15 06:13:20.654",
            "2023-11-15 06:13:20.654320",
            "2023-11-15 06:13:20.654320",
            "2023-11-15 06:13:20.654",
            "2023-11-15 06:13:20.654320",
            "2023-11-15",
            "2023-11-15",
            "2023-11-15 06:13:20",
            "2023-11-15 06:13:20.654",
            "2023-11-15 06:13:20.654320",
        ]
    );
    for (index, (code, flen, decimal)) in [
        (Datetime, 19, 0),
        (Datetime, 23, 3),
        (Datetime, 26, 6),
        (Datetime, 26, 6),
        (Datetime, 23, 3),
        (Datetime, 26, 6),
        (Date, 10, 0),
        (Date, 10, 0),
        (Datetime, 19, 0),
        (Datetime, 23, 3),
        (Datetime, 26, 6),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(columns[index].1.code(), code, "column {index}");
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (flen, decimal),
            "column {index}"
        );
    }
    assert!(warnings_of(&session).is_empty());
    // Refresh both context inputs: the timestamp advances one second, while
    // changing to UTC takes the local date back to November 14.
    session.run("SET time_zone='+00:00'").unwrap();
    session.run("SET timestamp=1700000001").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT NOW(6),CURDATE(),SYSDATE(6)")
        .unwrap()
    else {
        panic!("expected refreshed local-clock rows")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].iter().map(&time_text).collect::<Vec<_>>(),
        vec![
            "2023-11-14 22:13:21.000000",
            "2023-11-14",
            "2023-11-14 22:13:21.000000",
        ]
    );
    assert!(warnings_of(&session).is_empty());
}



#[test]
fn ready_value_json_merge_preserves_order_null_domains_errors_and_warning() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_json_merge_sql (a JSON, patch_doc JSON, last_doc JSON, arr1 JSON, arr2 JSON, jnull JSON, nil JSON, obj JSON, bad VARCHAR(16), num INT)").unwrap();
    session.run(r#"INSERT INTO shared_json_merge_sql VALUES ('{"a":1,"b":2}','{"a":null}','{"a":3}','[1,2]','[3]','null',NULL,'{"c":4}','nope',3)"#).unwrap();
    let json = |text: &str| Datum::Json(tidb_datatype::BinaryJSON::parse(text).unwrap());
    // One stored row, twelve literal output pins. SQL NULL truncation is not
    // interchangeable with JSON null, and PATCH can recover from the former
    // when a later non-object JSON document resets its target.
    for (expression, expected, deprecated) in [
        (
            "JSON_MERGE_PATCH(a,patch_doc,last_doc)",
            json(r#"{"a":3,"b":2}"#),
            false,
        ),
        ("JSON_MERGE_PATCH(arr1,arr2)", json("[3]"), false),
        ("JSON_MERGE_PATCH(nil,obj)", Datum::Null, false),
        ("JSON_MERGE_PATCH(nil,jnull,obj)", json(r#"{"c":4}"#), false),
        ("JSON_MERGE_PATCH(a,jnull)", json("null"), false),
        (
            "JSON_MERGE_PRESERVE(a,last_doc)",
            json(r#"{"a":[1,3],"b":2}"#),
            false,
        ),
        ("JSON_MERGE_PRESERVE(arr1,arr2)", json("[1,2,3]"), false),
        (
            "JSON_MERGE_PRESERVE(a,jnull)",
            json(r#"[{"a":1,"b":2},null]"#),
            false,
        ),
        (
            "JSON_MERGE_PRESERVE(a,arr1,obj)",
            json(r#"[{"a":1,"b":2},1,2,{"c":4}]"#),
            false,
        ),
        ("JSON_MERGE_PRESERVE(nil,bad)", Datum::Null, false),
        ("JSON_MERGE(a,last_doc)", json(r#"{"a":[1,3],"b":2}"#), true),
        ("JSON_MERGE(nil,bad)", Datum::Null, false),
    ] {
        let sql = format!("SELECT {expression} FROM shared_json_merge_sql");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected merge rows: {sql}")
        };
        assert_eq!(rows, vec![vec![expected]], "{sql}");
        let expected_warnings = if deprecated {
            vec![(
                1681,
                "JSON_MERGE is deprecated and will be removed in a future release.".to_owned(),
            )]
        } else {
            Vec::new()
        };
        assert_eq!(warnings_of(&session), expected_warnings, "{sql}");
    }
    // PATCH prepares all actual documents before considering a later reset;
    // PRESERVE/MERGE stop parsing document VALUES at the first SQL NULL.
    // The bad numeric argument is second, avoiding first-argument PLAN type
    // validation; these failures are from the existing native execution path.
    // Session statement completion records 3146 as its own Error diagnostic,
    // unlike 3140. That pre-existing diagnostic is not a MERGE deprecation.
    for (expression, code) in [
        ("JSON_MERGE(a,bad)", 3140),
        ("JSON_MERGE_PRESERVE(a,bad)", 3140),
        ("JSON_MERGE_PATCH(nil,bad)", 3140),
        ("JSON_MERGE_PATCH(bad,jnull)", 3140),
        ("JSON_MERGE_PATCH(a,num)", 3146),
        ("JSON_MERGE_PRESERVE(a,num)", 3146),
    ] {
        let sql = format!("SELECT {expression} FROM shared_json_merge_sql");
        let mysql = session
            .run_with_columns(&sql)
            .expect_err(&sql)
            .to_mysql_error();
        assert_eq!(mysql.code, code, "{sql}: {mysql:?}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        let warnings = warnings_of(&session);
        if code == 3140 {
            assert!(warnings.is_empty(), "{sql}: {warnings:?}");
        } else {
            // Do not suppress the statement's original diagnostics: the
            // warning contract under test excludes only deprecation 1681.
            assert!(
                warnings.iter().all(|(code, _)| *code != 1681),
                "{sql}: {warnings:?}"
            );
        }
    }
}



#[test]
fn ready_value_clock_context_preserves_pinned_time_zone_fsp_and_lifecycle() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("SET time_zone='+08:00'").unwrap();
    session.run("SET timestamp=1700000000.654321").unwrap();
    // SET timestamp's source f64 split gives 654320955 nanoseconds. The two
    // duration families first truncate to microseconds before explicit-FSP
    // rounding, whereas UTC_TIMESTAMP rounds the original nanoseconds.
    let StmtOutput::Rows { columns, rows, .. } = session.run_with_columns(
        "SELECT CURTIME(),CURTIME(0),CURTIME(6),CURRENT_TIME(3),UTC_TIME(),UTC_TIME(0),UTC_TIME(6),UTC_DATE(),UTC_TIMESTAMP(6),UTC_TIMESTAMP()",
    ).unwrap() else { panic!("expected four current-clock families") };
    let temporal_text = |value: &Datum| match value {
        Datum::Time(value) => value.to_string(),
        Datum::Duration(value) => value.to_string(),
        other => panic!("clock lost its native temporal domain: {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].iter().map(&temporal_text).collect::<Vec<_>>(),
        vec![
            "06:13:20",
            "06:13:21",
            "06:13:20.654320",
            "06:13:20.654",
            "22:13:20",
            "22:13:21",
            "22:13:20.654320",
            "2023-11-14",
            "2023-11-14 22:13:20.654321",
            "2023-11-14 22:13:21",
        ]
    );
    for index in 0..7 {
        assert!(
            matches!(rows[0][index], Datum::Duration(_)),
            "column {index}"
        );
    }
    for index in [7, 8, 9] {
        assert!(matches!(rows[0][index], Datum::Time(_)), "column {index}");
    }
    // Preserve the original native SQL metadata, not only rendered strings.
    for (index, code, flen, decimal) in [
        (2, tidb_datatype::FieldTypeCode::Duration, 15, 6),
        (3, tidb_datatype::FieldTypeCode::Duration, 12, 3),
        (6, tidb_datatype::FieldTypeCode::Duration, 15, 6),
        (7, tidb_datatype::FieldTypeCode::Date, 10, 0),
        (8, tidb_datatype::FieldTypeCode::Datetime, 26, 6),
    ] {
        assert_eq!(columns[index].1.code(), code);
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (flen, decimal)
        );
    }
    assert!(session.warnings().is_empty());
    // One day and one second later, with a different session offset. Every
    // literal below is independent of another clock function's answer.
    session.run("SET time_zone='+00:00'").unwrap();
    session.run("SET timestamp=1700086401").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns(
            "SELECT CURTIME(6),CURRENT_TIME(),UTC_TIME(6),UTC_DATE(),UTC_TIMESTAMP(6)",
        )
        .unwrap()
    else {
        panic!("expected refreshed statement clock")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].iter().map(&temporal_text).collect::<Vec<_>>(),
        vec![
            "22:13:21.000000",
            "22:13:21",
            "22:13:21.000000",
            "2023-11-15",
            "2023-11-15 22:13:21.000000",
        ]
    );
    assert!(session.warnings().is_empty());
    // parse_datetime_precision_func accepts only an IntLit precision. The
    // value-entry UTC_TIME NULL profile is not admitted by this SQL grammar;
    // this rejection is deliberately NOT counted as worker-admission proof.
    let mysql = session
        .run_with_columns("SELECT UTC_TIME(NULL)")
        .expect_err("NULL precision is outside the SQL parser domain")
        .to_mysql_error();
    assert_eq!(mysql.code, 1064);
    assert!(!mysql.is_from_evaluation());
}



#[test]
fn ready_value_json_unquote_preserves_stored_text_and_json_policies() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_json_unquote_sql (quoted_text VARCHAR(32), encoded_doc VARCHAR(64), jpayload JSON, plain_text VARCHAR(32), incomplete_text VARCHAR(32), empty_text VARCHAR(1), object_doc JSON, null_doc JSON, null_text VARCHAR(32), null_json JSON, bad_escape VARCHAR(32), multi_root VARCHAR(32), compact_array VARCHAR(32))").unwrap();
    // Hex literals first enter VARCHAR. A direct BinaryLiteral-to-JSON insert
    // is rejected by the existing binary-charset conversion policy.
    session.run(r#"INSERT INTO shared_json_unquote_sql VALUES (X'225c6e22',X'225c225c5c6e5c2222',NULL,'{bad',X'2278','','{"b":2,"a":1}','null',NULL,NULL,X'225c7122',X'22612220226222','[1,2]')"#).unwrap();
    session
        .run("UPDATE shared_json_unquote_sql SET jpayload=encoded_doc")
        .unwrap();
    // Both quoted_text and jpayload's decoded string carry 22 5c 6e 22.
    // Native SQL text parses the escape once; native typed JSON returns its
    // four payload bytes verbatim, unlike BinaryJSON::unquote's second decode.
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT JSON_UNQUOTE(quoted_text),JSON_UNQUOTE(jpayload),JSON_UNQUOTE(plain_text),JSON_UNQUOTE(incomplete_text),JSON_UNQUOTE(empty_text),JSON_UNQUOTE(object_doc),JSON_UNQUOTE(null_doc),JSON_UNQUOTE(null_text),JSON_UNQUOTE(null_json),JSON_UNQUOTE(compact_array) FROM shared_json_unquote_sql",
    ).unwrap() else { panic!("expected stored UNQUOTE inputs") };
    assert_eq!(
        rows,
        vec![vec![
            Datum::new_string("\n"),
            Datum::new_string("\"\\n\""),
            Datum::new_string("{bad"),
            Datum::new_string("\"x"),
            Datum::new_string(""),
            Datum::new_string("{\"a\": 1, \"b\": 2}"),
            Datum::new_string("null"),
            Datum::Null,
            Datum::Null,
            Datum::new_string("[1,2]"),
        ]]
    );
    assert!(session.warnings().is_empty());
    // Fully double-quoted SQL text must parse as one JSON string. These
    // literal unknown-escape and multiple-root inputs are InvalidText, not
    // the separate EXEC invalid-path diagnostic used by JSON path functions.
    for column in ["bad_escape", "multi_root"] {
        let sql = format!("SELECT JSON_UNQUOTE({column}) FROM shared_json_unquote_sql");
        let mysql = session
            .run_with_columns(&sql)
            .expect_err(&sql)
            .to_mysql_error();
        assert_eq!(mysql.code, 3140, "{sql}: {mysql:?}");
        assert!(mysql.is_from_evaluation(), "{sql}");
    }
}



#[test]
fn ready_value_json_paths_preserve_native_selection_mutation_and_errors() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_json_paths_sql (d JSON, arr JSON, scalar_doc JSON, child_doc JSON, nd JSON, pa VARCHAR(32), pn VARCHAR(32), deep_path VARCHAR(32), missing VARCHAR(32), pzero VARCHAR(32), pone VARCHAR(32), np VARCHAR(32), badpath VARCHAR(32), wild VARCHAR(32), rootpath VARCHAR(32), txt VARCHAR(16), vb VARBINARY(8), v INT, w INT, nv INT)").unwrap();
    session.run(r#"INSERT INTO shared_json_paths_sql VALUES ('{"a":1,"b":[2,3]}','[1,2,3]','1','{"x":1}',NULL,'$.a','$.new','$.absent.child','$.absent','$[0]','$[1]',NULL,'bad path','$.*','$','[9]','ab',9,8,NULL)"#).unwrap();
    // Literal goldens follow tests_json::json_mutation_functions and the native
    // JSON source vectors. These assert native policies, NOT legacy APPEND's
    // non-array rejection or raw extraction's different duplicate policy.
    let json = |text: &str| Datum::Json(tidb_datatype::BinaryJSON::parse(text).unwrap());
    let mut check = |sql: &str, expected: Vec<Datum>| {
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(sql).unwrap() else {
            panic!("expected JSON path rows: {sql}")
        };
        assert_eq!(rows, vec![expected], "{sql}");
        assert!(session.warnings().is_empty(), "{sql}");
    };
    check(
        "SELECT JSON_SET(d,pa,v,pn,child_doc),JSON_INSERT(d,pa,v,pn,txt),JSON_REPLACE(d,pa,v,pn,nv) FROM shared_json_paths_sql",
        vec![json(r#"{"a":9,"b":[2,3],"new":{"x":1}}"#), json(r#"{"a":1,"b":[2,3],"new":"[9]"}"#), json(r#"{"a":9,"b":[2,3]}"#)],
    );
    check(
        "SELECT JSON_SET(arr,pzero,v,'$[0][0]',w),JSON_ARRAY_INSERT(arr,pzero,v,pzero,w),JSON_REMOVE(arr,pzero,pone),JSON_ARRAY_APPEND(arr,rootpath,v,pzero,w) FROM shared_json_paths_sql",
        vec![json("[8,2,3]"), json("[8,9,1,2,3]"), json("[2]"), json("[[1,8],2,3,9]")],
    );
    check(
        "SELECT JSON_SET(d,deep_path,v),JSON_INSERT(d,deep_path,v),JSON_ARRAY_INSERT(d,'$.a[1]',v),JSON_ARRAY_APPEND(d,missing,v),JSON_EXTRACT(d,deep_path) FROM shared_json_paths_sql",
        vec![json(r#"{"a":1,"b":[2,3]}"#), json(r#"{"a":1,"b":[2,3]}"#), json(r#"{"a":1,"b":[2,3]}"#), json(r#"{"a":1,"b":[2,3]}"#), Datum::Null],
    );
    check(
        "SELECT JSON_SET(d,'$.a',nv),JSON_INSERT(d,'$.new',vb),JSON_REPLACE(d,'$.a',w<v),JSON_ARRAY_INSERT(arr,pzero,child_doc),JSON_ARRAY_APPEND(scalar_doc,rootpath,txt) FROM shared_json_paths_sql",
        vec![json(r#"{"a":null,"b":[2,3]}"#), json(r#"{"a":1,"b":[2,3],"new":"base64:type15:YWI="}"#), json(r#"{"a":true,"b":[2,3]}"#), json(r#"[{"x":1},1,2,3]"#), json(r#"[1,"[9]"]"#)],
    );
    check(
        "SELECT JSON_EXTRACT(d,pa,pa),JSON_EXTRACT(d,wild),JSON_EXTRACT(arr,'$[last]'),JSON_ARRAY_INSERT(arr,'$[last]',v),JSON_ARRAY_INSERT(arr,'$[last-9]',v) FROM shared_json_paths_sql",
        vec![json("[1,1]"), json("[1,[2,3]]"), json("3"), json("[1,2,9,3]"), json("[9,1,2,3]")],
    );
    check(
        "SELECT JSON_EXTRACT(nd,pa),JSON_EXTRACT(d,np),JSON_SET(nd,'$.a',v),JSON_INSERT(d,NULL,v),JSON_REPLACE(nd,'$.a',v),JSON_REMOVE(d,np),JSON_ARRAY_APPEND(d,np,v),JSON_ARRAY_INSERT(nd,pzero,v) FROM shared_json_paths_sql",
        vec![Datum::Null; 8],
    );
    for (expression, code) in [
        ("JSON_ARRAY_INSERT(arr,rootpath,v)", 3165),
        ("JSON_ARRAY_INSERT(d,pa,v)", 3165),
        ("JSON_SET(d,wild,v)", 3149),
        ("JSON_ARRAY_INSERT(arr,wild,v)", 3149),
        ("JSON_REMOVE(d,rootpath)", 3153),
        // Column EXEC path failures use 1105, not PLAN's nominal 3143.
        ("JSON_EXTRACT(d,badpath)", 1105),
        ("JSON_ARRAY_INSERT(arr,'$[-1]',v)", 1105),
    ] {
        let sql = format!("SELECT {expression} FROM shared_json_paths_sql");
        let mysql = session
            .run_with_columns(&sql)
            .expect_err(&sql)
            .to_mysql_error();
        assert_eq!(mysql.code, code, "{sql}: {mysql:?}");
        assert!(mysql.is_from_evaluation(), "{sql}");
    }
}



#[test]
fn ready_value_json_values_preserve_constructors_keys_pretty_and_types() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_json_values_sql (ival INT, other INT, vb VARBINARY(8), fb BINARY(3), bl BLOB, doc JSON, s VARCHAR(32), n INT, nj JSON, jnull JSON, p VARCHAR(32), missing VARCHAR(32), wild VARCHAR(32), badpath VARCHAR(32), np VARCHAR(32), ka VARCHAR(8), kb VARCHAR(8), firstval INT, lastval INT, emptyarr JSON, emptyobj JSON, floats JSON, badtext VARCHAR(8))").unwrap();
    session.run(r#"INSERT INTO shared_json_values_sql VALUES (1,2,'ab','ab','ab','{"a":{"z":1,"A":2},"b":[1,2]}','[1]',NULL,NULL,'null','$.a','$.missing','$.*','bad path',NULL,'z','A',1,3,'[]','{}','[1.0,1e15,1e-16,0.000000000000001]','nope')"#).unwrap();
    // These are literal fixtures, not another provider used as an oracle.
    // Constructor/opaque/key vectors come from the immutable native JSON
    // tables; PRETTY spacing and float cutoffs follow text.rs's source policy.
    let json = |text: &str| Datum::Json(tidb_datatype::BinaryJSON::parse(text).unwrap());
    let mut check = |sql: &str, expected: Vec<Datum>| {
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(sql).unwrap() else {
            panic!("expected JSON value rows: {sql}")
        };
        assert_eq!(rows, vec![expected], "{sql}");
        assert!(session.warnings().is_empty(), "{sql}");
    };
    check(
        "SELECT JSON_ARRAY(ival<other,vb,fb,bl,doc,s,n) FROM shared_json_values_sql",
        vec![json(
            r#"[true,"base64:type15:YWI=","base64:type254:YWIA","base64:type252:YWI=",{"a":{"A":2,"z":1},"b":[1,2]},"[1]",null]"#,
        )],
    );
    check(
        "SELECT JSON_OBJECT(ka,firstval,kb,other,ka,lastval),JSON_OBJECT(ka,n),JSON_OBJECT(ka,ival<other),JSON_ARRAY(nj,jnull) FROM shared_json_values_sql",
        vec![json(r#"{"A":2,"z":3}"#), json(r#"{"z":null}"#), json(r#"{"z":true}"#), json("[null,null]")],
    );
    check(
        "SELECT JSON_ARRAY(),JSON_OBJECT(),JSON_ARRAY(s),JSON_OBJECT(ka,doc) FROM shared_json_values_sql",
        vec![json("[]"), json("{}"), json(r#"["[1]"]"#), json(r#"{"z":{"a":{"A":2,"z":1},"b":[1,2]}}"#)],
    );
    check(
        "SELECT JSON_KEYS(doc),JSON_KEYS(doc,p),JSON_KEYS(doc,missing),JSON_KEYS(doc,np),JSON_KEYS(emptyarr),JSON_KEYS(emptyobj),JSON_KEYS(ival),JSON_KEYS(nj) FROM shared_json_values_sql",
        vec![json(r#"["a","b"]"#), json(r#"["A","z"]"#), Datum::Null, Datum::Null, Datum::Null, json("[]"), Datum::Null, Datum::Null],
    );
    check(
        "SELECT JSON_PRETTY(doc),JSON_PRETTY(emptyarr),JSON_PRETTY(emptyobj),JSON_PRETTY(jnull),JSON_PRETTY(nj),JSON_PRETTY(ival) FROM shared_json_values_sql",
        vec![Datum::new_string("{\n  \"a\": {\n    \"A\": 2,\n    \"z\": 1\n  },\n  \"b\": [\n    1,\n    2\n  ]\n}"), Datum::new_string("[]"), Datum::new_string("{}"), Datum::new_string("null"), Datum::Null, Datum::new_string("1")],
    );
    check(
        "SELECT JSON_PRETTY(floats) FROM shared_json_values_sql",
        vec![Datum::new_string(
            "[\n  1.0,\n  1e15,\n  1e-16,\n  0.000000000000001\n]",
        )],
    );
    for (expression, code) in [
        ("JSON_KEYS(doc,wild)", 3149),
        // Existing column-sourced EXEC InvalidPath policy, not PLAN's 3143.
        ("JSON_KEYS(doc,badpath)", 1105),
        ("JSON_PRETTY(badtext)", 3140),
        ("JSON_OBJECT(np,ival)", 3158),
    ] {
        let sql = format!("SELECT {expression} FROM shared_json_values_sql");
        let mysql = session
            .run_with_columns(&sql)
            .expect_err(&sql)
            .to_mysql_error();
        assert_eq!(mysql.code, code, "{sql}: {mysql:?}");
        assert!(mysql.is_from_evaluation(), "{sql}");
    }
}



#[test]
fn ready_value_json_predicates_and_nulleq_preserve_fixed_values_and_errors() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_json_predicate_sql (id INT PRIMARY KEY, d JSON, arr JSON, overlap_doc JSON, candidate VARCHAR(32), p VARCHAR(32), missing VARCHAR(32), badp VARCHAR(32), wild VARCHAR(32), mode VARCHAR(8), badmode VARCHAR(8), bad_doc VARCHAR(32), target BIGINT, n BIGINT, np VARCHAR(32))").unwrap();
    session.run(r#"INSERT INTO shared_json_predicate_sql VALUES (1,'{"a":[1,2],"n":null}','[1,2]','{"n":null}','2','$.a','$.missing','bad path','$.*','one','bad','nope',2,NULL,NULL),(2,'[1,3]','[1,3]','[2,4]','2','$[0]','$.missing','bad path','$.*','one','bad','nope',2,NULL,NULL),(3,NULL,NULL,NULL,NULL,NULL,'$.missing','bad path','$.*',NULL,'bad','nope',NULL,NULL,NULL)"#).unwrap();
    // Fixed literals from the established JSON source/SQL tables: containment,
    // shallow overlap, value-vs-document MEMBER semantics, and scalar length.
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT JSON_CONTAINS(arr,candidate),JSON_CONTAINS(d,candidate,p),JSON_OVERLAPS(d,overlap_doc),target MEMBER OF(arr),JSON_CONTAINS_PATH(d,mode,p),JSON_LENGTH(d),JSON_LENGTH(d,p) FROM shared_json_predicate_sql ORDER BY id",
    ).unwrap() else { panic!("expected five JSON predicate/report families") };
    assert_eq!(
        rows,
        vec![
            vec![
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(2),
                Datum::Int(2)
            ],
            vec![
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(1),
                Datum::Int(2),
                Datum::Int(1)
            ],
            vec![Datum::Null; 7],
        ]
    );
    assert!(session.warnings().is_empty());
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT candidate MEMBER OF(arr),target<=>target,target<=>n,n<=>n,d<=>d FROM shared_json_predicate_sql ORDER BY id",
    ).unwrap() else { panic!("expected value casts and NULL-safe equality") };
    assert_eq!(
        rows,
        vec![
            vec![
                Datum::Int(0),
                Datum::Int(1),
                Datum::Int(0),
                Datum::Int(1),
                Datum::Int(1)
            ],
            vec![
                Datum::Int(0),
                Datum::Int(1),
                Datum::Int(0),
                Datum::Int(1),
                Datum::Int(1)
            ],
            vec![
                Datum::Null,
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(1)
            ],
        ]
    );
    assert!(session.warnings().is_empty());
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT JSON_CONTAINS_PATH(d,'one',p,badp),JSON_CONTAINS_PATH(d,'all',missing,badp),JSON_CONTAINS_PATH(d,'one',p,np),JSON_LENGTH(d,missing) FROM shared_json_predicate_sql WHERE id=1",
    ).unwrap() else { panic!("expected original path parsing short circuit") };
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(1),
            Datum::Int(0),
            Datum::Int(1),
            Datum::Null
        ]]
    );
    assert!(session.warnings().is_empty());
    for (expression, code) in [
        ("JSON_CONTAINS(d,candidate,wild)", 3149),
        ("JSON_LENGTH(d,wild)", 3149),
        ("JSON_CONTAINS_PATH(d,badmode,p)", 3154),
        // Existing EXEC-tier InvalidPath mapping is 1105 for stored columns;
        // the nominal 3143 applies to the PLAN-tier constant-folding route.
        ("JSON_CONTAINS_PATH(d,mode,missing,badp)", 1105),
        ("JSON_LENGTH(bad_doc,np)", 3140),
        ("JSON_CONTAINS_PATH(bad_doc,np,p)", 3140),
        ("JSON_CONTAINS(target,candidate)", 3146),
        ("target MEMBER OF(target)", 3146),
    ] {
        let sql = format!("SELECT {expression} FROM shared_json_predicate_sql WHERE id=1");
        let error = session.run_with_columns(&sql).expect_err(&sql);
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, code, "{sql}: {mysql:?}");
        assert!(mysql.is_from_evaluation(), "{sql}");
    }
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT id FROM shared_json_predicate_sql WHERE target<=>n ORDER BY id")
        .unwrap()
    else {
        panic!("expected typed NULL-safe filter")
    };
    assert_eq!(rows, vec![vec![Datum::Int(3)]]);
    assert!(session.warnings().is_empty());
}



#[test]
fn ready_value_comparison_sql_values_typed_filters_and_row_tuples() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_comparison_sql (id INT PRIMARY KEY, a BIGINT, b BIGINT, u BIGINT UNSIGNED, v BIGINT UNSIGNED, r DOUBLE, t DOUBLE, d DECIMAL(30,2), e DECIMAL(30,2), s VARCHAR(8) COLLATE utf8mb4_general_ci, q VARCHAR(8) COLLATE utf8mb4_general_ci, x VARBINARY(8), y VARBINARY(8), j JSON, k JSON, vec VECTOR, other VECTOR, dt DATETIME(6), later DATETIME(6), tm TIME(6), endtm TIME(6))").unwrap();
    session.run("INSERT INTO shared_comparison_sql VALUES (1,-1,1,18446744073709551615,1,1.5e0,2e0,9007199254740993.25,9007199254740993.26,'A ','a',X'41',X'61','[1,2]','[1,3]','[1,2]','[1,3]','2024-01-01 00:00:00.000001','2024-01-01 00:00:00.000002','-01:00:00','01:00:00'),(2,1,1,1,1,2e0,2e0,1.25,1.25,'a','a',X'61',X'61','[1,3]','[1,3]','[1,3]','[1,3]','2024-01-01 00:00:00.000002','2024-01-01 00:00:00.000002','01:00:00','01:00:00'),(3,NULL,1,NULL,1,NULL,2e0,NULL,1.25,NULL,'a',NULL,X'61',NULL,'[1,3]',NULL,'[1,3]',NULL,'2024-01-01 00:00:00.000002',NULL,'01:00:00')").unwrap();
    // Fixed truth tables: less, greater, equal, then SQL NULL. Expected values
    // are literals, never another comparison implementation used as an oracle.
    for (left, right, first) in [
        ("a", "b", [0, 1, 1, 1, 0, 0]),
        ("u", "v", [0, 1, 0, 0, 1, 1]),
        ("a", "u", [0, 1, 1, 1, 0, 0]),
        ("r", "t", [0, 1, 1, 1, 0, 0]),
        ("d", "e", [0, 1, 1, 1, 0, 0]),
        ("s", "q", [1, 0, 0, 1, 0, 1]),
        ("x", "y", [0, 1, 1, 1, 0, 0]),
        ("j", "k", [0, 1, 1, 1, 0, 0]),
        ("vec", "other", [0, 1, 1, 1, 0, 0]),
        ("dt", "later", [0, 1, 1, 1, 0, 0]),
        ("tm", "endtm", [0, 1, 1, 1, 0, 0]),
    ] {
        let projection = ["=", "!=", "<", "<=", ">", ">="]
            .map(|op| format!("{left}{op}{right}"))
            .join(",");
        let sql = format!("SELECT {projection} FROM shared_comparison_sql ORDER BY id");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("{sql}")
        };
        assert_eq!(
            rows,
            vec![
                first.into_iter().map(Datum::Int).collect::<Vec<_>>(),
                vec![
                    Datum::Int(1),
                    Datum::Int(0),
                    Datum::Int(0),
                    Datum::Int(1),
                    Datum::Int(0),
                    Datum::Int(1)
                ],
                vec![Datum::Null; 6],
            ],
            "{sql}"
        );
        assert!(session.warnings().is_empty(), "{sql}");
    }
    // Actual numeric-column filters exercise the typed batch selection path;
    // row tuples also reach scalar comparison helpers without changing admission.
    for (op, expected) in [
        ("=", vec![2]),
        ("!=", vec![1]),
        ("<", vec![1]),
        ("<=", vec![1, 2]),
        (">", vec![]),
        (">=", vec![2]),
    ] {
        for (left, right) in [("a", "b"), ("d", "e"), ("(a,b)", "(b,a)")] {
            let sql =
                format!("SELECT id FROM shared_comparison_sql WHERE {left}{op}{right} ORDER BY id");
            let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                panic!("{sql}")
            };
            assert_eq!(
                rows,
                expected
                    .iter()
                    .map(|id| vec![Datum::Int(*id)])
                    .collect::<Vec<_>>(),
                "{sql}"
            );
            assert!(session.warnings().is_empty(), "{sql}");
        }
    }
}



#[test]
fn ready_value_between_sql_fixed_domains_and_existing_grouping_rollup() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    let domains = [
        ("BIGINT", "0", "-1", "1"),
        (
            "BIGINT UNSIGNED",
            "18446744073709551614",
            "18446744073709551613",
            "18446744073709551615",
        ),
        ("DOUBLE", "1.5e0", "1e0", "2e0"),
        (
            "DECIMAL(30,2)",
            "9007199254740993.25",
            "9007199254740993.24",
            "9007199254740993.26",
        ),
        (
            "VARCHAR(8) COLLATE utf8mb4_general_ci",
            "'B '",
            "'a'",
            "'c'",
        ),
        ("VARBINARY(8)", "X'42'", "X'41'", "X'43'"),
    ];
    for (index, (ty, value, lower, upper)) in domains.iter().enumerate() {
        session
            .run(&format!(
                "CREATE TABLE shared_between_sql_{index} (v {ty}, lo {ty}, hi {ty}, n {ty})"
            ))
            .unwrap();
        session
            .run(&format!(
                "INSERT INTO shared_between_sql_{index} VALUES ({value},{lower},{upper},NULL)"
            ))
            .unwrap();
    }
    session
        .run("CREATE TABLE shared_grouping_sql (a BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO shared_grouping_sql VALUES (1)")
        .unwrap();
    for index in 0..domains.len() {
        // Every operand is stored. These fixed answers cover inclusive bounds,
        // reversed bounds, NOT BETWEEN, and NULL with a true other comparison.
        let sql = format!(
            "SELECT v BETWEEN lo AND hi, v NOT BETWEEN lo AND hi, \
             lo BETWEEN lo AND hi, hi BETWEEN lo AND hi, \
             v BETWEEN hi AND lo, v NOT BETWEEN hi AND lo, \
             n BETWEEN lo AND hi, n NOT BETWEEN lo AND hi, \
             v BETWEEN n AND hi, v BETWEEN lo AND n FROM shared_between_sql_{index}"
        );
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("{sql}")
        };
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(1),
                Datum::Int(0),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(0),
                Datum::Int(1),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]],
            "{sql}"
        );
        assert!(session.warnings().is_empty(), "{sql}");
    }
    // This exact simple rollup projection is already admitted by the existing
    // grouping_with_rollup suite. Sort only the returned rows, not the SQL.
    let mut rows =
        row_text(session.run("SELECT GROUPING(a) FROM shared_grouping_sql GROUP BY a WITH ROLLUP"));
    rows.sort();
    assert_eq!(rows, [["0"], ["1"]]);
    assert!(session.warnings().is_empty());
}



#[test]
fn ready_value_aes_sql_preserves_twelve_mode_goldens_demand_and_diagnostics() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_aes_sql (id INT PRIMARY KEY, p VARBINARY(32), k VARBINARY(32), iv VARBINARY(32), longiv VARBINARY(64), shortiv VARBINARY(8), c VARBINARY(64), n VARBINARY(32), empty VARBINARY(1), w VARCHAR(8), bad VARBINARY(32))").unwrap();
    // Fixed Go TestAESEncrypt/TestAESDecrypt vectors already recorded in
    // crypto_encryption_source::{AES_ROWS,AES_ECB_EXTRA} and builtins.rs.
    // Decryption consumes these stored goldens, never this test's encryption output.
    let vectors = [
        ("aes-128-ecb", "697BFE9B3F8C2F289DD82C88C7BC95C4"),
        ("aes-192-ecb", "9B139FD002E6496EA2D5C73A2265E661"),
        ("aes-256-ecb", "F80DCDEDDBE5663BDB68F74AEDDB8EE3"),
        ("aes-128-cbc", "2ECA0077C5EA5768A0485AA522774792"),
        ("aes-192-cbc", "516391DB38E908ECA93AAB22870EC787"),
        ("aes-256-cbc", "5D0E22C1E77523AEF5C3E10B65653C8F"),
        ("aes-128-ofb", "0515A36BBF3DE0"),
        ("aes-192-ofb", "FE09DCCF14D458"),
        ("aes-256-ofb", "2E70FCAC0C0834"),
        ("aes-128-cfb", "0515A36BBF3DE0"),
        ("aes-192-cfb", "FE09DCCF14D458"),
        ("aes-256-cfb", "2E70FCAC0C0834"),
    ];
    for (id, (_, ciphertext)) in vectors.iter().enumerate() {
        session.run(&format!("INSERT INTO shared_aes_sql VALUES ({id},'pingcap','1234567890123456','1234567890123456','1234567890123456ignored-tail','short',X'{ciphertext}',NULL,X'','123x','not-16-bytes')")).unwrap();
    }
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    for (id, (mode, ciphertext)) in vectors.iter().enumerate() {
        session
            .run(&format!("SET block_encryption_mode='{mode}'"))
            .unwrap();
        let iv = if mode.ends_with("-ecb") { "" } else { ",iv" };
        let sql = format!("SELECT HEX(AES_ENCRYPT(p,k{iv})),AES_DECRYPT(c,k{iv}) FROM shared_aes_sql WHERE id={id}");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("{sql}")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(cell_text(&rows[0][0]), *ciphertext, "{mode}");
        assert_eq!(cell_text(&rows[0][1]), "pingcap", "{mode}");
        assert_eq!(
            rows[0][1].collation(),
            Some(tidb_datatype::Collation::Binary)
        );
        assert!(session.warnings().is_empty(), "{mode}");
        if !mode.ends_with("-ecb") {
            let sql = format!("SELECT HEX(AES_ENCRYPT(p,k,longiv)),AES_DECRYPT(c,k,longiv) FROM shared_aes_sql WHERE id={id}");
            let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                panic!("{sql}")
            };
            assert_eq!(
                cell_text(&rows[0][0]),
                *ciphertext,
                "long IV uses its first 16 bytes: {mode}"
            );
            assert_eq!(cell_text(&rows[0][1]), "pingcap", "{mode}");
            assert!(session.warnings().is_empty());
            for function in ["AES_ENCRYPT", "AES_DECRYPT"] {
                let sql =
                    format!("SELECT {function}(p,k,shortiv) FROM shared_aes_sql WHERE id={id}");
                let error = session.run_with_columns(&sql).expect_err(&sql);
                let mysql = error.to_mysql_error();
                assert_eq!(mysql.code, 1210, "{sql}");
                assert_eq!(mysql.message, format!("The initialization vector supplied to {} is too short. Must be at least 16 bytes long", function.to_ascii_lowercase()), "{sql}");
                assert!(mysql.is_from_evaluation(), "{sql}");
            }
        }
        for function in ["AES_ENCRYPT", "AES_DECRYPT"] {
            // Runtime columns prevent constant folding from demanding these
            // warning-bearing children before the lazy AES signature runs.
            for args in ["n,CAST(w AS SIGNED),REPEAT(p,2048)", "p,n,REPEAT(p,2048)"] {
                let sql = format!("SELECT {function}({args}) FROM shared_aes_sql WHERE id={id}");
                let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                    panic!("{sql}")
                };
                assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
                assert!(
                    session.warnings().is_empty(),
                    "NULL suppresses later arguments and ECB 1618: {sql}"
                );
            }
            if !mode.ends_with("-ecb") {
                let sql = format!("SELECT {function}(p,k,n) FROM shared_aes_sql WHERE id={id}");
                let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                    panic!("{sql}")
                };
                assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
                assert!(session.warnings().is_empty());
            }
        }
        if mode.ends_with("-ecb") || mode.ends_with("-cbc") {
            // Block modes reject non-block ciphertext and empty ciphertext as
            // quiet library failures, not worker failures or SQL warnings.
            let sql = format!("SELECT AES_DECRYPT(bad,k{iv}),AES_DECRYPT(empty,k{iv}),LENGTH(AES_ENCRYPT(empty,empty{iv})) FROM shared_aes_sql WHERE id={id}");
            let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                panic!("{sql}")
            };
            // Empty plaintext still encrypts one full PKCS#7 padding block,
            // including with MySQL's empty password (a zero-filled AES key).
            assert_eq!(
                rows,
                vec![vec![Datum::Null, Datum::Null, Datum::Int(16)]],
                "{sql}"
            );
        } else {
            // OFB/CFB have no padding: empty payload with an empty (zero-filled)
            // MySQL key has the hand-derived empty result in both directions.
            let sql = format!("SELECT AES_ENCRYPT(empty,empty,iv),AES_DECRYPT(empty,empty,iv) FROM shared_aes_sql WHERE id={id}");
            let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                panic!("{sql}")
            };
            assert_eq!(rows.len(), 1);
            for value in &rows[0] {
                assert_ne!(value, &Datum::Null);
                assert_eq!(cell_text(value), "");
            }
        }
        assert!(session.warnings().is_empty(), "{mode}");
    }
    session
        .run("SET block_encryption_mode='aes-128-ecb'")
        .unwrap();
    // A coercion warning belongs before the ignored-IV warning. The third
    // argument would produce 1301 if evaluated, so its absence pins laziness.
    let StmtOutput::Rows { rows, .. } = session.run_with_columns(
        "SELECT HEX(AES_ENCRYPT(p,CAST(w AS SIGNED),REPEAT(p,2048))) FROM shared_aes_sql WHERE id=0",
    ).unwrap() else { panic!("expected ignored-IV ECB ciphertext") };
    assert_eq!(cell_text(&rows[0][0]), "996E0CA8688D7AD20819B90B273E01C6");
    assert_eq!(
        session.warnings(),
        &[
            SqlWarning {
                level: WarningLevel::Warning,
                code: 1292,
                message: "Truncated incorrect INTEGER value: '123x'".to_owned()
            },
            SqlWarning {
                level: WarningLevel::Warning,
                code: 1618,
                message: "<IV> option ignored".to_owned()
            },
        ]
    );
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT AES_DECRYPT(c,k,REPEAT(p,2048)) FROM shared_aes_sql WHERE id=0")
        .unwrap()
    else {
        panic!("expected ignored-IV ECB plaintext")
    };
    assert_eq!(cell_text(&rows[0][0]), "pingcap");
    assert_eq!(
        session.warnings(),
        &[SqlWarning {
            level: WarningLevel::Warning,
            code: 1618,
            message: "<IV> option ignored".to_owned()
        }]
    );
    // Existing crypto::aes_ecb_go_vectors pins this valid-length bad padding.
    // Use the original invalid-padding bytes in a column to prevent folding.
    session
        .run("UPDATE shared_aes_sql SET bad='0123456789abcdef' WHERE id=0")
        .unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT AES_DECRYPT(bad,'wrong-key') FROM shared_aes_sql WHERE id=0")
        .unwrap()
    else {
        panic!("expected quiet padding failure")
    };
    assert_eq!(rows, vec![vec![Datum::Null]]);
    assert!(session.warnings().is_empty());
}



#[test]
fn ready_value_true_division_sql_preserves_precision_demand_and_diagnostics() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_div_sql (id INT PRIMARY KEY, a BIGINT, b BIGINT, d DECIMAL(65,30), e DECIMAL(65,30), r DOUBLE, t DOUBLE, nr DOUBLE, nd DECIMAL(20,2), s VARCHAR(8), copies BIGINT)").unwrap();
    session.run("INSERT INTO shared_div_sql VALUES (1,8,7,-7.5,2,-7.5e0,2e0,NULL,NULL,'x',2048),(2,7,0,7.5,0,7.5e0,0e0,NULL,NULL,'x',2048),(3,8,7,10000000000000000000.000000000000000000000000000000,3.000000000000000000000000000000,-1e308,0.1e0,NULL,NULL,'x',2048)").unwrap();
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    for (precision, expected) in [(0, "1.1429"), (4, "1.1429"), (10, "1.1428571429")] {
        session
            .run(&format!("SET div_precision_increment={precision}"))
            .unwrap();
        let StmtOutput::Rows { rows, .. } = session
            .run_with_columns("SELECT a/b,r/t FROM shared_div_sql WHERE id=1")
            .unwrap()
        else {
            panic!("expected typed true division")
        };
        assert!(
            matches!(&rows[0][0], Datum::Decimal(_)),
            "integer / promotes to Decimal"
        );
        assert_eq!(cell_text(&rows[0][0]), expected);
        assert_eq!(rows[0][1], Datum::Real(-3.75));
        assert!(session.warnings().is_empty());
    }
    session.run("SET div_precision_increment=4").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT d/e FROM shared_div_sql WHERE id=1")
        .unwrap()
    else {
        panic!("expected negative decimal quotient")
    };
    assert_eq!(
        rows,
        vec![vec![Datum::Decimal(
            tidb_datatype::Decimal::parse_mysql("-3.75").0
        )]]
    );
    // Both true-division signatures retain their original NULL-left short circuit.
    for expression in [
        "nr / CAST(REPEAT(s,copies) AS DOUBLE)",
        "nd / CAST(REPEAT(s,copies) AS DECIMAL(20,2))",
    ] {
        let sql = format!("SELECT {expression} FROM shared_div_sql WHERE id=1");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("{sql}")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        assert!(
            session.warnings().is_empty(),
            "undemanded RHS must not emit packet warning: {sql}"
        );
    }
    for mode in ["", "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO"] {
        session.run(&format!("SET sql_mode='{mode}'")).unwrap();
        for expression in ["a/b", "r/t", "d/e"] {
            let sql = format!("SELECT {expression} FROM shared_div_sql WHERE id=2");
            let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                panic!("{sql}")
            };
            assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1365,
                    message: "Division by 0".to_owned(),
                }],
                "{sql}"
            );
        }
        let StmtOutput::Rows { rows, .. } = session
            .run_with_columns("SELECT nr/t,nd/e FROM shared_div_sql WHERE id=2")
            .unwrap()
        else {
            panic!("expected silent NULL division")
        };
        assert_eq!(rows, vec![vec![Datum::Null; 2]]);
        assert!(session.warnings().is_empty());
    }
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT d/e FROM shared_div_sql WHERE id=3")
        .unwrap()
    else {
        panic!("truncated quotient still returns its value")
    };
    assert!(matches!(&rows[0][0], Datum::Decimal(_)));
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].level, WarningLevel::Warning);
    assert_eq!(session.warnings()[0].code, 1292);
    assert!(session.warnings()[0]
        .message
        .starts_with("Truncated incorrect DECIMAL value: '"));
    let error = session
        .run("UPDATE shared_div_sql SET r=r/t WHERE id=2")
        .expect_err("strict division by zero");
    assert!(matches!(
        &error,
        DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::DivisionByZero
        ))
    ));
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1365);
    assert_eq!(mysql.state, *b"22012");
    assert_eq!(mysql.message, "Division by 0");
    assert!(mysql.is_from_evaluation());
}



#[test]
fn ready_value_modulo_sql_preserves_domains_demand_and_zero_diagnostics() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session.run("CREATE TABLE shared_mod_sql (id INT PRIMARY KEY, a BIGINT, b BIGINT, u BIGINT UNSIGNED, d DECIMAL(65,30), e DECIMAL(65,30), r DOUBLE, t DOUBLE, ni BIGINT, nr DOUBLE, nd DECIMAL(20,2), s VARCHAR(8), count BIGINT)").unwrap();
    session.run("INSERT INTO shared_mod_sql VALUES (1,-7,3,3,9007199254740993.25,2,-7.5e0,2e0,NULL,NULL,NULL,'x',2048),(2,7,0,0,7.5,0,7.5e0,0e0,NULL,NULL,NULL,'x',2048)").unwrap();
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    let decimal = |text: &str| Datum::Decimal(tidb_datatype::Decimal::parse_mysql(text).0);
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT a%b,MOD(a,u),u%a,MOD(d,e),r%t FROM shared_mod_sql WHERE id=1")
        .unwrap()
    else {
        panic!("expected typed MOD values")
    };
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(-1),
            Datum::Int(-1),
            Datum::UInt(3),
            decimal("1.25"),
            Datum::Real(-1.5)
        ]]
    );
    assert!(session.warnings().is_empty());
    // The packet warning observes RHS demand without converting a non-NULL
    // operand into a fake NULL witness. Typed integer/real demand it; decimal does not.
    for (expression, warns) in [
        ("ni % CAST(REPEAT(s,count) AS SIGNED)", true),
        ("MOD(nr,CAST(REPEAT(s,count) AS DOUBLE))", true),
        ("MOD(nd,CAST(REPEAT(s,count) AS DECIMAL(20,2)))", false),
    ] {
        let sql = format!("SELECT {expression} FROM shared_mod_sql WHERE id=1");
        let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
            panic!("{sql}")
        };
        assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
        if warns {
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1301,
                    message:
                        "Result of repeat() was larger than max_allowed_packet (1024) - truncated"
                            .to_owned(),
                }],
                "{sql}"
            );
        } else {
            assert!(session.warnings().is_empty(), "{sql}");
        }
    }
    for mode in ["", "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO"] {
        session.run(&format!("SET sql_mode='{mode}'")).unwrap();
        for expression in ["a%b", "MOD(r,t)", "d%e"] {
            let sql = format!("SELECT {expression} FROM shared_mod_sql WHERE id=2");
            let StmtOutput::Rows { rows, .. } = session.run_with_columns(&sql).unwrap() else {
                panic!("{sql}")
            };
            assert_eq!(rows, vec![vec![Datum::Null]], "{sql}");
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1365,
                    message: "Division by 0".to_owned(),
                }],
                "{sql}"
            );
        }
        let StmtOutput::Rows { rows, .. } = session
            .run_with_columns("SELECT ni%b,MOD(nr,t),nd%e FROM shared_mod_sql WHERE id=2")
            .unwrap()
        else {
            panic!("expected silent NULL inputs")
        };
        assert_eq!(rows, vec![vec![Datum::Null; 3]]);
        assert!(session.warnings().is_empty());
    }
    let error = session
        .run("UPDATE shared_mod_sql SET a=a%b WHERE id=2")
        .expect_err("strict MOD by zero");
    assert!(matches!(
        &error,
        DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::DivisionByZero
        ))
    ));
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1365);
    assert_eq!(mysql.state, *b"22012");
    assert_eq!(mysql.message, "Division by 0");
    assert!(mysql.is_from_evaluation());
}







#[test]
fn ready_value_window_arguments_use_executor_lane_cache_without_session_pool() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ascii_window_lane (id INT, v VARBINARY(8))")
        .unwrap();
    session
        .run("INSERT INTO ascii_window_lane VALUES (1,X'41'),(2,X'42')")
        .unwrap();

    let output = session
        .run_with_columns(
            "SELECT SUM(ASCII(v)) OVER (ORDER BY id) FROM ascii_window_lane ORDER BY id",
        )
        .unwrap();
    let StmtOutput::Rows { rows, .. } = output else {
        panic!("expected window rows")
    };
    let decimal = |text: &str| Datum::Decimal(tidb_datatype::Decimal::parse_mysql(text).0);
    assert_eq!(rows, vec![vec![decimal("65")], vec![decimal("131")]]);
}
