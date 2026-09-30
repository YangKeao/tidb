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

// Explicit test limits, not production defaults. The activation regressions
// below exercise real SQL columns; the remaining lifecycle probes also use the
// already-evaluated value API. Neither is whole-family migration evidence.
fn ascii_session_policy(workers: usize) -> tidb_executor::AsciiPoolPolicy {
    tidb_executor::AsciiPoolPolicy::checked(
        workers,
        workers.min(1),
        1 << 24,
        1 << 20,
        1 << 20,
        64,
        16,
        1 << 20,
    )
    .unwrap()
}

fn ascii_session_execution(context: &tidb_executor::StmtContext) -> tidb_executor::AsciiExecution {
    context.evaluated_ascii_execution().unwrap().clone()
}

fn ascii_session_rows(session: &mut Session, sql: &str) -> StatementRecordSet {
    let statement = session.parse_statement(sql).unwrap();
    match session.open_record_set_parsed(statement, sql).unwrap() {
        OpenedStatement::Rows(rows) => rows,
        OpenedStatement::Complete(_) => panic!("expected a real opened query"),
    }
}

fn assert_ascii_session_failure(
    execution: &tidb_executor::AsciiExecution,
    expected: tidb_executor::ExpressionAdapterFailureClass,
) {
    // Never probe an old held worker for staleness: doing so would actively
    // dispose of its cached worker and invalidate the late-debt experiment.
    match execution.scope().evaluate_value(&Datum::Null) {
        Err(tidb_executor::EvalError::ExpressionAdapterFailure(failure)) => {
            assert_eq!(failure.class(), expected);
            assert_eq!(
                failure.origin(),
                tidb_executor::ExpressionAdapterFailureOrigin::Pool
            );
        }
        other => panic!("expected a typed pool failure, got {other:?}"),
    }
}

fn assert_ascii_session_live(execution: &tidb_executor::AsciiExecution) {
    assert_eq!(
        execution.scope().evaluate_value(&Datum::Null).unwrap(),
        Datum::Null
    );
}

#[test]
fn evaluated_ascii_session_installation_is_busy_safe_and_zero_slots_reject_sql() {
    let mut session = Session::new();
    for is_dml in [false, true] {
        assert!(session
            .statement_context(is_dml)
            .evaluated_ascii_execution()
            .is_none());
    }
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE ascii_zero_slots (id INT PRIMARY KEY, v VARBINARY(8))")
        .unwrap();
    session
        .run("INSERT INTO ascii_zero_slots VALUES (1,NULL),(2,X''),(3,X'FF'),(4,X'C3A9'),(5,X'E4B8AD'),(6,X'41')")
        .unwrap();
    session
        .run_with_columns_using("SELECT 1", false, |session| {
            assert!(!session
                .try_install_evaluated_ascii_policy(ascii_session_policy(0))
                .unwrap());
            for is_dml in [false, true] {
                assert!(session
                    .statement_context(is_dml)
                    .evaluated_ascii_execution()
                    .is_none());
            }
            session.execute_statement("SELECT 1")
        })
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());
    assert!(!session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    assert!(session
        .evaluated_ascii_runtime
        .latest_execution_for_test()
        .is_none());

    // Activation changes the previous dormant expectation: an explicitly
    // installed zero-slot pool must reject real SQL evaluation. No native or
    // missing-capability one-shot route may bypass this admission decision.
    // Probe NULL separately too: it must be computed, not short-circuited.
    for id in 1..=6 {
        let sql = format!("SELECT ASCII(v) FROM ascii_zero_slots WHERE id={id}");
        let error = session
            .run_with_columns(&sql)
            .expect_err("zero slots must reject SQL ASCII");
        let mysql = error.clone().to_mysql_error();
        match error {
            DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::ExpressionAdapterFailure(failure),
            )) => {
                assert_eq!(
                    failure.class(),
                    tidb_executor::ExpressionAdapterFailureClass::PoolResource
                );
                assert_eq!(
                    failure.origin(),
                    tidb_executor::ExpressionAdapterFailureOrigin::Pool
                );
                assert_eq!(mysql.message, failure.client_message());
            }
            other => panic!("SQL must retain the typed pool cause: {other:?}"),
        }
        assert_eq!(mysql.code, 1105);
        assert_eq!(mysql.state, *b"HY000");
        assert!(mysql.is_from_evaluation());
    }
    assert!(session
        .statement_context(false)
        .evaluated_ascii_execution()
        .is_none());
    assert_ascii_session_failure(
        session
            .evaluated_ascii_runtime
            .latest_execution_for_test()
            .unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
}

#[test]
fn evaluated_ascii_sql_columns_use_one_slot_for_null_empty_binary_and_utf8() {
    let mut session = Session::new();
    // One serial executor worker fits the explicit one-slot test policy; this
    // is not a claim that one pool slot supports arbitrary operator parallelism.
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE ascii_one_slot (id INT PRIMARY KEY, v VARBINARY(8))")
        .unwrap();
    session
        .run("INSERT INTO ascii_one_slot VALUES (1,NULL),(2,X''),(3,X'FF'),(4,X'C3A9'),(5,X'E4B8AD'),(6,X'41')")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_session_contexts_and_cow_borrow_one_live_execution() {
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let mut saved = Vec::new();
    session
        .run_with_columns_using("SELECT 1", false, |session| {
            let query = session.statement_context(false);
            let dml = session.statement_context(true);
            let original_sizes = query.executor_chunk_sizes();
            let cow = query.clone().with_executor_chunk_sizes(7, 23);
            let configured = query.clone().configure(|context| {
                let _ = context.with_max_allowed_packet(128);
            });
            let held = query.evaluated_ascii_execution().unwrap().scope();
            assert_eq!(
                held.evaluate_value(&Datum::Bytes(b"A".to_vec())).unwrap(),
                Datum::Int(65)
            );
            let repeated = session.statement_context(false);
            drop(session.statement_context(true));
            assert_eq!(held.evaluate_value(&Datum::Null).unwrap(), Datum::Null);
            drop(held);
            assert_eq!(query.executor_chunk_sizes(), original_sizes);
            assert_eq!(cow.executor_chunk_sizes(), (7, 23));
            for context in [query.clone(), query, dml, cow, configured, repeated] {
                let columns: &dyn tidb_executor::Columns = &context;
                assert!(columns.evaluated_ascii_scope().is_none());
                assert!(std::ptr::eq(
                    columns.evaluated_ascii_execution().unwrap(),
                    context.evaluated_ascii_execution().unwrap()
                ));
                let scope = context.evaluated_ascii_execution().unwrap().scope();
                assert_eq!(scope.evaluate_value(&Datum::Null).unwrap(), Datum::Null);
                assert_eq!(
                    scope.evaluate_value(&Datum::Bytes(vec![255])).unwrap(),
                    Datum::Int(255)
                );
                saved.push(context);
            }
            session.execute_statement("SELECT 1")
        })
        .unwrap();
    for context in saved {
        assert_ascii_session_failure(
            context.evaluated_ascii_execution().unwrap(),
            tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
        );
    }
}

#[test]
fn evaluated_ascii_real_execute_and_import_borrow_the_outer_epoch() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ascii_updates (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO ascii_updates VALUES (1,10),(2,20)")
        .unwrap();
    session
        .run("CREATE TABLE ascii_imported (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("PREPARE ascii_update FROM 'UPDATE ascii_updates SET v=? WHERE id=?'")
        .unwrap();
    session.run("SET @v=11, @id=1").unwrap();
    session.run("EXECUTE ascii_update USING @v,@id").unwrap();
    session.run("SET @v=22, @id=2").unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    for (sql, affected) in [
        ("EXECUTE ascii_update USING @v,@id", 1),
        (
            "IMPORT INTO ascii_imported FROM SELECT * FROM ascii_updates",
            2,
        ),
    ] {
        let mut captured = None;
        let (output, _) = session
            .run_with_columns_using(sql, false, |session| {
                let execution = ascii_session_execution(&session.statement_context(false));
                let held = execution.scope();
                assert_eq!(held.evaluate_value(&Datum::Null).unwrap(), Datum::Null);
                let output = session.execute_statement(sql)?;
                if sql.starts_with("EXECUTE") {
                    assert!(
                        session.found_in_plan_cache,
                        "exercise the cached UPDATE branch"
                    );
                }
                // IMPORT executes both its COUNT precheck and INSERT SELECT through
                // self.run; none of those inner reset/finish calls owns this epoch.
                assert_eq!(
                    held.evaluate_value(&Datum::Bytes(b"Z".to_vec())).unwrap(),
                    Datum::Int(90)
                );
                captured = Some(execution);
                Ok(output)
            })
            .unwrap();
        assert!(matches!(output, StmtOutput::Affected(count) if count == affected));
        assert_ascii_session_failure(
            &captured.unwrap(),
            tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
        );
    }
    assert_eq!(
        row_text(session.run("SELECT id,v FROM ascii_imported ORDER BY id")),
        [["1", "11"], ["2", "22"]]
    );
}

#[test]
fn evaluated_ascii_stream_next_eof_and_cancellation_do_not_close() {
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let mut rows = ascii_session_rows(&mut session, "SELECT 1");
    let execution = ascii_session_execution(rows.context_for_test());
    let mut chunk = rows.new_chunk();
    rows.next(&mut session, &mut chunk).unwrap();
    assert_eq!(chunk.num_rows(), 1);
    assert_ascii_session_live(&execution);
    rows.next(&mut session, &mut chunk).unwrap();
    assert_eq!(chunk.num_rows(), 0);
    assert_ascii_session_live(&execution);
    let cancellation = session.begin_query_cancellation();
    cancellation.cancel();
    // The record set is NOT finished: this is the real native cancellation
    // error, not Next's separate post-finish 1317 fast path.
    assert_eq!(
        rows.next(&mut session, &mut chunk)
            .unwrap_err()
            .to_mysql_error()
            .code,
        1317
    );
    assert_ascii_session_live(&execution);
    drop(cancellation);
    rows.finish(&mut session).unwrap();
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    rows.finish(&mut session).unwrap();
    rows.close(&mut session).unwrap();
    rows.close(&mut session).unwrap();
}

#[test]
fn evaluated_ascii_retain_and_finish_native_errors_still_close() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut session = Session::new();
    session
        .run("CREATE TABLE ascii_finish_error (id INT)")
        .unwrap();
    session
        .run("INSERT INTO ascii_finish_error VALUES (1)")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let mut rows = ascii_session_rows(&mut session, "SELECT id FROM ascii_finish_error");
    let execution = ascii_session_execution(rows.context_for_test());
    assert_ascii_session_live(&execution);
    let cancellation = session.begin_query_cancellation();
    cancellation.cancel();
    assert_eq!(
        rows.retain_chunks(&mut session)
            .unwrap_err()
            .to_mysql_error()
            .code,
        1317
    );
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    drop(cancellation);
    rows.close(&mut session).unwrap();

    let mut rows = ascii_session_rows(&mut session, "SELECT id FROM ascii_finish_error");
    let execution = ascii_session_execution(rows.context_for_test());
    assert_ascii_session_live(&execution);
    // A real native transaction-finish error, not a substituted RS or C4
    // factory: AutocommitRead cannot acquire this deliberately poisoned catalog.
    let catalog = session.shared_catalog();
    assert!(catch_unwind(AssertUnwindSafe(|| {
        let _lock = catalog.lock().unwrap();
        panic!("native catalog poison");
    }))
    .is_err());
    assert!(matches!(
        rows.finish(&mut session),
        Err(DriverError::CatalogPoisoned)
    ));
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    rows.finish(&mut session).unwrap();
    drop(rows);
}

#[test]
fn evaluated_ascii_detached_old_close_preserves_new_epoch_and_late_worker_debt() {
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let mut old_rows = ascii_session_rows(&mut session, "SELECT 1");
    let old_execution = ascii_session_execution(old_rows.context_for_test());
    let held = old_execution.scope();
    assert_eq!(held.evaluate_value(&Datum::Null).unwrap(), Datum::Null);
    let mut new_rows = ascii_session_rows(&mut session, "SELECT 2");
    let new_execution = ascii_session_execution(new_rows.context_for_test());
    assert_ascii_session_failure(
        &old_execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    assert_ascii_session_failure(
        &new_execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolResource,
    );
    old_rows.close(&mut session).unwrap();
    assert_ascii_session_failure(
        &new_execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolResource,
    );
    // Closing E1 cannot release a worker still owned by a late affine scope.
    drop(held);
    assert_ascii_session_live(&new_execution);
    drop(old_rows);
    assert_ascii_session_live(&new_execution);
    new_rows.close(&mut session).unwrap();
    assert_ascii_session_failure(
        &new_execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
}

#[test]
fn evaluated_ascii_attached_detached_and_session_drop_close_captured_epochs() {
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let rows = ascii_session_rows(&mut session, "SELECT 1");
    let execution = ascii_session_execution(rows.context_for_test());
    assert_ascii_session_live(&execution);
    drop(OpenedStatement::Rows(rows).attach(&mut session));
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    let rows = ascii_session_rows(&mut session, "SELECT 2");
    let execution = ascii_session_execution(rows.context_for_test());
    assert_ascii_session_live(&execution);
    drop(rows);
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    let rows = ascii_session_rows(&mut session, "SELECT 3");
    let context = rows.context_for_test().clone();
    let execution = ascii_session_execution(&context);
    assert_ascii_session_live(&execution);
    drop(session);
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    assert_ascii_session_failure(
        context.evaluated_ascii_execution().unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    drop(rows);
}

#[test]
fn evaluated_ascii_real_point_get_none_fallback_closes_its_attempt() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ascii_point (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO ascii_point VALUES (1,9)").unwrap();
    let sql = "SELECT v FROM ascii_point WHERE id=?";
    let prepared = session.prepare_ast(sql).unwrap();
    let plan = prepared.point_get_plan().unwrap();
    let execution = session
        .bind_cached_prepared_point_get(&plan, &[Datum::Int(1)])
        .unwrap();
    session
        .run("ALTER TABLE ascii_point ADD COLUMN added INT")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    assert!(session
        .open_prepared_point_get(execution, prepared.statement(), sql)
        .unwrap()
        .is_none());
    assert_ascii_session_failure(
        session
            .evaluated_ascii_runtime
            .latest_execution_for_test()
            .unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    let output = session.run_with_params(sql, &[Datum::Int(1)]).unwrap();
    let StmtOutput::Rows { rows, .. } = output else {
        panic!("expected fallback rows")
    };
    assert_eq!(rows, vec![vec![Datum::Int(9)]]);
    assert_ascii_session_failure(
        session
            .evaluated_ascii_runtime
            .latest_execution_for_test()
            .unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
}

#[test]
fn evaluated_ascii_pre_admission_rejections_preserve_detached_epoch() {
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let rows = ascii_session_rows(&mut session, "SELECT 1");
    let execution = ascii_session_execution(rows.context_for_test());
    assert_ascii_session_live(&execution);
    let metadata_statement = session.parse_statement("SELECT 9").unwrap();
    assert_eq!(
        session
            .plan_bound_prepared_columns(metadata_statement)
            .unwrap()
            .len(),
        1
    );
    assert_ascii_session_live(&execution);
    assert!(session.run_with_params("SELECT ?", &[]).is_err());
    assert_ascii_session_live(&execution);
    session.enable_sandbox_mode();
    assert_eq!(
        session.run("SELECT 1").unwrap_err().to_mysql_error().code,
        1820
    );
    assert_ascii_session_live(&execution);
    // Syntax parsing is post-admission (sandbox lets syntax errors through).
    assert!(session.run("SELECT (").is_err());
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    assert_ascii_session_failure(
        session
            .evaluated_ascii_runtime
            .latest_execution_for_test()
            .unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    drop(rows);
}

#[test]
fn evaluated_ascii_materialization_closes_live_epoch_before_retained_replay() {
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let mut old_rows = ascii_session_rows(&mut session, "SELECT 7");
    let execution = ascii_session_execution(old_rows.context_for_test());
    let authority = session.result_materialization_authority();
    assert_ascii_session_live(&execution);
    old_rows.retain_chunks(&mut session).unwrap();
    assert_ascii_session_failure(
        &execution,
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    let mut new_rows = ascii_session_rows(&mut session, "SELECT 8");
    let new_execution = ascii_session_execution(new_rows.context_for_test());
    let mut chunk = old_rows.new_chunk();
    old_rows.next(&mut session, &mut chunk).unwrap();
    assert_eq!(chunk.num_rows(), 1);
    old_rows.next(&mut session, &mut chunk).unwrap();
    assert_eq!(chunk.num_rows(), 0);
    assert_ascii_session_live(&new_execution);
    old_rows.close(&mut session).unwrap();
    drop(old_rows);
    drop(authority);
    assert_ascii_session_live(&new_execution);
    new_rows.close(&mut session).unwrap();
}

#[test]
fn evaluated_ascii_native_epilogue_unwinds_close_live_owning_results() {
    use crate::record_set::{set_native_epilogue_for_test, NativeEpilogue};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    for phase in [
        NativeEpilogue::Next,
        NativeEpilogue::Finish,
        NativeEpilogue::Retain,
    ] {
        let mut rows = ascii_session_rows(&mut session, "SELECT 1");
        let execution = ascii_session_execution(rows.context_for_test());
        assert_ascii_session_live(&execution);
        let probe = execution.clone();
        set_native_epilogue_for_test(phase, move || {
            // The real native operation has returned; its epilogue can still
            // use the live C4 capability before unwinding through our guard.
            assert_ascii_session_live(&probe);
            panic!("ASCII native epilogue");
        });
        let outcome = catch_unwind(AssertUnwindSafe(|| match phase {
            NativeEpilogue::Next => {
                let mut chunk = rows.new_chunk();
                let _ = rows.next(&mut session, &mut chunk);
            }
            NativeEpilogue::Finish => {
                let _ = rows.finish(&mut session);
            }
            NativeEpilogue::Retain => {
                let _ = rows.retain_chunks(&mut session);
            }
        }));
        let payload = outcome.unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some("ASCII native epilogue")
        );
        // The result object is deliberately still alive after the catcher.
        assert_ascii_session_failure(
            &execution,
            tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
        );
        rows.finish(&mut session).unwrap();
        rows.close(&mut session).unwrap();
    }
}

#[test]
fn evaluated_ascii_borrowed_result_panic_and_close_do_not_own_outer_epoch() {
    use crate::record_set::{set_native_epilogue_for_test, NativeEpilogue};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let mut captured = None;
    session
        .run_with_columns_using("SELECT 1", false, |session| {
            let execution = ascii_session_execution(&session.statement_context(false));
            let held = execution.scope();
            assert_eq!(held.evaluate_value(&Datum::Null).unwrap(), Datum::Null);
            let mut rows = ascii_session_rows(session, "SELECT 1");
            set_native_epilogue_for_test(NativeEpilogue::Next, || {
                panic!("borrowed native epilogue")
            });
            let mut chunk = rows.new_chunk();
            let payload = catch_unwind(AssertUnwindSafe(|| {
                let _ = rows.next(session, &mut chunk);
            }))
            .unwrap_err();
            assert_eq!(
                payload.downcast_ref::<&str>().copied(),
                Some("borrowed native epilogue")
            );
            assert_eq!(held.evaluate_value(&Datum::Null).unwrap(), Datum::Null);
            rows.finish(session).unwrap();
            rows.close(session).unwrap();
            drop(rows);
            assert_eq!(held.evaluate_value(&Datum::Null).unwrap(), Datum::Null);
            captured = Some(execution);
            Ok(StmtOutput::Done(true))
        })
        .unwrap();
    assert_ascii_session_failure(
        &captured.unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
}

#[test]
fn evaluated_ascii_outer_unwind_resets_marker_and_session_roots_are_isolated() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut session = Session::new();
    let payload = catch_unwind(AssertUnwindSafe(|| {
        let _ = session.run_with_columns_using("SELECT 1", false, |session| {
            assert!(!session
                .try_install_evaluated_ascii_policy(ascii_session_policy(1))
                .unwrap());
            panic!("unconfigured lexical unwind");
        });
    }))
    .unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("unconfigured lexical unwind")
    );
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    // Independent sessions must not share a root even with identical policy.
    let mut peer = Session::new();
    assert!(peer
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    let peer_rows = ascii_session_rows(&mut peer, "SELECT 2");
    let peer_execution = ascii_session_execution(peer_rows.context_for_test());
    let peer_scope = peer_execution.scope();
    assert_eq!(
        peer_scope.evaluate_value(&Datum::Null).unwrap(),
        Datum::Null
    );
    let payload = catch_unwind(AssertUnwindSafe(|| {
        let _ = session.run_with_columns_using("SELECT 1", false, |session| {
            assert_ascii_session_live(&ascii_session_execution(&session.statement_context(false)));
            panic!("configured lexical unwind");
        });
    }))
    .unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("configured lexical unwind")
    );
    assert_ascii_session_failure(
        session
            .evaluated_ascii_runtime
            .latest_execution_for_test()
            .unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    let rows = ascii_session_rows(&mut session, "SELECT 3");
    let execution = ascii_session_execution(rows.context_for_test());
    assert_ascii_session_live(&execution); // stale true marker would borrow the closed epoch
    drop(rows);
    drop(session);
    assert_eq!(
        peer_scope.evaluate_value(&Datum::Null).unwrap(),
        Datum::Null
    );
    drop(peer_scope);
    drop(peer_rows);
}

#[test]
fn evaluated_ascii_unwrapped_public_execute_statement_owns_its_epoch() {
    let mut session = Session::new();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());
    assert!(matches!(
        session.execute_statement("SELECT 1").unwrap(),
        StmtOutput::Rows { .. }
    ));
    assert_ascii_session_failure(
        session
            .evaluated_ascii_runtime
            .latest_execution_for_test()
            .unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
    assert!(session.execute_statement("SELECT (").is_err());
    assert_ascii_session_failure(
        session
            .evaluated_ascii_runtime
            .latest_execution_for_test()
            .unwrap(),
        tidb_executor::ExpressionAdapterFailureClass::PoolClosed,
    );
}

#[test]
fn evaluated_ascii_shared_pool_mixed_string_ops_on_sql_columns() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_all_string_op_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_string_zero (id INT PRIMARY KEY, v VARBINARY(8), h VARBINARY(8))")
        .unwrap();
    session
        .run("INSERT INTO shared_string_zero VALUES (1,NULL,NULL),(2,X'20FF20','F')")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // NULL and non-NULL column calls must both reach the installed pool; neither
    // native calculation nor a missing-capability one-shot may bypass its limit.
    for (function, column) in [
        ("LENGTH", "v"),
        ("OCTET_LENGTH", "v"),
        ("BIT_LENGTH", "v"),
        ("LTRIM", "v"),
        ("RTRIM", "v"),
        ("UNHEX", "h"),
    ] {
        for id in [1, 2] {
            let sql = format!("SELECT {function}({column}) FROM shared_string_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("{sql} must retain the typed pool cause: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_crc_reverse_char_length_quote_sql_values() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_crc_reverse_char_length_quote() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_text_zero (id INT PRIMARY KEY, b VARBINARY(16), \
             t VARCHAR(16) CHARACTER SET utf8mb4)",
        )
        .unwrap();
    session
        .run("INSERT INTO shared_text_zero VALUES (1,NULL,NULL),(2,X'C3A9E4B8AD','é中')")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    for (function, column) in [
        ("CRC32", "b"),
        ("REVERSE", "b"),
        ("REVERSE", "t"),
        ("CHAR_LENGTH", "b"),
        ("CHARACTER_LENGTH", "t"),
        ("QUOTE", "b"),
        ("QUOTE", "t"),
    ] {
        for id in [1, 2] {
            let sql = format!("SELECT {function}({column}) FROM shared_text_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("{sql} bypassed shared-pool admission: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_hex_bin_left_right_replace_sql_values() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_hex_bin_left_right_replace() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_shape_zero (id INT PRIMARY KEY, u BIGINT UNSIGNED, \
             k BIT(16), b VARBINARY(16), t VARCHAR(16) CHARACTER SET utf8mb4, \
             n BIGINT, f VARBINARY(8), r VARBINARY(8))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_shape_zero VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,9223372036854775808,65,X'C3A9E4B8AD','é中',1,X'C3A9',X'FF')",
        )
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Do not wrap LEFT/RIGHT/REPLACE in HEX here: HEX's own refusal must not
    // hide an inner operation that wrongly bypasses shared-pool admission.
    for expression in [
        "HEX(u)",
        "HEX(k)",
        "HEX(b)",
        "BIN(u)",
        "LEFT(b,n)",
        "RIGHT(b,n)",
        "LEFT(t,n)",
        "RIGHT(t,n)",
        "REPLACE(b,f,r)",
        "REPLACE(t,'é','X')",
    ] {
        for id in [1, 2] {
            let sql = format!("SELECT {expression} FROM shared_shape_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("unexpected SQL pool diagnostic for {sql}: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_bitwise_sql_values_and_original_metadata() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_bitwise_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_bit_zero (id INT PRIMARY KEY, u BIGINT UNSIGNED, s BIGINT, n BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO shared_bit_zero VALUES (1,NULL,NULL,NULL),(2,9223372036854775808,-1,63)")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Direct expressions, with no migrated outer function that could conceal
    // a native fast path. Both NULL and non-NULL columns must reach the pool.
    for expression in [
        "u & s",
        "u | s",
        "u ^ s",
        "~s",
        "u << n",
        "u >> n",
        "BIT_COUNT(s)",
        "BIT_COUNT(u)",
    ] {
        for id in [1, 2] {
            let sql = format!("SELECT {expression} FROM shared_bit_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("{sql} must refuse the installed zero-slot pool: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_predicate_sql_values_and_null_truth_table() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_predicate_sql_columns_and_filters() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_pred_zero (id INT PRIMARY KEY, v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO shared_pred_zero VALUES (1,NULL),(2,2)")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    let mut statements = Vec::new();
    for expression in [
        "NOT v",
        "!v",
        "ISNULL(v)",
        "v IS NULL",
        "v IS NOT NULL",
        "ISTRUE(v)",
        "ISFALSE(v)",
        "v IS TRUE",
        "v IS NOT TRUE",
        "v IS FALSE",
        "v IS NOT FALSE",
        "v IS UNKNOWN",
        "v IS NOT UNKNOWN",
    ] {
        for id in [1, 2] {
            // No migrated outer function can mask a direct predicate bypass.
            statements.push(format!(
                "SELECT {expression} FROM shared_pred_zero WHERE id={id}"
            ));
        }
    }
    // Cover the ISNULL bitmap candidate and a NOT filter as well as projection.
    // Keep NOT on the column: NOT(v = 0) may legitimately optimize to v != 0.
    statements.extend([
        "SELECT id FROM shared_pred_zero WHERE v IS NULL".to_owned(),
        "SELECT id FROM shared_pred_zero WHERE NOT v".to_owned(),
    ]);
    for sql in statements {
        let error = session.run_with_columns(&sql).expect_err(&sql);
        match &error {
            DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::ExpressionAdapterFailure(failure),
            )) => {
                assert_eq!(
                    failure.class(),
                    tidb_executor::ExpressionAdapterFailureClass::PoolResource
                );
                assert_eq!(
                    failure.origin(),
                    tidb_executor::ExpressionAdapterFailureOrigin::Pool
                );
            }
            other => panic!("predicate SQL must reach the zero-slot pool: {sql}: {other:?}"),
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
    }
}

#[test]
fn evaluated_ascii_shared_pool_md5_sha_sha1_sql_binary_and_text_values() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_md5_sha_sha1_sql_columns() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_hash_zero (id INT PRIMARY KEY, b VARBINARY(16), \
             t VARCHAR(16) CHARACTER SET utf8mb4)",
        )
        .unwrap();
    session
        .run("INSERT INTO shared_hash_zero VALUES (1,NULL,NULL),(2,X'FF','é中')")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Direct calls on both declared input kinds, not HEX(hash(...)) or another
    // migrated outer function whose refusal could hide a native hash route.
    for expression in ["MD5(b)", "SHA(b)", "SHA1(b)", "MD5(t)", "SHA(t)", "SHA1(t)"] {
        for id in [1, 2] {
            let sql = format!("SELECT {expression} FROM shared_hash_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("hash SQL must reach the zero-slot pool: {sql}: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_logical_sql_full_three_valued_table() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_logical_sql_left_states() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_logic_zero (id INT PRIMARY KEY, a BIGINT, b BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO shared_logic_zero VALUES (1,NULL,-3),(2,0,-3),(3,2,-3)")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Each operation gets NULL, zero and nonzero left columns. In particular,
    // false AND / true OR must delegate their answer even when RHS is not
    // demanded. Neither the operands nor the target expression are constants.
    for expression in ["a AND b", "a OR b", "a XOR b"] {
        for id in [1, 2, 3] {
            let sql = format!("SELECT {expression} FROM shared_logic_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("logical SQL must reach the zero-slot pool: {sql}: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_inet_sql_nullable_text_integer_and_binary_values() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_all_inet_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_inet_zero (id INT PRIMARY KEY, t VARCHAR(64), \
             n BIGINT UNSIGNED, b VARBINARY(16))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_inet_zero VALUES \
             (1,NULL,NULL,NULL),(2,'127.0.0.1',2130706433,X'7F000001')",
        )
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Direct stored-column calls keep every target visible to the pool; no
    // outer HEX or other migrated function can stand in for INET admission.
    for expression in [
        "INET_ATON(t)",
        "INET_NTOA(n)",
        "INET6_ATON(t)",
        "INET6_NTOA(b)",
    ] {
        for id in [1, 2] {
            let sql = format!("SELECT {expression} FROM shared_inet_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("INET SQL must reach the zero-slot pool: {sql}: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_math_real_sql_analytical_values_and_metadata() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_six_math_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run("CREATE TABLE shared_math_zero (id INT PRIMARY KEY, v DOUBLE)")
        .unwrap();
    session
        .run("INSERT INTO shared_math_zero VALUES (1,NULL),(2,1)")
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // NULL and non-NULL direct calls must each reach the installed pool;
    // no outer migrated function or constant expression can hide the target.
    for function in ["ASIN", "ACOS", "SQRT", "SIGN", "RADIANS", "DEGREES"] {
        for id in [1, 2] {
            let sql = format!("SELECT {function}(v) FROM shared_math_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("math SQL must reach the zero-slot pool: {sql}: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_pi_and_ip_predicate_sql_values_and_metadata() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_four_ip_predicate_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_ip_pred_zero (id INT PRIMARY KEY, \
             t VARCHAR(64), b VARBINARY(16))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_ip_pred_zero VALUES \
             (1,NULL,NULL),(2,'001.002.3.4',X'00000000000000000000FFFF01020304')",
        )
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Direct nullable-column calls only. PI is deliberately absent: a legal
    // constant fold must not be forced to retain its runtime call for this test.
    for expression in [
        "IS_IPV4(t)",
        "IS_IPV6(t)",
        "IS_IPV4_COMPAT(b)",
        "IS_IPV4_MAPPED(b)",
    ] {
        for id in [1, 2] {
            let sql = format!("SELECT {expression} FROM shared_ip_pred_zero WHERE id={id}");
            let error = session.run_with_columns(&sql).expect_err(&sql);
            match &error {
                DriverError::Exec(tidb_executor::ExecError::Eval(
                    tidb_executor::EvalError::ExpressionAdapterFailure(failure),
                )) => {
                    assert_eq!(
                        failure.class(),
                        tidb_executor::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        tidb_executor::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                other => panic!("IP predicate SQL must reach the zero-slot pool: {sql}: {other:?}"),
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
        }
    }
}
