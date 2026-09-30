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
