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

#[test]
fn evaluated_ascii_shared_pool_packet_string_sql_values_metadata_and_warnings() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_packet_string_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_packet_zero (id INT PRIMARY KEY, n BIGINT, \
             t VARCHAR(16), b VARBINARY(16), e VARCHAR(2048))",
        )
        .unwrap();
    let padded_base64 = format!("{}YQ==", " ".repeat(1364));
    session
        .run(&format!(
            "INSERT INTO shared_packet_zero VALUES \
             (1,NULL,NULL,NULL,NULL),(2,2,'ab',X'616263','YWJj'),\
             (3,1,'a',X'61','{padded_base64}')"
        ))
        .unwrap();
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    assert_eq!(session.max_allowed_packet(), 1024);
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Eight direct NULL/non-NULL calls, plus one warning-before-refusal case.
    // Even FROM's over-packet branch must call the real suppressed-result
    // kernel: returning native NULL after warning would evade this pool.
    for (expression, id) in [
        ("SPACE(n)", 1),
        ("SPACE(n)", 2),
        ("REPEAT(t,n)", 1),
        ("REPEAT(t,n)", 2),
        ("TO_BASE64(b)", 1),
        ("TO_BASE64(b)", 2),
        ("FROM_BASE64(e)", 1),
        ("FROM_BASE64(e)", 2),
        ("FROM_BASE64(e)", 3),
    ] {
        let sql = format!("SELECT {expression} FROM shared_packet_zero WHERE id={id}");
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
            other => panic!("packet string SQL must reach the zero-slot pool: {sql}: {other:?}"),
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        if id == 3 {
            // finish_statement_state does not append evaluation-origin 1105 to
            // the warning list. The earlier packet warning survives; the separate
            // returned typed refusal and its origin were checked above.
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1301,
                    message: "Result of from_base64() was larger than \
                              max_allowed_packet (1024) - truncated"
                        .to_owned(),
                }]
            );
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_case_sha2_ord_sql_values_and_metadata() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_case_sha2_ord_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_case_hash_ord_zero (id INT PRIMARY KEY, \
             t VARCHAR(16), b VARBINARY(8), bits INT)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_case_hash_ord_zero VALUES \
             (1,NULL,NULL,NULL),(2,'Ab',X'616263',256)",
        )
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    for expression in ["LOWER(t)", "UPPER(t)", "SHA2(b,bits)", "ORD(t)"] {
        for id in [1, 2] {
            let sql = format!("SELECT {expression} FROM shared_case_hash_ord_zero WHERE id={id}");
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
                other => {
                    panic!("case/SHA2/ORD SQL must reach the zero-slot pool: {sql}: {other:?}")
                }
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
            // Evaluation-origin 1105 is returned, not an Error warning row.
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_trim_split_pad_sql_values_metadata_and_packet_policy() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_trim_split_pad_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_trim_split_pad_zero (id INT PRIMARY KEY, \
             t VARCHAR(16), d VARCHAR(8), n BIGINT, p VARCHAR(8))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_trim_split_pad_zero VALUES \
             (1,NULL,NULL,NULL,NULL),(2,'ab','b',2,'x'),\
             (3,'ab','b',NULL,'x'),(4,NULL,'b',-1,'x')",
        )
        .unwrap();
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    assert_eq!(session.max_allowed_packet(), 1024);
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Eight direct NULL/non-NULL calls; then a NULL length with non-NULL
    // source, and a warning-bearing suppressed result whose source is NULL.
    for (expression, id) in [
        ("TRIM(t)", 1),
        ("TRIM(t)", 2),
        ("SUBSTRING_INDEX(t,d,n)", 1),
        ("SUBSTRING_INDEX(t,d,n)", 2),
        ("LPAD(t,n,p)", 1),
        ("LPAD(t,n,p)", 2),
        ("RPAD(t,n,p)", 1),
        ("RPAD(t,n,p)", 2),
        ("LPAD(t,n,p)", 3),
        ("RPAD(t,n,p)", 4),
    ] {
        let sql = format!("SELECT {expression} FROM shared_trim_split_pad_zero WHERE id={id}");
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
            other => panic!("trim/split/pad SQL must reach the zero-slot pool: {sql}: {other:?}"),
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        // The typed evaluation-origin 1105 is not a warning-buffer Error row.
        if id == 4 {
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1301,
                    message: "Result of rpad() was larger than \
                              max_allowed_packet (1024) - truncated"
                        .to_owned(),
                }]
            );
        } else {
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_log_pow_length_insert_sql_values_metadata_and_warnings() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_log_pow_length_insert_sql_columns() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_log_pow_length_insert_zero (id INT PRIMARY KEY, \
             x DOUBLE, base DOUBLE, e DOUBLE, z VARBINARY(8), t VARCHAR(16), \
             pos BIGINT, n BIGINT, r VARCHAR(1100))",
        )
        .unwrap();
    let replacement = "中".repeat(342);
    session
        .run(&format!(
            "INSERT INTO shared_log_pow_length_insert_zero VALUES \
             (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),\
             (2,1,2,2,X'FFFFFFFF00','abc',2,1,'{replacement}'),\
             (3,0,2,2,X'',NULL,NULL,NULL,NULL),\
             (4,NULL,NULL,NULL,X'000000',NULL,NULL,NULL,NULL)"
        ))
        .unwrap();
    session
        .vars
        .set_system("max_allowed_packet", "1024".to_owned())
        .unwrap();
    assert_eq!(session.max_allowed_packet(), 1024);
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Six families, NULL/non-NULL, with both LOG arities represented. The
    // final two calls retain frontend diagnostics before the typed refusal.
    for (expression, id) in [
        ("LN(x)", 1),
        ("LN(x)", 2),
        ("LOG(x)", 1),
        ("LOG(base,x)", 2),
        ("LOG2(x)", 1),
        ("LOG2(x)", 2),
        ("POW(x,e)", 1),
        ("POW(x,e)", 2),
        ("UNCOMPRESSED_LENGTH(z)", 1),
        ("UNCOMPRESSED_LENGTH(z)", 2),
        ("INSERT(t,pos,n,r)", 1),
        ("INSERT(t,pos,n,r)", 2),
        ("LOG(x)", 3),
        ("UNCOMPRESSED_LENGTH(z)", 4),
    ] {
        let sql =
            format!("SELECT {expression} FROM shared_log_pow_length_insert_zero WHERE id={id}");
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
            other => {
                panic!("log/pow/length/insert SQL must reach the zero-slot pool: {sql}: {other:?}")
            }
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        let expected_warning = match id {
            3 => Some((3020, "Invalid argument for logarithm")),
            4 => Some((1259, "ZLIB: Input data corrupted")),
            _ => None,
        };
        if let Some((code, message)) = expected_warning {
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code,
                    message: message.to_owned(),
                }],
                "{sql}"
            );
        } else {
            // INSERT id=2 would compute 1028 bytes, but packet policy is only
            // read AFTER a successful kernel result. No pre-1301 is allowed.
            // Evaluation-origin 1105 is returned, never an Error warning row.
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_substring_dispatch_sql_values_metadata_and_diagnostics() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_substring_dispatch_sql_columns() {
    let mut session = Session::new();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_substring_dispatch_zero (id INT PRIMARY KEY, \
             t VARCHAR(16), b VARBINARY(16), p BIGINT, n BIGINT, s VARCHAR(8))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_substring_dispatch_zero VALUES \
             (1,NULL,NULL,2,1,'bad'),(2,'abcd',X'61626364',0,1,'bad'),\
             (3,'abcd',X'61626364',2,NULL,'bad'),\
             (4,'abcd',X'61626364',2,9223372036854775807,'bad')",
        )
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Existing legal NULL, empty, ordinary and overflow-empty results must
    // still ask for a worker. These are ordinary SQL, not new PB admission.
    for (expression, id, warns) in [
        ("SUBSTRING(t,p)", 1, false),
        ("MID(t,p,n)", 1, false),
        ("SUBSTR(t,p)", 2, false),
        ("SUBSTRING(t,p,n)", 2, false),
        ("SUBSTRING(t,p)", 3, false),
        ("SUBSTR(t,p,n)", 3, false),
        ("MID(b,p,n)", 4, false),
        ("SUBSTRING(t,s)", 2, false),
        ("SUBSTRING(t,s,n)", 2, true),
    ] {
        let sql = format!("SELECT {expression} FROM shared_substring_dispatch_zero WHERE id={id}");
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
            other => panic!("substring dispatch must reach the zero-slot pool: {sql}: {other:?}"),
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        if warns {
            // Only the pre-existing three-argument diagnostic survives. The
            // returned evaluation-origin 1105 is not an Error warning row.
            assert_eq!(
                session.warnings(),
                &[SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1292,
                    message: "Truncated incorrect INTEGER value: 'bad'".to_owned(),
                }],
                "{sql}"
            );
        } else {
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_collated_search_set_dispatch_sql_values_metadata_and_cache() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_collated_search_set_dispatch_sql_columns() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_collated_search_set_zero (id INT PRIMARY KEY, \
             h VARCHAR(16) CHARSET utf8mb4 COLLATE utf8mb4_general_ci, \
             n VARCHAR(16) CHARSET utf8mb4 COLLATE utf8mb4_general_ci, p BIGINT, \
             f VARCHAR(16) CHARSET utf8mb4 COLLATE utf8mb4_general_ci, \
             v VARCHAR(32) CHARSET utf8mb4 COLLATE utf8mb4_general_ci)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_collated_search_set_zero VALUES \
             (1,NULL,NULL,NULL,NULL,NULL),\
             (2,'é','e',1,' ','  , , ,'),(3,'','',1,'','')",
        )
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Every family is called directly on NULL, ordinary, and empty columns;
    // neither a legal result nor a constant-list cache hit may bypass C4.
    // LOCATE3 and the constant-list cache get the same three probes as well.
    for expression in [
        "STRCMP(n,h)",
        "LOCATE(n,h)",
        "LOCATE(n,h,p)",
        "INSTR(h,n)",
        "POSITION(n IN h)",
        "FIND_IN_SET(f,v)",
        "FIND_IN_SET(f,'  , , ,')",
    ] {
        for id in 1..=3 {
            let sql =
                format!("SELECT {expression} FROM shared_collated_search_set_zero WHERE id={id}");
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
                other => {
                    panic!("collated search/set must reach the zero-slot pool: {sql}: {other:?}")
                }
            }
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, 1105, "{sql}");
            assert_eq!(mysql.state, *b"HY000", "{sql}");
            assert!(mysql.is_from_evaluation(), "{sql}");
            // No cast/packet diagnostics, and the returned evaluation-origin
            // 1105 is not appended as an Error warning row.
            assert!(warnings_of(&session).is_empty(), "{sql}");
        }
    }
}

#[test]
fn evaluated_ascii_shared_pool_variadic_oct_elt_dispatch_sql_values_and_metadata() {
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
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(1))
        .unwrap());

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
fn evaluated_ascii_shared_pool_zero_slots_reject_variadic_oct_elt_dispatch_sql_columns() {
    let mut session = Session::new();
    session.run("SET NAMES utf8mb4").unwrap();
    session
        .run("SET tidb_executor_concurrency=1, tidb_projection_concurrency=1")
        .unwrap();
    session
        .run(
            "CREATE TABLE shared_variadic_oct_elt_zero (id INT PRIMARY KEY, \
             o VARCHAR(32) CHARSET utf8mb4, u BIGINT UNSIGNED, k BIGINT, \
             t VARCHAR(8) CHARSET utf8mb4, s VARCHAR(1) CHARSET utf8mb4)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO shared_variadic_oct_elt_zero VALUES \
             (1,NULL,NULL,NULL,NULL,NULL),\
             (2,'\u{00a0}8',18446744073709551615,5,'z','|'),\
             (3,'',0,1,'',''),(4,' ',8,0,'A','|'),(5,'8',16,1,NULL,'|')",
        )
        .unwrap();
    assert!(session
        .try_install_evaluated_ascii_policy(ascii_session_policy(0))
        .unwrap());

    // Calls are direct and column-driven. Legal NULL and empty results still
    // demand a worker, including an invalid or selected-NULL ELT result.
    for (expression, id) in [
        ("OCT(o)", 1),
        ("OCT(o)", 2),
        ("OCT(o)", 3),
        ("OCT(o)", 4),
        ("OCT(u)", 2),
        ("CONCAT(t,'','','','',t)", 1),
        ("CONCAT(t,'','','','',t)", 2),
        ("CONCAT(t,'','','','',t)", 3),
        ("CONCAT_WS(s,t,'','','',t)", 1),
        ("CONCAT_WS(s,t,'','','',t)", 2),
        ("CONCAT_WS(s,t,'','','',t)", 3),
        ("ELT(k,t,'2','3','4','5','6')", 1),
        ("ELT(k,t,'2','3','4','5','6')", 2),
        ("ELT(k,t,'2','3','4','5','6')", 3),
        ("ELT(k,t,'2','3','4','5','6')", 4),
        ("ELT(k,t,'2','3','4','5','6')", 5),
    ] {
        let sql = format!("SELECT {expression} FROM shared_variadic_oct_elt_zero WHERE id={id}");
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
            other => panic!("variadic/OCT/ELT must reach the zero-slot pool: {sql}: {other:?}"),
        }
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1105, "{sql}");
        assert_eq!(mysql.state, *b"HY000", "{sql}");
        assert!(mysql.is_from_evaluation(), "{sql}");
        // These tiny legal operands introduce no cast/packet diagnostic, and
        // the returned evaluation-origin 1105 is never an Error warning row.
        assert!(warnings_of(&session).is_empty(), "{sql}");
    }
}
