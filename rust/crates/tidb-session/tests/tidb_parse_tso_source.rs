//! Current Go `pkg/expression/builtin_time.go` contracts for
//! `TIDB_PARSE_TSO` and `TIDB_PARSE_TSO_LOGICAL` after native deletion.

use tidb_session::Session;

fn assert_removed(session: &mut Session, sql: &str, marker: &str, former: &str) {
    let Err(tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(
        tidb_executor::EvalError::Unsupported(message),
    ))) = session.run(sql)
    else {
        panic!("{sql}: expected contraction; former {former}")
    };
    assert_eq!(message, marker, "{sql}: former {former}");
}

#[test]
fn tidb_parse_tso_is_reachable_with_go_types_and_session_timezone() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    assert_removed(
        &mut session,
        "SELECT TIDB_PARSE_TSO(404411537129996288)",
        "native temporal residual evaluation was removed; function unsupported",
        "DATETIME metadata (flen 10, decimal 0), 2018-11-20 09:53:04.877000 UTC",
    );
    for (sql, former) in [
        ("SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996288)", "0"),
        ("SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996289)", "1"),
        ("SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996290)", "2"),
    ] {
        assert_removed(
            &mut session,
            sql,
            "native temporal tail evaluation was removed; function unsupported",
            former,
        );
    }
    session.run("SET time_zone = '+08:00'").unwrap();
    assert_removed(
        &mut session,
        "SELECT TIDB_PARSE_TSO(404411537129996288)",
        "native temporal residual evaluation was removed; function unsupported",
        "2018-11-20 17:53:04.877000 at +08:00",
    );
}
