//! The INTERVAL(N, N1, N2, ...) comparison function: returns the index of
//! the first pivot strictly greater than N (binary-search semantics), -1
//! when N is NULL.

use tidb_session::Session;

fn assert_compare2_removed(session: &mut Session, sql: &str, former_expected: &str) {
    let Err(tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(
        tidb_executor::EvalError::Unsupported(message),
    ))) = session.run(sql)
    else {
        panic!("{sql}: expected comparison contraction; former {former_expected}")
    };
    assert_eq!(
        message, "native LEAST/GREATEST/INTERVAL evaluation was removed; TiKV engine required",
        "{sql}: former {former_expected}"
    );
}

#[test]
fn interval_oracles_now_contract() {
    let mut session = Session::new();
    for (sql, former_expected) in [
        ("select interval(5, 1, 2, 3)", "i:3"),
        ("select interval(2, 1, 3)", "i:1"),
        ("select interval(2, 1, 2, 3)", "i:2"),
        ("select interval(null, 1, 2)", "i:-1"),
    ] {
        assert_compare2_removed(&mut session, sql, former_expected);
    }
}
