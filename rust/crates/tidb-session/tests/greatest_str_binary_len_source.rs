//! String GREATEST picks the lexicographic maximum, and BINARY strings
//! lose character semantics: length and char_length both count bytes
//! (binary('中a') -> 4/4, not 4/2).

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

fn assert_string_length_removed(session: &mut Session, sql: &str, former_expected: &str) {
    let Err(tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(
        tidb_executor::EvalError::Unsupported(message),
    ))) = session.run(sql)
    else {
        panic!("{sql}: expected length contraction; former {former_expected}")
    };
    assert_eq!(
        message, "native string length evaluation was removed; TiKV engine required",
        "{sql}: former {former_expected}"
    );
}

#[test]
fn string_extremum_and_binary_lengths_contract() {
    let mut session = Session::new();

    // Lexicographic maximum ('b' > 'ab' > 'aa').
    assert_compare2_removed(&mut session, "select greatest('b', 'aa', 'ab')", "s:b");

    // Former binary result: both cells counted four bytes.
    assert_string_length_removed(
        &mut session,
        "select length(binary('中a')), char_length(binary('中a'))",
        "i:4|i:4",
    );
}
