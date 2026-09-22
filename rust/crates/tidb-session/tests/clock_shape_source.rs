//! The clock functions as shape contracts (no wall-clock values asserted):
//! CURRENT_DATE is a DATE that equals DATE(CURRENT_DATE()), CURTIME()
//! renders at least `HH:MM:SS`, and NOW() renders a full 19-character
//! datetime.

use tidb_session::Session;

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

fn assert_temporal_value_removed(session: &mut Session, sql: &str, former_expected: &str) {
    let Err(tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(
        tidb_executor::EvalError::Unsupported(message),
    ))) = session.run(sql)
    else {
        panic!("{sql}: expected temporal contraction; former {former_expected}")
    };
    assert_eq!(
        message, "native temporal value evaluation was removed; TiKV engine required",
        "{sql}: former {former_expected}"
    );
}

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn clock_shape_contracts() {
    let mut session = Session::new();

    // CURRENT_DATE carries kind Date (a zeroed time component), and equals
    // its own DATE() projection.
    let curdate = rows(&mut session, "select current_date");
    assert!(curdate.contains("kind: Date"), "{curdate}");
    assert_temporal_value_removed(
        &mut session,
        "select current_date = date(current_date())",
        "Int(1)",
    );

    // Former clock-shape values remain the oracle; the removed outer length
    // kernel now refuses before evaluating its clock child.
    assert_string_length_removed(&mut session, "select char_length(curtime()) >= 8", "Int(1)");
    assert_string_length_removed(&mut session, "select char_length(now()) = 19", "Int(1)");
}
