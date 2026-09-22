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

fn assert_temporal_clock_removed(session: &mut Session, sql: &str, former_expected: &str) {
    let Err(tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(
        tidb_executor::EvalError::Unsupported(message),
    ))) = session.run(sql)
    else {
        panic!("{sql}: expected clock contraction; former {former_expected}")
    };
    assert_eq!(
        message, "native temporal clock evaluation was removed; function unsupported",
        "{sql}: former {former_expected}"
    );
}

#[test]
fn clock_shape_contracts() {
    let mut session = Session::new();

    // CURRENT_DATE carries kind Date (a zeroed time component), and equals
    // its own DATE() projection.
    for (sql, former) in [
        ("select now()", "DATETIME at statement clock"),
        ("select current_timestamp", "DATETIME at statement clock"),
        ("select localtime()", "DATETIME alias of NOW"),
        ("select localtimestamp(3)", "DATETIME(3) alias of NOW"),
        ("select utc_timestamp(6)", "UTC DATETIME(6)"),
        ("select curdate()", "DATE at session statement clock"),
        ("select current_date", "a Time value with kind Date"),
        ("select utc_date()", "DATE at UTC statement clock"),
        ("select curtime(3)", "session DURATION(3)"),
        ("select current_time", "session DURATION"),
        ("select utc_time(6)", "UTC DURATION(6)"),
        ("select sysdate()", "host or statement DATETIME"),
        (
            "select tidb_bounded_staleness('2021-05-10 14:42:41', '2021-05-10 14:42:43')",
            "bounded safe DATETIME",
        ),
        ("select tidb_current_tso()", "transaction TSO integer"),
    ] {
        assert_temporal_clock_removed(&mut session, sql, former);
    }
    assert_temporal_clock_removed(
        &mut session,
        "select current_date = date(current_date())",
        "Int(1)",
    );

    // Former clock-shape values remain the oracle; the removed outer length
    // kernel now refuses before evaluating its clock child.
    assert_string_length_removed(&mut session, "select char_length(curtime()) >= 8", "Int(1)");
    assert_string_length_removed(&mut session, "select char_length(now()) = 19", "Int(1)");
}
