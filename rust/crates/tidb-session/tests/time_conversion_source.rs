//! The time/period conversion family: SEC_TO_TIME and TIME_TO_SEC are
//! exact inverses, MAKEDATE turns (year, day-of-year) into a date,
//! MAKETIME composes a duration (12:15:30 = 44130s), and PERIOD_ADD/
//! PERIOD_DIFF operate on YYMM periods.

use tidb_session::Session;

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

#[test]
fn seconds_hours_and_periods() {
    let mut session = Session::new();

    assert_temporal_value_removed(&mut session, "select time_to_sec('01:01:01')", "i:3661");
    assert_temporal_value_removed(
        &mut session,
        "select time_to_sec(sec_to_time(3661))",
        "i:3661",
    );
    assert_temporal_value_removed(&mut session, "select makedate(2024, 61)", "DATE:2024-03-01");
    assert_temporal_value_removed(
        &mut session,
        "select period_add(202401, 11), period_diff(202401, 202302)",
        "i:202412|i:11",
    );
}
