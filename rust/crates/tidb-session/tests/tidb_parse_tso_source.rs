//! Current Go `pkg/expression/builtin_time.go` contracts for
//! `TIDB_PARSE_TSO` and `TIDB_PARSE_TSO_LOGICAL`.

use tidb_datatype::{Datum, FieldTypeCode, Time, TimeType};
use tidb_session::{Session, StmtOutput};

fn datetime(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    microsecond: i32,
) -> Datum {
    Datum::Time(
        Time::from_date_checked(
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            TimeType::DateTime,
            6,
        )
        .expect("the Go source vector is a valid DATETIME"),
    )
}

#[test]
fn tidb_parse_tso_is_reachable_with_go_types_and_session_timezone() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();

    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT TIDB_PARSE_TSO(404411537129996288), \
                    TIDB_PARSE_TSO('404411537129996288'), \
                    TIDB_PARSE_TSO(1), \
                    TIDB_PARSE_TSO(0), \
                    TIDB_PARSE_TSO(-1)",
        )
        .unwrap()
    else {
        panic!("TSO parsing did not return a result set")
    };

    for (_, field_type) in &columns[..5] {
        assert_eq!(field_type.code(), FieldTypeCode::Datetime);
        assert_eq!(field_type.flen(), 10);
        assert_eq!(field_type.decimal(), 0);
    }
    assert_eq!(
        rows,
        vec![vec![
            datetime(2018, 11, 20, 9, 53, 4, 877_000),
            datetime(2018, 11, 20, 9, 53, 4, 877_000),
            datetime(1970, 1, 1, 0, 0, 0, 0),
            Datum::Null,
            Datum::Null,
        ]]
    );

    for (sql, former) in [
        ("SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996288)", 0),
        ("SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996289)", 1),
        ("SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996290)", 2),
    ] {
        let Err(tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::Unsupported(message),
        ))) = session.run(sql)
        else {
            panic!("{sql}: expected temporal-tail contraction; former {former}")
        };
        assert_eq!(
            message, "native temporal tail evaluation was removed; function unsupported",
            "{sql}: former {former}"
        );
    }

    session.run("SET time_zone = '+08:00'").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT TIDB_PARSE_TSO(404411537129996288)")
        .unwrap()
    else {
        panic!("TSO parsing did not return a result set")
    };
    assert_eq!(rows, vec![vec![datetime(2018, 11, 20, 17, 53, 4, 877_000)]]);
}
