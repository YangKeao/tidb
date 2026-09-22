//! Native result domains and metadata for temporal builtins.

use crate::tests_support::row_text;
use crate::*;

fn assert_removed(session: &mut Session, sql: &str, marker: &str, former: &str) {
    let error = session
        .run(sql)
        .expect_err("removed native kernel must refuse");
    assert!(
        error.to_string().contains(marker),
        "{sql}: {error}; former oracle: {former}"
    );
}

/// Go's `types.ETDatetime` argument declaration over real columns, where the
/// static `YEAR` type selects `ParseTimeFromYear` and other integers select
/// `ParseTimeFromNum`.
#[test]
fn an_etdatetime_argument_is_cast_from_its_column_type() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE yt (y YEAR, n BIGINT, d DATE)")
        .unwrap();
    session
        .run("INSERT INTO yt VALUES (2024, 20240315123045, '2024-03-15')")
        .unwrap();

    for (sql, former) in [
        (
            "SELECT month(y), day(y), quarter(y), year(y) FROM yt",
            "0,0,0,2024",
        ),
        (
            "SELECT month(n), day(n), quarter(n), year(n) FROM yt",
            "3,15,1,2024",
        ),
        ("SELECT month(d), quarter(d), year(d) FROM yt", "3,1,2024"),
    ] {
        assert_removed(
            &mut session,
            sql,
            "native calendar component evaluation was removed; TiKV engine required",
            former,
        );
    }
    assert_eq!(
        row_text(session.run(
            "SELECT to_days(n), date_format(n,'%Y-%m'), \
             timestampdiff(day,'2024-01-01',n) FROM yt"
        )),
        [["739325", "2024-03", "74"]]
    );
    assert_removed(
        &mut session,
        "SELECT timestampadd(day,1,n) FROM yt",
        "native temporal residual evaluation was removed; function unsupported",
        "2024-03-16 12:30:45",
    );
}

#[test]
fn in_casts_every_candidate_to_the_first_arguments_temporal_domain() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE in_time (d DATETIME, ds VARCHAR(32), t TIME, ts VARCHAR(32))")
        .unwrap();
    session
        .run(
            "INSERT INTO in_time VALUES \
             ('2024-03-15 12:30:45', '2024-03-15 12:30:45', '01:00:00', '1:00:00')",
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT d IN (ds), t = ts, t IN (ts) FROM in_time")),
        [["1", "0", "1"]]
    );
}

#[test]
fn current_clock_builtins_return_native_temporal_values() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session.run("SET timestamp = 1700000000").unwrap();
    let sql = "SELECT NOW(3), UTC_TIMESTAMP(6), CURDATE(), UTC_DATE(), CURTIME(3), UTC_TIME(6)";
    let error = session
        .run(sql)
        .expect_err("native clock execution is deleted");
    assert!(
        error
            .to_string()
            .contains("native temporal clock evaluation was removed; function unsupported"),
        "{error}; former metadata: DATETIME(3/6), DATE(0), DURATION(3/6)"
    );
}

#[test]
fn date_constructors_return_native_dates() {
    let mut session = Session::new();
    for (sql, marker, former) in [
        (
            "SELECT LAST_DAY('2024-02-10')",
            "native calendar component evaluation was removed; TiKV engine required",
            "DATE 2024-02-29, flen 10, decimal 0",
        ),
        (
            "SELECT MAKEDATE(2024, 60)",
            "native temporal value evaluation was removed; TiKV engine required",
            "DATE 2024-02-29, flen 10, decimal 0",
        ),
        (
            "SELECT FROM_DAYS(TO_DAYS('2024-02-29'))",
            "native temporal residual evaluation was removed; function unsupported",
            "DATE 2024-02-29, flen 10, decimal 0",
        ),
    ] {
        assert_removed(&mut session, sql, marker, former);
    }
}

#[test]
fn duration_constructors_preserve_source_scale() {
    let mut session = Session::new();
    for (sql, marker, former) in [
        (
            "SELECT SEC_TO_TIME(CAST(1.25 AS DECIMAL(10,2)))",
            "native temporal tail evaluation was removed; function unsupported",
            "Duration 00:00:01.25, flen 13, decimal 2",
        ),
        (
            "SELECT MAKETIME(1, 2, CAST(3.456 AS DECIMAL(10,3)))",
            "native temporal value evaluation was removed; TiKV engine required",
            "Duration 01:02:03.456, flen 14, decimal 3",
        ),
    ] {
        let error = session
            .run(sql)
            .expect_err("native duration constructor is deleted");
        assert!(
            error.to_string().contains(marker),
            "{error}; former {former}"
        );
    }
}

#[test]
fn str_to_date_uses_the_format_to_choose_its_native_domain() {
    let mut session = Session::new();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns(
            "SELECT STR_TO_DATE('2024-02-29', '%Y-%m-%d'), \
                    STR_TO_DATE('12:34:56.123456', '%H:%i:%s.%f'), \
                    STR_TO_DATE('2024-02-29 12:34:56.123456', '%Y-%m-%d %H:%i:%s.%f')",
        )
        .unwrap()
    else {
        panic!("STR_TO_DATE did not return rows")
    };
    for (index, code, flen, decimal) in [
        (0, tidb_datatype::FieldTypeCode::Date, 10, 0),
        (1, tidb_datatype::FieldTypeCode::Duration, 17, 6),
        (2, tidb_datatype::FieldTypeCode::Datetime, 26, 6),
    ] {
        assert_eq!(columns[index].1.code(), code);
        assert_eq!(
            (columns[index].1.flen(), columns[index].1.decimal()),
            (flen, decimal)
        );
    }
    assert!(matches!(rows[0][0], Datum::Time(_)));
    assert!(matches!(rows[0][1], Datum::Duration(_)));
    assert!(matches!(rows[0][2], Datum::Time(_)));

    session.run("CREATE TABLE f (format VARCHAR(20))").unwrap();
    session.run("INSERT INTO f VALUES ('%Y-%m-%d')").unwrap();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns("SELECT STR_TO_DATE('2024-02-29', format) FROM f")
        .unwrap()
    else {
        panic!("dynamic STR_TO_DATE did not return rows")
    };
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (26, 6));
    assert!(matches!(rows[0][0], Datum::Time(_)));

    session.run("UPDATE f SET format = '%H:%i:%s'").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT STR_TO_DATE('12:34:56', format) FROM f")
        .unwrap()
    else {
        panic!("dynamic time-only STR_TO_DATE did not return rows")
    };
    assert_eq!(rows[0][0], Datum::Null);
    session.run("SET sql_mode = ''").unwrap();
    let StmtOutput::Rows { rows, .. } = session
        .run_with_columns("SELECT STR_TO_DATE('12:34:56', format) FROM f")
        .unwrap()
    else {
        panic!("relaxed dynamic STR_TO_DATE did not return rows")
    };
    assert!(matches!(rows[0][0], Datum::Time(_)));
    assert_eq!(
        rows[0][0].sql_string().unwrap(),
        "0000-00-00 12:34:56.000000"
    );
}

#[test]
fn temporal_difference_and_zone_conversion_return_native_values() {
    let mut session = Session::new();
    assert_removed(
        &mut session,
        "SELECT TIMEDIFF('2024-01-02 00:00:00.123', '2024-01-01 23:59:59.120')",
        "native temporal value evaluation was removed; TiKV engine required",
        "DURATION 00:00:01.003, flen 14, decimal 3",
    );
    for (sql, former) in [
        (
            "SELECT CONVERT_TZ('2024-01-01 00:00:00.123', '+00:00', '+08:00')",
            "DATETIME 2024-01-01 08:00:00.123, flen 23, decimal 3",
        ),
        (
            "SELECT CONVERT_TZ('bad.prefix.12', '+00:00', '+08:00')",
            "NULL DATETIME, flen 22, decimal 2",
        ),
    ] {
        assert_removed(
            &mut session,
            sql,
            "native temporal residual evaluation was removed; function unsupported",
            former,
        );
    }
}

#[test]
fn from_unixtime_preserves_its_one_argument_datetime_signature() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    let StmtOutput::Rows { columns, rows, .. } = session
        .run_with_columns(
            "SELECT FROM_UNIXTIME(CAST(1700000000.123 AS DECIMAL(20,3))), \
                    FROM_UNIXTIME(1700000000), FROM_UNIXTIME(1700000000, '%Y')",
        )
        .unwrap()
    else {
        panic!("FROM_UNIXTIME did not return rows")
    };
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((columns[0].1.flen(), columns[0].1.decimal()), (23, 3));
    assert!(matches!(rows[0][0], Datum::Time(_)));
    assert_eq!(rows[0][0].sql_string().unwrap(), "2023-11-14 22:13:20.123");
    assert_eq!(columns[1].1.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((columns[1].1.flen(), columns[1].1.decimal()), (19, 0));
    assert!(matches!(rows[0][1], Datum::Time(_)));
    assert!(columns[2].1.code().is_string());
    assert!(matches!(rows[0][2], Datum::String(_)));
    assert_eq!(rows[0][2].sql_string().unwrap(), "2023");
}
