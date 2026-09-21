//! FORMAT's thousands separators with rounding at the digit count, and
//! ROUND with negative digits: TiDB's FLOAT path rounds half away (2.5 ->
//! 3) while the exact-DECIMAL path rounds half-even (1250, -2 -> 1200) —
//! both oracle conventions, pinned side by side.

use tidb_session::Session;

fn assert_removed(session: &mut Session, sql: &str, marker: &str) {
    let error = session
        .run(sql)
        .expect_err("native kernel was deleted")
        .to_string();
    assert!(error.contains(marker), "{sql}: {error}");
}

#[test]
fn thousands_separators_and_negative_digits() {
    let mut session = Session::new();

    let string2 =
        "native string2 evaluation was removed; TiKV engine required or function unsupported";
    for sql in [
        "select format(1234567.891, 2)",
        "select format(1234567.891, 0)",
    ] {
        assert_removed(&mut session, sql, string2);
    }

    let math = "native math evaluation was removed; TiKV engine required";
    for sql in [
        "select round(1234, -2)",
        "select round(1250, -2)",
        "select round(2.5), round(-2.5)",
    ] {
        assert_removed(&mut session, sql, math);
    }
}
