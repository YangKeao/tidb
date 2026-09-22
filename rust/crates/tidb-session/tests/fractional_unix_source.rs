//! Former fractional `UNIX_TIMESTAMP` scale rows after native deletion.

use tidb_session::Session;

#[test]
fn fractional_timestamp_scale() {
    let mut session = Session::new();
    for (sql, former) in [
        (
            "select unix_timestamp('2024-01-01 00:00:00')",
            "integer 1704038400",
        ),
        (
            "select unix_timestamp('2024-01-01 00:00:00.5')",
            "decimal 1704038400.5, scale 1",
        ),
        (
            "select unix_timestamp('2024-01-01 00:00:00.4')",
            "decimal 1704038400.4, scale 1",
        ),
    ] {
        let error = session
            .run(sql)
            .expect_err("native UNIX_TIMESTAMP must not run");
        assert!(
            error
                .to_string()
                .contains("native session temporal evaluation was removed; TiKV engine required"),
            "{sql}: {error}; former oracle: {former}"
        );
    }
}
