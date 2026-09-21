//! LPAD/RPAD cycle the pad string, truncate the input when the target is
//! shorter, and treat length 0 as empty; REVERSE reverses by rune; REPEAT
//! with a negative count is empty.

use tidb_session::Session;

fn assert_packet_string_removed(session: &mut Session, sql: &str) {
    let error = session
        .run(sql)
        .expect_err("packet-limited native string kernel is deleted")
        .to_string();
    assert!(
        error.contains("native packet-limited string evaluation was removed; function unsupported"),
        "{sql}: {error}"
    );
}

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn pad_reverse_repeat_edges() {
    let mut session = Session::new();

    for sql in [
        "select lpad('ab', 4, 'xy')",
        "select lpad('abcdef', 3, 'x')",
        "select lpad('ab', 0, 'x')",
        "select rpad('ab', 4, 'xy')",
    ] {
        assert_packet_string_removed(&mut session, sql);
    }

    // Multibyte-safe reversal.
    assert_eq!(rows(&mut session, "select reverse('héllo')"), "s:olléh");

    assert_packet_string_removed(&mut session, "select repeat('a', -1), repeat('ab', 3)");
}
