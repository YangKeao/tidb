//! CONCAT_WS skips NULL arguments but a NULL separator makes the whole
//! result NULL; TRIM/LTRIM/RTRIM and the remstr forms (BOTH/LEADING with a
//! custom character) strip as written.

use tidb_session::Session;
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;

fn assert_packet_string_removed(session: &mut Session, sql: &str) {
    let error = session
        .run(sql)
        .expect_err("deleted packet-limited string kernel must refuse");
    assert!(
        error
            .to_string()
            .contains("native packet-limited string evaluation was removed; function unsupported"),
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
fn concat_ws_and_trim_forms() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
    #[cfg(feature = "tikv-expr")]
    let engine_before = session.tikv_expression_rows();

    // Packet-context semantics are not exposed by the shared TiKV facade, so
    // both ordinary and NULL shapes fail closed before argument evaluation.
    assert_packet_string_removed(&mut session, "select concat_ws('-', 'a', null, 'b')");
    assert_packet_string_removed(&mut session, "select concat_ws(null, 'a', 'b')");

    assert_eq!(rows(&mut session, "select trim('  ab  ')"), "s:ab");
    #[cfg(feature = "tikv-expr")]
    {
        assert_eq!(rows(&mut session, "select ltrim('  ab  ')"), "s:ab  ");
        assert_eq!(rows(&mut session, "select rtrim('  ab  ')"), "s:  ab");
    }
    #[cfg(not(feature = "tikv-expr"))]
    for sql in ["select ltrim('  ab  ')", "select rtrim('  ab  ')"] {
        assert!(
            session
                .run(sql)
                .expect_err("native LTRIM/RTRIM kernels are deleted")
                .to_string()
                .contains(
                    "native string2 evaluation was removed; TiKV engine required or function unsupported"
                ),
            "{sql}"
        );
    }

    // remstr forms.
    assert_eq!(
        rows(&mut session, "select trim(both 'x' from 'xxabxx')"),
        "s:ab"
    );
    assert_eq!(
        rows(&mut session, "select trim(leading 'x' from 'xxab')"),
        "s:ab"
    );
    #[cfg(feature = "tikv-expr")]
    assert!(session.tikv_expression_rows() > engine_before);
}
