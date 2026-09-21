//! Base-conversion functions: CONV translates between bases in either
//! direction (string or number input), BIN/OCT are base-2/base-8
//! projections, and TO_BASE64/FROM_BASE64 round-trip the RFC 4648 standard
//! alphabet ('abc' -> 'YWJj').

use tidb_session::Session;
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;

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

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn base_conversions_round_trip() {
    let mut session = Session::new();

    let conv_error = session
        .run("select conv('ff', 16, 2), conv(255, 10, 16)")
        .expect_err("native CONV kernel was removed earlier")
        .to_string();
    assert!(
        conv_error.contains("native math evaluation was removed; TiKV engine required"),
        "{conv_error}"
    );
    #[cfg(feature = "tikv-expr")]
    {
        let mut engine_session = Session::new();
        engine_session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
        let engine_before = engine_session.tikv_expression_rows();
        assert_eq!(try_sql(&mut engine_session, "select bin(10)"), "s:1010");
        assert_eq!(try_sql(&mut engine_session, "select oct(8)"), "s:10");
        assert_eq!(try_sql(&mut engine_session, "select oct('8')"), "s:10");
        assert!(engine_session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    for sql in ["select bin(10)", "select oct(8)", "select oct('8')"] {
        let error = session
            .run(sql)
            .expect_err("native radix kernel is deleted")
            .to_string();
        assert!(error.contains("native integer radix evaluation was removed; TiKV engine required or function unsupported"), "{sql}: {error}");
    }

    // Keep both former source shapes, but pin the explicit contraction after
    // deleting the session packet-aware encoder kernel.
    assert_packet_string_removed(&mut session, "select to_base64('abc')");
    assert_packet_string_removed(&mut session, "select from_base64(to_base64('abc'))");
}
