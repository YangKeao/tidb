//! QUOTE renders a re-parseable SQL literal (doubling-quote style with
//! backslash escapes) and ORD/ASCII answer character codes — ORD computes
//! the multibyte first-character arithmetic (ORD('中') = 228*65536 +
//! 184*256 + 173 = 14989485) while ASCII('') is 0.

use tidb_session::Session;
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;

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
fn quoting_and_character_codes() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));

    // QUOTE doubles the quote so the output re-parses to the input.
    assert_eq!(try_sql(&mut session, "select quote('a''b')"), "s:'a\\'b'");

    assert_eq!(try_sql(&mut session, "select ord('A')"), "i:65");
    #[cfg(feature = "tikv-expr")]
    {
        let engine_before = session.tikv_expression_rows();
        assert_eq!(
            try_sql(&mut session, "select ascii('A'), ascii('')"),
            "i:65|i:0"
        );
        assert!(session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    {
        let error = session
            .run("select ascii('A')")
            .expect_err("native ASCII kernel is deleted")
            .to_string();
        assert!(error.contains("native string2 evaluation was removed; TiKV engine required or function unsupported"), "{error}");
    }

    // ORD on a multibyte character composes its leading bytes.
    assert_eq!(try_sql(&mut session, "select ord('中')"), "i:14989485");
}
