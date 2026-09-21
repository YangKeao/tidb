//! BINARY(4) zero-pads on store: `'a'` becomes 0x61000000 (`hex` output),
//! the 1-byte literal does NOT compare equal (`b = 'a'` is false), and the
//! explicit 4-byte form does (`b = 'a\0\0\0'`).

use tidb_session::Session;
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn binary_pads_with_nul_bytes() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
    session
        .run("create table t (id int primary key, b binary(4))")
        .unwrap();
    session.run("insert into t values (1, 'a')").unwrap();

    // HEX over a typed BINARY column executes only in TiKV.
    #[cfg(feature = "tikv-expr")]
    {
        let engine_before = session.tikv_expression_rows();
        let hex_row = rows(&mut session, "select hex(b) from t");
        assert!(
            hex_row.contains("54, 49, 48, 48, 48, 48, 48, 48"),
            "{hex_row}"
        );
        assert!(session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    {
        let error = session
            .run("select hex(b) from t")
            .expect_err("native HEX kernel is deleted")
            .to_string();
        assert!(error.contains("native integer radix evaluation was removed; TiKV engine required or function unsupported"), "{error}");
    }
    assert_eq!(
        first_count(&mut session, "select count(*) from t where b = 'a'"),
        "Int(0)"
    );
    assert_eq!(
        first_count(&mut session, r"select count(*) from t where b = 'a\0\0\0'"),
        "Int(1)"
    );
}

fn first_count(session: &mut Session, sql: &str) -> String {
    rows(session, sql)
}
