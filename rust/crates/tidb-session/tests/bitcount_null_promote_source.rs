//! BIT_COUNT's population count (64 for the full 64-bit pattern of -1),
//! DATE_FORMAT's NULL propagation, and IFNULL's string promotion making
//! `ifnull(1, 'x') = '1'` compare as strings.

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
fn popcount_null_and_promotion() {
    let mut session = Session::new();

    // 7 -> 3 bits; 0 -> 0; -1 -> all 64 bits of the two's complement.
    #[cfg(feature = "tikv-expr")]
    {
        session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
        let engine_before = session.tikv_expression_rows();
        assert_eq!(
            try_sql(
                &mut session,
                "select bit_count(7), bit_count(0), bit_count(-1)"
            ),
            "i:3|i:0|i:64"
        );
        assert!(session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    {
        let error = session
            .run("select bit_count(7)")
            .expect_err("native BIT_COUNT kernel is deleted")
            .to_string();
        assert!(error.contains("native integer radix evaluation was removed; TiKV engine required or function unsupported"), "{error}");
    }

    // NULL date -> NULL string.
    assert_eq!(
        try_sql(&mut session, "select date_format(null, '%Y')"),
        "Null"
    );

    // IFNULL promotes to the string type, so the comparison is textual.
    assert_eq!(try_sql(&mut session, "select ifnull(1, 'x') = '1'"), "i:1");
}
