//! STRCMP's three-way result (-1/0/1) and the NULL-safe <=> operator:
//! NULL <=> NULL is TRUE and anything <=> NULL is FALSE — where the plain =
//! operator yields NULL for both.

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
fn three_way_and_null_safe() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));

    #[cfg(feature = "tikv-expr")]
    {
        let engine_before = session.tikv_expression_rows();
        assert_eq!(
            try_sql(
                &mut session,
                "select strcmp('a', 'b'), strcmp('b', 'a'), strcmp('a', 'a')"
            ),
            "i:-1|i:1|i:0"
        );
        assert!(session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    {
        let error = session
            .run("select strcmp('a', 'b')")
            .expect_err("native STRCMP kernel is deleted")
            .to_string();
        assert!(error.contains("native string2 evaluation was removed; TiKV engine required or function unsupported"), "{error}");
    }

    // <=> treats NULL as an ordinary comparable value.
    assert_eq!(
        try_sql(&mut session, "select 1 <=> 1, 1 <=> null, null <=> null"),
        "i:1|i:0|i:1"
    );

    // Plain = yields NULL with a NULL operand.
    assert_eq!(
        try_sql(&mut session, "select 1 = null, null = null"),
        "Null|Null"
    );
}
