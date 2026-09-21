//! LOWER/UPPER: ASCII round-trips, accented Latin-1 letters fold (É -> é,
//! é -> É), the Greek final-sigma hazard folds with the simple mapping
//! (Σ -> σ, not ς), and NULL propagates.

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
                        tidb_datatype::Datum::Bytes(v) => {
                            format!("s:{}", String::from_utf8_lossy(v))
                        }
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
fn case_mapping_rules() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
    #[cfg(feature = "tikv-expr")]
    let engine_before = session.tikv_expression_rows();

    #[cfg(feature = "tikv-expr")]
    {
        assert_eq!(
            try_sql(&mut session, "select lower('AbC'), upper('AbC')"),
            "s:abc|s:ABC"
        );
        assert_eq!(
            try_sql(&mut session, "select lower('ÉÀ'), upper('éà')"),
            "s:éà|s:ÉÀ"
        );
        assert_eq!(try_sql(&mut session, "select lower(null)"), "Null");
        // NOT the word-final sigma a full Unicode fold would produce.
        assert_eq!(try_sql(&mut session, "select lower('Σ')"), "s:σ");
        assert!(session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    for sql in ["select lower('AbC')", "select upper('AbC')"] {
        let error = session
            .run(sql)
            .expect_err("native case kernel is deleted")
            .to_string();
        assert!(error.contains("native string2 evaluation was removed; TiKV engine required or function unsupported"), "{sql}: {error}");
    }

    // Go does not propagate a NOT NULL column flag through UPPER. Only a
    // folded non-NULL constant becomes NOT NULL metadata.
    session
        .run("create table case_meta (s varchar(8) not null)")
        .unwrap();
    session
        .run("create view case_meta_v as select upper(s) as u from case_meta")
        .unwrap();
    assert_eq!(
        try_sql(&mut session, "show columns from case_meta_v"),
        "s:u|s:varchar(8)|s:YES|s:|Null|s:"
    );
}
