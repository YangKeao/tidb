//! REPLACE substitutes every occurrence (none -> unchanged; an empty
//! search string changes nothing) and SPACE clamps negative counts to the
//! empty string; NULL input propagates.

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
fn replace_all_and_space_clamp() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));

    #[cfg(feature = "tikv-expr")]
    {
        let engine_before = session.tikv_expression_rows();
        // Every occurrence expands; no occurrence and an empty needle leave
        // the input unchanged; NULL propagates.
        for (sql, expected) in [
            ("select replace('aaa', 'a', 'bb')", "s:bbbbbb"),
            ("select replace('abc', 'z', 'y')", "s:abc"),
            ("select replace('abc', '', '-')", "s:abc"),
            ("select replace(null, 'a', 'b')", "Null"),
        ] {
            assert_eq!(try_sql(&mut session, sql), expected, "{sql}");
        }
        // Removing native constant folding must not make a non-NULL expression
        // nullable in persistent view metadata.
        session
            .run(
                r#"create view v as select cast(replace(substring_index(substring_index('', ',', 1), ':', -1), '"', '') as char(32)) as event_id"#,
            )
            .unwrap();
        assert_eq!(
            try_sql(&mut session, "show columns from v"),
            "s:event_id|s:varchar(32)|s:NO|s:|Null|s:"
        );
        assert!(session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    {
        let sql = "select replace('aaa', 'a', 'bb')";
        let error = session
            .run(sql)
            .expect_err("native REPLACE kernel is deleted")
            .to_string();
        assert!(error.contains("native string2 evaluation was removed; TiKV engine required or function unsupported"), "{sql}: {error}");
    }

    // Preserve all three SPACE source rows as an explicit contraction.
    assert_packet_string_removed(&mut session, "select space(3), space(0), space(-1)");
}
