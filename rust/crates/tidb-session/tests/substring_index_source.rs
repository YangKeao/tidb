//! SUBSTRING_INDEX count semantics: positive counts take everything before
//! the Nth delimiter from the left, negative counts from the right, zero
//! yields the empty string, and a missing delimiter returns the whole
//! input.

use tidb_session::Session;
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;

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
fn count_sign_and_missing_delimiter() {
    let mut session = Session::new();
    session
        .run("create table substring_index_counts(s varchar(16), c bigint)")
        .unwrap();
    session
        .run("insert into substring_index_counts values ('a.b.c', 2)")
        .unwrap();
    #[cfg(feature = "tikv-expr")]
    {
        session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
        let before = session.tikv_expression_rows();
        for (sql, want) in [
            ("select substring_index('a.b.c', '.', 2)", "s:a.b"),
            ("select substring_index('a.b.c', '.', -1)", "s:c"),
            ("select substring_index('a.b.c', '.', -2)", "s:b.c"),
            ("select substring_index('a.b.c', '.', 0)", "s:"),
            ("select substring_index('abc', '.', 2)", "s:abc"),
        ] {
            assert_eq!(rows(&mut session, sql), want, "{sql}");
        }
        let runtime_count_error = session
            .run("select substring_index(s, '.', c) from substring_index_counts")
            .expect_err("runtime SUBSTRING_INDEX count is an explicit contraction")
            .to_string();
        assert!(
            runtime_count_error.contains("native string auxiliary evaluation was removed; TiKV engine required or function unsupported"),
            "{runtime_count_error}"
        );
        assert!(session.tikv_expression_rows() > before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    for sql in [
        "select substring_index('a.b.c', '.', 2)",
        "select substring_index('a.b.c', '.', -1)",
        "select substring_index('a.b.c', '.', -2)",
        "select substring_index('a.b.c', '.', 0)",
        "select substring_index('abc', '.', 2)",
        "select substring_index(s, '.', c) from substring_index_counts",
    ] {
        let error = session
            .run(sql)
            .expect_err("native SUBSTRING_INDEX kernel is deleted")
            .to_string();
        assert!(
            error.contains("native string auxiliary evaluation was removed; TiKV engine required or function unsupported"),
            "{sql}: {error}"
        );
    }
}
