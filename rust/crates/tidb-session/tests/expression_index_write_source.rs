//! Expression (functional) index writes: `unique index ((lower(s)))`
//! enforces uniqueness over the COMPUTED value — 'Hello' and 'HELLO'
//! collide with Go's entry text — while a distinct value inserts fine.

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
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
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
fn expression_index_enforces_the_computed_value() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
    session
        .run("create table t (a int primary key, s varchar(8), unique index uq ((lower(s))))")
        .unwrap();

    #[cfg(feature = "tikv-expr")]
    {
        let engine_before = session.tikv_expression_rows();
        session.run("insert into t values (1, 'Hello')").unwrap();
        // 'HELLO' lowercases onto the indexed value: refused.
        let error = session
            .run("insert into t values (2, 'HELLO')")
            .expect_err("the lowercased duplicate must fail");
        assert!(
            error
                .to_string()
                .contains("Duplicate entry 'hello' for key 't.uq'"),
            "{error}"
        );
        // A genuinely distinct value inserts.
        session.run("insert into t values (3, 'world')").unwrap();
        assert_eq!(rows(&mut session, "select a from t order by a"), "1;3");
        assert!(
            session.tikv_expression_rows() > engine_before,
            "functional-index LOWER must execute through TiKV"
        );
    }
    #[cfg(not(feature = "tikv-expr"))]
    {
        let error = session
            .run("insert into t values (1, 'Hello')")
            .expect_err("functional-index LOWER has no native kernel")
            .to_string();
        assert!(error.contains("native string2 evaluation was removed; TiKV engine required or function unsupported"), "{error}");
    }
}
