//! Preserves the former generated-column CHECK outcomes as independent oracles
//! while pinning the local ISNULL-dependent constraint shape as unsupported.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int as (if(a % 2 = 0, null, a)) virtual, check (b is null or b > 0))")
        .unwrap();
}

#[test]
fn virtual_null_check_contracts_without_local_isnull() {
    let mut session = Session::new();
    setup(&mut session);

    crate::assert_removed_misc(
        &mut session,
        "insert into t (a) values (2)",
        "accepted as 2|NULL",
    );
    crate::assert_removed_misc(
        &mut session,
        "insert into t (a) values (1)",
        "accepted as 1|1",
    );
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "");
}
