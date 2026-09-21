//! Preserves the former CHECK/NULL independent outcomes while explicitly
//! pinning this local ISNULL-dependent constraint shape as unsupported.

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
        .run("create table t (a int primary key, b int null, check (b is null or b > 0))")
        .unwrap();
}

#[test]
fn check_null_allowance_contracts_without_local_isnull() {
    let mut session = Session::new();
    setup(&mut session);

    crate::assert_removed_misc(&mut session, "insert into t values (1, NULL)", "accepted");
    crate::assert_removed_misc(
        &mut session,
        "insert into t values (2, -5)",
        "check 't_chk_1' is violated",
    );
    crate::assert_removed_misc(&mut session, "insert into t values (3, 7)", "accepted");
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "");
}
