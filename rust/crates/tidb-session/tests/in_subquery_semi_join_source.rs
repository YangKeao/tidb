//! Retains the former IN-subquery result as an explicit contraction oracle;
//! the neighboring NOT IN subquery continues to verify its executable result.

use tidb_session::Session;

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
fn in_subquery_contracts_while_not_in_survives() {
    let mut session = Session::new();
    session
        .run("create table u (id int primary key, ref int)")
        .unwrap();
    session.run("create table v (ref int)").unwrap();
    session
        .run("insert into u values (1, 10), (2, 20), (3, 30)")
        .unwrap();
    session.run("insert into v values (10), (30)").unwrap();

    crate::assert_removed_misc(
        &mut session,
        "select id from u where ref in (select ref from v) order by id",
        "Int(1);Int(3)",
    );
    assert_eq!(
        rows(
            &mut session,
            "select id from u where ref not in (select ref from v) order by id"
        ),
        "Int(2)"
    );
}
