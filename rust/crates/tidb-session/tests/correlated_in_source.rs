//! Retains the former correlated-IN duplicate-match oracle as an explicit
//! contraction while the neighboring correlated NOT IN empty result remains
//! executable.

use tidb_session::Session;

fn setup(session: &mut Session) {
    session.run("create table u (a int primary key)").unwrap();
    session.run("create table s (grp int, x int)").unwrap();
    session.run("insert into u values (1), (2)").unwrap();
    session
        .run("insert into s values (1, 10), (1, 10), (2, 20)")
        .unwrap();
}

#[test]
fn correlated_in_contracts_while_not_in_survives() {
    let mut session = Session::new();
    setup(&mut session);

    crate::assert_removed_misc(
        &mut session,
        "select a from u where a in (select grp from s) order by a",
        "i:1;i:2",
    );
    assert_eq!(
        session
            .run("select a from u where a not in (select grp from s) order by a")
            .unwrap(),
        tidb_session::StmtResult::Rows(vec![])
    );
}
