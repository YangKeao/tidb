//! Correlated DELETE/NOT EXISTS remains executable; the former UPDATE/EXISTS
//! affected-row result is retained as an oracle for its explicit contraction.

use tidb_session::Session;

fn seed(session: &mut Session) {
    session.run("create table l (id int)").unwrap();
    session.run("insert into l values (1), (2), (3)").unwrap();
    session.run("create table r (id int)").unwrap();
    session.run("insert into r values (2)").unwrap();
}

#[test]
fn delete_not_exists_survives_while_update_exists_contracts() {
    let mut session = Session::new();
    seed(&mut session);

    assert_eq!(
        session
            .run("delete from l where not exists (select 1 from r where r.id = l.id)")
            .unwrap(),
        tidb_session::StmtResult::Affected(2)
    );
    assert_eq!(
        session.run("select id from l order by id").unwrap(),
        tidb_session::StmtResult::Rows(vec![vec![tidb_datatype::Datum::Int(2)]])
    );

    let mut update_session = Session::new();
    seed(&mut update_session);
    crate::assert_removed_misc(
        &mut update_session,
        "update l set id = id + 10 where exists (select 1 from r where r.id = l.id)",
        "affected:1;remaining:1;3;12",
    );
}
