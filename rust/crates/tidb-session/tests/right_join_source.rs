//! Retains the former RIGHT JOIN result as an independent oracle while
//! pinning its local null-completion/ISNULL-dependent shape as unsupported.

use tidb_session::Session;

fn setup(session: &mut Session) {
    session.run("create table l (id int, lv int)").unwrap();
    session.run("create table r (id int, rv int)").unwrap();
    session
        .run("insert into l values (1, 10), (2, 20)")
        .unwrap();
    session
        .run("insert into r values (1, 100), (3, 300)")
        .unwrap();
}

#[test]
fn right_join_null_completion_contracts_without_local_isnull() {
    let mut session = Session::new();
    setup(&mut session);

    crate::assert_removed_misc(
        &mut session,
        "select l.id, lv, r.id, rv from l right join r on l.id = r.id order by r.id",
        "i:1|i:10|i:1|i:100;Null|Null|i:3|i:300",
    );
}
