//! Retains the former JOIN ... USING result as an independent oracle while
//! pinning its local merged-column/ISNULL-dependent shape as unsupported.

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
fn using_clause_merge_contracts_without_local_isnull() {
    let mut session = Session::new();
    setup(&mut session);

    // Qualified per-side names do not exist after USING; bare `id` answers.
    crate::assert_removed_misc(
        &mut session,
        "select id, lv, rv from l join r using (id) order by id",
        "i:1|i:10|i:100",
    );
    // SELECT * shows the merged column once.
    crate::assert_removed_misc(
        &mut session,
        "select * from l join r using (id) order by id",
        "i:1|i:10|i:100",
    );
}
