//! Retains the former per-row correlated scalar-subquery result, including its
//! unmatched NULL, as an independent oracle for the current explicit contraction.

use tidb_session::Session;

fn seed(session: &mut Session) {
    session.run("create table t (a int primary key)").unwrap();
    session.run("insert into t values (1), (2), (9)").unwrap();
    session.run("create table s (k int, v int)").unwrap();
    session
        .run("insert into s values (1, 111), (1, 222), (2, 50)")
        .unwrap();
}

#[test]
fn correlated_scalar_null_completion_contracts() {
    let mut session = Session::new();
    seed(&mut session);

    crate::assert_removed_misc(
        &mut session,
        "select a, (select max(v) from s where s.k = t.a) from t order by a",
        "1|222;2|50;9|NULL",
    );
}
