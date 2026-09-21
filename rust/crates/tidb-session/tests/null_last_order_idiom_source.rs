//! Retains the former NULL-last ordering result as an independent oracle for
//! the current explicit contraction of its local ISNULL sort key.

use tidb_session::Session;

#[test]
fn null_last_isnull_sort_key_contracts() {
    let mut session = Session::new();
    session.run("create table t (s varchar(4))").unwrap();
    session
        .run("insert into t values ('b'), (NULL), ('a'), (NULL)")
        .unwrap();

    crate::assert_removed_misc(
        &mut session,
        "select s from t order by (s is null), s",
        "'a';'b';NULL;NULL",
    );
}
