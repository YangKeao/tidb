//! IS TRUE/FALSE and their negated forms remain executable; the former
//! IS UNKNOWN result is retained as an oracle for its explicit contraction.

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
fn true_false_survive_while_unknown_contracts() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select 1 is true, 0 is true, null is true"),
        "Int(1)|Int(0)|Int(0)"
    );
    assert_eq!(
        rows(&mut session, "select 1 is false, 0 is false, null is false"),
        "Int(0)|Int(1)|Int(0)"
    );
    crate::assert_removed_misc(
        &mut session,
        "select 1 is unknown, null is unknown, null is not unknown",
        "Int(0)|Int(1)|Int(0)",
    );
    assert_eq!(
        rows(&mut session, "select null is not true, 0 is not true"),
        "Int(1)|Int(1)"
    );
}
