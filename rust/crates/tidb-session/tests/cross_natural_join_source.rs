//! CROSS JOIN still verifies its Cartesian-product values; the former NATURAL
//! JOIN merged-column result remains an independent oracle for its contraction.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn cartesian_product_survives_while_natural_join_contracts() {
    let mut session = Session::new();

    session.run("create table l (a int)").unwrap();
    session.run("create table r (b int)").unwrap();
    session.run("insert into l values (1), (2)").unwrap();
    session.run("insert into r values (10), (20)").unwrap();

    assert_eq!(
        rows(
            &mut session,
            "select a, b from l cross join r order by a, b"
        ),
        "i:1|i:10;i:1|i:20;i:2|i:10;i:2|i:20"
    );

    session.run("create table u (id int, v int)").unwrap();
    session.run("create table w (id int, w int)").unwrap();
    session
        .run("insert into u values (1, 10), (2, 20)")
        .unwrap();
    session.run("insert into w values (1, 100)").unwrap();

    // Former value was `i:1|i:10|i:100`; join null-completion now contracts.
    crate::assert_removed_misc(
        &mut session,
        "select * from u natural join w",
        "i:1|i:10|i:100",
    );
}
