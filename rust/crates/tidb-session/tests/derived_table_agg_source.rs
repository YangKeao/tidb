//! The filtered derived aggregation remains executable; the former result of
//! joining it back to the base table is retained as an explicit contraction oracle.

use tidb_session::Session;

fn seed(session: &mut Session) {
    session.run("create table t (g int, v int)").unwrap();
    session
        .run("insert into t values (1, 1), (1, 2), (2, 3), (3, 4), (3, 5), (3, 6)")
        .unwrap();
}

#[test]
fn derived_filter_survives_while_join_contracts() {
    let mut session = Session::new();
    seed(&mut session);

    // Outer filter over the grouped subquery.
    assert_eq!(
        session
            .run("select g, c from (select g, count(*) as c from t group by g) d where c > 1 order by g")
            .unwrap(),
        tidb_session::StmtResult::Rows(vec![
            vec![tidb_datatype::Datum::Int(1), tidb_datatype::Datum::Int(2)],
            vec![tidb_datatype::Datum::Int(3), tidb_datatype::Datum::Int(3)],
        ])
    );

    // The derived result joins back to the base table (c = 1 matches g = 2).
    crate::assert_removed_misc(
        &mut session,
        "select d.g, d.c from (select g, count(*) as c from t group by g) d join t on t.g = d.g where d.c = 1 order by d.g",
        "2|1",
    );
}
