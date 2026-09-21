//! Retains the former full-pipeline result as an independent oracle while the
//! schema-join shape that depends on local ISNULL handling explicitly contracts.

use tidb_session::Session;

fn setup(session: &mut Session) {
    session
        .run("create table items (id int primary key, grp int, price int)")
        .unwrap();
    session
        .run("insert into items values (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 5)")
        .unwrap();
    session
        .run("create table grp_t (grp int primary key, name varchar(8))")
        .unwrap();
    session
        .run("insert into grp_t values (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .unwrap();
}

#[test]
fn full_pipeline_join_contracts_without_local_isnull() {
    let mut session = Session::new();
    setup(&mut session);

    // WHERE drops price<=5 (gamma), GROUP BY folds per name, HAVING keeps
    // the folded total >= 60, ORDER BY sorts by the folded total.
    crate::assert_removed_misc(
        &mut session,
        "select gr.name, sum(i.price) as total \
         from items i join grp_t gr on i.grp = gr.grp \
         where i.price > 5 \
         group by gr.name \
         having sum(i.price) >= 60 \
         order by total desc",
        "'beta'|70",
    );
}
