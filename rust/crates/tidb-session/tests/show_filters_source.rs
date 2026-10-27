//! SHOW statement filters: `LIKE 'pattern'` matches the name column,
//! `WHERE database = '...'` filters by value, and `SHOW COLLATION WHERE
//! charset = ...` restricts to the charset's collations.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        tidb_datatype::Datum::String(s) => {
                            String::from_utf8_lossy(&s.bytes()).into_owned()
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session.run("create database zzz_alpha").unwrap();
    session.run("create database zzz_beta").unwrap();
    session.run("create database other_db").unwrap();
}

#[test]
fn show_filters() {
    let mut session = Session::new();
    setup(&mut session);

    // LIKE matches the name column.
    let matched = strings(&mut session, "show databases like 'zzz%'");
    assert_eq!(matched, vec!["zzz_alpha", "zzz_beta"]);

    // WHERE filters by value.
    let filtered = strings(&mut session, "show databases where database = 'zzz_alpha'");
    assert_eq!(filtered, vec!["zzz_alpha"]);

    // SHOW COLLATION WHERE restricts to the charset's collations.
    let collations = strings(&mut session, "show collation where charset = 'utf8mb4'");
    assert!(!collations.is_empty());
    assert!(
        collations.iter().all(|row| row.contains("utf8mb4")),
        "{collations:?}"
    );

    // Direct SHOW row loops reuse the same real predicate across changing rows.
    assert_eq!(
        strings(
            &mut session,
            "show variables where variable_name = 'autocommit'"
        )
        .len(),
        1
    );
    assert_eq!(
        strings(
            &mut session,
            "show status where variable_name = 'Ssl_cipher'"
        )
        .len(),
        1
    );
    let charsets = strings(&mut session, "show charset where charset = 'utf8mb4'");
    assert_eq!(charsets.len(), 1);
    assert!(charsets[0].starts_with("utf8mb4|"));
    assert_eq!(
        strings(&mut session, "show engines where engine = 'InnoDB'").len(),
        1
    );
    assert_eq!(
        strings(
            &mut session,
            "show databases where case database when 'zzz_alpha' then 1 else 0 end"
        ),
        vec!["zzz_alpha"]
    );
    session.run("create table zzz_alpha.t1 (id int)").unwrap();
    session.run("create table zzz_alpha.t2 (id int)").unwrap();
    assert_eq!(
        strings(
            &mut session,
            "show full tables from zzz_alpha where table_type = 'BASE TABLE'"
        )
        .len(),
        2
    );

    // This entry has always resolved only its virtual output, without session
    // variable forwarding. Keep that original resolver capability boundary.
    session.run("set @ast_scope = 17").unwrap();
    assert_eq!(
        strings(&mut session, "show engines where @ast_scope is null").len(),
        1
    );
    let error = session
        .run("show engines where not_an_output = 1")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("not_an_output") && error.contains("where clause"),
        "{error}"
    );

    // No row means no name scan, compilation or demanded function header.
    session.run("create database zzz_empty").unwrap();
    assert!(strings(
        &mut session,
        "show tables from zzz_empty where unregistered_ast_function()"
    )
    .is_empty());
}
