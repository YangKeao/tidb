//! String GREATEST picks the lexicographic maximum, and BINARY strings
//! lose character semantics: length and char_length both count bytes
//! (binary('中a') -> 4/4, not 4/2).

use tidb_session::Session;

fn assert_compare2_removed(session: &mut Session, sql: &str, former_expected: &str) {
    let Err(tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(
        tidb_executor::EvalError::Unsupported(message),
    ))) = session.run(sql)
    else {
        panic!("{sql}: expected comparison contraction; former {former_expected}")
    };
    assert_eq!(
        message, "native LEAST/GREATEST/INTERVAL evaluation was removed; TiKV engine required",
        "{sql}: former {former_expected}"
    );
}

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

#[test]
fn string_extremum_contracts_while_binary_lengths_survive() {
    let mut session = Session::new();

    // Lexicographic maximum ('b' > 'ab' > 'aa').
    assert_compare2_removed(&mut session, "select greatest('b', 'aa', 'ab')", "s:b");

    // BINARY strips the charset: every byte is a character.
    assert_eq!(
        try_sql(
            &mut session,
            "select length(binary('中a')), char_length(binary('中a'))"
        ),
        "i:4|i:4"
    );
}
