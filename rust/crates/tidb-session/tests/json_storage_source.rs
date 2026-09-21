//! Preserves the former JSON_STORAGE_FREE/SIZE SQL vectors after their native
//! kernel module was physically deleted. Both names are explicit contractions
//! until TiKV has an admitted binary-storage-accounting implementation.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
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
        Err(e) => format!("ERR {e}"),
    }
}

#[test]
fn storage_semantics() {
    let mut session = Session::new();
    for sql in [
        "select json_storage_free('{\"a\": 1}')",
        "select json_storage_size('{\"a\": 1}')",
        "select json_storage_size(null)",
    ] {
        let outcome = try_sql(&mut session, sql);
        assert!(
            outcome.contains(
                "native JSON depth/storage evaluation was removed; TiKV engine required"
            ),
            "{sql}: {outcome}"
        );
    }
}
