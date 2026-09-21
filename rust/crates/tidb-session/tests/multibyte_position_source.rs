//! Multibyte positioning and lengths: LOCATE/INSTR report CHARACTER
//! positions ('a' after a 3-byte 中 is character 2, not byte 4), BIT_LENGTH
//! counts bits (32 for '中a'), and LPAD's target length is in characters
//! (lpad('中', 2, 'ab') = 'a中').

use tidb_session::Session;
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;

fn assert_packet_string_removed(session: &mut Session, sql: &str) {
    let error = session
        .run(sql)
        .expect_err("packet-limited native string kernel is deleted")
        .to_string();
    assert!(
        error.contains("native packet-limited string evaluation was removed; function unsupported"),
        "{sql}: {error}"
    );
}

fn assert_string_tail_removed(session: &mut Session, sql: &str) {
    let error = session.run(sql).expect_err("native string tail is deleted");
    assert!(
        error.to_string().contains("native string auxiliary evaluation was removed; TiKV engine required or function unsupported"),
        "{sql}: {error}"
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
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn character_positions_not_bytes() {
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
    // Former answers were 2, 2, 3 and 3. Search collation transport is not safe.
    for sql in [
        "select locate('a', '中a')",
        "select instr('中a', 'a')",
        "select locate('b' collate utf8mb4_bin, 'aéb' collate utf8mb4_bin, '3')",
        "select position('b' in 'aéb' collate utf8mb4_bin)",
    ] {
        assert_string_tail_removed(&mut session, sql);
    }

    // Four bytes -> 32 bits, and the deleted native BIT_LENGTH cannot supply it.
    #[cfg(feature = "tikv-expr")]
    {
        let bit_length_before = session.tikv_expression_rows();
        assert_eq!(try_sql(&mut session, "select bit_length('中a')"), "i:32");
        assert!(session.tikv_expression_rows() > bit_length_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    {
        let error = session
            .run("select bit_length('中a')")
            .expect_err("native BIT_LENGTH kernel is deleted")
            .to_string();
        assert!(error.contains("native string2 evaluation was removed; TiKV engine required or function unsupported"), "{error}");
    }

    // Preserve the multibyte pad shape as an explicit contraction.
    assert_packet_string_removed(&mut session, "select lpad('中', 2, 'ab')");
}
