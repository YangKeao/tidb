//! HEX/UNHEX edge semantics per Go `builtinUnHexSig`
//! (builtin_string.go:1844-1861): HEX(-1) renders the 64-bit two's
//! complement, UNHEX pads an odd digit count with a leading '0' (NOT NULL),
//! and invalid hex digits yield NULL.

use tidb_session::Session;
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            format!("bytes:{}", String::from_utf8_lossy(bytes))
                        }
                        tidb_datatype::Datum::String(value) => {
                            format!("bytes:{}", String::from_utf8_lossy(value.bytes()))
                        }
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn hex_unhex_edge_rules() {
    let mut session = Session::new();
    session
        .run("create table hex_typed_source(ts timestamp, j json)")
        .unwrap();
    session
        .run("insert into hex_typed_source values ('2020-01-02 03:04:05', '{\"a\":1}')")
        .unwrap();
    #[cfg(feature = "tikv-expr")]
    {
        session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
        let engine_before = session.tikv_expression_rows();

        let hexes = rows(&mut session, "select hex('ab'), hex(255), hex(-1)");
        assert!(hexes.contains("6162"), "{hexes}");
        assert!(hexes.contains("bytes:FF"), "{hexes}");
        assert!(hexes.contains("FFFFFFFFFFFFFFFF"), "{hexes}");

        let unhexes = rows(
            &mut session,
            "select unhex('6162'), unhex('abc'), unhex('zz')",
        );
        assert!(unhexes.contains("bytes:ab"), "{unhexes}");
        assert!(unhexes.contains("\u{a}\u{fffd}"), "{unhexes}");
        assert!(unhexes.contains("Null"), "{unhexes}");
        assert!(rows(&mut session, "select unhex(hex('ti'))").contains("bytes:ti"));
        assert_eq!(
            rows(&mut session, "select hex(ts) from hex_typed_source"),
            "bytes:323032302D30312D30322030333A30343A3035"
        );
        let json_error = session
            .run("select hex(j) from hex_typed_source")
            .expect_err("typed JSON HEX is an explicit standalone contraction")
            .to_string();
        assert!(
            json_error.contains("native integer radix evaluation was removed; TiKV engine required or function unsupported"),
            "{json_error}"
        );
        // Go's retained answer is 7B2261223A20317D (`{"a": 1}`); do not
        // substitute HEX's integer signature while CastJsonAsString is unavailable.
        assert!(session.tikv_expression_rows() > engine_before);
    }
    #[cfg(not(feature = "tikv-expr"))]
    for sql in [
        "select hex('ab')",
        "select unhex('6162')",
        "select hex(ts) from hex_typed_source",
        "select hex(j) from hex_typed_source",
    ] {
        let error = session
            .run(sql)
            .expect_err("native radix kernel is deleted")
            .to_string();
        assert!(
            error.contains("native integer radix evaluation was removed; TiKV engine required or function unsupported"),
            "{sql}: {error}"
        );
    }
}
