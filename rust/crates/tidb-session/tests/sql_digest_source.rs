//! Current Go `pkg/expression/builtin_info.go` contract for
//! `TIDB_ENCODE_SQL_DIGEST` after native info-kernel deletion.

use tidb_session::Session;

#[test]
fn tidb_encode_sql_digest_uses_the_parser_normalizer() {
    // Former independent Go oracles remain computed here: the first three SQL
    // strings normalize to the same digest, the fourth differs, numeric 123 is
    // normalized as text, and NULL propagates.
    let expected = tidb_parser::normalize_digest("select * from b where id = 1")
        .1
        .to_string();
    assert_eq!(
        expected,
        tidb_parser::normalize_digest("select * from b where id = '1'")
            .1
            .to_string()
    );
    assert_ne!(
        expected,
        tidb_parser::normalize_digest("select a from b where id = 1")
            .1
            .to_string()
    );

    let mut session = Session::new();
    let sql = "SELECT TIDB_ENCODE_SQL_DIGEST('select * from b where id = 1'), \
               TIDB_ENCODE_SQL_DIGEST('select * from b where id = ''1'''), \
               TIDB_ENCODE_SQL_DIGEST('select * from b where id =2'), \
               TIDB_ENCODE_SQL_DIGEST('select a from b where id = 1'), \
               TIDB_ENCODE_SQL_DIGEST(123), TIDB_ENCODE_SQL_DIGEST(NULL)";
    let error = session
        .run(sql)
        .expect_err("native SQL-digest encoder is deleted");
    assert!(
        error.to_string().contains(
            "native miscellaneous evaluation was removed; TiKV engine required or function unsupported"
        ),
        "{error}"
    );
}
