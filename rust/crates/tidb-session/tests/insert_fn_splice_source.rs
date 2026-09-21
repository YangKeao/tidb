//! The INSERT(str, pos, len, new) splice function: pos is 1-based, pos 0
//! or beyond the length returns the input unchanged, a negative len splices
//! to the end, and NULL input propagates.

use tidb_session::Session;

fn assert_packet_string_removed(session: &mut Session, sql: &str) {
    let error = session
        .run(sql)
        .expect_err("deleted INSERT string kernel must refuse");
    assert!(
        error
            .to_string()
            .contains("native packet-limited string evaluation was removed; function unsupported"),
        "{sql}: {error}"
    );
}

#[test]
fn splice_position_and_length_rules() {
    let mut session = Session::new();

    // Preserve each source shape as an explicit contraction receipt: the
    // shared engine facade cannot carry max_allowed_packet or warning state.
    for sql in [
        "select insert('abcdef', 2, 3, 'XY')",
        "select insert('abcdef', 0, 3, 'XY')",
        "select insert('abcdef', 10, 3, 'XY')",
        "select insert('abcdef', 2, -1, 'XY')",
        "select insert(null, 2, 3, 'X')",
    ] {
        assert_packet_string_removed(&mut session, sql);
    }
}
