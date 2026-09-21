//! Former UUID() shape, uniqueness, version, and stable-node source rows.
//! The native clock/RNG kernel is physically deleted, so both calls now pin
//! the structured contraction rather than generating test-only UUIDs.

use tidb_session::Session;

fn assert_misc_removed(session: &mut Session, sql: &str) {
    let error = session
        .run(sql)
        .expect_err("native UUID generator is deleted")
        .to_string();
    assert!(
        error.contains(
            "native miscellaneous evaluation was removed; TiKV engine required or function unsupported"
        ),
        "{sql}: {error}"
    );
}

#[test]
fn uuid_shape_uniqueness_and_node_are_explicitly_contracted() {
    let mut session = Session::new();
    assert_misc_removed(&mut session, "select uuid()");
    assert_misc_removed(&mut session, "select uuid()");
}
