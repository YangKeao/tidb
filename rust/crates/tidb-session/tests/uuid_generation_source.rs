//! Current Go `pkg/expression/builtin_miscellaneous.go` UUID generation source
//! shapes, retained as explicit contractions after native kernel deletion.

use tidb_session::Session;

fn assert_misc_removed(session: &mut Session, expression: &str) {
    let sql = format!("SELECT {expression}");
    let error = session
        .run(&sql)
        .expect_err("native UUID miscellaneous kernel is deleted")
        .to_string();
    assert!(
        error.contains(
            "native miscellaneous evaluation was removed; TiKV engine required or function unsupported"
        ),
        "{sql}: {error}"
    );
}

#[test]
fn uuid_generators_and_nested_consumers_are_explicitly_contracted() {
    let mut session = Session::new();
    for expression in [
        "UUID()",
        "UUID()",
        "UUID_V4()",
        "UUID_V4()",
        "UUID_V7()",
        "UUID_V7()",
        "UUID_VERSION(UUID())",
        "UUID_VERSION(UUID_V4())",
        "UUID_VERSION(UUID_V7())",
        "UUID_TIMESTAMP(UUID())",
        "UUID_TIMESTAMP(UUID_V4())",
        "UUID_TIMESTAMP(UUID_V7())",
    ] {
        assert_misc_removed(&mut session, expression);
    }
}
