// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The result ring at the query level, re-pointed at the LIVE engine.
//!
//! `tidb-executor`/`tidb-session` must produce the same result rows a real
//! (mock-backed) TiDB session does for a table-less `SELECT` or set
//! operation, captured in `corpus/query_golden.txt` / `corpus/query/*` by
//! the `gorun` tool. The driver that used to check this ran the dead
//! `tidb-exec` `Database` engine (deleted in `e8369b73e2`); this re-points
//! the same corpus at the live engine, through [`tidb_session::Session`] --
//! the same path the TCP convergence node and every other in-process caller
//! use.
//!
//! Results in the live engine's domain (`RS:...`) are asserted; statements
//! the Go side reports as `ERR` (needing a table, or out of scope) are
//! counted but not required.
//!
//! EXPECT DIVERGENCES: see `table_diff.rs`'s own note -- this corpus has not
//! run against anything since the dead engine was removed, and the live
//! engine is a different engine from the one it was recorded against.
//!
//! Regenerate the golden after changing the corpus (drops TiDB's stderr logs):
//! ```sh
//! grep -v '^##' rust/difftests/corpus/query_statements.txt \
//!   | go run ./rust/difftests/gorun 2>/dev/null | grep -E '^(RS:|ERR)' \
//!   > rust/difftests/corpus/query_golden.txt
//! ```

#[path = "common/removed_native.rs"]
mod removed_native;
#[path = "result_label.rs"]
mod result_label;

use std::fs;
use std::path::PathBuf;

use difftest::{difftest_root, parse_corpus};
use result_label::{rows_label, statement_is_ordered};
#[cfg(feature = "tikv-expr")]
use tidb_session::TikvExpressionBackend;
use tidb_session::{Session, StmtResult};

const MISC_REMOVED: &str =
    "native miscellaneous evaluation was removed; TiKV engine required or function unsupported";

fn is_any_value(sql: &str) -> bool {
    !sql.trim_start().to_ascii_lowercase().starts_with("explain")
        && removed_native::parsed_function_names(sql)
            .is_some_and(|names| names.contains("ANY_VALUE"))
}

fn requires_tikv_engine(sql: &str) -> bool {
    cfg!(feature = "tikv-expr")
        && (is_any_value(sql)
            || removed_native::requires_string2_engine(sql)
            || removed_native::requires_string_length_engine(sql)
            || removed_native::requires_calendar_component_engine(sql)
            || removed_native::requires_temporal_value_engine(sql)
            || removed_native::requires_inet_engine(sql)
            || removed_native::requires_radix_engine(sql)
            || removed_native::requires_string_aux_engine(sql))
}

fn may_accept_engine_required_marker(sql: &str, marker: &str) -> bool {
    if removed_native::requires_string_aux_engine(sql)
        && marker != removed_native::STRING_AUX_REMOVED
    {
        return false;
    }
    if matches!(
        marker,
        removed_native::STRING2_REMOVED | removed_native::INET_REMOVED
    ) {
        return false;
    }
    if marker == removed_native::RADIX_REMOVED {
        return removed_native::is_radix_shape_contraction(sql);
    }
    if marker == removed_native::STRING_LENGTH_REMOVED {
        return removed_native::is_string_length_shape_contraction(sql);
    }
    if marker == removed_native::CALENDAR_COMPONENT_REMOVED {
        return removed_native::is_calendar_component_shape_contraction(sql);
    }
    if marker == removed_native::TEMPORAL_TAIL_REMOVED {
        return removed_native::is_temporal_tail_contraction(sql);
    }
    if marker == removed_native::TEMPORAL_CLOCK_REMOVED {
        return false;
    }
    if marker == removed_native::TEMPORAL_RESIDUAL_REMOVED {
        return removed_native::is_temporal_residual_contraction(sql);
    }
    if marker == removed_native::TEMPORAL_VALUE_REMOVED {
        return removed_native::is_temporal_value_shape_contraction(sql);
    }
    marker != removed_native::STRING_AUX_REMOVED
        || removed_native::is_string_aux_shape_contraction(sql)
}

fn expected_removed_marker(sql: &str) -> Option<&'static str> {
    if let Some(marker) = removed_native::expected_removed_marker(sql) {
        return Some(marker);
    }
    let parsed = removed_native::parsed_function_names(sql)?;
    let has = |names: &[&str]| {
        names
            .iter()
            .any(|name| parsed.contains(&name.trim().trim_end_matches('(').to_ascii_uppercase()))
    };
    if has(&[
        "rand(",
        "abs(",
        "sign(",
        "ceil(",
        "ceiling(",
        "floor(",
        "round(",
        "truncate(",
        "sqrt(",
        "pow(",
        "power(",
        "exp(",
        "ln(",
        "log(",
        "log2(",
        "log10(",
        "pi(",
        "sin(",
        "cos(",
        "tan(",
        "asin(",
        "acos(",
        "atan(",
        "atan2(",
        "cot(",
        "radians(",
        "degrees(",
        "conv(",
        "crc32(",
    ]) {
        return Some("native math evaluation was removed; TiKV engine required");
    }
    if has(&[
        "md5(",
        "sha(",
        "sha1(",
        "sha2(",
        "sm3(",
        "random_bytes(",
        "password(",
        "validate_password_strength(",
        "encode(",
        "decode(",
        "compress(",
        "aes_encrypt(",
        "aes_decrypt(",
        "uncompress(",
        "uncompressed_length(",
    ]) {
        return Some("native crypto evaluation was removed; TiKV engine required");
    }
    if has(&[
        "vec_dims(",
        "vec_l1_distance(",
        "vec_l2_distance(",
        "vec_negative_inner_product(",
        "vec_cosine_distance(",
        "vec_l2_norm(",
        "vec_from_text(",
        "vec_as_text(",
    ]) {
        return Some("native vector evaluation was removed; TiKV engine required");
    }
    if has(&["json_depth(", "json_storage_free(", "json_storage_size("]) {
        return Some("native JSON depth/storage evaluation was removed; TiKV engine required");
    }
    if has(&[
        "regexp_like(",
        "regexp_substr(",
        "regexp_instr(",
        "regexp_replace(",
        " regexp ",
        " rlike ",
    ]) {
        return Some("native regexp evaluation was removed; TiKV engine required");
    }
    if has(&[
        "repeat(",
        "space(",
        "lpad(",
        "rpad(",
        "to_base64(",
        "weight_string(",
        "concat(",
        "concat_ws(",
        "insert_func(",
        "make_set(",
        "from_base64(",
    ]) {
        return Some("native packet-limited string evaluation was removed; function unsupported");
    }
    if has(&[
        "substring(",
        "substr(",
        "mid(",
        "format(",
        "export_set(",
        "ltrim(",
        "rtrim(",
        "translate(",
    ]) {
        return Some(
            "native string2 evaluation was removed; TiKV engine required or function unsupported",
        );
    }
    if has(&[
        "uuid(",
        "uuid_v4(",
        "uuid_v7(",
        "name_const(",
        "is_uuid(",
        "uuid_version(",
        "uuid_timestamp(",
        "uuid_to_bin(",
        "bin_to_uuid(",
        "tidb_shard(",
        "tidb_decode_key(",
        "vitess_hash(",
    ]) {
        return Some(MISC_REMOVED);
    }
    None
}

fn is_misc_contraction(sql: &str) -> bool {
    expected_removed_marker(sql) == Some(MISC_REMOVED)
        || (cfg!(not(feature = "tikv-expr")) && is_any_value(sql))
}

#[test]
fn locate_is_a_whole_call_contraction() {
    assert_eq!(
        expected_removed_marker("select locate('b', 'abc')"),
        Some(removed_native::STRING_AUX_REMOVED)
    );
    assert_eq!(
        expected_removed_marker("select hex(upper('a'))"),
        Some(removed_native::RADIX_REMOVED)
    );
    assert_eq!(
        expected_removed_marker("select upper(hex('a'))"),
        Some(removed_native::STRING2_REMOVED)
    );
    assert!(!may_accept_engine_required_marker(
        "select inet6_ntoa(unhex('00000000'))",
        removed_native::INET_REMOVED
    ));
    assert!(!may_accept_engine_required_marker(
        "select upper(hex('a'))",
        removed_native::STRING2_REMOVED
    ));
    assert!(!may_accept_engine_required_marker(
        "select oct('8')",
        removed_native::RADIX_REMOVED
    ));
    assert!(may_accept_engine_required_marker(
        "select oct(b'11111111')",
        removed_native::RADIX_REMOVED
    ));
    assert!(!may_accept_engine_required_marker(
        "select abs(substring_index('a.b.c', '.', 2))",
        "native math evaluation was removed; TiKV engine required"
    ));
}

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus")
}

/// Parses and executes a table-less `SELECT` against a fresh session,
/// returning its result label.
fn rust_run(sql: &str) -> Result<String, String> {
    let stmt = tidb_parser::parse(sql).map_err(|e| e.message)?;
    let ordered = statement_is_ordered(&stmt);
    let mut session = Session::new();
    #[cfg(feature = "tikv-expr")]
    let engine_before = if requires_tikv_engine(sql) {
        session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
        Some(session.tikv_expression_rows())
    } else {
        None
    };
    let result = match session.run(sql) {
        Ok(StmtResult::Rows(rows)) => Ok(rows_label(&rows, ordered)),
        Ok(StmtResult::Affected(_) | StmtResult::Done(_)) => {
            Err("statement produced no rows".to_owned())
        }
        Err(error) => Err(format!("{error:?}")),
    };
    #[cfg(feature = "tikv-expr")]
    if let Some(before) = engine_before {
        if let Err(error) = &result {
            return Err(format!("required TiKV execution failed: {error}"));
        }
        if session.tikv_expression_rows() <= before {
            return Err(format!(
                "required query did not execute a TiKV-engine row: {sql}"
            ));
        }
    }
    result
}

/// Runs one statements-file/golden-file pair, appending divergences to
/// `failures` (tagged with `label` so a multi-topic failure names its file).
fn run_pair(
    label: &str,
    stmts_path: &PathBuf,
    golden_path: &PathBuf,
    failures: &mut Vec<String>,
) -> (usize, usize) {
    let stmts = parse_corpus(&fs::read_to_string(stmts_path).unwrap());
    let golden: Vec<String> = fs::read_to_string(golden_path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();

    assert_eq!(
        stmts.len(),
        golden.len(),
        "[{label}] corpus/golden count mismatch (regenerate the golden)"
    );

    let mut matched = 0;
    let mut skipped = 0;
    for (sql, want) in stmts.iter().zip(&golden) {
        let outcome = rust_run(sql);
        let requires_tikv_engine = requires_tikv_engine(sql);
        if !requires_tikv_engine && is_misc_contraction(sql) {
            let error = outcome.expect_err("explicit native-misc contraction");
            assert!(error.contains(MISC_REMOVED), "{sql}: {error}");
            matched += 1;
            continue;
        }
        if let Some(marker) = (!requires_tikv_engine)
            .then(|| expected_removed_marker(sql))
            .flatten()
        {
            if outcome.as_ref().is_err_and(|error| error.contains(marker)) {
                matched += 1;
                continue;
            }
        }
        // A statement can contain both an engine-required retained string
        // function and an independently contracted family. Accept only that
        // other family's exact parsed marker. STRING2 must execute; RADIX may
        // refuse only for an explicitly enumerated provenance/NULL-mask shape.
        if requires_tikv_engine {
            if let Err(error) = &outcome {
                let exact_contraction = removed_native::removed_markers(sql)
                    .into_iter()
                    .filter(|marker| may_accept_engine_required_marker(sql, marker))
                    .any(|marker| error.contains(marker));
                if exact_contraction {
                    matched += 1;
                    continue;
                }
            }
        }
        if want == "ERR" {
            skipped += 1;
            continue;
        }
        match outcome {
            Ok(got) if &got == want => matched += 1,
            Ok(got) => failures.push(format!(
                "\n--- [{label}] {sql}\n  go  : {want}\n  rust: {got}"
            )),
            Err(e) => failures.push(format!(
                "\n--- [{label}] {sql}\n  go  : {want}\n  rust: <error: {e}>"
            )),
        }
    }
    (matched, skipped)
}

#[cfg(feature = "tikv-expr")]
#[test]
fn any_value_engine_requirement_excludes_explain_and_mixed_contractions() {
    assert!(requires_tikv_engine(
        "select any_value(v), abs(v) from t group by v"
    ));
    assert!(!requires_tikv_engine(
        "explain select any_value(v) from t group by v"
    ));
    assert_eq!(
        expected_removed_marker("select any_value(v), abs(v) from t group by v"),
        Some("native math evaluation was removed; TiKV engine required")
    );
    assert_eq!(
        expected_removed_marker("select find_in_set(c, 'a,b') from t"),
        None,
        "an unresolved column collation must never be masked as a contraction"
    );
}

#[test]
fn query_result_matches_go_engine() {
    // The corpus replays user-written SQL, including TiDB's own
    // twenty-deep unary-operator cases -- see `difftest::on_deep_stack`.
    difftest::on_deep_stack(check_query_results);
}

fn check_query_results() {
    let dir = corpus_dir();
    let mut failures = Vec::new();
    let mut matched = 0;
    let mut skipped = 0;

    // The legacy single pair.
    let (m, s) = run_pair(
        "query_statements",
        &dir.join("query_statements.txt"),
        &dir.join("query_golden.txt"),
        &mut failures,
    );
    matched += m;
    skipped += s;

    // Per-topic pairs under `corpus/query/` -- `<topic>.txt` +
    // `<topic>.golden.txt`, one topic per builtin family, so parallel
    // agents each own their own pair (the same splittable-topic pattern
    // `table_diff` established).
    let topic_dir = dir.join("query");
    if topic_dir.is_dir() {
        let mut topics: Vec<PathBuf> = fs::read_dir(&topic_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.extension().is_some_and(|x| x == "txt")
                    && !p.to_string_lossy().ends_with(".golden.txt")
            })
            .collect();
        topics.sort();
        for stmts_path in topics {
            let topic = stmts_path
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let golden_path = topic_dir.join(format!("{topic}.golden.txt"));
            assert!(
                golden_path.exists(),
                "[{topic}] has no golden file (generate it with gorun)"
            );
            let (m, s) = run_pair(&topic, &stmts_path, &golden_path, &mut failures);
            matched += m;
            skipped += s;
        }
    }

    // Every divergence below is a real gap against Go, printed in full so it
    // can be worked off. It is a ratchet, not a waiver: the count may only go
    // DOWN. A permanently red suite would destroy the signal every other gate
    // depends on, and deleting the cases would destroy the evidence -- so the
    // debt is carried as a number that fails the moment it grows.
    const KNOWN_DIVERGENCES: usize = 0;

    // One comparison, both directions: `>` is a regression, `<` means the
    // constant is stale. Written as a match on Ordering rather than two
    // inequalities so it still reads correctly at zero, where `len() >= 0`
    // would be vacuously true for a usize.
    match failures.len().cmp(&KNOWN_DIVERGENCES) {
        std::cmp::Ordering::Greater => panic!(
            "{} of {} in-domain queries diverged from the Go engine, up from {} \
             ({} skipped) -- a new divergence appeared:{}",
            failures.len(),
            matched + failures.len(),
            KNOWN_DIVERGENCES,
            skipped,
            failures.join("")
        ),
        std::cmp::Ordering::Less => panic!(
            "only {} of {} queries diverge now, down from {}. Lower \
             KNOWN_DIVERGENCES to {} so the ratchet holds.",
            failures.len(),
            matched + failures.len(),
            KNOWN_DIVERGENCES,
            failures.len()
        ),
        std::cmp::Ordering::Equal => {}
    }
}
