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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The design's "result ring" at Phase-0 scale: `tidb-expr` must evaluate each
//! constant expression to the same value the production Go engine produces.
//! The corpus is a directory of per-topic file pairs under `corpus/expr/`
//! (`<topic>.txt` + `<topic>.golden.txt`); see `difftest::load_corpus_dir`.
//!
//! Every golden result `tidb-expr` covers (`INT:<v>` / `STR:<valid UTF-8>` /
//! `STR_HEX:<invalid UTF-8 bytes>` / `DEC:<v>` / `NULL`) is asserted;
//! out-of-domain results (`SKIP:<k>` — floats, decimal division, ... — and
//! `ERR`) are counted but not required, since `tidb-expr` does not cover them.
//!
//! Regenerate one topic's golden after changing it:
//! ```sh
//! grep -v '^##' rust/difftests/corpus/expr/<topic>.txt \
//!   | go run ./rust/difftests/goeval > rust/difftests/corpus/expr/<topic>.golden.txt
//! ```
//!
//! Add a brand-new topic by creating a new `<topic>.txt` + regenerating its
//! `<topic>.golden.txt` the same way — never append to an existing topic's
//! file unless the addition genuinely belongs to that topic.

#[path = "common/removed_native.rs"]
mod removed_native;

use std::path::PathBuf;

use difftest::{difftest_root, load_corpus_dir, validate_executable_corpora};
use tidb_ast::{QueryStmt, SelectField, Stmt};
#[cfg(feature = "tikv-expr")]
use tidb_session::{Session, StmtResult, TikvExpressionBackend};

#[test]
fn locate_is_a_whole_call_contraction() {
    assert_eq!(
        expected_removed_marker("locate('b', 'abc')"),
        Some(removed_native::STRING_AUX_REMOVED)
    );
    assert_eq!(
        expected_removed_marker("hex(upper('a'))"),
        Some(removed_native::RADIX_REMOVED)
    );
    assert_eq!(
        expected_removed_marker("upper(hex('a'))"),
        Some(removed_native::STRING2_REMOVED)
    );
    assert!(!may_accept_removed_marker(
        "upper(hex('a'))",
        removed_native::STRING2_REMOVED
    ));
    assert!(!may_accept_removed_marker(
        "inet6_ntoa(unhex('00000000'))",
        removed_native::INET_REMOVED
    ));
    assert!(!may_accept_removed_marker(
        "oct('8')",
        removed_native::RADIX_REMOVED
    ));
    assert!(may_accept_removed_marker(
        "oct(b'11111111')",
        removed_native::RADIX_REMOVED
    ));
    assert_eq!(
        expected_removed_marker("abs(substring_index('a.b.c', '.', 2))"),
        Some("native math evaluation was removed; TiKV engine required")
    );
    assert!(!may_accept_removed_marker(
        "abs(substring_index('a.b.c', '.', 2))",
        "native math evaluation was removed; TiKV engine required"
    ));
}

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus").join("expr")
}

fn may_accept_removed_marker(expr: &str, marker: &str) -> bool {
    let sql = format!("select {expr}");
    if removed_native::requires_string_aux_engine(&sql)
        && marker != removed_native::STRING_AUX_REMOVED
    {
        return false;
    }
    if marker == removed_native::RADIX_REMOVED {
        return removed_native::is_radix_shape_contraction(&sql);
    }
    if marker == removed_native::STRING_LENGTH_REMOVED {
        return removed_native::is_string_length_shape_contraction(&sql);
    }
    if marker == removed_native::CALENDAR_COMPONENT_REMOVED {
        return removed_native::is_calendar_component_shape_contraction(&sql);
    }
    if marker == removed_native::TEMPORAL_TAIL_REMOVED {
        return removed_native::is_temporal_tail_contraction(&sql);
    }
    if marker == removed_native::TEMPORAL_CLOCK_REMOVED {
        return false;
    }
    if marker == removed_native::TEMPORAL_SESSION_REMOVED {
        return removed_native::is_temporal_session_contraction(&sql);
    }
    if marker == removed_native::TEMPORAL_RESIDUAL_REMOVED {
        return removed_native::is_temporal_residual_contraction(&sql);
    }
    if marker == removed_native::TEMPORAL_VALUE_REMOVED {
        return removed_native::is_temporal_value_shape_contraction(&sql);
    }
    if marker == removed_native::STRING_AUX_REMOVED {
        return removed_native::is_string_aux_shape_contraction(&sql);
    }
    // A retained outer family must not hide failure to lower/execute an inner
    // retained family merely because its own native boundary then refuses.
    if marker == removed_native::STRING2_REMOVED {
        return !removed_native::requires_radix_engine(&sql)
            && !removed_native::requires_inet_engine(&sql)
            && !removed_native::requires_string_aux_engine(&sql);
    }
    if marker == removed_native::INET_REMOVED {
        return !removed_native::requires_radix_engine(&sql)
            && !removed_native::requires_string2_engine(&sql)
            && !removed_native::requires_string_aux_engine(&sql);
    }
    true
}

/// Parses `expr` by wrapping it in `SELECT`, then returns its evaluated label.
fn expected_removed_marker(expr: &str) -> Option<&'static str> {
    let sql = format!("select {expr}");
    if let Some(marker) = removed_native::expected_removed_marker(&sql) {
        return Some(marker);
    }
    let parsed = removed_native::parsed_function_names(&sql)?;
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
        "any_value(",
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
        return Some(
            "native miscellaneous evaluation was removed; TiKV engine required or function unsupported",
        );
    }
    None
}

fn rust_eval_label(expr: &str) -> Result<String, String> {
    let sql = format!("select {expr}");
    #[cfg(feature = "tikv-expr")]
    if removed_native::requires_string2_engine(&sql)
        || removed_native::requires_string_length_engine(&sql)
        || removed_native::requires_calendar_component_engine(&sql)
        || removed_native::requires_temporal_value_engine(&sql)
        || removed_native::requires_inet_engine(&sql)
        || removed_native::requires_radix_engine(&sql)
        || removed_native::requires_string_aux_engine(&sql)
    {
        let mut session = Session::new();
        session.set_tikv_expression_backend(Some(TikvExpressionBackend::Copying));
        let before = session.tikv_expression_rows();
        let result = session.run(&sql).map_err(|e| e.to_string())?;
        let StmtResult::Rows(rows) = result else {
            return Err("engine-required expression returned no rows".to_owned());
        };
        if session.tikv_expression_rows() <= before {
            return Err("engine-required expression did not execute a TiKV row".to_owned());
        }
        return rows
            .first()
            .and_then(|row| row.first())
            .map(tidb_datatype::Datum::label)
            .ok_or_else(|| "engine-required expression returned no datum".to_owned());
    }
    let stmt = tidb_parser::parse(&sql).map_err(|e| e.message)?;
    let Stmt::Query(query) = stmt else {
        return Err("not a query".to_string());
    };
    let QueryStmt::Select(sel) = query.into_inner() else {
        return Err("not a select".to_string());
    };
    match sel.fields.first() {
        Some(SelectField::Expr { expr, .. }) => tidb_expr::eval(expr)
            .map(|v| v.label())
            .map_err(|e| format!("{e:?}")),
        _ => Err("no field expression".to_string()),
    }
}

#[test]
fn expr_eval_matches_go_engine() {
    let root = difftest::parser_oracle::repo_root();
    validate_executable_corpora(&root).expect("executable corpus contract");
    let (exprs, golden_text) = load_corpus_dir(&corpus_dir());
    let golden: Vec<String> = golden_text.lines().map(str::to_string).collect();

    assert_eq!(
        exprs.len(),
        golden.len(),
        "expr corpus/golden count mismatch in corpus/expr/ (regenerate the changed topic's golden)"
    );

    let mut failures = Vec::new();
    let mut matched = 0;
    let mut contracted = 0;
    let mut skipped = 0;
    for (expr, want) in exprs.iter().zip(&golden) {
        let evaluated = rust_eval_label(expr);
        if let Some(marker) = expected_removed_marker(expr) {
            if may_accept_removed_marker(expr, marker)
                && evaluated
                    .as_ref()
                    .is_err_and(|error| error.contains(marker))
            {
                contracted += 1;
                continue;
            }
        }
        if expr
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("export_set(")
            && evaluated == Err("Unsupported(\"un-cast types.ETString argument\")".to_owned())
        {
            skipped += 1;
            continue;
        }
        // Out-of-domain golden results are not required of tidb-expr yet.
        if want.starts_with("SKIP:") || want == "ERR" {
            skipped += 1;
            continue;
        }
        match evaluated {
            Ok(got) if &got == want => matched += 1,
            Ok(got) => failures.push(format!("\n--- {expr}\n  go  : {want}\n  rust: {got}")),
            Err(e) => failures.push(format!(
                "\n--- {expr}\n  go  : {want}\n  rust: <error: {e}>"
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} in-domain expressions diverged from the Go engine ({} explicit contractions, {} skipped):{}",
        failures.len(),
        matched + failures.len(),
        contracted,
        skipped,
        failures.join("")
    );
}
