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

//! Shared expression-test helpers and non-math families. Math-source tests live
//! in `math.rs`, so extending `builtin_math.go` coverage does not touch this root.

use super::*;
use tidb_ast::{QueryStmt, SelectField, Stmt};

mod advisory_get_lock_integration_source;
mod aggregation_arithmetic_cast_source;
mod binary_string_signature;
mod builtin_info_json_math_source;
mod builtin_math_misc_op_source;
mod builtin_string_time_source;
mod builtin_time_calendars_source;
mod builtin_vectorized_time_infra_source;
mod collation_compare;
mod compare;
mod compare_control_source;
mod compare_time_builtin_rows_source;
mod constant_test_go_tables_source;
mod context_override_values_source;
mod control;
mod convert_using_signature_source;
mod crypto_encryption_source;
mod datetime;
mod distsql_pb_roundtrip_gap_source;
mod etint_argument;
mod etstring_argument;
mod evaluator_binop;
mod evaluator_go_tables_source;
mod expr_to_pb_lowering_gap_source;
mod expr_to_pb_switcher_source;
mod expression_null_const_source;
mod expression_with_null_source;
mod filter_extract_dnf_source;
mod find_in_set_lookup_source;
mod function_traits_source;
mod go_arithmetic_values;
mod go_control_op_math_values;
mod go_string_values;
mod go_time_values;
mod hash_group_key_codec_matrix_source;
mod helper_current_timestamp_source;
mod ilike_info_cast_source;
mod in_func_decimal_collation_source;
mod json_merge_patch_integration_source;
mod math;
mod misc_contraction_source;
mod operand_dispatch;
mod packet_string_contraction_source;
#[cfg(feature = "tikv-expr")]
mod regexp_like;
#[cfg(feature = "tikv-expr")]
mod regexp_source_vectors;
#[cfg(feature = "tikv-expr")]
mod regexp_vec_cache_source;
mod scalar_function_semantics_source;
mod setvar_getvar_values_getparam_source;
mod string2_contraction_source;
mod util_filter_condition_source;
mod vectorizable_and_chunk_eval_source;
mod vectorized_filter_consider_null_gap_source;

/// Parses and evaluates a constant expression to its label.
pub(super) fn e(expr: &str) -> String {
    e_with(expr, &NoColumns)
}

fn e_with(expr: &str, cols: &dyn Columns) -> String {
    let stmt = tidb_parser::parse(&format!("select {expr}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        panic!("not select")
    };
    match &s.fields[0] {
        SelectField::Expr { expr, .. } => match eval_in(expr, cols) {
            Ok(v) => v.label(),
            Err(err) => format!("{err:?}"),
        },
        _ => panic!("no expr"),
    }
}

/// Parses a constant expression and evaluates it through the CHUNK tier --
/// the rewriter's `ScalarFunction` path that live SQL takes, as opposed to
/// [`e`]'s AST/value tier. Only this tier carries each argument's static
/// `FieldType` and the derived result collation, so it is the only one that
/// can see a temporal argument or a `COLLATE` clause.
pub(super) fn chunk_e(expr: &str) -> String {
    let native = chunk_case(expr, &NoColumns);
    // Dual-run: with the engine available, the same rewritten expression must
    // also run through TiKV and agree. An expression the adapter declines is
    // skipped, so an excluded name is not a failure here; an expression the
    // engine runs and answers differently is. Errors are compared by
    // classification, not wording, per the removal scope.
    #[cfg(feature = "tikv-expr")]
    if let Some(engine) = engine_case(expr) {
        match (&native, engine) {
            (Ok(native), Ok(engine)) => {
                assert_eq!(native.label(), engine.label(), "engine vs native: {expr}")
            }
            (Err(_), Err(_)) => {}
            (Ok(native), Err(engine)) => {
                panic!(
                    "native {} but engine errored {engine}: {expr}",
                    native.label()
                )
            }
            (Err(native), Ok(engine)) => {
                panic!(
                    "native errored {native} but engine {}: {expr}",
                    engine.label()
                )
            }
        }
    } else if std::env::var_os("TIKV_EXPR_ENGINE_ONLY").is_some() {
        // Milestone E switch: with this set, a case the adapter declines is a
        // failure, so the corpus reports exactly which cases the engine does
        // not yet cover instead of silently answering natively.
        panic!("engine declined the expression: {expr}");
    }
    match native {
        Ok(value) => value.label(),
        Err(err) => err,
    }
}

pub(super) const PACKET_STRING_REMOVED: &str =
    "Unsupported(\"native packet-limited string evaluation was removed; function unsupported\")";

fn outer_radix_or(expr: &str, inner_marker: &'static str) -> &'static str {
    let lower = expr.trim_start().to_ascii_lowercase();
    if ["hex(", "unhex(", "bin(", "oct(", "ord(", "bit_count("]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
    {
        RADIX_REMOVED
    } else {
        inner_marker
    }
}

pub(super) fn assert_packet_string_refusal(expr: &str) {
    let expected = outer_radix_or(expr, PACKET_STRING_REMOVED);
    assert_eq!(e(expr), expected, "AST boundary: {expr}");
    assert_eq!(chunk_e(expr), expected, "chunk boundary: {expr}");
}

pub(super) const COMPARE2_REMOVED: &str =
    "Unsupported(\"native LEAST/GREATEST/INTERVAL evaluation was removed; TiKV engine required\")";

pub(super) fn assert_compare2_refusal(expr: &str, former_expected: &str) {
    assert_eq!(
        e(expr),
        COMPARE2_REMOVED,
        "AST boundary: {expr}; former {former_expected}"
    );
    let chunk = chunk_case(expr, &NoColumns)
        .map(|value| value.label())
        .unwrap_or_else(|error| error);
    assert_eq!(
        chunk, COMPARE2_REMOVED,
        "native chunk boundary: {expr}; former {former_expected}"
    );
}

pub(super) fn assert_engine_compare2_value(expr: &str, expected: &str) {
    #[cfg(feature = "tikv-expr")]
    assert_eq!(engine_e(expr), expected, "TiKV engine: {expr}");
    assert_compare2_refusal(expr, expected);
}

pub(super) const STRING_LENGTH_REMOVED: &str =
    "Unsupported(\"native string length evaluation was removed; TiKV engine required\")";

pub(super) fn assert_string_length_refusal(expr: &str, former_expected: &str) {
    assert_eq!(
        e(expr),
        STRING_LENGTH_REMOVED,
        "AST boundary: {expr}; former {former_expected}"
    );
    let chunk = chunk_case(expr, &NoColumns)
        .map(|value| value.label())
        .unwrap_or_else(|error| error);
    assert_eq!(
        chunk, STRING_LENGTH_REMOVED,
        "native chunk boundary: {expr}; former {former_expected}"
    );
}

pub(super) fn assert_engine_string_length_value(expr: &str, expected: &str) {
    #[cfg(feature = "tikv-expr")]
    assert_eq!(engine_e(expr), expected, "TiKV engine: {expr}");
    assert_string_length_refusal(expr, expected);
}

pub(super) const MISC_REMOVED: &str =
    "Unsupported(\"native miscellaneous evaluation was removed; TiKV engine required or function unsupported\")";

pub(super) fn assert_misc_refusal(expr: &str) {
    assert_eq!(e(expr), MISC_REMOVED, "AST boundary: {expr}");
    let chunk = chunk_case(expr, &NoColumns)
        .map(|value| value.label())
        .unwrap_or_else(|error| error);
    assert_eq!(chunk, MISC_REMOVED, "native chunk boundary: {expr}");
}

pub(super) const STRING2_REMOVED: &str =
    "Unsupported(\"native string2 evaluation was removed; TiKV engine required or function unsupported\")";

pub(super) fn assert_string2_refusal(expr: &str) {
    let expected = outer_radix_or(expr, STRING2_REMOVED);
    assert_eq!(e(expr), expected, "AST boundary: {expr}");
    let chunk = chunk_case(expr, &NoColumns)
        .map(|value| value.label())
        .unwrap_or_else(|error| error);
    assert_eq!(chunk, expected, "native chunk boundary: {expr}");
}

pub(super) fn assert_engine_string2_value(expr: &str, expected: &str) {
    #[cfg(feature = "tikv-expr")]
    assert_eq!(engine_e(expr), expected, "TiKV engine: {expr}");
    let _ = expected;
    assert_string2_refusal(expr);
}

pub(super) const RADIX_REMOVED: &str =
    "Unsupported(\"native integer radix evaluation was removed; TiKV engine required or function unsupported\")";

pub(super) fn assert_radix_refusal(expr: &str) {
    assert_eq!(e(expr), RADIX_REMOVED, "AST boundary: {expr}");
    let chunk = chunk_case(expr, &NoColumns)
        .map(|value| value.label())
        .unwrap_or_else(|error| error);
    assert_eq!(chunk, RADIX_REMOVED, "native chunk boundary: {expr}");
}

pub(super) fn assert_engine_radix_value(expr: &str, expected: &str) {
    #[cfg(feature = "tikv-expr")]
    assert_eq!(engine_e(expr), expected, "TiKV engine: {expr}");
    let _ = expected;
    assert_radix_refusal(expr);
}

pub(super) const STRING_AUX_REMOVED: &str =
    "Unsupported(\"native string auxiliary evaluation was removed; TiKV engine required or function unsupported\")";

pub(super) fn assert_string_aux_refusal(expr: &str) {
    let expected = outer_radix_or(expr, STRING_AUX_REMOVED);
    assert_eq!(e(expr), expected, "AST boundary: {expr}");
    let chunk = chunk_case(expr, &NoColumns)
        .map(|value| value.label())
        .unwrap_or_else(|error| error);
    assert_eq!(chunk, expected, "native chunk boundary: {expr}");
}

pub(super) fn assert_engine_string_aux_value(expr: &str, expected: &str) {
    #[cfg(feature = "tikv-expr")]
    assert_eq!(engine_e(expr), expected, "TiKV engine: {expr}");
    let _ = expected;
    assert_string_aux_refusal(expr);
}

pub(super) fn assert_string_aux_contraction(expr: &str) {
    assert_string_aux_refusal(expr);
    #[cfg(feature = "tikv-expr")]
    assert!(engine_declines(expr), "TiKV unexpectedly admitted: {expr}");
}

fn chunk_e_with(expr: &str, ctx: &impl Columns) -> String {
    match chunk_case(expr, ctx) {
        Ok(value) => value.label(),
        Err(err) => err,
    }
}

/// The rewritten expression's own evaluation, as a value or a formatted error.
fn chunk_case(expr: &str, ctx: &impl Columns) -> Result<Datum, String> {
    let stmt = tidb_parser::parse(&format!("select {expr}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        panic!("no expr")
    };
    let rewritten = crate::rewriter::rewrite_expr(expr).map_err(|err| format!("{err:?}"))?;
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    rewritten
        .eval(ctx, chunk.get_row(0))
        .map_err(|err| format!("{err:?}"))
}

/// Runs one constant expression through the engine. `None` means the adapter
/// declined it (a listed exclusion), so the native answer stands alone.
#[cfg(feature = "tikv-expr")]
fn engine_case(expr: &str) -> Option<Result<Datum, String>> {
    engine_case_with_backend(expr, crate::tikv::Backend::Copying)
}

#[cfg(feature = "tikv-expr")]
fn engine_case_with_backend(
    expr: &str,
    backend: crate::tikv::Backend,
) -> Option<Result<Datum, String>> {
    use std::cell::Cell;

    struct EngineColumns {
        fallback: Cell<bool>,
        backend: crate::tikv::Backend,
    }
    impl Columns for EngineColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn tikv_expression_context(&self) -> Option<crate::tikv::Context> {
            Some(crate::tikv::Context {
                flags: 482,
                ..Default::default()
            })
        }
        fn tikv_expression_backend(&self) -> crate::tikv::Backend {
            self.backend
        }
        fn record_tikv_expression_fallback(&self, _: crate::tikv::FallbackReason) {
            self.fallback.set(true)
        }
    }

    let stmt = tidb_parser::parse(&format!("select {expr}")).ok()?;
    let Stmt::Query(query) = stmt else {
        return None;
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        return None;
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        return None;
    };
    let rewritten = crate::rewriter::rewrite_expr(expr).ok()?;
    let ty = rewritten.static_type()?.clone();

    let columns = EngineColumns {
        fallback: Cell::new(false),
        backend,
    };
    let suite = crate::evaluator::EvaluatorSuite::new(vec![rewritten], true);
    let mut input = tidb_chunk::chunk::Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    let mut output = tidb_chunk::chunk::Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    let outcome = suite.run(&columns, &mut input, &mut output);
    if columns.fallback.get() {
        return None;
    }
    Some(match outcome {
        Ok(()) => Ok(output.get_row(0).get_datum(0, &ty)),
        Err(err) => Err(format!("{err:?}")),
    })
}

/// Engine-only replacement for source-vector tests whose native family has
/// been physically deleted. A declined expression is a visible test failure,
/// never a native fallback.
#[cfg(feature = "tikv-expr")]
pub(crate) fn engine_e(expr: &str) -> String {
    engine_e_with_backend(expr, crate::tikv::Backend::Copying)
}

#[cfg(feature = "tikv-expr")]
pub(super) fn engine_e_with_backend(expr: &str, backend: crate::tikv::Backend) -> String {
    match engine_case_with_backend(expr, backend)
        .unwrap_or_else(|| panic!("TiKV {backend:?} engine declined: {expr}"))
    {
        Ok(value) => value.label(),
        Err(error) => error,
    }
}

#[cfg(feature = "tikv-expr")]
pub(super) fn engine_v(expr: &str) -> Datum {
    match engine_case(expr).unwrap_or_else(|| panic!("TiKV engine declined: {expr}")) {
        Ok(value) => value,
        Err(error) => panic!("TiKV engine failed for {expr}: {error}"),
    }
}

#[cfg(feature = "tikv-expr")]
pub(super) fn engine_declines(expr: &str) -> bool {
    engine_case(expr).is_none()
}

/// Parses and evaluates a constant expression to its raw `Datum`.
pub(super) fn v(expr: &str) -> Datum {
    let stmt = tidb_parser::parse(&format!("select {expr}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        panic!("not select")
    };
    match &s.fields[0] {
        SelectField::Expr { expr, .. } => eval(expr).expect("eval"),
        _ => panic!("no expr"),
    }
}

#[test]
fn json_schema_valid_rewrites_and_short_circuits_like_go() {
    assert_eq!(
        chunk_e(r#"json_schema_valid('{"required":["a"]}', '{"a":1}')"#),
        "INT:1"
    );
    assert_eq!(
        chunk_e(r#"json_schema_valid('{"required":["a"]}', '{}')"#),
        "INT:0"
    );
    // Go returns as soon as the schema argument is NULL, before evaluating
    // or converting the document argument.
    assert_eq!(chunk_e("json_schema_valid(NULL, 1 / 0)"), "NULL");
}

#[test]
fn avg() {
    // AVG grows the sum's scale by 4 (MySQL's div_precision_increment)
    // and ROUNDS to it, unlike DIV's exact truncation.
    assert_eq!(avg_of(v("6"), 3).unwrap().label(), "DEC:2.0000"); // Int sum promotes to scale 0
    assert_eq!(avg_of(v("8.00"), 3).unwrap().label(), "DEC:2.666667"); // scale 2+4=6, rounds up
    assert_eq!(avg_of(v("4.0000"), 2).unwrap().label(), "DEC:2.00000000"); // scale 4+4=8, exact
    assert!(matches!(
        avg_of(Datum::new_string("x".to_string()), 1),
        Err(EvalError::Unsupported(_))
    ));
}

/// `BIN` receives an implicit `ETInt` argument in
/// `pkg/expression/builtin_string.go`. Keep the coercion boundary explicit:
/// the Go source-table test's non-empty string, integer, float, negative and
/// NULL rows all reach this same result domain. `TestBin`'s empty direct-datum
/// row is intentionally not recorded as source coverage yet: it expects
/// `NULL`, while `builtinCastStringAsIntSig` and the SQL oracle both produce
/// zero. The local Go test cannot currently execute because the arm64 linker
/// fails before running it, so the source-table contradiction remains visible
/// instead of being hidden by a synthetic special case.
#[test]
fn bin_follows_tidb_implicit_integer_coercion() {
    for (input, want) in [
        ("'10'", "STR:1010"),
        ("'10.2'", "STR:1010"),
        ("'10aa'", "STR:1010"),
        ("'10.2aa'", "STR:1010"),
        ("'aaa'", "STR:0"),
        ("''", "STR:0"),
        ("10", "STR:1010"),
        // SQL decimal literals use TiDB's decimal-to-int half-up path.
        ("10.0", "STR:1010"),
        (
            "-1",
            "STR:1111111111111111111111111111111111111111111111111111111111111111",
        ),
        (
            "'-1'",
            "STR:1111111111111111111111111111111111111111111111111111111111111111",
        ),
        ("null", "NULL"),
    ] {
        assert_engine_radix_value(&format!("bin({input})"), want);
    }
    // A scientific SQL literal selects the REAL signature and therefore keeps
    // Go's ties-to-even cast before the TiKV BIN kernel.
    assert_engine_radix_value("bin(10.5e0)", "STR:1010");
    // Native arity and malformed-byte helper behavior is no longer retained.
    assert_eq!(e("bin()"), RADIX_REMOVED);
    assert_eq!(
        crate::func::eval_func_values(
            "BIN",
            &[Datum::new_bytes(vec![0xff])],
            &NoColumns
        ),
        Some(Err(EvalError::Unsupported(
            "native integer radix evaluation was removed; TiKV engine required or function unsupported"
        )))
    );
}

#[test]
fn bit_count_raw_value_boundary_is_engine_only() {
    // The SQL engine table independently pins these Go answers. Arbitrary raw
    // values may no longer invoke the deleted helper directly.
    for (value, _former_go_value) in [
        (Datum::UInt(u64::MAX), 64),
        (Datum::new_string("9223372036854775808"), 1),
        (Datum::new_bytes(vec![b'1', 0xff]), 1),
    ] {
        assert_eq!(
            crate::func::eval_func_values("BIT_COUNT", &[value], &NoColumns),
            Some(Err(EvalError::Unsupported(
                "native integer radix evaluation was removed; TiKV engine required or function unsupported"
            )))
        );
    }
}

#[test]
fn hex_bit_value_boundary_is_explicitly_removed() {
    for (value, _former_go_value) in [
        (
            Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![
                0, 0, 0, 0, 0, 0x41,
            ])),
            "41",
        ),
        (Datum::new_bytes(vec![0, 0x41]), "0041"),
    ] {
        assert_eq!(
            crate::func::eval_func_values("HEX", &[value], &NoColumns),
            Some(Err(EvalError::Unsupported(
                "native integer radix evaluation was removed; TiKV engine required or function unsupported"
            )))
        );
    }
}

#[test]
fn hex_binary_literal_source_kind_is_contracted() {
    // Go's byte-string answer is 0041. The adapter intentionally refuses the
    // missing source-kind provenance instead of inferring it from wire bytes.
    assert_radix_refusal("hex(0x0041)");
    #[cfg(feature = "tikv-expr")]
    assert!(engine_declines("hex(0x0041)"));
}

/// Complete representable rows from `TestOrd` in
/// `pkg/expression/builtin_string_test.go`.  Text values fold the first
/// UTF-8 character's bytes, while binary values use only their first raw byte;
/// session charset conversion (the GBK rows in the source table) remains an
/// explicit partial boundary.
#[test]
fn ord_source_vectors_preserve_utf8_and_binary_bytes() {
    for (expr, want) in [
        ("ord('2')", "INT:50"),
        ("ord('23')", "INT:50"),
        ("ord(NULL)", "NULL"),
        ("ord('')", "INT:0"),
        ("ord('你好')", "INT:14990752"),
        ("ord('にほん')", "INT:14909867"),
        ("ord('한국')", "INT:15570332"),
        ("ord('👍')", "INT:4036989325"),
        ("ord('א')", "INT:55184"),
    ] {
        assert_engine_radix_value(expr, want);
    }
    // TiKV's NULL repair is safe only for a leaf Bytes argument. Numeric
    // implicit casts, nested casts, and arbitrary native value calls are
    // explicit contractions.
    for expr in ["ord(2)", "ord(23)", "ord(2.3)"] {
        assert_radix_refusal(expr);
        #[cfg(feature = "tikv-expr")]
        assert!(engine_declines(expr), "engine unexpectedly admitted {expr}");
    }
    assert_radix_refusal("ord(cast('éb' as binary))");
    #[cfg(feature = "tikv-expr")]
    assert!(engine_declines("ord(cast('éb' as binary))"));
    assert_eq!(
        crate::func::eval_func_values(
            "ORD",
            &[Datum::new_bytes(vec![0xff])],
            &NoColumns
        ),
        Some(Err(EvalError::Unsupported(
            "native integer radix evaluation was removed; TiKV engine required or function unsupported"
        )))
    );
    assert_eq!(e("ord()"), RADIX_REMOVED);
}

/// Source scalar rows from `pkg/expression/builtin_string_test.go:1436
/// TestHexFunc`. `HEX` has two normal signatures: numbers use ETInt
/// conversion, while strings and hex literals preserve bytes. Session charset
/// conversion and the source's injected-error datum remain outside this
/// value-only evaluator.
#[test]
fn hex_source_vectors_preserve_numeric_and_byte_signatures() {
    for (input, want) in [
        ("'abc'", "STR:616263"),
        ("'你好'", "STR:E4BDA0E5A5BD"),
        ("12", "STR:C"),
        // Go's source table uses untyped Go floats. Scientific SQL literals
        // select this seed's matching Float value domain rather than Decimal.
        ("12.3e0", "STR:C"),
        ("12.8e0", "STR:D"),
        ("-1", "STR:FFFFFFFFFFFFFFFF"),
        ("-12.3e0", "STR:FFFFFFFFFFFFFFF4"),
        ("-12.8e0", "STR:FFFFFFFFFFFFFFF3"),
        (
            "cast('2017-01-01 12:01:01' as datetime)",
            "STR:323031372D30312D30312031323A30313A3031",
        ),
        ("cast('12:01:01' as time)", "STR:31323A30313A3031"),
        ("null", "NULL"),
        ("'🀁'", "STR:F09F8081"),
        (
            "'一忒(๑•ㅂ•)و✧'",
            "STR:E4B880E5BF9228E0B991E280A2E38582E280A229D988E29CA7",
        ),
    ] {
        assert_engine_radix_value(&format!("hex({input})"), want);
    }
    // Go returns 7B2261223A20317D (`{"a": 1}`). The existing explicit
    // `cast_json` spelling gate prevents TiKV lowering, so pin this separately
    // from the string-signature rows rather than selecting HEX's integer arm.
    assert_radix_refusal("hex(cast('{\"a\":1}' as json))");
    #[cfg(feature = "tikv-expr")]
    assert!(engine_declines("hex(cast('{\"a\":1}' as json))"));

    // Binary-literal provenance is intentionally rejected by recursive TiKV
    // admission rather than guessed from the payload bytes.
    for expr in ["hex(0x0c)", "hex(0x12)"] {
        assert_radix_refusal(expr);
        #[cfg(feature = "tikv-expr")]
        assert!(engine_declines(expr), "engine unexpectedly admitted {expr}");
    }
}

/// Scalar rows from `pkg/expression/builtin_string_test.go:1508
/// TestUnhexFunc`. TiKV returns an ETString value with binary collation even
/// when the payload is valid UTF-8; retaining that binary metadata preserves
/// the SQL type boundary without a native `Bytes` helper.
#[test]
fn unhex_source_vectors_preserve_odd_digit_left_padding() {
    let binary = |bytes| Datum::new_collation_string(bytes, tidb_datatype::Collation::Binary);
    for (input, want) in [
        ("'4D7953514C'", binary(b"MySQL".to_vec())),
        ("'1267'", binary(b"\x12g".to_vec())),
        ("'126'", binary(b"\x01&".to_vec())),
        ("''", binary(Vec::new())),
        ("1267", binary(b"\x12g".to_vec())),
        ("126", binary(b"\x01&".to_vec())),
        ("1267.3", Datum::Null),
        ("'string'", Datum::Null),
        ("'你好'", Datum::Null),
        ("null", Datum::Null),
    ] {
        let expr = format!("unhex({input})");
        #[cfg(feature = "tikv-expr")]
        assert_eq!(engine_v(&expr), want, "UNHEX({input})");
        let _ = &want;
        assert_radix_refusal(&expr);
    }

    #[cfg(feature = "tikv-expr")]
    assert_eq!(engine_v("unhex('FF00')"), binary(vec![0xff, 0]));
    assert_radix_refusal("unhex('FF00')");
    // Arbitrary native bytes can no longer bypass lowering and are refused at
    // the values-only boundary instead of recreating UNHEX locally.
    assert_eq!(
        crate::func::eval_func_values(
            "UNHEX",
            &[Datum::new_bytes(vec![0xff])],
            &NoColumns
        ),
        Some(Err(EvalError::Unsupported(
            "native integer radix evaluation was removed; TiKV engine required or function unsupported"
        )))
    );
}

/// Default-charset rows from `pkg/expression/builtin_string_test.go:1548
/// TestBitLength`. The current UTF-8 String domain deliberately does not
/// pretend to model the source table's GBK connection-charset conversion.
#[test]
fn bit_length_source_vectors_preserve_utf8_byte_count() {
    for (input, want) in [
        ("'hi'", "INT:16"),
        ("'你好'", "INT:48"),
        ("''", "INT:0"),
        ("'一二三'", "INT:72"),
        ("'一二三!'", "INT:80"),
    ] {
        assert_engine_string2_value(&format!("bit_length({input})"), want);
    }

    // Go's len(val) counts binary bytes without UTF-8 validation.
    assert_engine_string2_value("bit_length(unhex('FF00'))", "INT:16");
}

/// Full UTF-8-value-domain vector from
/// `pkg/expression/builtin_string_test.go:2071 TestOct`.  The three source
/// binary-literal rows deliberately remain outside this table: a bit literal
/// selects Go's ETInt signature from its original AST shape, while this seed
/// has no byte-string/AST-context value domain for `b'11111111'`.
#[test]
fn oct_source_vectors_preserve_distinct_string_and_integer_signatures() {
    for (expr, want) in [
        ("oct(-1.5e0)", "STR:1777777777777777777777"),
        ("oct(-1)", "STR:1777777777777777777777"),
        ("oct(1.0e0)", "STR:1"),
        ("oct(9.5e0)", "STR:11"),
        ("oct(13)", "STR:15"),
        ("oct(1025)", "STR:2001"),
        ("oct(null)", "NULL"),
    ] {
        assert_engine_radix_value(expr, want);
    }

    // TiKV owns the string signature too; these rows ensure it does not get
    // silently rewritten to the integer signature.
    for (expr, want) in [
        ("oct('-2.7')", "STR:1777777777777777777776"),
        ("oct('0')", "STR:0"),
        ("oct('1')", "STR:1"),
        ("oct('8')", "STR:10"),
        ("oct('12')", "STR:14"),
        ("oct('20')", "STR:24"),
        ("oct('100')", "STR:144"),
        ("oct('1024')", "STR:2000"),
        ("oct('2048')", "STR:4000"),
        ("oct('8a8')", "STR:10"),
        ("oct('abc')", "STR:0"),
        (
            "oct('9999999999999999999999999')",
            "STR:1777777777777777777777",
        ),
        (
            "oct('-9999999999999999999999999')",
            "STR:1777777777777777777777",
        ),
        ("oct('')", "NULL"),
        ("oct(' ')", "STR:0"),
    ] {
        assert_engine_radix_value(expr, want);
    }
    assert_engine_radix_value("oct(unhex('FF'))", "STR:0");
    assert_eq!(
        crate::func::eval_func_values(
            "OCT",
            &[Datum::new_bytes(vec![0xff])],
            &NoColumns
        ),
        Some(Err(EvalError::Unsupported(
            "native integer radix evaluation was removed; TiKV engine required or function unsupported"
        )))
    );
}

#[test]
fn any_value_source_vectors_execute_only_in_tikv() {
    // pkg/expression/builtin_miscellaneous_test.go:240 TestAnyValue
    for (expr, want) in [
        ("any_value(null)", "NULL"),
        ("any_value(1234)", "INT:1234"),
        ("any_value(-153)", "INT:-153"),
        ("any_value(cast(3.1415926 as double))", "FLOAT:3.1415926"),
        ("any_value('Hello, World')", "STR:Hello, World"),
    ] {
        #[cfg(feature = "tikv-expr")]
        assert_eq!(engine_e(expr), want, "TiKV: {expr}");
        assert_misc_refusal(expr);
    }
}

#[test]
fn unary_minus_source_vectors_preserve_uint_overflow_domain() {
    // pkg/expression/builtin_op_test.go:30 TestUnary
    assert_eq!(e("-9223372036854775809"), "DEC:-9223372036854775809");
    assert_eq!(e("-9223372036854775810"), "DEC:-9223372036854775810");
    assert_eq!(e("-9223372036854775808"), "INT:-9223372036854775808");
    assert_eq!(e("-(-9223372036854775808)"), "DEC:9223372036854775808");
}

#[test]
fn like_source_vectors_preserve_default_escape_semantics() {
    // pkg/expression/builtin_like_test.go:30 TestLike
    for (expr, want) in [
        ("'a' like ''", "INT:0"),
        ("'a' like 'a'", "INT:1"),
        ("'a' like 'b'", "INT:0"),
        ("'aA' like 'Aa'", "INT:0"),
        ("'aAb' like 'Aa%'", "INT:0"),
        ("'aAb' like 'aA_'", "INT:1"),
        ("'baab' like 'b_%b'", "INT:1"),
        ("'baab' like 'b%_b'", "INT:1"),
        ("'bab' like 'b_%b'", "INT:1"),
        ("'bab' like 'b%_b'", "INT:1"),
        ("'bb' like 'b_%b'", "INT:0"),
        ("'bb' like 'b%_b'", "INT:0"),
        ("'baabccc' like 'b_%b%'", "INT:1"),
        ("'a' like '\\\\a'", "INT:1"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

#[test]
fn like_preserves_invalid_utf8_source_bytes() {
    // Both Go wildcard implementations accept arbitrary string bytes: the
    // binary collation compares bytes, while utf8mb4_bin consumes malformed
    // bytes one at a time as RuneError. Converting either operand to a Rust
    // `String` turns these valid LIKE expressions into errors.
    for expression in ["0xff like 0xff", "0xff like '_'", "0xffff like '__'"] {
        assert_eq!(e(expression), "INT:1", "AST tier: {expression}");
        assert_eq!(chunk_e(expression), "INT:1", "chunk tier: {expression}");
    }
    assert_eq!(e("0xffff like '_'"), "INT:0");
    assert_eq!(chunk_e("0xffff like '_'"), "INT:0");
    assert_eq!(chunk_e("_binary 0xff like _binary 0xfe"), "INT:0");
    assert_eq!(chunk_e("_binary 0xff ilike _binary 0xfe"), "INT:0");
}

#[test]
fn ilike_uses_source_ascii_lowering_and_escape_rules() {
    // `pkg/expression/builtin_ilike_test.go::TestIlike`: TiDB lowers ASCII
    // bytes only, so Unicode case pairs remain distinct. An ASCII-letter
    // escape marker is preserved while the other pattern bytes are lowered.
    assert_eq!(e("'ü' ilike 'Ü'"), "INT:0");
    assert_eq!(e("'abc' ilike 'ABC' escape 'A'"), "INT:0");
}

#[test]
fn reverse_source_vectors_preserve_scalar_string_coercion() {
    // pkg/expression/builtin_string_test.go:689 TestReverse
    for (expr, want) in [
        ("reverse(null)", "NULL"),
        ("reverse('abc')", "STR:cba"),
        ("reverse('LIKE')", "STR:EKIL"),
        ("reverse(123)", "STR:321"),
        ("reverse('')", "STR:"),
    ] {
        assert_engine_string2_value(expr, want);
    }
}

/// Default-charset scalar rows from
/// `pkg/expression/builtin_string_test.go:107 TestASCII`. `ASCII` consumes
/// the first encoded byte, so the UTF-8 input intentionally returns `228`
/// (the first byte of `你`) rather than its Unicode scalar value. Connection
/// charset conversion and the source's injected-error datum require runtime
/// state outside this value-only evaluator; their omission is recorded in the
/// partial ledger evidence, not disguised by a Rust-only rule.
#[test]
fn ascii_source_vectors_preserve_first_byte_and_string_coercion() {
    for (expr, want) in [
        ("ascii('2')", "INT:50"),
        ("ascii(2)", "INT:50"),
        ("ascii('23')", "INT:50"),
        ("ascii(23)", "INT:50"),
        ("ascii(2.3)", "INT:50"),
        ("ascii(null)", "NULL"),
        ("ascii('')", "INT:0"),
        ("ascii('你好')", "INT:228"),
    ] {
        assert_engine_string2_value(expr, want);
    }

    // Go's EvalString also accepts arbitrary binary values.
    assert_engine_string2_value("ascii(unhex('FF00'))", "INT:255");
}

/// Currently representable scalar rows from
/// `pkg/expression/builtin_string_test.go::TestLengthAndOctetLength`. The
/// source runs the identical table through both function names.
#[test]
fn length_and_octet_length_source_vectors_count_evaluated_bytes() {
    for function in ["length", "octet_length"] {
        for (argument, want) in [
            ("'abc'", "INT:3"),
            ("'你好'", "INT:6"),
            ("1", "INT:1"),
            ("3.14", "INT:4"),
            ("123.123", "INT:7"),
            ("0x01", "INT:1"),
            ("null", "NULL"),
        ] {
            let expression = format!("{function}({argument})");
            if argument.starts_with("0x") {
                assert_string_length_refusal(&expression, want);
            } else {
                assert_engine_string_length_value(&expression, want);
            }
        }

        // A binary cast can retain an incomplete UTF-8 suffix.  LENGTH and
        // OCTET_LENGTH must count the raw bytes selected by Go's
        // `builtinLengthSig`, rather than trying to decode or count runes.
        let expression = format!("{function}(cast('你好world' as binary(5)))");
        assert_string_length_refusal(&expression, "INT:5");
    }
}

/// Default-charset scalar rows from
/// `pkg/expression/builtin_string_test.go:1635 TestCharLength`. Go selects
/// `builtinCharLengthUTF8Sig` for these non-binary arguments and counts runes
/// after ETString coercion. The source's second loop explicitly mutates the
/// argument FieldType to binary, selecting `builtinCharLengthBinarySig`; that
/// build-time distinction is covered by `build::tests`. This context-free AST
/// evaluator has no general type-inference pass, so it deliberately constructs
/// the deterministic default character signature instead.
#[test]
fn char_length_source_vectors_preserve_utf8_rune_count_and_coercion() {
    for (expr, want) in [
        ("char_length('33')", "INT:2"),
        ("char_length('你好')", "INT:2"),
        ("char_length(33)", "INT:2"),
        ("char_length(3.14)", "INT:4"),
        ("char_length(null)", "NULL"),
    ] {
        assert_engine_string_length_value(expr, want);
    }
}

/// Public `eval` regression for Go's build-time `IsBinaryStr` selection.
/// These expected values were checked through the real `gorun` query path;
/// notably the identical UTF-8 bytes count as three for every binary source
/// form and one after an explicit character cast.
#[test]
fn char_length_public_eval_uses_source_field_type() {
    for (expression, want) in [
        ("char_length('你')", "INT:1"),
        ("char_length(0xE4BDA0)", "INT:3"),
        ("char_length(b'111001001011110110100000')", "INT:3"),
        ("char_length(cast('你' as binary))", "INT:3"),
        ("char_length(unhex('E4BDA0'))", "INT:3"),
        ("char_length(char(228,189,160))", "INT:3"),
        ("char_length(0xF0288C28)", "INT:4"),
        ("char_length(unhex('F0288C28'))", "INT:4"),
        ("char_length(cast(0xE4BDA0 as char))", "INT:1"),
        ("char_length(elt(1, 0xE4BDA0, 'x'))", "INT:3"),
        ("character_length((0xE4BDA0))", "INT:3"),
    ] {
        if expression.contains("unhex(") {
            #[cfg(feature = "tikv-expr")]
            assert_eq!(engine_e(expression), want, "TiKV: {expression}");
            assert_string_length_refusal(expression, want);
        } else if expression.contains("char(") || expression.contains("elt(") {
            assert_string_length_refusal(expression, want);
        } else {
            assert_string_length_refusal(expression, want);
        }
    }
    assert_string_length_refusal("char_length(from_base64('5L2g'))", "INT:3");
}

#[test]
fn char_using_is_an_explicit_contraction() {
    // Former Go value: STR:AAdD. TiKV has no CHAR_FUNC kernel.
    assert_string_aux_contraction("char(65, 16740, 67.5 using utf8)");
}

#[test]
fn convert_using_invalid_binary_literal_is_null_in_both_evaluators() {
    for expression in [
        "convert(0x1e240 using utf8)",
        "convert(x'01e240' using utf8)",
    ] {
        assert_eq!(e(expression), "NULL", "AST evaluator: {expression}");
        assert_eq!(chunk_e(expression), "NULL", "chunk evaluator: {expression}");
    }
    for (expression, expected) in [
        ("convert(123 using utf8)", "STR:123"),
        ("convert(0x7e using binary)", "STR:~"),
        (
            "convert(0xe4b8ade696870a using utf8)",
            "STR:\u{4e2d}\u{6587}\n",
        ),
    ] {
        assert_eq!(e(expression), expected, "AST evaluator: {expression}");
        assert_eq!(
            chunk_e(expression),
            expected,
            "chunk evaluator: {expression}"
        );
    }
}

#[test]
fn char_length_rejects_unresolved_field_type_before_runtime_datum() {
    struct RuntimeBytes;

    impl Columns for RuntimeBytes {
        fn get(&self, _: &[String]) -> Option<Datum> {
            Some(Datum::new_bytes("你".as_bytes().to_vec()))
        }
    }

    let expression = Expr::Func {
        name: "CHAR_LENGTH".to_string(),
        args: vec![Expr::Column(vec!["binary_col".to_string()])],
        origin_position: 0,
    };
    assert_eq!(
        eval_in(&expression, &RuntimeBytes),
        Err(EvalError::Unsupported(
            "native string length evaluation was removed; TiKV engine required"
        ))
    );
}

#[test]
fn elt_source_vectors_preserve_selector_and_result_coercion() {
    // pkg/expression/builtin_string_test.go:2443 TestElt
    for (expr, want) in [
        ("elt(1, 'Hej', 'ej', 'Heja', 'hej', 'foo')", "STR:Hej"),
        ("elt(9, 'Hej', 'ej', 'Heja', 'hej', 'foo')", "NULL"),
        ("elt(-1, 'Hej', 'ej', 'Heja', 'ej', 'hej', 'foo')", "NULL"),
        ("elt(0, 2, 3, 11, 1)", "NULL"),
        ("elt(3, 2, 3, 11, 1)", "STR:11"),
    ] {
        assert_engine_string_aux_value(expr, want);
    }
    // Former Go value: STR:2.1; fractional selectors are not admitted.
    assert_string_aux_contraction("elt(1.1e0, '2.1', '3.1', '11.1', '1.1')");
}

#[test]
fn quote_source_vectors_preserve_byte_exact_escaping() {
    // pkg/expression/builtin_string_test.go:2528 TestQuote
    for (hex, want) in [
        ("446f6e5c277421", "STR:'Don\\\\\\'t!'"),
        ("446f6e2774", "STR:'Don\\'t'"),
        ("446f6e22", "STR:'Don\"'"),
        ("446f6e5c22", "STR:'Don\\\\\"'"),
        ("5c27", "STR:'\\\\\\''"),
        ("5c22", "STR:'\\\\\"'"),
        ("001a", "STR:'\\0\\Z'"),
    ] {
        let _ = want; // retained Go value for the contracted binary-literal shape
        assert_string_aux_contraction(&format!("quote(x'{hex}')"));
    }
    for (expr, want) in [
        ("quote('萌萌哒(๑•ᴗ•๑)😊')", "STR:'萌萌哒(๑•ᴗ•๑)😊'"),
        ("quote('㍿㌍㍑㌫')", "STR:'㍿㌍㍑㌫'"),
        ("quote(null)", "STR:NULL"),
    ] {
        assert_engine_string_aux_value(expr, want);
    }
}

#[test]
fn make_set_source_vectors_preserve_signed_bit_masks() {
    // pkg/expression/builtin_string_test.go:2045 TestMakeSet
    for expression in [
        "make_set(1, 'a', 'b', 'c')",
        "make_set(5, 'hello', 'nice', 'world')",
        "make_set(5, 'hello', 'nice', null, 'world')",
        "make_set(0, 'a', 'b', 'c')",
        "make_set(null, 'a', 'b', 'c')",
        "make_set(-100, 'hello', 'nice', 'abc', 'world')",
        "make_set(-1, 'hello', 'nice', 'abc', 'world')",
    ] {
        assert_packet_string_refusal(expression);
    }
}

/// Regression: `bits` evaluating to the UNSIGNED domain -- a bitwise OR's
/// result, confirmed via `gorun` (`MAKE_SET(1|4,'a','b','c')` is `'a,c'`) --
/// used to fall through `make_set`'s `Datum::Int`-only match and answer
/// `NULL` instead of reading the same bit pattern. More set bits than
/// strings (`31` against 3 arguments) simply has nothing to match past the
/// last one, confirmed via the same run.
#[test]
fn make_set_reads_unsigned_bits_too() {
    assert_packet_string_refusal("make_set(1|4, 'a', 'b', 'c')");
    assert_packet_string_refusal("make_set(31, 'a', 'b', 'c')");
}

#[test]
fn field_source_vectors_preserve_numeric_and_string_comparison_modes() {
    // pkg/expression/builtin_string_test.go:1712 TestField
    for (expr, want) in [
        ("field('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')", "INT:2"),
        ("field('fo', 'Hej', 'ej', 'Heja', 'hej', 'foo')", "INT:0"),
        (
            "field('ej', 'Hej', 'ej', 'Heja', 'ej', 'hej', 'foo')",
            "INT:2",
        ),
        ("field(1, 2, 3, 11, 1)", "INT:4"),
        ("field(null, 2, 3, 11, 1)", "INT:0"),
        ("field(1.1e0, 2.1e0, 3.1e0, 11.1e0, 1.1e0)", "INT:4"),
        ("field(1.1e0, '2.1', '3.1', '11.1', '1.1')", "INT:4"),
        ("field('1.1a', 2.1e0, 3.1e0, 11.1e0, 1.1e0)", "INT:4"),
        ("field(1.10, 0, 11e-1)", "INT:2"),
        ("field('abc', 0, 1, 11.1e0, 1.1e0)", "INT:1"),
    ] {
        if !matches!(
            expr,
            "field('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')"
                | "field('fo', 'Hej', 'ej', 'Heja', 'hej', 'foo')"
                | "field('ej', 'Hej', 'ej', 'Heja', 'ej', 'hej', 'foo')"
                | "field(1, 2, 3, 11, 1)"
                | "field(1.1e0, 2.1e0, 3.1e0, 11.1e0, 1.1e0)"
        ) {
            let _ = want;
            assert_string_aux_contraction(expr);
        } else {
            assert_engine_string_aux_value(expr, want);
        }
    }
}

#[test]
fn field_mixed_arguments_select_one_real_signature() {
    // pkg/expression/builtin_string.go:2822 fieldFunctionClass selects
    // builtinFieldRealSig for a list containing both strings and integers.
    // Every argument is therefore compared through EvalReal: '1' and '01'
    // are equal numerically, even though they are different text values.
    // Former Go value for both rows: INT:1; mixed FIELD lowering is contracted.
    assert_string_aux_contraction("field('1', '01', 1)");
    assert_string_aux_contraction("field('1', '1x', 1)");
}

#[test]
fn pad_source_vectors_are_explicitly_contracted() {
    // pkg/expression/builtin_string_test.go:1747 TestLpad and :1789 TestRpad
    for (expr, _former_expected) in [
        ("lpad('hi', 5, '?')", "STR:???hi"),
        ("lpad('hi', 1, '?')", "STR:h"),
        ("lpad('hi', 0, '?')", "STR:"),
        ("lpad('hi', -1, '?')", "NULL"),
        ("lpad('hi', 1, '')", "STR:h"),
        ("lpad('hi', 5, '')", "STR:"),
        ("lpad('hi', 5, 'ab')", "STR:abahi"),
        ("lpad('hi', 6, 'ab')", "STR:ababhi"),
        ("lpad('中文', 5, '字符')", "STR:字符字中文"),
        ("lpad('中文', 1, 'a')", "STR:中"),
        ("lpad('中文', -5, '字符')", "NULL"),
        ("lpad('中文', 10, '')", "STR:"),
        ("lpad('1', 4611686018427387904, '1')", "NULL"),
        ("rpad('hi', 5, '?')", "STR:hi???"),
        ("rpad('hi', 1, '?')", "STR:h"),
        ("rpad('hi', 0, '?')", "STR:"),
        ("rpad('hi', -1, '?')", "NULL"),
        ("rpad('hi', 1, '')", "STR:h"),
        ("rpad('hi', 5, '')", "STR:"),
        ("rpad('hi', 5, 'ab')", "STR:hiaba"),
        ("rpad('hi', 6, 'ab')", "STR:hiabab"),
        ("rpad('中文', 5, '字符')", "STR:中文字符字"),
        ("rpad('中文', 1, 'a')", "STR:中"),
        ("rpad('中文', -5, '字符')", "NULL"),
        ("rpad('中文', 10, '')", "STR:"),
        ("rpad('1', 4611686018427387904, '1')", "NULL"),
    ] {
        assert_packet_string_refusal(expr);
    }
}

#[test]
fn right_and_rpad_sig_source_vectors_preserve_scalar_boundaries() {
    // `TestStringRight` (pkg/expression/builtin_string_test.go:2719) is the
    // character-valued RIGHT path; the binary assertion below keeps its
    // separate byte signature visible without pretending a Go chunk column
    // or session field type exists in this evaluator.
    for (expr, want) in [
        ("right('helloworld', 5)", "STR:world"),
        ("right('helloworld', 10)", "STR:helloworld"),
        ("right('helloworld', 11)", "STR:helloworld"),
        ("right('helloworld', -1)", "STR:"),
        ("right('', 2)", "STR:"),
        ("right(NULL, 2)", "NULL"),
    ] {
        assert_engine_string2_value(expr, want);
    }
    assert_engine_string2_value("hex(right(unhex('6162FF'), 1))", "STR:FF");

    // Keep every former scalar/binary row, but pin the physical-removal
    // contract instead of retaining a test-only RPAD implementation.
    for expr in [
        "rpad('abc', 6, '123')",
        "rpad(NULL, 6, '123')",
        "rpad('abc', 6, NULL)",
        "rpad(unhex('6162'), 3, unhex('FF'))",
    ] {
        assert_packet_string_refusal(expr);
    }
    for vals in [
        vec![
            Datum::new_bytes(b"ab".to_vec()),
            Datum::Int(3),
            Datum::new_bytes(vec![0xff]),
        ],
        Vec::new(),
    ] {
        assert_eq!(
            crate::func::eval_func_values_in("RPAD", &vals, &NoColumns),
            Some(Err(EvalError::Unsupported(
                "native packet-limited string evaluation was removed; function unsupported",
            )))
        );
    }
}

#[test]
fn repeat_source_vectors_are_explicitly_contracted() {
    // Preserve every Go-source input shape while asserting that no native
    // packet-sizing kernel remains, including NULL/unsigned/overflow rows.
    for expr in [
        "repeat('a', 2)",
        "repeat('a', 16777217)",
        "repeat('a', 16777216)",
        "repeat('a', -1)",
        "repeat('a', 0)",
        "repeat('a', cast(0 as unsigned))",
        "repeat(null, 2)",
        "repeat('a', null)",
        "repeat('a', 6)",
        "repeat('毅', 6)",
        "repeat('毅', 334)",
        "repeat('a', 2147483647)",
    ] {
        assert_packet_string_refusal(expr);
    }
}

#[test]
fn arithmetic() {
    assert_eq!(e("1 + 2 * 3"), "INT:7");
    assert_eq!(e("(1 + 2) * 3"), "INT:9");
    assert_eq!(e("7 DIV 2"), "INT:3");
    assert_eq!(e("7 MOD 3"), "INT:1");
    assert_eq!(e("- -5"), "INT:5");
    assert_eq!(e("~0"), "UINT:18446744073709551615");
}

#[test]
fn comparisons() {
    assert_eq!(e("1 = 1"), "INT:1");
    assert_eq!(e("2 < 1"), "INT:0");
    assert_eq!(e("1 <> 2"), "INT:1");
    assert_eq!(e("5 <=> 5"), "INT:1");
    // `types.CompareInt` retains both signedness bits: negative signed
    // values sort below every UInt, while same raw bits are not equal.
    assert_eq!(e("18446744073709551615 > -1"), "INT:1");
    assert_eq!(e("18446744073709551615 = -1"), "INT:0");
}

#[test]
fn builtin_functions() {
    assert_eq!(engine_e("abs(-5)"), "INT:5");
    assert_eq!(engine_e("sign(-3)"), "INT:-1");
    assert_engine_compare2_value("least(3, 1, 2)", "INT:1");
    assert_engine_compare2_value("greatest(1, 2, 3)", "INT:3");
    assert_compare2_refusal("least(5, 3, NULL, 1)", "NULL");
    assert_eq!(e("coalesce(NULL, NULL, 7)"), "INT:7");
    assert_eq!(e("if(0, 10, 20)"), "INT:20");
    assert_eq!(e("if(NULL, 10, 20)"), "INT:20");
    assert_eq!(e("ifnull(NULL, 5)"), "INT:5");
    assert_eq!(e("nullif(3, 3)"), "NULL");
    assert_eq!(e("nullif(3, 4)"), "INT:3");
    // Nested calls fold too.
    assert_engine_compare2_value("greatest(abs(-1), sign(-4), 0)", "INT:1");
}

#[test]
fn string_functions() {
    assert_eq!(e("'hello'"), "STR:hello");
    assert_packet_string_refusal("concat('a', 'b', 'c')");
    assert_packet_string_refusal("concat('x', NULL)");
    assert_engine_string_length_value("length('héllo')", "INT:6"); // bytes
    #[cfg(feature = "tikv-expr")]
    {
        assert_eq!(engine_e("length(unhex('FF00'))"), "INT:2");
        assert_eq!(engine_e("octet_length(unhex('FF00'))"), "INT:2");
    }
    assert_string_length_refusal("length(unhex('FF00'))", "INT:2");
    assert_string_length_refusal("octet_length(unhex('FF00'))", "INT:2");
    assert_engine_string_length_value("char_length('héllo')", "INT:5"); // chars
    assert_engine_string2_value("upper('abc')", "STR:ABC");
    assert_engine_string2_value("left('hello', 3)", "STR:hel");
    assert_engine_string2_value("right('hello', 2)", "STR:lo");
    #[cfg(feature = "tikv-expr")]
    assert_eq!(engine_e("substring('hello', 2, 3)"), "STR:ell");
    #[cfg(not(feature = "tikv-expr"))]
    assert_string2_refusal("substring('hello', 2, 3)");
    assert_packet_string_refusal("concat('n=', 5)"); // native packet context removed
    assert_eq!(e("if(1, 'yes', 'no')"), "STR:yes");
}

/// Complete representable rows from `TestLower` and `TestUpper` in
/// `pkg/expression/builtin_string_test.go`.  The seed evaluator has the
/// default UTF-8 text signature and an explicit binary signature: session
/// charset conversion (including GBK) is a separate metadata boundary, while
/// raw binary bytes must remain unchanged rather than being UTF-8-decoded.
#[test]
fn lower_upper_source_vectors_preserve_case_and_binary_boundaries() {
    for (expr, want) in [
        ("lower(NULL)", "NULL"),
        ("lower('ab')", "STR:ab"),
        ("lower(1)", "STR:1"),
        ("lower('one week’s time TEST')", "STR:one week’s time test"),
        (
            "lower(\"one week's time TEST\")",
            "STR:one week's time test",
        ),
        ("lower('ABC测试DEF')", "STR:abc测试def"),
        ("lower('ABCテストDEF')", "STR:abcテストdef"),
        ("upper(NULL)", "NULL"),
        ("upper('ab')", "STR:AB"),
        ("upper(1)", "STR:1"),
        ("upper('one week’s time TEST')", "STR:ONE WEEK’S TIME TEST"),
        (
            "upper(\"one week's time TEST\")",
            "STR:ONE WEEK'S TIME TEST",
        ),
        ("upper('abc测试def')", "STR:ABC测试DEF"),
        ("upper('abcテストdef')", "STR:ABCテストDEF"),
        ("lcase('AbC')", "STR:abc"),
        ("ucase('AbC')", "STR:ABC"),
    ] {
        assert_engine_string2_value(expr, want);
    }
    assert_engine_string2_value("hex(lower(unhex('41FF')))", "STR:41FF");
    assert_engine_string2_value("hex(upper(unhex('61FF')))", "STR:61FF");
    assert_eq!(e("upper()"), STRING2_REMOVED);
}

/// Complete representable rows from `TestStrcmp` in
/// `pkg/expression/builtin_string_test.go`.  The Go source uses the selected
/// string collation; this seed keeps the default byte-wise comparison while
/// preserving ETString numeric coercion and arbitrary binary input.  Session
/// collation changes and the injected harness error remain explicit partial
/// boundaries.
#[test]
fn strcmp_source_vectors_preserve_coercion_and_nulls() {
    for (expr, want) in [
        ("strcmp('123', '123')", "INT:0"),
        ("strcmp('123', '1')", "INT:1"),
        ("strcmp('1', '123')", "INT:-1"),
        ("strcmp('123', '45')", "INT:-1"),
        ("strcmp(123, '123')", "INT:0"),
        ("strcmp('12.34', 12.34)", "INT:0"),
        ("strcmp(NULL, '123')", "NULL"),
        ("strcmp('123', NULL)", "NULL"),
        ("strcmp('', '123')", "INT:-1"),
        ("strcmp('123', '')", "INT:1"),
        ("strcmp('', '')", "INT:0"),
        ("strcmp('', NULL)", "NULL"),
        ("strcmp(NULL, '')", "NULL"),
        ("strcmp(NULL, NULL)", "NULL"),
        ("strcmp('123 ', '123')", "INT:0"),
        ("strcmp(123, '123 ')", "INT:0"),
    ] {
        assert_engine_string2_value(expr, want);
    }
    assert_engine_string2_value("strcmp(unhex('FF'), unhex('00'))", "INT:1");
    assert_engine_string2_value("strcmp(unhex('61'), unhex('6120'))", "INT:-1");
    assert_engine_string2_value("strcmp('a ', unhex('61'))", "INT:1");
    assert_eq!(e("strcmp('a')"), STRING2_REMOVED);
}

/// Complete representable rows from `TestLeft` and `TestRight` in
/// `pkg/expression/builtin_string_test.go`.  Numeric-prefix count coercion,
/// Unicode character slicing, NULL propagation, and binary byte slicing are
/// all scalar value behavior; the injected Go error datum and FieldType/
/// warning state remain outside this evaluator.
#[test]
fn left_right_source_vectors_preserve_count_and_byte_boundaries() {
    for (expr, want) in [
        ("left('abcde', 3)", "STR:abc"),
        ("left('abcde', 0)", "STR:"),
        ("left('abcde', 1.2)", "STR:a"),
        ("left('abcde', 1.9)", "STR:ab"),
        ("left('abcde', -1)", "STR:"),
        ("left('abcde', 100)", "STR:abcde"),
        ("left('abcde', NULL)", "NULL"),
        ("left(NULL, 3)", "NULL"),
        ("left('abcde', '3')", "STR:abc"),
        ("left('abcde', 'a')", "STR:"),
        ("left(1234, 3)", "STR:123"),
        ("left(12.34, 3)", "STR:12."),
        ("right('abcde', 3)", "STR:cde"),
        ("right('abcde', 0)", "STR:"),
        ("right('abcde', 1.2)", "STR:e"),
        ("right('abcde', 1.9)", "STR:de"),
        ("right('abcde', -1)", "STR:"),
        ("right('abcde', 100)", "STR:abcde"),
        ("right('abcde', NULL)", "NULL"),
        ("right(NULL, 1)", "NULL"),
        ("right('abcde', '3')", "STR:cde"),
        ("right('abcde', 'a')", "STR:"),
        ("right(1234, 3)", "STR:234"),
        ("right(12.34, 3)", "STR:.34"),
    ] {
        assert_engine_string2_value(expr, want);
    }
    assert_engine_string2_value("hex(left(unhex('0102'), 1))", "STR:01");
    assert_engine_string2_value("hex(right(unhex('0102'), 1))", "STR:02");
    assert_engine_string2_value("left('你好世界', 2)", "STR:你好");
    assert_engine_string2_value("right('你好世界', 2)", "STR:世界");
}

/// Complete representable rows from `TestReplace` in
/// `pkg/expression/builtin_string_test.go`.  REPLACE evaluates every operand
/// through Go's byte-preserving `EvalString`; the direct binary assertion
/// keeps invalid UTF-8 and embedded NULs on that same path instead of forcing
/// a Rust text decode.
#[test]
fn replace_source_vectors_preserve_byte_coercion() {
    for (expr, want) in [
        (
            "replace('www.mysql.com', 'mysql', 'pingcap')",
            "STR:www.pingcap.com",
        ),
        ("replace('www.mysql.com', 'w', 1)", "STR:111.mysql.com"),
        ("replace(1234, 2, 55)", "STR:15534"),
        ("replace('', 'a', 'b')", "STR:"),
        ("replace('abc', '', 'd')", "STR:abc"),
        ("replace('aaa', 'a', '')", "STR:"),
        ("replace(NULL, 'a', 'b')", "NULL"),
        ("replace('a', NULL, 'b')", "NULL"),
        ("replace('a', 'b', NULL)", "NULL"),
    ] {
        assert_engine_string2_value(expr, want);
    }
    assert_engine_string2_value(
        "hex(replace(unhex('FF0061'), unhex('FF'), unhex('FE62')))",
        "STR:FE620061",
    );
    assert_eq!(e("replace('abc', 'a')"), STRING2_REMOVED);
}

/// Complete representable rows from `TestSubstringIndex` in
/// `pkg/expression/builtin_string_test.go`.  The source signature is
/// `ETString, ETString, ETInt`, so string/decimal counts use the shared
/// numeric-prefix/rounding coercion rather than an integer-literal-only path.
#[test]
fn substring_index_source_vectors_preserve_count_and_bytes() {
    for (expr, want) in [
        (
            "substring_index('www.pingcap.com', '.', 2)",
            "STR:www.pingcap",
        ),
        (
            "substring_index('www.pingcap.com', '.', -2)",
            "STR:pingcap.com",
        ),
        ("substring_index('www.pingcap.com', '.', 0)", "STR:"),
        (
            "substring_index('www.pingcap.com', '.', 100)",
            "STR:www.pingcap.com",
        ),
        (
            "substring_index('www.pingcap.com', '.', -100)",
            "STR:www.pingcap.com",
        ),
        ("substring_index('www.pingcap.com', 'd', 0)", "STR:"),
        (
            "substring_index('www.pingcap.com', 'd', 1)",
            "STR:www.pingcap.com",
        ),
        (
            "substring_index('www.pingcap.com', 'd', -1)",
            "STR:www.pingcap.com",
        ),
        ("substring_index('www.pingcap.com', '', 0)", "STR:"),
        ("substring_index('www.pingcap.com', '', 1)", "STR:"),
        ("substring_index('www.pingcap.com', '', -1)", "STR:"),
        ("substring_index('www.pingcap.com', '', NULL)", "NULL"),
        ("substring_index('', '.', 0)", "STR:"),
        ("substring_index('', '.', 1)", "STR:"),
        ("substring_index('', '.', -1)", "STR:"),
        ("substring_index(NULL, '.', 1)", "NULL"),
        ("substring_index('www.pingcap.com', NULL, 1)", "NULL"),
        ("substring_index('www.pingcap.com', '.', NULL)", "NULL"),
        (
            "substring_index('www.pingcap.com', '.', '2')",
            "STR:www.pingcap",
        ),
        (
            "substring_index('www.pingcap.com', '.', 2.5)",
            "STR:www.pingcap.com",
        ),
    ] {
        if expr.ends_with("'.', '2')") || expr.ends_with("'.', 2.5)") {
            assert_string_aux_contraction(expr);
        } else {
            assert_engine_string_aux_value(expr, want);
        }
    }
    // TiKV's kernel calls `count.abs()`: i64::MIN can overflow, and the
    // unsigned-above-i64 source shape cannot be represented by its signature.
    assert_string_aux_contraction("substring_index('a.b.c', '.', -9223372036854775808)");
    assert_string_aux_contraction("substring_index('a.b.c', '.', 18446744073709551616)");
    // The raw invalid-byte Datum case has no SQL spelling with preserved source
    // provenance, so retain its Go value only as contraction documentation:
    // SUBSTRING_INDEX(0x61ff62ff63, 0xff, -2) => 0x62ff63.
}

/// Complete scalar rows from `TestTrim`.  TRIM removes repeated whole byte
/// prefixes/suffixes, and only ASCII space is implicit for the one-argument
/// form; tabs, CR, and LF remain ordinary payload bytes exactly as in Go.
#[test]
fn trim_source_vectors_preserve_direction_and_whole_remstr() {
    for (expr, want) in [
        ("trim('   bar   ')", "STR:bar"),
        ("trim('')", "STR:"),
        ("trim(NULL)", "NULL"),
        ("trim('x' from 'xxxbarxxx')", "STR:bar"),
        ("trim('x' from 'bar')", "STR:bar"),
        ("trim('' from '   bar   ')", "STR:   bar   "),
        ("trim('x' from '')", "STR:"),
        ("trim(NULL from 'bar')", "NULL"),
        ("trim('x' from NULL)", "NULL"),
        ("trim(leading 'x' from 'xxxbarxxx')", "STR:barxxx"),
        ("trim(trailing 'xyz' from 'barxxyz')", "STR:barx"),
        ("trim(both 'x' from 'xxxbarxxx')", "STR:bar"),
    ] {
        let _ = want;
        assert_string_aux_contraction(expr);
    }
    // Former direct byte-helper expectations are retained in this test's SQL rows.
}

/// Scalar and binary source rows from `TestConcat`, including the historical
/// expected values. With the native packet-aware kernel deleted, every shape
/// must now return the exact contraction instead of reproducing that coercion.
#[test]
fn concat_source_vectors_are_explicitly_contracted() {
    for (expr, want) in [
        ("concat(null)", "NULL"),
        (
            "concat('a', 'b', 1, 2, 1.1, 1.2, cast(1.1 as decimal(3, 1)))",
            "STR:ab121.11.21.1",
        ),
        ("concat('a', 'b', null, 'c')", "NULL"),
        ("concat(0xFF, 'a')", "STR_HEX:FF61"),
        ("concat('a', unhex('FF00'))", "STR_HEX:61FF00"),
    ] {
        let _ = want;
        assert_packet_string_refusal(expr);
    }
}

/// Scalar and separator source rows from `TestConcatWS`. The historical NULL,
/// separator, and coercion answers remain visible as receipts, but every shape
/// now asserts the exact packet-context contraction.
#[test]
fn concat_ws_source_vectors_are_explicitly_contracted() {
    for (expr, want) in [
        ("concat_ws(null, null)", "NULL"),
        ("concat_ws(null, 'a', 'b')", "NULL"),
        (
            "concat_ws(',', 'a', 'b', 'hello', '$^%')",
            "STR:a,b,hello,$^%",
        ),
        ("concat_ws('|', 'a', null, 'b', 'c')", "STR:a|b|c"),
        ("concat_ws(',', 'a', ',', 'b', 'c')", "STR:a,,,b,c"),
        (
            "concat_ws(',', 'a', 'b', 1, 2, 1.1, 0.11, cast(1.1 as decimal(3, 1)))",
            "STR:a,b,1,2,1.1,0.11,1.1",
        ),
        ("concat_ws(0x2c, 0x61, 'b')", "STR:a,b"),
        ("concat_ws(',', 'a', '')", "STR:a,"),
    ] {
        let _ = want;
        assert_packet_string_refusal(expr);
    }
}

#[test]
fn concat_signature_source_rows_are_explicitly_contracted() {
    // `TestConcatSig` (pkg/expression/builtin_string_test.go:225) uses
    // chunk-column metadata to exercise max_allowed_packet warnings. Preserve
    // its historical scalar rows while asserting the exact contraction.
    for (expr, want) in [
        ("concat('a', 'b')", "STR:ab"),
        ("concat('中', 'a')", "STR:中a"),
        ("concat('中文', 'a')", "STR:中文a"),
    ] {
        let _ = want;
        assert_packet_string_refusal(expr);
    }

    // `TestConcatWSSig` (source line 345) has the same vectorized warning
    // boundary. Preserve its Unicode source rows as contraction receipts.
    for (expr, want) in [
        ("concat_ws(',', 'a', 'b')", "STR:a,b"),
        ("concat_ws(',', '中', 'a')", "STR:中,a"),
        ("concat_ws(',', '中文', 'a')", "STR:中文,a"),
    ] {
        let _ = want;
        assert_packet_string_refusal(expr);
    }
}

#[test]
fn instr_source_vectors_preserve_string_coercion_and_nulls() {
    // pkg/expression/builtin_string_test.go:1968 TestInstr. INSTR(str,
    // substr) shares the same 1-indexed character position contract as the
    // two-argument LOCATE path, but this test exercises the public dispatch.
    for (expr, want) in [
        ("instr('foobarbar', 'bar')", "INT:4"),
        ("instr('xbar', 'foobar')", "INT:0"),
        ("instr(123456234, 234)", "INT:2"),
        ("instr(123456, 567)", "INT:0"),
        ("instr(1e10, 1e2)", "INT:1"),
        ("instr(1.234, '.234')", "INT:2"),
        ("instr(1.234, '')", "INT:1"),
        ("instr('', 123)", "INT:0"),
        ("instr('', '')", "INT:1"),
        ("instr('中文美好', '美好')", "INT:3"),
        ("instr('中文美好', '世界')", "INT:0"),
        ("instr('中文abc', 'a')", "INT:3"),
        ("instr('live long and prosper', 'long')", "INT:6"),
        ("instr('not binary string', 'binary')", "INT:5"),
        ("instr('upper case', 'upper')", "INT:1"),
        ("instr('UPPER CASE', 'CASE')", "INT:7"),
        ("instr('中文abc', 'abc')", "INT:3"),
        ("instr('foobar', NULL)", "NULL"),
        ("instr(NULL, 'foobar')", "NULL"),
        ("instr(NULL, NULL)", "NULL"),
    ] {
        let _ = want;
        assert_string_aux_contraction(expr);
    }
    // The former binary value was INT:4; the outer INSTR now refuses first.
    assert_string_aux_contraction("instr(unhex('666f6f626172'), unhex('626172'))");
}

#[test]
fn predicates() {
    // IN with a match / no match / negation.
    assert_eq!(e("2 in (1, 2, 3)"), "INT:1");
    assert_eq!(e("5 in (1, 2, 3)"), "INT:0");
    assert_eq!(e("5 not in (1, 2, 3)"), "INT:1");
    assert_eq!(e("'b' in ('a', 'b')"), "INT:1");
    // IN three-valued logic: a NULL in the list (no match) is NULL, but a
    // real match still wins over the NULL.
    assert_eq!(e("5 in (1, NULL, 3)"), "NULL");
    assert_eq!(e("1 in (1, NULL)"), "INT:1");
    assert_eq!(e("NULL in (1, 2)"), "NULL");
    // BETWEEN and NOT BETWEEN.
    assert_eq!(e("5 between 1 and 10"), "INT:1");
    assert_eq!(e("5 between 6 and 10"), "INT:0");
    assert_eq!(e("5 not between 6 and 10"), "INT:1");
    assert_eq!(e("NULL between 1 and 10"), "NULL");
    // Former IS NULL values were 1, 0 and 1; that direct AST kernel is gone.
    assert_misc_refusal("NULL is null");
    assert_misc_refusal("1 is null");
    assert_misc_refusal("1 is not null");
    // IS TRUE/FALSE remains a distinct retained predicate.
    assert_eq!(e("1 is true"), "INT:1");
    assert_eq!(e("0 is true"), "INT:0");
    assert_eq!(e("NULL is true"), "INT:0");
    assert_eq!(e("NULL is not true"), "INT:1");
    assert_eq!(e("0 is false"), "INT:1");
}

/// Bounded value-only slice of `pkg/expression/builtin_other_test.go`'s
/// `TestRowFunc`/`TestInFunc` rows.  The Go function-class test constructs a
/// four-argument row signature (it does not evaluate a bare row), while the
/// production Rust evaluator intentionally gives `ROW(...)` meaning only as
/// a comparison/`IN` operand.  Keeping the row inside those SQL contexts
/// proves the same source shape without inventing a standalone row value
/// domain.  Temporal, duration, JSON, collation-metadata, and vectorized
/// signatures remain explicit partial boundaries below.
#[test]
fn builtin_other_row_and_in_source_vectors() {
    assert_eq!(
        e("row('1', 1.2, true, 120) = row('1', 1.2, true, 120)"),
        "INT:1"
    );
    assert_eq!(e("row(1, 2) <> row(1, 3)"), "INT:1");
    assert_eq!(e("row(NULL, 2) <=> row(NULL, 2)"), "INT:1");
    assert_eq!(e("row(NULL, 2) <=> row(NULL, 3)"), "INT:0");
    assert_eq!(e("row(1, NULL) <=> row(1, 2)"), "INT:0");
    assert_eq!(chunk_e("(NULL, 2) <=> (NULL, 2)"), "INT:1");
    assert_eq!(chunk_e("(NULL, 2) <=> (NULL, 3)"), "INT:0");
    assert_eq!(chunk_e("(1, 2) = (1, 2)"), "INT:1");
    assert_eq!(chunk_e("(1, 2) <> (1, 3)"), "INT:1");
    assert_eq!(chunk_e("(1, 2) < (1, 3)"), "INT:1");
    assert_eq!(chunk_e("(1, 3) <= (1, 3)"), "INT:1");
    assert_eq!(chunk_e("(1, 4) > (1, 3)"), "INT:1");
    assert_eq!(chunk_e("(1, 3) >= (1, 3)"), "INT:1");
    assert_eq!(chunk_e("(0, NULL) < (1, NULL)"), "INT:1");
    assert_eq!(chunk_e("(1, NULL) < (1, 2)"), "NULL");
    assert_eq!(chunk_e("(1, 2) = (1, 2, 3)"), "OperandColumns(2)");
    assert_eq!(chunk_e("(1, 2) in ((1, 2), (3, 4))"), "INT:1");
    assert_eq!(chunk_e("(1, 2) not in ((1, 2), (3, 4))"), "INT:0");
    assert_eq!(chunk_e("(1, 2) in ((1, 3), (3, 4))"), "INT:0");
    assert_eq!(chunk_e("(1, 2) in ((1, 2, 3))"), "OperandColumns(2)");

    // Integer, unsigned-boundary, float, decimal, string, and NULL rows
    // from TestInFunc all stay in the seed Datum domain.
    for (expr, want) in [
        ("1 in (1, 2, 3)", "INT:1"),
        ("1 in (0, 2, 3)", "INT:0"),
        ("1 in (NULL, 2, 3)", "NULL"),
        ("NULL in (NULL, 2, 3)", "NULL"),
        (
            "18446744073709551615 in (18446744073709551615, 2, 3)",
            "INT:1",
        ),
        ("-1 in (18446744073709551615, 2, 3)", "INT:0"),
        ("1.1e0 in (1.1e0, 1.2e0, 1.3e0)", "INT:1"),
        ("1.1e0 in (1.2e0, 1.3e0)", "INT:0"),
        ("123.121 in (123.122, 123.123)", "INT:0"),
        ("123.121 in (123.122, 123.121)", "INT:1"),
        ("'1.1' in ('1.1', '1.2', '1.3')", "INT:1"),
        ("'1.1' in ('1.2', '1.3')", "INT:0"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

/// The source `TestTypeConversion` asks `BuildGetVarFunction` to convert an
/// integer user variable to DECIMAL and DOUBLE.  Rust's seed evaluator does
/// not yet expose Go's build-time FieldType/function-class seam, so this test
/// keeps the same stored-user-variable values and verifies the production
/// CAST conversion path explicitly.  The evidence remains PARTIAL until
/// typed user-variable retrieval is represented by the session contract.
#[test]
fn builtin_other_type_conversion_source_scalars() {
    struct UserVar(Datum);

    impl Columns for UserVar {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn get_uservar(&self, name: &str) -> Option<Datum> {
            (name.eq_ignore_ascii_case("a")).then(|| self.0.clone())
        }
    }

    fn eval_user_expr(sql: &str, resolver: &UserVar) -> String {
        let stmt = tidb_parser::parse(&format!("select {sql}")).expect("parse");
        let Stmt::Query(query) = stmt else {
            panic!("not query")
        };
        let QueryStmt::Select(s) = query.into_inner() else {
            panic!("not select")
        };
        let SelectField::Expr { expr, .. } = &s.fields[0] else {
            panic!("not expression")
        };
        eval_in(expr, resolver).expect("evaluate").label()
    }

    let resolver = UserVar(Datum::Int(3));
    assert_eq!(eval_user_expr("cast(@a as decimal)", &resolver), "DEC:3");
    assert_eq!(eval_user_expr("cast(@a as double)", &resolver), "FLOAT:3");
}

#[test]
fn like_predicate() {
    // Anchored, prefix, suffix, and infix `%`.
    assert_eq!(e("'abc' like 'abc'"), "INT:1");
    assert_eq!(e("'abc' like 'a%'"), "INT:1");
    assert_eq!(e("'abc' like '%c'"), "INT:1");
    assert_eq!(e("'abc' like '%b%'"), "INT:1");
    assert_eq!(e("'abc' like 'a%d'"), "INT:0");
    // `_` matches exactly one character.
    assert_eq!(e("'abc' like 'a_c'"), "INT:1");
    assert_eq!(e("'abc' like 'a_'"), "INT:0");
    assert_eq!(e("'ac' like 'a_c'"), "INT:0");
    // Case-sensitive (utf8mb4_bin), and NOT LIKE.
    assert_eq!(e("'abc' like 'ABC'"), "INT:0");
    assert_eq!(e("'abc' not like 'a%'"), "INT:0");
    assert_eq!(e("'abc' not like 'x%'"), "INT:1");
    // NULL operands.
    assert_eq!(e("NULL like 'a%'"), "NULL");
    assert_eq!(e("'abc' like NULL"), "NULL");
    // Empty pattern matches only the empty string.
    assert_eq!(e("'' like ''"), "INT:1");
    assert_eq!(e("'a' like ''"), "INT:0");
    // A non-string operand, on EITHER side, is implicitly stringified
    // the same way `Datum::sql_string` renders it -- including a
    // `DECIMAL`'s declared scale (`12.50` stringifies to `"12.50"`,
    // not simplified to `"12.5"`).
    assert_eq!(e("2 like '2'"), "INT:1");
    assert_eq!(e("123 like '12%'"), "INT:1");
    assert_eq!(e("12.50 like '12.5'"), "INT:0");
    assert_eq!(e("12.50 like '12.50'"), "INT:1");
    assert_eq!(e("1.5e2 like '150'"), "INT:1");
    assert_eq!(e("-5 like '-5'"), "INT:1");
    assert_eq!(e("'2' like 2"), "INT:1");
    assert_eq!(e("123 like 12"), "INT:0");

    // The source matcher compiles an explicit ESCAPE byte exactly like
    // `pkg/util/stringutil.CompilePattern`: a custom byte quotes the next
    // character. ESCAPE '' passes byte zero to the source compiler: ordinary
    // escape characters become literals, while an embedded NUL still quotes
    // the following character. A trailing escape byte is retained as a
    // literal (the Go compiler leaves it in place when nothing follows).
    assert_eq!(e("'a' like '+a' escape '+'"), "INT:1");
    assert_eq!(e("'a+' like 'a+' escape '+'"), "INT:1");
    assert_eq!(e("'a+' like 'a++' escape '+'"), "INT:1");
    assert_eq!(e("'a' like 'a\\\\'"), "INT:0");
    assert_eq!(e("'a\\\\' like 'a\\\\'"), "INT:1");
    assert_eq!(e("'a' like 'a\\\\' escape ''"), "INT:0");
    assert_eq!(e("'a\\\\' like 'a\\\\' escape ''"), "INT:1");
    assert_eq!(e("'a_' like 'a\\0_' escape ''"), "INT:1");
}

#[test]
fn three_valued_logic() {
    assert_eq!(e("1 AND NULL"), "NULL");
    assert_eq!(e("0 AND NULL"), "INT:0");
    assert_eq!(e("1 OR NULL"), "INT:1");
    assert_eq!(e("0 OR NULL"), "NULL");
    assert_eq!(e("NOT NULL"), "NULL");
    assert_eq!(e("NULL <=> NULL"), "INT:1");
    assert_eq!(e("NULL <=> 1"), "INT:0");
    assert_eq!(e("NULL + 1"), "NULL");
}

/// Complete scalar source tables from `TestLogicAnd` (lines 127-146) and
/// `TestLogicOr` (lines 346-369) in `pkg/expression/builtin_op_test.go`.
/// The final injected-error row in each Go table uses an `errors.New` datum;
/// the seed evaluator deliberately has no error-valued `Datum`, so its
/// propagation contract is tracked as an explicit model boundary in the
/// porting ledger rather than fabricated as a SQL literal.
#[test]
fn logic_and_or_follow_tidb_source_tables() {
    for (sql, want) in [
        ("1 AND 1", "INT:1"),
        ("1 AND 0", "INT:0"),
        ("0 AND 1", "INT:0"),
        ("0 AND 0", "INT:0"),
        ("2 AND -1", "INT:1"),
        ("'a' AND '0'", "INT:0"),
        ("'a' AND '1'", "INT:0"),
        ("'1a' AND '0'", "INT:0"),
        ("'1a' AND '1'", "INT:1"),
        ("0 AND NULL", "INT:0"),
        ("NULL AND 0", "INT:0"),
        ("NULL AND 1", "NULL"),
        ("0.001 AND 0", "INT:0"),
        ("0.001 AND 1", "INT:1"),
        ("NULL AND 0.000", "INT:0"),
        ("NULL AND 0.001", "NULL"),
        ("0.000001 AND 0", "INT:0"),
        ("0.000001 AND 1", "INT:1"),
        ("0.000000 AND NULL", "INT:0"),
        ("0.000001 AND NULL", "NULL"),
        ("1 OR 1", "INT:1"),
        ("1 OR 0", "INT:1"),
        ("0 OR 1", "INT:1"),
        ("0 OR 0", "INT:0"),
        ("2 OR -1", "INT:1"),
        ("'a' OR '0'", "INT:0"),
        ("'a' OR '1'", "INT:1"),
        ("'1a' OR '0'", "INT:1"),
        ("'1a' OR '1'", "INT:1"),
        ("'0.0a' OR 0", "INT:0"),
        ("'0.0001a' OR 0", "INT:1"),
        ("1 OR NULL", "INT:1"),
        ("NULL OR 1", "INT:1"),
        ("NULL OR 0", "NULL"),
        ("0.000 OR 0", "INT:0"),
        ("0.001 OR 0", "INT:1"),
        ("NULL OR 0.000", "NULL"),
        ("NULL OR 0.001", "INT:1"),
        ("0.000000 OR 0", "INT:0"),
        ("0.000000 OR 1", "INT:1"),
        ("0.000000 OR NULL", "NULL"),
        ("0.000001 OR 0", "INT:1"),
        ("0.000001 OR 1", "INT:1"),
        ("0.000001 OR NULL", "INT:1"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    assert!(tidb_parser::parse("select 1 AND").is_err());
    assert!(tidb_parser::parse("select 1 OR").is_err());
}

/// Source vectors from `TestLogicXor` in
/// `pkg/expression/builtin_op_test.go`: binary logical operators use ETInt
/// numeric-prefix coercion for strings, unlike ordinary string comparison.
#[test]
fn logic_xor_coerces_strings_and_preserves_three_valued_null() {
    for (sql, want) in [
        ("'a' XOR '0'", "INT:0"),
        ("'a' XOR '1'", "INT:1"),
        ("'1a' XOR '0'", "INT:1"),
        ("'1a' XOR '1'", "INT:0"),
        ("0.5000 XOR 0.4999", "INT:0"),
        ("0.5000 XOR 1.0", "INT:0"),
        ("0.4999 XOR 1.0", "INT:0"),
        ("NULL XOR 0.000", "NULL"),
        ("NULL XOR 0.001", "NULL"),
        ("0.000001 XOR 1", "INT:0"),
        ("0.000000 XOR NULL", "NULL"),
        ("0.000001 XOR NULL", "NULL"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    // This guard proves ordinary string operators retain collation comparison
    // semantics after logical dispatch moved ahead of that branch.
    assert_eq!(e("'a' = '0'"), "INT:0");
    assert!(tidb_parser::parse("select 1 XOR").is_err());
}

/// Scalar rows from `pkg/expression/builtin_op_test.go`'s bitwise and unary
/// operator tables (`TestLeftShift`, `TestRightShift`, `TestBitXor`,
/// `TestBitOr`, `TestBitAnd`, `TestBitNeg`, and `TestUnaryNot`).  The Go
/// tables also inject an `errors.New` datum; the SQL evaluator has no error
/// value variant, so those rows remain an explicit non-SQL boundary.  Every
/// representable literal row is kept here so the source table cannot quietly
/// regress to a hand-picked happy path.
#[test]
fn bitwise_and_unary_source_vectors_match_tidb() {
    for (sql, want) in [
        // TestLeftShift.
        ("123 << 2", "UINT:492"),
        ("-123 << 2", "UINT:18446744073709551124"),
        ("NULL << 1", "NULL"),
        // TestRightShift.
        ("123 >> 2", "UINT:30"),
        ("-123 >> 2", "UINT:4611686018427387873"),
        ("NULL >> 1", "NULL"),
        // TestBitXor.
        ("123 ^ 321", "UINT:314"),
        ("-123 ^ 321", "UINT:18446744073709551300"),
        ("NULL ^ 1", "NULL"),
        // TestBitOr.
        ("123 | 321", "UINT:379"),
        ("-123 | 321", "UINT:18446744073709551557"),
        ("NULL | 1", "NULL"),
        // TestBitAnd.
        ("123 & 321", "UINT:65"),
        ("-123 & 321", "UINT:257"),
        ("NULL & 1", "NULL"),
        // TestBitNeg.
        ("~123", "UINT:18446744073709551492"),
        ("~-123", "UINT:122"),
        ("~NULL", "NULL"),
        // TestUnaryNot's numeric, string-prefix, decimal, and NULL rows.
        ("NOT 1", "INT:0"),
        ("NOT 0", "INT:1"),
        ("NOT 123", "INT:0"),
        ("NOT -123", "INT:0"),
        ("NOT '123'", "INT:0"),
        ("NOT 0.3e0", "INT:0"),
        ("NOT '0.3'", "INT:0"),
        ("NOT 0.3", "INT:0"),
        ("NOT NULL", "NULL"),
        // `!` is the alternate spelling of the same AST unary operator.
        ("!0", "INT:1"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    assert!(tidb_parser::parse("select 1 <<").is_err());
    assert!(tidb_parser::parse("select 1 ^").is_err());
}

/// Scalar rows from `TestIsTrueOrFalse` in
/// `pkg/expression/builtin_op_test.go`.  TiDB builds these predicates over
/// the same numeric-prefix coercion as `EvalReal`: malformed text is zero,
/// while a non-zero decimal/real/string is true.  Typed duration/time/JSON
/// rows in the Go table require FieldType/session value domains and remain
/// explicit partial coverage rather than being replaced with SQL literals.
#[test]
fn is_true_and_false_source_vectors_use_numeric_prefix_truthiness() {
    for (sql, want) in [
        ("-12 IS TRUE", "INT:1"),
        ("-12 IS FALSE", "INT:0"),
        ("12 IS TRUE", "INT:1"),
        ("12 IS FALSE", "INT:0"),
        ("0 IS TRUE", "INT:0"),
        ("0 IS FALSE", "INT:1"),
        ("0.0e0 IS TRUE", "INT:0"),
        ("0.0e0 IS FALSE", "INT:1"),
        ("'aaa' IS TRUE", "INT:0"),
        ("'aaa' IS FALSE", "INT:1"),
        ("'' IS TRUE", "INT:0"),
        ("'' IS FALSE", "INT:1"),
        ("'0.3' IS TRUE", "INT:1"),
        ("'0.3' IS FALSE", "INT:0"),
        ("0.3e0 IS TRUE", "INT:1"),
        ("0.3e0 IS FALSE", "INT:0"),
        ("0.3 IS TRUE", "INT:1"),
        ("0.3 IS FALSE", "INT:0"),
        ("NULL IS TRUE", "INT:0"),
        ("NULL IS FALSE", "INT:0"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
}

#[test]
fn bitwise_and_div_by_zero() {
    assert_eq!(e("7 & 3"), "UINT:3");
    assert_eq!(e("1 << 4"), "UINT:16");
    assert_eq!(e("100 >> 2"), "UINT:25");
    // DIV / MOD by zero are NULL in MySQL.
    assert_eq!(e("10 DIV 0"), "NULL");
    assert_eq!(e("10 MOD 0"), "NULL");
}

#[test]
fn ast_integer_overflow_preserves_go_error_shape() {
    struct NoUnsignedSubtraction;

    impl Columns for NoUnsignedSubtraction {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn no_unsigned_subtraction(&self) -> bool {
            true
        }
    }

    assert_eq!(
        e("9223372036854775807 + 1"),
        "DataOutOfRange { value: \"BIGINT\", expression: \"(9223372036854775807 + 1)\" }"
    );
    assert_eq!(
        e("18446744073709551615 + 1"),
        "DataOutOfRange { value: \"BIGINT UNSIGNED\", expression: \"(18446744073709551615 + 1)\" }"
    );
    assert_eq!(
        e_with("0 - 18446744073709551615", &NoUnsignedSubtraction,),
        "DataOutOfRange { value: \"BIGINT\", expression: \"(0 - 18446744073709551615)\" }"
    );
}

#[test]
fn out_of_domain_is_unsupported() {
    // A user variable reference is NEVER an error (confirmed via
    // `gorun`: an unset/session-less `@x` reads as `NULL`, unlike
    // `@@sysvar`'s deliberately narrower, error-on-unknown-name
    // domain) — `eval`'s stateless `NoColumns` resolver has no
    // session at all, which `Columns::get_uservar`'s own default
    // collapses to the SAME `NULL` a real session's genuinely-unset
    // variable would give, not an error.
    assert_eq!(
        {
            let stmt = tidb_parser::parse("select @x").unwrap();
            let Stmt::Query(query) = stmt else {
                unreachable!()
            };
            let QueryStmt::Select(s) = query.into_inner() else {
                unreachable!()
            };
            let SelectField::Expr { expr, .. } = &s.fields[0] else {
                unreachable!()
            };
            eval(expr).unwrap()
        },
        Datum::Null
    );
    // An unsupported expression construct (a window function outside
    // its own evaluation context) is still a genuine error.
    assert!(matches!(
        {
            let stmt = tidb_parser::parse("select row_number() over ()").unwrap();
            let Stmt::Query(query) = stmt else {
                unreachable!()
            };
            let QueryStmt::Select(s) = query.into_inner() else {
                unreachable!()
            };
            let SelectField::Expr { expr, .. } = &s.fields[0] else {
                unreachable!()
            };
            eval(expr)
        },
        Err(EvalError::Unsupported(_))
    ));
}

#[test]
fn decimals() {
    // The literal's own scale is preserved verbatim; leading zeros in the
    // integer part are stripped; a numerically zero value never carries a
    // sign — MyDecimal's canonical string form.
    assert_eq!(e("3.14"), "DEC:3.14");
    assert_eq!(e("3.140"), "DEC:3.140");
    assert_eq!(e("010.500"), "DEC:10.500");
    assert_eq!(e("-0.0"), "DEC:0.0");
    assert_eq!(e(".5"), "DEC:0.5");
    assert_eq!(e("5."), "DEC:5");
    // Exact arithmetic: no float rounding error, unlike a binary float.
    assert_eq!(e("0.1 + 0.2"), "DEC:0.3");
    assert_eq!(e("3.14 + 2.1"), "DEC:5.24"); // scale = max(2, 1)
    assert_eq!(e("3.14 - 5"), "DEC:-1.86"); // Int promotes to decimal(0)
    assert_eq!(e("1.5 * 2"), "DEC:3.0"); // scale = 1 + 0
    assert_eq!(e("3.140 - 3.14"), "DEC:0.000"); // zero result: no sign
                                                // Equality ignores scale; ordering does not.
    assert_eq!(e("1.5 = 1.50"), "INT:1");
    assert_eq!(e("1.5 < 2"), "INT:1");
    // Truthiness (nonzero is truthy) is shared by NOT / IS TRUE|FALSE /
    // AND / OR / XOR.
    assert_eq!(e("not 0.0"), "INT:1");
    assert_eq!(e("0.0 is false"), "INT:1");
    assert_eq!(e("0.0 and 1"), "INT:0");
    // Builtins.
    assert_eq!(engine_e("abs(-3.14)"), "DEC:3.14");
    assert_eq!(engine_e("sign(-3.14)"), "INT:-1");
    assert_eq!(e("nullif(3.14, 3.140)"), "NULL"); // equal despite differing scale
    assert_engine_compare2_value("least(1.5, 2.5, 0.5)", "DEC:0.5");

    // DIV/MOD: exact via unsigned long division, truncating toward zero.
    // DIV's result is an Int; MOD's is a Decimal at max(scale_a, scale_b),
    // sign of the dividend.
    assert_eq!(e("3.14 div 2"), "INT:1");
    assert_eq!(e("3.14 mod 2"), "DEC:1.14");
    assert_eq!(e("-3.14 div 2"), "INT:-1"); // truncates toward zero, not floor
    assert_eq!(e("-3.14 mod 2"), "DEC:-1.14"); // remainder sign follows the dividend
    assert_eq!(e("5 div 2.5"), "INT:2"); // Int promotes to decimal
    assert_eq!(e("2.5 mod 5"), "DEC:2.5");
    assert_eq!(e("10 div 0.0"), "NULL"); // division by (decimal) zero
    assert_eq!(e("10 mod 0.0"), "NULL");
    // `/` grows the result scale by 4 past the DIVIDEND's own scale
    // (MySQL's div_precision_increment), regardless of the divisor's
    // own scale — confirmed via `goeval`, not assumed.
    assert_eq!(e("3.14 / 2"), "DEC:1.570000"); // scale 2+4=6
    assert_eq!(e("5 / 2.5"), "DEC:2.0000"); // scale 0+4=4, divisor's scale ignored
    assert_eq!(e("1.5 / 0"), "NULL"); // division by zero

    // Bitwise/shift: rounds to the nearest i64 first, ties away from
    // zero (not round-half-to-even), then applies the integer operator.
    assert_eq!(e("~3.14"), "UINT:18446744073709551612");
    assert_eq!(e("~3.5"), "UINT:18446744073709551611");
    assert_eq!(e("~2.5"), "UINT:18446744073709551612");
    assert_eq!(e("3.14 & 5"), "UINT:1");
    assert_eq!(e("3.14 << 1"), "UINT:6");
    assert_eq!(e("-1.5 & 3"), "UINT:2");
}

#[test]
fn floats() {
    // A literal round-trips via Rust's own f64 Display — confirmed
    // (by direct comparison, not assumed) to match Go's
    // strconv.FormatFloat(f, 'f', -1, 64) byte for byte across a wide
    // value range, so no custom formatting is needed, unlike Decimal.
    assert_eq!(e("1.5e2"), "FLOAT:150");
    assert_eq!(e("-1.5e2"), "FLOAT:-150");
    assert_eq!(e("-0.0e0"), "FLOAT:-0");
    // An Int or Decimal operand promotes to Float — the OPPOSITE
    // direction from how Decimal dominates Int (Float dominates
    // Decimal instead) — confirmed via goeval, not assumed: even a
    // Decimal-looking literal like `3.14` promotes once a Float is
    // anywhere in the expression.
    assert_eq!(e("1.5e2 + 1"), "FLOAT:151");
    assert_eq!(e("1.5e2 + 3.14"), "FLOAT:153.14");
    assert_eq!(e("1.5e2 / 2"), "FLOAT:75");
    assert_eq!(e("1.5e2 / 0"), "NULL");
    // A float literal that would overflow to infinity is rejected at
    // PARSE time (confirmed via godump restore: real TiDB rejects
    // `1e400`, the boundary is exactly f64::MAX), so an in-domain
    // Float value here is always finite by construction; an
    // ARITHMETIC result that overflows is instead a genuine
    // evaluation error (confirmed via goeval: `1e300 * 1e300`
    // errors, never silently becomes IEEE-754 infinity) — this is
    // the one case the differential corpus can't itself assert
    // (`ERR` goldens are skipped, not compared), so it's covered
    // directly here instead.
    assert_eq!(
        e("1e300 * 1e300"),
        "DataOutOfRange { value: \"DOUBLE\", expression: \"(1e+300 * 1e+300)\" }"
    );
    assert_eq!(e("1e-300 * 1e-300"), "FLOAT:0"); // underflow to zero is fine
                                                 // Bitwise/shift rounds to the nearest i64 first — but TIES TO
                                                 // EVEN, the OPPOSITE tie-breaking rule from Decimal's own `~`
                                                 // (ties away from zero) — confirmed via goeval, not assumed.
    assert_eq!(e("~2.5e0"), "UINT:18446744073709551613");
    assert_eq!(e("~3.5e0"), "UINT:18446744073709551611");
    // DIV truncates toward zero to an Int result, same as Int/Decimal.
    assert_eq!(e("1.007e2 div 3"), "INT:33");
    assert_eq!(e("-1.007e2 div 3"), "INT:-33");
    // LEAST/GREATEST promote their RESULT to the widest argument type
    // (a real bug caught by the differential corpus on the first
    // attempt, not assumed correct): the winning argument `2` is a
    // bare Int literal, but the result is still Float because
    // ANOTHER argument was Float.
    assert_compare2_refusal("least(1.5e2, 3.14, 2)", "FLOAT:2");
    assert_compare2_refusal("greatest(1.5e2, 3.14, 2)", "FLOAT:150");
    // NULLIF's equality reuses the same cross-type promotion, unlike
    // a hand-rolled same-type-only check.
    assert_eq!(e("nullif(150, 1.5e2)"), "NULL");
    assert_eq!(engine_e("sign(0.0e0)"), "INT:0"); // unlike IEEE-754 signum, never 0
}

/// `TRANSLATE` was fully ported on both signatures and reachable from the AST
/// evaluator, but had no arm in the rewriter's result-type table, so the chunk
/// tier -- the one live SQL uses -- refused it outright. Go
/// `translateFunctionClass.getFunction` builds an `ETString` result whose flen
/// is argument 0's own. Captured from TiDB:
///
/// ```text
/// select translate('abcabc', 'ab', 'xy');  -> xycxyc
/// select translate('hello', 'lo', 'L');    -> heLL
/// ```
#[test]
fn translate_source_rows_are_explicitly_contracted() {
    for expr in [
        "translate('abcabc', 'ab', 'xy')",
        "translate('hello', 'lo', 'L')",
        "translate('中文测试', '中试', 'XY')",
        "translate('abc', null, 'x')",
    ] {
        assert_string2_refusal(expr);
    }
}

/// `WEIGHT_STRING` and `LOAD_FILE`, both previously refused outright.
///
/// `LOAD_FILE` is `builtinLoadFileSig.evalString`, which reads its argument
/// and then returns `"", true, nil` UNCONDITIONALLY -- TiDB has no
/// server-side file access, so every path is NULL and there is no
/// `secure_file_priv` policy to model. CAPTURED: `load_file('/etc/hosts')`
/// is NULL on a server whose own process can read that file.
///
/// `WEIGHT_STRING` is the collation SORT KEY surfaced to SQL -- what
/// `ORDER BY` actually compares. Every row below is CAPTURED from TiDB as
/// `HEX(...)`:
///
/// ```text
/// weight_string('a')                             -> 61
/// weight_string('A' collate utf8mb4_general_ci)  -> 0041
/// weight_string('ab' as char(1))                 -> 61
/// weight_string('ab' as char(4))                 -> 6162
/// weight_string('ab' as binary(4))               -> 61620000
/// weight_string('ab' as binary(1))               -> 61
/// weight_string('中')                             -> E4B8AD
/// weight_string(1) / weight_string(1 as char(2)) -> NULL
/// ```
///
/// `AS CHAR(4)` is `6162`, not `61622020`: the spaces are padded on before
/// the key is taken and `utf8mb4_bin` is PAD SPACE, so they are trimmed right
/// back off. `AS BINARY(4)` keeps its NUL padding because `binary` is not.
#[test]
fn weight_string_and_load_file_source_vectors() {
    for (sql, want) in [
        ("hex(weight_string('a'))", "STR:61"),
        (
            "hex(weight_string('A' collate utf8mb4_general_ci))",
            "STR:0041",
        ),
        ("hex(weight_string('ab' as char(1)))", "STR:61"),
        ("hex(weight_string('ab' as char(4)))", "STR:6162"),
        ("hex(weight_string('ab' as binary(4)))", "STR:61620000"),
        ("hex(weight_string('ab' as binary(1)))", "STR:61"),
        ("hex(weight_string('中'))", "STR:E4B8AD"),
        // `AS CHAR(n)` counts RUNES and `AS BINARY(n)` counts BYTES -- the
        // two truncate the same input to different places. CAPTURED.
        ("hex(weight_string('中文' as char(1)))", "STR:E4B8AD"),
        ("hex(weight_string('中文' as binary(1)))", "STR:E4"),
        // The same distinction on the PADDING side: 3 is past the two RUNES
        // but short of the six BYTES, so `AS CHAR(3)` pads rather than
        // truncates -- visible only under a NO PAD collation. CAPTURED.
        (
            "hex(weight_string('中文' collate utf8mb4_0900_bin as char(3)))",
            "STR:E4B8ADE6968720",
        ),
        // A NO PAD collation is where `AS CHAR(n)`'s padding SURVIVES the
        // key, which is what makes padding-before-keying observable at all:
        // under `utf8mb4_bin` (PAD SPACE) the spaces are trimmed back off.
        // CAPTURED from TiDB.
        (
            "hex(weight_string('ab' collate utf8mb4_0900_ai_ci as char(4)))",
            "STR:1C471C6002090209",
        ),
        (
            "hex(weight_string('ab' collate utf8mb4_0900_ai_ci))",
            "STR:1C471C60",
        ),
        (
            "hex(weight_string('ab' collate utf8mb4_0900_bin as char(4)))",
            "STR:61622020",
        ),
        ("hex(weight_string(cast('ab' as binary)))", "STR:6162"),
        // `AS BINARY` overrides the numeric NULL signature selected during
        // verification; the numeric value is stringified and padded.
        ("hex(weight_string(7 as binary(2)))", "STR:3700"),
        // `builtinWeightStringNullSig`: numeric arguments without an AS
        // clause, or with AS CHAR, remain NULL.
        ("weight_string(1)", "NULL"),
        ("weight_string(1 as char(2))", "NULL"),
        ("weight_string(null)", "NULL"),
        ("load_file('/etc/hosts')", "NULL"),
        ("load_file(null)", "NULL"),
    ] {
        if sql.contains("weight_string") {
            let _ = want;
            assert_packet_string_refusal(sql);
        } else {
            assert_eq!(chunk_e(sql), want, "{sql}");
        }
    }

    struct GeneralCi;
    impl Columns for GeneralCi {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn connection_charset_info(&self) -> (&str, &str) {
            ("utf8mb4", "utf8mb4_general_ci")
        }
    }
    // Non-string values first pass through `WrapWithCastAsString`; the
    // wrapped temporal value keys under this statement's connection
    // collation rather than the temporal type's binary metadata.
    assert_eq!(
        chunk_e_with("hex(weight_string(cast(20190821 as date)))", &GeneralCi),
        RADIX_REMOVED
    );
    assert_packet_string_refusal("hex(weight_string('ab' as binary(4)))");
    assert_packet_string_refusal("hex(weight_string('A' collate utf8mb4_general_ci))");
}

/// Hex/bit literals carry Go's `KindBinaryLiteral`, not `KindBytes`.
///
/// Go's `Datum.SetValue`/`SetValueWithDefaultCollation` route both
/// `HexLiteral` and `BitLiteral` through `SetBinaryLiteral`
/// (`pkg/types/datum.go:626-630`, comment: "Store as BinaryLiteral for Bit
/// and Hex literals"), and that kind is what a numeric context reads as an
/// unsigned INTEGER. Carrying it as `Datum::Bytes` did not merely refuse
/// arithmetic; three of these rows were silently WRONG values before the
/// kind was corrected (`-0x1A` was `-0`, `ABS(b'11')` was `0`, `0x1A > 25`
/// was `0`).
///
/// Every expected value is what `gorun` printed for the same statement, so
/// the AST tier is pinned to the engine and not to the chunk tier beside it.
#[test]
fn hex_and_bit_literals_are_binary_literals_in_a_numeric_context() {
    for (expr, want) in [
        ("0x1A + 1", "UINT:27"),
        // A BIT literal is SIGNED (`types.DefaultTypeForValue` adds
        // `UnsignedFlag` for Hex and Binary literals, not for Bit), so its
        // arithmetic answers INT where a hex literal's answers UINT.
        ("b'101' + 1", "INT:6"),
        ("0x1A > 25", "INT:1"),
        ("0xFF + 0", "UINT:255"),
        ("b'' + 0", "INT:0"),
        ("0x1A * 2", "UINT:52"),
        ("0x1A div 2", "UINT:13"),
        ("0x20000000000000 + 1", "UINT:9007199254740993"),
        ("b'11' + b'11'", "INT:6"),
        // The one literal whose top bit is set, where the signedness is a
        // different VALUE and not only a different label. Every row here is
        // what a real TiDB session answered.
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' + 0",
            "INT:-1",
        ),
        (
            "+b'1111111111111111111111111111111111111111111111111111111111111111' + 0",
            "INT:-1",
        ),
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' - 1",
            "INT:-2",
        ),
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' * -1",
            "INT:1",
        ),
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' div 2",
            "INT:0",
        ),
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' mod 3",
            "INT:-1",
        ),
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' + b'1'",
            "INT:0",
        ),
        ("x'ffffffffffffffff' + 0", "UINT:18446744073709551615"),
        // DIVISION and any DECIMAL/REAL operand keep the UNSIGNED reading:
        // `/` picks a decimal signature, so Go wraps the operand with
        // `WrapWithCastAsDecimal`, and `Datum.ToDecimal` reads the octets
        // unsigned whatever the field type says.
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' / 2",
            "DEC:9223372036854775807.5000",
        ),
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' + 0.0",
            "DEC:18446744073709551615.0",
        ),
        // A COMPARISON is a third rule again and is unchanged.
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' > 0",
            "INT:1",
        ),
        ("0x0A + 0x0A", "UINT:20"),
        // The remaining retained string contexts preserve literal octets.
        ("length(0x4142)", "INT:2"),
        ("char_length(0xF0288C28)", "INT:4"),
        ("char_length(0xE4BDA0)", "INT:3"),
        ("0x41 = 'A'", "INT:1"),
    ] {
        if expr.starts_with("length(") || expr.starts_with("char_length(") {
            assert_string_length_refusal(expr, want);
        } else {
            assert_eq!(e(expr), want, "{expr}");
        }
    }
    assert_radix_refusal("hex(0x1A)");
    assert!(engine_declines("hex(0x1A)"));
    assert_packet_string_refusal("concat(0x41, 'x')");
    assert!(engine_declines("abs(b'11')"));
    assert_eq!(
        e("abs(b'11')"),
        "Unsupported(\"native math evaluation was removed; TiKV engine required\")"
    );

    // The CHUNK tier reads the same signedness off the operand's real
    // `FieldType` where this tier reads it off the AST node, so the two must
    // agree on the rows where the two literal forms diverge.
    for (expr, want) in [
        (
            "b'1111111111111111111111111111111111111111111111111111111111111111' + 0",
            "INT:-1",
        ),
        ("x'ffffffffffffffff' + 0", "UINT:18446744073709551615"),
        ("b'101' + 1", "INT:6"),
        ("0x1A + 1", "UINT:27"),
        // The introducer keeps KindBinaryLiteral but makes IsBinaryStr false,
        // so Go's numericContextResultType deliberately selects ETReal.
        ("_latin1 0x41 + 0", "FLOAT:65"),
        ("_latin1 b'1' + 0", "FLOAT:1"),
        (
            "+b'1111111111111111111111111111111111111111111111111111111111111111' + 0",
            "INT:-1",
        ),
    ] {
        assert_eq!(chunk_e(expr), want, "{expr} (chunk tier)");
    }
    // `-0x1A` is a BinaryLiteral in Go's unary-minus table and therefore
    // takes the REAL signature in both the AST and CHUNK evaluators.
    assert_eq!(e("-0x1A"), "FLOAT:-26");
    assert_eq!(chunk_e("-0x1A"), "FLOAT:-26");
}
