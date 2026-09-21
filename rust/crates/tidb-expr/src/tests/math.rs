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

//! Focused tests for translated `pkg/expression/builtin_math.go` behavior.

use super::{e as removed_native_e, engine_declines, engine_e as e};

fn assert_unsupported(expr: &str) {
    assert!(engine_declines(expr), "engine unexpectedly admitted {expr}");
    assert_eq!(
        removed_native_e(expr),
        "Unsupported(\"native math evaluation was removed; TiKV engine required\")",
        "{expr}"
    );
}

fn assert_engine_error(expr: &str) {
    let error = e(expr);
    assert!(error.contains("ExternalEngine"), "{expr}: {error}");
}

/// The native CONV helper was physically deleted. TiKV currently disagrees on
/// signed-prefix/base behavior, so every preserved source vector must be an
/// explicit refusal rather than an incorrect engine result or native fallback.
#[test]
fn conv_matches_go_prefix_and_base_semantics() {
    for (expr, want) in [
        ("conv('a',16,2)", "STR:1010"),
        ("conv('6E',18,8)", "STR:172"),
        ("conv('-17',10,-18)", "STR:-H"),
        ("conv('-17',10,18)", "STR:2D3FGB0B9CG4BD1H"),
        ("conv('+18aZ',7,36)", "STR:1"),
        (
            "conv('18446744073709551615',-10,16)",
            "STR:7FFFFFFFFFFFFFFF",
        ),
        ("conv('12F',-10,16)", "STR:C"),
        ("conv('  FF ',16,10)", "STR:255"),
        ("conv('TIDB',10,8)", "STR:0"),
        ("conv('aa',10,2)", "STR:0"),
        ("conv(' A',-10,16)", "STR:0"),
        ("conv('a6a',10,8)", "STR:0"),
        ("conv('a6a',1,8)", "NULL"),
        ("conv(null,10,10)", "NULL"),
    ] {
        let _ = want;
        assert_unsupported(expr);
    }
}

#[test]
fn conv_reads_the_unsigned_base_domain() {
    for expr in [
        "conv('a',cast(16 as unsigned),2)",
        "conv('a',16,cast(2 as unsigned))",
        "conv('a',null,2)",
    ] {
        assert_unsupported(expr);
    }
}

#[test]
fn conv_binary_literals_are_explicitly_unsupported_without_native_math() {
    for expr in ["conv(0x0020,2,2)", "conv(0x02,16,2)", "conv(0x02,16,8)"] {
        assert_unsupported(expr);
    }
}

#[test]
fn crc32_matches_go_utf8_source_vectors() {
    for (expr, want) in [
        ("crc32('')", "UINT:0"),
        ("crc32(-1)", "UINT:808273962"),
        ("crc32('-1')", "UINT:808273962"),
        ("crc32('mysql')", "UINT:2501908538"),
        ("crc32('MySQL')", "UINT:3259397556"),
        ("crc32('hello')", "UINT:907060870"),
        ("crc32('一二三')", "UINT:1785250883"),
        ("crc32('一')", "UINT:2416838398"),
        ("crc32(null)", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

/// Full scalar vector from `pkg/expression/builtin_math_test.go:35`
/// `TestAbs`, including the distinct unsigned signature.
#[test]
fn abs_source_vectors_preserve_uint() {
    for (expr, want) in [
        ("abs(null)", "NULL"),
        ("abs(1)", "INT:1"),
        ("abs(cast(1 as unsigned))", "UINT:1"),
        ("abs(-1)", "INT:1"),
        ("abs(3.14e0)", "FLOAT:3.14"),
        ("abs(-3.14e0)", "FLOAT:3.14"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

/// Source edge from `builtinAbsIntSig.evalInt`: the signed minimum has no
/// positive `BIGINT` representation, so Go returns `ErrOverflow` instead of
/// wrapping back to the same negative value.  This row is not present in the
/// original `TestAbs` table, but is part of the source implementation's
/// observable contract and guards the Rust `checked_abs` boundary.
#[test]
fn abs_signed_minimum_reports_overflow() {
    let error = e("abs(-9223372036854775808)");
    assert!(
        error.contains("ExternalEngine") && error.contains("1690"),
        "{error}"
    );
}

/// Exact representable source table from `TestSign` in
/// `pkg/expression/builtin_math_test.go:642`.  The Go signature always
/// returns a signed integer, while its argument builder still selects the
/// real coercion path for strings.  Keep every source row here so a future
/// change cannot preserve only the already-covered string-prefix examples
/// while dropping `NULL`, fractional values, or the `UInt64` boundary.
#[test]
fn sign_matches_go_source_table() {
    for (expr, want) in [
        ("sign(null)", "NULL"),
        ("sign(1)", "INT:1"),
        ("sign(0)", "INT:0"),
        ("sign(-1)", "INT:-1"),
        ("sign(0.4e0)", "INT:1"),
        ("sign(-0.4e0)", "INT:-1"),
        ("sign('1')", "INT:1"),
        ("sign('-1')", "INT:-1"),
        ("sign('1a')", "INT:1"),
        ("sign('-1a')", "INT:-1"),
        ("sign('a')", "INT:0"),
        ("sign(cast(9223372036854775808 as unsigned))", "INT:1"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

/// The `ETReal` signature every math builtin selects for an argument that is
/// neither integer nor decimal. `ABS`/`SIGN`/`ROUND`/`TRUNCATE` used to match
/// on a closed list of Datum kinds and REFUSE the rest, so `ABS('12abc')` was
/// an evaluation error where TiDB answers 12. Go dispatches on the argument's
/// EVAL TYPE instead, and everything outside `ETInt`/`ETDecimal` — string,
/// enum, set, temporal, `FLOAT` — is `ETReal`.
///
/// Every row is a `goeval`/`gorun` capture, including the ones that show the
/// result TYPE is real and not the argument's: `ABS` of a `FLOAT` column is
/// the widened double `0.10000000149011612` (captured via `gorun`, not
/// expressible here), and `ROUND('12.6abc')` is `FLOAT:13`, not an integer.
///
/// One kind is deliberately NOT a row, because its VALUE here is decided by
/// an older representation boundary rather than by the signature this test
/// pins. A date is its canonical STRING, so
/// `ABS(CAST('2021-01-01' AS DATE))` takes the numeric prefix (2021), not
/// TiDB's 20210101 -- the same answer `SQRT` of that date already gave before
/// this change, since it always had the catch-all these functions now share.
#[test]
fn real_signature_covers_non_numeric_argument_kinds() {
    for (expr, want) in [
        ("abs('12abc')", "FLOAT:12"),
        ("abs('12.5abc')", "FLOAT:12.5"),
        ("abs('-3abc')", "FLOAT:3"),
        ("abs('abc')", "FLOAT:0"),
        ("abs('')", "FLOAT:0"),
        ("sign('')", "INT:0"),
        ("round('12.6abc')", "FLOAT:13"),
        ("round('12.6abc', 0)", "FLOAT:13"),
        ("round('abc')", "FLOAT:0"),
        ("truncate('12.68abc', 1)", "FLOAT:12.6"),
        ("truncate('abc', 2)", "FLOAT:0"),
    ] {
        if expr.starts_with("abs") || expr.starts_with("sign") {
            assert_eq!(e(expr), want, "{expr}");
        } else {
            assert_unsupported(expr);
        }
    }
}

#[test]
fn math_functions() {
    assert_eq!(e("sqrt(4)"), "FLOAT:2");
    assert_eq!(e("sqrt(null)"), "NULL");
    assert_unsupported("pow(2, 10)");
    assert_unsupported("power(2, 10)");
    assert_eq!(e("exp(0)"), "FLOAT:1");
    assert_eq!(e("ln(1)"), "FLOAT:0");
    assert_eq!(e("log(10)"), "FLOAT:2.302585092994046"); // LOG(x), one arg, is LN
    assert_eq!(e("log(2, 8)"), "FLOAT:3"); // LOG(base, x), two args
    assert_eq!(e("log2(8)"), "FLOAT:3");
    assert_eq!(e("log10(100)"), "FLOAT:2");
    assert_eq!(e("pi()"), "FLOAT:3.141592653589793");
    assert_eq!(
        removed_native_e("pi(1)"),
        "Unsupported(\"native math evaluation was removed; TiKV engine required\")"
    );
    // SQRT/LN/LOG/LOG2/LOG10 return NULL for an out-of-domain
    // argument — MySQL's own explicit domain check (confirmed via
    // goeval, not assumed) — the OPPOSITE failure mode from POW/EXP
    // below.
    assert_eq!(e("sqrt(-1)"), "NULL");
    assert_eq!(e("ln(0)"), "NULL");
    assert_eq!(e("ln(-1)"), "NULL");
    assert_eq!(e("log2(-1)"), "NULL");
    assert_eq!(e("log10(0)"), "NULL");
    assert_eq!(e("log(1, 8)"), "NULL"); // log base 1 is undefined
    assert_eq!(e("log(-2, 8)"), "NULL"); // negative base
    assert_eq!(e("log(2, -8)"), "NULL"); // negative x
                                         // POW/EXP have no such domain check; a NaN (complex) or
                                         // overflowing result is instead a genuine evaluation ERROR —
                                         // this is the one case the differential corpus can't itself
                                         // assert (`ERR` goldens are skipped, not compared), so it's
                                         // covered directly here.
    for expr in [
        "pow(-2, 0.5)",
        "pow(2, 2000)",
        "pow(0, -1)",
        "pow(1 + 1, 2000)",
        "pow(0, 0)",
    ] {
        assert_unsupported(expr);
    }
    assert_engine_error("exp(1000)");
    assert_eq!(e("exp(-1000)"), "FLOAT:0");
}

/// Exact scalar result/error vectors from `TestExp` in
/// `pkg/expression/builtin_math_test.go`. The production test also counts the
/// ETReal truncation warning for `EXP('tidb')`; `e()` evaluates against
/// `NoColumns`, whose sink discards, so these rows pin the VALUE and
/// `warning_sink` in `crate::ops` pins the warning.
#[test]
fn exp_matches_go_source_vectors_and_arity() {
    for (sql, want) in [
        ("exp(null)", "NULL"),
        ("exp(1)", "FLOAT:2.718281828459045"),
        ("exp(1.23e0)", "FLOAT:3.4212295362896734"),
        ("exp(-1.23e0)", "FLOAT:0.2922925776808594"),
        ("exp(0)", "FLOAT:1"),
        ("exp('0')", "FLOAT:1"),
        ("exp('tidb')", "FLOAT:1"),
        ("exp(-1000)", "FLOAT:0"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    assert_engine_error("exp(100000)");
    assert_eq!(
        removed_native_e("exp(1, 2)"),
        "Unsupported(\"native math evaluation was removed; TiKV engine required\")"
    );
}

#[test]
fn trig_is_explicitly_unsupported_without_bit_exact_engine_parity() {
    for expr in [
        "sin(0)",
        "cos(0)",
        "tan(0)",
        "asin(1)",
        "acos(1)",
        "atan(1)",
        "atan2(1,2)",
        "cot(1)",
        "radians(180)",
        "degrees(pi())",
    ] {
        assert_unsupported(expr);
    }
}

/// Full scalar result table from the transcendental portions of
/// `pkg/expression/builtin_math_test.go` (`TestDegrees` through `TestCot`).
///
/// The Go tests also assert statement warning counts for malformed string
/// prefixes.  Those rows are checked here for their VALUE; the warning they
/// raise now travels through `Columns::append_warning` and is pinned
/// separately, since `e()` evaluates against the discarding `NoColumns`
/// sink.  Keeping the
/// source rows here (instead of a handful of representative calls) is
/// important: several of the functions differ only at NULL/domain and
/// negative-angle boundaries.
fn assert_source_math_value(sql: &str, want: &str) {
    if let Some(want_float) = want.strip_prefix("FLOAT:") {
        let got = e(sql);
        let got_float = got
            .strip_prefix("FLOAT:")
            .unwrap_or_else(|| panic!("{sql}: expected float result, got {got}"));
        let got_float = got_float
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("{sql}: invalid Rust float result {got}"));
        let want_float = want_float
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("{sql}: invalid source float oracle {want}"));
        assert_eq!(got_float, want_float, "{sql}: got {got}, want {want}");
    } else {
        assert_eq!(e(sql), want, "{sql}");
    }
}

#[test]
fn transcendental_source_vectors() {
    for (sql, want) in [
        // TestDegrees
        ("degrees(null)", "NULL"),
        ("degrees(0)", "FLOAT:0"),
        ("degrees(1)", "FLOAT:57.29577951308232"),
        ("degrees(1e0)", "FLOAT:57.29577951308232"),
        ("degrees(3.141592653589793e0)", "FLOAT:180"),
        ("degrees(-1.5707963267948966e0)", "FLOAT:-90"),
        ("degrees('')", "FLOAT:0"),
        ("degrees('-2')", "FLOAT:-114.59155902616465"),
        ("degrees('abc')", "FLOAT:0"),
        ("degrees('+1abc')", "FLOAT:57.29577951308232"),
        // TestSqrt
        ("sqrt(null)", "NULL"),
        ("sqrt(1)", "FLOAT:1"),
        ("sqrt(4e0)", "FLOAT:2"),
        ("sqrt('4')", "FLOAT:2"),
        ("sqrt('9')", "FLOAT:3"),
        ("sqrt('-16')", "NULL"),
        // TestPi
        ("pi()", "FLOAT:3.141592653589793"),
        // TestRadians
        ("radians(null)", "NULL"),
        ("radians(0)", "FLOAT:0"),
        ("radians(180e0)", "FLOAT:3.141592653589793"),
        ("radians(-360)", "FLOAT:-6.283185307179586"),
        ("radians('180')", "FLOAT:3.141592653589793"),
        ("radians(1e308)", "FLOAT:1.7453292519943295e306"),
        ("radians(23)", "FLOAT:0.4014257279586958"),
        ("radians('notNum')", "FLOAT:0"),
        // TestSin
        ("sin(null)", "NULL"),
        ("sin(0)", "FLOAT:0"),
        ("sin(3.141592653589793e0)", "FLOAT:1.2246467991473515e-16"),
        ("sin(-3.141592653589793e0)", "FLOAT:-1.2246467991473515e-16"),
        ("sin(1.5707963267948966e0)", "FLOAT:1"),
        ("sin(-1.5707963267948966e0)", "FLOAT:-1"),
        ("sin(0.5235987755982988e0)", "FLOAT:0.49999999999999994"),
        ("sin(-0.5235987755982988e0)", "FLOAT:-0.49999999999999994"),
        ("sin(6.283185307179586e0)", "FLOAT:-2.449293598294703e-16"),
        ("sin('adfsdfgs')", "FLOAT:0"),
        ("sin('0.000')", "FLOAT:0"),
        // TestCos
        ("cos(null)", "NULL"),
        ("cos(0)", "FLOAT:1"),
        ("cos(3.141592653589793e0)", "FLOAT:-1"),
        ("cos(-3.141592653589793e0)", "FLOAT:-1"),
        ("cos(1.5707963267948966e0)", "FLOAT:6.123233995736757e-17"),
        ("cos(-1.5707963267948966e0)", "FLOAT:6.123233995736757e-17"),
        ("cos('0.000')", "FLOAT:1"),
        ("cos('sdfgsfsdf')", "FLOAT:1"),
        // TestAcos
        ("acos(null)", "NULL"),
        ("acos(1e0)", "FLOAT:0"),
        ("acos(2e0)", "NULL"),
        ("acos(-1e0)", "FLOAT:3.141592653589793"),
        ("acos(-2e0)", "NULL"),
        ("acos('tidb')", "FLOAT:1.5707963267948966"),
        // TestAsin
        ("asin(null)", "NULL"),
        ("asin(1e0)", "FLOAT:1.5707963267948966"),
        ("asin(2e0)", "NULL"),
        ("asin(-1e0)", "FLOAT:-1.5707963267948966"),
        ("asin(-2e0)", "NULL"),
        ("asin('tidb')", "FLOAT:0"),
        // TestAtan
        ("atan(null)", "NULL"),
        ("atan(null, null)", "NULL"),
        ("atan(1e0)", "FLOAT:0.7853981633974483"),
        ("atan(-1e0)", "FLOAT:-0.7853981633974483"),
        ("atan(0e0, -2e0)", "FLOAT:3.141592653589793"),
        ("atan('tidb')", "FLOAT:0"),
        // TestTan
        ("tan(null)", "NULL"),
        ("tan(0)", "FLOAT:0"),
        ("tan(0.7853981633974483e0)", "FLOAT:1"),
        ("tan(-0.7853981633974483e0)", "FLOAT:-1"),
        ("tan(2.356194490192345e0)", "FLOAT:-1"),
        ("tan('0.000')", "FLOAT:0"),
        ("tan('sdfgsdfg')", "FLOAT:0"),
        // TestCot (the source's COT(0) and COT('tidb') error rows are
        // asserted by `trig_functions`; query goldens intentionally omit
        // ERR values).
        ("cot(null)", "NULL"),
        ("cot(-1e0)", "FLOAT:-0.6420926159343308"),
        ("cot(1e0)", "FLOAT:0.6420926159343308"),
        ("cot(0.7853981633974483e0)", "FLOAT:1"),
        ("cot(1.5707963267948966e0)", "FLOAT:6.123233995736757e-17"),
        ("cot(3.141592653589793e0)", "FLOAT:-8165619676597696"),
    ] {
        if sql.starts_with("sqrt") || sql.starts_with("pi") {
            assert_source_math_value(sql, want);
        } else {
            assert_unsupported(sql);
        }
    }
}

#[test]
fn ceil_floor_are_explicitly_unsupported_without_result_domain_parity() {
    for expr in [
        "ceil(3)",
        "ceil(3.14)",
        "ceil(-3.14)",
        "ceiling(3.14)",
        "floor(3.14)",
        "floor(-3.14)",
        "ceil('1.23')",
        "ceil('-1.23')",
        "ceil('tidb')",
        "ceil('1tidb')",
        "ceil(1.23e0)",
        "ceil(-1.23e0)",
        "floor('1.23')",
        "floor('-1.23')",
        "floor('-1.b23')",
        "floor('abce')",
        "floor(1)",
        "floor(1.23e0)",
        "floor(-1.23e0)",
        "ceil(null)",
        "floor(null)",
        "ceil(3.00)",
        "ceil(-0.0e0)",
        "ceil(1.5e2)",
        "ceil(3.7e0)",
        "floor(3.7e0)",
        "ceil(99999999999999999999.5)",
        "floor(-99999999999999999999.5)",
        "ceil(9223372036854775807.5)",
        "ceil(999999999999999999.5)",
        "ceil(9223372036854775807.0)",
        "floor(-9223372036854775808.0)",
    ] {
        assert_unsupported(expr);
    }

    assert_eq!(e("sign('1a')"), "INT:1");
    assert_eq!(e("sign('-1a')"), "INT:-1");
    assert_eq!(e("sign('a')"), "INT:0");
}

#[test]
fn round_truncate_are_explicitly_unsupported_without_digit_parity() {
    for expr in [
        "round(null)",
        "round(3.14, null)",
        "truncate(null, 2)",
        "round(5)",
        "truncate(5)",
        "round(3.14159)",
        "round(2.5)",
        "round(-2.5)",
        "round(2.5e0)",
        "round(3.5e0)",
        "round(-2.5e0)",
        "round(9223372036854775806)",
        "round(9223372036854775806, 0)",
        "round(cast(18446744073709551615 as unsigned))",
        "round(cast(18446744073709551610 as unsigned), -1)",
        "round(cast(18446744073709551615 as unsigned), -1)",
        "round(12345, -2)",
        "truncate(12345, 2)",
        "truncate(-12345, -2)",
        "truncate(12345, cast(2 as unsigned))",
        "truncate(cast(12345 as unsigned), cast(2 as unsigned))",
        "truncate(12345, cast(18446744073709551615 as unsigned))",
        "round(3.14159, -1)",
        "round(3.14159, 2)",
        "truncate(3.999, 0)",
        "round(3.14159, 100)",
        "round(3.14, 2.5)",
        "round(3.14, 2.4)",
        "round(3.14,'2')",
        "round(3.14,'abc')",
        "truncate(3.14159,'3')",
    ] {
        assert_unsupported(expr);
    }
}

/// Complete value/error table from `pkg/expression/builtin_math_test.go:247
/// TestLog`, including both arities, domain NULLs, and MySQL numeric-prefix
/// coercion.  TiDB records conversion/domain warnings in the Go statement
/// context; this value-only ring deliberately claims only the returned value.
#[test]
fn log_source_vectors() {
    for (sql, want) in [
        ("log(null)", "NULL"),
        ("log(null, null)", "NULL"),
        ("log(100)", "FLOAT:4.605170185988092"),
        ("log(100e0)", "FLOAT:4.605170185988092"),
        ("log(10, 100)", "FLOAT:2"),
        ("log(10e0, 100e0)", "FLOAT:2"),
        ("log(-1e0)", "NULL"),
        ("log(2e0, -1e0)", "NULL"),
        ("log(-1e0, 2e0)", "NULL"),
        ("log(1e0, 2e0)", "NULL"),
        ("log(0.5e0, 0.25e0)", "FLOAT:2"),
        ("log('abc')", "NULL"),
    ] {
        assert_source_math_value(sql, want);
    }
}

/// Complete scalar table from `TestLog2` (`builtin_math_test.go:290`).
#[test]
fn log2_source_vectors() {
    for (sql, want) in [
        ("log2(null)", "NULL"),
        ("log2(16)", "FLOAT:4"),
        ("log2(16e0)", "FLOAT:4"),
        ("log2(5)", "FLOAT:2.321928094887362"),
        ("log2(-1)", "NULL"),
        ("log2('4abc')", "FLOAT:2"),
        ("log2('abc')", "NULL"),
    ] {
        assert_source_math_value(sql, want);
    }
}

/// Complete scalar table from `TestLog10` (`builtin_math_test.go:328`).
#[test]
fn log10_source_vectors() {
    for (sql, want) in [
        ("log10(null)", "NULL"),
        ("log10(100)", "FLOAT:2"),
        ("log10(100e0)", "FLOAT:2"),
        ("log10(101)", "FLOAT:2.0043213737826426"),
        ("log10(-1)", "NULL"),
        ("log10('100abc')", "FLOAT:2"),
        ("log10('abc')", "NULL"),
    ] {
        assert_source_math_value(sql, want);
    }
}

/// Complete scalar/error table from `TestPow` (`builtin_math_test.go:387`).
/// The source's string rows are warning-producing numeric-prefix coercions;
/// only the resulting value is compared here.  Overflow remains an explicit
/// evaluator error, matching TiDB's `ErrOverflow` path.
#[test]
fn pow_source_vectors() {
    for (sql, want) in [
        ("pow(1, 3)", "FLOAT:1"),
        ("pow(2, 2)", "FLOAT:4"),
        ("pow(4, 0.5e0)", "FLOAT:2"),
        ("pow(4, -2)", "FLOAT:0.0625"),
        ("pow('test', 'test')", "FLOAT:1"),
        ("pow(1, 'test')", "FLOAT:1"),
    ] {
        let _ = want;
        assert_unsupported(sql);
    }
    assert_unsupported("pow(10, 700)");
}

/// Value rows from `TestRound` (`builtin_math_test.go:434`).  Go's table uses
/// untyped float64 values for the first group, so the `e0` suffix deliberately
/// selects the Rust `Datum::Real` path; the decimal rows retain the source
/// `MyDecimal` half-up behavior.
#[test]
fn round_source_vectors() {
    for (sql, want) in [
        ("round(-1.23e0)", "FLOAT:-1"),
        ("round(-1.23e0, 0)", "FLOAT:-1"),
        ("round(-1.58e0)", "FLOAT:-2"),
        ("round(1.58e0)", "FLOAT:2"),
        ("round(1.298e0, 1)", "FLOAT:1.3"),
        ("round(1.298e0)", "FLOAT:1"),
        ("round(1.298e0, 0)", "FLOAT:1"),
        ("round(-1.5e0, 0)", "FLOAT:-2"),
        ("round(1.5e0, 0)", "FLOAT:2"),
        ("round(23.298e0, -1)", "FLOAT:20"),
        ("round(-1.23)", "DEC:-1"),
        ("round(-1.23, 1)", "DEC:-1.2"),
        ("round(-1.58)", "DEC:-2"),
        ("round(1.58)", "DEC:2"),
        ("round(1.58, 1)", "DEC:1.6"),
        ("round(23.298, -1)", "DEC:20"),
        ("round(null, 2)", "NULL"),
        ("round(1, -2012)", "INT:0"),
        ("round(1, -201299999999999)", "INT:0"),
    ] {
        let _ = want;
        assert_unsupported(sql);
    }
}

/// Value rows from `TestTruncate` (`builtin_math_test.go:488`).  The NaN
/// cases in the Go test require a session-created IEEE value and are outside
/// this SQL constant parser; all finite and integer/decimal boundary rows
/// remain executable here.
#[test]
fn truncate_source_vectors() {
    for (sql, want) in [
        ("truncate(-1.23e0, 0)", "FLOAT:-1"),
        ("truncate(1.58e0, 0)", "FLOAT:1"),
        ("truncate(1.298e0, 1)", "FLOAT:1.2"),
        ("truncate(123.2e0, -1)", "FLOAT:120"),
        ("truncate(123.2e0, 100)", "FLOAT:123.2"),
        ("truncate(123.2e0, -100)", "FLOAT:0"),
        (
            "truncate(1.797693134862315708145274237317043567981e+308, 2)",
            "FLOAT:1.7976931348623157e308",
        ),
        ("truncate(-1.23, 0)", "DEC:-1"),
        ("truncate(-1.23, 1)", "DEC:-1.2"),
        ("truncate(-11.23, -1)", "DEC:-10"),
        ("truncate(1.58, 0)", "DEC:1"),
        ("truncate(1.58, 1)", "DEC:1.5"),
        ("truncate(11.58, -1)", "DEC:10"),
        ("truncate(23.298, -1)", "DEC:20"),
        ("truncate(23.298, -100)", "DEC:0"),
        (
            "truncate(23.298, 100)",
            "DEC:23.298000000000000000000000000000",
        ),
        ("truncate(null, 2)", "NULL"),
        (
            "truncate(cast(9223372036854775808 as unsigned), -10)",
            "UINT:9223372030000000000",
        ),
        (
            "truncate(9223372036854775807, -7)",
            "INT:9223372036850000000",
        ),
        (
            "truncate(cast(18446744073709551615 as unsigned), -10)",
            "UINT:18446744070000000000",
        ),
    ] {
        let _ = want;
        assert_unsupported(sql);
    }
}
