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

//! `compare2` family builtins. Every builtin here is transcreated from its
//! implementation in `pkg/expression/builtin_*.go`, cited per function.

use std::cmp::Ordering;

#[cfg(test)]
use tidb_datatype::EvalType;
use tidb_datatype::FieldType;

use crate::coerce::coerce_str;
use crate::{Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("LEAST", _) => Some(extremum(vals, Ordering::Less, ctx)),
        ("GREATEST", _) => Some(extremum(vals, Ordering::Greater, ctx)),
        ("INTERVAL", n) if n >= 2 => Some(interval(vals, ctx)),
        ("ISNULL", 1) => Some(crate::eval_boolean_ready_in(
            crate::BooleanFunction::IsNull,
            (!vals[0].is_null()).then_some(false),
            ctx,
        )),
        ("INET_ATON", 1) => Some(inet_aton(&vals[0], ctx)),
        ("INET_NTOA", 1) => Some(inet_ntoa(&vals[0], ctx)),
        ("INET6_ATON", 1) => Some(inet6_aton(&vals[0], ctx)),
        ("INET6_NTOA", 1) => Some(inet6_ntoa(&vals[0], ctx)),
        ("IS_IPV4", 1) => Some(is_ipv4_value(&vals[0], ctx)),
        ("IS_IPV4_MAPPED", 1) => Some(is_ipv4_mapped_value(&vals[0], ctx)),
        ("IS_IPV4_COMPAT", 1) => Some(is_ipv4_compat_value(&vals[0], ctx)),
        ("IS_IPV6", 1) => Some(is_ipv6_value(&vals[0], ctx)),
        _ => None,
    }
}

/// LEAST/GREATEST: `NULL` if any argument is `NULL`, else the extreme value
/// by `want` (`Less` for LEAST, `Greater` for GREATEST) — a MIXED
/// Int/Decimal/Float argument list promotes through `eval_binary`'s own
/// comparison (confirmed via goeval: `GREATEST(1.5e2, 3.14, 2)` — Float,
/// Decimal, Int all in one call — is `FLOAT:150`, not an error), so no
/// per-type-pair matching is hand-rolled here. When a string is present,
/// Go's FieldType aggregation selects the string signature and stringifies
/// every argument before comparing it; the byte-preserving scalar path below
/// keeps that source boundary without inventing a numeric coercion. Port of
/// the signatures built by `leastFunctionClass` and
/// `greatestFunctionClass` in `pkg/expression/builtin_compare.go`.
fn extremum(vals: &[Datum], want: Ordering, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    // The AST/value evaluator has no argument `FieldType`s, so it can name
    // neither Go's signature nor a derived collation. Both are the chunk
    // evaluator's ([`extremum_with_signature`]'s other caller,
    // `ScalarFunction::eval_by_signature`) -- this tier asks for the
    // value-derived signature and the connection default collation.
    extremum_with_signature(
        vals,
        want,
        None,
        &[],
        false,
        crate::ops::DERIVATION_FREE_COLLATION,
        ctx,
    )
}

/// Go `GLCmpStringMode` (`pkg/expression/builtin_compare.go`): which of the
/// three ETString GREATEST/LEAST signatures `resolveType4Extremum` selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GlCmpStringMode {
    /// `GLCmpStringDirectly` -> `builtinGreatestStringSig`: compare the
    /// strings themselves, under the function's derived collation.
    Directly,
    /// `GLCmpStringAsDate` -> `builtinGreatestCmpStringAsTimeSig{cmpAsDate:
    /// true}`: parse every argument as a DATE and compare the re-rendered
    /// canonical text.
    AsDate,
    /// `GLCmpStringAsDatetime` -> the same signature with `cmpAsDate: false`,
    /// parsing every argument as a DATETIME.
    AsDatetime,
}

/// Go's `resolveType4Extremum` answer for one GREATEST/LEAST call: which of
/// the eight signatures `getFunction` built. Produced by
/// `crate::rewriter::result_type::gl_signature`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GlSignature {
    /// Go `argTp` -- `aggregateType(args).EvalType()`, forced to
    /// `types.ETString` by a non-`Directly` compare mode or by an ETJson
    /// aggregate. This is what Go's `switch` selects on.
    pub arg_type: tidb_datatype::EvalType,
    /// Which of the three ETString signatures.
    pub cmp_string_mode: GlCmpStringMode,
    /// `fieldTimeType == GLRetDate`, i.e. `builtinGreatestTimeSig`'s
    /// `cmpAsDate`.
    pub ret_date: bool,
}

/// [`extremum`] with the two things only the chunk evaluator knows: the
/// SIGNATURE `resolveType4Extremum` derived from the argument FieldTypes, and
/// the collation `deriveCollation` derived for the function.
///
/// The signature is the load-bearing half. Go's `argTp` is the AGGREGATE of
/// the argument types, so it fixes the comparison domain before any value
/// exists; reading the domain off the runtime datums instead answers a
/// different question whenever an argument's declared type and its datum
/// disagree about a domain. Every MySQL type whose datum is neither a string
/// nor a number is such an argument. CAPTURED from TiDB over
/// `enum('{}','[1]','x')` holding `'{}'` and `set('a','b','c')` holding
/// `'b'`:
///
/// ```text
/// greatest(e, 2) -> {}      least(e, 2) -> 2
/// greatest(s, 2) -> b       least(s, 2) -> 2
/// ```
///
/// Both aggregate to a string kind, so Go stringifies the `2` and compares
/// text. A value-derived domain sees an ENUM datum beside an integer, compares
/// them as numbers against the enum's ORDINAL, and returns the two answers
/// swapped -- and returns the enum datum itself where a string was declared,
/// which the wire encoder then renders as its raw 8-byte ordinal followed by
/// the name.
///
/// `mode != Directly` is `builtinGreatestCmpStringAsTimeSig` /
/// `builtinLeastCmpStringAsTimeSig`: EVERY argument is parsed as a time and
/// re-emitted canonically before comparison, so which argument wins changes.
/// CAPTURED from TiDB over a `DATE` column holding `2020-01-01`:
///
/// ```text
/// greatest(d, '99-1-1')  -> 2020-01-01     least(d, '99-1-1')  -> 1999-01-01
/// greatest(d, 'zzz')     -> zzz            least(d, '2019-5-5') -> 2019-05-05
/// ```
///
/// The last row is the one that pins the ERROR rule: an argument that does not
/// parse keeps its ORIGINAL text (Go `doTimeConversionForGL` leaves `strVal`
/// alone once `handleInvalidTimeError` has downgraded the error to a warning),
/// which is why `'zzz'` -- not the date -- is the greatest. Note also that this
/// signature compares with `strings.Compare`, NOT the collator: only the
/// `Directly` mode is collation-aware.
///
/// # What this selection does NOT decide
///
/// Go's three numeric arms differ only in which cast
/// `newBaseBuiltinFuncWithTp` wrapped the arguments in, so they share
/// the shared numeric reducer -- and that reducer still reads the result's
/// promotion off the runtime datums rather than off the aggregate. One
/// measured consequence, present before this selection landed and unchanged by
/// it: over `create table g(i int, d decimal(10,3))` holding `-5` and `2.500`,
/// TiDB answers `least(i, d)` with `-5` and this tier with `-5.000`.
/// `WrapWithCastAsDecimal` takes each argument's OWN decimal
/// (`tp.SetDecimalUnderLimit(expr.GetType().GetDecimal())`), so an integer
/// COLUMN keeps scale 0 -- while the all-constant `least(1, 2.5)` really is
/// `1.0`, which is the capture the block was built on. Separating a typed
/// integer argument from a folded integer constant is the next rung, not this
/// one.
///
/// # Mutation probes
///
/// Run against `cargo test -p tidb-expr -p tidb-session -p
/// difftest-result-tests`:
///
///  * IGNORE the passed signature and always take the value-derived one --
///    killed by `greatest_least_source_vectors_compare_strings_as_time`.
///  * `ret_date` forced to `false` -- killed by
///    `an_all_temporal_greatest_returns_the_aggregated_temporal_type`.
///  * DROP the ETJson-to-ETString fold -- killed only after
///    `a_json_greatest_compares_the_rendered_text` was added; a JSON aggregate
///    needs two values that rank one way as text and the other as numbers
///    (`'10'` and `'9'`), because identical JSON arguments agree in both
///    domains.
///  * `ret_date` from the DATUM kinds instead of the aggregate -- killed only
///    after `the_temporal_greatest_result_type_follows_the_aggregate_not_the
///    _values`. A declared type and its datums differ only where an
///    expression's type is merged from branches it did not take, which is what
///    `IFNULL(d, dt)` is.
///  * DROP the ENUM/SET/JSON widening in `extremum_return_type` -- killed by a
///    PANIC, not a wrong value: the chunk column expects a name/value cell.
///  * Route ETDuration through the STRING arm (over-application) -- killed only
///    after `100:00:00`/`20:00:00` was added. The declared duration result type
///    casts a stray string answer straight back into a duration, so this hides
///    completely unless the two domains ORDER the values differently.
///  * DROP the time arm entirely -- killed by both temporal tests.
///  * DROP the temporal scan's DATETIME preference -- killed only after
///    `greatest(d, dt, '2020-01-01 05:00:00')` was added.
///  * FLATTEN the value-derived fallback to one domain -- killed by
///    `expr_eval_matches_go_engine`.
///  * DROP the ETDatetime arm's ARGUMENT CAST, back to requiring every datum
///    to already be a `Datum::Time` -- killed by
///    `a_duration_beside_a_temporal_literal_lands_on_the_statement_date`, and
///    by `expression/issues` losing the statement to an out-of-domain refusal.
///  * Route ETDuration through the TIME arm as well (over-application) --
///    killed by `greatest_and_least_compare_in_the_aggregated_argument_domain`.
///
/// THREE SURVIVED, and all three are argued rather than fixture-covered:
///
///  * DROP the `cmpStringMode != Directly => argTp = ETString` override.
///    Go writes it as the `if` arm of an `if`/`else if` whose `else` handles
///    ETJson, and `resolveType4Extremum` only leaves `Directly` when the
///    aggregate is a string KIND -- which is `ETString || ETJson`. So the
///    override can differ from the ETJson fold only for a JSON aggregate that
///    also has a temporal argument, and `mergeFieldType` sends JSON beside
///    anything else to VARCHAR. CAPTURED, closing the hole: `greatest(j, d)`
///    over a JSON and a DATE column is `[1]`, the compare-as-date answer, not
///    a JSON one. The branch is unreachable; the faithful form is kept because
///    it is Go's, and because Go's ordering DOES decide whether
///    `unsupportedJSONComparison` warns.
///  * DROP `gl_signature`'s requirement that EVERY argument be statically
///    typed, aggregating only the typed ones. No fixture reaches it because
///    the rewriter types every expression it hands to `eval_by_signature`
///    today -- but `ScalarFunction::ret_type` is an `Option`, so an inner
///    builtin the return-type table cannot name would make the aggregate a
///    claim about a partial argument list. Go always holds every type, so
///    naming a domain from a subset is a claim Go never makes.
///  * SKIP an argument whose cast reports NULL instead of making the whole
///    call NULL (Go: `if isNull || err != nil { return types.ZeroTime, true,
///    err }`). No value reaches it. Every argument this arm can receive is
///    either already a `Datum::Time` -- which `cast_arg_as_datetime` hands
///    through -- or a `Datum::Duration`, whose conversion is anchored on
///    `@@timestamp` (bounded below by the epoch and above by its own
///    `MaxValue`) and shifted by at most `±838:59:59`, so the result stays
///    inside year `1..=9999` and the conversion cannot fail. CAPTURED at the
///    lower end: `@@timestamp = 1` with `time '-838:59:59'` is
///    `1969-11-27 01:00:01`, not NULL. The faithful form is kept because it is
///    Go's, and because it is the only correct answer the moment either bound
///    widens.
pub(crate) fn extremum_with_signature(
    vals: &[Datum],
    want: Ordering,
    signature: Option<GlSignature>,
    arg_decimals: &[i64],
    all_constant: bool,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_extremum_in(
        ctx,
        vals,
        want,
        signature,
        arg_decimals,
        all_constant,
        collation,
    )
}

/// `INTERVAL(n, n1, n2, ...)`: return the zero-based position of the first
/// boundary greater than `n`, or the number of boundaries when none is
/// greater. Port of `builtinIntervalIntSig.evalInt` and
/// `builtinIntervalRealSig.evalInt` in `pkg/expression/builtin_compare.go`.
///
/// Like TiDB, an integer-only call uses its exact integer signature; the
/// presence of any decimal, float, or string selects the lossy real
/// signature. A NULL target is `-1`; NULL boundaries participate only in the
/// nullable signature and are skipped. The binary search intentionally keeps
/// TiDB's documented precondition that non-NULL boundaries are sorted.
fn interval(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::eval_interval_in(ctx, vals)
}

/// Evaluates `INTERVAL` directly from its argument expressions.
///
/// Go's integer and real signatures evaluate only the boundaries visited by
/// their linear or binary search. Keeping the arguments behind this closure
/// is therefore semantic: eagerly collecting every value would surface a
/// warning or error from a boundary that the search never reads.
pub(crate) fn interval_lazy(
    arg_types: &[Option<FieldType>],
    eval: impl FnMut(usize) -> Result<Datum, EvalError>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    debug_assert!(arg_types.len() >= 2);
    crate::tikv::eval_interval_lazy_in(ctx, arg_types, eval)
}

/// `INET_ATON(expr)`: the frontend retains checked text conversion and the
/// unsigned result tag. The official kernel owns shorthand parsing and NULL
/// results for malformed addresses; invalid UTF-8 still fails `coerce_str`.
fn inet_aton(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::InetAton,
        ctx,
        || Ok(coerce_str(value)?.map(String::into_bytes)),
        crate::tikv::EvaluatedBytesResult::into_uint_bits_datum,
    )
}

/// `INET_NTOA(expr)`: keep the original ETInt conversion and warning policy;
/// the official kernel owns the IPv4 range test and canonical text formatting.
fn inet_ntoa(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::InetNtoa,
        ctx,
        || {
            let ready = match value {
                Datum::Null => None,
                Datum::Int(value) => Some(*value),
                // Carry the original UInt bits, not a saturating signed cast.
                Datum::UInt(value) => Some(*value as i64),
                other => {
                    crate::cast::report_int_truncation(other, ctx)?;
                    Some(crate::cast::to_i64_signed(other))
                }
            };
            Ok(crate::tikv::EvaluatedArgs::Int(ready))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `INET6_ATON(expr)`: preserve String/Bytes payloads and the binary result
/// tag. Even malformed raw UTF-8 reaches the official parser, which owns NULL
/// parse failures and the choice of four- versus sixteen-byte output.
fn inet6_aton(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::Inet6Aton,
        ctx,
        || eval_string_bytes(value),
        |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
    )
}

/// `INET6_NTOA(expr)`: preserve raw input bytes and pack the official result
/// as text. Byte-length validation, IPv4/IPv6 formatting and mapped-address
/// spelling all belong to the kernel, not this conversion boundary.
fn inet6_ntoa(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::Inet6Ntoa,
        ctx,
        || eval_string_bytes(value),
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `IS_IPV4(expr)`: retain checked text coercion and its historical decimal
/// leading-zero spelling, while the official predicate owns all validation.
fn is_ipv4_value(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::IsIpv4Nullable,
        ctx,
        || Ok(coerce_str(value)?.map(|text| ipv4_zero_spelling(&text))),
        crate::tikv::EvaluatedBytesResult::into_boolean_datum,
    )
}

/// Only remove redundant leading ASCII zeroes from each existing dot segment.
/// Nonempty all-zero segments retain one zero; empty segments, dot count and
/// every other byte are left intact. This neither checks a component's range
/// nor computes a predicate answer, and is not used for the other IP families.
fn ipv4_zero_spelling(value: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(value.len());
    for (index, segment) in value.split('.').enumerate() {
        if index != 0 {
            bytes.push(b'.');
        }
        let stripped = segment.trim_start_matches('0');
        if stripped.is_empty() && !segment.is_empty() {
            bytes.push(b'0');
        } else {
            bytes.extend_from_slice(stripped.as_bytes());
        }
    }
    bytes
}

/// `IS_IPV4_MAPPED(expr)`: true only for a sixteen-byte binary payload whose
/// first twelve bytes are the IPv4-mapped prefix (`::ffff:`).  The Go
/// signature receives an ETString and tests the raw bytes directly; keeping
/// this helper byte-oriented is important because arbitrary SQL strings are
/// allowed to contain invalid UTF-8.  Port of
/// `builtinIsIPv4MappedSig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_mapped_value(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::IsIpv4MappedNullable,
        ctx,
        || eval_string_bytes(value),
        crate::tikv::EvaluatedBytesResult::into_boolean_datum,
    )
}

/// `IS_IPV4_COMPAT(expr)`: true only for a sixteen-byte binary payload whose
/// first twelve bytes are all zero (`::/96`, excluding the mapped `::ffff:`
/// prefix by construction).  Port of `builtinIsIPv4CompatSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_compat_value(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::IsIpv4CompatNullable,
        ctx,
        || eval_string_bytes(value),
        crate::tikv::EvaluatedBytesResult::into_boolean_datum,
    )
}

/// Evaluates the ETString argument used by the IPv4 binary predicates and
/// INET6 conversions without decoding or replacing arbitrary bytes. Numeric
/// constants still follow the original EvalString coercion. Predicate and
/// INET6 algorithms are delegated; this helper owns only the value conversion.
fn eval_string_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::String(value) => Ok(Some(value.bytes().to_vec())),
        Datum::Bytes(value) => Ok(Some(value.clone())),
        _ => Ok(coerce_str(value)?.map(|text| text.into_bytes())),
    }
}

/// `IS_IPV6(expr)`: true for a parseable IPv6 address, including an IPv4
/// mapped spelling, but false for a pure IPv4 address. Port of
/// `builtinIsIPv6Sig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv6_value(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::IsIpv6Nullable,
        ctx,
        || Ok(coerce_str(value)?.map(String::into_bytes)),
        crate::tikv::EvaluatedBytesResult::into_boolean_datum,
    )
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::Datum;
    use crate::Decimal;
    use tidb_datatype::VectorFloat32;

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals, &crate::NoColumns)
            .expect("name/arity should dispatch to compare2")
            .expect("Go vector should evaluate")
    }

    fn s(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    /// `TestIntervalFunc` vectors that fit the current signed `Datum` domain.
    #[test]
    fn interval_go_vectors() {
        let cases: &[(Vec<Datum>, i64)] = &[
            (vec![Datum::Null, Datum::Int(1), Datum::Int(2)], -1),
            (vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)], 0),
            (vec![Datum::Int(2), Datum::Int(1), Datum::Int(3)], 1),
            (vec![Datum::Int(3), Datum::Int(1), Datum::Int(2)], 2),
            (vec![Datum::Int(0), s("b"), s("1"), s("2")], 1),
            (vec![s("a"), s("b"), s("1"), s("2")], 1),
            (
                vec![
                    Datum::Int(23),
                    Datum::Int(1),
                    Datum::Int(23),
                    Datum::Int(23),
                    Datum::Int(23),
                    Datum::Int(30),
                    Datum::Int(44),
                    Datum::Int(200),
                ],
                4,
            ),
            (
                vec![
                    Datum::Int(23),
                    Datum::Decimal(Decimal::from_literal("1.7")),
                    Datum::Decimal(Decimal::from_literal("15.3")),
                    Datum::Decimal(Decimal::from_literal("23.1")),
                    Datum::Int(30),
                    Datum::Int(44),
                    Datum::Int(200),
                ],
                2,
            ),
            (
                vec![
                    Datum::Int(9_007_199_254_740_992),
                    Datum::Int(9_007_199_254_740_993),
                ],
                0,
            ),
            (
                vec![
                    Datum::UInt(9_223_372_036_854_775_808),
                    Datum::UInt(9_223_372_036_854_775_809),
                ],
                0,
            ),
            (
                vec![Datum::Int(i64::MAX), Datum::UInt(9_223_372_036_854_775_808)],
                0,
            ),
            (
                vec![
                    Datum::Int(-9_223_372_036_854_775_807),
                    Datum::UInt(9_223_372_036_854_775_808),
                ],
                0,
            ),
            (
                vec![Datum::UInt(9_223_372_036_854_775_806), Datum::Int(i64::MAX)],
                0,
            ),
            (
                vec![
                    Datum::UInt(9_223_372_036_854_775_806),
                    Datum::Int(-9_223_372_036_854_775_807),
                ],
                1,
            ),
            (vec![Datum::Int(-1), Datum::Int(2333), Datum::Null], 0),
            (
                vec![Datum::Int(1), Datum::Null, Datum::Null, Datum::Null],
                3,
            ),
            (
                vec![
                    Datum::Int(1),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Int(2),
                ],
                3,
            ),
            (
                vec![Datum::Int(9_007_199_254_740_992), s("9007199254740993")],
                1,
            ),
            (
                vec![s("9007199254740992"), Datum::Int(9_007_199_254_740_993)],
                1,
            ),
            (vec![s("9007199254740992"), s("9007199254740993")], 1),
            // Go's StrToFloat saturates an overflowing real conversion to
            // MAXFLOAT (with truncation ignored by TestIntervalFunc). The
            // old Rust fallback returned zero, placing this target before a
            // 1e308 boundary instead of after it.
            (vec![s("1e999"), Datum::Real(1e308)], 1),
        ];
        for (args, want) in cases {
            assert_eq!(call("INTERVAL", args), Datum::Int(*want));
        }
    }

    /// `TestInetAton` exact valid/NULL vectors. Its malformed-input vectors
    /// assert a strict-context TiDB error, which is still an error here.
    #[test]
    fn inet_aton_go_vectors() {
        let cases = [
            (Datum::Null, Datum::Null),
            (s("255.255.255.255"), Datum::UInt(4_294_967_295)),
            (s("0.0.0.0"), Datum::UInt(0)),
            (s("127.0.0.1"), Datum::UInt(2_130_706_433)),
            (s("113.14.22.3"), Datum::UInt(1_896_748_547)),
            (s("127"), Datum::UInt(127)),
            (s("127.255"), Datum::UInt(2_130_706_687)),
            (s("127.2.1"), Datum::UInt(2_130_837_505)),
        ];
        for (arg, want) in cases {
            assert_eq!(call("INET_ATON", &[arg]), want);
        }
        // go's TestInetAton marks every malformed input `expectNil`: the sig
        // answers NULL, not a strict-context error.
        for invalid in ["", "0.0.0.256", "127,256", "123.2.1.", "127.0.0.1.1"] {
            assert_eq!(
                dispatch("INET_ATON", &[s(invalid)], &crate::NoColumns).unwrap(),
                Ok(Datum::Null),
                "INET_ATON({invalid:?})"
            );
        }
    }

    /// `TestInetNtoa` vectors, including values outside the IPv4 range.
    #[test]
    fn inet_ntoa_go_vectors() {
        let cases = [
            (
                Datum::Int(167_773_449),
                Datum::new_string("10.0.5.9".to_string()),
            ),
            (
                Datum::Int(2_063_728_641),
                Datum::new_string("123.2.0.1".to_string()),
            ),
            (Datum::Int(0), Datum::new_string("0.0.0.0".to_string())),
            (Datum::Int(545_460_846_593), Datum::Null),
            (Datum::Int(-1), Datum::Null),
            (
                Datum::Int(4_294_967_295),
                Datum::new_string("255.255.255.255".to_string()),
            ),
            (Datum::Null, Datum::Null),
        ];
        for (arg, want) in cases {
            assert_eq!(call("INET_NTOA", &[arg]), want);
        }
    }

    /// `TestInet6AtoN` exact source vectors.  The result is binary even when
    /// the input is ordinary dotted IPv4 text: a plain IPv4 spelling uses
    /// four bytes, while every colon-containing spelling uses sixteen.
    #[test]
    fn inet6_aton_go_vectors() {
        let cases = [
            ("0.0.0.0", Datum::new_bytes([0, 0, 0, 0])),
            ("10.0.5.9", Datum::new_bytes([0x0a, 0, 5, 9])),
            (
                "fdfe::5a55:caff:fefa:9089",
                Datum::new_bytes([
                    0xfd, 0xfe, 0, 0, 0, 0, 0, 0, 0x5a, 0x55, 0xca, 0xff, 0xfe, 0xfa, 0x90, 0x89,
                ]),
            ),
            (
                "::ffff:1.2.3.4",
                Datum::new_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            ),
            ("", Datum::Null),
            ("Not IP address", Datum::Null),
            ("1.0002.3.4", Datum::Null),
            ("1.2.256", Datum::Null),
            (
                "::ffff:255.255.255.255",
                Datum::new_bytes([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                ]),
            ),
        ];
        for (text, want) in cases {
            match want {
                // go `net.ParseIP` failure answers NULL, not an error.
                Datum::Null => assert_eq!(
                    dispatch("INET6_ATON", &[s(text)], &crate::NoColumns).unwrap(),
                    Ok(Datum::Null)
                ),
                want => assert_eq!(call("INET6_ATON", &[s(text)]), want),
            }
        }
        assert_eq!(call("INET6_ATON", &[Datum::Null]), Datum::Null);
    }

    /// `TestInet6NtoA` exact source vectors, including invalid byte lengths
    /// and the NULL input path.
    #[test]
    fn inet6_ntoa_go_vectors() {
        let cases = [
            (Datum::new_bytes([0, 0, 0, 0]), "0.0.0.0"),
            (Datum::new_bytes([0x0a, 0, 5, 9]), "10.0.5.9"),
            (
                Datum::new_bytes([
                    0xfd, 0xfe, 0, 0, 0, 0, 0, 0, 0x5a, 0x55, 0xca, 0xff, 0xfe, 0xfa, 0x90, 0x89,
                ]),
                "fdfe::5a55:caff:fefa:9089",
            ),
            (
                Datum::new_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
                "::ffff:1.2.3.4",
            ),
            (
                Datum::new_bytes([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                ]),
                "::ffff:255.255.255.255",
            ),
        ];
        for (value, want) in cases {
            assert_eq!(call("INET6_NTOA", &[value]), s(want));
        }
        for bytes in [Vec::new(), vec![0x0a, 0, 5], vec![0; 15]] {
            assert_eq!(call("INET6_NTOA", &[Datum::new_bytes(bytes)]), Datum::Null);
        }
        assert_eq!(call("INET6_NTOA", &[Datum::Null]), Datum::Null);
    }

    /// `TestIsIPv4` and `TestIsIPv6` vectors, plus their NULL checks.
    #[test]
    fn is_ip_go_vectors() {
        for (ip, want) in [
            ("192.168.1.1", 1),
            ("255.255.255.255", 1),
            ("10.t.255.255", 0),
            ("10.1.2.3.4", 0),
            ("2001:250:207:0:0:eef2::1", 0),
            ("::ffff:1.2.3.4", 0),
            ("1...1", 0),
            ("192.168.1.", 0),
            (".168.1.2", 0),
            ("168.1.2", 0),
            ("1.2.3.4.5", 0),
        ] {
            assert_eq!(call("IS_IPV4", &[s(ip)]), Datum::Int(want));
        }
        assert_eq!(call("IS_IPV4", &[Datum::Null]), Datum::Null);
        for (ip, want) in [
            ("2001:250:207:0:0:eef2::1", 1),
            ("2001:0250:0207:0001:0000:0000:0000:ff02", 1),
            ("2001:250:207::eff2::1，", 0),
            ("192.168.1.1", 0),
            ("::ffff:1.2.3.4", 1),
        ] {
            assert_eq!(call("IS_IPV6", &[s(ip)]), Datum::Int(want));
        }
        assert_eq!(call("IS_IPV6", &[Datum::Null]), Datum::Null);
    }

    /// `TestIsIPv4Mapped` and `TestIsIPv4Compat` operate on raw ETString
    /// bytes, not textual IPv6 spellings.  Keep every source row here,
    /// including the malformed lengths and the SQL NULL path.
    #[test]
    fn is_ipv4_binary_predicate_go_vectors() {
        let mapped_cases = [
            (vec![], 0),
            (vec![0x10, 0x10, 0x10, 0x10], 0),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4],
                1,
            ),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4],
                0,
            ),
            (vec![0, 1, 2, 3, 4, 5, 6], 0),
            // Go's EvalString preserves arbitrary bytes; invalid UTF-8 is
            // still just a non-matching binary payload.
            (vec![0xff; 16], 0),
        ];
        for (bytes, want) in mapped_cases {
            assert_eq!(
                call("IS_IPV4_MAPPED", &[Datum::new_bytes(bytes)]),
                Datum::Int(want)
            );
        }
        assert_eq!(call("IS_IPV4_MAPPED", &[Datum::Null]), Datum::Null);

        let compat_cases = [
            (vec![], 0),
            (vec![0x10, 0x10, 0x10, 0x10], 0),
            (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4], 1),
            (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 2, 3, 4], 0),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4],
                0,
            ),
            (vec![0, 1, 2, 3, 4, 5, 6], 0),
            (vec![0xff; 16], 0),
        ];
        for (bytes, want) in compat_cases {
            assert_eq!(
                call("IS_IPV4_COMPAT", &[Datum::new_bytes(bytes)]),
                Datum::Int(want)
            );
        }
        assert_eq!(call("IS_IPV4_COMPAT", &[Datum::Null]), Datum::Null);
    }

    /// Go `TestIsNullFunc`; `builtin*IsNullSig.evalInt` in `builtin_op.go`
    /// has the same result for every represented eval type, so retain the
    /// source's integer/NULL rows and cover the other Rust datum families too.
    #[test]
    fn test_is_null_func() {
        for value in [
            Datum::Int(0),
            s(""),
            Datum::Decimal(Decimal::from_literal("0.0")),
            Datum::Real(0.0),
        ] {
            assert_eq!(call("ISNULL", &[value]), Datum::Int(0));
        }
        assert_eq!(call("ISNULL", &[Datum::Null]), Datum::Int(1));
    }

    /// LEAST/GREATEST print the SIGNATURE's scale, not the winner's own.
    ///
    /// Go aggregates the arguments' FieldTypes into one return type whose
    /// `Decimal` is the max over them and casts every argument to it, so an
    /// INTEGER argument that wins the comparison still prints a fraction.
    /// Every expectation is a verbatim capture from real TiDB.
    #[test]
    fn extremum_decimal_carries_the_aggregated_scale() {
        let d = |text: &str| Datum::Decimal(Decimal::from_literal(text));
        let text = |value: Datum| value.sql_string().expect("a decimal renders");
        for (name, args, expected) in [
            // The winner is the INT and still carries the fraction.
            ("LEAST", vec![Datum::Int(1), d("2.5")], "1.0"),
            ("LEAST", vec![d("2.5"), Datum::Int(1)], "1.0"),
            ("LEAST", vec![Datum::Int(1), d("2.555")], "1.000"),
            ("LEAST", vec![Datum::Int(1), d("2.50")], "1.00"),
            ("LEAST", vec![Datum::Int(-1), d("2.5")], "-1.0"),
            ("GREATEST", vec![Datum::Int(3), d("2.5")], "3.0"),
            ("GREATEST", vec![Datum::Int(1), d("2.0")], "2.0"),
            // The MAX scale over ALL arguments, not the winner's.
            ("LEAST", vec![Datum::Int(1), d("2.5"), d("3.25")], "1.00"),
            (
                "GREATEST",
                vec![Datum::Int(3), d("2.55"), Datum::Int(1)],
                "3.00",
            ),
            ("GREATEST", vec![d("2.5"), d("1.234")], "2.500"),
            ("LEAST", vec![d("2.5"), d("1.234")], "1.234"),
            ("GREATEST", vec![d("2.5"), d("1.2")], "2.5"),
            // No decimal argument at all keeps the integer domain.
            ("LEAST", vec![Datum::Int(1), Datum::Int(2)], "1"),
            // A signed/unsigned mix promotes to DECIMAL but to scale 0,
            // because neither argument carries a fraction.
            ("LEAST", vec![Datum::Int(1), Datum::UInt(2)], "1"),
            ("GREATEST", vec![Datum::Int(1), Datum::UInt(2)], "2"),
        ] {
            assert_eq!(text(call(name, &args)), expected, "{name}({args:?})");
        }
    }

    #[test]
    fn extremum_uses_the_vector_signature_when_either_argument_is_a_vector() {
        let vector = |values| Datum::new_vector_float32(VectorFloat32::must_create(values));
        assert_eq!(
            call(
                "GREATEST",
                &[vector(vec![1.0, 2.0]), Datum::new_string("[1,3]")]
            ),
            vector(vec![1.0, 3.0])
        );
        assert_eq!(
            call("LEAST", &[vector(vec![1.0, 3.0]), vector(vec![1.0, 2.0])]),
            vector(vec![1.0, 2.0])
        );

        let expression = tidb_ast::Expr::Func {
            name: "greatest".to_owned(),
            args: vec![
                tidb_ast::Expr::Func {
                    name: "vec_from_text".to_owned(),
                    args: vec![tidb_ast::Expr::String("[1,2]".to_owned())],
                    origin_position: 0,
                },
                tidb_ast::Expr::String("[1,3]".to_owned()),
            ],
            origin_position: 0,
        };
        let rewritten =
            crate::rewriter::rewrite_expr(&expression).expect("vector extremum rewrite");
        assert_eq!(
            rewritten
                .static_type()
                .expect("vector return type")
                .eval_type(),
            tidb_datatype::EvalType::VectorFloat32
        );
    }
}

#[cfg(test)]
#[test]
fn extremum_policy_adapter_preserves_null_ties_scale_and_eager_demand() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::RefCell;
    use tidb_datatype::{DateModes, Decimal, FieldTypeCode, SessionTimeZone};
    #[derive(Debug, Eq, PartialEq)]
    enum Event {
        Child(usize),
        Modes,
        Zone,
    }
    struct Demand {
        values: Vec<Datum>,
        fail: Option<usize>,
        events: RefCell<Vec<Event>>,
    }
    impl crate::Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.param_value(path[0].parse().unwrap()).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.events.borrow_mut().push(Event::Child(index));
            if self.fail == Some(index) {
                return Err(EvalError::Unsupported("extremum child"));
            }
            Ok(self.values[index].clone())
        }
        fn date_modes(&self) -> DateModes {
            self.events.borrow_mut().push(Event::Modes);
            DateModes::default()
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push(Event::Zone);
            SessionTimeZone::utc()
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            panic!("no caller truncation policy for these extrema")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("these extrema do not append warnings")
        }
    }
    let context = |values, fail| Demand {
        values,
        fail,
        events: RefCell::new(Vec::new()),
    };
    let ctx = context(vec![], None);
    let signature = |arg_type, cmp_string_mode| GlSignature {
        arg_type,
        cmp_string_mode,
        ret_date: false,
    };
    let run = |values: &[Datum], want, sig, decimals: &[i64], constant| {
        extremum_with_signature(
            values,
            want,
            sig,
            decimals,
            constant,
            crate::ops::DERIVATION_FREE_COLLATION,
            &ctx,
        )
    };
    let decimal = |s: &str| Datum::Decimal(Decimal::from_literal(s));
    for want in [Ordering::Less, Ordering::Greater] {
        assert_eq!(
            run(&[], want, None, &[], false),
            Err(EvalError::Unsupported("bad function arity"))
        );
        assert_eq!(
            run(&[Datum::MaxValue], want, None, &[], false),
            Ok(Datum::MaxValue)
        );
        for arg_type in EvalType::ALL {
            assert_eq!(
                run(
                    &[Datum::MinNotNull, Datum::new_bytes(vec![255]), Datum::Null],
                    want,
                    Some(signature(arg_type, GlCmpStringMode::AsDatetime)),
                    &[i64::MAX],
                    true
                ),
                Ok(Datum::Null)
            );
        }
        for (values, scales, expected) in [
            (vec![decimal("1.0"), decimal("1.000")], vec![1, 3], "1.0"),
            (vec![decimal("1.000"), decimal("1.0")], vec![3, 1], "1.000"),
        ] {
            let value = run(&values, want, None, &scales, false).unwrap();
            assert_eq!(value.sql_string().unwrap(), expected);
        }
        let nan = f64::from_bits(0x7ff8_0000_0000_0123);
        let Datum::Real(value) = run(
            &[Datum::Real(nan), Datum::Real(1.0)],
            want,
            None,
            &[],
            false,
        )
        .unwrap() else {
            panic!("real promotion");
        };
        assert_eq!(value.to_bits(), nan.to_bits());
        assert_eq!(
            run(
                &[Datum::Real(1.0), Datum::Real(nan)],
                want,
                None,
                &[],
                false
            ),
            Ok(Datum::Real(1.0))
        );
        let Datum::Real(value) = run(
            &[Datum::Real(-0.0), Datum::Real(0.0)],
            want,
            None,
            &[],
            false,
        )
        .unwrap() else {
            panic!("real promotion");
        };
        assert_eq!(value.to_bits(), (-0.0f64).to_bits());
    }
    for (scales, constant, want, expected) in [
        (vec![-1, -1, 4], true, Ordering::Less, "1.0000"),
        (vec![-1, -1, 4], false, Ordering::Less, "1.0"),
        (vec![3], false, Ordering::Less, "1.000"),
        (vec![3], false, Ordering::Greater, "2.5"),
        (vec![3], true, Ordering::Greater, "2.500"),
        (vec![i64::MIN], true, Ordering::Less, "1.0"),
        (vec![i64::from(u32::MAX) + 2], false, Ordering::Less, "1.0"),
    ] {
        let value = run(
            &[Datum::Int(1), decimal("2.5")],
            want,
            None,
            &scales,
            constant,
        )
        .unwrap();
        assert_eq!(value.sql_string().unwrap(), expected);
    }
    // Explicit numeric signature does not override the existing runtime
    // promotion; conversely ETReal metadata does not force an integer to Real.
    assert_eq!(
        run(
            &[Datum::Int(1), Datum::Real(2.0)],
            Ordering::Less,
            Some(signature(EvalType::Int, GlCmpStringMode::Directly)),
            &[],
            false
        ),
        Ok(Datum::Real(1.0))
    );
    assert_eq!(
        run(
            &[Datum::Int(1), Datum::Int(2)],
            Ordering::Greater,
            Some(signature(EvalType::Real, GlCmpStringMode::Directly)),
            &[],
            false
        ),
        Ok(Datum::Int(2))
    );
    assert_eq!(
        run(
            &[Datum::Int(1), Datum::UInt(2)],
            Ordering::Greater,
            None,
            &[],
            false
        ),
        Ok(decimal("2"))
    );
    assert!(ctx.events.borrow().is_empty());
    let as_time = Some(signature(EvalType::String, GlCmpStringMode::AsDatetime));
    assert_eq!(
        run(
            &[
                Datum::new_string("2020-01-01"),
                Datum::new_bytes(vec![255]),
                Datum::new_string("later")
            ],
            Ordering::Greater,
            as_time,
            &[],
            false
        ),
        Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
    );
    assert_eq!(*ctx.events.borrow(), vec![Event::Modes, Event::Zone]);
    ctx.events.borrow_mut().clear();
    assert_eq!(
        run(
            &[Datum::new_string("2020-01-01"), Datum::new_string("bad")],
            Ordering::Greater,
            as_time,
            &[],
            false
        ),
        Ok(Datum::new_string("bad"))
    );
    assert_eq!(
        *ctx.events.borrow(),
        vec![Event::Modes, Event::Zone, Event::Modes, Event::Zone]
    );
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    for typed in [false, true] {
        for (fail, null_first) in [(None, true), (Some(2), true), (None, false)] {
            let values = if null_first {
                vec![Datum::Null, Datum::Int(2), Datum::Int(3)]
            } else {
                vec![Datum::Int(1), Datum::Real(2.0), Datum::Int(3)]
            };
            let ctx = context(values, fail);
            let result = if typed {
                let args = (0..3)
                    .map(|index| {
                        let mut value =
                            Constant::new(Datum::Null, FieldType::new(FieldTypeCode::LongLong));
                        value.param_marker = Some(ParamMarker {
                            order: i64::from(index),
                        });
                        Expression::Constant(value)
                    })
                    .collect();
                ScalarFunction::new(
                    tidb_ast::CiString::new("greatest"),
                    FieldType::new(FieldTypeCode::LongLong),
                    args,
                )
                .eval(&ctx, row.to_row())
            } else {
                let args = (0..3)
                    .map(|index| tidb_ast::Expr::Column(vec![index.to_string()]))
                    .collect::<Vec<_>>();
                crate::func::eval_func("GREATEST", &args, &ctx, None)
            };
            if fail.is_some() {
                assert!(result.is_err());
            } else {
                assert_eq!(
                    result,
                    Ok(if null_first {
                        Datum::Null
                    } else if typed {
                        // ParamMarker supplies the Real unchanged, so the
                        // reducer promotes to Real; ScalarFunction::eval then
                        // projects that result to this node's declared LongLong.
                        Datum::Int(3)
                    } else {
                        Datum::Real(3.0)
                    })
                );
            }
            assert_eq!(
                *ctx.events.borrow(),
                vec![Event::Child(0), Event::Child(1), Event::Child(2)]
            );
        }
    }
}

#[cfg(test)]
#[cfg(test)]
#[test]
fn interval_entries_preserve_eager_and_lazy_search_demand() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::RefCell;
    use tidb_datatype::{FieldTypeCode as Code, FieldTypeFlags};

    #[derive(Debug, Eq, PartialEq)]
    enum Event {
        Child(usize),
        Level,
        Warning(u16, String),
    }
    struct Probe {
        values: Vec<Datum>,
        fail: Option<usize>,
        level: crate::ErrorLevel,
        events: RefCell<Vec<Event>>,
    }
    impl crate::Columns for Probe {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.param_value(path[0].parse().unwrap()).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.events.borrow_mut().push(Event::Child(index));
            if self.fail == Some(index) {
                return Err(EvalError::Unsupported("INTERVAL child"));
            }
            Ok(self.values[index].clone())
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.events.borrow_mut().push(Event::Level);
            self.level
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.events
                .borrow_mut()
                .push(Event::Warning(code, message.to_owned()));
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            panic!("no date policy for these INTERVAL arguments")
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("no timezone for these INTERVAL arguments")
        }
    }
    let probe = |values, fail, level| Probe {
        values,
        fail,
        level,
        events: RefCell::new(Vec::new()),
    };
    let fields = |code, nullable, len| {
        let field = FieldType::new(code);
        vec![
            Some(if nullable {
                field
            } else {
                field.with_added_flags(FieldTypeFlags::NOT_NULL)
            });
            len
        ]
    };
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate = |entry, types: &[Option<FieldType>], ctx: &Probe| match entry {
        0 => dispatch("INTERVAL", &ctx.values, ctx).unwrap(),
        1 => {
            let args = (0..ctx.values.len())
                .map(|index| tidb_ast::Expr::Column(vec![index.to_string()]))
                .collect::<Vec<_>>();
            crate::func::eval_func("INTERVAL", &args, ctx, None)
        }
        2 => interval_lazy(types, |index| crate::Columns::param_value(ctx, index), ctx),
        _ => {
            let args = types
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let mut value = Constant::new(Datum::Null, FieldType::new(Code::LongLong));
                    value.ret_type = field.clone();
                    value.param_marker = Some(ParamMarker {
                        order: i64::try_from(index).unwrap(),
                    });
                    Expression::Constant(value)
                })
                .collect();
            ScalarFunction::new(
                tidb_ast::CiString::new("interval"),
                FieldType::new(Code::LongLong),
                args,
            )
            .eval(ctx, row.to_row())
        }
    };
    let children = |entry, len, lazy: &[usize]| -> Vec<Event> {
        match entry {
            0 => vec![],
            1 => (0..len).map(Event::Child).collect(),
            _ => lazy.iter().copied().map(Event::Child).collect(),
        }
    };
    for entry in 0..4 {
        // Eager's boundary <= target and lazy's target < boundary are NOT
        // interchangeable on unordered IEEE operands.
        for (values, lazy_result, visits) in [
            (
                vec![Datum::Real(f64::NAN), Datum::Real(1.0), Datum::Real(2.0)],
                2,
                vec![0, 2],
            ),
            (
                vec![Datum::Real(0.0), Datum::Real(f64::NAN), Datum::Real(2.0)],
                1,
                vec![0, 2, 1],
            ),
        ] {
            let ctx = probe(values, None, crate::ErrorLevel::Warn);
            assert_eq!(
                evaluate(entry, &fields(Code::Double, false, 3), &ctx),
                Ok(Datum::Int(if entry < 2 { 0 } else { lazy_result }))
            );
            assert_eq!(*ctx.events.borrow(), children(entry, 3, &visits));
        }
        // Actual NULL selects eager linear search. Lazy search instead obeys
        // declared nullability, even when a NOT_NULL boundary returns NULL.
        for nullable in [false, true] {
            let ctx = probe(
                vec![Datum::Int(0), Datum::Int(1), Datum::Null, Datum::Int(5)],
                None,
                crate::ErrorLevel::Warn,
            );
            assert_eq!(
                evaluate(entry, &fields(Code::LongLong, nullable, 4), &ctx),
                Ok(Datum::Int(if entry < 2 || nullable { 0 } else { 2 }))
            );
            let visited: &[usize] = if nullable { &[0, 1] } else { &[0, 2, 3] };
            assert_eq!(*ctx.events.borrow(), children(entry, 4, visited));
        }
        // Datum integer kinds preserve unsigned order and precision. Missing
        // lazy metadata selects Real; eager classification still sees Ints.
        for (values, exact, real) in [
            (
                vec![
                    Datum::UInt(9_007_199_254_740_992),
                    Datum::UInt(9_007_199_254_740_993),
                ],
                0,
                1,
            ),
            (vec![Datum::Int(-1), Datum::UInt(0)], 0, 0),
            (vec![Datum::UInt(u64::MAX), Datum::Int(-1)], 1, 1),
        ] {
            for typed in [false, true] {
                let types = if typed {
                    fields(Code::LongLong, false, 2)
                } else {
                    vec![None, None]
                };
                let ctx = probe(values.clone(), None, crate::ErrorLevel::Warn);
                assert_eq!(
                    evaluate(entry, &types, &ctx),
                    Ok(Datum::Int(if entry < 2 || typed { exact } else { real }))
                );
                assert_eq!(*ctx.events.borrow(), children(entry, 2, &[0, 1]));
            }
        }
        let ctx = probe(
            vec![Datum::Null, Datum::MaxValue],
            None,
            crate::ErrorLevel::Warn,
        );
        assert_eq!(
            evaluate(entry, &fields(Code::LongLong, false, 2), &ctx),
            if entry < 2 {
                Err(EvalError::Unsupported("range sentinel INTERVAL argument"))
            } else {
                Ok(Datum::Int(-1))
            }
        );
        assert_eq!(*ctx.events.borrow(), children(entry, 2, &[0]));

        // Eager converts ALL boundaries before search. Nullable lazy stops at
        // index 1; NOT_NULL lazy visits index 2 first, warning/error included.
        for nullable in [false, true] {
            for level in [crate::ErrorLevel::Warn, crate::ErrorLevel::Error] {
                let ctx = probe(
                    vec![
                        Datum::Real(0.0),
                        Datum::Real(1.0),
                        Datum::new_string("2tail"),
                    ],
                    None,
                    level,
                );
                let demanded = entry < 2 || !nullable;
                let message = "Truncated incorrect DOUBLE value: '2tail'";
                let expected = if demanded && level == crate::ErrorLevel::Error {
                    Err(EvalError::TruncatedWrongValue(message.to_owned()))
                } else {
                    Ok(Datum::Int(0))
                };
                assert_eq!(
                    evaluate(entry, &fields(Code::Double, nullable, 3), &ctx),
                    expected
                );
                let mut events = children(entry, 3, if nullable { &[0, 1] } else { &[0, 2] });
                if demanded {
                    events.push(Event::Level);
                    if level == crate::ErrorLevel::Warn {
                        events.push(Event::Warning(1292, message.to_owned()));
                    }
                    if entry >= 2 && level == crate::ErrorLevel::Warn {
                        events.push(Event::Child(1));
                    }
                }
                assert_eq!(*ctx.events.borrow(), events);
            }
        }
        if entry != 0 {
            let ctx = probe(
                vec![Datum::Real(0.0), Datum::Real(1.0), Datum::Real(2.0)],
                Some(2),
                crate::ErrorLevel::Warn,
            );
            let result = evaluate(entry, &fields(Code::Double, true, 3), &ctx);
            if entry == 1 {
                assert!(result.is_err());
            } else {
                assert_eq!(result, Ok(Datum::Int(0)));
            }
            assert_eq!(*ctx.events.borrow(), children(entry, 3, &[0, 1]));
        }
    }
}
