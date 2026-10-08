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

//! `string2` family builtins. Every builtin here is transcreated from its
//! implementation in `pkg/expression/builtin_*.go`, cited per function.

use tidb_datatype::Collation;

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::string_fn::substring_with_contexts;
use crate::string_signature::{is_binary_str, normalize_utf8_go};
use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp, NativeCollation, ReadyBytesArg, ReadyIntArg};
use crate::{Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("SUBSTRING" | "SUBSTR" | "MID", 2) => {
            Some(substring_with_contexts(vals, &crate::NoColumns, ctx))
        }
        ("LOCATE", 3) => Some(locate3_in(vals, ctx)),
        ("FORMAT", 3) => Some(format_with_locale(vals, ctx)),
        ("FIND_IN_SET", 2) => Some(find_in_set(vals, ctx)),
        ("EXPORT_SET", 3..=5) => Some(export_set_in(vals, ctx)),
        ("LTRIM", 1) => Some(ltrim(&vals[0], ctx)),
        ("RTRIM", 1) => Some(rtrim(&vals[0], ctx)),
        ("TRANSLATE", 3) => Some(translate(vals, ctx)),
        _ => None,
    }
}

/// `TRANSLATE` retains its original signature and left-to-right NULL demand.
/// Any binary argument selects byte substitution, but only arg 0 selects the
/// result charset. Both substitution maps and first-occurrence-wins policy
/// belong to the worker, including deletion when `from` outlasts `to`.
///
/// Operation selection needs the actual coercion outcome. With no selected-op
/// driver, prepare the original inputs before capability discovery/scope guard
/// and admission, then make one ordinary call. An observed NULL uses its own
/// witness without pretending that any undemanded suffix was a SQL NULL.
fn translate(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let binary = vals.iter().take(3).any(is_binary_str);
    let coerce = |value: &Datum| -> Result<Option<Vec<u8>>, EvalError> {
        if binary {
            coerce_str_bytes(value)
        } else {
            Ok(coerce_str(value)?.map(String::into_bytes))
        }
    };
    let (operation, ready) = 'prepare: {
        let Some(src) = coerce(&vals[0])? else {
            break 'prepare (
                EvaluatedBytesOp::TranslateNullNative,
                EvaluatedArgs::NullWitness(None),
            );
        };
        let Some(from) = coerce(&vals[1])? else {
            break 'prepare (
                EvaluatedBytesOp::TranslateNullNative,
                EvaluatedArgs::NullWitness(None),
            );
        };
        let Some(to) = coerce(&vals[2])? else {
            break 'prepare (
                EvaluatedBytesOp::TranslateNullNative,
                EvaluatedArgs::NullWitness(None),
            );
        };
        let operation = if binary {
            EvaluatedBytesOp::TranslateBinaryNative
        } else {
            EvaluatedBytesOp::TranslateUtf8Native
        };
        (
            operation,
            EvaluatedArgs::Bytes3([Some(src), Some(from), Some(to)]),
        )
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(ready),
        |computed| {
            Ok(match computed.into_bytes()? {
                None => Datum::Null,
                Some(bytes) if is_binary_str(&vals[0]) => Datum::new_bytes(bytes),
                Some(bytes) => Datum::new_string(bytes),
            })
        },
    )
}

/// `LTRIM(str)`, ported from `builtinLTrimSig.evalString` in
/// `pkg/expression/builtin_string.go`.
///
/// TiDB passes the argument through ETString (`builtin_string.go:2029`, now
/// `crate::arg_eval_type`'s cast), then calls Go's `strings.TrimLeft(str,
/// " ")`: it removes only U+0020 SPACE, not general Unicode whitespace.
/// The TiKV evaluated-bytes worker owns that U+0020-only scan.
///
/// The scan is over BYTES because Go's is: `strings.TrimLeft` works on a Go
/// string, which is not UTF-8 validated, and U+0020 is a single byte that
/// cannot occur inside a multi-byte UTF-8 sequence, so a byte scan and a
/// character scan strip exactly the same prefix. Captured from real TiDB
/// (`gorun`), `hex(ltrim(v))` over a `varbinary` holding `0xFF` is `FF` and
/// `hex(ltrim(b))` over a `bit(8)` holding `b'11111111'` is `FF`; both were
/// hard errors under the previous UTF-8 coercion.
fn ltrim(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    trimmed(value, crate::tikv::EvaluatedBytesOp::LTrim, ctx)
}

/// `RTRIM(str)`, ported from `builtinRTrimSig.evalString` in
/// `pkg/expression/builtin_string.go`; it has the same ETString and
/// U+0020-only contract as [`ltrim`].
fn rtrim(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    trimmed(value, crate::tikv::EvaluatedBytesOp::RTrim, ctx)
}

/// The shared half of [`ltrim`] and [`rtrim`]: read the `types.ETString`
/// argument's bytes, delegate stripping, and pack as binary or character data.
/// Go's `SetBinFlagOrBinStr(argType, bf.tp)` sets the higher return descriptor.
fn trimmed(
    value: &Datum,
    operation: crate::tikv::EvaluatedBytesOp,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        operation,
        ctx,
        || crate::arg_eval_type::eval_string(value),
        |result| {
            let bytes = result.into_bytes()?;
            Ok(bytes.map_or(Datum::Null, |bytes| {
                if is_binary_str(value) {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

/// `LOCATE(substr, str, pos)`, from `builtinLocate3ArgsSig` and
/// `builtinLocate3ArgsUTF8Sig` in `pkg/expression/builtin_string.go`.
/// This historical value-only path uses exact equality, not the main typed
/// path's collator search. TiKV owns its wrapping start, ranges and scan.
fn locate3_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let binary = crate::string_fn::locate_collation(&vals[0], &vals[1]) == Collation::Binary;
    let operation = if binary {
        EvaluatedBytesOp::Locate3BytesExtNative
    } else {
        EvaluatedBytesOp::Locate3Utf8ExtNative
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            let prepare_bytes = |value: &Datum| {
                coerce_str_bytes(value).map(|bytes| {
                    bytes.map(|bytes| {
                        if binary {
                            bytes
                        } else {
                            normalize_utf8_go(bytes)
                        }
                    })
                })
            };
            // Preserve all three tuple coercions, even after an earlier NULL.
            // Only preparation stays here; there is no native unit/slice scan.
            let needle = prepare_bytes(&vals[0])?;
            let haystack = prepare_bytes(&vals[1])?;
            let position = crate::arg_eval_type::eval_int(&vals[2])?;
            Ok(EvaluatedArgs::BytesBytesInt(needle, haystack, position))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
fn locate3(vals: &[Datum]) -> Result<Datum, EvalError> {
    locate3_in(vals, &crate::NoColumns)
}

/// `FORMAT(x, d, locale)`, ported from `builtinFormatWithLocaleSig` in
/// `pkg/expression/builtin_string.go`. A `NULL` locale warns 1649 naming the
/// literal text `NULL` and then falls back to `en_US`; an unrecognized one
/// warns 1649 naming itself. The shared guarded preparation owns both phases.
fn format_with_locale(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::string_fn::format_num_with_locale_prepare(vals, || coerce_str(&vals[2]), ctx)
}

/// `FIND_IN_SET(str, strlist)`, ported from `builtinFindInSetSig.evalInt` in
/// `pkg/expression/builtin_string.go`. The values-only entry point retains a
/// non-default collation already carried by a datum; plain AST literals use
/// the connection default because that tier has no derived function type.
fn find_in_set(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    // The AST/value tier has no separately derived function type, so retain
    // the explicit collation stamped on a COLLATE operand. Plain literals
    // carry the connection default (`utf8mb4_bin`) and therefore do not
    // override an explicit non-default operand collation.
    let collation = vals
        .iter()
        .filter_map(Datum::collation)
        .find(|collation| *collation != tidb_datatype::Collation::DEFAULT)
        .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION);
    find_in_set_with_collation_in(vals, collation, ctx)
}

/// Constant-list ownership for Go `getConstStrlistLookup`. Only NULL metadata
/// is visible here; TiKV owns the immutable keys and first-position scan.
#[derive(Clone, Debug)]
pub(crate) struct FindInSetLookup {
    pub(crate) is_null: bool,
    keys: crate::tikv::PreparedFindInSetKeys,
}

/// FIND follows the live collator mode, unlike explicit-collation STRCMP.
/// Binary is also the raw NoPad-key policy when new collations are disabled.
fn find_in_set_key_policy(collation: Collation) -> NativeCollation {
    tidb_datatype::get_collator(collation.name())
        .new_collation()
        .map_or(NativeCollation::Binary, Collation::native_policy)
}

/// Prepares only Go `getConstStrlistLookup`'s constant-list keys, not a SQL
/// answer. A successful context cache retains this build-time key snapshot;
/// lookup must not re-key a raw list under a later collator mode.
pub(crate) fn build_find_in_set_lookup(
    list: &Datum,
    collation: tidb_datatype::Collation,
) -> Result<FindInSetLookup, EvalError> {
    let list = coerce_str_bytes(list)?;
    let key_policy = if list.as_ref().is_some_and(|list| !list.is_empty()) {
        find_in_set_key_policy(collation)
    } else {
        // The original constructor did not fetch a collator for NULL/empty.
        // This unused selector is not a substituted SQL value.
        NativeCollation::Binary
    };
    let keys = crate::tikv::prepare_find_in_set_keys(list.as_deref(), key_policy)?;
    Ok(FindInSetLookup {
        is_null: keys.is_null(),
        keys,
    })
}

/// Probes an immutable constant-list owner through C4. The caller already
/// evaluated the needle expression before constructing or retrieving the cache.
pub(crate) fn find_in_set_lookup_in(
    needle: &Datum,
    lookup: &FindInSetLookup,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::FindInSetPreparedNative,
        ctx,
        || {
            let needle = if lookup.is_null {
                ReadyBytesArg::Undemanded
            } else {
                // An empty, non-NULL cache must still coerce the needle.
                ReadyBytesArg::Value(coerce_str_bytes(needle)?)
            };
            let collation = if matches!(&needle, ReadyBytesArg::Value(Some(_))) {
                find_in_set_key_policy(collation)
            } else {
                NativeCollation::Binary
            };
            Ok(EvaluatedArgs::FindInSetPreparedReady {
                needle,
                keys: lookup.keys.clone(),
                collation,
            })
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
pub(crate) fn find_in_set_lookup(
    needle: &Datum,
    lookup: &FindInSetLookup,
    collation: tidb_datatype::Collation,
) -> Result<Datum, EvalError> {
    find_in_set_lookup_in(needle, lookup, collation, &crate::NoColumns)
}

/// [`find_in_set`] under the collation derived over BOTH arguments (Go
/// `deriveCollation`'s `ast.FindInSet` arm). Go `findInSetByKey` compares
/// KeyWithoutTrimRightSpace, so even PAD SPACE collations distinguish 'a' from
/// 'a '. TiKV owns the same NoPad-key scan, not wire compare(false).
pub(crate) fn find_in_set_with_collation_in(
    vals: &[Datum],
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::FindInSetNative,
        ctx,
        || {
            // Tuple coercion demands the list even when the needle is NULL.
            let left = coerce_str_bytes(&vals[0])?;
            let right = coerce_str_bytes(&vals[1])?;
            let collation = if left.is_some() && right.as_ref().is_some_and(|list| !list.is_empty())
            {
                find_in_set_key_policy(collation)
            } else {
                // Preserve the old NULL/empty-list collator demand boundary.
                NativeCollation::Binary
            };
            Ok(EvaluatedArgs::CollatedBytes2 {
                left,
                right,
                collation,
            })
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `EXPORT_SET(bits, on, off[, separator[, number_of_bits]])`, ported from
/// `builtinExportSet{3,4,5}ArgSig` and `exportSet` in
/// `pkg/expression/builtin_string.go`.
///
/// `bits` and `number_of_bits` are both `EvalInt` arguments in Go, so every
/// argument domain that has an integer reading reaches them -- notably
/// `Datum::UInt`, which is what `1|4` produces here (`ops.rs`'s bitwise
/// operators return the unsigned domain, exactly as TiDB's do) and what
/// `CAST(5 AS UNSIGNED)` produces. `EvalInt` on an unsigned source keeps the
/// same 64 bits and reads them as `int64`, which is `crate::cast::to_i64_signed`.
/// Matching on `Datum::Int` alone made the idiomatic `EXPORT_SET(1|4, ...)`
/// spelling return NULL.
fn export_set_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::ExportSetNative,
        ctx,
        || {
            let bits = if vals[0].is_null() {
                None
            } else {
                Some(crate::cast::to_i64_signed(&vals[0]))
            };
            let mut on = ReadyBytesArg::Undemanded;
            let mut off = ReadyBytesArg::Undemanded;
            // Outer None is an omitted SQL operand. A provided operand remains
            // Undemanded only after a real NULL stops the original preparation.
            let mut separator = (vals.len() >= 4).then_some(ReadyBytesArg::Undemanded);
            let mut count = (vals.len() == 5).then_some(ReadyIntArg::Undemanded);
            if bits.is_some() {
                // Preserve the old tuple: on=NULL still demands off, including
                // off's strict UTF-8/coercion error, before either NULL wins.
                let (on_value, off_value) = (coerce_str(&vals[1])?, coerce_str(&vals[2])?);
                let demand_tail = on_value.is_some() && off_value.is_some();
                on = ReadyBytesArg::Value(on_value.map(String::into_bytes));
                off = ReadyBytesArg::Value(off_value.map(String::into_bytes));
                if demand_tail {
                    let demand_count = match separator.as_mut() {
                        Some(separator) => {
                            let value = coerce_str(&vals[3])?;
                            let non_null = value.is_some();
                            *separator = ReadyBytesArg::Value(value.map(String::into_bytes));
                            non_null
                        }
                        None => true,
                    };
                    if demand_count {
                        if let Some(count) = count.as_mut() {
                            *count = ReadyIntArg::Value(if vals[4].is_null() {
                                None
                            } else {
                                Some(crate::cast::to_i64_signed(&vals[4]))
                            });
                        }
                    }
                }
            }
            // Defaults, count clamping and the signed bit test belong to C.
            Ok(EvaluatedArgs::ExportSetReady(
                crate::tikv::prepare_export_set_args(
                    ReadyIntArg::Value(bits),
                    on,
                    off,
                    separator,
                    count,
                )?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{build_find_in_set_lookup, dispatch, find_in_set_lookup, locate3};
    use crate::coerce::coerce_str;
    use crate::string_fn::{char_func, format_num, format_num_locale, position, substring};
    use crate::string_packet::to_base64;
    use crate::Datum;
    use tidb_datatype::{Collation, MysqlEnum, MysqlSet};

    fn string(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals, &crate::NoColumns)
            .expect("string2 name/arity should dispatch")
            .expect("Go-derived vector should evaluate")
    }

    /// Both integer arguments are `EvalInt` in Go, so the UNSIGNED domain has
    /// to reach them. `1|4` is the idiomatic spelling of a bit mask and this
    /// crate's bitwise operators return `Datum::UInt`, exactly as TiDB's do;
    /// matching on `Datum::Int` alone answered NULL for it, and an unsigned
    /// `number_of_bits` was a hard error. Captured from TiDB:
    ///
    /// ```text
    /// select export_set(1|4,'Y','N',',',5);                  -> Y,N,Y,N,N
    /// select export_set(5,'Y','N',',',cast(5 as unsigned));  -> Y,N,Y,N,N
    /// ```
    #[test]
    fn export_set_reads_the_unsigned_integer_domain() {
        let want = Datum::new_string("Y,N,Y,N,N".to_string());
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::UInt(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5)
                ]
            ),
            want
        );
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::UInt(5)
                ]
            ),
            want
        );
        // A NULL in either integer position still short-circuits to NULL.
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Null,
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5)
                ]
            ),
            Datum::Null
        );
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Null
                ]
            ),
            Datum::Null
        );
    }

    /// `builtinTranslateUTF8Sig` (rune path): map, delete-past-`to`, first
    /// occurrence wins, multi-byte, and NULL propagation.
    #[test]
    fn translate_utf8_vectors() {
        let cases: &[(&str, &str, &str, &str)] = &[
            ("abcabc", "ab", "xy", "xycxyc"),
            ("hello", "lo", "L", "heLL"), // 'o' has no counterpart -> deleted
            ("中文测试", "中试", "XY", "X文测Y"), // rune-based multi-byte
            ("aaa", "aa", "xy", "xxx"),   // first occurrence of 'a' wins
            ("hello", "", "x", "hello"),  // empty from -> unchanged
            ("mississippi", "sp", "SP", "miSSiSSiPPi"),
        ];
        for (src, from, to, want) in cases {
            assert_eq!(
                call("TRANSLATE", &[string(src), string(from), string(to)]),
                string(want),
                "TRANSLATE({src:?}, {from:?}, {to:?})"
            );
        }
        assert_eq!(
            call("TRANSLATE", &[Datum::Null, string("a"), string("b")]),
            Datum::Null
        );
        assert_eq!(
            call("TRANSLATE", &[string("x"), Datum::Null, string("y")]),
            Datum::Null
        );
        assert_eq!(
            call("TRANSLATE", &[string("x"), string("y"), Datum::Null]),
            Datum::Null
        );
        assert!(dispatch("TRANSLATE", &[string("x"), string("y")], &crate::NoColumns).is_none());
    }

    #[test]
    fn go_string_vectors_cover_new_arities_and_functions() {
        assert_eq!(
            call("SUBSTRING", &[string("Sakila"), Datum::Int(-3)])
                .sql_string()
                .unwrap(),
            "ila"
        );
        assert_eq!(
            call("LOCATE", &[string("A"), string("大A写的A"), Datum::Int(3)])
                .sql_string()
                .unwrap(),
            "5"
        );
        assert_eq!(
            call(
                "FORMAT",
                &[string("1234.567"), Datum::Int(2), string("de_DE")]
            )
            .sql_string()
            .unwrap(),
            "1.234,57"
        );
        assert_eq!(
            call("FIND_IN_SET", &[string("b"), string("a,b,c")])
                .sql_string()
                .unwrap(),
            "2"
        );
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(-6),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5)
                ]
            )
            .sql_string()
            .unwrap(),
            "N,Y,N,Y,Y"
        );
    }

    #[test]
    fn find_in_set_adapter_preserves_null_empty_and_coercion() {
        let collation = Collation::Binary;
        let null = build_find_in_set_lookup(&Datum::Null, collation).unwrap();
        assert!(null.is_null);
        for needle in [Datum::Null, Datum::MinNotNull, string("x")] {
            assert_eq!(
                super::find_in_set_lookup_in(&needle, &null, collation, &crate::NoColumns).unwrap(),
                Datum::Null
            );
        }

        let empty = build_find_in_set_lookup(&string(""), collation).unwrap();
        assert!(!empty.is_null);
        assert_eq!(
            super::find_in_set_lookup_in(&string(""), &empty, collation, &crate::NoColumns)
                .unwrap(),
            Datum::Int(0)
        );
        assert_eq!(
            super::find_in_set_lookup_in(&Datum::Null, &empty, collation, &crate::NoColumns)
                .unwrap(),
            Datum::Null
        );
        let byte_error = crate::EvalError::Unsupported("range sentinel byte coercion");
        assert_eq!(
            super::find_in_set_lookup_in(&Datum::MinNotNull, &empty, collation, &crate::NoColumns,),
            Err(byte_error.clone())
        );
        assert_eq!(
            super::find_in_set_with_collation_in(
                &[Datum::Null, Datum::MinNotNull],
                collation,
                &crate::NoColumns,
            ),
            Err(byte_error.clone())
        );
        assert!(matches!(
            build_find_in_set_lookup(&Datum::MinNotNull, collation),
            Err(crate::EvalError::Unsupported(
                "range sentinel byte coercion"
            ))
        ));
        for (list, expected) in [("", 0), (",", 1), ("x,", 2)] {
            let list = string(list);
            let lookup = build_find_in_set_lookup(&list, collation).unwrap();
            assert_eq!(
                super::find_in_set_lookup_in(&string(""), &lookup, collation, &crate::NoColumns,)
                    .unwrap(),
                Datum::Int(expected)
            );
            assert_eq!(
                super::find_in_set_with_collation_in(
                    &[string(""), list],
                    collation,
                    &crate::NoColumns,
                )
                .unwrap(),
                Datum::Int(expected)
            );
        }
    }

    #[test]
    fn locate3_extension_adapter_preserves_units_wrapping_and_demand() {
        let text = |bytes: Vec<u8>| Datum::new_collation_string(bytes, Collation::Utf8Mb4Bin);
        for (values, expected) in [
            ([string(""), string("abc"), Datum::Int(i64::MIN)], 0),
            ([string(""), string("abc"), Datum::UInt(u64::MAX)], 0),
            ([string(""), string("中a"), Datum::Int(3)], 3),
            ([string(""), string("中a"), Datum::Int(4)], 0),
            ([string(""), string(""), Datum::Int(1)], 1),
            (
                [
                    Datum::new_collation_string("A", Collation::Utf8Mb4GeneralCi),
                    Datum::new_collation_string("a", Collation::Utf8Mb4GeneralCi),
                    Datum::Int(1),
                ],
                0,
            ),
            ([text(vec![0xe2]), text(vec![0xe2, 0x82]), Datum::Int(2)], 2),
            (
                [
                    Datum::Bytes(vec![0xe2]),
                    Datum::Bytes(vec![0xe2, 0x82]),
                    Datum::Int(2),
                ],
                0,
            ),
        ] {
            assert_eq!(
                super::locate3_in(&values, &crate::NoColumns).unwrap(),
                Datum::Int(expected),
                "{values:?}"
            );
        }
        assert_eq!(
            super::locate3_in(
                &[Datum::Null, string("abc"), Datum::Int(1)],
                &crate::NoColumns,
            )
            .unwrap(),
            Datum::Null
        );
        assert_eq!(
            super::locate3_in(
                &[string("a"), string("abc"), Datum::Null],
                &crate::NoColumns,
            )
            .unwrap(),
            Datum::Null
        );
        assert_eq!(
            super::locate3_in(
                &[Datum::Null, Datum::MinNotNull, Datum::Int(1)],
                &crate::NoColumns,
            ),
            Err(crate::EvalError::Unsupported(
                "range sentinel byte coercion"
            ))
        );
        assert_eq!(
            super::locate3_in(
                &[Datum::Null, Datum::Null, Datum::MinNotNull],
                &crate::NoColumns,
            ),
            Err(crate::EvalError::Unsupported(
                "un-cast types.ETInt argument"
            ))
        );
    }

    #[test]
    fn find_in_set_lookup_preserves_first_position_and_spaces() {
        let lookup =
            build_find_in_set_lookup(&string("  , , ,"), tidb_datatype::Collation::Binary).unwrap();
        assert_eq!(
            find_in_set_lookup(&string(" "), &lookup, tidb_datatype::Collation::Binary,).unwrap(),
            Datum::Int(2)
        );
        let empty =
            build_find_in_set_lookup(&string(""), tidb_datatype::Collation::Binary).unwrap();
        assert_eq!(
            find_in_set_lookup(&string(""), &empty, tidb_datatype::Collation::Binary,).unwrap(),
            Datum::Int(0)
        );
    }

    /// Complete representable rows from `TestChar` and `TestFindInSet` in
    /// `pkg/expression/builtin_string_test.go`.  Charset conversion warnings
    /// are session state, but the default binary CHAR signature and the
    /// source's exact `KeyWithoutTrimRightSpace` comparison are fully scalar
    /// here.
    #[test]
    fn char_and_find_in_set_match_go_source_vectors() {
        assert_eq!(
            char_func(&[
                string("65"),
                Datum::Int(16_740),
                Datum::Real(67.5),
                string("utf8"),
            ])
            .unwrap(),
            Datum::new_collation_string(b"AAdD".to_vec(), Collation::Utf8Bin)
        );
        assert_eq!(
            char_func(&[
                string("65"),
                Datum::Int(16_740),
                Datum::Real(67.5),
                Datum::Null,
            ])
            .unwrap(),
            Datum::new_bytes(b"AAdD".to_vec())
        );
        assert_eq!(
            char_func(&[string("a"), Datum::Int(-1), Datum::Real(67.5), Datum::Null]).unwrap(),
            Datum::new_bytes(vec![0, 0xff, 0xff, 0xff, 0xff, b'D'])
        );

        for (needle, list, want) in [
            ("foo", "foo,bar", 1),
            ("foo", "foobar,bar", 0),
            (" foo ", "foo, foo ", 2),
            ("", "foo,bar,", 3),
            ("", "", 0),
            ("1", "1", 1),
            ("a,b", "a,b,c", 0),
        ] {
            assert_eq!(
                call("FIND_IN_SET", &[string(needle), string(list)]),
                Datum::Int(want),
                "FIND_IN_SET({needle:?}, {list:?})"
            );
        }
        assert_eq!(
            call("FIND_IN_SET", &[Datum::Int(1), Datum::Int(1)]),
            Datum::Int(1)
        );
        assert_eq!(
            call("FIND_IN_SET", &[Datum::Int(1), string("1")]),
            Datum::Int(1)
        );
        assert_eq!(
            call("FIND_IN_SET", &[string("1"), Datum::Int(1)]),
            Datum::Int(1)
        );
        assert_eq!(
            call(
                "FIND_IN_SET",
                &[
                    Datum::new_enum(MysqlEnum::new([0xff], 1), Collation::Binary),
                    Datum::new_set(MysqlSet::new([0xfe, b',', 0xff], 3), Collation::Binary,),
                ],
            ),
            Datum::Int(2),
            "Go EvalString compares arbitrary enum/set name bytes"
        );
        for args in [
            vec![string("foo"), Datum::Null],
            vec![Datum::Null, string("bar")],
        ] {
            assert_eq!(call("FIND_IN_SET", &args), Datum::Null);
        }
    }

    /// Complete value-domain rows from `TestFormat`: numeric-prefix
    /// coercion, rounding, precision clamping, grouping, and NULL results.
    ///
    /// The result is `Datum::Bytes`, not a string, because
    /// `tidb_mysql::locale::format_by_locale` returns BYTES -- Go's
    /// `FormatByLocale` counts the integer part in bytes, and a grouping
    /// separator inserted every three BYTES can split a multi-byte rune, so
    /// there is no valid-UTF-8 carrier for its output. See
    /// `unknown_and_null_locales_warn_1649` for the locale half.
    #[allow(clippy::excessive_precision)]
    #[test]
    fn format_matches_go_source_vectors() {
        for (number, precision, want) in [
            (
                Datum::Real(12332.12341111111111111111111111111111111111111),
                Datum::Int(4),
                Some("12,332.1234"),
            ),
            (Datum::Null, Datum::Int(22), None),
            (Datum::Real(1.12345), Datum::Int(4), Some("1.1235")),
            (Datum::Real(9.99999), Datum::Int(4), Some("10.0000")),
            (Datum::Real(1.99999), Datum::Int(4), Some("2.0000")),
            (Datum::Real(1.09999), Datum::Int(4), Some("1.1000")),
            (Datum::Real(-2.5), Datum::Int(0), Some("-3")),
            (
                Datum::Real(12332.123444),
                Datum::Int(4),
                Some("12,332.1234"),
            ),
            (Datum::Real(12332.123444), Datum::Int(0), Some("12,332")),
            (Datum::Real(12332.123444), Datum::Int(-4), Some("12,332")),
            (
                Datum::Real(-12332.123444),
                Datum::Int(4),
                Some("-12,332.1234"),
            ),
            (Datum::Real(-12332.123444), Datum::Int(0), Some("-12,332")),
            (Datum::Real(-12332.123444), Datum::Int(-4), Some("-12,332")),
            (string("12332.123444"), string("4"), Some("12,332.1234")),
            (string("12332.123444A"), string("4"), Some("12,332.1234")),
            (string("-12332.123444"), string("4"), Some("-12,332.1234")),
            (string("-12332.123444A"), string("4"), Some("-12,332.1234")),
            (string("A123345"), string("4"), Some("0.0000")),
            (string("-A123345"), string("4"), Some("0.0000")),
            (string("-12332.123444"), string("A"), Some("-12,332")),
            (string("12332.123444"), string("A"), Some("12,332")),
            (string("-12332.123444"), string("4A"), Some("-12,332.1234")),
            (string("12332.123444"), string("4A"), Some("12,332.1234")),
            (string("-A12332.123444"), string("A"), Some("0")),
            (string("A12332.123444"), string("A"), Some("0")),
            (string("-A12332.123444"), string("4A"), Some("0.0000")),
            (string("A12332.123444"), string("4A"), Some("0.0000")),
            (string("-.12332.123444"), string("4A"), Some("-0.1233")),
            (string(".12332.123444"), string("4A"), Some("0.1233")),
            (
                string("12332.1234567890123456789012345678901"),
                Datum::Int(22),
                Some("12,332.1234567890110000000000"),
            ),
            (
                Datum::Int(1),
                Datum::Int(1024),
                Some("1.000000000000000000000000000000"),
            ),
            (string(""), Datum::Int(1), Some("0.0")),
            (Datum::Int(1), string(""), Some("1")),
        ] {
            let result = format_num(&[number, precision], &crate::NoColumns).unwrap();
            let expected = want
                .map(|text| Datum::new_bytes(text.as_bytes().to_vec()))
                .unwrap_or(Datum::Null);
            assert_eq!(result, expected);
        }
        assert_eq!(
            call(
                "FORMAT",
                &[string("-12332.123456"), Datum::Int(-4), string("zh_CN")]
            ),
            Datum::new_bytes(b"-12,332".to_vec())
        );
        assert_eq!(
            call(
                "FORMAT",
                &[string("-12332.123456"), string("4"), string("de_GE")]
            ),
            Datum::new_bytes(b"-12,332.1235".to_vec())
        );
        assert_eq!(
            call("FORMAT", &[Datum::Int(1), Datum::Int(4), Datum::Null]),
            Datum::new_bytes(b"1.0000".to_vec())
        );
    }

    /// Go `builtinFormatWithLocaleSig.evalString`
    /// (`pkg/expression/builtin_string.go:3685-3705`): the `found` flag
    /// `mysql.FormatByLocale` returns raises `errUnknownLocale` (1649), with
    /// a NULL locale warned FIRST under the literal text `NULL` and then
    /// falling back to `en_US`.
    ///
    /// The live path used a second, less faithful locale table with no
    /// `found` flag, so no locale could ever be reported unknown. The
    /// grouping answers below did already agree; the warnings did not exist.
    ///
    /// Captured with `gorunmsg` on `FORMAT(1234567.891, 2, <locale>)`.
    #[test]
    fn unknown_and_null_locales_warn_1649() {
        use crate::context::Columns;

        #[derive(Default)]
        struct Sink(std::cell::RefCell<Vec<(u16, String)>>);
        impl Columns for Sink {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.0.borrow_mut().push((code, message.to_owned()));
            }
        }

        let number = Datum::Real(1_234_567.891);
        let two = Datum::Int(2);
        for (locale, text, warnings) in [
            // Recognized: the answer differs per locale and nothing warns.
            (Some("de_DE"), "1.234.567,89", vec![]),
            (Some("en_IN"), "12,34,567.89", vec![]),
            (Some("de_CH"), "1'234'567.89", vec![]),
            // Case-insensitive, so this is Russia's space/comma style.
            (Some("RU_ru"), "1 234 567,89", vec![]),
            (Some("ar_SA"), "1234567.89", vec![]),
            (
                Some("not_REAL"),
                "1,234,567.89",
                vec![(1649_u16, "Unknown locale: 'not_REAL'".to_owned())],
            ),
            (
                None,
                "1,234,567.89",
                vec![(1649_u16, "Unknown locale: 'NULL'".to_owned())],
            ),
        ] {
            let sink = Sink::default();
            let result = format_num_locale(&[number.clone(), two.clone()], locale, &sink).unwrap();
            assert_eq!(
                result,
                Datum::new_bytes(text.as_bytes().to_vec()),
                "{locale:?}"
            );
            assert_eq!(sink.0.into_inner(), warnings, "{locale:?}");
        }

        // The TWO-argument form is `builtinFormatSig`, which discards `found`
        // and never warns even though it passes the same `en_US`.
        let sink = Sink::default();
        format_num(&[number.clone(), two.clone()], &sink).unwrap();
        assert!(sink.0.into_inner().is_empty());

        // Through the DISPATCHER, so the three-argument arm's own NULL/found
        // handling is covered rather than only the shared evaluator's: a NULL
        // locale is a distinct state from the string "en_US", not a default
        // the arm can collapse into one.
        for (locale, warnings) in [
            (
                Datum::Null,
                vec![(1649_u16, "Unknown locale: 'NULL'".to_owned())],
            ),
            (
                Datum::new_string("nope_XX".to_owned()),
                vec![(1649_u16, "Unknown locale: 'nope_XX'".to_owned())],
            ),
            (Datum::new_string("en_US".to_owned()), vec![]),
        ] {
            let sink = Sink::default();
            dispatch(
                "FORMAT",
                &[number.clone(), two.clone(), locale.clone()],
                &sink,
            )
            .expect("FORMAT/3 dispatches")
            .expect("FORMAT/3 evaluates");
            assert_eq!(sink.0.into_inner(), warnings, "{locale:?}");
        }
    }

    #[test]
    fn export_set_extension_adapter_preserves_nullable_prefix_and_strict_errors() {
        use crate::EvalError;

        let evaluate = |args: &[Datum]| {
            dispatch("EXPORT_SET", args, &crate::NoColumns).expect("EXPORT_SET extension arity")
        };
        let five = |on, off, separator, count| vec![Datum::Int(1), on, off, separator, count];
        // A real NULL witnesses each skipped suffix; later bad values are not
        // evaluated, but an on=NULL still demands off as part of the tuple.
        for args in [
            vec![Datum::Null, Datum::new_bytes([0xff]), Datum::MaxValue],
            vec![
                Datum::Null,
                Datum::new_bytes([0xff]),
                Datum::MaxValue,
                Datum::new_bytes([0xff]),
                Datum::MaxValue,
            ],
            five(
                Datum::Null,
                string("N"),
                Datum::new_bytes([0xff]),
                Datum::MaxValue,
            ),
            five(
                string("Y"),
                Datum::Null,
                Datum::new_bytes([0xff]),
                Datum::MaxValue,
            ),
            five(string("Y"), string("N"), Datum::Null, Datum::MaxValue),
            five(string("Y"), string("N"), string(","), Datum::Null),
            vec![Datum::Int(1), string("Y"), string("N"), Datum::Null],
        ] {
            assert_eq!(evaluate(&args), Ok(Datum::Null), "{args:?}");
        }

        for args in [
            vec![Datum::Int(1), Datum::Null, Datum::new_bytes([0xff])],
            vec![Datum::Int(1), Datum::new_bytes([0xff]), Datum::Null],
            five(
                Datum::new_bytes([0xff]),
                string("N"),
                string(","),
                Datum::Null,
            ),
            five(
                string("Y"),
                string("N"),
                Datum::new_bytes([0xff]),
                Datum::Null,
            ),
            five(
                Datum::new_bytes([0xff]),
                string("N"),
                string(","),
                Datum::Int(0),
            ),
            five(
                string("Y"),
                string("N"),
                Datum::new_bytes([0xff]),
                Datum::Int(0),
            ),
        ] {
            assert_eq!(
                evaluate(&args),
                Err(EvalError::Unsupported("invalid UTF-8 byte datum")),
                "{args:?}"
            );
        }
        for args in [
            vec![Datum::Int(1), Datum::Null, Datum::MaxValue],
            vec![Datum::Int(1), Datum::MaxValue, Datum::new_bytes([0xff])],
        ] {
            assert_eq!(
                evaluate(&args),
                Err(EvalError::Unsupported("range sentinel string coercion")),
                "{args:?}"
            );
        }
    }

    #[test]
    fn export_set_extension_adapter_preserves_signed_bits_defaults_and_quiet_context() {
        struct QuietColumns;
        impl crate::Columns for QuietColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn max_allowed_packet(&self) -> u64 {
                panic!("EXPORT_SET extension has no packet getter")
            }

            fn append_warning(&self, _: u16, _: &str) {
                panic!("EXPORT_SET extension has no warning policy")
            }
        }
        let columns = QuietColumns;
        let evaluate = |args: &[Datum]| {
            dispatch("EXPORT_SET", args, &columns).expect("EXPORT_SET extension arity")
        };
        assert_eq!(
            evaluate(&[
                Datum::UInt(u64::MAX),
                string("1"),
                string("0"),
                string(""),
                Datum::Int(64),
            ]),
            Ok(string(&format!("{}0", "1".repeat(63))))
        );
        assert_eq!(
            evaluate(&[Datum::Int(i64::MIN), string("1"), string("0")]),
            Ok(string(&vec!["0"; 64].join(",")))
        );
        assert_eq!(
            evaluate(&[Datum::Int(i64::MIN), string("1"), string("0"), string(":")]),
            Ok(string(&vec!["0"; 64].join(":")))
        );
        for count in [Datum::Int(-1), Datum::Int(65), Datum::UInt(u64::MAX)] {
            assert_eq!(
                evaluate(&[Datum::Int(1), string("1"), string("0"), string(""), count]),
                Ok(string(&format!("1{}", "0".repeat(63))))
            );
        }
        for (bits, count, expected) in [
            (Datum::Int(1), Datum::Int(0), ""),
            (Datum::Int(1), Datum::Real(2.5), "10"),
            (
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5")),
                "100",
            ),
            (Datum::Real(2.5), Datum::Int(3), "010"),
            (
                Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5")),
                Datum::Int(3),
                "110",
            ),
            (Datum::new_bytes(b"5tail"), Datum::Int(3), "101"),
            (Datum::new_bytes([0xff]), Datum::Int(3), "000"),
        ] {
            assert_eq!(
                evaluate(&[bits, string("1"), string("0"), string(""), count]),
                Ok(string(expected))
            );
        }
        assert_eq!(
            evaluate(&[Datum::Null, Datum::MaxValue, Datum::MaxValue]),
            Ok(Datum::Null)
        );
    }

    /// Complete scalar rows from `TestExportSet`.  The Go implementation's
    /// warning/session plumbing is absent, while bit ordering, default and
    /// explicit separators, zero-width output, and out-of-range bit counts
    /// are all value-domain behavior.
    #[test]
    fn export_set_matches_go_source_vectors() {
        let cases = [
            (
                vec![
                    Datum::Int(-9_223_372_036_854_775_807),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5),
                ],
                "Y,N,N,N,N",
            ),
            (
                vec![
                    Datum::Int(-6),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5),
                ],
                "N,Y,N,Y,Y",
            ),
            (
                vec![
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(4),
                ],
                "Y,N,Y,N",
            ),
            (
                vec![
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(0),
                ],
                "",
            ),
            (
                vec![
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(1),
                ],
                "Y",
            ),
            (
                vec![
                    Datum::Int(6),
                    string("1"),
                    string("0"),
                    string(","),
                    Datum::Int(10),
                ],
                "0,1,1,0,0,0,0,0,0,0",
            ),
            (
                vec![
                    Datum::Int(333_333),
                    string("Ysss"),
                    string("sN"),
                    string("---"),
                    Datum::Int(9),
                ],
                "Ysss---sN---Ysss---sN---Ysss---sN---sN---sN---sN",
            ),
        ];
        for (args, want) in cases {
            assert_eq!(call("EXPORT_SET", &args), string(want));
        }
        assert_eq!(
            call("EXPORT_SET", &[Datum::Int(7), string("Y"), string("N")])
                .sql_string()
                .unwrap()
                .split(',')
                .count(),
            64
        );
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(7),
                    string("Y"),
                    string("N"),
                    Datum::Int(6),
                    Datum::Int(4),
                ]
            ),
            string("Y6Y6Y6N")
        );
        // Go clamps a five-argument bit count above 64 back to the default.
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(7),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(133),
                ]
            )
            .sql_string()
            .unwrap()
            .split(',')
            .count(),
            64
        );
    }

    /// Complete value-domain rows from `TestToBase64`, including Go's 76
    /// column wrapping and numeric/NULL coercion.  Charset conversion tests
    /// are represented by passing their resulting GBK bytes directly, which
    /// is exactly the byte boundary consumed by `EvalString`.
    #[test]
    fn to_base64_matches_go_source_vectors() {
        for (input, want) in [
            (string(""), ""),
            (string("abc"), "YWJj"),
            (string("ab c"), "YWIgYw=="),
            (Datum::Int(1), "MQ=="),
            (Datum::Real(1.1), "MS4x"),
            (string("ab\nc"), "YWIKYw=="),
            (string("ab\tc"), "YWIJYw=="),
            (string("qwerty123456"), "cXdlcnR5MTIzNDU2"),
            (string("一二三"), "5LiA5LqM5LiJ"),
        ] {
            assert_eq!(
                to_base64(&[input], &crate::NoColumns).unwrap(),
                string(want)
            );
        }
        assert_eq!(
            to_base64(&[Datum::Null], &crate::NoColumns).unwrap(),
            Datum::Null
        );
        assert_eq!(
            to_base64(
                &[Datum::new_bytes(vec![0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd])],
                &crate::NoColumns
            )
            .unwrap(),
            string("0ru2/sj9")
        );
        assert_eq!(
            to_base64(&[Datum::new_bytes(vec![0xff, 0x00])], &crate::NoColumns).unwrap(),
            string("/wA=")
        );
        let long = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        assert_eq!(
            to_base64(&[string(long)], &crate::NoColumns).unwrap(),
            string(
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw=="
            )
        );
        let triple = format!("{long}{long}{long}");
        assert_eq!(
            to_base64(&[string(&triple)], &crate::NoColumns).unwrap(),
            string("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv")
        );
    }

    /// Complete representable scalar tables from `TestSubstring` and
    /// `TestLocate` in `pkg/expression/builtin_string_test.go`.  These tests
    /// deliberately call the same value-domain helpers used by the public
    /// dispatcher: the Go source's injected error datum, session-selected
    /// collations, and vector warning plumbing are not values this seed
    /// evaluator can carry, but every ordinary UTF-8 and valid binary-literal
    /// row is still asserted here.
    #[test]
    fn substring_and_locate_match_go_source_vectors() {
        for (input, pos, want) in [
            ("Quadratically", 5, "ratically"),
            ("Sakila", 1, "Sakila"),
            ("Sakila", 2, "akila"),
            ("Sakila", -3, "ila"),
            ("Sakila", 0, ""),
            ("Sakila", 100, ""),
            ("Sakila", -100, ""),
        ] {
            assert_eq!(
                substring(&[string(input), Datum::Int(pos)], &crate::NoColumns).unwrap(),
                string(want),
                "SUBSTRING({input:?}, {pos})"
            );
        }
        for (input, pos, length, want) in [
            ("Quadratically", 5, 6, "ratica"),
            ("Sakila", -5, 3, "aki"),
            ("Sakila", 2, 0, ""),
            ("Sakila", 2, -1, ""),
            ("Sakila", 2, 100, "akila"),
        ] {
            assert_eq!(
                substring(
                    &[string(input), Datum::Int(pos), Datum::Int(length),],
                    &crate::NoColumns
                )
                .unwrap(),
                string(want),
                "SUBSTRING({input:?}, {pos}, {length})"
            );
        }
        for args in [
            vec![Datum::Null, Datum::Int(2), Datum::Int(3)],
            vec![string("Sakila"), Datum::Null, Datum::Int(3)],
            vec![string("Sakila"), Datum::Int(2), Datum::Null],
        ] {
            assert_eq!(substring(&args, &crate::NoColumns).unwrap(), Datum::Null);
        }

        // LOCATE(substr, str) is the two-argument form.  The source's
        // binary rows contain valid UTF-8 bytes, so they exercise the same
        // exact byte/rune result in this value domain without inventing a
        // session collation object.
        let locate2 = [
            ("bar", "foobarbar", 4),
            ("xbar", "foobar", 0),
            ("", "foobar", 1),
            ("foobar", "", 0),
            ("", "", 1),
            ("好世", "你好世界", 2),
            ("界面", "你好世界", 0),
            ("b", "中a英b文", 4),
            ("bAr", "foobArbar", 4),
        ];
        for (needle, haystack, want) in locate2 {
            assert_eq!(
                position(Some(needle.to_string()), Some(haystack.to_string())),
                Datum::Int(want),
                "LOCATE({needle:?}, {haystack:?})"
            );
        }
        for (needle, haystack) in [
            (None, Some("foobar".to_string())),
            (Some("bar".to_string()), None),
            (None, None),
            (Some("".to_string()), None),
            (None, Some("".to_string())),
        ] {
            assert_eq!(position(needle, haystack), Datum::Null);
        }

        let locate3_cases = [
            ("bar", "foobarbar", 5, 7),
            ("xbar", "foobar", 1, 0),
            ("", "foobar", 2, 2),
            ("foobar", "", 1, 0),
            ("", "", 2, 0),
            ("A", "大A写的A", 0, 0),
            ("A", "大A写的A", 1, 2),
            ("A", "大A写的A", 2, 2),
            ("A", "大A写的A", 3, 5),
            ("BaR", "foobarBaR", 5, 7),
            ("foo", "foobar", -1, 0),
        ];
        for (needle, haystack, start, want) in locate3_cases {
            assert_eq!(
                locate3(&[string(needle), string(haystack), Datum::Int(start)]).unwrap(),
                Datum::Int(want),
                "LOCATE({needle:?}, {haystack:?}, {start})"
            );
        }
        for args in [
            vec![Datum::Null, Datum::Null, Datum::Int(1)],
            vec![string(""), Datum::Null, Datum::Int(1)],
            vec![Datum::Null, string(""), Datum::Int(1)],
            vec![Datum::Null, string("bar"), Datum::Int(0)],
            vec![string("bar"), Datum::Null, Datum::Int(-1)],
        ] {
            assert_eq!(locate3(&args).unwrap(), Datum::Null);
        }

        let binary = [
            (Datum::new_bytes(b"BaR".to_vec()), string("foobArbar"), 0),
            (string("BaR"), Datum::new_bytes(b"foobArbar".to_vec()), 0),
            (Datum::new_bytes(b"bAr".to_vec()), string("foobarBaR"), 0),
            (string("bAr"), Datum::new_bytes(b"foobarBaR".to_vec()), 0),
            (string("bAr"), Datum::new_bytes(b"foobarbAr".to_vec()), 7),
        ];
        for (needle, haystack, want) in binary {
            assert_eq!(
                position(coerce_str(&needle).unwrap(), coerce_str(&haystack).unwrap(),),
                Datum::Int(want)
            );
        }
    }

    #[test]
    fn substring_positive_length_overflow_matches_go_int64_boundary() {
        // `builtinSubstring3ArgsUTF8Sig` computes `end := pos + length` as
        // int64 and returns an empty string when that addition wraps.  This
        // catches the tempting-but-wrong Rust `saturating_add` translation.
        assert_eq!(
            substring(
                &[string("Sakila"), Datum::Int(2), Datum::Int(i64::MAX),],
                &crate::NoColumns
            )
            .unwrap(),
            string("")
        );
        // At position one the same maximum length does not overflow and
        // therefore returns the complete string.
        assert_eq!(
            substring(
                &[string("Sakila"), Datum::Int(1), Datum::Int(i64::MAX),],
                &crate::NoColumns
            )
            .unwrap(),
            string("Sakila")
        );
    }

    #[test]
    fn ltrim_and_rtrim_preserve_non_space_whitespace() {
        // Exact vectors from TestLTrim/TestRTrim in
        // pkg/expression/builtin_string_test.go. These are intentionally not
        // `str::trim*`: Go's `spaceChars` is the one-byte ASCII space.
        let cases = [
            ("LTRIM", "   bar   ", "bar   "),
            ("LTRIM", "\t   bar   ", "\t   bar   "),
            ("LTRIM", "   \rbar   ", "\rbar   "),
            ("LTRIM", "   \nbar   ", "\nbar   "),
            ("RTRIM", "   bar   ", "   bar"),
            ("RTRIM", "bar     \n", "bar     \n"),
            ("RTRIM", "bar\n     ", "bar\n"),
            ("RTRIM", "bar     \t", "bar     \t"),
            ("RTRIM", "bar\t     ", "bar\t"),
        ];
        for (name, input, want) in cases {
            assert_eq!(
                call(name, &[string(input)]),
                string(want),
                "{name}({input:?})"
            );
        }
        for name in ["LTRIM", "RTRIM"] {
            assert_eq!(call(name, &[Datum::Null]), Datum::Null);
            // Go declares this argument `types.ETString`, so an INTEGER one
            // is already rendered by the time the signature body runs --
            // `crate::arg_eval_type::wrap_string_args` does it, not this
            // body. Handing the body an un-cast integer is a refusal now,
            // which is the layer's contract; the wrap is what makes the
            // Go-derived answer come back.
            let cast = crate::arg_eval_type::wrap_string_args(
                name,
                vec![Datum::Int(123)],
                &[],
                &crate::NoColumns,
            )
            .unwrap();
            assert_eq!(call(name, &cast), string("123"));
            assert!(dispatch(name, &[], &crate::NoColumns).is_none());
        }
    }

    #[test]
    fn to_base64_wraps_at_the_go_76_column_boundary() {
        let input = string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");
        assert_eq!(
            to_base64(&[input], &crate::NoColumns).unwrap().sql_string().unwrap(),
            "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw=="
        );
    }

    #[test]
    fn char_preserves_go_four_byte_signed_integer_encoding() {
        assert_eq!(
            char_func(&[
                Datum::Int(65),
                Datum::Int(16_740),
                Datum::Int(67),
                Datum::Null
            ])
            .unwrap()
            .sql_string()
            .unwrap(),
            "AAdC"
        );
        assert_eq!(
            char_func(&[Datum::Int(-1), Datum::Null]).unwrap(),
            Datum::new_bytes(vec![0xff; 4])
        );
    }
}
