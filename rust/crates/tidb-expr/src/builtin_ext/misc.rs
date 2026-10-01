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

//! Miscellaneous scalar builtins. This is a separate family so unrelated
//! expression workers do not have to edit the central dispatcher.

use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::coerce::coerce_str;
use crate::{Datum, EvalError};
use tidb_query_expr::{
    format_uuid_native as format_uuid, NATIVE_UUID_EPOCH_100NS as UUID_EPOCH_100NS,
};
#[cfg(test)]
use tidb_util::vitess::hash_uint64;

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
#[cfg(test)]
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    dispatch_in(name, vals, &crate::NoColumns)
}

/// Dispatches this family while preserving warnings from implicit casts.
pub(crate) fn dispatch_in(
    name: &str,
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Option<Result<Datum, EvalError>> {
    match (name, vals) {
        ("UUID", []) => Some(uuid_v1()),
        ("UUID_V4", []) => Some(uuid_v4()),
        ("UUID_V7", []) => Some(uuid_v7()),
        ("ANY_VALUE", [value]) => Some(Ok(value.clone())),
        // Go's nameConstFunctionClass selects a typed signature from the
        // second argument and every builtinNameConst*Sig evaluator returns
        // that argument directly.  The first argument is column-label
        // metadata, not part of the scalar value.  This value-only leaf can
        // therefore preserve every representable Datum without converting
        // it through a text or numeric signature.  ETDatetime, ETDuration,
        // ETJson, and ETVectorFloat32 remain explicit boundaries because the
        // seed Datum domain intentionally has no corresponding variants.
        ("NAME_CONST", [_, value]) => Some(Ok(value.clone())),
        ("IS_UUID", [value]) => Some(is_uuid(value, ctx)),
        ("UUID_VERSION", [value]) => Some(uuid_version(value, ctx)),
        ("UUID_TIMESTAMP", [value]) => Some(uuid_timestamp(value, ctx)),
        ("UUID_TO_BIN", [value]) => Some(uuid_to_bin(value, None, ctx)),
        ("UUID_TO_BIN", [value, flag]) => Some(uuid_to_bin(value, Some(flag), ctx)),
        ("BIN_TO_UUID", [value]) => Some(bin_to_uuid(ctx, value, None)),
        ("BIN_TO_UUID", [value, flag]) => Some(bin_to_uuid(ctx, value, Some(flag))),
        ("TIDB_SHARD", [value]) => Some(tidb_shard(value, ctx)),
        ("TIDB_DECODE_KEY", [value]) => Some(tidb_decode_key(value, ctx)),
        ("VITESS_HASH", [value]) => Some(vitess_hash(value, ctx)),
        _ => None,
    }
}

fn tidb_decode_key(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let input = value
        .sql_bytes()
        .map_err(|_| EvalError::IncorrectArguments("tidb_decode_key".to_owned()))?;
    Ok(Datum::new_string(ctx.tidb_decode_key(&input)))
}

#[derive(Default)]
struct UuidClock {
    last_v1: u64,
    clock_sequence: u16,
    node: [u8; 6],
    initialized: bool,
    last_v7: u64,
}

fn uuid_clock() -> &'static Mutex<UuidClock> {
    static CLOCK: OnceLock<Mutex<UuidClock>> = OnceLock::new();
    CLOCK.get_or_init(|| Mutex::new(UuidClock::default()))
}

fn unix_nanos() -> Result<u128, EvalError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|_| EvalError::Unsupported("system clock is before the Unix epoch"))
}

fn random_bytes(bytes: &mut [u8]) -> Result<(), EvalError> {
    getrandom::fill(bytes).map_err(|_| EvalError::Unsupported("OS random source unavailable"))
}

/// `UUID()`: RFC 9562 version 1, following the clock/sequence layout used by
/// TiDB's pinned `google/uuid.NewUUID`. The node identifier uses that
/// library's process-random fallback instead of exposing a hardware address;
/// it remains process-stable, and the 14-bit clock sequence advances whenever
/// the clock does not advance.
fn uuid_v1() -> Result<Datum, EvalError> {
    let unix_100ns = u64::try_from(unix_nanos()? / 100)
        .map_err(|_| EvalError::Unsupported("system clock exceeds the UUID time domain"))?;
    let timestamp =
        unix_100ns
            .checked_add(UUID_EPOCH_100NS as u64)
            .ok_or(EvalError::Unsupported(
                "system clock exceeds the UUID time domain",
            ))?;
    let mut state = uuid_clock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !state.initialized {
        let mut seed = [0_u8; 8];
        random_bytes(&mut seed)?;
        state.clock_sequence = (u16::from_be_bytes([seed[0], seed[1]]) & 0x3fff) | 0x8000;
        state.node.copy_from_slice(&seed[2..]);
        state.initialized = true;
    }
    if timestamp <= state.last_v1 {
        state.clock_sequence = ((state.clock_sequence + 1) & 0x3fff) | 0x8000;
    }
    state.last_v1 = timestamp;

    let mut uuid = [0_u8; 16];
    uuid[..4].copy_from_slice(&(timestamp as u32).to_be_bytes());
    uuid[4..6].copy_from_slice(&((timestamp >> 32) as u16).to_be_bytes());
    uuid[6..8].copy_from_slice(&(((timestamp >> 48) as u16 & 0x0fff) | 0x1000).to_be_bytes());
    uuid[8..10].copy_from_slice(&state.clock_sequence.to_be_bytes());
    uuid[10..].copy_from_slice(&state.node);
    Ok(Datum::new_string(format_uuid(&uuid)))
}

/// `UUID_V4()`: 122 random bits with the RFC version and variant bits.
fn uuid_v4() -> Result<Datum, EvalError> {
    let mut uuid = [0_u8; 16];
    random_bytes(&mut uuid)?;
    uuid[6] = (uuid[6] & 0x0f) | 0x40;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    Ok(Datum::new_string(format_uuid(&uuid)))
}

/// `UUID_V7()`: the Unix-millisecond prefix and sub-millisecond sequence
/// used by TiDB's pinned `google/uuid.NewV7`, plus random tail bits.
fn uuid_v7() -> Result<Datum, EvalError> {
    let mut uuid = [0_u8; 16];
    random_bytes(&mut uuid)?;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;

    let nanos = unix_nanos()?;
    let millis = u64::try_from(nanos / 1_000_000)
        .map_err(|_| EvalError::Unsupported("system clock exceeds the UUID time domain"))?;
    let sub_millis = u64::try_from((nanos % 1_000_000) >> 8)
        .map_err(|_| EvalError::Unsupported("system clock exceeds the UUID time domain"))?;
    let mut state = uuid_clock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut combined = millis
        .checked_mul(1 << 12)
        .and_then(|value| value.checked_add(sub_millis))
        .ok_or(EvalError::Unsupported(
            "system clock exceeds the UUID time domain",
        ))?;
    if combined <= state.last_v7 {
        combined = state.last_v7.checked_add(1).ok_or(EvalError::Unsupported(
            "system clock exceeds the UUID time domain",
        ))?;
    }
    state.last_v7 = combined;
    let millis = combined >> 12;
    let sequence = combined & 0x0fff;

    let time = millis.to_be_bytes();
    uuid[..6].copy_from_slice(&time[2..]);
    uuid[6] = 0x70 | (sequence >> 8) as u8;
    uuid[7] = sequence as u8;
    Ok(Datum::new_string(format_uuid(&uuid)))
}

/// `UUID_TO_BIN(string_uuid, swap_flag)`, ported from
/// `builtinUUIDToBinSig.evalString` in `pkg/expression/builtin_miscellaneous.go`.
/// The Go signature is binary `ETString`: successful output is therefore raw
/// sixteen-byte data, not UTF-8 text.  `StringDatum` and `Bytes` both retain
/// those bytes; numeric values use the source's `ETString` coercion.  The
/// strict whitespace check runs before parsing inside the worker, because
/// MySQL rejects surrounding spaces although `google/uuid.Parse` accepts the
/// inner spelling.
fn uuid_to_bin(
    value: &Datum,
    flag: Option<&Datum>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    // The first complete call returns actual parsed UUID bytes, not a marker.
    // Invalid text and NULL skip flag coercion. Admission therefore precedes
    // the as-yet-undemanded flag; success pays for two leases/result transport
    // and two one-shot workers when no context capability is available.
    let parsed = crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::UuidToBinParseNative,
        ctx,
        || Ok(crate::tikv::EvaluatedArgs::Bytes(eval_string_bytes(value)?)),
        crate::tikv::EvaluatedBytesResult::into_bytes,
    )?;
    let Some(parsed) = parsed else {
        return Ok(Datum::Null);
    };
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::UuidToBinSwapNative,
        ctx,
        || {
            // Missing and actual NULL flags retain their original quiet zero.
            let flag = eval_int_flag(flag);
            Ok(crate::tikv::EvaluatedArgs::BytesInt(
                Some(parsed),
                Some(flag),
            ))
        },
        |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::Bytes)),
    )
}

/// `BIN_TO_UUID(binary_uuid, swap_flag)`, ported from
/// `builtinBinToUUIDSig.evalString` in the same Go source.  The first
/// argument is consumed as raw string bytes and must be exactly one UUID
/// payload (16 bytes); unlike `coerce_str`, this path deliberately accepts
/// arbitrary non-UTF-8 bytes so binary UUID data cannot be corrupted by a
/// text conversion.
fn bin_to_uuid(
    ctx: &dyn crate::Columns,
    value: &Datum,
    flag: Option<&Datum>,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::BinToUuidNative,
        ctx,
        || {
            // Preserve the original flag warning/cast before even a NULL
            // payload. The worker owns length validation and its 1411 cause.
            let flag_int = match flag {
                Some(flag @ (Datum::String(_) | Datum::Bytes(_))) => {
                    crate::cast::report_int_truncation(flag, ctx)?;
                    crate::cast::to_i64_signed(flag)
                }
                Some(flag) if !matches!(flag, Datum::Null) => crate::cast::to_i64_signed(flag),
                _ => 0,
            };
            let input = eval_string_bytes(value)?;
            Ok(crate::tikv::EvaluatedArgs::BytesInt(input, Some(flag_int)))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// The Go `EvalString` payload boundary used by UUID_TO_BIN/BIN_TO_UUID.
/// String/bytes datums are returned byte-for-byte; scalar numeric values are
/// stringified. Callers retain their distinct optional-flag demand order.
fn eval_string_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    crate::coerce::coerce_str_bytes(value)
}

fn eval_int_flag(flag: Option<&Datum>) -> i64 {
    flag.filter(|value| !matches!(value, Datum::Null))
        .map(crate::cast::to_i64_signed)
        .unwrap_or(0)
}

/// `TIDB_SHARD(value)`, ported from `builtinTidbShardSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// TiDB first casts the one argument to signed `ETInt`, then Vitess-hashes its
/// two's-complement `uint64` bits and takes the big-endian ciphertext's low
/// byte. The bucket count is 256, so the low byte is exactly the modulo.
fn tidb_shard(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::TidbShardNative,
        ctx,
        || {
            // Preserve the original ETInt prefix conversion and warnings.
            let value = if matches!(value, Datum::Null) {
                None
            } else {
                Some(crate::cast::to_i64_signed_with_warnings(value, ctx)?)
            };
            Ok(crate::tikv::EvaluatedArgs::Int(value))
        },
        crate::tikv::EvaluatedBytesResult::into_uint_bits_datum,
    )
}

/// `VITESS_HASH(shard_key)`, ported from `builtinVitessHashSig.evalInt`. Like
/// `TIDB_SHARD`, the ETInt-coerced argument is Vitess-hashed, but the whole
/// 64-bit digest is returned. The result column is UNSIGNED, so it is a
/// `Datum::UInt`.
fn vitess_hash(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::VitessHashNative,
        ctx,
        || {
            let value = if matches!(value, Datum::Null) {
                None
            } else {
                Some(crate::cast::to_i64_signed_with_warnings(value, ctx)?)
            };
            Ok(crate::tikv::EvaluatedArgs::Int(value))
        },
        // The signed carrier contains all 64 result bits, not a SQL flag.
        crate::tikv::EvaluatedBytesResult::into_uint_bits_datum,
    )
}

/// `IS_UUID(value)`, ported from `builtinIsUUIDSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// The Go implementation deliberately delegates validation to
/// `github.com/google/uuid.Parse`, after rejecting leading/trailing
/// whitespace. That parser accepts RFC UUID text, raw 32-hex text, URNs,
/// and its 38-byte "Microsoft style" form where only the *middle* 36 bytes
/// are examined. Keep that last behavior: it is explicitly covered by
/// TiDB's `TestIsUUID` and is why this is not a canonical-format validator.
fn is_uuid(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::IsUuidNative,
        ctx,
        || eval_string_bytes(value),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `UUID_VERSION(value)`, ported from `builtinUUIDVersionSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
///
/// TiDB invokes `github.com/google/uuid.Parse` over an `ETString` argument,
/// then returns the high nibble of UUID byte 6. Preserve the original strict
/// UTF-8 string coercion before admission; the worker parses all accepted UUID
/// spellings. Its typed cause retains the original Unsupported diagnostic for
/// malformed UUIDs rather than changing this frontend's policy to error 1411.
fn uuid_version(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::UuidVersionNative,
        ctx,
        || Ok(coerce_str(value)?.map(String::into_bytes)),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `UUID_TIMESTAMP(value)`, ported from `builtinUUIDTimestampSig.evalDecimal`
/// in `pkg/expression/builtin_miscellaneous.go`.
///
/// TiDB accepts the UUID spellings parsed by `google/uuid.Parse`, returns
/// `NULL` for a valid UUID that does not carry a timestamp, and renders the
/// Version 1, 6, and 7 time as an exact `DECIMAL(18,6)`. The worker owns the
/// `google/uuid.UUID.Time` / `Time.UnixTime` decoding, truncation toward zero
/// to microseconds and fixed six-place decimal construction. Original strict
/// string coercion stays in the guard; malformed UUIDs retain Unsupported.
fn uuid_timestamp(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::UuidTimestampNative,
        ctx,
        || Ok(coerce_str(value)?.map(String::into_bytes)),
        crate::tikv::EvaluatedBytesResult::into_decimal_datum,
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::{dispatch, dispatch_in, hash_uint64};
    use crate::Datum;
    use tidb_datatype::{BinaryLiteral, Collation, MysqlEnum, MysqlSet};

    #[derive(Default)]
    struct WarningContext(RefCell<Vec<(u16, String)>>);

    impl crate::Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    /// Exact scalar vectors from `TestAnyValue` in
    /// `pkg/expression/builtin_miscellaneous_test.go`. Each Go signature's
    /// evaluator delegates directly to its one argument.
    #[test]
    fn any_value_returns_its_argument() {
        let source_float = "3.1415926"
            .parse::<f64>()
            .expect("the exact Go test vector is a valid float");
        let cases = [
            (Datum::Null, Datum::Null),
            (Datum::Int(1234), Datum::Int(1234)),
            (Datum::Int(-0x99), Datum::Int(-0x99)),
            (Datum::Real(source_float), Datum::Real(source_float)),
            (
                Datum::new_string("Hello, World".to_string()),
                Datum::new_string("Hello, World".to_string()),
            ),
        ];
        for (argument, want) in cases {
            assert_eq!(
                dispatch("ANY_VALUE", std::slice::from_ref(&argument))
                    .expect("ANY_VALUE must dispatch")
                    .expect("ANY_VALUE must evaluate"),
                want
            );
        }
    }

    /// Go `TestAnyValueHybridStringEvalWithIntSig`: ENUM, SET and BIT select
    /// the integer signature but must still expose their underlying hybrid
    /// bytes in string context. Rust has one value dispatcher rather than
    /// separate scalar/vector signatures, so both halves are asserted here:
    /// `ANY_VALUE` preserves the hybrid datum and string coercion reads its
    /// name/raw bytes instead of its integer ordinal.
    #[test]
    fn test_any_value_hybrid_string_eval_with_int_sig() {
        let cases = [
            (
                Datum::Enum(MysqlEnum::new("b", 2), Collation::Utf8Mb4Bin),
                b"b".as_slice(),
            ),
            (
                Datum::Set(MysqlSet::new("a,b", 3), Collation::Utf8Mb4Bin),
                b"a,b".as_slice(),
            ),
            (
                Datum::Bit(BinaryLiteral::from(vec![0x01])),
                b"\x01".as_slice(),
            ),
        ];

        for (argument, expected_string) in cases {
            let result = dispatch("ANY_VALUE", std::slice::from_ref(&argument))
                .expect("ANY_VALUE must dispatch")
                .expect("hybrid source row must evaluate");
            assert_eq!(result, argument, "the hybrid datum must be preserved");
            assert_eq!(
                crate::coerce::coerce_str_bytes(&result)
                    .expect("hybrid string coercion must succeed")
                    .expect("source rows are non-NULL"),
                expected_string
            );
        }
    }

    #[test]
    fn dispatch_declines_wrong_arity_and_foreign_functions() {
        assert!(dispatch("ANY_VALUE", &[]).is_none());
        assert!(dispatch("ANY_VALUE", &[Datum::Int(1), Datum::Int(2)]).is_none());
        assert!(dispatch("NAME_CONST", &[]).is_none());
        assert!(dispatch("NAME_CONST", &[Datum::new_string("name".to_string())]).is_none());
        assert!(dispatch("NAME_CONST", &[Datum::Int(1)]).is_none());
        assert!(dispatch("NAME_CONST", &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]).is_none());
    }

    /// Exact scalar vectors from `TestNameConst` in
    /// `pkg/expression/builtin_miscellaneous_test.go`.  Every Go
    /// `builtinNameConst*Sig` returns its second argument without changing the
    /// payload; this direct table keeps NULL, signed/unsigned integers,
    /// real, string, binary, and decimal values in their original Datum
    /// domains.  Go's typed temporal/duration/JSON/vector signatures and
    /// FieldType/column-label metadata are deliberately not fabricated here:
    /// the seed Datum domain has no representable variants for them.
    #[test]
    fn name_const_preserves_representable_value_domains() {
        let decimal = crate::Decimal::from_literal("123.123");
        let source_float = "3.14159"
            .parse::<f64>()
            .expect("the exact Go vector is a valid float");
        let cases = [
            (Datum::new_string("test_int"), Datum::Int(3)),
            (Datum::new_string("test_uint"), Datum::UInt(u64::MAX)),
            (Datum::new_string("test_float"), Datum::Real(source_float)),
            (Datum::new_string("test_string"), Datum::new_string("TiDB")),
            (
                Datum::new_string("test_binary"),
                Datum::new_bytes(vec![0, 0xff, 0x80]),
            ),
            (Datum::new_string("test_null"), Datum::Null),
            (Datum::new_string("test_decimal"), Datum::Decimal(decimal)),
            // A NULL label is accepted by Go's ETString conversion; it does
            // not alter the value returned by NAME_CONST.
            (Datum::Null, Datum::Int(-7)),
        ];
        for (name, value) in cases {
            let got = dispatch("NAME_CONST", &[name, value.clone()])
                .expect("NAME_CONST must dispatch for two arguments")
                .expect("NAME_CONST must preserve the scalar value");
            assert_eq!(got, value);
        }
    }

    /// Exact version vectors from `TestUUIDVersion` in
    /// `pkg/expression/builtin_miscellaneous_test.go`; the extra spellings
    /// exercise the same `google/uuid.Parse` compatibility shape as TiDB.
    #[test]
    fn uuid_version_matches_go_uuid_parse_and_version_nibble() {
        let cases = [
            ("5f13f854-d74a-11f0-9b7a-0ae0156bd76b", 1),
            ("c6437ef1-5b86-3a4e-a071-c2d4ad414e65", 3),
            ("a3e3b4a1-ea6d-471e-9860-8303a8b261f6", 4),
            ("271a8175-dadd-5df9-b0bd-20a4a0b441e6", 5),
            ("1f0e48c1-7860-69cc-9b3f-35f89c103d4d", 6),
            ("019b1440-87b7-7380-ab00-ce413e795004", 7),
            ("6ccd780cbaba102695645b8c656024db", 1),
            ("urn:uuid:6ccd780c-baba-1026-9564-5b8c656024db", 1),
            ("{99a9ad03-5298-11ec-8f5c-00ff90147ac3*", 1),
            ("123e4567-e89b-02d3-a456-426614174000", 0),
        ];
        for (text, want) in cases {
            assert_eq!(
                dispatch("UUID_VERSION", &[Datum::new_string(text.to_string())])
                    .expect("UUID_VERSION must dispatch")
                    .expect("well-formed UUID must evaluate"),
                Datum::Int(want),
                "UUID_VERSION({text:?})"
            );
        }
        assert_eq!(
            dispatch("UUID_VERSION", &[Datum::Null])
                .expect("UUID_VERSION must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(
            dispatch("UUID_VERSION", &[Datum::new_string("abc".to_string())])
                .expect("UUID_VERSION must dispatch")
                .is_err()
        );
    }

    /// Exact vectors from `TestIsUUID` in
    /// `pkg/expression/builtin_miscellaneous_test.go`, including the
    /// `google/uuid.Parse` 38-byte compatibility quirk.
    #[test]
    fn is_uuid_matches_go_parse_acceptance() {
        let cases = [
            ("6ccd780c-baba-1026-9564-5b8c656024db", 1),
            ("6CCD780C-BABA-1026-9564-5B8C656024DB", 1),
            ("6ccd780cbaba102695645b8c656024db", 1),
            ("{6ccd780c-baba-1026-9564-5b8c656024db}", 1),
            ("6ccd780c-baba-1026-9564-5b8c6560", 0),
            ("6CCD780C-BABA-1026-9564-5B8C656024DQ", 0),
            (" 6ccd780c-baba-1026-9564-5b8c656024db", 0),
            ("6ccd780c-baba-1026-9564-5b8c656024db ", 0),
            (" 6ccd780c-baba-1026-9564-5b8c656024db ", 0),
            // `uuid.Parse` examines only the middle 36 bytes in a 38-byte
            // input; the leading `{` and trailing `*` are both ignored.
            ("{99a9ad03-5298-11ec-8f5c-00ff90147ac3*", 1),
            ("urn:uuid:99a9ad03-5298-11ec-8f5c-00ff90147ac3", 1),
        ];
        for (text, want) in cases {
            assert_eq!(
                dispatch("IS_UUID", &[Datum::new_string(text.to_string())])
                    .expect("IS_UUID must dispatch")
                    .expect("IS_UUID must evaluate"),
                Datum::Int(want),
                "IS_UUID({text:?})"
            );
        }
        assert_eq!(
            dispatch("IS_UUID", &[Datum::Null])
                .expect("IS_UUID must dispatch")
                .expect("IS_UUID must evaluate"),
            Datum::Null
        );
    }

    #[test]
    fn is_uuid_coerces_the_supported_scalar_domain_to_etstring() {
        // `isUUIDFunctionClass` builds its argument as ETString, so native
        // scalar values are text-coerced before the parser runs.
        for value in [Datum::Int(1), Datum::Real(1.0)] {
            assert_eq!(
                dispatch("IS_UUID", &[value])
                    .expect("IS_UUID must dispatch")
                    .expect("IS_UUID must evaluate"),
                Datum::Int(0)
            );
        }
    }

    #[test]
    fn is_uuid_preserves_go_byte_string_boundaries() {
        for value in [
            Datum::new_string(vec![0xff]),
            Datum::Bytes(vec![0xff]),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff])),
        ] {
            assert_eq!(
                dispatch("IS_UUID", &[value])
                    .expect("IS_UUID must dispatch")
                    .expect("invalid bytes are a non-UUID value, not an evaluation error"),
                Datum::Int(0)
            );
        }

        let mut invalid_wrapper = vec![0xff];
        invalid_wrapper.extend_from_slice(b"99a9ad03-5298-11ec-8f5c-00ff90147ac3*");
        assert_eq!(
            dispatch("IS_UUID", &[Datum::Bytes(invalid_wrapper)])
                .expect("IS_UUID must dispatch")
                .expect("Go's 38-byte UUID parse ignores the invalid wrapper bytes"),
            Datum::Int(1)
        );

        let mut leading_space = vec![b' '];
        leading_space.extend_from_slice(b"99a9ad03-5298-11ec-8f5c-00ff90147ac3");
        leading_space.push(0xff);
        assert_eq!(
            dispatch("IS_UUID", &[Datum::Bytes(leading_space)])
                .expect("IS_UUID must dispatch")
                .expect("Go checks surrounding whitespace before its UUID parse"),
            Datum::Int(0)
        );
    }

    /// Exact source vectors from `TestUUIDTimestamp` in
    /// `pkg/expression/builtin_miscellaneous_test.go`, plus Go's accepted
    /// compact UUID spelling and its invalid-input error outcome.
    #[test]
    fn uuid_timestamp_matches_go_versioned_timestamp_semantics() {
        let cases = [
            ("5f13f854-d74a-11f0-9b7a-0ae0156bd76b", "1765537487.118139"),
            ("1f0e48c1-7860-69cc-9b3f-35f89c103d4d", "1766995078.970004"),
            ("019b1440-87b7-7380-ab00-ce413e795004", "1765571332.023000"),
            ("6ccd780cbaba102695645b8c656024db", "-11129156903.290674"),
        ];
        for (text, want) in cases {
            let expected = if let Some(magnitude) = want.strip_prefix('-') {
                crate::Decimal::from_literal(magnitude).negate()
            } else {
                crate::Decimal::from_literal(want)
            };
            assert_eq!(
                dispatch("UUID_TIMESTAMP", &[Datum::new_string(text.to_string())])
                    .expect("UUID_TIMESTAMP must dispatch")
                    .expect("timestamp UUID must evaluate"),
                Datum::Decimal(expected),
                "UUID_TIMESTAMP({text:?})"
            );
        }
        for text in [
            "c6437ef1-5b86-3a4e-a071-c2d4ad414e65",
            "a3e3b4a1-ea6d-471e-9860-8303a8b261f6",
            "271a8175-dadd-5df9-b0bd-20a4a0b441e6",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ] {
            assert_eq!(
                dispatch("UUID_TIMESTAMP", &[Datum::new_string(text.to_string())])
                    .expect("UUID_TIMESTAMP must dispatch")
                    .expect("valid non-timestamp UUID must evaluate"),
                Datum::Null,
                "UUID_TIMESTAMP({text:?})"
            );
        }
        assert_eq!(
            dispatch("UUID_TIMESTAMP", &[Datum::Null])
                .expect("UUID_TIMESTAMP must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(
            dispatch("UUID_TIMESTAMP", &[Datum::new_string("abc".to_string())])
                .expect("UUID_TIMESTAMP must dispatch")
                .is_err()
        );
    }

    /// Exact success/NULL/error vectors from `TestUUIDToBin` and
    /// `TestBinToUUID` in `pkg/expression/builtin_miscellaneous_test.go`.
    /// UUID_TO_BIN returns raw bytes (including the swap permutation), while
    /// BIN_TO_UUID accepts those bytes without UTF-8 decoding and restores a
    /// lower-case canonical spelling. Warning-count behavior for a malformed
    /// textual swap flag is a session boundary; its ETInt value conversion is
    /// still covered here.
    #[test]
    fn uuid_binary_builtins_match_go_swap_and_raw_byte_vectors() {
        let canonical = "6ccd780c-baba-1026-9564-5b8c656024db";
        let normal = vec![
            0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60,
            0x24, 0xdb,
        ];
        let swapped = vec![
            0x10, 0x26, 0xba, 0xba, 0x6c, 0xcd, 0x78, 0x0c, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60,
            0x24, 0xdb,
        ];

        for spelling in [
            canonical,
            "6CCD780C-BABA-1026-9564-5B8C656024DB",
            "6ccd780cbaba102695645b8c656024db",
            "{6ccd780c-baba-1026-9564-5b8c656024db}",
        ] {
            assert_eq!(
                dispatch("UUID_TO_BIN", &[Datum::new_string(spelling)])
                    .expect("UUID_TO_BIN must dispatch")
                    .expect("valid UUID must evaluate"),
                Datum::Bytes(normal.clone()),
                "UUID_TO_BIN({spelling:?})"
            );
        }
        assert_eq!(
            dispatch(
                "UUID_TO_BIN",
                &[Datum::new_string(canonical), Datum::Int(1)],
            )
            .expect("UUID_TO_BIN must dispatch")
            .expect("swap flag must evaluate"),
            Datum::Bytes(swapped.clone())
        );
        assert_eq!(
            dispatch("UUID_TO_BIN", &[Datum::new_string(canonical), Datum::Null],)
                .expect("UUID_TO_BIN must dispatch")
                .expect("NULL swap flag defaults to zero"),
            Datum::Bytes(normal.clone())
        );
        // Go records a truncation warning for the textual flag "a" but its
        // ETInt value is zero; the warning channel is outside this seed while
        // the value-domain result remains pinned here.
        assert_eq!(
            dispatch(
                "UUID_TO_BIN",
                &[Datum::new_string(canonical), Datum::new_string("a")],
            )
            .expect("UUID_TO_BIN must dispatch")
            .expect("textual flag coercion must evaluate"),
            Datum::Bytes(normal.clone())
        );
        assert_eq!(
            dispatch("UUID_TO_BIN", &[Datum::Null])
                .expect("UUID_TO_BIN must dispatch")
                .expect("NULL UUID must evaluate"),
            Datum::Null
        );
        for invalid in [
            "6ccd780c-baba-1026-9564-5b8c6560",
            " 6ccd780c-baba-1026-9564-5b8c656024db",
            "6ccd780c-baba-1026-9564-5b8c656024db ",
            " 6ccd780c-baba-1026-9564-5b8c656024db ",
        ] {
            assert!(
                dispatch("UUID_TO_BIN", &[Datum::new_string(invalid)])
                    .expect("UUID_TO_BIN must dispatch")
                    .is_err(),
                "invalid UUID_TO_BIN input {invalid:?}"
            );
        }

        assert_eq!(
            dispatch("BIN_TO_UUID", &[Datum::Bytes(normal.clone())])
                .expect("BIN_TO_UUID must dispatch")
                .expect("binary UUID must evaluate"),
            Datum::new_string(canonical)
        );
        assert_eq!(
            dispatch(
                "BIN_TO_UUID",
                &[Datum::Bytes(normal.clone()), Datum::Int(1)],
            )
            .expect("BIN_TO_UUID must dispatch")
            .expect("swap flag must evaluate"),
            Datum::new_string("baba1026-780c-6ccd-9564-5b8c656024db")
        );
        assert_eq!(
            dispatch(
                "BIN_TO_UUID",
                &[Datum::Bytes(normal.clone()), Datum::new_string("a")],
            )
            .expect("BIN_TO_UUID must dispatch")
            .expect("textual flag coercion must evaluate"),
            Datum::new_string(canonical)
        );
        // A raw binary UUID is valid even though the payload is not UTF-8.
        assert_eq!(
            dispatch("BIN_TO_UUID", &[Datum::Bytes(swapped)])
                .expect("BIN_TO_UUID must dispatch")
                .expect("raw bytes must evaluate"),
            Datum::new_string("1026baba-6ccd-780c-9564-5b8c656024db")
        );
        assert_eq!(
            dispatch("BIN_TO_UUID", &[Datum::Null])
                .expect("BIN_TO_UUID must dispatch")
                .expect("NULL binary UUID must evaluate"),
            Datum::Null
        );
        assert!(
            dispatch("BIN_TO_UUID", &[Datum::Bytes(normal[..15].to_vec())])
                .expect("BIN_TO_UUID must dispatch")
                .is_err()
        );
        assert!(dispatch("UUID_TO_BIN", &[]).is_none());
        assert!(dispatch(
            "UUID_TO_BIN",
            &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
        )
        .is_none());
        assert!(dispatch("BIN_TO_UUID", &[]).is_none());
        assert!(dispatch(
            "BIN_TO_UUID",
            &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
        )
        .is_none());
    }

    /// Exact vectors from `TestTidbShard` in
    /// `pkg/expression/builtin_miscellaneous_test.go`. The ciphertext bytes
    /// are produced by Vitess' `HashUint64` (DES-ECB, all-zero key), not by a
    /// result-derived lookup. The additional scalar/string rows pin the
    /// `WrapWithCastAsInt` coercion boundary before hashing.
    #[test]
    fn tidb_shard_matches_vitess_des_and_etint_coercion() {
        let integer_cases = [
            (Datum::Int(-1), 81),
            (Datum::Int(0), 167),
            (Datum::Int(1), 214),
            (Datum::Int(9_999_999_999_999_999), 63),
            // An unsigned source is cast to ETInt and retains its two's-
            // complement bits before `HashUint64` receives the uint64.
            (Datum::UInt(u64::MAX), 81),
        ];
        for (value, want) in integer_cases {
            assert_eq!(
                dispatch("TIDB_SHARD", &[value])
                    .expect("TIDB_SHARD must dispatch")
                    .expect("integer TIDB_SHARD must evaluate"),
                Datum::UInt(want),
            );
        }

        for (text, want) in [
            ("abc", 167),
            ("ope", 167),
            ("wopddd", 167),
            ("1", 214),
            ("-1", 81),
            ("1.9", 214),
        ] {
            assert_eq!(
                dispatch("TIDB_SHARD", &[Datum::new_string(text.to_string())])
                    .expect("TIDB_SHARD must dispatch")
                    .expect("string TIDB_SHARD must evaluate"),
                Datum::UInt(want),
                "TIDB_SHARD({text:?})",
            );
        }

        assert_eq!(
            dispatch("TIDB_SHARD", &[Datum::Real(1.9)])
                .expect("TIDB_SHARD must dispatch")
                .expect("real TIDB_SHARD must evaluate"),
            Datum::UInt(143),
        );
        assert_eq!(
            dispatch(
                "TIDB_SHARD",
                &[Datum::Decimal(crate::Decimal::from_literal("1.9"))],
            )
            .expect("TIDB_SHARD must dispatch")
            .expect("decimal TIDB_SHARD must evaluate"),
            Datum::UInt(143),
        );
        assert_eq!(
            dispatch("TIDB_SHARD", &[Datum::Null])
                .expect("TIDB_SHARD must dispatch")
                .expect("NULL TIDB_SHARD must evaluate"),
            Datum::Null,
        );

        // The Go function class rejects every arity other than one before
        // evaluation; this family dispatch has the same boundary.
        assert!(dispatch("TIDB_SHARD", &[]).is_none());
        assert!(dispatch("TIDB_SHARD", &[Datum::Int(1), Datum::Int(2)]).is_none());
        assert!(dispatch("UNKNOWN", &[Datum::Int(1)]).is_none());
    }

    #[test]
    fn hash_builtins_preserve_implicit_signed_cast_overflow_warnings() {
        for (name, expected) in [
            ("VITESS_HASH", Datum::UInt(hash_uint64((-2_i64) as u64))),
            (
                "TIDB_SHARD",
                Datum::UInt(hash_uint64((-2_i64) as u64) % 256),
            ),
        ] {
            let ctx = WarningContext::default();
            let value = dispatch_in(
                name,
                &[Datum::new_string("18446744073709551614".to_owned())],
                &ctx,
            )
            .expect("the builtin must dispatch")
            .expect("the implicit cast must evaluate");
            assert_eq!(value, expected);
            assert_eq!(
                *ctx.0.borrow(),
                vec![(
                    8030,
                    "Cast to signed converted positive out-of-range integer to its negative complement"
                        .to_owned(),
                )],
            );
        }

        let ctx = WarningContext::default();
        let value = dispatch_in(
            "VITESS_HASH",
            &[Datum::new_string("18446744073709551616".to_owned())],
            &ctx,
        )
        .expect("VITESS_HASH must dispatch")
        .expect("the overflowing implicit cast must retain its best-effort value");
        assert_eq!(value, Datum::UInt(hash_uint64(u64::MAX)));
        assert_eq!(
            *ctx.0.borrow(),
            vec![(
                1292,
                "Truncated incorrect INTEGER value: '18446744073709551616'".to_owned(),
            )],
        );
    }

    /// `VITESS_HASH` returns the whole Vitess DES digest as an UNSIGNED value.
    /// Vectors from `pkg/util/vitess/vitess_hash_test.go` TestVitessHash plus
    /// the two's-complement `u64::MAX` case.
    #[test]
    fn vitess_hash_full_digest() {
        let cases = [
            (Datum::Int(30_375_298_039), 221_350_820_965_191_987_u64),
            (Datum::Int(1123), 223_867_565_019_887_818),
            (Datum::Int(30_573_721_600), 2_233_051_190_281_965_565),
            (Datum::Int(116), 2_168_352_374_666_430_780),
            (Datum::Int(1), 1_615_456_034_434_468_822),
            (Datum::Int(0), 10_134_873_677_816_210_343),
            // An unsigned source is cast to ETInt and keeps its two's-complement
            // bits before the hash receives the uint64.
            (Datum::UInt(u64::MAX), 3_843_066_582_818_235_473),
        ];
        for (value, want) in cases {
            assert_eq!(
                dispatch("VITESS_HASH", &[value])
                    .expect("VITESS_HASH must dispatch")
                    .expect("VITESS_HASH must evaluate"),
                Datum::UInt(want),
            );
        }
        assert_eq!(
            dispatch("VITESS_HASH", &[Datum::Null])
                .expect("VITESS_HASH must dispatch")
                .expect("NULL VITESS_HASH must evaluate"),
            Datum::Null,
        );
        assert!(dispatch("VITESS_HASH", &[]).is_none());
    }
}
