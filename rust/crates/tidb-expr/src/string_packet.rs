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

//! The string builtins whose RESULT SIZE is checked against the session's
//! `max_allowed_packet` before the result is built.
//!
//! In Go these are exactly the signatures that capture
//! `ctx.GetEvalCtx().GetMaxAllowedPacket()` into a struct field while BUILDING
//! (`builtinSpaceSig`, `builtinRepeatSig`, `builtinLpadSig`/`builtinRpadSig`
//! and their UTF-8 twins, `builtinToBase64Sig`, `builtinWeightStringSig`),
//! and that answer NULL with warning 1301 rather than allocating. What they
//! share is that limit rather than any string semantics, which is why they sit
//! together here and not in `crate::string_fn`.
//!
//! [`crate::Columns::max_allowed_packet`] and
//! [`crate::Columns::handle_allowed_packet_overflowed`] are the one seam all
//! of them read.

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp, OutputDisposition, ReadyBytesArg, ReadyIntArg};
use crate::{Datum, EvalError};

/// `REPEAT(str, count)`: `str` concatenated `count` times (empty for
/// `count <= 0`); `NULL` if either argument is `NULL`.
///
/// TiDB selects an `ETString, ETInt` signature, so the count follows the
/// same signed-integer coercion boundary as `EvalInt` (including string
/// numeric prefixes, decimal rounding, and float ties-to-even).  Go strings
/// are byte sequences rather than guaranteed UTF-8; retaining the evaluated
/// string bytes keeps binary input lossless here.  `builtinRepeatSig` checks
/// `byteLength*num` against the session `max_allowed_packet` and answers NULL
/// with warning 1301; the multiplication overflow arm takes the same exit,
/// since a product too large for `usize` is by construction over any limit.
pub(crate) fn repeat(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let [value, count] = vals else {
        return Err(EvalError::Unsupported("bad REPEAT arity"));
    };
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::RepeatNative,
        ctx,
        || {
            let bytes = coerce_str_bytes(value)?;
            let ready_count = if bytes.is_none() {
                // Preserve the original NULL-left demand boundary, rather than
                // coercing an unused count or claiming it was evaluated NULL.
                ReadyIntArg::Undemanded
            } else if *count == Datum::Null {
                ReadyIntArg::Value(None)
            } else {
                ReadyIntArg::Value(Some(crate::cast::to_i64_signed(count)))
            };
            let mut disposition = OutputDisposition::Allow;
            if let (Some(bytes), ReadyIntArg::Value(Some(count))) = (&bytes, &ready_count) {
                // Only packet sizing remains here. Empty/negative answers and
                // count clamping for result generation belong to the kernel.
                if *count > 0 && !bytes.is_empty() {
                    let effective_count = (*count).min(i64::from(i32::MAX)) as usize;
                    let over_packet = match bytes.len().checked_mul(effective_count) {
                        Some(length) => length as u64 > ctx.max_allowed_packet(),
                        None => true,
                    };
                    if over_packet {
                        ctx.handle_allowed_packet_overflowed("repeat")?;
                        disposition = OutputDisposition::SuppressByPacket;
                    }
                }
            }
            Ok(EvaluatedArgs::PacketBytesInt {
                bytes,
                count: ready_count,
                disposition,
            })
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}
/// `SPACE(n)`: a string of `n` spaces (empty for `n <= 0`); `NULL` if the
/// argument is `NULL`.  This is the `ETInt` signature from
/// `builtinSpaceSig.evalString` in `pkg/expression/builtin_string.go`, not
/// an integer-literal-only convenience: decimal arguments round away from
/// zero while float arguments round ties to even through the shared `EvalInt`
/// conversion.  TiDB returns `NULL` rather than allocating above
/// `mysql.MaxBlobWidth`, and warns 1301 above `max_allowed_packet`.
pub(crate) fn space(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad SPACE arity"));
    };
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::SpaceNative,
        ctx,
        || {
            let value = (*value != Datum::Null).then(|| crate::cast::to_i64_signed(value));
            let mut disposition = OutputDisposition::Allow;
            if let Some(width) = value {
                // Packet policy precedes the kernel's silent MaxBlobWidth
                // NULL, including the original getter for zero/negative width.
                if width.max(0) as u64 > ctx.max_allowed_packet() {
                    ctx.handle_allowed_packet_overflowed("space")?;
                    disposition = OutputDisposition::SuppressByPacket;
                }
            }
            Ok(EvaluatedArgs::PacketInt { value, disposition })
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}
/// `LPAD(str, len, pad)` / `RPAD(str, len, pad)`: pad (or truncate) `str` to
/// `len` characters using `pad` on the left/right. Ported from
/// `builtinLpadUTF8Sig`/`builtinRpadUTF8Sig` in `pkg/expression/
/// builtin_string.go` (rune-based, the default for non-binary strings): a
/// NEGATIVE `len` yields `NULL` (not the empty string); `len == 0` yields
/// the empty string; truncation keeps the first `len` chars; an empty `pad`
/// that can't reach `len` yields the empty string. `NULL` if any argument is
/// `NULL` or `len` exceeds TiDB's `mysql.MaxBlobWidth`. A `len` whose result
/// could not fit `max_allowed_packet` is NULL with warning 1301.
pub(crate) fn pad(
    vals: &[Datum],
    left: bool,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    const MAX_BLOB_WIDTH: i64 = 16_777_216;
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad LPAD/RPAD arguments"));
    }
    // `lpadFunctionClass.getFunction` tests BOTH string arguments, so a binary
    // pad string makes the whole call byte-based. This only inspects signature
    // metadata; both string coercions remain after count and packet policy.
    let binary = crate::string_signature::is_binary_str(&vals[0])
        || crate::string_signature::is_binary_str(&vals[2]);
    let operation = match (left, binary) {
        (true, true) => EvaluatedBytesOp::LpadBytesNative,
        (false, true) => EvaluatedBytesOp::RpadBytesNative,
        (true, false) => EvaluatedBytesOp::LpadUtf8Native,
        (false, false) => EvaluatedBytesOp::RpadUtf8Native,
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            let count = match &vals[1] {
                Datum::Null => None,
                // Preserve the ETInt cast's original 1292 warning policy.
                value => Some(crate::cast::to_i64_signed_with_warnings(value, ctx)?),
            };
            let mut disposition = OutputDisposition::Allow;
            let mut demand_strings = false;
            if let Some(len) = count {
                // Packet policy precedes the silent count-domain rejection:
                // byte width for binary, worst-case UTF-8 width otherwise.
                let requested = if binary {
                    u64::try_from(len).unwrap_or(u64::MAX)
                } else {
                    u64::try_from(len)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(MAX_BYTES_OF_CHARACTER)
                };
                if requested > ctx.max_allowed_packet() {
                    ctx.handle_allowed_packet_overflowed(if left { "lpad" } else { "rpad" })?;
                    disposition = OutputDisposition::SuppressByPacket;
                } else {
                    // Only operand demand is decided here; C4 owns every
                    // result, including count-domain NULL and zero length.
                    demand_strings = (0..=MAX_BLOB_WIDTH).contains(&len);
                }
            }
            let (bytes, pad) = if demand_strings {
                // Keep tuple evaluation: a NULL source still coerces the pad.
                let (bytes, pad) = if binary {
                    (coerce_str_bytes(&vals[0])?, coerce_str_bytes(&vals[2])?)
                } else {
                    let (source, pad) = (coerce_str(&vals[0])?, coerce_str(&vals[2])?);
                    (source.map(String::into_bytes), pad.map(String::into_bytes))
                };
                (ReadyBytesArg::Value(bytes), ReadyBytesArg::Value(pad))
            } else {
                (ReadyBytesArg::Undemanded, ReadyBytesArg::Undemanded)
            };
            Ok(EvaluatedArgs::PacketBytesIntBytes {
                bytes,
                count,
                pad,
                disposition,
            })
        },
        |computed| {
            Ok(computed.into_bytes()?.map_or(Datum::Null, |bytes| {
                if binary {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}
/// `WEIGHT_STRING(str [AS {CHAR|BINARY}(n)])`, ported from
/// `builtinWeightStringSig.evalString` in `pkg/expression/builtin_string.go`:
/// the collation SORT KEY of `str`, which is what `ORDER BY` actually
/// compares, surfaced to SQL.
///
/// `padding` is the `AS` clause: `Some((binary, n))`. The two paddings are
/// genuinely different operations, not one with a flag:
///
/// - `AS CHAR(n)` counts RUNES, pads with SPACES, and keys under the
///   ARGUMENT's own collation (`b.args[0].GetType(ctx).GetCollate()`, not the
///   function's -- the function's is forced to `binary`).
/// - `AS BINARY(n)` counts BYTES, pads with NUL, keys under `binary`, and
///   WARNS 1292 when it truncates.
///
/// CAPTURED from TiDB (`HEX` of each):
///
/// ```text
/// weight_string('a')                                -> 61
/// weight_string('A' collate utf8mb4_general_ci)     -> 0041
/// weight_string('ab' as char(1))                    -> 61
/// weight_string('ab' as char(4))                    -> 6162
/// weight_string('ab' as binary(4))                  -> 61620000
/// weight_string('ab' as binary(1))                  -> 61  + warning 1292
/// weight_string('中')                                -> E4B8AD
/// ```
///
/// `weight_string('ab' AS CHAR(4))` is `6162`, NOT `61622020`: the padding
/// spaces go in before the key, and `utf8mb4_bin` is PAD SPACE, so
/// `Collator::key` trims them right back off. That is the whole reason the
/// padding cannot be applied after keying.
///
/// A NUMERIC argument is `builtinWeightStringNullSig` -- always NULL -- and
/// is decided from the argument's FieldType by the CALLER, since Go decides it
/// while BUILDING the function.
pub(crate) fn weight_string(
    value: &Datum,
    padding: Option<(bool, i64)>,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let Some(bytes) = coerce_str_bytes(value)? else {
                return Ok((
                    EvaluatedBytesOp::GetFormatNullNative,
                    EvaluatedArgs::Bytes(None),
                ));
            };
            let tag = collation.native_policy().tag();
            let Some((binary, length)) = padding else {
                return Ok((
                    EvaluatedBytesOp::WeightStringNative,
                    crate::tikv::prepare_weight_string_args(
                        bytes,
                        tag,
                        tidb_datatype::new_collation_enabled(),
                    )?,
                ));
            };
            // Shared classification is used here only to preserve packet getter
            // and warning demand. Original bytes and length enter the worker.
            let delta = if binary {
                tidb_query_expr::native_weight_binary_padding(&bytes, length)
            } else {
                tidb_query_expr::native_weight_char_padding(&bytes, length)
            };
            let mut suppressed = false;
            let budget = if let Some(delta) = delta {
                let budget = ctx.max_allowed_packet();
                if delta > budget {
                    ctx.handle_allowed_packet_overflowed(if binary {
                        "cast_as_binary"
                    } else {
                        "weight_string"
                    })?;
                    suppressed = true;
                }
                Some(budget)
            } else {
                if binary {
                    let length = usize::try_from(length).unwrap_or(0);
                    ctx.append_warning(
                        1292,
                        &format!(
                            "Truncated incorrect BINARY({length}) value: '{}'",
                            tidb_datatype::warning_subject_byte_cap(&String::from_utf8_lossy(
                                &bytes
                            ))
                        ),
                    );
                }
                None
            };
            // The original collator lookup happened after callbacks, and never
            // happened on packet suppression. The worker rechecks the real cap.
            let new_mode = (!suppressed).then(tidb_datatype::new_collation_enabled);
            Ok((
                if binary {
                    EvaluatedBytesOp::WeightStringBinaryNative
                } else {
                    EvaluatedBytesOp::WeightStringCharNative
                },
                crate::tikv::prepare_weight_padded_args(bytes, length, budget, tag, new_mode)?,
            ))
        },
        |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
    )
}

/// The numeric NULL signature consumes real type metadata, not a made-up NULL
/// value: the typed caller must not evaluate its numeric child at all.
pub(crate) fn weight_string_numeric_type(
    code: tidb_datatype::FieldTypeCode,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::WeightStringNumericNative,
        ctx,
        || Ok(EvaluatedArgs::Int(Some(i64::from(code.mysql_type())))),
        |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
    )
}
/// Go `mysql.MaxBytesOfCharacter`: the widest a single character can encode
/// to, which `builtinLpadUTF8Sig`/`builtinRpadUTF8Sig` multiply the requested
/// character count by before testing `max_allowed_packet`.
const MAX_BYTES_OF_CHARACTER: u64 = 4;
/// Go `base64NeededEncodedLength`: the encoded width of `n` input bytes,
/// including the newline every 76 output characters. `None` is Go's `-1`,
/// the input width past which the answer would overflow a signed `int` --
/// which `builtinToBase64Sig` answers NULL for WITHOUT a packet warning,
/// since it never got as far as comparing a length.
fn base64_needed_encoded_length(n: usize) -> Option<u64> {
    // Go's 64-bit arm; the 32-bit constant is for a platform this crate does
    // not build for, and `usize` here is the same width Go's `int` is there.
    if n > 6_827_690_988_321_067_803 {
        return None;
    }
    let length = (n as u64).div_ceil(3) * 4;
    // Go computes `(length-1)/76` in signed `int`, where the empty input's
    // `-1/76` truncates toward zero rather than wrapping.
    Some(length + length.saturating_sub(1) / 76)
}
/// `TO_BASE64(str)`: standard base-64 encoding (with `=` padding) of the
/// argument's bytes.  `builtinToBase64Sig.evalString` in
/// `pkg/expression/builtin_string.go` inserts a newline after every 76
/// encoded characters; `NULL` propagates.  A result over `max_allowed_packet`
/// is NULL with warning 1301.
pub(crate) fn to_base64(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::ToBase64Native,
        ctx,
        || {
            let value = coerce_str_bytes(&vals[0])?;
            let mut disposition = OutputDisposition::Allow;
            if let Some(bytes) = value.as_ref() {
                // A nonrepresentable length skips packet diagnostics, but the
                // original bytes still enter C4: the kernel owns silent NULL.
                if let Some(needed) = base64_needed_encoded_length(bytes.len()) {
                    if needed > ctx.max_allowed_packet() {
                        ctx.handle_allowed_packet_overflowed("to_base64")?;
                        disposition = OutputDisposition::SuppressByPacket;
                    }
                }
            }
            Ok(EvaluatedArgs::PacketBytes { value, disposition })
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}
#[cfg(test)]
mod space_tests {
    use super::{pad, repeat, space, to_base64};
    use crate::{Columns, Datum, Decimal, EvalError, NoColumns};

    /// A session whose `max_allowed_packet` is `limit` and that records the
    /// warnings the builtins raise. The warning is half of what is ported
    /// here: the oversized answer was ALREADY NULL, so a version that only
    /// returned NULL would pass every value assertion and still leave the
    /// client unable to tell a truncated result from a genuine one.
    #[derive(Default)]
    struct Packet {
        limit: u64,
        warnings: std::cell::RefCell<Vec<String>>,
    }

    impl Packet {
        fn new(limit: u64) -> Self {
            Self {
                limit,
                warnings: std::cell::RefCell::default(),
            }
        }
        fn warnings(&self) -> Vec<String> {
            self.warnings.borrow().clone()
        }
    }

    impl Columns for Packet {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn max_allowed_packet(&self) -> u64 {
            self.limit
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push(format!("{code} {message}"));
        }
    }

    /// The whole `max_allowed_packet` family, each against the packet limit
    /// its Go source test uses.
    ///
    /// `TestSpaceSig` and `TestRepeatSig` both build their signature with a
    /// 1000-byte limit; `LPAD`/`RPAD`/`TO_BASE64` are the three that had NO
    /// packet check at all here. The message is Go's
    /// `ErrWarnAllowedPacketOverflowed` verbatim -- CAPTURED from TiDB at the
    /// default limit:
    ///
    /// ```text
    /// select space(70000000);  show warnings;
    ///   Warning 1301 Result of space() was larger than
    ///                max_allowed_packet (67108864) - truncated
    /// ```
    #[test]
    fn max_allowed_packet_overflow_is_null_with_warning_1301() {
        let over = |name: &str, result: Datum, ctx: &Packet| {
            assert_eq!(result, Datum::Null, "{name} over the packet limit is NULL");
            assert_eq!(
                ctx.warnings(),
                vec![format!(
                    "1301 Result of {name}() was larger than max_allowed_packet ({}) - truncated",
                    ctx.limit
                )],
                "{name} must warn exactly once"
            );
        };

        let ctx = Packet::new(1_000);
        assert_eq!(
            space(&[Datum::Int(6)], &ctx),
            Ok(Datum::new_string("      ".to_string()))
        );
        assert!(
            ctx.warnings().is_empty(),
            "an in-budget result must not warn"
        );
        let ctx = Packet::new(1_000);
        over("space", space(&[Datum::Int(1_001)], &ctx).unwrap(), &ctx);

        let ctx = Packet::new(1_000);
        let repeated = repeat(
            &[Datum::new_string("a".to_string()), Datum::Int(1_001)],
            &ctx,
        );
        over("repeat", repeated.unwrap(), &ctx);

        // The rune signature multiplies the requested CHARACTER count by
        // `mysql.MaxBytesOfCharacter` (4) before the comparison, so 251
        // characters already exceed a 1000-byte packet while 250 do not.
        let lpad_args = |len: i64| {
            [
                Datum::new_string("a".to_string()),
                Datum::Int(len),
                Datum::new_string("x".to_string()),
            ]
        };
        let ctx = Packet::new(1_000);
        assert!(pad(&lpad_args(250), true, &ctx).unwrap() != Datum::Null);
        assert!(ctx.warnings().is_empty());
        let ctx = Packet::new(1_000);
        over("lpad", pad(&lpad_args(251), true, &ctx).unwrap(), &ctx);
        let ctx = Packet::new(1_000);
        over("rpad", pad(&lpad_args(251), false, &ctx).unwrap(), &ctx);

        // `base64NeededEncodedLength` is the 4/3 expansion plus one newline
        // per 76 output characters: 741 input bytes need exactly 1000 and fit,
        // 742 need 1005 and do not.
        let ctx = Packet::new(1_000);
        assert!(to_base64(&[Datum::new_string("a".repeat(741))], &ctx).unwrap() != Datum::Null);
        assert!(ctx.warnings().is_empty());
        let ctx = Packet::new(1_000);
        over(
            "to_base64",
            to_base64(&[Datum::new_string("a".repeat(742))], &ctx).unwrap(),
            &ctx,
        );
    }

    /// Complete scalar table from `TestSpace` in
    /// `pkg/expression/builtin_string_test.go`.  The Go test's injected
    /// `errors.New` input is a test harness error path, not a SQL value.
    #[test]
    fn space_matches_go_source_scalar_vectors() {
        let cases = [
            (Datum::Int(0), Datum::new_string(String::new())),
            (Datum::Int(3), Datum::new_string("   ".to_string())),
            (Datum::Int(16_777_217), Datum::Null),
            (Datum::Int(-1), Datum::new_string(String::new())),
            (
                Datum::new_string("abc".to_string()),
                Datum::new_string(String::new()),
            ),
            (
                Datum::new_string("3".to_string()),
                Datum::new_string("   ".to_string()),
            ),
            (Datum::Real(1.2), Datum::new_string(" ".to_string())),
            (Datum::Real(1.9), Datum::new_string("  ".to_string())),
            (Datum::Null, Datum::Null),
        ];
        for (input, want) in cases {
            assert_eq!(space(&[input], &NoColumns), Ok(want));
        }

        // EvalInt's decimal and FLOAT tie rules are intentionally different
        // in TiDB; retain that distinction at this function boundary.
        assert_eq!(
            space(&[Datum::Decimal(Decimal::from_literal("2.5"))], &NoColumns),
            Ok(Datum::new_string("   ".to_string()))
        );
        assert_eq!(
            space(&[Datum::Real(2.5)], &NoColumns),
            Ok(Datum::new_string("  ".to_string()))
        );
        assert_eq!(
            space(&[], &NoColumns),
            Err(EvalError::Unsupported("bad SPACE arity"))
        );
    }
    /// Go `lpadFunctionClass`/`rpadFunctionClass` content semantics: pad to
    /// the target length and TRUNCATE when the source is longer; the rune
    /// signature counts CHARACTERS (the binary one counts bytes); a
    /// negative target length yields NULL.
    #[test]
    fn pad_truncates_and_counts_characters_like_go() {
        let ctx = Packet::new(1_000);
        let args = |s: &str, len: i64, pad_str: &str| {
            [
                Datum::new_string(s.to_string()),
                Datum::Int(len),
                Datum::new_string(pad_str.to_string()),
            ]
        };
        // Truncation: the target length wins over the source.
        assert_eq!(
            pad(&args("hi", 1, "??"), true, &ctx).unwrap(),
            Datum::new_string("h".to_string())
        );
        assert_eq!(
            pad(&args("hi", 1, "??"), false, &ctx).unwrap(),
            Datum::new_string("h".to_string())
        );
        // Padding: `??` repeats left/right to the target length.
        assert_eq!(
            pad(&args("hi", 5, "??"), true, &ctx).unwrap(),
            Datum::new_string("???hi".to_string())
        );
        assert_eq!(
            pad(&args("hi", 5, "??"), false, &ctx).unwrap(),
            Datum::new_string("hi???".to_string())
        );
        // The rune signature counts CHARACTERS: `好` is one character even
        // though it is three bytes.
        assert_eq!(
            pad(&args("好", 2, "xy"), true, &ctx).unwrap(),
            Datum::new_string("x好".to_string())
        );
        // A negative target length yields NULL (Go's out-of-range arm).
        assert_eq!(pad(&args("hi", -1, "??"), true, &ctx).unwrap(), Datum::Null);
    }
}
#[cfg(test)]
mod to_base64_tests {
    use super::to_base64;
    use crate::{Columns, Datum, NoColumns};

    /// Length and newline count of `TO_BASE64` over `byte_count` `'a'` bytes.
    fn shape(byte_count: usize) -> (usize, usize) {
        match to_base64(&[Datum::new_bytes(vec![b'a'; byte_count])], &NoColumns).unwrap() {
            Datum::String(text) => {
                let bytes = text.bytes();
                (bytes.len(), bytes.iter().filter(|&&b| b == b'\n').count())
            }
            other => panic!("expected a string, got {other:?}"),
        }
    }

    /// Go `builtinToBase64Sig` joins the encoded output into 76-char lines with
    /// `\n` (`splitToSubN` + `strings.Join`) ONLY when the length EXCEEDS 76, so
    /// exactly 76 gets no newline and there is never a trailing newline.
    /// goeval-verified `LENGTH(TO_BASE64(REPEAT('a', n)))`: 57 -> 76, 58 -> 81,
    /// 114 -> 153.
    #[test]
    fn to_base64_wraps_at_76_chars_like_go() {
        // 57 bytes -> exactly 76 base64 chars: no wrap, no newline.
        assert_eq!(shape(57), (76, 0));
        // 58 bytes -> 80 base64 chars -> "76\n4": one newline.
        assert_eq!(shape(58), (81, 1));
        // 114 bytes -> 152 base64 chars -> "76\n76": one newline, no trailing.
        assert_eq!(shape(114), (153, 1));
    }

    #[test]
    fn to_base64_null_is_null() {
        assert_eq!(to_base64(&[Datum::Null], &NoColumns).unwrap(), Datum::Null);
    }

    /// The `maxAllowPacket` rows of `TestToBase64Sig`
    /// (`pkg/expression/builtin_string_test.go:2649`) -- the only half of that
    /// test not already covered: its four value rows are asserted byte for byte
    /// by `builtin_ext::string2::tests::to_base64_matches_go_source_vectors`,
    /// including the 76-column wrap of the 64-char alphabet and its triple.
    ///
    /// Here: when the encoded result would exceed `max_allowed_packet`, Go
    /// returns NULL and warns `errWarnAllowedPacketOverflowed`. This layer
    /// used to have no `max_allowed_packet` seam at all and encoded anyway,
    /// so the whole row set was `#[ignore]`d.
    #[test]
    fn to_base64_source_max_allowed_packet_rows() {
        const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        struct Limit(u64);
        impl Columns for Limit {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn max_allowed_packet(&self) -> u64 {
                self.0
            }
        }
        for (input, max_allowed_packet) in [
            ("abc".to_owned(), 3u64),
            (ALPHABET.to_owned(), 88),
            (ALPHABET.repeat(3), 258),
        ] {
            assert_eq!(
                to_base64(
                    &[Datum::new_string(input.clone())],
                    &Limit(max_allowed_packet)
                )
                .unwrap(),
                Datum::Null,
                "{input:?} over its max_allowed_packet must be NULL"
            );
        }
    }
}

#[cfg(test)]
mod weight_string_source_tests {
    use super::weight_string;
    use crate::{Datum, NoColumns};
    use tidb_datatype::Collation;

    #[test]
    fn weight_workers_preserve_raw_bytes_packet_demand_and_diagnostics() {
        use crate::{Columns, EvalError};
        use std::cell::{Cell, RefCell};
        struct Context {
            budget: Cell<u64>,
            strict: Cell<bool>,
            events: RefCell<Vec<String>>,
        }
        impl Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn max_allowed_packet(&self) -> u64 {
                self.events.borrow_mut().push("packet".to_owned());
                self.budget.get()
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.events.borrow_mut().push(format!("{code}:{message}"));
            }
            fn handle_allowed_packet_overflowed(&self, name: &str) -> Result<(), EvalError> {
                self.events.borrow_mut().push(format!("overflow:{name}"));
                if self.strict.get() {
                    Err(EvalError::Unsupported("strict weight packet"))
                } else {
                    Ok(())
                }
            }
        }
        let ctx = Context {
            budget: Cell::new(0),
            strict: Cell::new(false),
            events: RefCell::new(Vec::new()),
        };
        let bytes = |value: &[u8]| Datum::new_bytes(value.to_vec());
        let cases = [
            (bytes(b"a "), None, 0, bytes(b"a "), vec![]),
            (
                bytes(&[0xff, b'a']),
                Some((false, 1)),
                0,
                bytes(b"\xef\xbf\xbd"),
                vec![],
            ),
            (
                bytes(&[0xff]),
                Some((false, 1)),
                0,
                bytes(&[0xff]),
                vec!["packet"],
            ),
            (
                bytes(&[0xff]),
                Some((false, 3)),
                2,
                bytes(&[0xff, b' ', b' ']),
                vec!["packet"],
            ),
            (
                bytes(b"ab"),
                Some((true, 1)),
                0,
                bytes(b"a"),
                vec!["1292:Truncated incorrect BINARY(1) value: 'ab'"],
            ),
            (
                bytes(b"ab"),
                Some((true, 4)),
                2,
                bytes(b"ab\0\0"),
                vec!["packet"],
            ),
            (
                bytes(b"ab"),
                Some((true, 4)),
                1,
                Datum::Null,
                vec!["packet", "overflow:cast_as_binary"],
            ),
            (
                bytes(b"ab"),
                Some((false, 4)),
                1,
                Datum::Null,
                vec!["packet", "overflow:weight_string"],
            ),
            (bytes(b""), Some((false, -1)), 0, bytes(b""), vec!["packet"]),
            (Datum::Null, Some((true, 100)), 0, Datum::Null, vec![]),
        ];
        for slots in [1, 0] {
            let owner = crate::ReadyValuePoolOwner::new(
                crate::ReadyValuePoolPolicy::checked(
                    slots,
                    slots,
                    16 * 1024 * 1024,
                    4 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64,
                    8,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap();
            let execution = owner.begin_execution().unwrap();
            for (value, padding, budget, expected, events) in &cases {
                ctx.budget.set(*budget);
                let result = execution.scope().with_columns(&ctx, |columns| {
                    weight_string(value, *padding, Collation::Binary, columns)
                });
                if slots == 1 {
                    assert_eq!(result.unwrap(), *expected);
                } else {
                    let error = result.expect_err("all WEIGHT results require the actual worker");
                    let EvalError::ExpressionAdapterFailure(failure) = error else {
                        panic!("{error:?}")
                    };
                    assert_eq!(
                        failure.class(),
                        crate::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        crate::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                assert_eq!(ctx.events.take(), *events);
            }
            ctx.budget.set(0);
            ctx.strict.set(true);
            assert!(matches!(
                execution
                    .scope()
                    .with_columns(&ctx, |columns| weight_string(
                        &bytes(b"a"),
                        Some((false, 2)),
                        Collation::Binary,
                        columns
                    )),
                Err(EvalError::Unsupported("strict weight packet"))
            ));
            assert_eq!(ctx.events.take(), ["packet", "overflow:weight_string"]);
            ctx.strict.set(false);
            assert!(matches!(
                execution
                    .scope()
                    .with_columns(&ctx, |columns| weight_string(
                        &Datum::MinNotNull,
                        None,
                        Collation::Binary,
                        columns
                    )),
                Err(EvalError::Unsupported("range sentinel byte coercion"))
            ));
            assert!(ctx.events.take().is_empty());
        }
    }

    type SourceCase = (&'static str, Option<(bool, i64)>, &'static [u8]);

    fn check(collation: Collation, cases: &[SourceCase]) {
        for &(input, padding, expected) in cases {
            assert_eq!(
                weight_string(
                    &Datum::new_string(input.to_string()),
                    padding,
                    collation,
                    &NoColumns,
                ),
                Ok(Datum::new_bytes(expected.to_vec())),
                "{} {input:?} {padding:?}",
                collation.name()
            );
        }
    }

    /// Exact 42-row port of Go `TestCIWeightString` in
    /// `pkg/expression/builtin_string_test.go`. Expected values are the Go
    /// test's literal bytes, independent of Rust's collator implementation.
    #[test]
    fn test_ci_weight_string() {
        check(
            Collation::Utf8Mb4GeneralCi,
            &[
                ("aAÁàãăâ", None, b"\x00A\x00A\x00A\x00A\x00A\x00A\x00A"),
                ("中", None, b"\x4e\x2d"),
                ("a", Some((false, 5)), b"\x00A"),
                ("a ", Some((false, 5)), b"\x00A"),
                ("中", Some((false, 5)), b"\x4e\x2d"),
                ("中 ", Some((false, 5)), b"\x4e\x2d"),
                ("a", Some((true, 1)), b"a"),
                ("ab", Some((true, 1)), b"a"),
                ("a", Some((true, 5)), b"a\0\0\0\0"),
                ("a ", Some((true, 5)), b"a \0\0\0"),
                ("中", Some((true, 1)), b"\xe4"),
                ("中", Some((true, 2)), b"\xe4\xb8"),
                ("中", Some((true, 3)), "中".as_bytes()),
                ("中", Some((true, 5)), b"\xe4\xb8\xad\0\0"),
            ],
        );
        check(
            Collation::Utf8Mb4UnicodeCi,
            &[
                ("aAÁàãăâ", None, b"\x0e3\x0e3\x0e3\x0e3\x0e3\x0e3\x0e3"),
                ("中", None, b"\xfb\x40\xce\x2d"),
                ("a", Some((false, 5)), b"\x0e3"),
                ("a ", Some((false, 5)), b"\x0e3"),
                ("中", Some((false, 5)), b"\xfb\x40\xce\x2d"),
                ("中 ", Some((false, 5)), b"\xfb\x40\xce\x2d"),
                ("a", Some((true, 1)), b"a"),
                ("ab", Some((true, 1)), b"a"),
                ("a", Some((true, 5)), b"a\0\0\0\0"),
                ("a ", Some((true, 5)), b"a \0\0\0"),
                ("中", Some((true, 1)), b"\xe4"),
                ("中", Some((true, 2)), b"\xe4\xb8"),
                ("中", Some((true, 3)), "中".as_bytes()),
                ("中", Some((true, 5)), b"\xe4\xb8\xad\0\0"),
            ],
        );
        check(
            Collation::Utf8Mb40900AiCi,
            &[
                ("aAÁàãăâ", None, b"\x1cG\x1cG\x1cG\x1cG\x1cG\x1cG\x1cG"),
                ("中", None, b"\xfb\x40\xce\x2d"),
                (
                    "a",
                    Some((false, 5)),
                    b"\x1cG\x02\x09\x02\x09\x02\x09\x02\x09",
                ),
                (
                    "a ",
                    Some((false, 5)),
                    b"\x1cG\x02\x09\x02\x09\x02\x09\x02\x09",
                ),
                (
                    "中",
                    Some((false, 5)),
                    b"\xfb\x40\xce\x2d\x02\x09\x02\x09\x02\x09\x02\x09",
                ),
                (
                    "中 ",
                    Some((false, 5)),
                    b"\xfb\x40\xce\x2d\x02\x09\x02\x09\x02\x09\x02\x09",
                ),
                ("a", Some((true, 1)), b"a"),
                ("ab", Some((true, 1)), b"a"),
                ("a", Some((true, 5)), b"a\0\0\0\0"),
                ("a ", Some((true, 5)), b"a \0\0\0"),
                ("中", Some((true, 1)), b"\xe4"),
                ("中", Some((true, 2)), b"\xe4\xb8"),
                ("中", Some((true, 3)), "中".as_bytes()),
                ("中", Some((true, 5)), b"\xe4\xb8\xad\0\0"),
            ],
        );
    }
}
