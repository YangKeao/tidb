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

//! `pkg/expression/builtin_convert_charset.go`: the charset TRANSCODING seam.
//!
//! # Where the bytes of a non-UTF-8 string actually live
//!
//! TiDB stores and evaluates EVERY character string as UTF-8, whatever its
//! declared charset. A `CHARSET gbk` column holds the UTF-8 bytes of its
//! value tagged with a `gbk` collation; the charset constrains which
//! characters may be written (`Datum.ConvertTo` -> `GetStringWithCheck`
//! rejects an unrepresentable one with 1366) and which comparer orders them,
//! but it does NOT change the stored bytes. Captured from TiDB:
//!
//! ```text
//! INSERT INTO g1 /* VARCHAR CHARSET gbk */ VALUES ('一列')   -- accepted
//! INSERT INTO g1 VALUES ('😉')                              -- 1366
//! SELECT HEX(a), LENGTH(a), CHAR_LENGTH(a) FROM g1  ->  D2BBC1D0, 4, 2
//! SELECT HEX(a), LENGTH(a) FROM u1 /* utf8mb4 */    ->  E4B880E58897, 6
//! ```
//!
//! The `D2BBC1D0` is NOT what is stored -- it is produced at the boundary.
//! Go's `HandleBinaryLiteral` wraps a non-legacy-charset argument of a
//! "binary-aware" function (`HEX`, `LENGTH`, `ASCII`, `BIT_LENGTH`,
//! `OCTET_LENGTH`, `TO_BASE64`, the digest functions) with an implicit
//! `to_binary` call, and `to_binary` is the only place the UTF-8 -> GBK
//! transcode happens. `CAST(x AS BINARY)` is the same wrap through the
//! `funcPropAuto` arm. Everything else -- storage, comparison, `CHAR_LENGTH`,
//! `SUBSTRING`, `UPPER` -- reads the UTF-8 form untouched.
//!
//! # latin1 is deliberately NOT transcoded
//!
//! `isLegacyCharset` (utf8, utf8mb4, ascii, latin1, binary) is never wrapped,
//! and `pkg/parser/charset/encoding_latin1.go` builds `latin1` on top of
//! `encodingUTF8` with `encoding.Nop`, an always-true `IsValid`, and an
//! identity `Transform`. So TiDB's `latin1` is a byte-preserving alias for
//! UTF-8, not ISO-8859-1. Captured: `HEX()` of `'é'` in a `latin1` column is
//! `C3A9` (the UTF-8 bytes), not `E9`; a raw `0xE9` inserted into that column
//! stays `E9`; and `CONVERT('一' USING latin1)` returns the untouched
//! `E4B880`. That behavior needs no code here -- `Encoding::Latin1`'s
//! transform is already the identity -- which is why `latin1` never appears
//! in the wrap decision below.

use tidb_datatype::{Datum, FieldType};

use crate::{Columns, EvalError};

/// Go `funcProp`: how a function's arguments meet the charset boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuncProp {
    /// The argument is passed through untouched.
    None,
    /// The result is binary-aware, so a non-legacy-charset argument is
    /// wrapped with `to_binary`.
    BinAware,
    /// The argument is wrapped with `to_binary` or `from_binary` according to
    /// the evaluated result charset.
    Auto,
}

/// Go `isLegacyCharset`: the charsets whose arguments are never wrapped,
/// because their in-memory form already IS their encoded form.
pub fn is_legacy_charset(charset: &str) -> bool {
    matches!(charset, "utf8" | "utf8mb4" | "ascii" | "latin1" | "binary")
}

/// Go `convertActionMap`, inverted into the `convertFuncsMap` lookup.
///
/// Verified 2026-09-04 against Go master
/// (`builtin_convert_charset.go:301-329`) entry by entry: 27 None, 17
/// BinAware, and 37 Auto names plus the five-entry `isLegacyCharset` set all
/// match (see `testport/receipts/convert_funcs_prop_parity.md`).
///
/// The names are this crate's own lowered builtin spellings, which is why
/// `char_length`/`character_length` and the `octet_length` alias appear where
/// Go lists `ast.CharLength`/`ast.OctetLength`.
pub fn func_prop(name: &str) -> FuncProp {
    match name {
        /* args != strings */
        "bin" | "char_func" | "date_format" | "oct" | "space"
        /* only 1 string arg, no implicit conversion */
        | "char_length" | "character_length" | "from_base64" | "lcase" | "left" | "load_file"
        | "lower" | "ltrim" | "mid" | "ord" | "quote" | "repeat" | "reverse" | "right"
        | "rtrim" | "soundex" | "substr" | "substring" | "ucase" | "unhex" | "upper"
        | "weight_string" => FuncProp::None,
        /* result is binary-aware */
        "ascii" | "bit_length" | "hex" | "length" | "octet_length" | "to_base64"
        /* encrypt functions */
        | "aes_decrypt" | "decode" | "encode" | "password" | "md5" | "sha" | "sha1" | "sha2"
        | "sm3" | "compress" | "aes_encrypt" => FuncProp::BinAware,
        /* string functions */
        "concat" | "concat_ws" | "export_set" | "field" | "find_in_set" | "insert_func"
        | "instr" | "lpad" | "locate" | "make_set" | "position" | "replace" | "rpad"
        | "substring_index" | "trim" | "elt"
        /* operators */
        | "ge" | "le" | "gt" | "lt" | "eq" | "ne" | "nulleq" | "if" | "ifnull" | "in"
        | "case" | "cast"
        /* string comparing */
        | "like" | "ilike" | "strcmp"
        /* regex */
        | "regexp" | "regexp_like" | "regexp_instr" | "regexp_substr" | "regexp_replace"
        /* math */
        | "crc32" => FuncProp::Auto,
        _ => FuncProp::None,
    }
}

/// Whether Go's `HandleBinaryLiteral` would wrap this argument with
/// `to_binary` -- the ONLY implicit transcode in the expression tree.
///
/// `from_binary` (the `funcPropAuto` binary-argument-into-string-result arm)
/// is deliberately NOT modelled: reaching it needs the DERIVED result charset
/// of the whole function, which `collation_derive` owns, and every captured
/// case that actually transcodes goes through `to_binary`.
pub fn needs_to_binary(prop: FuncProp, arg_charset: &str, result_charset: &str) -> bool {
    if is_legacy_charset(arg_charset) {
        return false;
    }
    match prop {
        FuncProp::None => false,
        FuncProp::BinAware => true,
        FuncProp::Auto => result_charset == "binary",
    }
}

/// Go `builtinInternalToBinarySig`: UTF-8 in, the argument charset's own
/// encoded bytes out. An unrepresentable character is an error, not a
/// replacement -- `OpEncode` carries `opTruncateTrim`, so the caller sees
/// `ErrInvalidCharacterString`.
pub fn to_binary(value: &Datum, arg_charset: &str) -> Result<Datum, EvalError> {
    to_binary_in(value, arg_charset, &crate::NoColumns)
}

/// Encodes with the caller's execution context.
pub fn to_binary_in(
    value: &Datum,
    arg_charset: &str,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_to_binary_in(ctx, value, arg_charset)
}

/// [`to_binary`] driven by the datum's OWN collation rather than a static
/// argument type, for the value-only evaluator that has no field types.
pub fn to_binary_by_collation(value: &Datum) -> Result<Datum, EvalError> {
    to_binary_by_collation_in(value, &crate::NoColumns)
}

/// Uses the caller's context only when the datum needs actual transcoding.
pub fn to_binary_by_collation_in(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let Some(collation) = value.collation() else {
        return Ok(value.clone());
    };
    let charset = collation.charset().name();
    if is_legacy_charset(charset) {
        return Ok(value.clone());
    }
    to_binary_in(value, charset, ctx)
}

/// Go `builtinInternalFromBinarySig`: encoded bytes in, UTF-8 out.
pub fn from_binary(value: &Datum, target_charset: &str) -> Result<Datum, EvalError> {
    from_binary_in(value, target_charset, &crate::NoColumns)
}

/// Decodes with the caller's execution context.
pub fn from_binary_in(
    value: &Datum,
    target_charset: &str,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_from_binary_in(ctx, value, target_charset)
}

/// Go `builtinConvertSig`: `CONVERT(expr USING charset)`.
///
/// Because the in-memory form is always UTF-8, converting BETWEEN two
/// character sets does not touch the bytes at all -- it only RETAGS the
/// value, replacing any character the target cannot represent with `?`
/// (`OpReplaceNoErr`) rather than failing. Only the binary boundary really
/// transcodes. Captured: `HEX(CONVERT('一列' USING gbk))` is `D2BBC1D0`
/// because `HEX` then wraps the gbk-tagged result in `to_binary`, while
/// `HEX(CONVERT(CONVERT('一列' USING gbk) USING utf8mb4))` is `E4B880E58897`
/// -- the same untouched bytes -- and `CONVERT('😉' USING gbk)` is `'?'`.
pub fn convert_using(
    value: &Datum,
    arg_type: &FieldType,
    result_charset: &str,
) -> Result<Datum, EvalError> {
    convert_using_in(value, arg_type, result_charset, &crate::NoColumns)
}

/// Converts using the caller's execution context and the original source type.
pub fn convert_using_in(
    value: &Datum,
    arg_type: &FieldType,
    result_charset: &str,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_convert_using_in(ctx, value, arg_type, result_charset)
}
