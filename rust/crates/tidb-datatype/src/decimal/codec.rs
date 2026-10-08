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

use super::{pad_scale, Decimal, DecimalDigits};

// ===========================================================================
// Binary storage codec: faithful port of Go `MyDecimal` `ToBin`/`DecimalBinSize`
// (`pkg/types/mydecimal.go`). This is how a `DECIMAL` is stored in a TiKV row
// payload, and it is memcmp-comparable, so the emitted bytes must be
// byte-identical to TiDB (verified against Go `ToBin` output vectors).
//
// The algorithm operates on the base-1e9 word-buffer view of the value, exactly
// as Go does; the Rust `Decimal` keeps a normalized digit string, so
// `MyDecimalWords::from_decimal` reconstructs Go's `wordBuf`/`digitsInt`/
// `digitsFrac` view (mirroring Go `FromString`'s population), and `to_bin`
// below is then a line-for-line port of Go `WriteBin`.
//
// Public parsing applies Go's fixed nine-word bound before values reach this
// representation. The word view below therefore reconstructs the exact source
// payload rather than accepting an arbitrary-precision compatibility branch.

pub(super) const DIGITS_PER_WORD: usize = 9;
pub(super) const CODEC_WORD_BUF_LEN: usize = 9;
/// Powers retained for native word-view validation and projection.
pub(super) const CODEC_POWERS10: [i32; 10] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
];
/// Hard codec failure — Go `ErrBadNumber` (illegal precision/scale, or a corrupt
/// binary). Truncation/overflow are soft and reported as [`DecimalCodecWarning`].
pub use tidb_query_datatype::codec::mysql::NativeDecimalCodecError as DecimalCodecError;

/// Failure state returned by [`Decimal::from_bin_with_failure`].
///
/// Go's `MyDecimal.FromBin` mutates its receiver to the zero value before
/// returning `ErrBadNumber` for a corrupt payload, while still returning the
/// legal fixed payload length. Keeping those values beside the error lets row
/// decoders make the same cursor-progress decision without weakening the
/// existing [`Decimal::from_bin`] API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalCodecFailure {
    /// Go's post-error receiver state (`zeroMyDecimal`).
    pub value: Decimal,
    /// Fixed payload bytes consumed/available according to the requested
    /// precision and scale. Zero means the shape itself was invalid.
    pub consumed: usize,
    /// The underlying hard codec error.
    pub error: DecimalCodecError,
}

/// Soft codec outcome carried beside a valid result, mirroring Go's non-fatal
/// `ErrTruncated`/`ErrOverflow` returned from `ToBin`/`FromBin`.
pub use tidb_query_datatype::codec::mysql::NativeDecimalCodecWarning as DecimalCodecWarning;

/// Go `digitsToWords`: number of 1e9 words needed for `digits` decimal digits.
pub(super) fn digits_to_words(digits: usize) -> usize {
    digits.div_ceil(DIGITS_PER_WORD)
}

/// Go `DecimalBinSize`: byte length of the fixed-length binary for
/// `{precision, frac}`, independent of any particular value.
pub fn decimal_bin_size(precision: i32, frac: i32) -> Result<usize, DecimalCodecError> {
    tidb_query_datatype::codec::mysql::native_decimal_bin_size(precision, frac)
}

/// Go `MyDecimal`'s codec-relevant view: sign, integer/fraction digit counts,
/// and the base-1e9 word buffer, built from a [`Decimal`] exactly as Go
/// `FromString` builds it so the ported `ToBin` is line-for-line.
pub(crate) struct MyDecimalWords {
    pub(crate) negative: bool,
    pub(crate) digits_int: i32,
    pub(crate) digits_frac: i32,
    pub(crate) word_buf: [i32; CODEC_WORD_BUF_LEN],
}

impl MyDecimalWords {
    /// Mirrors Go `FromString`'s `wordBuf` population: integer digits packed
    /// most-significant-word-first, the trailing partial fraction word
    /// left-aligned into the high digit positions, and the nine-word clamp.
    pub(super) fn from_decimal(d: &Decimal) -> Self {
        let parts = d
            .try_to_shared_math(usize::MAX)
            .and_then(|value| value.try_native_word_projection(usize::MAX))
            .expect("shared native decimal word projection failed");
        MyDecimalWords {
            negative: parts.negative,
            digits_int: i32::from(parts.int_digits),
            digits_frac: i32::from(parts.frac_digits),
            word_buf: parts.words.map(|word| word as i32),
        }
    }

    /// Reconstructs a normalized [`Decimal`] from the word view, extracting the
    /// coefficient digits exactly as Go `ToString` reads them: integer words
    /// least-significant-first (each digit via `/10`), fraction words
    /// most-significant-first (each digit via `/1e8`, left-aligned). The Rust
    /// `Decimal` re-normalizes and `Display` re-inserts the point/sign, matching
    /// Go `ToString`.
    pub(super) fn to_decimal(&self) -> Decimal {
        let parts = tidb_query_datatype::codec::mysql::native_decimal_words_to_parts(
            self.negative,
            self.digits_int,
            self.digits_frac,
            &self.word_buf,
        );
        let digits = DecimalDigits::from_ascii(parts.coefficient);
        if parts.scale > 0 {
            Decimal::new_with_storage_preserving_zero_sign(
                parts.negative,
                digits,
                parts.scale,
                parts.scale,
            )
        } else {
            Decimal::new_with_storage(parts.negative, digits, parts.scale, parts.scale)
        }
    }
}

impl Decimal {
    /// Faithful port of Go `MyDecimal.ToBin`/`WriteBin`: the fixed-length,
    /// memcmp-comparable binary encoding at `{precision, frac}`. Returns the
    /// bytes plus any soft truncation/overflow, or [`DecimalCodecError`] for an
    /// illegal `{precision, frac}`.
    pub fn to_bin(
        &self,
        precision: i32,
        frac: i32,
    ) -> Result<(Vec<u8>, Option<DecimalCodecWarning>), DecimalCodecError> {
        let mut bin = vec![0u8; checked_bin_size(precision, frac)?];
        let warning = MyDecimalWords::from_decimal(self).write_bin(precision, frac, &mut bin)?;
        Ok((bin, warning))
    }
}

/// Go `ToBin`'s legality check plus `DecimalBinSize`: the byte length of the
/// encoding at `{precision, frac}`, or `ErrBadNumber` for a shape Go rejects.
pub(crate) fn checked_bin_size(precision: i32, frac: i32) -> Result<usize, DecimalCodecError> {
    tidb_query_datatype::codec::mysql::native_decimal_checked_bin_size(precision, frac)
}

impl MyDecimalWords {
    /// Go `MyDecimal.WriteBin`: encodes the words at `{precision, frac}` into
    /// `bin`, which the caller has sized with [`checked_bin_size`]. Returns
    /// the soft truncation/overflow Go reports beside the bytes.
    pub(crate) fn write_bin(
        &self,
        precision: i32,
        frac: i32,
        bin: &mut [u8],
    ) -> Result<Option<DecimalCodecWarning>, DecimalCodecError> {
        tidb_query_datatype::codec::mysql::native_decimal_write_bin(
            self.negative,
            self.digits_int,
            self.digits_frac,
            &self.word_buf,
            precision,
            frac,
            bin,
        )
    }
}

impl Decimal {
    /// Faithful port of Go `MyDecimal.FromBin`: decodes the fixed-length binary
    /// produced by [`Self::to_bin`] at `{precision, frac}` back into a
    /// [`Decimal`], returning it, the number of bytes consumed, and any soft
    /// truncation/overflow. A hard [`DecimalCodecError`] signals an illegal
    /// `{precision, frac}`, an oversized layout, or a corrupt payload.
    pub fn from_bin(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(Decimal, usize, Option<DecimalCodecWarning>), DecimalCodecError> {
        Self::from_bin_with_failure(bin, precision, frac).map_err(|failure| failure.error)
    }

    /// Decodes a binary decimal while retaining Go's post-error receiver and
    /// cursor state. For a corrupt payload with a legal shape, the error value
    /// contains zero and `consumed == DecimalBinSize(precision, frac)`;
    /// malformed shapes and an empty input report `consumed == 0`.
    pub fn from_bin_with_failure(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(Decimal, usize, Option<DecimalCodecWarning>), DecimalCodecFailure> {
        let decoded =
            tidb_query_datatype::codec::mysql::native_decimal_decode_bin(bin, precision, frac)
                .map_err(|failure| DecimalCodecFailure {
                    value: Decimal::from_literal("0"),
                    consumed: failure.consumed,
                    error: failure.error,
                })?;
        let words = MyDecimalWords {
            negative: decoded.negative,
            digits_int: decoded.digits_int,
            digits_frac: decoded.digits_frac,
            word_buf: decoded.words,
        };
        Ok((words.to_decimal(), decoded.consumed, decoded.warning))
    }

    /// Go `MyDecimal.MarshalJSON`'s exact persistence object.
    pub fn mysql_json_value(&self) -> serde_json::Value {
        let words = MyDecimalWords::from_decimal(self);
        let mut object = serde_json::Map::new();
        object.insert(
            "DigitsInt".to_owned(),
            serde_json::Value::from(words.digits_int),
        );
        object.insert(
            "DigitsFrac".to_owned(),
            serde_json::Value::from(words.digits_frac),
        );
        object.insert("ResultFrac".to_owned(), serde_json::Value::from(self.scale));
        object.insert(
            "Negative".to_owned(),
            serde_json::Value::from(words.negative),
        );
        object.insert(
            "WordBuf".to_owned(),
            serde_json::Value::Array(
                words
                    .word_buf
                    .into_iter()
                    .map(serde_json::Value::from)
                    .collect(),
            ),
        );
        serde_json::Value::Object(object)
    }

    /// Go `MyDecimal.UnmarshalJSON`'s persistence object decoder.
    pub fn from_mysql_json_value(value: &serde_json::Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "MyDecimal JSON must be an object".to_owned())?;
        let read_i32 = |name: &str| {
            object
                .get(name)
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| format!("MyDecimal JSON is missing {name}"))
        };
        let digits_int = read_i32("DigitsInt")?;
        let digits_frac = read_i32("DigitsFrac")?;
        let result_frac = read_i32("ResultFrac")?;
        if digits_int < 0 || digits_frac < 0 || result_frac < 0 {
            return Err("MyDecimal JSON contains negative metadata".to_owned());
        }
        let negative = object
            .get("Negative")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| "MyDecimal JSON is missing Negative".to_owned())?;
        let encoded_words = object
            .get("WordBuf")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "MyDecimal JSON is missing WordBuf".to_owned())?;
        if encoded_words.len() != CODEC_WORD_BUF_LEN {
            return Err("MyDecimal JSON WordBuf must contain nine words".to_owned());
        }
        let mut word_buf = [0; CODEC_WORD_BUF_LEN];
        for (output, encoded) in word_buf.iter_mut().zip(encoded_words) {
            *output = encoded
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| "MyDecimal JSON contains an invalid word".to_owned())?;
        }
        let raw = MyDecimalWords {
            negative,
            digits_int,
            digits_frac,
            word_buf,
        }
        .to_decimal();
        let result_frac = result_frac as u32;
        let storage_scale = raw.storage_scale.max(result_frac);
        let digits = pad_scale(&raw.digits, raw.storage_scale, storage_scale);
        Ok(Decimal::new_with_storage_preserving_zero_sign(
            negative,
            digits,
            result_frac,
            storage_scale,
        ))
    }
}
