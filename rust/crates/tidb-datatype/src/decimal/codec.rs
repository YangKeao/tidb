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

use smallvec::SmallVec;

use super::{pad_scale, Decimal, DecimalDigits, INLINE_DECIMAL_DIGITS};

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
const CODEC_WORD_SIZE: usize = 4;
pub(super) const CODEC_WORD_BUF_LEN: usize = 9;
/// Largest value one 1e9 word holds (Go `wordMax` = `wordBase - 1`).
const CODEC_WORD_MAX: i32 = 999_999_999;
/// Bytes needed to store `k` decimal digits packed into one partial word.
const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
/// `10^k` for `k` in `0..=9` (all fit in `i32`; `10^9 < i32::MAX`).
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

/// Go `fixWordCntError`: clamp a word count to the nine-word buffer, reporting
/// the overflow/truncation Go would.
pub(super) fn fix_word_cnt_error(
    words_int: usize,
    words_frac: usize,
) -> (usize, usize, Option<DecimalCodecWarning>) {
    if words_int + words_frac > CODEC_WORD_BUF_LEN {
        if words_int > CODEC_WORD_BUF_LEN {
            return (CODEC_WORD_BUF_LEN, 0, Some(DecimalCodecWarning::Overflow));
        }
        return (
            words_int,
            CODEC_WORD_BUF_LEN - words_int,
            Some(DecimalCodecWarning::Truncated),
        );
    }
    (words_int, words_frac, None)
}

/// Go `readWord`: sign-extending big-endian load of a `size`-byte word.
fn read_word(b: &[u8], size: usize) -> i32 {
    match size {
        1 => i32::from(b[0] as i8),
        2 => (i32::from(b[0] as i8) << 8) + i32::from(b[1]),
        3 => {
            if b[0] & 128 > 0 {
                (0xFF00_0000u32
                    | (u32::from(b[0]) << 16)
                    | (u32::from(b[1]) << 8)
                    | u32::from(b[2])) as i32
            } else {
                ((u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2])) as i32
            }
        }
        4 => {
            i32::from(b[3])
                + (i32::from(b[2]) << 8)
                + (i32::from(b[1]) << 16)
                + (i32::from(b[0] as i8) << 24)
        }
        _ => 0,
    }
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
        let (word_start_idx, digits_int) = self.remove_leading_zeros();
        let digits_frac = self.digits_frac;

        // Build one coefficient buffer. The previous implementation allocated
        // separate integer/fraction vectors and then copied both into a
        // String; that turns every DECIMAL cell into several heap operations.
        // Go's MyDecimal already owns a fixed word buffer, so keep the Rust
        // representation to one final coefficient allocation as well.
        let int_len = digits_int.max(0) as usize;
        let fraction_len = digits_frac.max(0) as usize;
        let mut coefficient = SmallVec::<[u8; INLINE_DECIMAL_DIGITS]>::new();
        coefficient.resize(int_len + fraction_len, b'0');
        if digits_int > 0 {
            let mut pos = int_len;
            let mut word_idx = word_start_idx + digits_to_words(digits_int as usize);
            let mut remaining = digits_int;
            while remaining > 0 {
                word_idx -= 1;
                let mut x = self.word_buf[word_idx];
                let take = remaining.min(DIGITS_PER_WORD as i32);
                for _ in 0..take {
                    let y = x / 10;
                    pos -= 1;
                    coefficient[pos] = b'0' + (x - y * 10) as u8;
                    x = y;
                }
                remaining -= DIGITS_PER_WORD as i32;
            }
        }

        // Fraction coefficient digits, built left-to-right like Go `ToString`.
        if digits_frac > 0 {
            let dig_mask = CODEC_POWERS10[DIGITS_PER_WORD - 1]; // ten8 = 10^8
            let mut word_idx = word_start_idx + digits_to_words(digits_int.max(0) as usize);
            let mut remaining = digits_frac;
            let mut offset = int_len;
            while remaining > 0 {
                let mut x = self.word_buf[word_idx];
                word_idx += 1;
                let take = remaining.min(DIGITS_PER_WORD as i32);
                for _ in 0..take {
                    let y = x / dig_mask;
                    coefficient[offset] = b'0' + y as u8;
                    offset += 1;
                    x -= y * dig_mask;
                    x *= 10;
                }
                remaining -= DIGITS_PER_WORD as i32;
            }
        }

        if coefficient.is_empty() {
            coefficient.push(b'0');
        }
        let digits = DecimalDigits::from_ascii(coefficient);
        let scale = digits_frac.max(0) as u32;
        if digits_frac > 0 {
            Decimal::new_with_storage_preserving_zero_sign(self.negative, digits, scale, scale)
        } else {
            // Go `FromBin` resets to `zeroMyDecimal` only when both digit
            // counts are zero, clearing a non-canonical sign at scale zero.
            Decimal::new_with_storage(self.negative, digits, scale, scale)
        }
    }

    /// Go `removeLeadingZeros`: index of the first significant word and the
    /// count of significant integer digits.
    fn remove_leading_zeros(&self) -> (usize, i32) {
        tidb_query_datatype::codec::mysql::native_decimal_remove_leading_zeros(
            self.digits_int,
            &self.word_buf,
        )
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
        let zero = || DecimalCodecFailure {
            value: Decimal::from_literal("0"),
            consumed: 0,
            error: DecimalCodecError::BadNumber,
        };
        if bin.is_empty() {
            return Err(zero());
        }
        let digits_int = precision - frac;
        let words_int = digits_int / DIGITS_PER_WORD as i32;
        let leading_digits = digits_int - words_int * DIGITS_PER_WORD as i32;
        let mut words_frac = frac / DIGITS_PER_WORD as i32;
        let mut trailing_digits = frac - words_frac * DIGITS_PER_WORD as i32;
        let mut words_int_to = words_int;
        if leading_digits > 0 {
            words_int_to += 1;
        }
        let mut words_frac_to = words_frac;
        if trailing_digits > 0 {
            words_frac_to += 1;
        }

        // Sign lives in the top bit of the first byte (0 => negative).
        let mask: i32 = if bin[0] & 0x80 > 0 { 0 } else { -1 };
        let bin_size = decimal_bin_size(precision, frac)
            .map_err(|error| DecimalCodecFailure { error, ..zero() })?;
        if bin_size > 40 {
            return Err(DecimalCodecFailure {
                value: Decimal::from_literal("0"),
                consumed: 0,
                error: DecimalCodecError::BadNumber,
            });
        }

        // Private copy with the sign bit restored (Go pads to 40 then slices;
        // only [0..bin_size] is ever read). Keep this fixed-size buffer on the
        // stack: DecodeDecimal is on the hot row-response path and the Go
        // MyDecimal decoder does not allocate a payload-sized buffer.
        let mut buf = [0u8; 40];
        let n = bin.len().min(bin_size);
        buf[..n].copy_from_slice(&bin[..n]);
        buf[0] ^= 0x80;

        let mut bin_idx = 0usize;
        let mut warning: Option<DecimalCodecWarning> = None;
        let old_words_int_to = words_int_to;
        let (fixed_int, fixed_frac, warn) =
            fix_word_cnt_error(words_int_to as usize, words_frac_to as usize);
        words_int_to = fixed_int as i32;
        words_frac_to = fixed_frac as i32;
        if warn.is_some() {
            warning = warn;
            if words_int_to < old_words_int_to {
                bin_idx += DIG2BYTES[leading_digits as usize]
                    + (words_int - words_int_to) as usize * CODEC_WORD_SIZE;
            } else {
                trailing_digits = 0;
                words_frac = words_frac_to;
            }
        }

        let mut w = MyDecimalWords {
            negative: mask != 0,
            digits_int: words_int * DIGITS_PER_WORD as i32 + leading_digits,
            digits_frac: words_frac * DIGITS_PER_WORD as i32 + trailing_digits,
            word_buf: [0i32; CODEC_WORD_BUF_LEN],
        };

        let mut word_idx = 0usize;
        if leading_digits > 0 {
            let i = DIG2BYTES[leading_digits as usize];
            let x = read_word(&buf[bin_idx..], i);
            bin_idx += i;
            w.word_buf[word_idx] = x ^ mask;
            if u64::from(w.word_buf[word_idx] as u32)
                >= u64::from(CODEC_POWERS10[leading_digits as usize + 1] as u32)
            {
                return Err(DecimalCodecFailure {
                    value: Decimal::from_literal("0"),
                    consumed: bin_size,
                    error: DecimalCodecError::BadNumber,
                });
            }
            if word_idx > 0 || w.word_buf[word_idx] != 0 {
                word_idx += 1;
            } else {
                w.digits_int -= leading_digits;
            }
        }

        let stop = bin_idx + words_int as usize * CODEC_WORD_SIZE;
        while bin_idx < stop {
            w.word_buf[word_idx] = read_word(&buf[bin_idx..], CODEC_WORD_SIZE) ^ mask;
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(DecimalCodecFailure {
                    value: Decimal::from_literal("0"),
                    consumed: bin_size,
                    error: DecimalCodecError::BadNumber,
                });
            }
            if word_idx > 0 || w.word_buf[word_idx] != 0 {
                word_idx += 1;
            } else {
                w.digits_int -= DIGITS_PER_WORD as i32;
            }
            bin_idx += CODEC_WORD_SIZE;
        }

        let stop = bin_idx + words_frac as usize * CODEC_WORD_SIZE;
        while bin_idx < stop {
            w.word_buf[word_idx] = read_word(&buf[bin_idx..], CODEC_WORD_SIZE) ^ mask;
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(DecimalCodecFailure {
                    value: Decimal::from_literal("0"),
                    consumed: bin_size,
                    error: DecimalCodecError::BadNumber,
                });
            }
            word_idx += 1;
            bin_idx += CODEC_WORD_SIZE;
        }

        if trailing_digits > 0 {
            let i = DIG2BYTES[trailing_digits as usize];
            let x = read_word(&buf[bin_idx..], i);
            w.word_buf[word_idx] =
                (x ^ mask) * CODEC_POWERS10[DIGITS_PER_WORD - trailing_digits as usize];
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(DecimalCodecFailure {
                    value: Decimal::from_literal("0"),
                    consumed: bin_size,
                    error: DecimalCodecError::BadNumber,
                });
            }
        }

        Ok((w.to_decimal(), bin_size, warning))
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
